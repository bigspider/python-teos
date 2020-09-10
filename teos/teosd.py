import os
import daemon
import subprocess
from sys import argv, exit
import multiprocessing
import threading

from getopt import getopt, GetoptError
from signal import signal, SIGINT, SIGQUIT, SIGTERM

from common.logger import setup_logging, get_logger
from common.config_loader import ConfigLoader
from common.cryptographer import Cryptographer
from common.tools import setup_data_folder

import teos.api as api
import teos.rpc as rpc
from teos.logging_server import serve as serve_logging
from teos.help import show_usage
from teos.watcher import Watcher
from teos.builder import Builder
from teos.carrier import Carrier
from teos.users_dbm import UsersDBM
from teos.responder import Responder
from teos.gatekeeper import Gatekeeper
from teos.internal_api import InternalAPI
from teos.chain_monitor import ChainMonitor
from teos.block_processor import BlockProcessor
from teos.appointments_dbm import AppointmentsDBM
from teos import DATA_DIR, DEFAULT_CONF, CONF_FILE_NAME
from teos.tools import can_connect_to_bitcoind, in_correct_network, get_default_rpc_port
from teos.constants import INTERNAL_API_ENDPOINT, SHUTDOWN_GRACE_TIME

parent_pid = os.getpid()


def get_config(command_line_conf, data_dir):
    """
    Combines the command line config with the config loaded from the file and the default config in order to construct
    the final config object.

    Args:
        command_line_conf (:obj:`dict`): a collection of the command line parameters.

    Returns:
        :obj:`dict`: A dictionary containing all the system's configuration parameters.
    """

    config_loader = ConfigLoader(data_dir, CONF_FILE_NAME, DEFAULT_CONF, command_line_conf)
    config = config_loader.build_config()

    # Set default RPC port if not overwritten by the user.
    if "BTC_RPC_PORT" not in config_loader.overwritten_fields:
        config["BTC_RPC_PORT"] = get_default_rpc_port(config.get("BTC_NETWORK"))

    return config


class TeosDaemon:
    """
    The :class:`TeosDaemon` organizes the code to initialize all the components of teos, start the service, stop and
    teardown.

    Args:
        config (:obj:`dict`): the configuration object.
        sk (:obj:`PrivateKey`): the ``PrivateKey`` of the tower.
        logger: the logger instance
        ready_event: (:obj:`multiprocessing.Event` or :obj:`None`): if given, an Event that will be set as soon as the
            all the components and services are initialized, therefore the tower is ready to use.

    Attributes:
        stop_command_event (:obj:`threading.Event`): the Event that will be set to initiate a graceful shutdown.
        stop_event (:obj:`multiprocessing.Event`): the Event that services running on different processes will monitor
            in order to be informed that they should shutdown.
        block_processor (:obj:`teos.block_processor.BlockProcessor`): the BlockProcessor instance.
        db_manager (:obj:`teos.appointments_dbm.AppointmentsDBM`): the db manager for appointments.
        watcher (:obj:`teos.watcher.Watcher`): the `Watcher` instance.
        watcher_thread (:obj:`multithreading.Thread` or :obj:`None`): after ``bootstrap_components``, the thread that
            runs the Watcher monitoring.
        responder_thread (:obj:`multithreading.Thread` or :obj:`None`): after ``bootstrap_components``, the thread that
            runs the Responder monitoring.
        chain_monitor (:obj:`teos.chain_monitor.ChainMonitor`): the ``ChainMonitor`` instance.
        self.api_proc (:obj:`subprocess.Popen` or :obj:`multiprocessing.Process` or :obj:`None`): once the rpc process
            is created, the instance of either ``Popen`` or ``Process`` that is serving the public API.
        self.rpc_process (:obj:`multiprocessing.Process`): the instance of the internal RPC server; only set if running.
        self.internal_api (:obj:`teos.internal_api.InternalAPI`): the InternalAPI instance.
    """

    def __init__(self, config, sk, logger, ready_event=None):
        self.config = config
        self.logger = logger
        self.ready_event = ready_event

        # event triggered when a ``stop`` command is issued
        # Using multiprocessing.Event seems to cause a deadlock if event.set() is called in a signal handler that
        # interrupted event.wait(). This does not happen with threading.Event.
        # See https://bugs.python.org/issue41606
        self.stop_command_event = threading.Event()

        # event triggered when the public API is halted, hence teosd is ready to stop
        self.stop_event = multiprocessing.Event()

        bitcoind_connect_params = {k: v for k, v in config.items() if k.startswith("BTC_RPC")}
        bitcoind_feed_params = {k: v for k, v in config.items() if k.startswith("BTC_FEED")}

        if not can_connect_to_bitcoind(bitcoind_connect_params):
            raise RuntimeError("Cannot connect to bitcoind")

        elif not in_correct_network(bitcoind_connect_params, config.get("BTC_NETWORK")):
            raise RuntimeError("bitcoind is running on a different network, check teos.conf and bitcoin.conf")

        self.logger.info("tower_id = {}".format(Cryptographer.get_compressed_pk(sk.public_key)))
        self.block_processor = BlockProcessor(bitcoind_connect_params)
        carrier = Carrier(bitcoind_connect_params)

        gatekeeper = Gatekeeper(
            UsersDBM(self.config.get("USERS_DB_PATH")),
            self.block_processor,
            self.config.get("SUBSCRIPTION_SLOTS"),
            self.config.get("SUBSCRIPTION_DURATION"),
            self.config.get("EXPIRY_DELTA"),
        )
        self.db_manager = AppointmentsDBM(self.config.get("APPOINTMENTS_DB_PATH"))
        responder = Responder(self.db_manager, gatekeeper, carrier, self.block_processor)
        self.watcher = Watcher(
            self.db_manager,
            gatekeeper,
            self.block_processor,
            responder,
            sk,
            self.config.get("MAX_APPOINTMENTS"),
            self.config.get("LOCATOR_CACHE_SIZE"),
        )

        self.watcher_thread = None
        self.responder_thread = None

        # Create the chain monitor
        self.chain_monitor = ChainMonitor(
            [self.watcher.block_queue, self.watcher.responder.block_queue], self.block_processor, bitcoind_feed_params
        )

        # Set up the internal API
        self.internal_api = InternalAPI(self.watcher, INTERNAL_API_ENDPOINT, self.stop_command_event)

        # Create the rpc, without starting it
        self.rpc_process = multiprocessing.Process(
            target=rpc.serve,
            args=(
                self.config.get("RPC_BIND"),
                self.config.get("RPC_PORT"),
                INTERNAL_API_ENDPOINT,
                self.stop_event,
                self.config.get("LOG_FILE"),
            ),
            daemon=True,
        )

        # This variables will contain the handle of the process running the API, when the service is started.
        # It will be an instance of either Popen or Process, depending on the WSGI config setting.
        self.api_proc = None

    def bootstrap_components(self):
        """
        Performs the initial setup of the components. It loads the appointments and tracker for the watcher and the
        responder (if any), and awakes the components. It also populates the block queues with any missing data, in
        case the tower has been offline for some time. Finally, it starts the chain monitor.
        """

        # Make sure that the ChainMonitor starts listening to new blocks while we bootstrap
        self.chain_monitor.monitor_chain()

        watcher_appointments_data = self.db_manager.load_watcher_appointments()
        responder_trackers_data = self.db_manager.load_responder_trackers()

        if len(watcher_appointments_data) == 0 and len(responder_trackers_data) == 0:
            self.logger.info("Fresh bootstrap")

            self.watcher_thread = self.watcher.awake()
            self.responder_thread = self.watcher.responder.awake()

        else:
            self.logger.info("Bootstrapping from backed up data")

            # Update the Watcher backed up data if found.
            if len(watcher_appointments_data) != 0:
                self.watcher.appointments, self.watcher.locator_uuid_map = Builder.build_appointments(
                    watcher_appointments_data
                )

            # Update the Responder with backed up data if found.
            if len(responder_trackers_data) != 0:
                self.watcher.responder.trackers, self.watcher.responder.tx_tracker_map = Builder.build_trackers(
                    responder_trackers_data
                )

            # Awaking components so the states can be updated.
            self.watcher_thread = self.watcher.awake()
            self.responder_thread = self.watcher.responder.awake()

            last_block_watcher = self.db_manager.load_last_block_hash_watcher()
            last_block_responder = self.db_manager.load_last_block_hash_responder()

            # Populate the block queues with data if they've missed some while offline. If the blocks of both match
            # we don't perform the search twice.

            # FIXME: 32-reorgs-offline dropped txs are not used at this point.
            last_common_ancestor_watcher, dropped_txs_watcher = self.block_processor.find_last_common_ancestor(
                last_block_watcher
            )
            missed_blocks_watcher = self.block_processor.get_missed_blocks(last_common_ancestor_watcher)

            if last_block_watcher == last_block_responder:
                dropped_txs_responder = dropped_txs_watcher
                missed_blocks_responder = missed_blocks_watcher

            else:
                last_common_ancestor_responder, dropped_txs_responder = self.block_processor.find_last_common_ancestor(
                    last_block_responder
                )
                missed_blocks_responder = self.block_processor.get_missed_blocks(last_common_ancestor_responder)

            # If only one of the instances needs to be updated, it can be done separately.
            if len(missed_blocks_watcher) == 0 and len(missed_blocks_responder) != 0:
                Builder.populate_block_queue(self.watcher.responder.block_queue, missed_blocks_responder)
                self.watcher.responder.block_queue.join()

            elif len(missed_blocks_responder) == 0 and len(missed_blocks_watcher) != 0:
                Builder.populate_block_queue(self.watcher.block_queue, missed_blocks_watcher)
                self.watcher.block_queue.join()

            # Otherwise they need to be updated at the same time, block by block
            elif len(missed_blocks_responder) != 0 and len(missed_blocks_watcher) != 0:
                Builder.update_states(self.watcher, missed_blocks_watcher, missed_blocks_responder)

        # Activate ChainMonitor
        self.chain_monitor.activate()

    def start_services(self):
        """Readies the tower by setting up signal handling, and starting all the services."""
        signal(SIGINT, self.handle_signals)
        signal(SIGTERM, self.handle_signals)
        signal(SIGQUIT, self.handle_signals)

        # Start the rpc process
        self.rpc_process.start()

        # Start the internal API
        # This MUST be done after rpc_process.start to avoid the issue that was solved in
        # https://github.com/talaia-labs/python-teos/pull/198
        self.internal_api.rpc_server.start()
        self.logger.info(f"Internal API initialized. Serving at {INTERNAL_API_ENDPOINT}")

        # Start the public API server
        api_endpoint = f"{self.config.get('API_BIND')}:{self.config.get('API_PORT')}"
        if self.config.get("WSGI") == "gunicorn":
            # FIXME: We may like to add workers depending on a config value
            self.api_proc = subprocess.Popen(
                [
                    "gunicorn",
                    f"--bind={api_endpoint}",
                    f"teos.api:serve(internal_api_endpoint='{INTERNAL_API_ENDPOINT}', "
                    f"endpoint='{api_endpoint}', min_to_self_delay='{self.config.get('MIN_TO_SELF_DELAY')}')",
                ]
            )
        else:
            self.api_proc = multiprocessing.Process(
                target=api.serve,
                kwargs={
                    "internal_api_endpoint": INTERNAL_API_ENDPOINT,
                    "endpoint": api_endpoint,
                    "min_to_self_delay": self.config.get("MIN_TO_SELF_DELAY"),
                    "auto_run": True,
                },
            )
            self.api_proc.start()

    def handle_signals(self, signum, frame):
        """Handles signals by initiating a graceful shutdown."""
        self.logger.debug(f"Signal {signum} received. Stopping")

        self.stop_command_event.set()

    def teardown(self):
        """Shuts down all services and closes the DB, then exits. This method does not return."""
        self.logger.info("Terminating public API")

        # Stop the public API first
        if isinstance(self.api_proc, subprocess.Popen):
            self.api_proc.terminate()
            self.api_proc.wait()
        elif isinstance(self.api_proc, multiprocessing.Process):
            # FIXME: when the public API process is ran with flask, there is no SIGTERM handler attempting
            # a graceful shutdown (rejecting new requests, trying to complete ongoing ones); therefore, we send
            # a SIGKILL instead.
            self.api_proc.kill()
            self.api_proc.join()

        self.logger.info("Terminated public API")

        # Signals readiness to shutdown to the other processes
        self.stop_event.set()

        # wait for RPC process to shutdown
        self.rpc_process.join()

        # Stops the internal API, after waiting for some grace time
        self.logger.info("Internal API stopping")
        self.internal_api.rpc_server.stop(SHUTDOWN_GRACE_TIME).wait()
        self.logger.info("Internal API stopped")

        # terminate the ChainMonitor
        self.chain_monitor.terminate()

        # wait for watcher and responder to finish processing their queues
        self.watcher_thread.join()
        self.responder_thread.join()

        self.logger.info("Closing connection with appointments db")
        self.db_manager.db.close()

        self.logger.info("Shutting down TEOS")
        exit(0)

    def start(self):
        """This method implements the whole lifetime cycle of the the TEOS tower. This method does not return."""
        self.logger.info("Starting TEOS")
        self.bootstrap_components()
        self.start_services()

        if self.ready_event:
            self.ready_event.set()

        self.stop_command_event.wait()

        self.teardown()


def main(config, ready_event=None):
    """
    Main startup script of TEOS. It sets up the data folder and logging, creates the tower keys if necessary, then
    creates and starts the TeosDaemon.

    Args:
        config (:obj:`dict`): a dictionary containing all the system's configuration parameters
        ready_event (:obj:`multiprocessing.Event` or :obj:`None`): if given, an Event that will be set as soon as TEOS
            is fully initialized.
    """

    setup_data_folder(config.get("DATA_DIR"))

    silent = config.get("DAEMON")
    logging_server_ready = multiprocessing.Event()
    logging_process = multiprocessing.Process(
        target=serve_logging, daemon=True, args=(config.get("LOG_FILE"), silent, logging_server_ready)
    )
    logging_process.start()

    logging_server_ready.wait()

    setup_logging()
    logger = get_logger(component="Daemon")

    if not os.path.exists(config.get("TEOS_SECRET_KEY")) or config.get("OVERWRITE_KEY"):
        logger.info("Generating a new key pair")
        sk = Cryptographer.generate_key()
        Cryptographer.save_key_file(sk.to_der(), "teos_sk", config.get("DATA_DIR"))

    else:
        logger.info("Tower identity found. Loading keys")
        secret_key_der = Cryptographer.load_key_file(config.get("TEOS_SECRET_KEY"))

        if not secret_key_der:
            raise IOError("TEOS private key cannot be loaded")
        sk = Cryptographer.load_private_key_der(secret_key_der)

    try:
        TeosDaemon(config, sk, logger, ready_event=ready_event).start()
    except Exception as e:
        logger.error("An error occurred: {}. Shutting down".format(e))
        exit(1)


if __name__ == "__main__":
    # Subprocess need to be run using "spawn" for consistent execution between different OS. No state is really shared
    # between process.
    multiprocessing.set_start_method("spawn")

    command_line_conf = {}
    data_dir = DATA_DIR

    opts, _ = getopt(
        argv[1:],
        "hd",
        [
            "apibind=",
            "apiport=",
            "rpcbind=",
            "rpcport=",
            "btcnetwork=",
            "btcrpcuser=",
            "btcrpcpassword=",
            "btcrpcconnect=",
            "btcrpcport=",
            "btcfeedconnect=",
            "btcfeedport=",
            "datadir=",
            "wsgi=",
            "daemon",
            "overwritekey",
            "help",
        ],
    )
    try:
        for opt, arg in opts:
            if opt in ["--apibind"]:
                command_line_conf["API_BIND"] = arg
            if opt in ["--apiport"]:
                try:
                    command_line_conf["API_PORT"] = int(arg)
                except ValueError:
                    exit("apiport must be an integer")
            if opt in ["--rpcbind"]:
                command_line_conf["RPC_BIND"] = arg
            if opt in ["--rpcport"]:
                try:
                    command_line_conf["RPC_PORT"] = int(arg)
                except ValueError:
                    exit("rpcport must be an integer")
            if opt in ["--btcnetwork"]:
                command_line_conf["BTC_NETWORK"] = arg
            if opt in ["--btcrpcuser"]:
                command_line_conf["BTC_RPC_USER"] = arg
            if opt in ["--btcrpcpassword"]:
                command_line_conf["BTC_RPC_PASSWORD"] = arg
            if opt in ["--btcrpcconnect"]:
                command_line_conf["BTC_RPC_CONNECT"] = arg
            if opt in ["--btcrpcport"]:
                try:
                    command_line_conf["BTC_RPC_PORT"] = int(arg)
                except ValueError:
                    exit("btcrpcport must be an integer")
            if opt in ["--btcfeedconnect"]:
                command_line_conf["BTC_FEED_CONNECT"] = arg
            if opt in ["--btcfeedport"]:
                try:
                    command_line_conf["BTC_FEED_PORT"] = int(arg)
                except ValueError:
                    exit("btcfeedport must be an integer")
            if opt in ["--datadir"]:
                data_dir = os.path.expanduser(arg)
            if opt in ["--wsgi"]:
                if arg in ["gunicorn", "flask"]:
                    command_line_conf["WSGI"] = arg
                else:
                    exit("wsgi must be either gunicorn or flask")
            if opt in ["-d", "--daemon"]:
                command_line_conf["DAEMON"] = True
            if opt in ["--overwritekey"]:
                command_line_conf["OVERWRITE_KEY"] = True
            if opt in ["-h", "--help"]:
                exit(show_usage())

    except GetoptError as e:
        exit(e)

    config = get_config(command_line_conf, data_dir)

    if config.get("DAEMON"):
        print("Starting TEOS")
        with daemon.DaemonContext():
            main(config)
    else:
        main(config)
