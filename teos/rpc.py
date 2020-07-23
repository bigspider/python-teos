from flask import Flask
from flask_jsonrpc import JSONRPC

from common.logger import get_logger
from common.cryptographer import Cryptographer


class RPC:
    """
    The :class:`RPC` exposes admin functionality of the watchtower.

    Args:
        host (:obj:`str`): the hostname to listen on.
        port (:obj:`int`): the port of the webserver.
        rw_lock (:obj:`RWLockWrite <readwritelock.rwlock.RWLockWrite>`): lock that must be acquired before reading or
            writing to the watchtower's state.
        inspector (:obj:`Inspector <teos.inspector.Inspector>`): an ``Inspector`` instance to check the correctness of
            the received appointment data.
        watcher (:obj:`Watcher <teos.watcher.Watcher>`): a ``Watcher`` instance to pass the requests to.
        responder (:obj:`Watcher <teos.responder.Responder>`): a ``Responder`` instance to pass the requests to.

    Attributes:
        logger: the logger for this component.
    """

    def __init__(self, host, port, rw_lock, inspector, watcher, responder):
        app = Flask(__name__)
        jsonrpc = JSONRPC(app, "/rpc", enable_web_browsable_api=True)
        self.app = app
        self.jsonrpc = jsonrpc

        self.logger = get_logger(component=RPC.__name__)

        self.host = host
        self.port = port
        self.rw_lock = rw_lock
        self.inspector = inspector
        self.watcher = watcher
        self.responder = responder
        self.logger.info("Initialized")

        @jsonrpc.method("get_all_appointments")
        def get_all_appointments() -> dict:
            return self.get_all_appointments()

        @jsonrpc.method("get_tower_info")
        def get_tower_info() -> dict:
            return self.get_tower_info()

    def start(self):
        """ This function starts the Flask server used to run the RPC """

        # ToDo: #185-serve-teosd-production
        self.app.run(host=self.host, port=self.port)

    def get_all_appointments(self):
        """
        Gives information about all the appointments in the Watchtower.

        Returns:
            :obj:`str`: A dictionary containing all the appointments hold by the ``Watcher``
            (``watcher_appointments``) and by the ``Responder`` (``responder_trackers``).
        """

        # ToDo: #15-add-system-monitor

        with self.rw_lock.gen_rlock():
            watcher_appointments = self.watcher.db_manager.load_watcher_appointments()
            responder_trackers = self.watcher.db_manager.load_responder_trackers()

        return {"watcher_appointments": watcher_appointments, "responder_trackers": responder_trackers}

    def get_tower_info(self):
        """
        Gives generic information about the watchtower.

        Returns:
            :obj:`str`: A dictionary containing information about the watchtower (TODO).
        """

        with self.rw_lock.gen_rlock():
            n_watcher_appointments = len(self.watcher.appointments)
            n_responder_trackers = len(self.responder.trackers)

            registered_users = len(self.watcher.gatekeeper.registered_users)
            tower_id = Cryptographer.get_compressed_pk(self.watcher.signing_key.public_key)

        return {
            "tower_id": tower_id,
            "registered_users": registered_users,
            "n_watcher_appointments": n_watcher_appointments,
            "n_responder_trackers": n_responder_trackers,
        }
