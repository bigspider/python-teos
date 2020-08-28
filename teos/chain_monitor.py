from enum import Enum
from queue import Queue, Empty
import zmq
import binascii
from threading import Thread, Event, Condition

from common.logger import get_logger


class ChainMonitorStatus(Enum):
    IDLE = 0
    LISTENING = 1
    ACTIVE = 2
    TERMINATED = 3


class ChainMonitor:
    """
    The :class:`ChainMonitor` is in charge of monitoring the blockchain (via ``bitcoind``) to detect new blocks on top
    of the best chain. If a new best block is spotted, the chain monitor will notify the given ``Queues``.

    The :class:`ChainMonitor` monitors the chain using two methods: ``zmq`` and ``polling``. Blocks are only notified
    once per queue and the notification is triggered by the method that detects the block faster.

    The :class:`ChainMonitor` lifecycle goes through 4 states: idle, listening, active and terminated.
    When a :class:`ChainMonitor` instance is created, it is not yet monitoring the chain and the ``status`` attribute
    is set to `ChainMonitorStatus.IDLE`.
    Once the ``monitor_chain`` method is called, the chain monitor changes ``status`` to
    `ChainMonitorStatus.LISTENING`, and starts monitoring the chain for new blocks; it does not yet notify the
    receiving queues, but keeps the block hashes in the order they where spotted in an interna queue.
    Once the ``activate`` method is called, the ``status`` changes to ``ChainMonitorStatus.ACTIVE``, and the receiving
    queues are notified in order for all the block hashes that are in the internal queue or any new one that is
    detected.
    Finally, once the ``terminate`` method is called, the ``status`` is changed to ``ChainMonitorStatus.TERMINATED``,
    the chain monitor stops monitoring the chain and no receiving queue will be notified about new blocks (including
    any block that is currently in the internal queue).

    Args:
        receiving_queues (:obj:`list`): a list of ``Queue`` objects that will be notified when the chain_monitor is
            active and it received new blocks hashes.
        block_processor (:obj:`BlockProcessor <teos.block_processor.BlockProcessor>`): a ``BlockProcessor`` instance.
        bitcoind_feed_params (:obj:`dict`): a dict with the feed (ZMQ) connection parameters.

    Attributes:
        logger: the logger for this component.
        best_tip (:obj:`str`): a block hash representing the current best tip.
        last_tips (:obj:`list`): a list of last chain tips. Used as a sliding window to avoid notifying about old tips.
        terminate (:obj:`bool`): a flag to signal the termination of the :class:`ChainMonitor` (shutdown the tower).
        check_tip (:obj:`Event`): an event that is triggered at fixed time intervals and controls the polling thread.
        lock (:obj:`Condition`): a lock used to protect concurrent access to the queues and ``best_tip`` by the zmq and
            polling threads.
        zmqSubSocket (:obj:`socket`): a socket to connect to ``bitcoind`` via ``zmq``.
        polling_delta (:obj:`int`): time between polls (in seconds).
        max_block_window_size (:obj:`int`): max size of last_tips.
        queue (:obj:`Queue`): a ``Queue`` where blocks are stored before they are processed.
        status (:obj:`ChainMonitorStatus`): the current status of the monitor, either `IDLE`, `LISTENING`, `ACTIVE` or
            `TERMINATED`.
    """

    def __init__(self, receiving_queues, block_processor, bitcoind_feed_params):
        self.logger = get_logger(component=ChainMonitor.__name__)
        self.best_tip = None
        self.last_tips = []

        self.check_tip = Event()
        self.lock = Condition()

        self.zmqContext = zmq.Context()
        self.zmqSubSocket = self.zmqContext.socket(zmq.SUB)
        self.zmqSubSocket.setsockopt(zmq.RCVHWM, 0)
        self.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "hashblock")
        self.zmqSubSocket.connect(
            "%s://%s:%s"
            % (
                bitcoind_feed_params.get("BTC_FEED_PROTOCOL"),
                bitcoind_feed_params.get("BTC_FEED_CONNECT"),
                bitcoind_feed_params.get("BTC_FEED_PORT"),
            )
        )

        self.receiving_queues = receiving_queues

        self.polling_delta = 60
        self.max_block_window_size = 10
        self.block_processor = block_processor
        self.queue = Queue()
        self.status = ChainMonitorStatus.IDLE

    def notify_subscribers(self, block_hash):
        """
        Notifies the subscribers (``Watcher`` and ``Responder``) about a new block. It does so by putting the hash in
        the corresponding queue(s).

        Args:
            block_hash (:obj:`str`): the new block hash to be sent to the subscribers.
        """

        for rec_queue in self.receiving_queues:
            rec_queue.put(block_hash)

    def enqueue(self, block_hash):
        """
        Adds a new block hash to the internal queue of the  ``ChainMonitor`` and the internal state. The state contains
        the ``best_tip`` field and the list of ``last_tips`` to prevent notfying about old blocks. ``last_tips`` is
        bounded to ``max_block_window_size``.

        Args:
            block_hash (:obj:`block_hash`): the new best tip.

        Returns:
            :obj:`bool`: True if the state was successfully updated, False otherwise.
        """

        if block_hash != self.best_tip and block_hash not in self.last_tips:
            self.queue.put(block_hash)
            self.last_tips.append(self.best_tip)
            self.best_tip = block_hash

            if len(self.last_tips) > self.max_block_window_size:
                self.last_tips.pop(0)

            return True

        else:
            return False

    def monitor_chain_polling(self):
        """
        Monitors ``bitcoind`` via polling. Once the method is fired, it keeps monitoring as long as the ``status``
        attribute is not ``ChainMonitorStatus.TERMINATED``. Polling is performed once every ``polling_delta`` seconds.
        If a new best tip is found, it is added to the internal queue.
        """

        while self.status != ChainMonitorStatus.TERMINATED:
            self.check_tip.wait(timeout=self.polling_delta)

            # Terminate could have been set while the thread was blocked in wait
            if self.status != ChainMonitorStatus.TERMINATED:
                current_tip = self.block_processor.get_best_block_hash()

                # get_best_block_hash may return None if the RPC times out.
                if current_tip and current_tip not in self.last_tips:
                    self.logger.info("New block received via polling", block_hash=current_tip)
                    self.enqueue(current_tip)

    def monitor_chain_zmq(self):
        """
        Monitors ``bitcoind`` via zmq. Once the method is fired, it keeps monitoring as long as the ``status``
        attribute is not ``ChainMonitorStatus.TERMINATED``. If a new best tip is found, it is added to the internal
        queue.
        """

        while self.status != ChainMonitorStatus.TERMINATED:
            msg = self.zmqSubSocket.recv_multipart()

            # Terminate could have been set while the thread was blocked in recv
            if self.status != ChainMonitorStatus.TERMINATED:
                topic = msg[0]
                body = msg[1]

                if topic == b"hashblock":
                    block_hash = binascii.hexlify(body).decode("utf-8")
                    if block_hash not in self.last_tips:
                        self.logger.info("New block received via zmq", block_hash=block_hash)
                        self.enqueue(block_hash)

    def notify_listeners(self):
        """
        Once the method is fired, it keeps getting the elements added to the internal queue and notifies the receiving
        queues about them. It terminates whenever the internal state is set to ``ChainMonitorStatus.TERMINATED``.
        """
        while self.status != ChainMonitorStatus.TERMINATED:
            try:
                # We add a `timeout` to give the thread a chance to terminate even if the queue is empty
                block_hash = self.queue.get(block=True, timeout=0.1)
                with self.lock:
                    self.notify_subscribers(block_hash)
            except Empty:
                pass

    def monitor_chain(self):
        """
        Changes the ``status`` of the :class:`ChainMonitor` from idle to listening. It initializes the ``best_tip`` to
        the current one (by querying the :obj:`BlockProcessor <teos.block_processor.BlockProcessor>`) and creates two
        threads, one per each monitoring approach (``zmq`` and ``polling``).

        Raises:
            :obj:RuntimeError: if the ``status`` was not ``ChainMonitor.IDLE`` when the method was called.
        """

        if self.status != ChainMonitorStatus.IDLE:
            raise RuntimeError(f"This method can only be called in IDLE status. Current status is {self.status.name}.")

        self.status = ChainMonitorStatus.LISTENING

        self.best_tip = self.block_processor.get_best_block_hash()
        Thread(target=self.monitor_chain_polling, daemon=True).start()
        Thread(target=self.monitor_chain_zmq, daemon=True).start()

    def activate(self):
        """
        Changes the ``status`` of the :class:`ChainMonitor` from listening to active. It creates a new thread that runs
        the ``notify_listener`` method, which is in charge of notifying the receiving queue for each block hash that is
        added to the internal queue.

        Raises:
            :obj:RuntimeError: if the ``status`` was not ``ChainMonitor.LISTENING`` when the method was called.
        """

        if self.status != ChainMonitorStatus.LISTENING:
            raise RuntimeError(
                f"This method can only be called in LISTENING status. Current status is {self.status.name}."
            )
        self.status = ChainMonitorStatus.ACTIVE
        Thread(target=self.notify_listeners, daemon=True).start()

    def terminate(self):
        """
        Changes the ``status`` of the :class:`ChainMonitor` to terminated. All the threads will stop as soon as
        possible.
        """

        self.status = ChainMonitorStatus.TERMINATED
