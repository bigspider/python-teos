import zmq
import time
from queue import Queue
from threading import Thread, Event, Condition

from teos.chain_monitor import ChainMonitor, ChainMonitorStatus

from test.teos.conftest import generate_blocks
from test.teos.unit.conftest import get_random_value_hex, bitcoind_feed_params


def test_init(block_processor):
    # run_bitcoind is started here instead of later on to avoid race conditions while it initializes

    # Not much to test here, just sanity checks to make sure nothing goes south in the future
    chain_monitor = ChainMonitor([Queue(), Queue()], block_processor, bitcoind_feed_params)

    assert chain_monitor.best_tip is None
    assert isinstance(chain_monitor.last_tips, list) and len(chain_monitor.last_tips) == 0
    assert chain_monitor.status == ChainMonitorStatus.IDLE
    assert isinstance(chain_monitor.check_tip, Event)
    assert isinstance(chain_monitor.lock, Condition)
    assert isinstance(chain_monitor.zmqSubSocket, zmq.Socket)

    # The Queues and asleep flags are initialized when attaching the corresponding subscriber
    assert isinstance(chain_monitor.receiving_queues[0], Queue)
    assert isinstance(chain_monitor.receiving_queues[1], Queue)


def test_notify_subscribers(block_processor):
    chain_monitor = ChainMonitor([Queue(), Queue()], block_processor, bitcoind_feed_params)
    # Subscribers are only notified as long as they are awake
    new_block = get_random_value_hex(32)

    # Queues should be empty to start with
    assert chain_monitor.receiving_queues[0].empty()
    assert chain_monitor.receiving_queues[1].empty()

    chain_monitor.notify_subscribers(new_block)

    assert chain_monitor.receiving_queues[0].get() == new_block
    assert chain_monitor.receiving_queues[1].get() == new_block


def test_enqueue(block_processor):
    # The state is updated after receiving a new block (and only if the block is not already known).
    # Let's start by setting a best_tip and a couple of old tips
    new_block_hash = get_random_value_hex(32)
    chain_monitor = ChainMonitor([Queue(), Queue()], block_processor, bitcoind_feed_params)
    chain_monitor.best_tip = new_block_hash
    chain_monitor.last_tips = [get_random_value_hex(32) for _ in range(5)]

    # Now we can try to update the state with an old best_tip and see how it doesn't work
    assert chain_monitor.enqueue(chain_monitor.last_tips[0]) is False

    # Same should happen with the current tip
    assert chain_monitor.enqueue(chain_monitor.best_tip) is False

    # The state should be correctly updated with a new block hash, the chain tip should change and the old tip should
    # have been added to the last_tips
    another_block_hash = get_random_value_hex(32)
    assert chain_monitor.enqueue(another_block_hash) is True
    assert chain_monitor.best_tip == another_block_hash and new_block_hash == chain_monitor.last_tips[-1]


def test_monitor_chain_polling(block_processor):
    chain_monitor = ChainMonitor([Queue(), Queue()], block_processor, bitcoind_feed_params)
    chain_monitor.best_tip = block_processor.get_best_block_hash()
    chain_monitor.polling_delta = 0.1

    # monitor_chain_polling runs until not terminated
    polling_thread = Thread(target=chain_monitor.monitor_chain_polling, daemon=True)
    polling_thread.start()

    # Check that nothing changes as long as a block is not generated
    for _ in range(5):
        assert chain_monitor.queue.empty()
        time.sleep(0.1)

    # And that it does if we generate a block
    generate_blocks(1)

    chain_monitor.queue.get()
    assert chain_monitor.queue.empty()

    chain_monitor.terminate()


def test_monitor_chain_zmq(block_processor):
    responder_queue = Queue()
    chain_monitor = ChainMonitor([Queue(), responder_queue], block_processor, bitcoind_feed_params)
    chain_monitor.best_tip = block_processor.get_best_block_hash()

    zmq_thread = Thread(target=chain_monitor.monitor_chain_zmq, daemon=True)
    zmq_thread.start()

    # the internal queue should start empty
    assert chain_monitor.queue.empty()

    # And have a new block every time we generate one
    for _ in range(3):
        generate_blocks(1)

        chain_monitor.queue.get()
        assert chain_monitor.queue.empty()

    chain_monitor.terminate()
    # The zmq thread needs a block generation to release from the recv method.
    generate_blocks(1)


def test_monitor_chain(block_processor):
    # We don't activate it but we start listening; therefore received blocks should accumulate in the internal queue
    chain_monitor = ChainMonitor([Queue(), Queue()], block_processor, bitcoind_feed_params)
    chain_monitor.polling_delta = 0.1

    chain_monitor.monitor_chain()

    # The tip is updated before starting the threads, so it should have changed.
    assert chain_monitor.best_tip is not None

    # Blocks should be received
    count = 0
    for _ in range(5):
        generate_blocks(1)
        count += 1
        time.sleep(0.5)  # higher than the polling interval
        print(f"Best block: {block_processor.get_best_block_hash()}")
        assert chain_monitor.receiving_queues[0].empty()
        assert chain_monitor.receiving_queues[1].empty()
        assert chain_monitor.queue.qsize() == count

    chain_monitor.terminate()
    # The zmq thread needs a block generation to release from the recv method.
    generate_blocks(1)


def test_activate(block_processor):
    # Not much to test here, this should launch two threads (one per monitor approach) and finish on terminate
    chain_monitor = ChainMonitor([Queue(), Queue()], block_processor, bitcoind_feed_params)
    chain_monitor.monitor_chain()
    chain_monitor.activate()

    # The tip is updated before starting the threads, so it should have changed.
    assert chain_monitor.best_tip is not None

    # Blocks should be received
    for _ in range(5):
        generate_blocks(1)
        watcher_block = chain_monitor.receiving_queues[0].get()
        responder_block = chain_monitor.receiving_queues[1].get()
        assert watcher_block == responder_block
        assert chain_monitor.receiving_queues[0].empty()
        assert chain_monitor.receiving_queues[1].empty()

    chain_monitor.terminate()
    # The zmq thread needs a block generation to release from the recv method.
    generate_blocks(1)


def test_monitor_chain_single_update(block_processor):
    # This test tests that if both threads try to add the same block to the queue, only the first one will make it
    chain_monitor = ChainMonitor([Queue(), Queue()], block_processor, bitcoind_feed_params)

    chain_monitor.best_tip = None
    chain_monitor.polling_delta = 2

    # We will create a block and wait for the polling thread. Then check the queues to see that the block hash has only
    # been added once.
    chain_monitor.monitor_chain()
    chain_monitor.activate()
    generate_blocks(1)

    assert len(chain_monitor.receiving_queues) == 2

    queue0_block = chain_monitor.receiving_queues[0].get()
    queue1_block = chain_monitor.receiving_queues[1].get()
    assert queue0_block == queue1_block
    assert chain_monitor.receiving_queues[0].empty()
    assert chain_monitor.receiving_queues[1].empty()

    # The delta for polling is 2 secs, so let's wait and see
    time.sleep(2)
    assert chain_monitor.receiving_queues[0].empty()
    assert chain_monitor.receiving_queues[1].empty()

    # We can also force an update and see that it won't go through
    assert chain_monitor.enqueue(queue0_block) is False

    chain_monitor.terminate()
    # The zmq thread needs a block generation to release from the recv method.
    generate_blocks(1)
