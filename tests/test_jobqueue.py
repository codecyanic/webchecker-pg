import pytest
import time
import signal

from queue import Queue
from jobqueue import JobQueue


def store(num, queue):
    queue.put(num)
    if num % 2:
        raise Exception()


def test_jobqueue():
    signal.signal(signal.SIGALRM, lambda: pytest.fail())
    signal.alarm(5)

    with pytest.raises(ValueError):
        JobQueue(0)

    source = list(range(1024))
    queue = Queue()

    jq = JobQueue(8)

    for n in source:
        jq.put(store, (n, queue))

    while not jq.queue.empty():
        time.sleep(.1)

    assert(set(source) == set(queue.queue))
