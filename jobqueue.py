import logging
from threading import Thread
from queue import Queue

logger = logging.getLogger(__name__)


class Work:
    def __init__(self, target, args):
        self.target = target
        self.args = args


class JobQueue:
    def __init__(self, num_threads):
        if num_threads <= 0:
            raise ValueError('num_threads must be positive')

        self.queue = Queue(num_threads * 2)
        for _ in range(num_threads):
            Thread(target=self.worker, args=(self.queue,), daemon=True).start()

    @staticmethod
    def worker(queue):
        while True:
            work = queue.get()
            try:
                work.target(*work.args)
            except Exception as e:
                logger.exception(e)
            queue.task_done()

    def put(self, target, args):
        self.queue.put(Work(target, args))
