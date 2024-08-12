import logging
import sys
from functools import wraps

# import asyncio
from dataclasses import dataclass
from asyncio import (
    get_event_loop,
    new_event_loop,
    get_running_loop,
    set_event_loop,
    run,
    sleep,
    # get,
    create_task,
)

from typing import Optional, Callable, Union, Coroutine

# from typing import Callable
from collections import deque
# import collections

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s-%(asctime)s-%(message)s")
logger = logging.getLogger("Async Scheduler")


@dataclass
class Job:
    type: str
    when: int
    status: str
    func: Callable
    args: tuple
    kwargs: dict


def keyboard_interrupt(func):
    @wraps(func)
    async def decorator(*args, **kwargs):
        try:
            logger.debug("keyboard_interrupt")
            await func(*args, **kwargs)

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            # self.loop.stop()

    return decorator


class AsyncScheduler:
    """Infinity loop in asyncio sleep"""

    def __init__(self):
        # if sys.version_info < (3, 10):
        # self.loop = get_event_loop()
        # else:
        # try:
        # self.loop = get_running_loop()
        # except RuntimeError:
        #     self.loop = new_event_loop()

        # set_event_loop(self.loop)

        self.sleep_timer = 0.2
        self.jobs = deque()

        """
        When you add the task, it mights the current time and calculates how much time is left to launch and sends it to call_ay
        
        """

    # @keyboard_interrupt
    async def start(self) -> None:
        while self.jobs:
            logger.debug("Iter run")
            job = self.jobs.popleft()
            logger.debug(job)
            match job.type:
                case "sync":
                    # await self.run_sync_job(job.func, args=job.args, kwargs={})
                    await self.run_sync_job(job.func, args=job.args, kwargs=job.kwargs)
                case "async":
                    await self.run_async_job(job.func)
            # ...
            # check status jobs and run if ok
            await sleep(self.sleep_timer)

    def add_sync_job(self, job: Callable, args: tuple, kwargs: dict) -> None:
        logger.debug("adding sync job")
        logger.debug(job)
        # calculate time this
        sync_job = Job(
            type="sync",
            # when=self.loop.time() + 5,
            when=10 + 5,
            status="PENDING",
            func=job,
            args=args,
            kwargs=kwargs,
        )
        logger.debug(sync_job)
        import time

        logger.debug(time.time())
        self.jobs.append(sync_job)

    async def run_sync_job(self, job: Callable, args: tuple, kwargs: dict) -> None:
        logger.debug("starting sync job")
        from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

        # pool = ThreadPoolExecutor
        pool = ProcessPoolExecutor
        # pool = None
        loop = get_event_loop()
        t = loop.run_in_executor(pool(), job, *args, **kwargs)
        # task = create_task(t)
        # await task
        # r = await self.event_loop.run_in_executor(pool, job)
        # return r

    async def add_async_job(self, job: Coroutine) -> None:
        # self.jobs_.append(job)
        # job_sleep_timer = min(
        #     self.sleep_timer, self.jobs_[0].__await__() - self.event_loop.time()
        # )  # We calculate the time until the next task and set the minimum value of Sleep Timer
        pass

    async def run_async_job(self, job: Coroutine) -> None:
        # self.event_loop.call_at()
        # self.event_loop.time()

        pass


# event_loop.call_at()


# if __name__ == '__main__':


def test(sleep_timer):
    import time

    time.sleep(sleep_timer)
    print(f"hello world! {sleep_timer}")


sc = AsyncScheduler()
for i in range(1, 10):
    # sc.add_sync_job(test, args=(i,))
    # sc.add_sync_job(test, args=(i,), kwargs={"sleep_timer": i})
    sc.add_sync_job(test, args=(3,), kwargs={})
run(sc.start())
