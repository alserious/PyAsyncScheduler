import cProfile
import logging
import sys
import time
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import asyncio
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

logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)s-%(asctime)s-%(process)s-%(thread)s-%(message)s",
)
logger = logging.getLogger("Async Scheduler")


@dataclass
class Job:
    type: str
    when: int
    status: str
    func: Callable | Coroutine
    args: tuple
    kwargs: dict
    pool: str | None = None


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


def async_monitor(func):
    @wraps(func)
    async def decorator(*args, **kwargs):
        t1 = time.time()
        logger.debug("async_monitor")
        result = await func(*args, **kwargs)

        seconds = time.time() - t1
        logger.debug("spending %s seconds", seconds)
        return result

    return decorator


def sync_monitor(func):
    @wraps(func)
    def decorator(*args, **kwargs):
        t1 = time.time()
        # logger.debug("monitor")
        result = func(*args, **kwargs)

        seconds = time.time() - t1
        logger.debug("spending %s seconds", round(seconds, 5))
        return result

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

        # self.sleep_timer = 0
        self.sleep_timer = 0.2
        self.jobs = deque()

        """
        When you add the task, it mights the current time and calculates how much time is left to launch and sends it to call_ay
        
        """

    @staticmethod
    def define_pool(pool: str) -> ThreadPoolExecutor | ProcessPoolExecutor:
        match pool:
            case "thread":
                return ThreadPoolExecutor()
            case "process":
                return ProcessPoolExecutor()
            case _:
                raise ValueError("Invalid pool type")

    # @keyboard_interrupt
    async def start(self) -> None:
        while self.jobs:
            logger.debug("Iter run")
            job = self.jobs.popleft()
            logger.debug(job)
            match job.type:
                case "sync":
                    # await self.run_sync_job(job.func, args=job.args, kwargs={})
                    # pool = ThreadPoolExecutor
                    # pool = ProcessPoolExecutor
                    # pool = None
                    await self.run_sync_job(
                        job.func,
                        args=job.args,
                        kwargs=job.kwargs,
                        pool=AsyncScheduler.define_pool(job.pool),
                    )
                case "async":
                    match job.status:
                        case "PENDING":
                            task = await self.run_async_job(
                                job.func,
                                args=job.args,
                                kwargs=job.kwargs,
                            )
                            job.status = "PROCESSING"
                            job.func = task
                            self.jobs.append(job)
                        case "PROCESSING":
                            await self.processing_async_job(job.func)

            # ...
            # check status jobs and run if ok
            await sleep(self.sleep_timer)

    def add_sync_job(self, job: Callable, args: tuple, kwargs: dict, pool: str) -> None:
        logger.debug("adding sync job: %s", job)
        # calculate time this
        sync_job = Job(
            type="sync",
            # when=self.loop.time() + 5,
            when=10 + 5,
            status="PENDING",
            func=job,
            args=args,
            kwargs=kwargs,
            pool=pool,
        )
        self.jobs.append(sync_job)

    def add_async_job(self, job: Coroutine, args: tuple, kwargs: dict) -> None:
        logger.debug("adding async job: %s", job)
        # calculate time this
        async_job = Job(
            type="async",
            # when=self.loop.time() + 5,
            when=10 + 5,
            status="PENDING",
            func=job,
            args=args,
            kwargs=kwargs,
        )
        self.jobs.append(async_job)

    async def run_sync_job(
        self,
        job: Callable,
        args: tuple,
        kwargs: dict,
        pool: ThreadPoolExecutor | ProcessPoolExecutor,
    ) -> None:
        logger.debug("starting sync job")
        loop = get_event_loop()
        t = loop.run_in_executor(pool, job, *args, **kwargs)
        logger.debug("finished sync job")
        # task = create_task(t)
        # await task
        # r = await self.event_loop.run_in_executor(pool, job)
        # return r

    async def run_async_job(
        self,
        job: Coroutine,
        args: tuple,
        kwargs: dict,
    ) -> None:
        logger.debug("starting async job")

        # loop = get_event_loop()
        # loop.create_task()

        import asyncio

        asyncio.gather

        task = asyncio.create_task(job(*args, **kwargs))

        # await task

        # asyncio.t

        # await job(*args, **kwargs)
        logger.debug("finished async job")

        return task

        # loop.create_task(job, args, kwargs)
        # self.event_loop.call_at()
        # self.event_loop.time()

    async def processing_async_job(self, job: asyncio.Task) -> None:
        logger.debug("start processing async job")

        # loop = get_event_loop()
        # loop.create_task()

        # import asyncio

        logger.debug(job)
        logger.debug(type(job))

        if job.done:
            logger.debug("async job finished")
            return
        else:
            logger.debug("async job not finished")
            

        # asyncio.create_task(job(*args, **kwargs))

        # asyncio.t

        # await job(*args, **kwargs)
        logger.debug("finished processing async job")
        # loop.create_task(job, args, kwargs)
        # self.event_loop.call_at()
        # self.event_loop.time()


@sync_monitor
def test(sleep_timer):
    import time

    time.sleep(sleep_timer)
    print(f"sync hello world! {sleep_timer}")


# @async_monitor
async def async_test(sleep_timer):
    sleep_timer = 3
    # import asyncio

    print(f"async_test start! {sleep_timer}")
    await asyncio.sleep(sleep_timer)
    print(f"async_test finished! {sleep_timer}")


sc = AsyncScheduler()
for i in range(1, 5):
    # sc.add_sync_job(test, args=(i,))
    # sc.add_sync_job(test, args=(i,), kwargs={"sleep_timer": i})
    # sc.add_sync_job(test, args=(5,), kwargs={}, pool="thread")
    sc.add_async_job(async_test, args=(5,), kwargs={})

    # with cProfile.Profile() as pr:
    # run(sc.start())

    # pr.print_stats()
# @async_monitor
run(sc.start())
