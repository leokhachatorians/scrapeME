import asyncio
import aiohttp
import socket
import time

class Crawl:
    def __init__(self, loop, num_workers):
        self.q = asyncio.Queue()
        self.loop = loop
        self.client = aiohttp.ClientSession(loop=self.loop)
        self.num_workers = num_workers
        self.start_time = None
        self.end_time = None

        self.q.put_nowait('http://leokhachatorians.com')
        self.q.put_nowait('http://google.com')
        self.q.put_nowait('http://amazon.com')
        self.q.put_nowait('http://yahoo.com')
        self.q.put_nowait('http://facebook.com')

    async def fetch(self, url):
        resp = await self.client.get(url)
        assert resp.status == 200
        return await resp.text()
        await resp.release()

    async def work(self):
        try:
            while True:
                url = await self.q.get()
                html = await self.fetch(url)
                print(url)
                self.q.task_done()
        except asyncio.CancelledError:
            pass

    async def crawl(self):
        workers = [asyncio.Task(self.work(), loop=self.loop) for i in range(self.num_workers)]
        self.start_time = time.time()
        await self.q.join()
        self.end_time = time.time()
        self.dt = self.end_time - self.start_time
        for w in workers:
            w.cancel()
        self.client.close()

loop = asyncio.get_event_loop()
c = Crawl(loop, num_workers=5)
loop.run_until_complete(c.crawl())
print("It took %.3f seconds to get all the pages" % c.dt)
