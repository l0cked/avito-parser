from db import Db
from log import Log
from parse import Parse
from proxy import Proxy
import asyncio
import time


async def main():
    async with Db('avito_parser.db') as db, Log(db) as log, Proxy(db, log) as proxy:
        await proxy.update()

        start = time.time()
        parse = Parse(db, log, proxy)
        new, items, requests = await parse.get()

        await log.msg(f'Saved {new} new / {len(items)} items / {requests} requests in {time.time()-start:.2f} sec')

asyncio.run(main())
