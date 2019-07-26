from datetime import datetime
import aiohttp
import asyncio
import random
import time


class Proxy:
    options = {
        'check_url': 'https://m.avito.ru',
        'check_timeout': 2,
        'check_threads_enable': True,
        'check_threads': 999,
        'check_sleep': .1
    }

    proxylist = []
    __proxylist = []
    checking = 0
    lastupdate = None
    hours = 0

    def __init__(self, db, log):
        self.db = db.conn
        self.log = log.msg

    async def __aenter__(self):
        if await self.load():
            await self.log(f'Load {len(self.proxylist)} proxy (last update: {self.lastupdate}, hours: {self.hours:.2f})')
        else:
            await self.update()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.db.executemany('UPDATE proxy SET used=?, error=? WHERE id=?',
            [(proxy['used'], proxy['error'], proxy['id']) for proxy in self.proxylist])
        await self.db.commit()

    async def update(self):
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            html = None
            try:
                async with session.get('https://raw.githubusercontent.com/fate0/proxylist/master/proxy.list', timeout=5) as response:
                    if response.status == 200:
                        html = await response.text()
            except:
                await self.log('ERROR load HTML proxylist')

            if html:
                self.proxylist = []
                self.__proxylist = []
                await self.log(f'Proxylist HTML load ({len(html)} bytes)')

                if await self.parse(html):
                    await self.log(f'{len(self.__proxylist)} http proxy found')
                    await self.log(f'Checking proxylist url: {self.options["check_url"]}, timeout: {self.options["check_timeout"]}, threads: {self.options["check_threads"]}')

                    await asyncio.gather(*[self.check(session, proxy) for proxy in self.__proxylist])

                    await self.clear()
                    await self.save()

                    return

        await self.log('ERROR load proxylist')

    async def parse(self, html):
        data = html.split('\n')
        null = None
        for d in data:
            if d != '':
                try:
                    proxy = eval(d)
                except:
                    return False
                if proxy['type'] == 'http':
                    del proxy['type']
                    del proxy['anonymity']
                    del proxy['export_address']
                    del proxy['from']
                    del proxy['response_time']
                    proxy['port'] = str(proxy['port'])
                    proxy['url'] = 'http://{host}:{port}'.format(**proxy)
                    proxy['index'] = len(self.__proxylist)

                    if proxy['country']:
                        self.__proxylist.append(proxy)
        return True

    async def check(self, session, proxy):
        if self.options['check_threads_enable']:
            while self.checking > self.options['check_threads']:
                await asyncio.sleep(self.options['check_sleep'])

        start = time.time()
        self.checking += 1

        try:
            async with session.get(self.options['check_url'], proxy=proxy['url'], timeout=self.options['check_timeout']) as response:
                data = await response.text()
                if response.status == 200:
                    proxy['response_time'] = time.time()-start
                    proxy['id'] = len(self.proxylist)

                    await self.log('[{id}/{index}] {country} {url}  {response_time:.2f} sec'.format(**proxy))

                    self.proxylist.append(proxy)
        except:
            pass

        self.checking -= 1

    async def clear(self):
        try:
            await self.db.execute(f'DELETE FROM proxy')
            await self.db.execute(f'DELETE FROM sqlite_sequence WHERE name="proxy"')
            await self.db.commit()
        except:
            await self.log('Error clear proxylist')

    async def save(self):
        try:
            await self.db.executemany('INSERT OR REPLACE INTO proxy (country,host,port,url,response_time) VALUES (?,?,?,?,?)',
                [(proxy['country'], proxy['host'], proxy['port'], proxy['url'], proxy['response_time']) for proxy in self.proxylist])
            await self.db.commit()

            await self.log(f'{len(self.proxylist)} proxys saved')

            await self.load()
        except:
            await self.log('Error save proxylist')

    async def load(self):
        cursor = await self.db.execute('SELECT * FROM proxy LIMIT 1')
        proxy = await cursor.fetchone()
        if not proxy: return False
        self.lastupdate = datetime.strptime(proxy[1], '%Y-%m-%d %H:%M:%S')
        self.hours = abs(self.lastupdate - datetime.now()).total_seconds() / 3600.0

        if self.hours > 6:
            return False

        try:
            cursor = await self.db.execute('SELECT * FROM proxy')
            proxylist = await cursor.fetchall()
            if proxylist:
                self.proxylist = [{'id': p[0], 'added': p[1], 'country': p[2], 'host': p[3], 'port': p[4], 'url': p[5], 'response_time': p[6], 'used': p[7], 'error': p[8]} for p in proxylist]
                return True
        except:
            pass

        return False

    async def get(self):
        cycle = 0
        while True:
            proxy = self.proxylist[random.randint(0, len(self.proxylist)-1)]
            if proxy['error'] < 5 or cycle == 5:
                return proxy
            cycle += 1
