from datetime import datetime, timedelta
import aiohttp
import asyncio
import lxml.html as html
import time

# Формат запроса
# m.avito.ru/{region}/{category}?{option1}&{option2}&{.....}

# Цена :
# От:              priceMin=1
# До:              priceMax=2

# Продавцы:
# Частные:         owner[]=private
# Компании:        owner[]=company

# Сортировка:
# По умолчанию:    sort=default
# По дате:         sort=date
# Сначала дешевые: sort=priceAsc
# Сначала дорогие: sort=priceDesc

# Только с фото:   withImagesOnly=true

# С доставкой:     withDeliveryOnly=true


class Parse:
    options = {
        'timeout': 2,
        'timeout_multiplier': 1.25
    }

    new_items = 0
    items = []
    requests = 0

    def __init__(self, db, log, proxy):
        self.db = db.conn
        self.log = log.msg
        self.proxy = proxy

    async def get(self):
        if not self.proxy.proxylist:
            raise RuntimeError('Not found proxylist. Parsing abort')

        urls = [
            '/odintsovo?sort=date',
            '/golitsyno?sort=date',
            '/lesnoy_gorodok?sort=date',
            '/kubinka?sort=date',
            '/bolshie_vyazemy?sort=date',
            '/gorki-10?sort=date',
            '/zvenigorod?sort=date',
            '/moskovskaya_oblast_krasnoznamensk?sort=date',
            '/novoivanovskoe?sort=date',

            '/odintsovo/transport?sort=date',
            '/odintsovo/bytovaya_elektronika?sort=date',
            '/odintsovo/hobbi_i_otdyh?sort=date',

            '/moskva_i_mo?sort=date'
        ]

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            await asyncio.gather(*[self.get_head(session, url) for url in urls])

        await self.save()

        return self.new_items, self.items, self.requests

    async def get_head(self, session, url, cycle=0, timeout=None):
        if cycle > 4:
            await self.log(f'[....{cycle}]{url} > max cycles')
            return

        start = time.time()
        if not timeout:
            timeout = self.options['timeout']

        proxy = await self.proxy.get()
        data = await self.get_html(session, 'https://m.avito.ru' + url, proxy['url'], timeout)

        if data:
            dom = html.fromstring(data)
            urls = dom.xpath('//a[@itemprop="url"]/@href')

            if len(urls) == 0:
                proxy['error'] += 1

                cycle += 1
                await self.get_head(session, url, cycle)
                return
            else:
                proxy['used'] += 1
                await self.log(f'{len(urls)} from [{proxy["id"]:3d}|{proxy["used"]:3d}|{proxy["error"]:2d}|{cycle:2d}|{len(data):6d}|{time.time()-start:.2f}] {url}')

                await asyncio.gather(*[self.get_page(session, url) for url in urls])
        else:
            proxy['error'] += 1
            cycle += 1
            await self.get_head(session, url, cycle, timeout*self.options['timeout_multiplier'])

    async def get_page(self, session, url, cycle=0, timeout=None):
        if cycle > 4:
            await self.log(f'  [....{cycle}]{url} > max cycles')
            return

        proxy = await self.proxy.get()
        if not timeout:
            timeout = self.options['timeout']

        data = await self.get_html(session, 'https://m.avito.ru' + url, proxy['url'], timeout)
        if data:
            proxy['used'] += 1
            await self.extract_data(url, data)
        else:
            proxy['error'] += 1
            cycle += 1
            await self.get_page(session, url, cycle, timeout*self.options['timeout_multiplier'])

    async def save(self):
        cursor = await self.db.execute('SELECT count(*) FROM items')
        items_count = (await cursor.fetchone())[0]

        await self.db.executemany('INSERT OR IGNORE INTO items(dt,url,name,price,desc,author,address,phone) VALUES (?,?,?,?,?,?,?,?)',
            [(item['dt'], item['url'], item['name'], item['price'], item['desc'], item['author'], item['address'], item['phone']) for item in self.items])
        await self.db.commit()

        cursor = await self.db.execute('SELECT count(*) FROM items')
        new_items_count = (await cursor.fetchone())[0]

        self.new_items = new_items_count - items_count

    async def get_html(self, session, url, proxy_url, timeout):
        self.requests += 1
        try:
            async with session.get(url, proxy=proxy_url, timeout=timeout) as response:
                return await response.text()
        except:
            return False

    async def extract_data(self, url, data):
        dom = html.fromstring(data)

        item = {
            'dt': self.extract_datetime(self.xpath(dom, '//div[@data-marker="item-stats/timestamp"]/span/text()')),
            'url': url,
            'name': self.xpath(dom, '//h1[@data-marker="item-description/title"]/span/text()'),
            'price': self.xpath(dom, '//span[@data-marker="item-description/price"]/text()'),
            'desc': self.xpath(dom, '//div[@data-marker="item-description/text"]/text()'),
            'author': self.xpath(dom, '//span[@data-marker="seller-info/name"]/text()'),
            'author_postfix': self.xpath(dom, '//span[@data-marker="seller-info/postfix"]/text()'),
            'author_summary': self.xpath(dom, '//span[@data-marker="seller-info/summary"]/text()').split(),

            'address': self.xpath(dom, '//span[@data-marker="delivery/location"]/text()'),
            'phone': self.xpath(dom, '//a[@data-marker="item-contact-bar/call"]/@href'),

            'avatar': self.xpath(dom, '//div[@data-marker="avatar-seller-info"]/img/@src'),

            'stats': self.xpath(dom, '//div[@data-marker="item-stats/views"]'),
            'timestamp': self.xpath(dom, '//div[@data-marker="item-stats/timestamp"]')
        }

        if item['author_summary']:
            item['author_summary'] = item['author_summary'][0]
        else:
            item['author_summary'] = 0

        if item['phone'].strip() != '':
            item['phone'] = item['phone'].replace('tel:', '')
            if len(item['phone']) > 12:
                item['phone'] = ''

        if item['stats'] != '':
            item['stats'] = item['stats'].text_content()

        if item['timestamp'] != '':
            item['timestamp'] = item['timestamp'].text_content()

        if item['phone'] != '':
            self.items.append(item)

    def extract_datetime(self, dt):
        # Сегодня, 10:32
        # Вчера, 16:04
        # 12 июля, 18:14
        if dt.strip() == '':
            return datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        if 'Сегодня' in dt:
            dt = dt.replace('Сегодня,', datetime.strftime(datetime.now(), '%Y-%m-%d')) + ':00'
            dt = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
        elif 'Вчера' in dt:
            dt = dt.replace('Вчера,', datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d')) + ':00'
        else:
            dt = dt.replace(',', '')
            dt = dt.split()
            month = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'].index(dt[1]) + 1
            dt = f'{datetime.strftime(datetime.now(), "%Y")}-{month}-{dt[0]} {dt[2]}:00'
            dt = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
        return dt

    def xpath(self, dom, query):
        tmp = dom.xpath(query)
        if tmp:
            return tmp[0]
        return ''
