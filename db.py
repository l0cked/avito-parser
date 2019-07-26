import aiosqlite


class Db:
    conn = None

    def __init__(self, filename):
        self.filename = filename

    async def __aenter__(self):
        self.conn = await aiosqlite.connect(self.filename)
        await self.create()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.conn.close()

    async def create(self):
        await self.conn.executescript('''

            CREATE TABLE IF NOT EXISTS log (
            id integer primary key autoincrement not null,
            added datetime default (datetime('now', 'localtime')),
            message text not null,
            messageType text not null,
            messageData text not null);

            CREATE TABLE IF NOT EXISTS proxy (
            id integer primary key autoincrement not null,
            added datetime default (datetime('now', 'localtime')),
            country text not null,
            host text not null,
            port text not null,
            url text not null unique,
            response_time real not null,
            used integer not null default 0,
            error integer not null default 0);

            CREATE TABLE IF NOT EXISTS items (
            id integer primary key autoincrement not null,
            added datetime default (datetime('now', 'localtime')),
            dt datetime not null,
            url text not null unique,
            name text not null,
            price text not null,
            desc text not null,
            author text not null,
            address text not null,
            phone text not null);

            ''')
        await self.conn.commit()
