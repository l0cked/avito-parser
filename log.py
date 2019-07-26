from datetime import datetime
import json


class Log:
    loglist = []

    def __init__(self, db):
        self.__db = db
        self.db = db.conn

    async def __aenter__(self):
        cursor = await self.db.execute('SELECT count(*) FROM log')

        if (await cursor.fetchone())[0] > 10000:
            await self.db.execute('DELETE FROM log')
            await self.db.execute(
                'DELETE FROM sqlite_sequence WHERE name="log"')
            await self.db.commit()

            await self.__db.create()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.db.executemany(
            'INSERT INTO log(message,messageType,messageData) VALUES (?,?,?)',
            self.loglist)
        await self.db.commit()

    async def msg(self, message, messageType='System', messageData={}):
        print(datetime.strftime(datetime.now(), '%H:%M:%S'), ' ', message)
        self.loglist.append((message, messageType, json.dumps(messageData)))
