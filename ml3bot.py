import asyncio
import datetime,re,random,time,hashlib,uuid
from sys import stderr, stdout

from pyrogram import Client
from pyrogram.enums import MessageMediaType,ChatType,ParseMode
from pyrogram.errors import FileReferenceExpired,FloodWait,AuthBytesInvalid
from pyrogram.client import Cache
from pyrogram import filters
import mysql.connector
import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()

api_id = 11451422222222222
api_hash = "qweasdzxc666666666666666666666"
app = Client("mlk3auto", api_id=api_id, api_hash=api_hash, max_concurrent_transmissions = 1, sleep_threshold = 60)

app.message_cache = Cache(1000000)
groups = [-100131413141314, -100151615161516, -100171817181718]
use_record = {}
database = {"host": "127.0.0.1", "user" : "mlkauto", "password": "mlkbot_pwd_5555555555", "dbname": "mlbot"}

processed_media_groups = {}
expiration_time = 1800
decode_users = {}

conn = mysql.connector.connect(user=database["user"], password=database["password"], host=database["host"], database=database["dbname"])

def read_rec():
    cursor = conn.cursor(dictionary=True)
    sql = 'SELECT * FROM records WHERE destc is NULL'
    cursor.execute(sql)
    result = cursor.fetchall()
    if len(result) > 0:
        return result
    else:
        return False

def update_rec(mlk, res_id):
    cursor = conn.cursor(dictionary=True)
    sql = 'UPDATE records SET destc = %s WHERE mlk = %s'
    cursor.execute(sql, (res_id, mlk))
    conn.commit()
    cursor.close()
    conn.close()
    return True

def copy_main():
    pass

async def copy_prep():
    async with app:
        data = read_rec()
        if data:
            for w in data:
                if w['mgroup_id']:
                    res = await app.copy_media_group(chat_id = groups[2], from_chat_id = groups[0], message_id = w['desta'])
                    res = res[0]
                else:
                    res = await app.copy_message(chat_id = groups[2], from_chat_id = groups[0], message_id = w['desta'])
                if res and res.id:
                    update_rec(w['mlk'], res.id)
                time.sleep(1)  

app.run(copy_prep())