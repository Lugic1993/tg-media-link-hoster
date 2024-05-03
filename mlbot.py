import asyncio
import datetime,re,random,time,hashlib,uuid
from sys import stderr, stdout
from threading import Timer

from pyrogram import Client
from pyrogram.enums import MessageMediaType,ChatType,ParseMode
from pyrogram.errors import FileReferenceExpired,FloodWait,AuthBytesInvalid
from pyrogram.client import Cache
from pyrogram import filters
import mysql.connector
import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()

api_id = 11451400000000
api_hash = "qweasdzxc888888888888888888"
bot_token = "23333333333:poilkjmnb00000000000000"
app = Client("mlkauto", api_id=api_id, api_hash=api_hash,bot_token=bot_token, max_concurrent_transmissions = 1, sleep_threshold = 60)

app.message_cache = Cache(1000000)
groups = [-100131413141314]
use_record = {}
database = {"host": "127.0.0.1", "user" : "mlkauto", "password": "mlkbot_pwd_5555555555", "dbname": "mlbot"}

processed_media_groups = {}
expiration_time = 1800
decode_users = {}

conn = mysql.connector.connect(user=database["user"], password=database["password"], host=database["host"], database=database["dbname"])

# Function to periodically clean up expired entries
def cleanup_processed_media_groups():
    current_time = time.time()
    expired_keys = [key for key, timestamp in processed_media_groups.items() if current_time - timestamp > expiration_time]
    for key in expired_keys:
        del processed_media_groups[key]

async def decode_rate_con(uid, p = 0):
    if p:
        decode_users[uid] = time.time() + p
        return
    expired_keys = [key for key, timestamp in decode_users.items() if time.time() - timestamp > 120]
    for key in expired_keys:
        del decode_users[key]
    if (uid in decode_users):
        if(time.time() - decode_users[uid] < 30):
            return True
        return False
    else:
        decode_users[uid] = time.time()
        return False

def write_rec(mlk, mkey, skey, owner, desta, mgroup_id = ""):
    cursor = conn.cursor(dictionary=True)
    sql = 'INSERT INTO records (mlk, mkey, skey, owner, mgroup_id, desta ) VALUES (%s, %s, %s, %s, %s, %s)'
    cursor.execute(sql, (mlk, mkey, skey, owner, mgroup_id, desta))
    conn.commit()
    cursor.close()
    conn.close()

def read_rec(mlk):
    cursor = conn.cursor(dictionary=True)
    sql = 'SELECT * FROM records WHERE mlk = %s'
    cursor.execute(sql, (mlk,))
    result = cursor.fetchone()
    conn.commit()
    if len(result) > 0:
        sql = 'UPDATE records SET views = views + 1 WHERE mlk = %s'
        cursor.execute(sql, (mlk,))
        conn.commit()
        cursor.close()
        conn.close()
        return result
    else:
        cursor.close()
        conn.close()
        return False

def rotate_skey(mlk):
    skey = str(uuid.uuid4()).split("-")[-1]
    cursor = conn.cursor(dictionary=True)
    sql = 'UPDATE records SET skey = %s WHERE mlk = %s'
    cursor.execute(sql, (skey, mlk))
    conn.commit()
    cursor.close()
    conn.close()

def set_name(mlk, name):
    cursor = conn.cursor(dictionary=True)
    sql = 'UPDATE records SET name = %s WHERE mlk = %s'
    cursor.execute(sql, (name, mlk))
    conn.commit()
    cursor.close()
    conn.close()

def search_names(owner, name):
    cursor = conn.cursor(dictionary=True)
    sql = 'SELECT * FROM records WHERE owner = %s AND name like %s ORDER BY ID DESC LIMIT 12'
    cursor.execute(sql, (owner, '%' + name + '%'))
    result = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()
    if len(result) > 0:
        return result
    else:
        return False

async def media_to_link(mlk, mkey, skey, chat_id, msg_id, owner, mgroup_id, stor_sem):
    async with stor_sem:
        await asyncio.sleep(1 + random.randint(23,35) / 10)
        if len(mgroup_id) == 0:
            try:
                dup_message = await app.copy_message(chat_id = groups[0], from_chat_id = chat_id, message_id = msg_id)
            except FloodWait as e:
                await asyncio.sleep(e.value)
                dup_message = await app.copy_message(chat_id = groups[0], from_chat_id = chat_id, message_id = msg_id)
        else:
            try:
                dup_message = await app.copy_media_group(chat_id = groups[0], from_chat_id = chat_id, message_id = msg_id)
            except FloodWait as e:
                await asyncio.sleep(e.value)
                dup_message = await app.copy_media_group(chat_id = groups[0], from_chat_id = chat_id, message_id = msg_id)
            dup_message = dup_message[0]
        if (not dup_message.id):
            await asyncio.sleep(3.5)
            return media_to_link(mlk, mkey, skey, chat_id, msg_id, owner, mgroup_id, stor_sem)
        write_rec(mlk, mkey, skey, owner, dup_message.id, mgroup_id)
        keyout = '<b>主分享KEY</b>: `mlk$' + mlk + '#' + mkey + '`\n<b>一次性KEY</b>: `mlk$' + mlk + '#' + skey + '`' + '\n\n主分享KEY可重复使用，一次性KEY在获取一次后会失效，如果你是资源上传者，可以向机器人发送主分享KEY来获取最新可用的一次性KEY'
        await app.send_message(chat_id, text = keyout, reply_to_message_id = msg_id)
        await asyncio.sleep(0.3)

async def media_prep(chat_id, msg_id, owner, msg_dt, mgroup_id = ""):
    mlk = hashlib.sha3_256()
    prep_key = str(chat_id) + str(msg_id) + str(owner) + str(msg_dt) + str(uuid.uuid4())
    mlk.update(prep_key.encode())
    mlk = mlk.hexdigest()
    mkey = str(uuid.uuid4()).split("-")[-1]
    skey = str(uuid.uuid4()).split("-")[-1]
    copy_task = []
    stor_sem = asyncio.Semaphore(2)
    task = asyncio.create_task(media_to_link(mlk, mkey, skey, chat_id, msg_id, owner, mgroup_id, stor_sem))
    copy_task.append(task)
    await asyncio.gather(*copy_task)

async def link_to_media(chat_id, msg_id, desta, mgroup_id, ret_sem):
    async with ret_sem:
        if (mgroup_id):
            try:
                await app.copy_media_group(chat_id, from_chat_id = groups[0], message_id = desta, reply_to_message_id = msg_id)
            except FloodWait as e:
                await asyncio.sleep(e.value)
                await app.copy_media_group(chat_id, from_chat_id = groups[0], message_id = desta, reply_to_message_id = msg_id)
        else:
            try:
                await app.copy_message(chat_id, from_chat_id = groups[0], message_id = desta)
            except:
                await asyncio.sleep(e.value)
                await app.copy_message(chat_id, from_chat_id = groups[0], message_id = desta)
        await asyncio.sleep(1 + random.randint(28,35) / 10)

async def link_prep(chat_id, msg_id, from_id, result):
    for m in result:
        mkey = m[4:68]
        rkey = m[69:82]
        data_set = read_rec(mkey)
        ret_task = []
        ret_sem = asyncio.Semaphore(2)
        if data_set:
            desta = data_set['desta']
            mgroup_id = data_set['mgroup_id']
            if rkey == data_set["mkey"]:
                #return media and current skey
                task = asyncio.create_task(link_to_media(chat_id, msg_id, desta, mgroup_id, ret_sem))
                ret_task.append(task)
                await asyncio.gather(*ret_task)
                if from_id == data_set['owner']:
                    #return skey
                    skey_disp = '本资源当前一次性KEY: `mlk$' + data_set['mlk'] + '#' + data_set['skey'] + '`'
                    await app.send_message(chat_id, text = skey_disp, reply_to_message_id = msg_id)
                continue
            if rkey == data_set["skey"]:
                #return media and rotate skey
                rotate_skey(mkey)
                task = asyncio.create_task(link_to_media(chat_id, msg_id, desta, mgroup_id, ret_sem))
                ret_task.append(task)
                await asyncio.gather(*ret_task)
                await app.send_message(chat_id, text = "当前使用的是一次性KEY，该KEY已自动销毁，无法再用")
                continue
            if rkey != data_set["mkey"] and rkey != data_set["skey"]:
                await app.send_message(chat_id, text = "资源索引有效，但密钥不正确，一分钟后可以再试", reply_to_message_id = msg_id)
            await decode_rate_con(from_id, p = 30)


@app.on_message(filters.command("start") & filters.private)
async def cmd_main(client, message):
    from_user = message.from_user.id
    welcome_text = '''
我是一个资源存储机器人，能够帮你把媒体资源转换为代码链接，便于分享和转发
直接向我发送媒体开始使用，或者发送 /help 查看帮助
'''
    await app.send_message(from_user, welcome_text)

@app.on_message(filters.command("help") & filters.private)
async def cmd_main(client, message):
    from_user = message.from_user.id
    help_message = '''
向我发送媒体或媒体组，你将得到两个代码链接：<u>主分享KEY</u>和<u>一次性KEY</u>
链接格式均为：<pre>mlk$[64位资源索引]#[12位密钥]</pre> 主分享KEY和一次性KEY的资源索引相同，但密钥不同

🔖 一次性KEY在被获取后，其密钥会自动销毁，即仅能获取一次，主分享KEY可以重复被获取
如果你是资源上传者，可以向机器人发送主分享KEY来获取最新的一次性KEY
为避免爆破攻击，当资源索引正确但密钥错误时系统会给出提示，并进入一分钟的冷却时间

📒 资源上传者可以向任意一条带资源链接的消息回复 <pre>/name 资源名称</pre> 来对资源命名，该名称只有上传者可见，用于资源搜索。资源名称中切勿包含空格

🔎 资源上传者可以使用 <pre>/s 关键词</pre> 来搜索自己上传的、有主动命名过的资源，[举例] 关键词'数字'可以匹配'阿拉伯数字'，'大写数字捌'等，搜索结果最多返回最近12条，搜索冷却时间为30秒

🔑 对于同一用户，链接转媒体的冷却时间为30秒，每条消息最多提交三个链接进行解析，超出部分会被忽略
'''
    await app.send_message(from_user, help_message)

@app.on_message(filters.command("lsa") & filters.private)
async def cmd_main(client, message):
     await app.copy_media_group(chat_id = message.chat.id, from_chat_id = groups[0], message_id = 466)

@app.on_message(filters.command("s") & filters.private)
async def cmd_main(client, message):
    if (message.text.find(" ") > 0):
        search_word = message.text.split(" ")[-1]
        if await decode_rate_con(message.from_user.id):
            await app.send_message(chat_id = message.chat.id, text = "每30秒最多提交一次搜索请求，请稍后再试")
            return
        data = search_names(message.from_user.id, search_word[0:32])
        if data:
            search_rr = '<b>搜索结果</b>：\n'
            n = 1
            for w in data:
                search_rr += str(n) + '.' + str(w['name']) + ': `mlk$' + w['mlk'] + '#' + w['mkey'] + '`\n'
                n += 1
            await app.send_message(chat_id = message.chat.id, text = search_rr)
        else:
            await app.send_message(chat_id = message.chat.id, text = "搜索无结果")

@app.on_message(filters.media_group & filters.private)
async def media_main(client, message):
    if len(processed_media_groups) % 1000 == 0:
        cleanup_processed_media_groups()
    if (message.from_user and message.from_user.id):
        owner = message.from_user.id
    else:
        owner = 0
    msg_id = message.id
    chat_id = message.chat.id
    mgroup_id = str(message.media_group_id)
    msg_dt = message.date
    if mgroup_id in processed_media_groups:
        return
    #send to storage func
    processed_media_groups[mgroup_id] = time.time()
    await media_prep(chat_id, msg_id, owner, msg_dt, mgroup_id)

@app.on_message(filters.media & filters.private)
async def media_main(client, message):
    if (message.media_group_id):
        return
    if (message.from_user and message.from_user.id):
        owner = message.from_user.id
    else:
        owner = 0
    msg_id = message.id
    chat_id = message.chat.id
    msg_dt = message.date
    #send to storage func
    await media_prep(chat_id, msg_id, owner, msg_dt)

@app.on_message(filters.reply & filters.private & filters.command("name"))
async def reply_main(client, message):
    msg_id = message.id
    chat_id = message.chat.id
    content = message.reply_to_message.text
    result = re.search(r'mlk\$\w{64}#\w{12}', content)
    result = result.group(0)
    if await decode_rate_con(message.from_user.id):
        await app.send_message(chat_id = message.chat.id, text = "每30秒最多提交一次命名请求，请稍后再试")
        return        
    if (message.text.find(" ") > 0):
        new_name = message.text.split(" ")[-1]
        if len(result):
            data_set = read_rec(result[4:68])
            if (data_set and data_set['owner'] == message.from_user.id):
                try:
                    set_name(result[4:68], new_name[0:32])
                    await app.send_message(chat_id, text = "命名成功", reply_to_message_id = message.id)
                except Exception as e:
                    await app.send_message(chat_id, text = "命名失败，请勿使用特殊符号", reply_to_message_id = msg_id)
            else:
                await app.send_message(chat_id, text = "你不是资源上传者，无权进行命名操作", reply_to_message_id = msg_id)
        
@app.on_message(filters.text & filters.private)
async def ret_main(client, message):
    in_text = message.text
    result = re.findall(r'mlk\$\w{64}#\w{12}', in_text)
    msg_id = message.id
    chat_id = message.chat.id
    if (message.from_user and message.from_user.id):
        from_id = message.from_user.id
    else:
        from_id = 0
    if len(result) > 0:
        if await decode_rate_con(message.from_user.id):
            await app.send_message(chat_id = message.chat.id, text = "每30秒最多提交一次解析请求，请稍后再试")
            return
        if len(result) > 3:
            #return warning info
            await app.send_message(chat_id = message.chat.id, text = "一次最多解析三个KEY，超出部分会被忽略")
            result = result[0:3]
        #send to decode func
        await link_prep(chat_id, msg_id, from_id, result)
app.run()