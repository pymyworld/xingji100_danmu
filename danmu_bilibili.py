import asyncio
import pymysql
import requests
import re
import json
import threading
import danmu_config
from bilibiliClient import bilibiliClient

select_sql = "SELECT id,live_url FROM xj_star WHERE live_url LIKE '%bilibili%' AND view_num>10000 AND is_publish=1"
bilibili_api = "https://api.live.bilibili.com/room/v1/Room/room_init?id={}"


def select_db():
    connect = pymysql.connect(
        host=danmu_config.MYSQL_HOST,
        port=danmu_config.MYSQL_PORT,
        user=danmu_config.MYSQL_USER,
        password=danmu_config.MYSQL_PASS,
        database=danmu_config.MYSQL_DB,
        charset=danmu_config.MYSQL_CHAR
    )
    cursor = connect.cursor()
    cursor.execute(select_sql)
    return cursor.fetchall()


def find_roomid(live_url):
    bili_id = re.findall(r"\d+", live_url)[0]
    response = requests.get(bilibili_api.format(bili_id))
    data = json.loads(response.content.decode())
    if data["data"]["live_status"] == 0:    # 未开播
        return None
    else:
        return data["data"]["room_id"]


def run(loop, roomid, anchor_id, live_url):
    danmuji = bilibiliClient(roomid, anchor_id, live_url)
    tasks = [
                danmuji.connectServer() ,
                danmuji.HeartbeatLoop()
            ]
    # loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(asyncio.wait(tasks))
    except KeyboardInterrupt:
        danmuji.connected = False
        for task in asyncio.Task.all_tasks():
            task.cancel()
        loop.run_forever()
    loop.close()


def main():
    select_result = select_db()
    for i in select_result:
        roomid = find_roomid(i[1])
        if roomid is None:
            continue
        else:
            new_loop = asyncio.new_event_loop()
            t = threading.Thread(target=run, args=(new_loop, roomid, i[0], i[1]))
            t.daemon = True
            t.start()
    flag = True
    while flag:
        if len(threading.enumerate()) > 0:
            flag = True
        else:
            flag = False


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass

