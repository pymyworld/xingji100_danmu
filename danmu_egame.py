import pymysql
import danmu_config as dbo
import requests
import re
import os
import json
from pymongo import MongoClient
import threading
import time
import logging

platform_id = 14
select_sql = "SELECT id,live_url FROM xj_star WHERE live_url LIKE '%egame%' AND view_num>10000 AND is_publish=1"
time_s = int(time.time())
time_ms = int(round(time.time() * 1000))

# 日志配置
date = time.strftime('%Y-%m-%d', time.localtime(int(time.time())))
LOG_FILE = "./log/egame_{}.log".format(date)
logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s %(filename)s-%(levelname)s:%(message)s",
    filename=os.path.join(os.getcwd(), LOG_FILE),
    filemode="a"
)


def select_db():
    connect = pymysql.connect(
        host=dbo.MYSQL_HOST,
        port=dbo.MYSQL_PORT,
        user=dbo.MYSQL_USER,
        password=dbo.MYSQL_PASS,
        database=dbo.MYSQL_DB,
        charset=dbo.MYSQL_CHAR
    )
    cursor = connect.cursor()
    cursor.execute(select_sql)
    result = cursor.fetchall()
    return result


def main(anchor_id, live_url):
    response = requests.get(live_url)
    html = response.content.decode()
    try:
        anch_id = re.findall(r',"anchorId":(.*?),', html)[0]
        pid = re.findall(r',"liveAddr":"(.*?)",', html)[0]
        live_status = re.findall(r',"isLive":(.*?),"', html)[0]
    except Exception as e:
        logging.error("该直播间页面结构不同:{}  err:{}".format(live_url, e))
        return
    if live_status == "0":  # 关播状态
        return
    # 建立mongo连接
    client = MongoClient(host=dbo.MONGO_HOST, port=dbo.MONGO_PORT)
    db = client[dbo.MONGO_DB]
    coll = db["egame"]
    while True:
        danmu_url = 'http://wdanmaku.egame.qq.com/cgi-bin/pgg_barrage_async_fcgi?param={"key":{"module":"pgg_live_barrage_svr","method":"get_barrage","param":{"anchor_id":%s,"vid":"%s","scenes":4096,"last_tm":%s}}}&app_info={"platform":4,"terminal_type":2,"egame_id":"egame_official","version_code":"9.9.9","version_name":"9.9.9"}&g_tk=&p_tk=&tt=1&_t=%s' % (anch_id, pid, time_s, time_ms)
        try:
            res = requests.get(danmu_url)
        except Exception as e:
            logging.error("该弹幕url请求失败:{}  err:{}".format(danmu_url, e))
            return
        if res.status_code != 200:
            logging.error("danmu_url:{}  response:{}".format(danmu_url, res.status_code))
            return
        danmu_json = json.loads(res.content.decode())
        try:
            msg_list = danmu_json["data"]["key"]["retBody"]["data"]["msg_list"]
        except Exception as e:
            logging.error("未解析到msg_list url:{} err:{}".format(danmu_url, e))
            return
        if len(msg_list) > 0:
            for msg in msg_list:
                item = dict()
                item["anchor_id"] = anchor_id
                item["live_url"] = live_url
                item["platform_id"] = platform_id
                item["add_time"] = time_s
                if msg["type"] == 7:
                    item["content"] = msg["ext"]["n"]
                    item["type"] = "gift"
                    item["gift_num"] = msg["ext"]["gn"]
                else:
                    item["content"] = msg["content"]
                    item["type"] = "danmu"
                try:
                    coll.insert(item)
                except Exception as e:
                    logging.error("数据插入失败! err:{}".format(e))
        time.sleep(1)


def run():
    result = select_db()
    for i in result:
        # 建立线程
        t = threading.Thread(target=main, args=(i[0], i[1]))
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
        run()
    except KeyboardInterrupt:
        pass

