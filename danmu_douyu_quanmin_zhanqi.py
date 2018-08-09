# 使用danmu工具包监听斗鱼,全民,战旗平台弹幕礼物信息
import danmu_config
from pymongo import MongoClient
from danmu import DanMuClient
import pymysql
import time
import sys
import threading


douyu_id = 11
panda_id = 10
quanmin_id = 8
zhanqi_id = 9
bilibili_id = 15
huomao_id = 6

sql_str = "SELECT id,live_url FROM xj_star WHERE is_publish=1 AND view_num>10000 AND (live_url LIKE '%douyu%' OR live_url LIKE '%zhanqi%' OR live_url LIKE '%quanmin%')"


def get_star_info(sql):
    '''
    查询数据，构建关系
    :param sql:
    :return:
    '''
    dbparams = dict(
        host=danmu_config.MYSQL_HOST,
        port=danmu_config.MYSQL_PORT,
        db=danmu_config.MYSQL_DB,
        user=danmu_config.MYSQL_USER,
        passwd=danmu_config.MYSQL_PASS,
        charset=danmu_config.MYSQL_CHAR,
    )
    connect = pymysql.connect(**dbparams)
    cursor = connect.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    connect.close()
    live_list = list()
    # live_url与id的关系字典
    relation = dict()
    for i in result:
        # if i[1].find('douyu') > 0:
        #     live_list.append(i[1])
        #     relation[i[1]] = i[0]
        # if i[1].find('panda'):    # 熊猫运行结果无响应
        #     live_list.append(i[1])
        if i[1].find('quanmin') > 0:
            live_list.append(i[1])
            relation[i[1]] = i[0]
        # if i[1].find('huomao'):   # 火猫运行结果无响应
        #     live_list.append(i[1])
        if i[1].find('zhanqi') > 0:
            live_list.append(i[1])
            relation[i[1]] = i[0]
        # if i[1].find('bilibili'):   # b站运行结果无响应
        #     live_list.append(i[1])
    return live_list, relation


def save_danmu(live_url, info, relation):
    data = dict()
    # client = MongoClient(host=danmu_config.MONGO_HOST, port=danmu_config.MONGO_PORT)
    # db = client[danmu_config.MONGO_DB]
    data['anchor_id'] = relation[live_url]
    data['live_url'] = live_url
    data['add_time'] = int(time.time())
    data['type'] = 'danmu'
    data['content'] = info['Content']
    # if live_url.find('douyu') > 0:
    #     coll_name = danmu_config.douyu_coll
    #     data['platform_id'] = douyu_id
    #     return data, coll_name
    if live_url.find('quanmin') > 0:
        coll_name = danmu_config.quanmin_coll
        data['platform_id'] = quanmin_id
        return data, coll_name
    if live_url.find('zhanqi') > 0:
        coll_name = danmu_config.zhanqi_coll
        data['platform_id'] = zhanqi_id
        return data, coll_name
    else:
        return None, None


def save_gift(live_url, info, relation):
    data = dict()
    data['anchor_id'] = relation[live_url]
    data['live_url'] = live_url
    data['add_time'] = int(time.time())
    data['type'] = 'gift'
    # if live_url.find('douyu') > 0:
    #     coll_name = danmu_config.douyu_coll
    #     data['platform_id'] = douyu_id
    #     data['content'] = info['gfid']
    #     data['gift_num'] = 1
    #     return data, coll_name
    if live_url.find('quanmin') > 0:
        coll_name = danmu_config.quanmin_coll
        data['platform_id'] = quanmin_id
        data['content'] = info['Content']
        # data['gift_num'] = info['Gift_num']
        data['gift_num'] = 1
        return data, coll_name
    if live_url.find('zhanqi') > 0:
        coll_name = danmu_config.zhanqi_coll
        data['platform_id'] = zhanqi_id
        data['content'] = info['Content']
        # data['gift_num'] = info['Gift_num']
        data['gift_num'] = 1
        return data, coll_name
    else:
        return None, None


def start_danmu(live_url, relation):
    '''
    开始运行弹幕监听
    :param live_url:
    :return:
    '''
    client = MongoClient(host=danmu_config.MONGO_HOST, port=danmu_config.MONGO_PORT)
    db = client[danmu_config.MONGO_DB]

    dmc = DanMuClient(live_url)
    if not dmc.isValid():
        client.close()
        pass

    @dmc.danmu
    def danmu_fn(msg):
        # info = msg.encode(sys.stdin.encoding, 'ignore').decode(sys.stdin.encoding)
        data, coll_name = save_danmu(live_url, msg, relation)
        if data is not None and coll_name is not None:
            coll = db[coll_name]
            try:
                coll.insert(data)
            except Exception:
                pass

    @dmc.gift
    def gift_fn(msg):
        # info = msg.encode(sys.stdin.encoding, 'ignore').decode(sys.stdin.encoding)
        data, coll_name = save_gift(live_url, msg, relation)
        if data is not None and coll_name is not None:
            coll = db[coll_name]
            try:
                coll.insert(data)
            except Exception:
                pass

    dmc.start(blockThread=True)
    client.close()


def run():
    live_list, relation = get_star_info(sql_str)
    for live_url in live_list:
        t = threading.Thread(target=start_danmu, args=(live_url, relation))
        t.daemon = True
        t.start()

    flag = True
    while flag:
        if len(threading.enumerate()) > 0:
            flag = True
        else:
            flag = False


if __name__ == '__main__':
    run()



