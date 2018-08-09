# 弹幕总量计算，将计算结果存入xj_anchor_data表中，每日运行一次，在xj_anchor_data爬虫运行结束后运行
import pymysql
import time
from pymongo import MongoClient
import logging
import os
import danmu_config as dbo

date = time.strftime('%Y-%m-%d', time.localtime(int(time.time())))
LOG_FILE = "./log/totalCount_{}.log".format(date)
logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s %(filename)s-%(levelname)s:%(message)s",
    filename=os.path.join(os.getcwd(), LOG_FILE),
    filemode="a"
)

select_sql = "SELECT id,platform_id FROM xj_star"
select_exist = "SELECT id FROM xj_anchor_data WHERE date=%s AND anchor_id=%s"
update_sql = "UPDATE xj_anchor_data SET danmu=%s WHERE date=%s AND anchor_id=%s"
date = time.strftime('%Y-%m-%d', time.localtime(int(time.time())))


def main():
    mysql_connect = pymysql.connect(
        host=dbo.MYSQL_HOST,
        port=dbo.MYSQL_PORT,
        database=dbo.MYSQL_DB,
        user=dbo.MYSQL_USER,
        password=dbo.MYSQL_PASS,
        charset=dbo.MYSQL_CHAR
    )
    mysql_connect.autocommit(1)
    cursor = mysql_connect.cursor()
    try:
        cursor.execute(select_sql)
    except Exception as err:
        logging.critical("从star表中查询基本数据失败！err:{}".format(err))
        return
    result = cursor.fetchall()
    for i in result:
        try:
            cursor.execute(select_exist, [date, int(i[0])])
            exist_res = cursor.fetchall()
        except Exception as err:
            logging.error("查询主播 {} {} data表中数据是否存在时失败！err:{}".format(i[0], date, err))
            continue
        if len(exist_res) > 0:
            danmu = danmu_count(i[0], i[1])
            try:
                cursor.execute(update_sql, [int(danmu), date, int(i[0])])
            except Exception as err:
                logging.error("主播 {} {} 在data表中弹幕统计量更新操作失败！err:{}".format(i[0], date, err))
                continue
        else:
            logging.error("主播 {} 在 {} xj_anchor_data表中无数据...".format(i[0], date))
            pass
    mysql_connect.close()


def danmu_count(anchor_id, platform_id):
    mongo_connect = MongoClient(host=dbo.MONGO_HOST, port=dbo.MONGO_PORT)
    db = mongo_connect[dbo.MONGO_DB]
    coll = db[dbo.platform_dict[str(platform_id)]]
    try:
        danmu = coll.find({"anchor_id": str(anchor_id)}).count()
        return danmu
    except Exception as err:
        logging.error("主播 {} 的弹幕数查询失败！返回danmu为0 err:{}".format(anchor_id, err))
        return 0


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
