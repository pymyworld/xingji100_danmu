// 该脚本执行虎牙平台弹幕监听，执行前注意将xj_star表中将残缺live_url进行修改，例如http://www.huya.com/
const huya_danmu = require('huya-danmu');
// var mysql = require('mysql');
var mongoclient = require('mongodb').MongoClient;
var time = parseInt(Date.now() / 1000);
var log4js = require('log4js');
var format = require('string-format');
var date = new Date();
var year  = date.getFullYear();
var month = date.getMonth()+1;
var day = date.getDate();
var log_date = year + '-' + month + '-' + day;
var danmu_con = require('./danmu_config');
const huya_id = 7;
var dbhelper = require("./dbhelper").DBHelper();
// 日志配置
log4js.configure({
    appenders: {
        // out: {type: 'console'},
        app:{type: 'file', filename: format('./log/huya_{}.log', log_date)}
    },
    categories: {
        default: {appenders: ['app'], level: 'debug'}
    }
});

var logger = log4js.getLogger();
// 创建一个connection
// var connection = mysql.createConnection({
    // host: danmu_con.mysql_host,
    // user: danmu_con.mysql_user,
    // password: danmu_con.mysql_passwd,
    // port: danmu_con.mysql_port,
    // database: danmu_con.mysql_db,
// });
// 创建一个connection
// connection.connect();

var select_sql = "SELECT * FROM xj_gift_value WHERE name='{}' AND platform_id={}";
var insert_sql = "INSERT INTO xj_gift_value VALUES(0,'{}','{}',{},{},{},{})";

mongoclient.connect(danmu_con.mongo_url, function (err, db) {
    if(err) {
        logger.debug(`${err}`);
    };
    var dbo = db.db('danmu');
    // 执行查询语句
    // connection.query('SELECT id,live_url FROM xj_star WHERE live_url LIKE "%huya%"', function (err, results) {
    dbhelper.Query('SELECT id,live_url FROM xj_star WHERE live_url LIKE "%huya%" AND view_num>10000 AND is_publish=1', function (err, results) {
        if (err) {
            logger.debug('[query] - :' + err);
            return;
        };

        for (let i = 0; i < results.length; i++) {
            const element = results[i].live_url;
            const id = results[i].id;
            const roomid = element.match('^http://www.huya.com/(.+)$')[1];

            // 获得roomid，执行监听房间弹幕，以下均为监听代码
            const client = new huya_danmu(roomid);

            client.on('connect', () => {
                logger.debug(`已连接huya ${roomid}房间弹幕~`);
            });

            client.on('message', msg => {
                switch (msg.type) {
                    case 'chat':
                        // 存入mongodb数据库
                        var data = {anchor_id: `${id}`, live_url: `${element}`, platform_id: 7, add_time: parseInt(`${time}`), type: 'danmu', content: `${msg.content}`};
                        dbo.collection('huya').insertOne(data, function (err, result) {
                            if (err) {
                                logger.debug(`${err}`);
                            };
                            // console.log('弹幕数据插入成功');
                        });
                        // console.log(`[${msg.from.name}]:${msg.content}`);
                        break;
                    case 'gift':
                        // 存入mongodb数据库
                        var data = {anchor_id: `${id}`, live_url: `${element}`, platform_id: 7, add_time: parseInt(`${time}`), type: 'gift', content: `${msg.name}`, gift_num: `${msg.count}`};
                        dbo.collection('huya').insertOne(data, function (err, result) {
                            if(err) {
                                logger.debug(`${err}`);
                            };
                            // console.log('礼物数据插入成功');
                        });
                        // console.log(`[${msg.from.name}]->赠送${msg.count}个${msg.name}`);

                        // 将礼物信息存入xj_gift_value表中
                        dbhelper.Query(format(select_sql, msg.name, huya_id), function (err, rows) {
                            if (err) {
                                logger.debug(err);
                            };
                            if (rows.length == 0) {
                                dbhelper.Query(format(insert_sql, msg.id, msg.name, huya_id, parseFloat(msg.earn/msg.count), time, time), function (err, rows) {
                                    if (err) {
                                        logger.debug(err);
                                    }else{
                                        logger.debug('insert successfully!');
                                    };
                                });
                            };
                        });
                        break;
                    case 'online':
                        // console.log(`[当前人气]:${msg.count}`);
                        break;
                };
            });

            client.on('error', e => {
                logger.debug(e);
            });

            client.on('close', () => {
                logger.debug('close');
            });

            client.start();
            // 监听代码结束
        };
    });
});
