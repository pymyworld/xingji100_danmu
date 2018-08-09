//该脚本执行douyu平台但弹幕监听，运行前注意将xj_star表中http://yuba.douyu.com/ 这种无效直播地址清除
const douyu_danmu = require('douyu-danmu');
var mysql = require('mysql');
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
var dbhelper = require("./dbhelper").DBHelper();
// 日志配置
log4js.configure({
    appenders: {
        // out: {type: 'console'},
        app:{type: 'file', filename: format('./log/douyu_{}.log', log_date)}
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

mongoclient.connect(danmu_con.mongo_url, function (err, db) {
    if(err) {
        logger.debug(`${err}`);
    };
    var dbo = db.db('danmu');
    // 执行查询语句
    // connection.query('SELECT id,live_url FROM xj_star WHERE live_url LIKE "%douyu%"', function (err, results) {
    dbhelper.Query('SELECT id,live_url FROM xj_star WHERE live_url LIKE "%douyu%" AND view_num>10000 AND is_publish=1',function(err, results) {
        if (err) {
            logger.debug('[query] - :' + err);
            return;
        };
        for (let i = 0; i < results.length; i++) {
            const element = results[i].live_url;
            const id = results[i].id;
            const roomid = element.match('^http://www\.douyu\.com/(.+)$')[1];

            // 获得roomid，执行监听房间弹幕，以下均为监听代码
            const client = new douyu_danmu(roomid);

            client.on('connect', () => {
                logger.debug(`已连接douyu ${roomid}房间弹幕~`);
            });

            client.on('message', msg => {
                switch (msg.type) {
                    case 'chat':
                        // 存入mongodb数据库
                        var data = {anchor_id: `${id}`, live_url: `${element}`, platform_id: 11, add_time: parseInt(`${time}`), type: 'danmu', content: `${msg.content}`};
                        dbo.collection('douyu').insertOne(data, function (err, result) {
                            if (err) {
                                logger.debug(`${err}`);
                            };
                            // console.log('弹幕数据插入成功');
                        });
                        // console.log(`[${msg.from.name}]:${msg.content}`);
                        break;
                    case 'gift':
                        // 存入mongodb数据库
                        var data = {anchor_id: `${id}`, live_url: `${element}`, platform_id: 11, add_time: parseInt(`${time}`), type: 'gift', content: `${msg.name}`, gift_num: `${msg.count}`};
                        dbo.collection('douyu').insertOne(data, function (err, result) {
                            if(err) {
                                logger.debug(`${err}`);
                            };
                            // console.log('礼物数据插入成功');
                        });
                        // console.log(`[${msg.from.name}]->赠送${msg.count}个${msg.name}`);
                        break;
                    case 'yuwan':
                        // 存入mongodb数据库
                        var data = {anchor_id: `${id}`, live_url: `${element}`, platform_id: 11, add_time: parseInt(`${time}`), type: 'gift', content: `${msg.name}`, gift_num: `${msg.count}`};
                        dbo.collection('douyu').insertOne(data, function (err, result) {
                            if (err) {
                                logger.debug(`${err}`);
                            };
                            // console.log('礼物数据插入成功');
                        });
                        // console.log(`[${msg.from.name}]->赠送${msg.count}个${msg.name}`);
                        break;
                    case 'deserve':
                        // 存入mongodb数据库
                        var data = {anchor_id: `${id}`, live_url: `${element}`, platform_id: 11, add_time: parseInt(`${time}`), type: 'gift', content: `${msg.name}`, gift_num: `${msg.count}`};
                        dbo.collection('douyu').insertOne(data, function (err, result) {
                            if (err) {
                                logger.debug(`${err}`);
                            };
                            // console.log('礼物数据插入成功');
                        });
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
