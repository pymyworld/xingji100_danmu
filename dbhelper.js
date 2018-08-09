var mysql_con = require('./danmu_config');

var mysql = require('mysql');
var mysqlenv = {
    host : mysql_con.mysql_host,
    user : mysql_con.mysql_user,
    password : mysql_con.mysql_passwd,
    database : mysql_con.mysql_db,
    dateStrings : true,
    port : mysql_con.mysql_port,
    connectionLimit : 2
};

// function callback(error, results, param1, param2, param3)
exports.DBHelper = function() {
    var pool = mysql.createPool(mysqlenv);

    var op = {
        Query : function(sql, callback, param1, param2, param3) {
            pool.getConnection(function(err, connection) {
                if (err) {
                    throw err;
                }

                // Use the connection
                connection.query(sql, function(err, rows, fields) {
                    connection.release();
                    // Don't use the connection here, it has been returned to the pool.
                    try {
                        callback(err, rows, param1, param2, param3);
                    } catch(exp) {
                        param1.end(); // param1 is res
                    }
                 });
            });
        },

	Escape : function(data) {
            return mysql.escape(data);
        },

        QueryTest : function() {
            var conn = mysql.createConnection(mysqlenv);
            conn.connect()
             //conn.query( "CALL sp_login_sg('1001','abc','10','0');", function(err, rows, fields) {
             conn.query( "CALL sp_login(0,'abc');", function(err, rows, fields) {
                // console.log("err:",err);
                if (err) {
                    throw err;
                }
                // console.log("rows:", rows);
                //console.log("fields", fields);
                //var results = rows[0];
                //var row = results[0];
                // console.log("rows[0]", typeof(rows), "=>", rows[0], rows[0][0]);
                // console.log("--->", rows[0][0]["_ret"], rows[0][0]["_userid"]);

            });
            conn.end()
        },
    };

    return op;
};