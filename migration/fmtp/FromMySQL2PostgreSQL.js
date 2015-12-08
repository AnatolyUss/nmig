/* 
 * This file is a part of "NMIG" - the database migration tool.
 * 
 * Copyright 2015 Anatoly Khaytovich <anatolyuss@gmail.com>
 * 
 * @author Anatoly Khaytovich <anatolyuss@gmail.com>  
 */
'use strict';
var fs    = require('fs');
var pg    = require('pg');
var mysql = require('mysql');

/**
 * Constructor.
 */
function FromMySQL2PostgreSQL() {
    this._0777 = '0777';
}

/**
 * Sets configuration parameters.
 * 
 * @param   {FromMySQL2PostgreSQL} self 
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.boot = function(self) {
    return new Promise(function(resolve, reject) {
        self.log(self, '\t--Boot...');
        
        if (self._config.source === undefined) {
            self.log(self, '\t--Cannot perform a migration due to missing source database (MySQL) connection string');
            self.log(self, '\t--Please, specify source database (MySQL) connection string, and run the tool again');
            reject();
        }
        
        if (self._config.target === undefined) {
            self.log(self, '\t--Cannot perform a migration due to missing target database (PostgreSQL) connection string');
            self.log(self, '\t--Please, specify target database (PostgreSQL) connection string, and run the tool again');
            reject();
        }
        
        self._sourceConString     = self._config.source;
        self._targetConString     = self._config.target;
        self._tempDirPath         = self._config.tempDirPath;
        self._logsDirPath         = self._config.logsDirPath;
        self._allLogsPath         = self._logsDirPath + '/all.log';
        self._reportOnlyPath      = self._logsDirPath + '/report-only.log';
        self._errorLogsPath       = self._logsDirPath + '/errors-only.log';
        self._notCreatedViewsPath = self._logsDirPath + '/not_created_views';
        self._encoding            = self._config.encoding === undefined ? 'utf-8' : self._config.encoding;
        self._schema              = self._config.schema === undefined ? '' : self._config.schema;
        self._dataChunkSize       = self._config.data_chunk_size === undefined ? 10 : +self._config.data_chunk_size;
        self._dataChunkSize       = self._dataChunkSize < 1 ? 1 : self._dataChunkSize;
        self._mysql               = null;
        self._pgsql               = null;
        self._tablesToMigrate     = [];
        self._viewsToMigrate      = [];
        self._summaryReport       = [];
        
        var params          = self._sourceConString.split(',');
        var conStringParams = params[0].split(';');
        
        for (var i = 0; i < conStringParams.length; i++) {
            if (conStringParams[i].indexOf('dbname') === 0) {
                var arrPair = conStringParams[i].split('=');
                self._mySqlDbName = arrPair[1];
                break;
            }
        }
        
        self.log(self, '\t--Boot accomplished');
        resolve(self);
    });
};

/**
 * Creates temporary directory.
 *  
 * @param   {FromMySQL2PostgreSQL} self 
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.createTemporaryDirectory = function(self) {
    return new Promise(function(resolve, reject) {
        self.log(self, '\t--Creating temporary directory...');
        fs.stat(self._tempDirPath, function (directoryDoesNotExist, stat) {
            if (directoryDoesNotExist) {
                fs.mkdir(self._tempDirPath, self._0777, function(e) {
                    if (e) {
                        self.log(self, 
                            '\t--Cannot perform a migration due to impossibility to create ' 
                            + '"temporary_directory": ' + self._tempDirPath
                        );
                        reject();
                    } else {
                        self.log(self, '\t--Temporary directory is created...');
                        resolve(self);
                    }
                });
                
            } else if (!stat.isDirectory()) {
                self.log(self, '\t--Cannot perform a migration due to unexpected error');
                reject();
                
            } else {
                self.log(self, '\t--Temporary directory already exists...');
                resolve(self);
            }
        });
    });
};

/**
 * Creates logs directory.
 *  
 * @param   {FromMySQL2PostgreSQL} self 
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.createLogsDirectory = function(self) {
    return new Promise(function(resolve, reject) {
        self.log(self, '\t--Creating logs directory');
        fs.stat(self._logsDirPath, function (directoryDoesNotExist, stat) {
            if (directoryDoesNotExist) {
                fs.mkdir(self._logsDirPath, self._0777, function(e) {
                    if (e) {
                        self.log(self, 
                            '\t--Cannot perform a migration due to impossibility to create ' 
                            + '"logs_directory": ' + self._logsDirPath
                        );
                        reject();
                    } else {
                        self.log(self, '\t--Logs directory is created...');
                        resolve(self);
                    }
                });
                
            } else if (!stat.isDirectory()) {
                self.log(self, '\t--Cannot perform a migration due to unexpected error');
                reject();
                
            } else {
                self.log(self, '\t--Logs directory already exists...');
                resolve(self);
            }
        });
    });
};

/**
 * Outputs given log.
 * Writes given string to the "/all.log" file.
 * 
 * @param   {FromMySQL2PostgreSQL} self
 * @param   {string}               log
 * @param   {boolean}              isErrorLog
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.log = function(self, log, isErrorLog) {
    return new Promise(function(resolve, reject) {
        if (isErrorLog === undefined || isErrorLog === false) {
            console.log(log);
        }
        
        var buffer = new Buffer(log + '\n\n');
        
        if (self._allLogsPathFd === undefined) {
            fs.open(self._allLogsPath, 'a', self._0777, function(error, fd) {
                self._allLogsPathFd = fd;
                fs.write(self._allLogsPathFd, buffer, 0, buffer.length, null, function(error) {
                    resolve(self);
                });
            });
            
        } else {
            fs.write(self._allLogsPathFd, buffer, 0, buffer.length, null, function(error) {
                resolve(self);
            });
        }
    });
};

/**
 * Writes a ditailed error message to the "/errors-only.log" file
 * 
 * @param   {FromMySQL2PostgreSQL} self
 * @param   {string}               message
 * @param   {string}               sql
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.generateError = function(self, message, sql) {
    return new Promise(function(resolve, reject) {
        message    += sql === undefined ? '' : '\nSQL: ' + sql + '\n\n';
        var buffer  = new Buffer(message);
        self.log(self, message, true);
        
        if (self._errorLogsPathFd === undefined) {
            fs.open(self._errorLogsPath, 'a', self._0777, function(error, fd) {
                self._errorLogsPathFd = fd;
                fs.write(self._errorLogsPathFd, buffer, 0, buffer.length, null, function(error) {
                    resolve(self);
                });
            });
            
        } else {
            fs.write(self._errorLogsPathFd, buffer, 0, buffer.length, null, function(error) {
                resolve(self);
            });
        }
    });
};

/**
 * Check if both servers are connected.
 * If not, than create connections.
 * 
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.connect = function(self) {
    return new Promise(function(resolveOuter, rejectOuter) {
        self.log(self, '\t--Check DB connections...');
        
        // Check if MySQL server is connected.
        // If not connected - connect.
        if (!self._mysql) {
            self.log(self, '\t--Connecting to MySQL...');
            var arrSourceConnectionString = self._sourceConString.split(',');
            var strConStr                 = arrSourceConnectionString[0];
            var arrConStr                 = strConStr.split(';');
            var credentials               = {};
            
            for (var i = 0; i < arrConStr.length; i++) {
                var arrPair = arrConStr[i].split('=');
                
                if (arrPair[0].indexOf('host') !== -1) {
                    credentials['host'] = arrPair[1];
                } else if (arrPair[0] === 'port') {
                    credentials['port'] = arrPair[1];
                } else if (arrPair[0] === 'dbname') {
                    credentials['database'] = arrPair[1];
                }
            }
	    
            // Omit arrConStr.
            for (var i = 1; i < arrSourceConnectionString.length; i++) {
                if (arrSourceConnectionString[i] === 'charset') {
                    credentials['charset'] = arrSourceConnectionString[i];
                } else if (credentials['user'] === undefined) {
                    credentials['user'] = arrSourceConnectionString[i];
                } else {
                    credentials['password'] = arrSourceConnectionString[i];
                }
            }
	    
            var pool = mysql.createPool(credentials);
            
            if (pool) {
                self.log(self, '\t--MySQL server is connected...');
                self._mysql = pool;
                //resolve(self); // PASS RESOLVE() ONLY WHEN BOTH MYSQL & PGSQL WILL BE ESTABLISHED.
            }
            
            // TEST.
            /*self._mysql.getConnection(function(error, connection) {
                if (error) {
                    self.log(self, '\t--Cannot connect to MySQL server...');
                    rejectOuter();
                } else {
                    var sql = 'SELECT * FROM `admins`';
                    connection.query(sql, function(strErr, rows) {
                        if (strErr) {
                            self.generateError(self, strErr, sql);
                        } else {
                            console.log(rows);
                            
                            rows.forEach(function(objRow) {
                                console.log(JSON.stringify(objRow));
                            });
                        }
                        
                        // Release connection back to the pool.
                        connection.release();
                    });

                }
            });*/
            
        }
        
        // Check if PostgreSQL server is connected.
        // If not connected - connect.
        if (!self._pgsql) {
            self.log(self, '\t--Connecting to PostgreSQL');
        }
    });
};

/**
 * Runs migration according to user's configuration.
 * 
 * @param   {object} config
 * @returns {undefined} 
 */
FromMySQL2PostgreSQL.prototype.run = function(config) {
    var self     = this;
    self._config = config;
    var promise  = new Promise(function(resolve, reject) {
        resolve(self);
    });
    
    promise.then(
        self.boot,
        function() {
            self.log(self, '\t--Failed to boot migration');
        }
    ).then(
        self.createTemporaryDirectory, 
        function() {
            self.log(self, '\t--Temporary directory was not created...');
        }
        
    ).then(
        self.createLogsDirectory, 
        function() {
            self.log(self, '\t--Logs directory was not created...');
        }
	
    ).then(self.connect);
};

module.exports.FromMySQL2PostgreSQL = FromMySQL2PostgreSQL;


// node C:\xampp\htdocs\nmig\main.js C:\xampp\htdocs\nmig\sample_config.json  
// http://stackoverflow.com/questions/6731214/node-mysql-connection-pooling 
/*
 var path = 'public/uploads/file.txt',
buffer = new Buffer("some content\n");
fs.open(path, 'w', function(err, fd) {
    if (err) {
        throw 'error opening file: ' + err;
    }
    fs.write(fd, buffer, 0, buffer.length, null, function(err) {
        if (err) throw 'error writing file: ' + err;
        fs.close(fd, function() {
            console.log('file written');
        })
    });
}); 
 */
