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
 * @param   {object} config
 * @returns {boolean}
 */
FromMySQL2PostgreSQL.prototype.boot = function(config) {
    console.log('\t--Boot...');
    var self = this;
    
    if (config.source === undefined) {
        console.log('\t--Cannot perform a migration due to missing source database (MySQL) connection string');
        console.log('\t--Please, specify source database (MySQL) connection string, and run the tool again');
        return false;
    }
    
    if (config.target === undefined) {
        console.log('\t--Cannot perform a migration due to missing target database (PostgreSQL) connection string');
        console.log('\t--Please, specify target database (PostgreSQL) connection string, and run the tool again');
        return false;
    }
    
    self._sourceConString     = config.source;
    self._targetConString     = config.target;
    self._tempDirPath         = config.tempDirPath;
    self._logsDirPath         = config.logsDirPath;
    self._allLogsPath         = self._logsDirPath + '/all.log';
    self._reportOnlyPath      = self._logsDirPath + '/report-only.log';
    self._errorLogsPath       = self._logsDirPath + '/errors-only.log';
    self._notCreatedViewsPath = self._logsDirPath + '/not_created_views';
    self._encoding            = config.encoding === undefined ? 'utf-8' : config.encoding;
    self._schema              = config.schema === undefined ? '' : config.schema;
    self._dataChunkSize       = config.data_chunk_size === undefined ? 10 : +config.data_chunk_size;
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
    
    console.log('\t--Boot accomplished');
    return true;
};

/**
 * Creates temporary directory.
 *  
 * @param   {FromMySQL2PostgreSQL} self 
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.createTemporaryDirectory = function(self) {
    return new Promise(function(resolve, reject) {
        console.log('\t--Creating temporary directory...');
        fs.stat(self._tempDirPath, function (directoryDoesNotExist, stat) {
            if (directoryDoesNotExist) {
                fs.mkdir(self._tempDirPath, self._0777, function(e) {
                    if (e) {
                        console.log(
                            '\t--Cannot perform a migration due to impossibility to create ' 
                            + '"temporary_directory": ' + self._tempDirPath
                        );
                        reject();
                    } else {
                        console.log('\t--Temporary directory is created...');
                        resolve(self);
                    }
                });
                
            } else if (!stat.isDirectory()) {
                console.log('\t--Cannot perform a migration due to unexpected error');
                reject();
                
            } else {
                console.log('\t--Temporary directory already exists...');
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
        console.log('\t--Creating logs directory');
        fs.stat(self._logsDirPath, function (directoryDoesNotExist, stat) {
            if (directoryDoesNotExist) {
                fs.mkdir(self._logsDirPath, self._0777, function(e) {
                    if (e) {
                        console.log(
                            '\t--Cannot perform a migration due to impossibility to create ' 
                            + '"logs_directory": ' + self._logsDirPath
                        );
                        reject();
                    } else {
                        console.log('\t--Logs directory is created...');
                        resolve(self);
                    }
                });
                
            } else if (!stat.isDirectory()) {
                console.log('\t--Cannot perform a migration due to unexpected error');
                reject();
                
            } else {
                console.log('\t--Logs directory already exists...');
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
        if (isErrorLog !== undefined && isErrorLog === true) {
            console.log(log);
        }
        
        var buffer = new Buffer(log);
        
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
 * @param   {Error|string}         appError
 * @param   {string}               message
 * @param   {string}               sql
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.generateError = function(self, appError, message, sql) {
    return new Promise(function(resolve, reject) {
        message    += sql === undefined ? '' : '\nSQL: ' + sql;
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
        var arrSourceConnectionString = self._sourceConString.split(',');
        var strConStr                 = arrSourceConnectionString[0];
        var arrConStr                 = strConStr.split(';');
        var credentials               = {};
        
        for (var i = 0; i < arrConStr.length; i++) {
            if (arrConStr[i].indexOf('host') !== -1) {
                credentials['host'] = arrConStr[i].split('=')[1];
            } else if (arrConStr[i] === 'port') {
                credentials['port'] = arrConStr[i];
            } else if (arrConStr[i] === 'dbname') {
                credentials['database'] = arrConStr[i];
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
        
        var connection = mysql.createConnection(credentials);
        
        // FOLLOWING SNIPPET MUST BE PROMISIFICATED.
        connection.connect(function(error) {
            if (error) {
                rejectOuter();
            } else {
                self._mysql = connection;
                //resolve(self); // PASS RESOLVE() ONLY WHEN BOTH MYSQL & PGSQL WILL BE ESTABLISHED.
            }
        });
        
        
    });
};

/**
 * Runs migration according to user's configuration.
 * 
 * @param   {object} config
 * @returns {undefined} 
 */
FromMySQL2PostgreSQL.prototype.run = function(config) {
    var self    = this;
    var promise = new Promise(function(resolve, reject) {
        if (self.boot(config)) {
            resolve(self);
        } else {
            reject();
        }
    });
    
    promise.then(
        self.createTemporaryDirectory, 
        function() {
            console.log('\t--Temporary directory was not created...');
        }
        
    ).then(
        self.createLogsDirectory, 
        function() {
            console.log('\t--Logs directory was not created...');
        }
    );
};

module.exports.FromMySQL2PostgreSQL = FromMySQL2PostgreSQL;



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


