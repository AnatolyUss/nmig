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
 * Reads "./DataTypesMap.json" and converts its json content to js object.
 * Appends this object to "FromMySQL2PostgreSQL" instance.
 * 
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.readDataTypesMap = function(self) {
    return new Promise(function(resolve, reject) {
        fs.readFile(self._dataTypesMapAddr, function(error, data) {
            if (error) {
                console.log('\t--[readDataTypesMap] Cannot read "DataTypesMap" from ' + self._dataTypesMapAddr);
                reject();
            } else {
                try {
                    self._dataTypesMap = JSON.parse(data.toString());
                    console.log('\t--[readDataTypesMap] Data Types Map is loaded...');
                    resolve(self);
                } catch (err) {
                    console.log('\t--[readDataTypesMap] Cannot parse JSON from' + self._dataTypesMapAddr);
                    reject();
                }
            }
        });
    });
};

/**
 * Sets configuration parameters.
 * 
 * @param   {FromMySQL2PostgreSQL} self 
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.boot = function(self) {
    return new Promise(function(resolve, reject) {
        console.log('\n\t--[boot] Boot...');
        
        if (self._config.source === undefined) {
            console.log('\t--[boot] Cannot perform a migration due to missing source database (MySQL) connection string');
            console.log('\t--[boot] Please, specify source database (MySQL) connection string, and run the tool again');
            reject();
        }
        
        if (self._config.target === undefined) {
            console.log('\t--[boot] Cannot perform a migration due to missing target database (PostgreSQL) connection string');
            console.log('\t--[boot] Please, specify target database (PostgreSQL) connection string, and run the tool again');
            reject();
        }
        
        self._sourceConString     = self._config.source;
        self._targetConString     = self._config.target;
        self._tempDirPath         = self._config.tempDirPath;
        self._logsDirPath         = self._config.logsDirPath;
        self._dataTypesMapAddr    = self._config.dataTypesMapAddr;
        self._allLogsPath         = self._logsDirPath + '/all.log';
        self._reportOnlyPath      = self._logsDirPath + '/report-only.log';
        self._errorLogsPath       = self._logsDirPath + '/errors-only.log';
        self._notCreatedViewsPath = self._logsDirPath + '/not_created_views';
        self._encoding            = self._config.encoding === undefined ? 'utf-8' : self._config.encoding;
        self._dataChunkSize       = self._config.data_chunk_size === undefined ? 10 : +self._config.data_chunk_size;
        self._dataChunkSize       = self._dataChunkSize < 1 ? 1 : self._dataChunkSize;
        self._mysql               = null;
        self._pgsql               = null;
        self._tablesToMigrate     = [];
        self._viewsToMigrate      = [];
        self._summaryReport       = [];
        self._tablesCnt           = 0;
        self._viewsCnt            = 0;
        self._mySqlDbName         = self._sourceConString.database;
        self._schema              = self._config.schema === undefined || 
                                    self._config.schema === '' 
                                    ? self._mySqlDbName 
                                    : self._config.schema;
        
        self._maxPoolSizeSource   = self._config.max_pool_size_source !== undefined && 
                                    self.isIntNumeric(self._config.max_pool_size_source) 
                                    ? +self._config.max_pool_size_source 
                                    : 10;
        
        self._maxPoolSizeTarget   = self._config.max_pool_size_target !== undefined && 
                                    self.isIntNumeric(self._config.max_pool_size_target) 
                                    ? +self._config.max_pool_size_target 
                                    : 10;
        
        self._maxPoolSizeSource   = self._maxPoolSizeSource > 0 ? self._maxPoolSizeSource : 10;
        self._maxPoolSizeTarget   = self._maxPoolSizeTarget > 0 ? self._maxPoolSizeTarget : 10;
        
        var targetConString = 'postgresql://' + self._targetConString.user + ':' + self._targetConString.password 
                            + '@' + self._targetConString.host + ':' + self._targetConString.port + '/' 
                            + self._targetConString.database + '?client_encoding=' + self._targetConString.charset;
        
        self._targetConString = targetConString;
        pg.defaults.poolSize  = self._maxPoolSizeTarget;
        resolve(self);
    }).then(
        self.readDataTypesMap
    ).then(
        function() {
            return new Promise(function(resolveBoot, rejectBoot) {
                console.log('\t--[boot] Boot is accomplished...');
                resolveBoot(self);
            });
        }, 
        function() {
            console.log('\t--[boot] Cannot parse JSON from' + self._dataTypesMapAddr + '\t--[Boot] Boot failed.');
        }
    );
};

/**
 * Checks if given value is integer number.
 * 
 * @param   {String|Number} value
 * @returns {Boolean}
 */
FromMySQL2PostgreSQL.prototype.isIntNumeric = function(value) {
    return !isNaN(parseInt(value)) && isFinite(value);
};

/**
 * Returns a clone of given object.
 * 
 * @param   {Object} obj
 * @returns {Object}
 */
FromMySQL2PostgreSQL.prototype.clone = function(obj) {
    var clone = {};
    
    for (var attr in obj) {
        clone[attr] = obj[attr];
    }
    
    return clone;
};

/**
 * Converts MySQL data types to corresponding PostgreSQL data types.
 * This conversion performs in accordance to mapping rules in './DataTypesMap.json'.
 * './DataTypesMap.json' can be customized.
 * 
 * @param   {Object} objDataTypesMap
 * @param   {String} mySqlDataType
 * @returns {String}
 */
FromMySQL2PostgreSQL.prototype.mapDataTypes = function(objDataTypesMap, mySqlDataType) {
    var retVal               = '';
    var arrDataTypeDetails   = mySqlDataType.split(' ');
    mySqlDataType            = arrDataTypeDetails[0].toLowerCase();
    var increaseOriginalSize = arrDataTypeDetails.indexOf('unsigned') !== -1 
                               || arrDataTypeDetails.indexOf('zerofill') !== -1;
    
    if (mySqlDataType.indexOf('(') === -1) {
        // No parentheses detected.   
        retVal = increaseOriginalSize 
                 ? objDataTypesMap[mySqlDataType].increased_size 
                 : objDataTypesMap[mySqlDataType].type;
        
    } else {
        // Parentheses detected.
        var arrDataType = mySqlDataType.split('(');
        var strDataType = arrDataType[0].toLowerCase();
        
        if ('enum' === strDataType) {
            retVal = 'varchar(255)';
        } else if ('decimal' === strDataType || 'numeric' === strDataType) {
            retVal = objDataTypesMap[strDataType].type + '(' + arrDataType[1];
        } else if ('decimal(19,2)' === mySqlDataType || objDataTypesMap[strDataType].mySqlVarLenPgSqlFixedLen) {
            // Should be converted without a length definition.
            retVal = increaseOriginalSize 
                     ? objDataTypesMap[strDataType].increased_size 
                     : objDataTypesMap[strDataType].type;
        } else {
            // Should be converted with a length definition.
            retVal = increaseOriginalSize 
                     ? objDataTypesMap[strDataType].increased_size + '(' + arrDataType[1] 
                     : objDataTypesMap[strDataType].type + '(' + arrDataType[1];
        }
    }
    
    // Prevent incompatible length (CHARACTER(0) or CHARACTER VARYING(0)).
    if (retVal === 'character(0)') {
            retVal = 'character(1)';
    } else if (retVal === 'character varying(0)') {
            retVal = 'character varying(1)';
    }
    
    return retVal.toUpperCase();
};

/**
 * Creates temporary directory.
 *  
 * @param   {FromMySQL2PostgreSQL} self 
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.createTemporaryDirectory = function(self) {
    return new Promise(function(resolve, reject) {
        self.log(self, '\t--[createTemporaryDirectory] Creating temporary directory...');
        fs.stat(self._tempDirPath, function(directoryDoesNotExist, stat) {
            if (directoryDoesNotExist) {
                fs.mkdir(self._tempDirPath, self._0777, function(e) {
                    if (e) {
                        self.log(self, 
                            '\t--[createTemporaryDirectory] Cannot perform a migration due to impossibility to create ' 
                            + '"temporary_directory": ' + self._tempDirPath
                        );
                        reject();
                    } else {
                        self.log(self, '\t--[createTemporaryDirectory] Temporary directory is created...');
                        resolve(self);
                    }
                });
                
            } else if (!stat.isDirectory()) {
                self.log(self, '\t--[createTemporaryDirectory] Cannot perform a migration due to unexpected error');
                reject();
                
            } else {
                self.log(self, '\t--[createTemporaryDirectory] Temporary directory already exists...');
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
        console.log('\t--[createLogsDirectory] Creating logs directory...');
        fs.stat(self._logsDirPath, function(directoryDoesNotExist, stat) {
            if (directoryDoesNotExist) {
                fs.mkdir(self._logsDirPath, self._0777, function(e) {
                    if (e) {
                        console.log( 
                            '\t--[createLogsDirectory] Cannot perform a migration due to impossibility to create ' 
                            + '"logs_directory": ' + self._logsDirPath
                        );
                        reject();
			
                    } else {
                        self.log(self, '\t--[createLogsDirectory] Logs directory is created...');
                        resolve(self);
                    }
                });
                
            } else if (!stat.isDirectory()) {
                console.log('\t--[createLogsDirectory] Cannot perform a migration due to unexpected error');
                reject();
                
            } else {
                self.log(self, '\t--[createLogsDirectory] Logs directory already exists...');
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
 * @param   {String}               log
 * @param   {Boolean}              isErrorLog
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
                if (!error) {
                    self._allLogsPathFd = fd;
                    fs.write(self._allLogsPathFd, buffer, 0, buffer.length, null, function(error) {
                        resolve(self);
                    });
                    
                } else {
                    resolve(self);
                }
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
 * @param   {String}               message
 * @param   {String}               sql
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.generateError = function(self, message, sql) {
    return new Promise(function(resolve, reject) {
        message    += sql === undefined ? '' : '\n\tSQL: ' + sql + '\n\n';
        var buffer  = new Buffer(message);
        self.log(self, message, true);
        
        if (self._errorLogsPathFd === undefined) {
            fs.open(self._errorLogsPath, 'a', self._0777, function(error, fd) {
                if (!error) {
                    self._errorLogsPathFd = fd;
                    fs.write(self._errorLogsPathFd, buffer, 0, buffer.length, null, function(error) {
                        resolve(self);
                    });
                    
                } else {
                    resolve(self);
                }
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
    return new Promise(function(resolve, reject) {
        // Check if MySQL server is connected.
        // If not connected - connect.
        if (!self._mysql) {
            self._sourceConString.connectionLimit = self._maxPoolSizeSource;
            var pool                              = mysql.createPool(self._sourceConString);
            
            if (pool) {
                self._mysql = pool;
                resolve(self);
            } else {
                self.log(self, '\t--[connect] Cannot connect to MySQL server...');
                reject(self);
            }
            
        } else {
            resolve(self);
        }
    });
};

/**
 * Create a new database schema.
 * Insure a uniqueness of a new schema name.
 * 
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.createSchema = function(self) {
    return new Promise(function(resolve, reject) {
        var sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '" + self._schema + "';";
        pg.connect(self._targetConString, function(error, client, done) {
            if (error) {
                done();
                self.generateError(self, '\t--[createSchema] Cannot connect to PostgreSQL server...', sql);
                reject();
            } else {
                client.query(sql, function(err, result) {
                    if (err) {
                        done();
                        self.generateError(self, '\t--[createSchema] Error running PostgreSQL query:', sql);
                        reject();
                    } else if (result.rows.length === 0) {
                        // If 'self._schema !== 0' (schema is defined and already exists), then no need to create it.
                        // Such schema will be just used...
                        sql = 'CREATE SCHEMA "' + self._schema + '";';
                        client.query(sql, function(err) {
                            done();
                            
                            if (err) {
                                self.generateError(self, '\t--[createSchema] Error running PostgreSQL query: ', sql);
                                reject();
                            } else {
                                resolve(self);
                            }
                        });
                        
                    } else {
                        resolve(self);
                    }
                });
            }
        });
    });
};

/**
 * Load source tables and views, that need to be migrated.
 * 
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.loadStructureToMigrate = function(self) {
    return new Promise(function(resolve, reject) {
        resolve(self);
    }).then(
        self.connect, 
        function() {
            self.log(self, '\t--[loadStructureToMigrate] Cannot establish DB connections...');
        }
        
    ).then(
        function(self) {
            return new Promise(function(resolve, reject) {
                var sql = 'SHOW FULL TABLES IN `' + self._mySqlDbName + '`;';
                self._mysql.getConnection(function(error, connection) {
                    if (error) {
                        // No connection.
                        self.log(self, '\t--[loadStructureToMigrate] Cannot connect to MySQL server...');
                        reject();
                    } else {
                        connection.query(sql, function(strErr, rows) {
                            connection.release();
                            
                            if (strErr) {
                                self.generateError(self, '\t--[loadStructureToMigrate] Error running MySQL query:', sql);
                                reject();
                            } else {
                                var tablesCnt            = 0;
                                var viewsCnt             = 0;
                                var processTablePromises = [];
                                var createViewPromises   = [];
				
                                for (var i = 0; i < rows.length; i++) {
                                    if (rows[i].Table_type === 'BASE TABLE') {
                                        self._tablesToMigrate.push(rows[i]);
                                        tablesCnt++;
                                        processTablePromises.push(
                                            self.processTable(self, rows[i]['Tables_in_' + self._mySqlDbName])
                                        );
                                        
                                    } else if (rows[i].Table_type === 'VIEW') {
                                        self._viewsToMigrate.push(rows[i]);
                                        viewsCnt++;
                                    }
                                }
                                
                                self._tablesCnt = tablesCnt;
                                self._viewsCnt  = viewsCnt;
                                var message     = '\t--[loadStructureToMigrate] Source DB structure is loaded...\n' 
                                                + '\t--[loadStructureToMigrate] Tables to migrate: ' + tablesCnt + '\n' 
                                                + '\t--[loadStructureToMigrate] Views to migrate: ' + viewsCnt;
                                
                                self.log(self, message);
                                
                                Promise.all(processTablePromises).then(
                                    function(self) {
                                        resolve(self);
                                    }, 
                                    function() {
                                        reject();
                                    }
                                );
                            }
                        });
                    }
                });
            });
        }
    );
};

/**
 * Migrates structure of a single table to PostgreSql server.
 * 
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.createTable = function(self) {
    return new Promise(function(resolve, reject) {
        resolve(self);
    }).then(
        self.connect 
    ).then(
        function(self) {
            return new Promise(function(resolveCreateTable, rejectCreateTable) {
                self.log(self, '\t--[createTable] Currently creating table: `' + self._clonedSelfTableName + '`');
                var sql = 'SHOW COLUMNS FROM `' + self._clonedSelfTableName + '`;';
                self._mysql.getConnection(function(error, connection) {
                    if (error) {
                        // No connection.
                        self.log(self, '\t--[createTable] Cannot connect to MySQL server...');
                        rejectCreateTable();
                    } else {
                        connection.query(sql, function(err, rows) {
                            connection.release();
                            
                            if (err) {
                                self.generateError(self, '\t--[createTable] Error running MySQL query:', sql);
                                rejectCreateTable();
                            } else {
                                sql = 'CREATE TABLE "' + self._schema + '"."' + self._clonedSelfTableName + '"(';
                                
                                for (var i = 0; i < rows.length; i++) {
                                    sql += '"' + rows[i].Field + '" ' 
                                        +  self.mapDataTypes(self._dataTypesMap, rows[i].Type) + ',';
                                }
                                
                                sql = sql.slice(0, -1) + ');';
				
                                pg.connect(self._targetConString, function(error, client, done) {
                                    if (error) {
                                        done();
                                        self.generateError(self, '\t--[createTable] Cannot connect to PostgreSQL server...', sql);
                                        rejectCreateTable();
                                    } else {
                                        client.query(sql, function(err) {
                                            done();
                                            
                                            if (err) {
                                                self.generateError(self, '\t--[createTable] Error running PostgreSQL query: ', sql);
                                                rejectCreateTable();
                                            } else {
                                                self.log(self, '\t--[createTable] Table "' + self._schema + '"."' + self._clonedSelfTableName + '" is created...');
                                                resolveCreateTable(self);
                                            }
                                        });
                                    }
                                });
                            }
                        });
                    }
                });
            });
        }, 
        function() {
            self.log(self, '\t--[createTable] Cannot establish DB connections...');
        }
    );
};

/**
 * Runs migration process for given table.
 * 
 * @param   {FromMySQL2PostgreSQL} self 
 * @param   {String}               tableName 
 * @returns {Promise} 
 */
FromMySQL2PostgreSQL.prototype.processTable = function(self, tableName) {
    return new Promise(function(resolve, reject) {
        self                      = self.clone(self);
        self._clonedSelfTableName = tableName;
        resolve(self);
	
    }).then(
        self.connect 
    ).then(
        self.createTable, 
        function() {
            self.log(self, '\t--[processTable] Cannot establish DB connections...');
        }
	
    ).then(
        function(self) {
            delete self._clonedSelfTableName;
        }, 
        function() {
            self.log(self, '\t--[processTable] Temporary error hadler...');
        }
    );
};

/**
 * Runs migration according to user's configuration.
 * 
 * @param   {Object} config
 * @returns {undefined} 
 */
FromMySQL2PostgreSQL.prototype.run = function(config) {
    var self     = this;
    self._config = config;
    var promise  = new Promise(function(resolve, reject) {
        resolve(self);
    });
    
    promise.then(
        self.boot
    ).then(
        self.createLogsDirectory,
        function() {
            console.log('\t--[run] Failed to boot migration');
        }
	
    ).then(
        self.createTemporaryDirectory,
        function() {
            self.log(self, '\t--[run] Logs directory was not created...');
        }
	
    ).then(
        self.createSchema, 
        function() {
            self.log(self, '\t--[run] Temporary directory was not created...');
        }
	
    ).then(
        self.loadStructureToMigrate, 
        function() {
            self.log(self, '\t--[run] Cannot create new DB schema...');
        }
        
    ).then(
        function() {
            self.log(self, '\t--[run] NMIG migration is accomplished.');
        }, 
        function() {
            self.log(self, '\t--[run] Cannot load source database structure...');
        }
    );
};

module.exports.FromMySQL2PostgreSQL = FromMySQL2PostgreSQL;


// node C:\xampp\htdocs\nmig\main.js 
// http://stackoverflow.com/questions/6731214/node-mysql-connection-pooling 

///////////////////////////////////////////////////////////////////////////////////

/*var mysql = require('mysql');
var pool  = mysql.createPool({
    connectionLimit : 10,
    host            : 'example.org',
    user            : 'bob',
    password        : 'secret'
});*/

///////////////////////////////////////////////////////////////////////////////////

/*var pg = require('pg');
pg.defaults.poolSize = 25;
//pool is created on first call to pg.connect
pg.connect(function(err, client, done) {
    done();
});*/

///////////////////////////////////////////////////////////////////////////////////

// TEST MySQL START ///////////////////////////////////////////////////////////////
/*self._mysql.getConnection(function(error, connection) {
	if (error) {
		self.log(self, '\t--Cannot connect to MySQL server...');
		mysqlReject();
	} else {
		var sql = 'SELECT * FROM `admins`';
		connection.query(sql, function(strErr, rows) {
			if (strErr) {
				self.generateError(self, strErr, sql);
			} else {
				rows.forEach(function(objRow) {
					console.log('MYSQL');
					console.log(JSON.stringify(objRow));
				});
			}
			// Release connection back to the pool.
			connection.release();
			mysqlResolve(self);
		});
	}
});*/
// TEST MySQL END ///////////////////////////////////////////////////////

// TEST PostgreSQL START ///////////////////////////////////////////////////////////////////
/*pg.connect(self._targetConString, function(error, client, done) {
	if (error) {
		return console.error('error fetching client from pool', error);
	}
	// TEST.
	client.query('SELECT $1::int AS number', ['3'], function(err, result) {
		//call `done()` to release the client back to the pool
		done();
		if (err) {
			return console.error('error running query', err);
		}
		console.log('PGSQL Output3: ' + result.rows[0].number);
	});
});*/
// TEST PostgreSQL END //////////////////////////////////////////////////////////////////

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

