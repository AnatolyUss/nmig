/* 
 * This file is a part of "NMIG" - the database migration tool.
 * 
 * Copyright 2015 Anatoly Khaytovich <anatolyuss@gmail.com>
 * 
 * @author Anatoly Khaytovich <anatolyuss@gmail.com>  
 */
'use strict';
var fs           = require('fs');
var pg           = require('pg');
var mysql        = require('mysql');
var csvStringify = require('csv-stringify');

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
        self._timeBegin           = new Date();
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
            return new Promise(function(resolveBoot) {
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
 * Removes temporary directory.
 * 
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.removeTemporaryDirectory = function(self) {
    return new Promise(function(resolve) {
        fs.rmdir(self._tempDirPath, function(error) {
            var msg;
            
            if (error) {
                msg = '\t--[removeTemporaryDirectory] Note, TemporaryDirectory located at "' + self._tempDirPath + '" is nor removed';
            } else {
                msg = '\t--[removeTemporaryDirectory] TemporaryDirectory located at "' + self._tempDirPath + '" is removed';
            }
            
            self.log(self, msg);
            resolve(self);
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
 * Writes given log to the "/all.log" file.
 * If necessary, writes given log to the "/self._clonedSelfTableName.log" file.
 * 
 * @param   {FromMySQL2PostgreSQL} self
 * @param   {String}               log
 * @param   {Boolean}              isErrorLog
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.log = function(self, log, isErrorLog) {
    var buffer = new Buffer(log + '\n\n');
    
    return new Promise(function(resolve) {
        if (isErrorLog === undefined || isErrorLog === false) {
            console.log(log);
        }
        
        if (self._allLogsPathFd === undefined) {
            fs.open(self._allLogsPath, 'a', self._0777, function(error, fd) {
                if (!error) {
                    self._allLogsPathFd = fd;
                    fs.write(self._allLogsPathFd, buffer, 0, buffer.length, null, function() {
                        resolve(self);
                    });
                } else {
                    resolve(self);
                }
            });
        } else {
            fs.write(self._allLogsPathFd, buffer, 0, buffer.length, null, function() {
                resolve(self);
            });
        }
    }).then(
        function() {
            return new Promise(function(resolveTableLog) {
                if (self._clonedSelfTableNamePath === undefined) {
                    resolveTableLog(self);
                } else if (self._clonedSelfTableNamePathFd === undefined) {
                    fs.open(self._clonedSelfTableNamePath, 'a', self._0777, function(error, fd) {
                        if (!error) {
                            self._clonedSelfTableNamePathFd = fd;
                            fs.write(self._clonedSelfTableNamePathFd, buffer, 0, buffer.length, null, function() {
                                resolveTableLog(self);
                            });
                        } else {
                            resolveTableLog(self);
                        }
                    });
                } else {
                    fs.write(self._clonedSelfTableNamePathFd, buffer, 0, buffer.length, null, function() {
			resolveTableLog(self);
                    });
		}
            });
	}
    );
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
    return new Promise(function(resolve) {
        message    += sql === undefined ? '' : '\n\tSQL: ' + sql + '\n\n';
        var buffer  = new Buffer(message);
        self.log(self, message, true);
        
        if (self._errorLogsPathFd === undefined) {
            fs.open(self._errorLogsPath, 'a', self._0777, function(error, fd) {
                if (!error) {
                    self._errorLogsPathFd = fd;
                    fs.write(self._errorLogsPathFd, buffer, 0, buffer.length, null, function() {
                        resolve(self);
                    });
                    
                } else {
                    resolve(self);
                }
            });
            
        } else {
            fs.write(self._errorLogsPathFd, buffer, 0, buffer.length, null, function() {
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
                self.generateError(self, '\t--[createSchema] Cannot connect to PostgreSQL server...\n' + error, sql);
                reject();
            } else {
                client.query(sql, function(err, result) {
                    if (err) {
                        done();
                        self.generateError(self, '\t--[createSchema] ' + err, sql);
                        reject();
                    } else if (result.rows.length === 0) {
                        // If 'self._schema !== 0' (schema is defined and already exists), then no need to create it.
                        // Such schema will be just used...
                        sql = 'CREATE SCHEMA "' + self._schema + '";';
                        client.query(sql, function(err) {
                            done();
                            
                            if (err) {
                                self.generateError(self, '\t--[createSchema] ' + err, sql);
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
    return new Promise(function(resolve) {
        resolve(self);
    }).then(
        self.connect, 
        function() {
            self.log(self, '\t--[loadStructureToMigrate] Cannot establish DB connections...');
        }
    ).then(
        function() {
            return new Promise(function(resolve, reject) {
                var sql = 'SHOW FULL TABLES IN `' + self._mySqlDbName + '`;';
                self._mysql.getConnection(function(error, connection) {
                    if (error) {
                        connection.release();
                        self.log(self, '\t--[loadStructureToMigrate] Cannot connect to MySQL server...\n' + error);
                        reject();
                    } else {
                        connection.query(sql, function(strErr, rows) {
                            connection.release();
                            
                            if (strErr) {
                                self.generateError(self, '\t--[loadStructureToMigrate] ' + strErr, sql);
                                reject();
                            } else {
                                var tablesCnt            = 0;
                                var viewsCnt             = 0;
                                var processTablePromises = [];
                                var createViewPromises   = [];
				
                                for (var i = 0; i < rows.length; i++) {
                                    if (i < 3) // TODO.
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
                                
                                Promise.all(
                                    processTablePromises
                                ).then(
                                    function() {
                                        resolve(self);
                                    },
                                    function() {
                                        reject();
                                    }
                                ).then(
                                    // TODO: treat FKs, views etc....
                                    function() {
                                        resolve(self);
                                    }, 
                                    // TODO: probably one o tables was not created, do not create FKs, views etc....
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
    return new Promise(function(resolve) {
        resolve(self);
    }).then(
        self.connect 
    ).then(
        function() {
            return new Promise(function(resolveCreateTable, rejectCreateTable) {
                self.log(self, '\t--[createTable] Currently creating table: `' + self._clonedSelfTableName + '`');
                var sql = 'SHOW COLUMNS FROM `' + self._clonedSelfTableName + '`;';
                self._mysql.getConnection(function(error, connection) {
                    if (error) {
                        connection.release();
                        self.log(self, '\t--[createTable] Cannot connect to MySQL server...\n' + error);
                        rejectCreateTable();
                    } else {
                        connection.query(sql, function(err, rows) {
                            connection.release();
                            
                            if (err) {
                                self.generateError(self, '\t--[createTable] ' + err, sql);
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
                                        self.generateError(self, '\t--[createTable] Cannot connect to PostgreSQL server...\n' + error, sql);
                                        rejectCreateTable();
                                    } else {
                                        client.query(sql, function(err) {
                                            done();
                                            
                                            if (err) {
                                                self.generateError(self, '\t--[createTable] ' + err, sql);
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
 * Populates given table.
 *  
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.populateTable = function(self) {
    return new Promise(function(resolve) {
        resolve(self);
    }).then(
        self.connect 
    ).then(
        function() {
            return new Promise(function(resolvePopulateTable, rejectPopulateTable) {
                self.log(self, '\t--[populateTable] Currently populating table: `' + self._clonedSelfTableName + '`');
                
                // Determine current table size, apply "chunking".
                var sql = "SELECT ((data_length + index_length) / 1024 / 1024) AS size_in_mb "
                        + "FROM information_schema.TABLES "
                        + "WHERE table_schema = '" + self._mySqlDbName + "' "
                        + "AND table_name = '" + self._clonedSelfTableName + "';";
                
                self._mysql.getConnection(function(error, connection) {
                    if (error) {
                        connection.release();
                        self.log(self, '\t--[populateTable] Cannot connect to MySQL server...\n\t' + error);
                        rejectPopulateTable();
                    } else {
                        connection.query(sql, function(err, rows) {
                            if (err) {
                                connection.release();
                                self.generateError(self, '\t--[populateTable] ' + err, sql);
                                rejectPopulateTable();
                            } else {
                                var tableSizeInMb = rows[0].size_in_mb;
                                tableSizeInMb     = tableSizeInMb < 1 ? 1 : tableSizeInMb;
                                
                                sql = 'SELECT COUNT(1) AS rows_count FROM `' + self._clonedSelfTableName + '`;';
                                connection.query(sql, function(err2, rows2) {
                                    connection.release();
                                    
                                    if (err2) {
                                        self.generateError(self, '\t--[populateTable] ' + err2, sql);
                                        rejectPopulateTable();
                                    } else {
                                        var rowsCnt              = rows2[0].rows_count;
                                        var chunksCnt            = tableSizeInMb / self._dataChunkSize;
                                        chunksCnt                = chunksCnt < 1 ? 1 : chunksCnt;
                                        var rowsInChunk          = Math.ceil(rowsCnt / chunksCnt);
                                        var populateTableWorkers = [];
                                        var msg                  = '\t--[populateTable] Total rows to insert into ' 
                                                                 + '"' + self._schema + '"."' 
                                                                 + self._clonedSelfTableName + '": ' + rowsCnt;
                                        
                                        self.log(self, msg);
                                        
                                        for (var offset = 0; offset < rowsCnt; offset += rowsInChunk) {
                                            populateTableWorkers.push(
                                                self.populateTableWorker(self, offset, rowsInChunk, rowsCnt)
                                            );
                                        }
                                        
                                        Promise.all(populateTableWorkers).then(
                                            function() {
                                                resolvePopulateTable(self);
                                            }, 
                                            function() {
                                                rejectPopulateTable();
                                            }
                                        );
                                    }
                                });
                            }
                        });
                    }
                });
            });
        }, 
        function() {
            self.log(self, '\t--[populateTable] Cannot establish DB connections...');
        }
    );
};

/**
 * Load a chunk of data using "PostgreSQL COPY".
 * 
 * @param   {FromMySQL2PostgreSQL} self
 * @param   {Number}               offset
 * @param   {Number}               rowsInChunk
 * @param   {Number}               rowsCnt
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.populateTableWorker = function(self, offset, rowsInChunk, rowsCnt) {
    return new Promise(function(resolve) {
        resolve(self);
    }).then(
        self.connect
    ).then(
        function() {
            return new Promise(function(resolvePopulateTableWorker) {
                var csvAddr = self._tempDirPath + '/' + self._clonedSelfTableName + offset + '.csv';
                var sql     = 'SELECT * FROM `' + self._clonedSelfTableName + '` LIMIT ' + offset + ',' + rowsInChunk + ';';
                
                self._mysql.getConnection(function(error, connection) {
                    if (error) {
                        connection.release();
                        self.log(self, '\t--[populateTableWorker] Cannot connect to MySQL server...\n\t' + error);
                        resolvePopulateTableWorker(self);
                    } else {
                        connection.query(sql, function(err, rows) {
                            connection.release();
                            
                            if (err) {
                                self.generateError(self, '\t--[populateTableWorker] ' + err, sql);
                                resolvePopulateTableWorker(self);
                            } else {
                                // Loop through current result set.
                                // Sanitize records.
                                // When sanitized - write them to a csv file.
                                rowsInChunk          = rows.length; // Must check amount of rows BEFORE sanitizing.
				var sanitizedRecords = [];
				
                                for (var cnt = 0; cnt < rows.length; cnt++) {
                                    var sanitizedRecord = Object.create(null);
                                    
                                    for (var attr in rows[cnt]) {
                                        sanitizedRecord[attr] = self.sanitizeValue(rows[cnt][attr]);
                                    }
                                    
                                    sanitizedRecords.push(sanitizedRecord);
                                }
				
                                csvStringify(sanitizedRecords, function(csvError, csvString) {
                                    var buffer = new Buffer(csvString);
                                    
                                    if (csvError) {
                                        self.generateError(self, '\t--[populateTableWorker] ' + csvError);
                                        resolvePopulateTableWorker(self);
                                    } else {
                                        fs.open(csvAddr, 'a', self._0777, function(csvErrorFputcsvOpen, fd) {
                                            if (csvErrorFputcsvOpen) {
                                                self.generateError(self, '\t--[populateTableWorker] ' + csvErrorFputcsvOpen);
                                                resolvePopulateTableWorker(self);
                                            } else {
                                                fs.write(fd, buffer, 0, buffer.length, null, function(csvErrorFputcsvWrite) {
                                                    if (csvErrorFputcsvWrite) {
                                                        self.generateError(self, '\t--[populateTableWorker] ' + csvErrorFputcsvWrite);
                                                        resolvePopulateTableWorker(self);
                                                    } else {
                                                        pg.connect(self._targetConString, function(error, client, done) {
                                                            if (error) {
                                                                done();
                                                                self.generateError(self, '\t--[populateTableWorker] Cannot connect to PostgreSQL server...\n' + error, sql);
                                                                resolvePopulateTableWorker(self);
                                                            } else {
                                                                sql = 'COPY "' + self._schema + '"."' + self._clonedSelfTableName + '" FROM '
                                                                    + '\'' + csvAddr + '\' DELIMITER \'' + ',\'' + ' CSV;';
                                                                
                                                                client.query(sql, function(err, result) {
                                                                    done();
								    
                                                                    if (err) {
									self.generateError(self, '\t--[populateTableWorker] ' + err, sql);
                                                                        resolvePopulateTableWorker(self);
                                                                    } else {
                                                                        self._totalRowsInserted += result.rowCount;
                                                                        var msg                  = '\t--[populateTableWorker]  For now inserted: ' + self._totalRowsInserted + ' rows, '
                                                                                                 + 'Total rows to insert into "' + self._schema + '"."' + self._clonedSelfTableName + '": ' + rowsCnt;
                                                                        
                                                                        self.log(self, msg);
                                                                        
                                                                        fs.unlink(csvAddr, function() {
                                                                            fs.close(fd, function() {
                                                                                resolvePopulateTableWorker(self);
                                                                            });
                                                                        });
                                                                    }
								});
                                                            }
							});
                                                    }
                                                });
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
            self.log(self, '\t--[populateTableWorker] Cannot establish DB connections...');
        }
    );
};

/**
 * Sanitize an input value.
 * 
 * @param   {String} value
 * @returns {String}
 */
FromMySQL2PostgreSQL.prototype.sanitizeValue = function(value) {
    if (value === '0000-00-00' || value === '0000-00-00 00:00:00') {
        return '-INFINITY';
    } else {
        return value;
    }
};

/**
 * Runs migration process for given table.
 * 
 * @param   {FromMySQL2PostgreSQL} self 
 * @param   {String}               tableName 
 * @returns {Promise} 
 */
FromMySQL2PostgreSQL.prototype.processTable = function(self, tableName) {
    return new Promise(function(resolve) {
        self                          = self.clone(self);
        self._clonedSelfTableName     = tableName;
        self._totalRowsInserted       = 0;
        self._clonedSelfTableNamePath = self._logsDirPath + '/' + tableName + '.log';
        resolve(self);
    }).then(
        self.connect 
    ).then(
        self.createTable, 
        function() {
            self.log(self, '\t--[processTable] Cannot establish DB connections...');
        }
    ).then(
        self.populateTable,  // TODO: Promise chain MUST continue, even after unsuccessfull 'self.populateTable'.
        function() {
            self.log(self, '\t--[processTable] Cannot create table "' + self._schema + '"."' + self._clonedSelfTableName + '"...');
            self.cleanupLocal(self);
        }
    ).then(
        self.cleanupLocal
    );
};

/**
 * Cleanup resources related to given table.
 * 
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.cleanupLocal = function(self) {
    return new Promise(function(resolve) {
        if (self._clonedSelfTableNamePathFd === undefined) {
            self.log(self, '\t--[cleanupLocal] Finished processing table `' + self._clonedSelfTableName + '`...');
            resolve(self);
        } else {
            fs.close(self._clonedSelfTableNamePathFd, function() {
                self.log(self, '\t--[cleanupLocal] Finished processing table `' + self._clonedSelfTableName + '`...');
                resolve(self);
            });
        }
    });
};

/**
 * Closes DB connections.
 * 
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.closeConnections = function(self) {
    return new Promise(function(resolve) {
        if (self._mysql) {
            self._mysql.end(function(error) {
                if (error) {
                    self.log(self, '\t--[closeConnections] ' + error);
                }
                
                self.log(self, '\t--[closeConnections] All DB connections to both MySQL and PostgreSQL servers have been closed...');
                pg.end();
                resolve(self);
            });
        } else {
            self.log(self, '\t--[closeConnections] All DB connections to both MySQL and PostgreSQL servers have been closed...');
            pg.end();
            resolve(self);
        }
    });
};

/**
 * Closes log files.
 * 
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.closeLogFiles = function(self) {
    return new Promise(function(resolveAllLogs) {
        if (self._allLogsPathFd) {
            fs.close(self._allLogsPathFd, function() {
                resolveAllLogs(self);
            });
        } else {
            resolveAllLogs(self);
        }
    }).then(
        function() {
            return new Promise(function(resolveErrorLogs) {
                if (self._errorLogsPathFd) {
                    fs.close(self._errorLogsPathFd, function() {
                        resolveErrorLogs(self);
                    });
                } else {
                    resolveErrorLogs(self);
                }
            });
        }
    );
};

/**
 * Closes DB connections and removes the "./temporary_directory".
 * 
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.cleanup = function(self) {
    return new Promise(function(resolve) {
        self.log(self, '\t--[cleanup] Cleanup resources...');
        resolve(self);
    }).then(
        self.removeTemporaryDirectory
    ).then(
        self.closeConnections
    ).then(
        self.closeLogFiles
    ).then(
        function() {
            return new Promise(function(resolve) {
		self.log(self, '\t--[cleanup] Cleanup finished...');
                resolve(self);
            });
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
    var promise  = new Promise(function(resolve) {
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
            return new Promise(function(resolveError) {
                resolveError(self);
            }).then(
                function() {
                    self.log(self, '\t--[run] Cannot create new DB schema...');
                    self.cleanup(self);
                }
            );
        }
    ).then(
        function() {
            return new Promise(function(resolve) {
                resolve(self);
            }).then(
                self.cleanup
            ).then(
                function() {
                    var timeTaken = (new Date()) - self._timeBegin;
                    var hours     = Math.floor(timeTaken / 1000 / 3600);
                    timeTaken    -= hours * 1000 * 3600;
                    var minutes   = Math.floor(timeTaken / 1000 / 60);
                    timeTaken    -= minutes * 1000 * 60;
                    var seconds   = Math.ceil(timeTaken / 1000);
                    hours         = hours < 10 ? '0' + hours : hours;
                    minutes       = minutes < 10 ? '0' + minutes : minutes;
                    seconds       = seconds < 10 ? '0' + seconds : seconds;
                    var endMsg    = '\t--[run] NMIG migration is accomplished.' 
                                  + '\n\t--[run] Total time: ' + hours + ':' + minutes + ':' + seconds 
                                  + '\n\t--[run] (hours:minutes:seconds)';
                    
                    self.log(self, endMsg);
                }
            );
        }, 
        function() {
            return new Promise(function(resolveErr) {
                resolveErr(self);
            }).then(
                function() {
                    self.cleanup(self);
                }
            ).then(
                function() {
                    var timeTaken = (new Date()) - self._timeBegin;
                    var hours     = Math.floor(timeTaken / 1000 / 3600);
                    timeTaken    -= hours * 1000 * 3600;
                    var minutes   = Math.floor(timeTaken / 1000 / 60);
                    timeTaken    -= minutes * 1000 * 60;
                    var seconds   = Math.ceil(timeTaken / 1000);
                    hours         = hours < 10 ? '0' + hours : hours;
                    minutes       = minutes < 10 ? '0' + minutes : minutes;
                    seconds       = seconds < 10 ? '0' + seconds : seconds;
                    var endMsg    = '\t--[run] NMIG cannot load source database structure.' 
                                  + '\n\t--[run] Total time: ' + hours + ':' + minutes + ':' + seconds 
                                  + '\n\t--[run] (hours:minutes:seconds)';
                    
                    self.log(self, endMsg);
                } 
            );
        }
    );
};

module.exports.FromMySQL2PostgreSQL = FromMySQL2PostgreSQL;


