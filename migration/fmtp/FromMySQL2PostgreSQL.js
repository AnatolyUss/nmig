/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright 2015 Anatoly Khaytovich <anatolyuss@gmail.com>
 *
 * @author Anatoly Khaytovich <anatolyuss@gmail.com>
 */
'use strict';
const fs           = require('fs');
const pg           = require('pg');
const mysql        = require('mysql');
const csvStringify = require('csv-stringify');

/**
 * Constructor.
 */
function FromMySQL2PostgreSQL() {
    // No code should be put here.
}

/**
 * Sets configuration parameters.
 *
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.boot = function(self) {
    return new Promise((resolve, reject) => {
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
        self._encoding            = self._config.encoding === undefined ? 'utf8' : self._config.encoding;
        self._dataChunkSize       = self._config.data_chunk_size === undefined ? 10 : +self._config.data_chunk_size;
        self._dataChunkSize       = self._dataChunkSize < 1 ? 1 : self._dataChunkSize;
        self._0777                = '0777';
        self._mysql               = null;
        self._pgsql               = null;
        self._tablesToMigrate     = [];
        self._viewsToMigrate      = [];
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

        let targetConString       = 'postgresql://' + self._targetConString.user + ':' + self._targetConString.password
                                  + '@' + self._targetConString.host + ':' + self._targetConString.port + '/'
                                  + self._targetConString.database + '?client_encoding=' + self._targetConString.charset;

        self._targetConString     = targetConString;
        pg.defaults.poolSize      = self._maxPoolSizeTarget;
        resolve(self);
    }).then(
        self.readDataTypesMap
    ).then(
        () => {
            return new Promise(resolveBoot => {
                console.log('\t--[boot] Boot is accomplished...');
                resolveBoot(self);
            });
        },
        () => console.log('\t--[boot] Cannot parse JSON from' + self._dataTypesMapAddr + '\t--[Boot] Boot failed.')
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
 * Checks if given value is float number.
 *
 * @param   {String|Number} value
 * @returns {Boolean}
 */
FromMySQL2PostgreSQL.prototype.isFloatNumeric = function(value) {
    return !isNaN(parseFloat(value)) && isFinite(value);
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
 * Converts MySQL data types to corresponding PostgreSQL data types.
 * This conversion performs in accordance to mapping rules in './DataTypesMap.json'.
 * './DataTypesMap.json' can be customized.
 *
 * @param   {Object} objDataTypesMap
 * @param   {String} mySqlDataType
 * @returns {String}
 */
FromMySQL2PostgreSQL.prototype.mapDataTypes = function(objDataTypesMap, mySqlDataType) {
    let retVal               = '';
    let arrDataTypeDetails   = mySqlDataType.split(' ');
    mySqlDataType            = arrDataTypeDetails[0].toLowerCase();
    let increaseOriginalSize = arrDataTypeDetails.indexOf('unsigned') !== -1
                               || arrDataTypeDetails.indexOf('zerofill') !== -1;

    if (mySqlDataType.indexOf('(') === -1) {
        // No parentheses detected.
        retVal = increaseOriginalSize
                 ? objDataTypesMap[mySqlDataType].increased_size
                 : objDataTypesMap[mySqlDataType].type;

    } else {
        // Parentheses detected.
        let arrDataType = mySqlDataType.split('(');
        let strDataType = arrDataType[0].toLowerCase();

        if ('enum' === strDataType) {
            retVal = 'character varying(255)';
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
    return new Promise((resolve, reject) => {
        fs.readFile(self._dataTypesMapAddr, (error, data) => {
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
    return new Promise((resolve, reject) => {
        self.log(self, '\t--[createTemporaryDirectory] Creating temporary directory...');
        fs.stat(self._tempDirPath, (directoryDoesNotExist, stat) => {
            if (directoryDoesNotExist) {
                fs.mkdir(self._tempDirPath, self._0777, e => {
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
                reject();
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
    return new Promise(resolve => {
        fs.rmdir(self._tempDirPath, error => {
            let msg;

            if (error) {
                msg = '\t--[removeTemporaryDirectory] Note, TemporaryDirectory located at "'
                    + self._tempDirPath + '" is not removed';
            } else {
                msg = '\t--[removeTemporaryDirectory] TemporaryDirectory located at "'
                    + self._tempDirPath + '" is removed';
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
    return new Promise((resolve, reject) => {
        console.log('\t--[createLogsDirectory] Creating logs directory...');
        fs.stat(self._logsDirPath, (directoryDoesNotExist, stat) => {
            if (directoryDoesNotExist) {
                fs.mkdir(self._logsDirPath, self._0777, e => {
                    if (e) {
                        let msg = '\t--[createLogsDirectory] Cannot perform a migration due to impossibility to create '
                                + '"logs_directory": ' + self._logsDirPath;

                        console.log(msg);
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
    let buffer = new Buffer(log + '\n\n', self._encoding);

    return new Promise(resolve => {
        if (isErrorLog === undefined || isErrorLog === false) {
            console.log(log);
        }

        fs.open(self._allLogsPath, 'a', self._0777, (error, fd) => {
            if (error) {
                resolve(self);
            } else {
                fs.write(fd, buffer, 0, buffer.length, null, () => {
                    fs.close(fd, () => resolve(self));
                });
            }
        });
    }).then(
        self => {
            return new Promise(resolveTableLog => {
                if (self._clonedSelfTableNamePath === undefined) {
                    resolveTableLog(self);
                } else {
                    fs.open(self._clonedSelfTableNamePath, 'a', self._0777, (error, fd) => {
                        if (error) {
                            resolveTableLog(self);
                        } else {
                            fs.write(fd, buffer, 0, buffer.length, null, () => {
                                fs.close(fd, () => resolveTableLog(self));
                            });
                        }
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
    return new Promise(resolve => {
        message    += '\n\n';
        message    += sql === undefined ? '' : '\n\tSQL: ' + sql + '\n\n';
        let buffer  = new Buffer(message, self._encoding);
        self.log(self, message, true);

        fs.open(self._errorLogsPath, 'a', self._0777, (error, fd) => {
            if (error) {
                resolve(self);
            } else {
                fs.write(fd, buffer, 0, buffer.length, null, () => {
                    fs.close(fd, () => resolve(self));
                });
            }
        });
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
    return new Promise((resolve, reject) => {
        // Check if MySQL server is connected.
        // If not connected - connect.
        if (!self._mysql) {
            self._sourceConString.connectionLimit = self._maxPoolSizeSource;
            let pool                              = mysql.createPool(self._sourceConString);

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
    return new Promise((resolve, reject) => {
        pg.connect(self._targetConString, (error, client, done) => {
            if (error) {
                done();
                self.generateError(self, '\t--[createSchema] Cannot connect to PostgreSQL server...\n' + error);
                reject();
            } else {
                let sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '" + self._schema + "';";
                client.query(sql, (err, result) => {
                    if (err) {
                        done();
                        self.generateError(self, '\t--[createSchema] ' + err, sql);
                        reject();
                    } else if (result.rows.length === 0) {
                        // If 'self._schema !== 0' (schema is defined and already exists), then no need to create it.
                        // Such schema will be just used...
                        sql = 'CREATE SCHEMA "' + self._schema + '";';
                        client.query(sql, err => {
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
    return new Promise(
        resolve => resolve(self)
    ).then(
        self.connect,
        () => self.log(self, '\t--[loadStructureToMigrate] Cannot establish DB connections...')
    ).then(
        self => {
            return new Promise((resolve, reject) => {
                self._mysql.getConnection((error, connection) => {
                    if (error) {
                        // The connection is undefined.
                        self.generateError(self, '\t--[loadStructureToMigrate] Cannot connect to MySQL server...\n' + error);
                        reject();
                    } else {
                        let sql = 'SHOW FULL TABLES IN `' + self._mySqlDbName + '`;';
                        connection.query(sql, (strErr, rows) => {
                            connection.release();

                            if (strErr) {
                                self.generateError(self, '\t--[loadStructureToMigrate] ' + strErr, sql);
                                reject();
                            } else {
                                let tablesCnt            = 0;
                                let viewsCnt             = 0;
                                let processTablePromises = [];
                                let createViewPromises   = [];

                                for (let i = 0; i < rows.length; i++) {
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
                                let message     = '\t--[loadStructureToMigrate] Source DB structure is loaded...\n'
                                                + '\t--[loadStructureToMigrate] Tables to migrate: ' + tablesCnt + '\n'
                                                + '\t--[loadStructureToMigrate] Views to migrate: ' + viewsCnt;

                                self.log(self, message);
                                Promise.all(processTablePromises).then(() => resolve(self), () => reject());
                            }
                        });
                    }
                });
            });
        }
    );
};

/**
 * Creates foreign keys.
 *
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.processForeignKey = function(self) {
    return new Promise(resolve => {
        let fkPromises = [];

        for (let i = 0; i < self._tablesToMigrate.length; i++) {
            fkPromises.push(
                new Promise(fkResolveConnection => {
                    let msg = '\t--[processForeignKey] Search foreign keys for table "' + self._schema + '"."' + self._tablesToMigrate[i] + '"...';
                    self.log(self, msg);
                    fkResolveConnection(self);
                }).then(
                    self.connect
                ).then(
                    self => {
                        return new Promise(fkResolve => {
                            self._mysql.getConnection((error, connection) => {
                                if (error) {
                                    // The connection is undefined.
                                    self.generateError(self, '\t--[processForeignKey] Cannot connect to MySQL server...\n' + error);
                                    fkResolve(self);
                                } else {
                                    let sql = "SELECT cols.COLUMN_NAME, refs.REFERENCED_TABLE_NAME, refs.REFERENCED_COLUMN_NAME, "
                                            + "cRefs.UPDATE_RULE, cRefs.DELETE_RULE, cRefs.CONSTRAINT_NAME "
                                            + "FROM INFORMATION_SCHEMA.`COLUMNS` AS cols "
                                            + "INNER JOIN INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` AS refs "
                                            + "ON refs.TABLE_SCHEMA = cols.TABLE_SCHEMA "
                                            + "AND refs.REFERENCED_TABLE_SCHEMA = cols.TABLE_SCHEMA "
                                            + "AND refs.TABLE_NAME = cols.TABLE_NAME "
                                            + "AND refs.COLUMN_NAME = cols.COLUMN_NAME "
                                            + "LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS cRefs "
                                            + "ON cRefs.CONSTRAINT_SCHEMA = cols.TABLE_SCHEMA "
                                            + "AND cRefs.CONSTRAINT_NAME = refs.CONSTRAINT_NAME "
                                            + "LEFT JOIN INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` AS links "
                                            + "ON links.TABLE_SCHEMA = cols.TABLE_SCHEMA "
                                            + "AND links.REFERENCED_TABLE_SCHEMA = cols.TABLE_SCHEMA "
                                            + "AND links.REFERENCED_TABLE_NAME = cols.TABLE_NAME "
                                            + "AND links.REFERENCED_COLUMN_NAME = cols.COLUMN_NAME "
                                            + "LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS cLinks "
                                            + "ON cLinks.CONSTRAINT_SCHEMA = cols.TABLE_SCHEMA "
                                            + "AND cLinks.CONSTRAINT_NAME = links.CONSTRAINT_NAME "
                                            + "WHERE cols.TABLE_SCHEMA = '" + self._mySqlDbName + "' "
                                            + "AND cols.TABLE_NAME = '" + self._tablesToMigrate[i] + "';";

                                    connection.query(sql, (err, rows) => {
                                        connection.release();
                                        
                                        if (err) {
                                            self.generateError(self, '\t--[processForeignKey] ' + err, sql);
                                            fkResolve(self);
                                        } else {
                                            pg.connect(self._targetConString, (error, client, done) => {
                                                if (error) {
                                                    done();
                                                    self.generateError(self, '\t--[processForeignKey] Cannot connect to PostgreSQL server...');
                                                    fkResolve(self);
                                                } else {
                                                    sql = '';
                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        });
                    },
                    () => {
                        return new Promise(resolveError => {
                            self.generateError(self, '\t--[processForeignKey] Cannot establish DB connections...');
                            resolveError(self);
                        });
                    }
                )
            );
        }

        Promise.all(fkPromises).then(() => resolve(self));
    });
};

/**
 * Runs "vacuum full" and "analyze".
 *
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.runVacuumFullAndAnalyze = function(self) {
    return new Promise(resolve => {
        resolve(self); // TODO: implement this method.
    });
};

/**
 * Migrates structure of a single table to PostgreSql server.
 *
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.createTable = function(self) {
    return new Promise(
        resolve => resolve(self)
    ).then(
        self.connect
    ).then(
        self => {
            return new Promise((resolveCreateTable, rejectCreateTable) => {
                self.log(self, '\t--[createTable] Currently creating table: `' + self._clonedSelfTableName + '`');
                self._mysql.getConnection((error, connection) => {
                    if (error) {
                        // The connection is undefined.
                        self.generateError(self, '\t--[createTable] Cannot connect to MySQL server...\n' + error);
                        rejectCreateTable();
                    } else {
                        let sql = 'SHOW COLUMNS FROM `' + self._clonedSelfTableName + '`;';
                        connection.query(sql, (err, rows) => {
                            connection.release();

                            if (err) {
                                self.generateError(self, '\t--[createTable] ' + err, sql);
                                rejectCreateTable();
                            } else {
                                pg.connect(self._targetConString, (error, client, done) => {
                                    if (error) {
                                        done();
                                        self.generateError(self, '\t--[createTable] Cannot connect to PostgreSQL server...\n' + error, sql);
                                        rejectCreateTable();
                                    } else {
                                        sql                          = 'CREATE TABLE "' + self._schema + '"."' + self._clonedSelfTableName + '"(';
                                        self._clonedSelfTableColumns = rows;

                                        for (let i = 0; i < rows.length; i++) {
                                            sql += '"' + rows[i].Field + '" '
                                                +  self.mapDataTypes(self._dataTypesMap, rows[i].Type) + ',';
                                        }

                                        sql = sql.slice(0, -1) + ');';
                                        client.query(sql, err => {
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
        () => self.generateError(self, '\t--[createTable] Cannot establish DB connections...')
    );
};

/**
 * Populates given table.
 *
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.populateTable = function(self) {
    return new Promise(
        resolve => resolve(self)
    ).then(
        self.connect
    ).then(
        self => {
            return new Promise(resolvePopulateTable => {
                self.log(self, '\t--[populateTable] Currently populating table: `' + self._clonedSelfTableName + '`');
                self._mysql.getConnection((error, connection) => {
                    if (error) {
                        // The connection is undefined.
                        self.generateError(self, '\t--[populateTable] Cannot connect to MySQL server...\n\t' + error);
                        resolvePopulateTable();
                    } else {
                        // Determine current table size, apply "chunking".
                        let sql = "SELECT ((data_length + index_length) / 1024 / 1024) AS size_in_mb "
                                + "FROM information_schema.TABLES "
                                + "WHERE table_schema = '" + self._mySqlDbName + "' "
                                + "AND table_name = '" + self._clonedSelfTableName + "';";

                        connection.query(sql, (err, rows) => {
                            if (err) {
                                connection.release();
                                self.generateError(self, '\t--[populateTable] ' + err, sql);
                                resolvePopulateTable();
                            } else {
                                let tableSizeInMb = rows[0].size_in_mb;
                                tableSizeInMb     = tableSizeInMb < 1 ? 1 : tableSizeInMb;

                                sql = 'SELECT COUNT(1) AS rows_count FROM `' + self._clonedSelfTableName + '`;';
                                connection.query(sql, (err2, rows2) => {
                                    connection.release();

                                    if (err2) {
                                        self.generateError(self, '\t--[populateTable] ' + err2, sql);
                                        resolvePopulateTable();
                                    } else {
                                        let rowsCnt              = rows2[0].rows_count;
                                        let chunksCnt            = tableSizeInMb / self._dataChunkSize;
                                        chunksCnt                = chunksCnt < 1 ? 1 : chunksCnt;
                                        let rowsInChunk          = Math.ceil(rowsCnt / chunksCnt);
                                        let populateTableWorkers = [];
                                        let msg                  = '\t--[populateTable] Total rows to insert into '
                                                                 + '"' + self._schema + '"."'
                                                                 + self._clonedSelfTableName + '": ' + rowsCnt;

                                        self.log(self, msg);

                                        for (let offset = 0; offset < rowsCnt; offset += rowsInChunk) {
                                            populateTableWorkers.push(
                                                self.populateTableWorker(self, offset, rowsInChunk, rowsCnt)
                                            );
                                        }

                                        Promise.all(populateTableWorkers).then(() => resolvePopulateTable(self));
                                    }
                                });
                            }
                        });
                    }
                });
            });
        },
        () => self.generateError(self, '\t--[populateTable] Cannot establish DB connections...')
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
    return new Promise(
        resolve => resolve(self)
    ).then(
        self.connect
    ).then(
        self => {
            return new Promise(resolvePopulateTableWorker => {
                self._mysql.getConnection((error, connection) => {
                    if (error) {
                        // The connection is undefined.
                        self.generateError(self, '\t--[populateTableWorker] Cannot connect to MySQL server...\n\t' + error);
                        resolvePopulateTableWorker();
                    } else {
                        let csvAddr = self._tempDirPath + '/' + self._clonedSelfTableName + offset + '.csv';
                        let sql     = 'SELECT * FROM `' + self._clonedSelfTableName + '` LIMIT ' + offset + ',' + rowsInChunk + ';';
                        connection.query(sql, (err, rows) => {
                            connection.release();

                            if (err) {
                                self.generateError(self, '\t--[populateTableWorker] ' + err, sql);
                                resolvePopulateTableWorker();
                            } else {
                                // Loop through current result set.
                                // Sanitize records.
                                // When sanitized - write them to a csv file.
                                rowsInChunk          = rows.length; // Must check amount of rows BEFORE sanitizing.
		                            let sanitizedRecords = [];

                                for (let cnt = 0; cnt < rows.length; cnt++) {
                                    let sanitizedRecord = Object.create(null);

                                    for (let attr in rows[cnt]) {
                                        sanitizedRecord[attr] = self.sanitizeValue(rows[cnt][attr]);
                                    }

                                    sanitizedRecords.push(sanitizedRecord);
                                }

                                csvStringify(sanitizedRecords, (csvError, csvString) => {
                                    let buffer = new Buffer(csvString, self._encoding);

                                    if (csvError) {
                                        self.generateError(self, '\t--[populateTableWorker] ' + csvError);
                                        resolvePopulateTableWorker();
                                    } else {
                                        fs.open(csvAddr, 'a', self._0777, (csvErrorFputcsvOpen, fd) => {
                                            if (csvErrorFputcsvOpen) {
                                                self.generateError(self, '\t--[populateTableWorker] ' + csvErrorFputcsvOpen);
                                                resolvePopulateTableWorker();
                                            } else {
                                                fs.write(fd, buffer, 0, buffer.length, null, csvErrorFputcsvWrite => {
                                                    if (csvErrorFputcsvWrite) {
                                                        self.generateError(self, '\t--[populateTableWorker] ' + csvErrorFputcsvWrite);
                                                        resolvePopulateTableWorker();
                                                    } else {
                                                        pg.connect(self._targetConString, (error, client, done) => {
                                                            if (error) {
                                                                done();
                                                                self.generateError(self, '\t--[populateTableWorker] Cannot connect to PostgreSQL server...\n' + error, sql);
                                                                resolvePopulateTableWorker();
                                                            } else {
                                                                sql = 'COPY "' + self._schema + '"."' + self._clonedSelfTableName + '" FROM '
                                                                    + '\'' + csvAddr + '\' DELIMITER \'' + ',\'' + ' CSV;';

                                                                client.query(sql, (err, result) => {
                                                                    done();

                                                                    if (err) {
                                                                        self.generateError(self, '\t--[populateTableWorker] ' + err, sql);
                                                                        self.populateTableByInsert(self, sanitizedRecords, () => {
                                                                            let msg = '\t--[populateTableWorker]  For now inserted: ' + self._totalRowsInserted + ' rows, '
                                                                                    + 'Total rows to insert into "' + self._schema + '"."' + self._clonedSelfTableName + '": ' + rowsCnt;

                                                                            self.log(self, msg);
                                                                            fs.unlink(csvAddr, () => {
                                                                                fs.close(fd, () => resolvePopulateTableWorker());
                                                                            });
                                                                        });
                                                                    } else {
                                                                        self._totalRowsInserted += result.rowCount;
                                                                        let msg                  = '\t--[populateTableWorker]  For now inserted: ' + self._totalRowsInserted + ' rows, '
                                                                                                 + 'Total rows to insert into "' + self._schema + '"."' + self._clonedSelfTableName + '": ' + rowsCnt;

                                                                        self.log(self, msg);
                                                                        fs.unlink(csvAddr, () => {
                                                                            fs.close(fd, () => resolvePopulateTableWorker());
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
        () => self.generateError(self, '\t--[populateTableWorker] Cannot establish DB connections...')
    );
};

/**
 * Populates data using INSERT statment.
 *
 * @param   {FromMySQL2PostgreSQL} self
 * @param   {Array}                rows
 * @param   {Function}             callback
 * @returns {undefined}
 */
FromMySQL2PostgreSQL.prototype.populateTableByInsert = function(self, rows, callback) {
    let insertPromises = [];

    for (let i = 0; i < rows.length; i++) {
        insertPromises.push(
            new Promise(resolveInsert => {
                // Execution of populateTableByInsert() must be successful, that is why no reject handler presented here.
                pg.connect(self._targetConString, (error, client, done) => {
                    if (error) {
                        done();
                        let msg = '\t--[populateTableByInsert] Cannot connect to PostgreSQL server...\n' + error;
                        self.generateError(self, msg, sql);
                        resolveInsert();
                    } else {
                        let sql                = 'INSERT INTO "' + self._schema + '"."' + self._clonedSelfTableName + '"';
                        let columns            = '(';
                        let valuesPlaceHolders = 'VALUES(';
                        let valuesData         = [];
                        let cnt                = 1;

                        for (let attr in rows[i]) {
                            columns             += '"' + attr + '",';
                            valuesPlaceHolders  += '$' + cnt + ',';
                            valuesData.push(rows[i][attr]); // rows are sanitized.
                            cnt++;
                        }

                        sql += columns.slice(0, -1) + ')' + valuesPlaceHolders.slice(0, -1) + ');';
                        client.query(sql, valuesData, err => {
                            done();

                            if (err) {
                                self.generateError(self, '\t--[populateTableByInsert] INSERT failed...\n' + err, sql);
                                resolveInsert();
                            } else {
                                self._totalRowsInserted++;
                                resolveInsert();
                            }
                        });
                    }
                });
            })
        );
    }

    Promise.all(insertPromises).then(() => callback.call(self));
};

/**
 * Define which columns of the given table are of type "enum".
 * Set an appropriate constraint, if need.
 *
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.processEnum = function(self) {
    return new Promise(resolve => {
        self.log(self, '\t--[processEnum] Defines "ENUMs" for table "' + self._schema + '"."' + self._clonedSelfTableName + '"');
        let processEnumPromises = [];

        for (let i = 0; i < self._clonedSelfTableColumns.length; i++) {
            if (self._clonedSelfTableColumns[i].Type.indexOf('(') !== -1) {
                let arrType = self._clonedSelfTableColumns[i].Type.split('(');

                if ('enum' === arrType[0]) { // arrType[1] ends with ')'.
                    processEnumPromises.push(
                        new Promise(resolveProcessEnum => {
                            pg.connect(self._targetConString, (error, client, done) => {
                                if (error) {
                                    done();
                                    let msg = '\t--[processEnum] Cannot connect to PostgreSQL server...\n' + error;
                                    self.generateError(self, msg);
                                    resolveProcessEnum();
                                } else {
                                    let sql = 'ALTER TABLE "' + self._schema + '"."' + self._clonedSelfTableName + '" '
                                            + 'ADD CHECK ("' + self._clonedSelfTableColumns[i].Field + '" IN (' + arrType[1] + ');';

                                    client.query(sql, err => {
                                        done();

                                        if (err) {
                                            let msg = '\t--[processEnum] Error while processing ENUM ...\n' + err;
                                            self.generateError(self, msg, sql);
                                            resolveProcessEnum();
                                        } else {
                                            let success = '\t--[processEnum] Set "ENUM" for table "' + self._schema + '"."' + self._clonedSelfTableName
                                                        + '" column: "' + self._clonedSelfTableColumns[i].Field + '"';

                                            self.log(self, success);
                                            resolveProcessEnum();
                                        }
                                    });
                                }
                            });
                        })
                    );
                }
            }
        }

        Promise.all(processEnumPromises).then(() => resolve(self));
    });
};

/**
 * Define which columns of the given table can contain the "NULL" value.
 * Set an appropriate constraint, if need.
 *
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.processNull = function(self) {
    return new Promise(resolve => {
        self.log(self, '\t--[processNull] Defines "NULLs" for table: "' + self._schema + '"."' + self._clonedSelfTableName + '"');
        let processNullPromises = [];

        for (let i = 0; i < self._clonedSelfTableColumns.length; i++) {
            if (self._clonedSelfTableColumns[i].Null.toLowerCase() === 'no') {
                processNullPromises.push(
                    new Promise(resolveProcessNull => {
                        pg.connect(self._targetConString, (error, client, done) => {
                            if (error) {
                                done();
                                let msg = '\t--[processNull] Cannot connect to PostgreSQL server...\n' + error;
                                self.generateError(self, msg);
                                resolveProcessNull();
                            } else {
                                let sql = 'ALTER TABLE "' + self._schema + '"."' + self._clonedSelfTableName
                                        + '" ALTER COLUMN "' + self._clonedSelfTableColumns[i].Field + '" SET NOT NULL;';

                                client.query(sql, err => {
                                    done();

                                    if (err) {
                                        let msg = '\t--[processNull] Error while processing NULLs...\n' + err;
                                        self.generateError(self, msg, sql);
                                        resolveProcessNull();
                                    } else {
                                        let success = '\t--[processNull] Set "ENUM" for table "' + self._schema + '"."' + self._clonedSelfTableName
                                                    + '" column: "' + self._clonedSelfTableColumns[i].Field + '"';

                                        self.log(self, success);
                                        resolveProcessNull();
                                    }
                                });
                            }
                        });
                    })
                );
            }
        }

        Promise.all(processNullPromises).then(() => resolve(self));
    });
};

/**
 * Define which columns of the given table have default value.
 * Set default values, if need.
 *
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.processDefault = function(self) {
    return new Promise(resolve => {
        self.log(self, '\t--[processDefault] Defines default values for table: "' + self._schema + '"."' + self._clonedSelfTableName + '"');
        let processDefaultPromises = [];
        let sqlReservedValues      = {
            'CURRENT_DATE'        : 'CURRENT_DATE',
            '0000-00-00'          : "'-INFINITY'",
            'CURRENT_TIME'        : 'CURRENT_TIME',
            '00:00:00'            : '00:00:00',
            'CURRENT_TIMESTAMP'   : 'CURRENT_TIMESTAMP',
            '0000-00-00 00:00:00' : "'-INFINITY'",
            'LOCALTIME'           : 'LOCALTIME',
            'LOCALTIMESTAMP'      : 'LOCALTIMESTAMP',
            'NULL'                : 'NULL',
            'UTC_DATE'            : "(CURRENT_DATE AT TIME ZONE 'UTC')",
            'UTC_TIME'            : "(CURRENT_TIME AT TIME ZONE 'UTC')",
            'UTC_TIMESTAMP'       : "(NOW() AT TIME ZONE 'UTC')",
        };

        for (let i = 0; i < self._clonedSelfTableColumns.length; i++) {
            if (self._clonedSelfTableColumns[i].Default) {
                processDefaultPromises.push(
                    new Promise(resolveProcessDefault => {
                        pg.connect(self._targetConString, (error, client, done) => {
                            if (error) {
                                done();
                                let msg = '\t--[processDefault] Cannot connect to PostgreSQL server...\n' + error;
                                self.generateError(self, msg);
                                resolveProcessDefault();
                            } else {
                                let sql = 'ALTER TABLE "' + self._schema + '"."' + self._clonedSelfTableName
                                        + '" ' + 'ALTER COLUMN "' + self._clonedSelfTableColumns[i].Field + '" SET DEFAULT ';

                                if (sqlReservedValues[self._clonedSelfTableColumns[i].Default]) {
                                    sql += sqlReservedValues[self._clonedSelfTableColumns[i].Default] + ';';
                                } else {
                                    sql += self.isFloatNumeric(self._clonedSelfTableColumns[i].Default)
                                           ? self._clonedSelfTableColumns[i].Default + ';'
                                           : "'" + self._clonedSelfTableColumns[i].Default + "';";
                                }

                                client.query(sql, err => {
                                    done();

                                    if (err) {
                                        let msg = '\t--[processDefault] Error while processing default values...\n' + err;
                                        self.generateError(self, msg, sql);
                                        resolveProcessDefault();
                                    } else {
                                        let success = '\t--[processDefault] Set default value for table "' + self._schema + '"."' + self._clonedSelfTableName
                                                    + '" column: "' + self._clonedSelfTableColumns[i].Field + '"';

                                        self.log(self, success);
                                        resolveProcessDefault();
                                    }
                                });
                            }
                        });
                    })
                );
            }
        }

        Promise.all(processDefaultPromises).then(() => resolve(self));
    });
};

/**
 * Define which column in given table has the "auto_increment" attribute.
 * Create an appropriate sequence.
 *
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.createSequence = function(self) {
    return new Promise(resolve => {
        let createSequencePromises = [];

        for (let i = 0; i < self._clonedSelfTableColumns.length; i++) {
            if (self._clonedSelfTableColumns[i].Extra === 'auto_increment') {
                createSequencePromises.push(
                    new Promise(resolveCreateSequence => {
                        let seqName = self._clonedSelfTableName + '_' + self._clonedSelfTableColumns[i].Field + '_seq';
                        self.log(self, '\t--[createSequence] Trying to create sequence : "' + self._schema + '"."' + seqName + '"');
                        pg.connect(self._targetConString, (error, client, done) => {
                            if (error) {
                                done();
                                let msg = '\t--[createSequence] Cannot connect to PostgreSQL server...\n' + error;
                                self.generateError(self, msg);
                                resolveCreateSequence();
                            } else {
                                let sql = 'CREATE SEQUENCE "' + self._schema + '"."' + seqName + '";';
                                client.query(sql, err => {
                                    if (err) {
                                        done();
                                        let errMsg = '\t--[createSequence] Failed to create sequence "' + self._schema + '"."' + seqName + '"';
                                        self.generateError(self, errMsg, sql);
                                        resolveCreateSequence();
                                    } else {
                                         sql = 'ALTER TABLE "' + self._schema + '"."' + self._clonedSelfTableName + '" '
                                             + 'ALTER COLUMN "' + self._clonedSelfTableColumns[i].Field + '" '
                                             + 'SET DEFAULT NEXTVAL(\'"' + self._schema + '"."' + seqName + '"\');';

                                         client.query(sql, err2 => {
                                             if (err2) {
                                                 done();
                                                 let err2Msg = '\t--[createSequence] Failed to set default value for "' + self._schema + '"."'
                                                            + self._clonedSelfTableName + '"."' + self._clonedSelfTableColumns[i].Field + '"...'
                                                            + '\n\t--[createSequence] Note: sequence "' + self._schema + '"."' + seqName + '" was created...';

                                                 self.generateError(self, err2Msg, sql);
                                                 resolveCreateSequence();
                                             } else {
                                                   sql = 'ALTER SEQUENCE "' + self._schema + '"."' + seqName + '" '
                                                       + 'OWNED BY "' + self._schema + '"."' + self._clonedSelfTableName
                                                       + '"."' + self._clonedSelfTableColumns[i].Field + '";';

                                                   client.query(sql, err3 => {
                                                        if (err3) {
                                                            done();
                                                            let err3Msg = '\t--[createSequence] Failed to relate sequence "' + self._schema + '"."' + seqName + '" to '
                                                                       + '"' + self._schema + '"."'
                                                                       + self._clonedSelfTableName + '"."' + self._clonedSelfTableColumns[i].Field + '"...';

                                                            self.generateError(self, err3Msg, sql);
                                                            resolveCreateSequence();
                                                        } else {
                                                           sql = 'SELECT SETVAL(\'"' + self._schema + '"."' + seqName + '"\', '
                                                               + '(SELECT MAX("' + self._clonedSelfTableColumns[i].Field + '") FROM "'
                                                               + self._schema + '"."' + self._clonedSelfTableName + '"));';

                                                           client.query(sql, err4 => {
                                                              done();

                                                              if (err4) {
                                                                  let err4Msg = '\t--[createSequence] Failed to set max-value of "' + self._schema + '"."'
                                                                              + self._clonedSelfTableName + '"."' + self._clonedSelfTableColumns[i].Field + '" '
                                                                              + 'as the "NEXTVAL of "' + self._schema + '"."' + seqName + '"...';

                                                                  self.generateError(self, err4Msg, sql);
                                                                  resolveCreateSequence();
                                                              } else {
                                                                  let success = '\t--[createSequence] Sequence "' + self._schema + '"."' + seqName + '" is created...';
                                                                  self.log(self, success);
                                                                  resolveCreateSequence();
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
                    })
                );
            }
        }

        Promise.all(createSequencePromises).then(() => resolve(self));
    });
};

/**
 * Create primary key and indices.
 *
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.processIndexAndKey = function(self) {
    return new Promise(
        resolve => resolve(self)
    ).then(
        self.connect
    ).then(
        self => {
            return new Promise(resolveProcessIndexAndKey => {
                self._mysql.getConnection((error, connection) => {
                    if (error) {
                        // The connection is undefined.
                        self.generateError(self, '\t--[processIndexAndKey] Cannot connect to MySQL server...\n\t' + error);
                        resolveProcessIndexAndKey();
                    } else {
                        let sql = 'SHOW INDEX FROM `' + self._clonedSelfTableName + '`;';
                        connection.query(sql, (err, arrIndices) => {
                            connection.release();

                            if (err) {
                                self.generateError(self, '\t--[processIndexAndKey] ' + err, sql);
                                resolveProcessIndexAndKey();
                            } else {
                                let objPgIndices               = {};
                                let cnt                        = 0;
                                let indexType                  = '';
                                let processIndexAndKeyPromises = [];

                                for (let i = 0; i < arrIndices.length; i++) {
                                    if (arrIndices[i].Key_name in objPgIndices) {
                                        objPgIndices[arrIndices[i].Key_name].column_name.push('"' + arrIndices[i].Column_name + '"');
                                    } else {
                                        objPgIndices[arrIndices[i].Key_name] = {
                                            'is_unique'   : arrIndices[i].Non_unique === 0 ? true : false,
                                            'column_name' : ['"' + arrIndices[i].Column_name + '"']
                                        };
                                    }
                                }

                                for (let attr in objPgIndices) {
                                    processIndexAndKeyPromises.push(
                                        new Promise(resolveProcessIndexAndKeySql => {
                                            pg.connect(self._targetConString, (pgError, pgClient, done) => {
                                                if (pgError) {
                                                    done();
                                                    let msg = '\t--[processIndexAndKey] Cannot connect to PostgreSQL server...\n' + pgError;
                                                    self.generateError(self, msg);
                                                    resolveProcessIndexAndKeySql();
                                                } else {
                                                    if (attr.toLowerCase() === 'primary') {
                                                        indexType = 'PK';
                                                        sql       = 'ALTER TABLE "' + self._schema + '"."' + self._clonedSelfTableName + '" '
                                                                  + 'ADD PRIMARY KEY(' + objPgIndices[attr].column_name.join(',') + ');';

                                                    } else {
                                                        // "schema_idxname_{integer}_idx" - is NOT a mistake.
                                                        let columnName = objPgIndices[attr].column_name[0].slice(1, -1) + cnt++;
                                                        indexType      = 'index';
                                                        sql            = 'CREATE ' + (objPgIndices[attr].is_unique ? 'UNIQUE ' : '') + 'INDEX "'
                                                                       + self._schema + '_' + self._clonedSelfTableName + '_' + columnName + '_idx" ON "'
                                                                       + self._schema + '"."' + self._clonedSelfTableName
                                                                       + '" (' + objPgIndices[attr].column_name.join(',') + ');';
                                                    }

                                                    pgClient.query(sql, err2 => {
                                                        done();

                                                        if (err2) {
                                                            self.generateError(self, '\t--[processIndexAndKey] ' + err2, sql);
                                                            resolveProcessIndexAndKeySql();
                                                        } else {
                                                            resolveProcessIndexAndKeySql();
                                                        }
                                                    });
                                                }
                                            });
                                        })
                                    );
                                }

                                Promise.all(processIndexAndKeyPromises).then(() => {
                                    let success = '\t--[processIndexAndKey] "' + self._schema + '"."'
                                                + self._clonedSelfTableName + '": PK/indices are successfully set...';

                                    self.log(self, success);
                                    resolveProcessIndexAndKey(self);
                                });
                            }
                        });
                    }
                });
            });
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
    return new Promise(resolve => {
        self                          = Object.create(self);
        self._clonedSelfTableName     = tableName;
        self._totalRowsInserted       = 0;
	      self._clonedSelfTableNamePath = self._logsDirPath + '/' + tableName + '.log';
        resolve(self);
    }).then(
        self.connect
    ).then(
        self.createTable,
        () => {
            // Braces are essential. Without them promises-chain will continue execution.
            self.log(self, '\t--[processTable] Cannot establish DB connections...');
        }
    ).then(
        self.populateTable,
        () => {
            // Braces are essential. Without them promises-chain will continue execution.
            self.log(self, '\t--[processTable] Cannot create table "' + self._schema + '"."' + tableName + '"...');
        }
    ).then(
        self.processEnum
    ).then(
        self.processNull
    ).then(
        self.processDefault
    ).then(
        self.createSequence
    ).then(
        self.processIndexAndKey
    );
};

/**
 * Closes DB connections.
 *
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.closeConnections = function(self) {
    return new Promise(resolve => {
        if (self._mysql) {
            self._mysql.end(error => {
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
 * Closes DB connections and removes the "./temporary_directory".
 *
 * @param   {FromMySQL2PostgreSQL} self
 * @returns {Promise}
 */
FromMySQL2PostgreSQL.prototype.cleanup = function(self) {
    return new Promise(resolve => {
        self.log(self, '\t--[cleanup] Cleanup resources...');
        resolve(self);
    }).then(
        self.removeTemporaryDirectory
    ).then(
        self.closeConnections
    ).then(self => {
        return new Promise(resolve => {
            self.log(self, '\t--[cleanup] Cleanup finished...');
            resolve(self);
        });
    });
};

/**
 * Generates a summary report.
 *
 * @param   {FromMySQL2PostgreSQL} self
 * @param   {String}               endMsg
 * @returns {undefined}
 */
FromMySQL2PostgreSQL.prototype.generateReport = function(self, endMsg) {
    let timeTaken  = (new Date()) - self._timeBegin;
    let hours      = Math.floor(timeTaken / 1000 / 3600);
    timeTaken     -= hours * 1000 * 3600;
    let minutes    = Math.floor(timeTaken / 1000 / 60);
    timeTaken     -= minutes * 1000 * 60;
    let seconds    = Math.ceil(timeTaken / 1000);
    hours          = hours < 10 ? '0' + hours : hours;
    minutes        = minutes < 10 ? '0' + minutes : minutes;
    seconds        = seconds < 10 ? '0' + seconds : seconds;
    let output     = '\t--[generateReport] ' + endMsg
                   + '\n\t--[generateReport] Total time: ' + hours + ':' + minutes + ':' + seconds
                   + '\n\t--[generateReport] (hours:minutes:seconds)';

    self.log(self, output);
    process.exit();
};

/**
 * Runs migration according to user's configuration.
 *
 * @param   {Object} config
 * @returns {undefined}
 */
FromMySQL2PostgreSQL.prototype.run = function(config) {
    let self     = this;
    self._config = config;
    let promise  = new Promise(resolve => resolve(self));

    promise.then(
        self.boot
    ).then(
        self.createLogsDirectory,
        () => {
            // Braces are essential. Without them promises-chain will continue execution.
            console.log('\t--[run] Failed to boot migration');
        }
    ).then(
        self.createTemporaryDirectory,
        () => {
            // Braces are essential. Without them promises-chain will continue execution.
            self.log(self, '\t--[run] Logs directory was not created...');
        }
    ).then(
        self.createSchema,
        () => {
            let msg = '\t--[run] The temporary directory [' + self._tempDirPath + '] already exists...'
                    + '\n\t  Please, remove this directory and rerun NMIG...';

            self.log(self, msg);
        }
    ).then(
        self.loadStructureToMigrate,
        () => {
            return new Promise(resolveError => resolveError(self)).then(() => {
                self.log(self, '\t--[run] Cannot create new DB schema...');
                self.cleanup(self);
            });
        }
    ).then(
        self.processForeignKey,
        () => {
            return new Promise(resolveError => resolveError(self)).then(() => {
                self.log(self, '\t--[run] NMIG cannot load source database structure...');
                self.cleanup(self);
            });
        }
    ).then(
        self.runVacuumFullAndAnalyze
    ).then(
        () => {
            return new Promise(
                resolve => resolve(self)
            ).then(
                self.cleanup
            ).then(
                self => self.generateReport(self, 'NMIG migration is accomplished.')
            );
        },
        () => {
            return new Promise(
                resolveErr => resolveErr(self)
            ).then(
                () => self.cleanup(self)
            ).then(
                () => {
                    let message = 'NMIG migration is accomplished with errors. '
                                + 'Please, check log files under [' + self._logsDirPath + ']';

                    self.generateReport(self, message);
                }
            );
        }
    );
};

module.exports.FromMySQL2PostgreSQL = FromMySQL2PostgreSQL;

