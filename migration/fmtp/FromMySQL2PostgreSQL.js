/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright 2016 Anatoly Khaytovich <anatolyuss@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program (please see the "LICENSE.md" file).
 * If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 * @author Anatoly Khaytovich <anatolyuss@gmail.com>
 */
'use strict';

const fs                 = require('fs');
const pg                 = require('pg');
const mysql              = require('mysql');
const events             = require('events');
const csvStringify       = require('./CsvStringifyModified');
const viewGenerator      = require('./ViewGenerator');
const arrangeColumnsData = require('./ColumnsDataArranger');
const Table              = require('./Table');

const eventEmitter = new events.EventEmitter();
const self         = {
    _config              : null,
    _sourceConString     : null,
    _targetConString     : null,
    _tempDirPath         : '',
    _logsDirPath         : '',
    _dataTypesMapAddr    : '',
    _allLogsPath         : '',
    _errorLogsPath       : '',
    _notCreatedViewsPath : '',
    _copyOnly            : null,
    _noVacuum            : null,
    _excludeTables       : null,
    _timeBegin           : new Date(),
    _encoding            : '',
    _dataChunkSize       : 0,
    _0777                : '0777',
    _mysql               : null,
    _tablesToMigrate     : [],
    _viewsToMigrate      : [],
    _tablesCnt           : 0,
    _viewsCnt            : 0,
    _mySqlDbName         : '',
    _schema              : '',
    _maxPoolSizeSource   : 0,
    _maxPoolSizeTarget   : 0,
    _pipeWidth           : 0,
    _targetConString     : '',
    _dataPool            : [],
    _dicTables           : {}
};

/**
 * Sets configuration parameters.
 *
 * @returns {Promise}
 */
function boot() {
    return new Promise((resolve, reject) => {
        console.log('\n\tNMIG - the database migration tool');
        console.log('\tCopyright 2016 Anatoly Khaytovich <anatolyuss@gmail.com>');
        console.log('\n\t--[boot] Boot...');

        if (self._config.source === undefined) {
            console.log('\t--[boot] Cannot perform a migration due to missing source database (MySQL) connection string');
            console.log('\t--[boot] Please, specify source database (MySQL) connection string, and run the tool again');
            return reject();
        }

        if (self._config.target === undefined) {
            console.log('\t--[boot] Cannot perform a migration due to missing target database (PostgreSQL) connection string');
            console.log('\t--[boot] Please, specify target database (PostgreSQL) connection string, and run the tool again');
            return reject();
        }

        self._sourceConString     = self._config.source;
        self._targetConString     = self._config.target;
        self._tempDirPath         = self._config.tempDirPath;
        self._logsDirPath         = self._config.logsDirPath;
        self._dataTypesMapAddr    = __dirname + '/DataTypesMap.json';
        self._allLogsPath         = self._logsDirPath + '/all.log';
        self._errorLogsPath       = self._logsDirPath + '/errors-only.log';
        self._notCreatedViewsPath = self._logsDirPath + '/not_created_views';
        self._copyOnly            = self._config.copy_only;
        self._noVacuum            = self._config.no_vacuum;
        self._excludeTables       = self._config.exclude_tables;
        self._encoding            = self._config.encoding === undefined ? 'utf8' : self._config.encoding;
        self._dataChunkSize       = self._config.data_chunk_size === undefined ? 100 : +self._config.data_chunk_size;
        self._dataChunkSize       = self._dataChunkSize < 100 ? 100 : self._dataChunkSize;
        self._mySqlDbName         = self._sourceConString.database;
        self._schema              = self._config.schema === undefined ||
                                    self._config.schema === ''
                                    ? self._mySqlDbName
                                    : self._config.schema;

        self._maxPoolSizeSource   = self._config.max_pool_size_source !== undefined &&
                                    isIntNumeric(self._config.max_pool_size_source)
                                    ? +self._config.max_pool_size_source
                                    : 10;

        self._maxPoolSizeTarget   = self._config.max_pool_size_target !== undefined &&
                                    isIntNumeric(self._config.max_pool_size_target)
                                    ? +self._config.max_pool_size_target
                                    : 10;

        self._maxPoolSizeSource   = self._maxPoolSizeSource > 0 ? self._maxPoolSizeSource : 10;
        self._maxPoolSizeTarget   = self._maxPoolSizeTarget > 0 ? self._maxPoolSizeTarget : 10;

        self._pipeWidth           = self._config.pipe_width !== undefined &&
                                    isIntNumeric(self._config.pipe_width)
                                    ? Math.abs(+self._config.pipe_width)
                                    : self._maxPoolSizeTarget;

        self._pipeWidth           = self._pipeWidth > self._maxPoolSizeTarget ? self._maxPoolSizeTarget : self._pipeWidth;

        let targetConString       = 'postgresql://' + self._targetConString.user + ':' + self._targetConString.password
                                  + '@' + self._targetConString.host + ':' + self._targetConString.port + '/'
                                  + self._targetConString.database + '?client_encoding=' + self._targetConString.charset;

        self._targetConString     = targetConString;
        pg.defaults.poolSize      = self._maxPoolSizeTarget;
        resolve();
    }).then(
        readDataTypesMap
    ).then(
        () => {
            return new Promise(resolveBoot => {
                console.log('\t--[boot] Boot is accomplished...');
                resolveBoot();
            });
        },
        () => console.log('\t--[boot] Cannot parse JSON from' + self._dataTypesMapAddr + '\t--[Boot] Boot failed.')
    );
}

/**
 * Checks if given value is integer number.
 *
 * @param   {String|Number} value
 * @returns {Boolean}
 */
function isIntNumeric(value) {
    return !isNaN(parseInt(value)) && isFinite(value);
}

/**
 * Checks if given value is float number.
 *
 * @param   {String|Number} value
 * @returns {Boolean}
 */
function isFloatNumeric(value) {
    return !isNaN(parseFloat(value)) && isFinite(value);
}

/**
 * Converts MySQL data types to corresponding PostgreSQL data types.
 * This conversion performs in accordance to mapping rules in './DataTypesMap.json'.
 * './DataTypesMap.json' can be customized.
 *
 * @param   {Object} objDataTypesMap
 * @param   {String} mySqlDataType
 * @returns {String}
 */
function mapDataTypes(objDataTypesMap, mySqlDataType) {
    let retVal               = '';
    let arrDataTypeDetails   = mySqlDataType.split(' ');
    mySqlDataType            = arrDataTypeDetails[0].toLowerCase();
    let increaseOriginalSize = arrDataTypeDetails.indexOf('unsigned') !== -1 || arrDataTypeDetails.indexOf('zerofill') !== -1;
    arrDataTypeDetails       = null;

    if (mySqlDataType.indexOf('(') === -1) {
        // No parentheses detected.
        retVal = increaseOriginalSize ? objDataTypesMap[mySqlDataType].increased_size : objDataTypesMap[mySqlDataType].type;
    } else {
        // Parentheses detected.
        let arrDataType             = mySqlDataType.split('(');
        let strDataType             = arrDataType[0].toLowerCase();
        let strDataTypeDisplayWidth = arrDataType[1];
        arrDataType                 = null;

        if ('enum' === strDataType) {
            retVal = 'character varying(255)';
        } else if ('decimal' === strDataType || 'numeric' === strDataType) {
            retVal = objDataTypesMap[strDataType].type + '(' + strDataTypeDisplayWidth;
        } else if ('decimal(19,2)' === mySqlDataType || objDataTypesMap[strDataType].mySqlVarLenPgSqlFixedLen) {
            // Should be converted without a length definition.
            retVal = increaseOriginalSize
                     ? objDataTypesMap[strDataType].increased_size
                     : objDataTypesMap[strDataType].type;
        } else {
            // Should be converted with a length definition.
            retVal = increaseOriginalSize
                     ? objDataTypesMap[strDataType].increased_size + '(' + strDataTypeDisplayWidth
                     : objDataTypesMap[strDataType].type + '(' + strDataTypeDisplayWidth;
        }
    }

    // Prevent incompatible length (CHARACTER(0) or CHARACTER VARYING(0)).
    if (retVal === 'character(0)') {
        retVal = 'character(1)';
    } else if (retVal === 'character varying(0)') {
        retVal = 'character varying(1)';
    }

    return retVal;
}

/**
 * Reads "./DataTypesMap.json" and converts its json content to js object.
 * Appends this object to "FromMySQL2PostgreSQL" instance.
 *
 * @returns {Promise}
 */
function readDataTypesMap() {
    return new Promise((resolve, reject) => {
        fs.readFile(self._dataTypesMapAddr, (error, data) => {
            if (error) {
                console.log('\t--[readDataTypesMap] Cannot read "DataTypesMap" from ' + self._dataTypesMapAddr);
                reject();
            } else {
                try {
                    self._dataTypesMap = JSON.parse(data.toString());
                    console.log('\t--[readDataTypesMap] Data Types Map is loaded...');
                    resolve();
                } catch (err) {
                    console.log('\t--[readDataTypesMap] Cannot parse JSON from' + self._dataTypesMapAddr);
                    reject();
                }
            }
        });
    });
}

/**
 * Creates temporary directory.
 *
 * @returns {Promise}
 */
function createTemporaryDirectory() {
    return new Promise((resolve, reject) => {
        log('\t--[createTemporaryDirectory] Creating temporary directory...');
        fs.stat(self._tempDirPath, (directoryDoesNotExist, stat) => {
            if (directoryDoesNotExist) {
                fs.mkdir(self._tempDirPath, self._0777, e => {
                    if (e) {
                        log(
                            '\t--[createTemporaryDirectory] Cannot perform a migration due to impossibility to create '
                            + '"temporary_directory": ' + self._tempDirPath
                        );
                        reject();
                    } else {
                        log('\t--[createTemporaryDirectory] Temporary directory is created...');
                        resolve();
                    }
                });
            } else if (!stat.isDirectory()) {
                log('\t--[createTemporaryDirectory] Cannot perform a migration due to unexpected error');
                reject();
            } else {
                reject();
            }
        });
    });
}

/**
 * Removes temporary directory.
 *
 * @returns {Promise}
 */
function removeTemporaryDirectory() {
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

            log(msg);
            resolve();
        });
    });
}

/**
 * Creates logs directory.
 *
 * @returns {Promise}
 */
function createLogsDirectory() {
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
                        log('\t--[createLogsDirectory] Logs directory is created...');
                        resolve();
                    }
                });
            } else if (!stat.isDirectory()) {
                console.log('\t--[createLogsDirectory] Cannot perform a migration due to unexpected error');
                reject();
            } else {
                log('\t--[createLogsDirectory] Logs directory already exists...');
                resolve();
            }
        });
    });
}

/**
 * Writes a log, containing a view code.
 *
 * @param   {String} viewName
 * @param   {String} sql
 * @returns {undefined}
 */
function logNotCreatedView(viewName, sql) {
    fs.stat(self._notCreatedViewsPath, (directoryDoesNotExist, stat) => {
        if (directoryDoesNotExist) {
            fs.mkdir(self._notCreatedViewsPath, self._0777, e => {
                if (e) {
                    log('\t--[logNotCreatedView] ' + e);
                } else {
                    log('\t--[logNotCreatedView] "not_created_views" directory is created...');
                    // "not_created_views" directory is created. Can write the log...
                    fs.open(self._notCreatedViewsPath + '/' + viewName + '.sql', 'w', self._0777, (error, fd) => {
                        if (error) {
                            log(error);
                        } else {
                            let buffer = new Buffer(sql, self._encoding);
                            fs.write(fd, buffer, 0, buffer.length, null, () => {
                                buffer = null;
                                fs.close(fd);
                            });
                        }
                    });
                }
            });
        } else if (!stat.isDirectory()) {
            log('\t--[logNotCreatedView] Cannot write the log due to unexpected error');
        } else {
            // "not_created_views" directory already exists. Can write the log...
            fs.open(self._notCreatedViewsPath + '/' + viewName + '.sql', 'w', self._0777, (error, fd) => {
                if (error) {
                    log(error);
                } else {
                    let buffer = new Buffer(sql, self._encoding);
                    fs.write(fd, buffer, 0, buffer.length, null, () => {
                        buffer = null;
                        fs.close(fd);
                    });
                }
            });
        }
    });
}

/**
 * Outputs given log.
 * Writes given log to the "/all.log" file.
 * If necessary, writes given log to the "/{tableName}.log" file.
 *
 * @param   {String}  log
 * @param   {String}  tableLogPath
 * @param   {Boolean} isErrorLog
 * @returns {undefined}
 */
function log(log, tableLogPath, isErrorLog) {
    let buffer = new Buffer(log + '\n\n', self._encoding);

    if (!isErrorLog) {
        console.log(log);
    }

    fs.open(self._allLogsPath, 'a', self._0777, (error, fd) => {
        if (!error) {
            fs.write(fd, buffer, 0, buffer.length, null, () => {
                fs.close(fd, () => {
                    if (tableLogPath) {
                        fs.open(tableLogPath, 'a', self._0777, (error, fd) => {
                            if (!error) {
                                fs.write(fd, buffer, 0, buffer.length, null, () => {
                                    buffer = null;
                                    fs.close(fd);
                                });
                            }
                        });
                    }
                });
            });
        }
    });
}

/**
 * Writes a ditailed error message to the "/errors-only.log" file
 *
 * @param   {String} message
 * @param   {String} sql
 * @returns {undefined}
 */
function generateError(message, sql) {
    message    += '\n\n\tSQL: ' + (sql || '') + '\n\n';
    let buffer  = new Buffer(message, self._encoding);
    log(message, undefined, true);

    fs.open(self._errorLogsPath, 'a', self._0777, (error, fd) => {
        if (!error) {
            fs.write(fd, buffer, 0, buffer.length, null, () => {
                buffer = null;
                fs.close(fd);
            });
        }
    });
}

/**
 * Check if both servers are connected.
 * If not, than create connections.
 *
 * @returns {Promise}
 */
function connect() {
    return new Promise((resolve, reject) => {
        // Check if MySQL server is connected.
        // If not connected - connect.
        if (!self._mysql) {
            self._sourceConString.connectionLimit = self._maxPoolSizeSource;
            let pool                              = mysql.createPool(self._sourceConString);

            if (pool) {
                self._mysql = pool;
                resolve();
            } else {
                log('\t--[connect] Cannot connect to MySQL server...');
                reject();
            }
        } else {
            resolve();
        }
    });
}

/**
 * Create a new database schema.
 * Insure a uniqueness of a new schema name.
 *
 * @returns {Promise}
 */
function createSchema() {
    return new Promise((resolve, reject) => {
        pg.connect(self._targetConString, (error, client, done) => {
            if (error) {
                done();
                generateError('\t--[createSchema] Cannot connect to PostgreSQL server...\n' + error);
                reject();
            } else {
                let sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '" + self._schema + "';";
                client.query(sql, (err, result) => {
                    if (err) {
                        done();
                        generateError('\t--[createSchema] ' + err, sql);
                        reject();
                    } else if (result.rows.length === 0) {
                        // If 'self._schema !== 0' (schema is defined and already exists), then no need to create it.
                        // Such schema will be just used...
                        sql = 'CREATE SCHEMA "' + self._schema + '";';
                        client.query(sql, err => {
                            done();

                            if (err) {
                                generateError('\t--[createSchema] ' + err, sql);
                                reject();
                            } else {
                                resolve();
                            }
                        });
                    } else {
                        resolve();
                    }
                });
            }
        });
    });
}

/**
 * Create the "{schema}"."data_pool_{self._schema + self._mySqlDbName} temporary table."
 *
 * @returns {Promise}
 */
function createDataPoolTable() {
    return new Promise((resolve, reject) => {
        pg.connect(self._targetConString, (error, client, done) => {
            if (error) {
                done();
                generateError('\t--[createDataPoolTable] Cannot connect to PostgreSQL server...\n' + error);
                reject();
            } else {
                let sql = 'CREATE TABLE "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '"(' + '"json" TEXT' + ');';
                client.query(sql, err => {
                    done();

                    if (err) {
                        generateError('\t--[createDataPoolTable] ' + err, sql);
                        reject();
                    } else {
                        log('\t--[createDataPoolTable] table "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '" is created...');
                        resolve();
                    }
                });
            }
        });
    });
}

/**
 * Drop the "{schema}"."data_pool_{self._schema + self._mySqlDbName} temporary table."
 *
 * @returns {Promise}
 */
function dropDataPoolTable() {
    return new Promise(resolve => {
        pg.connect(self._targetConString, (error, client, done) => {
            if (error) {
                done();
                generateError('\t--[dropDataPoolTable] Cannot connect to PostgreSQL server...\n' + error);
                resolve();
            } else {
                let sql = 'DROP TABLE "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '";';
                client.query(sql, err => {
                    done();

                    if (err) {
                        generateError('\t--[dropDataPoolTable] ' + err, sql);
                    } else {
                        log('\t--[dropDataPoolTable] table "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '" is dropped...');
                    }

                    resolve();
                });
            }
        });
    });
}

/**
 * Load source tables and views, that need to be migrated.
 *
 * @returns {Promise}
 */
function loadStructureToMigrate() {
    return connect().then(
        () => {
            return new Promise((resolve, reject) => {
                self._mysql.getConnection((error, connection) => {
                    if (error) {
                        // The connection is undefined.
                        generateError('\t--[loadStructureToMigrate] Cannot connect to MySQL server...\n' + error);
                        reject();
                    } else {
                        let sql = 'SHOW FULL TABLES IN `' + self._mySqlDbName + '`;';
                        connection.query(sql, (strErr, rows) => {
                            connection.release();

                            if (strErr) {
                                generateError('\t--[loadStructureToMigrate] ' + strErr, sql);
                                reject();
                            } else {
                                let tablesCnt            = 0;
                                let viewsCnt             = 0;
                                let processTablePromises = [];

                                for (let i = 0; i < rows.length; ++i) {
                                    let relationName = rows[i]['Tables_in_' + self._mySqlDbName];

                                    if (rows[i].Table_type === 'BASE TABLE' && self._excludeTables.indexOf(relationName) === -1) {
                                        self._tablesToMigrate.push(relationName);
                                        self._dicTables[relationName] = new Table(self._logsDirPath + '/' + relationName + '.log');
                                        processTablePromises.push(processTableBeforeDataLoading(relationName));
                                        tablesCnt++;
                                    } else if (rows[i].Table_type === 'VIEW') {
                                        self._viewsToMigrate.push(relationName);
                                        viewsCnt++;
                                    }
                                }

                                rows            = null;
                                self._tablesCnt = tablesCnt;
                                self._viewsCnt  = viewsCnt;
                                let message     = '\t--[loadStructureToMigrate] Source DB structure is loaded...\n'
                                                + '\t--[loadStructureToMigrate] Tables to migrate: ' + tablesCnt + '\n'
                                                + '\t--[loadStructureToMigrate] Views to migrate: ' + viewsCnt;

                                log(message);
                                Promise.all(processTablePromises).then(() => resolve(), () => reject());
                            }
                        });
                    }
                });
            });
        },
        () => log('\t--[loadStructureToMigrate] Cannot establish DB connections...')
    );
}

/**
 * Attempts to convert MySQL view to PostgreSQL view.
 *
 * @returns {Promise}
 */
function processView() {
    return new Promise(resolve => {
        let createViewPromises = [];

        for (let i = 0; i < self._viewsToMigrate.length; ++i) {
            createViewPromises.push(
                connect().then(
                    () => {
                        return new Promise(resolveProcessView2 => {
                            self._mysql.getConnection((error, connection) => {
                                if (error) {
                                    // The connection is undefined.
                                    generateError('\t--[processView] Cannot connect to MySQL server...\n' + error);
                                    resolveProcessView2();
                                } else {
                                    let sql = 'SHOW CREATE VIEW `' + self._viewsToMigrate[i] + '`;';
                                    connection.query(sql, (strErr, rows) => {
                                        connection.release();

                                        if (strErr) {
                                            generateError('\t--[processView] ' + strErr, sql);
                                            resolveProcessView2();
                                        } else {
                                            pg.connect(self._targetConString, (error, client, done) => {
                                                if (error) {
                                                    done();
                                                    generateError('\t--[processView] Cannot connect to PostgreSQL server...');
                                                    resolveProcessView2();
                                                } else {
                                                    sql  = viewGenerator(self._schema, self._viewsToMigrate[i], rows[0]['Create View']);
                                                    rows = null;
                                                    client.query(sql, err => {
                                                        done();

                                                        if (err) {
                                                            generateError('\t--[processView] ' + err, sql);
                                                            logNotCreatedView(self._viewsToMigrate[i], sql);
                                                            resolveProcessView2();
                                                        } else {
                                                            log('\t--[processView] View "' + self._schema + '"."' + self._viewsToMigrate[i] + '" is created...');
                                                            resolveProcessView2();
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
                    () => generateError('\t--[processView] Cannot establish DB connections...')
                )
            );
        }

        Promise.all(createViewPromises).then(() => resolve());
    });
}

/**
 * Starts a process of foreign keys creation.
 *
 * @returns {Promise}
 */
function processForeignKey() {
    return new Promise(resolve => {
        let fkPromises = [];

        for (let i = 0; i < self._tablesToMigrate.length; ++i) {
            let tableName = self._tablesToMigrate[i];
            log('\t--[processForeignKey] Search foreign keys for table "' + self._schema + '"."' + tableName + '"...');
            fkPromises.push(
                connect().then(() => {
                    return new Promise(fkResolve => {
                        self._mysql.getConnection((error, connection) => {
                            if (error) {
                                // The connection is undefined.
                                generateError('\t--[processForeignKey] Cannot connect to MySQL server...\n' + error);
                                fkResolve();
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
                                        + "AND cols.TABLE_NAME = '" + tableName + "';";

                                  connection.query(sql, (err, rows) => {
                                      connection.release();

                                      if (err) {
                                          generateError(self, '\t--[processForeignKey] ' + err, sql);
                                          fkResolve();
                                      } else {
                                          processForeignKeyWorker(tableName, rows).then(() => {
                                              log('\t--[processForeignKey] Foreign keys for table "' + self._schema + '"."' + tableName + '" are set...');
                                              fkResolve();
                                          });
                                      }
                                  });
                              }
                        });
                    });
                })
            );
        }

        Promise.all(fkPromises).then(() => resolve());
    });
}

/**
 * Creates foreign keys for given table.
 *
 * @param   {String} tableName
 * @param   {Array}  rows
 * @returns {Promise}
 */
function processForeignKeyWorker(tableName, rows) {
    return new Promise(resolve => {
        let constraintsPromises = [];
        let objConstraints      = Object.create(null);

        for (let i = 0; i < rows.length; ++i) {
            if (rows[i].CONSTRAINT_NAME in objConstraints) {
                objConstraints[rows[i].CONSTRAINT_NAME].column_name.push('"' + rows[i].COLUMN_NAME + '"');
                objConstraints[rows[i].CONSTRAINT_NAME].referenced_column_name.push('"' + rows[i].REFERENCED_COLUMN_NAME + '"');
            } else {
                objConstraints[rows[i].CONSTRAINT_NAME]                        = Object.create(null);
                objConstraints[rows[i].CONSTRAINT_NAME].column_name            = ['"' + rows[i].COLUMN_NAME + '"'];
                objConstraints[rows[i].CONSTRAINT_NAME].referenced_column_name = ['"' + rows[i].REFERENCED_COLUMN_NAME + '"'];
                objConstraints[rows[i].CONSTRAINT_NAME].referenced_table_name  = rows[i].REFERENCED_TABLE_NAME;
                objConstraints[rows[i].CONSTRAINT_NAME].update_rule            = rows[i].UPDATE_RULE;
                objConstraints[rows[i].CONSTRAINT_NAME].delete_rule            = rows[i].DELETE_RULE;
            }
        }

        rows = null;

        for (let attr in objConstraints) {
            constraintsPromises.push(
                new Promise(resolveConstraintPromise => {
                    pg.connect(self._targetConString, (error, client, done) => {
                        if (error) {
                            done();
                            objConstraints[attr] = null;
                            generateError('\t--[processForeignKeyWorker] Cannot connect to PostgreSQL server...');
                            resolveConstraintPromise();
                        } else {
                            let sql = 'ALTER TABLE "' + self._schema + '"."' + tableName + '" ADD FOREIGN KEY ('
                                    + objConstraints[attr].column_name.join(',') + ') REFERENCES "' + self._schema + '"."'
                                    + objConstraints[attr].referenced_table_name + '" (' + objConstraints[attr].referenced_column_name.join(',')
                                    + ') ON UPDATE ' + objConstraints[attr].update_rule + ' ON DELETE ' + objConstraints[attr].delete_rule + ';';

                            objConstraints[attr] = null;
                            client.query(sql, err => {
                                done();

                                if (err) {
                                    generateError('\t--[processForeignKeyWorker] ' + err, sql);
                                    resolveConstraintPromise();
                                } else {
                                    resolveConstraintPromise();
                                }
                            });
                        }
                    });
                })
            );
        }

        Promise.all(constraintsPromises).then(() => resolve());
    });
}

/**
 * Runs "vacuum full" and "analyze".
 *
 * @returns {Promise}
 */
function runVacuumFullAndAnalyze() {
    return new Promise(resolve => {
        let vacuumPromises = [];

        for (let i = 0; i < self._tablesToMigrate.length; ++i) {
            if (self._noVacuum.indexOf(self._tablesToMigrate[i]) === -1) {
                let msg = '\t--[runVacuumFullAndAnalyze] Running "VACUUM FULL and ANALYZE" query for table "'
                        + self._schema + '"."' + self._tablesToMigrate[i] + '"...';

                log(msg);
                vacuumPromises.push(
                    new Promise(resolveVacuum => {
                        pg.connect(self._targetConString, (error, client, done) => {
                            if (error) {
                                done();
                                generateError('\t--[runVacuumFullAndAnalyze] Cannot connect to PostgreSQL server...');
                                resolveVacuum();
                            } else {
                                let sql = 'VACUUM (FULL, ANALYZE) "' + self._schema + '"."' + self._tablesToMigrate[i] + '";';
                                client.query(sql, err => {
                                    done();

                                    if (err) {
                                        generateError('\t--[runVacuumFullAndAnalyze] ' + err, sql);
                                        resolveVacuum();
                                    } else {
                                        let msg2 = '\t--[runVacuumFullAndAnalyze] Table "' + self._schema + '"."' + self._tablesToMigrate[i] + '" is VACUUMed...';
                                        log(msg2);
                                        resolveVacuum();
                                    }
                                });
                            }
                        });
                    })
                );
            }
        }

        Promise.all(vacuumPromises).then(() => resolve());
    });
}

/**
 * Migrates structure of a single table to PostgreSql server.
 *
 * @param   {String} tableName
 * @returns {Promise}
 */
function createTable(tableName) {
    return connect().then(
        () => {
            return new Promise((resolveCreateTable, rejectCreateTable) => {
                log('\t--[createTable] Currently creating table: `' + tableName + '`', self._dicTables[tableName].tableLogPath);
                self._mysql.getConnection((error, connection) => {
                    if (error) {
                        // The connection is undefined.
                        generateError('\t--[createTable] Cannot connect to MySQL server...\n' + error);
                        rejectCreateTable();
                    } else {
                        let sql = 'SHOW FULL COLUMNS FROM `' + tableName + '`;';
                        connection.query(sql, (err, rows) => {
                            connection.release();

                            if (err) {
                                generateError('\t--[createTable] ' + err, sql);
                                rejectCreateTable();
                            } else {
                                pg.connect(self._targetConString, (error, client, done) => {
                                    if (error) {
                                        done();
                                        generateError('\t--[createTable] Cannot connect to PostgreSQL server...\n' + error, sql);
                                        rejectCreateTable();
                                    } else {
                                        sql                                        = 'CREATE TABLE "' + self._schema + '"."' + tableName + '"(';
                                        self._dicTables[tableName].arrTableColumns = rows;

                                        for (let i = 0; i < rows.length; ++i) {
                                            let strConvertedType  = mapDataTypes(self._dataTypesMap, rows[i].Type);
                                            sql                  += '"' + rows[i].Field + '" ' + strConvertedType + ',';
                                        }

                                        rows = null;
                                        sql  = sql.slice(0, -1) + ');';
                                        client.query(sql, err => {
                                            done();

                                            if (err) {
                                                generateError('\t--[createTable] ' + err, sql);
                                                rejectCreateTable();
                                            } else {
                                                log(
                                                    '\t--[createTable] Table "' + self._schema + '"."' + tableName + '" is created...',
                                                    self._dicTables[tableName].tableLogPath
                                                );
                                                resolveCreateTable();
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
        () => generateError('\t--[createTable] Cannot establish DB connections...')
    );
}

/**
 * Load a chunk of data using "PostgreSQL COPY".
 *
 * @param   {String} tableName
 * @param   {String} strSelectFieldList
 * @param   {Number} offset
 * @param   {Number} rowsInChunk
 * @param   {Number} rowsCnt
 * @returns {Promise}
 */
function populateTableWorker(tableName, strSelectFieldList, offset, rowsInChunk, rowsCnt) {
    return connect().then(
        () => {
            return new Promise(resolvePopulateTableWorker => {
                self._mysql.getConnection((error, connection) => {
                    if (error) {
                        // The connection is undefined.
                        generateError('\t--[populateTableWorker] Cannot connect to MySQL server...\n\t' + error);
                        resolvePopulateTableWorker();
                    } else {
                        let csvAddr = self._tempDirPath + '/' + tableName + offset + '.csv';
                        let sql     = 'SELECT ' + strSelectFieldList + ' FROM `' + tableName + '` LIMIT ' + offset + ',' + rowsInChunk + ';';

                        connection.query(sql, (err, rows) => {
                            connection.release();

                            if (err) {
                                generateError('\t--[populateTableWorker] ' + err, sql);
                                resolvePopulateTableWorker();
                            } else {
                                rowsInChunk = rows.length;

                                csvStringify(rows, (csvError, csvString) => {
                                    rows = null;

                                    if (csvError) {
                                        generateError('\t--[populateTableWorker] ' + csvError);
                                        resolvePopulateTableWorker();
                                    } else {
                                        let buffer = new Buffer(csvString, self._encoding);
                                        csvString  = null;

                                        fs.open(csvAddr, 'a', self._0777, (csvErrorFputcsvOpen, fd) => {
                                            if (csvErrorFputcsvOpen) {
                                                buffer = null;
                                                generateError('\t--[populateTableWorker] ' + csvErrorFputcsvOpen);
                                                resolvePopulateTableWorker();
                                            } else {
                                                fs.write(fd, buffer, 0, buffer.length, null, csvErrorFputcsvWrite => {
                                                    buffer = null;

                                                    if (csvErrorFputcsvWrite) {
                                                        generateError('\t--[populateTableWorker] ' + csvErrorFputcsvWrite);
                                                        resolvePopulateTableWorker();
                                                    } else {
                                                        pg.connect(self._targetConString, (error, client, done) => {
                                                            if (error) {
                                                                done();
                                                                generateError('\t--[populateTableWorker] Cannot connect to PostgreSQL server...\n' + error, sql);
                                                                resolvePopulateTableWorker();
                                                            } else {
                                                                sql = 'COPY "' + self._schema + '"."' + tableName + '" FROM '
                                                                    + '\'' + csvAddr + '\' DELIMITER \'' + ',\'' + ' CSV;';

                                                                client.query(sql, (err, result) => {
                                                                    done();

                                                                    if (err) {
                                                                        generateError('\t--[populateTableWorker] ' + err, sql);

                                                                        if (self._copyOnly.indexOf(tableName) === -1) {
                                                                            populateTableByInsert(tableName, strSelectFieldList, offset, rowsInChunk, () => {
                                                                                let msg = '\t--[populateTableWorker]  For now inserted: ' + self._dicTables[tableName].totalRowsInserted + ' rows, '
                                                                                        + 'Total rows to insert into "' + self._schema + '"."' + tableName + '": ' + rowsCnt;

                                                                                log(msg);
                                                                                fs.unlink(csvAddr, () => {
                                                                                    fs.close(fd, () => {
                                                                                        global.gc();
                                                                                        resolvePopulateTableWorker();
                                                                                    });
                                                                                });
                                                                            });
                                                                        } else {
                                                                            let msg = '\t--[populateTableWorker]  For now inserted: ' + self._dicTables[tableName].totalRowsInserted + ' rows, '
                                                                                    + 'Total rows to insert into "' + self._schema + '"."' + tableName + '": ' + rowsCnt;

                                                                            log(msg);
                                                                            fs.unlink(csvAddr, () => {
                                                                                fs.close(fd, () => {
                                                                                    global.gc();
                                                                                    resolvePopulateTableWorker();
                                                                                });
                                                                            });
                                                                        }

                                                                    } else {
                                                                        self._dicTables[tableName].totalRowsInserted += result.rowCount;
                                                                        let msg = '\t--[populateTableWorker]  For now inserted: ' + self._dicTables[tableName].totalRowsInserted + ' rows, '
                                                                                + 'Total rows to insert into "' + self._schema + '"."' + tableName + '": ' + rowsCnt;

                                                                        log(msg);
                                                                        fs.unlink(csvAddr, () => {
                                                                            fs.close(fd, () => {
                                                                                global.gc();
                                                                                resolvePopulateTableWorker();
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
        () => generateError('\t--[populateTableWorker] Cannot establish DB connections...')
    );
}

/**
 * Populates data using INSERT statment.
 *
 * @param   {String}   tableName
 * @param   {String}   strSelectFieldList
 * @param   {Number}   offset
 * @param   {Number}   rowsInChunk
 * @param   {Function} callback
 * @returns {undefined}
 */
function populateTableByInsert(tableName, strSelectFieldList, offset, rowsInChunk, callback) {
    connect().then(
        () => {
            return new Promise(resolve => {
                self._mysql.getConnection((error, connection) => {
                    if (error) {
                        // The connection is undefined.
                        generateError('\t--[populateTableByInsert] Cannot connect to MySQL server...\n\t' + error);
                        resolve();
                    } else {
                        let sql = 'SELECT ' + strSelectFieldList + ' FROM `' + tableName + '` LIMIT ' + offset + ',' + rowsInChunk + ';';
                        connection.query(sql, (err, rows) => {
                            connection.release();

                            if (err) {
                                generateError('\t--[populateTableByInsert] ' + err, sql);
                                resolve();
                            } else {
                                let insertPromises = [];

                                for (let i = 0; i < rows.length; ++i) {
                                    insertPromises.push(
                                        new Promise(resolveInsert => {
                                            pg.connect(self._targetConString, (error, client, done) => {
                                                if (error) {
                                                    done();
                                                    let msg = '\t--[populateTableByInsert] Cannot connect to PostgreSQL server...\n' + error;
                                                    generateError(msg);
                                                    resolveInsert();
                                                } else {
                                                    let sql                = 'INSERT INTO "' + self._schema + '"."' + tableName + '"';
                                                    let valuesPlaceHolders = ' VALUES(';
                                                    let valuesData         = [];
                                                    let cnt                = 1;

                                                    for (let strColumnName in rows[i]) {
                                                        valuesPlaceHolders  += '$' + cnt + ',';
                                                        valuesData.push(rows[i][strColumnName]);
                                                        cnt++;
                                                    }

                                                    sql += valuesPlaceHolders.slice(0, -1) + ');';
                                                    client.query(sql, valuesData, err => {
                                                        done();

                                                        if (err) {
                                                            generateError('\t--[populateTableByInsert] INSERT failed...\n' + err, sql);
                                                            resolveInsert();
                                                        } else {
                                                            self._dicTables[tableName].totalRowsInserted++;
                                                            resolveInsert();
                                                        }
                                                    });
                                                }
                                            });
                                        })
                                    );
                                }

                                Promise.all(insertPromises).then(() => resolve());
                            }
                        });
                    }
                });
            });
        }
    ).then(
        () => callback()
    ).catch(
        () => callback()
    );
}

/**
 * Define which columns of the given table are of type "enum".
 * Set an appropriate constraint, if need.
 *
 * @param   {String} tableName
 * @returns {Promise}
 */
function processEnum(tableName) {
    return new Promise(resolve => {
        log('\t--[processEnum] Defines "ENUMs" for table "' + self._schema + '"."' + tableName + '"', self._dicTables[tableName].tableLogPath);
        let processEnumPromises = [];

        for (let i = 0; i < self._dicTables[tableName].arrTableColumns.length; ++i) {
            if (self._dicTables[tableName].arrTableColumns[i].Type.indexOf('(') !== -1) {
                let arrType = self._dicTables[tableName].arrTableColumns[i].Type.split('(');

                if ('enum' === arrType[0]) {
                    processEnumPromises.push(
                        new Promise(resolveProcessEnum => {
                            pg.connect(self._targetConString, (error, client, done) => {
                                if (error) {
                                    done();
                                    let msg = '\t--[processEnum] Cannot connect to PostgreSQL server...\n' + error;
                                    generateError(msg);
                                    resolveProcessEnum();
                                } else {
                                    let sql = 'ALTER TABLE "' + self._schema + '"."' + tableName + '" '
                                            + 'ADD CHECK ("' + self._dicTables[tableName].arrTableColumns[i].Field + '" IN (' + arrType[1] + ');';

                                    client.query(sql, err => {
                                        done();

                                        if (err) {
                                            let msg = '\t--[processEnum] Error while setting ENUM for "' + self._schema + '"."'
                                                    + tableName + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '"...\n' + err;

                                            generateError(msg, sql);
                                            resolveProcessEnum();
                                        } else {
                                            let success = '\t--[processEnum] Set "ENUM" for "' + self._schema + '"."' + tableName
                                                        + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '"...';

                                            log(success, self._dicTables[tableName].tableLogPath);
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

        Promise.all(processEnumPromises).then(() => resolve());
    });
}

/**
 * Define which columns of the given table can contain the "NULL" value.
 * Set an appropriate constraint, if need.
 *
 * @param   {String} tableName
 * @returns {Promise}
 */
function processNull(tableName) {
    return new Promise(resolve => {
        log('\t--[processNull] Defines "NULLs" for table: "' + self._schema + '"."' + tableName + '"', self._dicTables[tableName].tableLogPath);
        let processNullPromises = [];

        for (let i = 0; i < self._dicTables[tableName].arrTableColumns.length; ++i) {
            if (self._dicTables[tableName].arrTableColumns[i].Null.toLowerCase() === 'no') {
                processNullPromises.push(
                    new Promise(resolveProcessNull => {
                        pg.connect(self._targetConString, (error, client, done) => {
                            if (error) {
                                done();
                                let msg = '\t--[processNull] Cannot connect to PostgreSQL server...\n' + error;
                                generateError(msg);
                                resolveProcessNull();
                            } else {
                                let sql = 'ALTER TABLE "' + self._schema + '"."' + tableName
                                        + '" ALTER COLUMN "' + self._dicTables[tableName].arrTableColumns[i].Field + '" SET NOT NULL;';

                                client.query(sql, err => {
                                    done();

                                    if (err) {
                                        let msg = '\t--[processNull] Error while setting NULL for "' + self._schema + '"."'
                                                + tableName + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '"...\n' + err;

                                        generateError(msg, sql);
                                        resolveProcessNull();
                                    } else {
                                        let success = '\t--[processNull] Set NULL for "' + self._schema + '"."' + tableName
                                                    + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '"...';

                                        log(success, self._dicTables[tableName].tableLogPath);
                                        resolveProcessNull();
                                    }
                                });
                            }
                        });
                    })
                );
            }
        }

        Promise.all(processNullPromises).then(() => resolve());
    });
}

/**
 * Define which columns of the given table have default value.
 * Set default values, if need.
 *
 * @param   {String} tableName
 * @returns {Promise}
 */
function processDefault(tableName) {
    return new Promise(resolve => {
        log('\t--[processDefault] Defines default values for table: "' + self._schema + '"."' + tableName + '"', self._dicTables[tableName].tableLogPath);
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
            'UTC_TIMESTAMP'       : "(NOW() AT TIME ZONE 'UTC')"
        };

        for (let i = 0; i < self._dicTables[tableName].arrTableColumns.length; ++i) {
            if (self._dicTables[tableName].arrTableColumns[i].Default) {
                processDefaultPromises.push(
                    new Promise(resolveProcessDefault => {
                        pg.connect(self._targetConString, (error, client, done) => {
                            if (error) {
                                done();
                                let msg = '\t--[processDefault] Cannot connect to PostgreSQL server...\n' + error;
                                generateError(msg);
                                resolveProcessDefault();
                            } else {
                                let sql = 'ALTER TABLE "' + self._schema + '"."' + tableName
                                        + '" ' + 'ALTER COLUMN "' + self._dicTables[tableName].arrTableColumns[i].Field + '" SET DEFAULT ';

                                if (sqlReservedValues[self._dicTables[tableName].arrTableColumns[i].Default]) {
                                    sql += sqlReservedValues[self._dicTables[tableName].arrTableColumns[i].Default] + ';';
                                } else {
                                    sql += isFloatNumeric(self._dicTables[tableName].arrTableColumns[i].Default)
                                           ? self._dicTables[tableName].arrTableColumns[i].Default + ';'
                                           : "'" + self._dicTables[tableName].arrTableColumns[i].Default + "';";
                                }

                                client.query(sql, err => {
                                    done();

                                    if (err) {
                                        let msg = '\t--[processDefault] Error occurred when tried to set default value for "'
                                                + self._schema + '"."' + tableName
                                                + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '"...\n' + err;

                                        generateError(msg, sql);
                                        resolveProcessDefault();
                                    } else {
                                        let success = '\t--[processDefault] Set default value for "' + self._schema + '"."' + tableName
                                                    + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '"...';

                                        log(success, self._dicTables[tableName].tableLogPath);
                                        resolveProcessDefault();
                                    }
                                });
                            }
                        });
                    })
                );
            }
        }

        Promise.all(processDefaultPromises).then(() => resolve());
    });
}

/**
 * Define which column in given table has the "auto_increment" attribute.
 * Create an appropriate sequence.
 *
 * @param   {String} tableName
 * @returns {Promise}
 */
function createSequence(tableName) {
    return new Promise(resolve => {
        let createSequencePromises = [];

        for (let i = 0; i < self._dicTables[tableName].arrTableColumns.length; ++i) {
            if (self._dicTables[tableName].arrTableColumns[i].Extra === 'auto_increment') {
                createSequencePromises.push(
                    new Promise(resolveCreateSequence => {
                        let seqName = tableName + '_' + self._dicTables[tableName].arrTableColumns[i].Field + '_seq';
                        log('\t--[createSequence] Trying to create sequence : "' + self._schema + '"."' + seqName + '"', self._dicTables[tableName].tableLogPath);
                        pg.connect(self._targetConString, (error, client, done) => {
                            if (error) {
                                done();
                                let msg = '\t--[createSequence] Cannot connect to PostgreSQL server...\n' + error;
                                generateError(msg);
                                resolveCreateSequence();
                            } else {
                                let sql = 'CREATE SEQUENCE "' + self._schema + '"."' + seqName + '";';
                                client.query(sql, err => {
                                    if (err) {
                                        done();
                                        let errMsg = '\t--[createSequence] Failed to create sequence "' + self._schema + '"."' + seqName + '"';
                                        generateError(errMsg, sql);
                                        resolveCreateSequence();
                                    } else {
                                         sql = 'ALTER TABLE "' + self._schema + '"."' + tableName + '" '
                                             + 'ALTER COLUMN "' + self._dicTables[tableName].arrTableColumns[i].Field + '" '
                                             + 'SET DEFAULT NEXTVAL(\'"' + self._schema + '"."' + seqName + '"\');';

                                         client.query(sql, err2 => {
                                             if (err2) {
                                                 done();
                                                 let err2Msg = '\t--[createSequence] Failed to set default value for "' + self._schema + '"."'
                                                            + tableName + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '"...'
                                                            + '\n\t--[createSequence] Note: sequence "' + self._schema + '"."' + seqName + '" was created...';

                                                 generateError(err2Msg, sql);
                                                 resolveCreateSequence();
                                             } else {
                                                   sql = 'ALTER SEQUENCE "' + self._schema + '"."' + seqName + '" '
                                                       + 'OWNED BY "' + self._schema + '"."' + tableName
                                                       + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '";';

                                                   client.query(sql, err3 => {
                                                        if (err3) {
                                                            done();
                                                            let err3Msg = '\t--[createSequence] Failed to relate sequence "' + self._schema + '"."' + seqName + '" to '
                                                                       + '"' + self._schema + '"."'
                                                                       + tableName + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '"...';

                                                            generateError(err3Msg, sql);
                                                            resolveCreateSequence();
                                                        } else {
                                                           sql = 'SELECT SETVAL(\'"' + self._schema + '"."' + seqName + '"\', '
                                                               + '(SELECT MAX("' + self._dicTables[tableName].arrTableColumns[i].Field + '") FROM "'
                                                               + self._schema + '"."' + tableName + '"));';

                                                           client.query(sql, err4 => {
                                                              done();

                                                              if (err4) {
                                                                  let err4Msg = '\t--[createSequence] Failed to set max-value of "' + self._schema + '"."'
                                                                              + tableName + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '" '
                                                                              + 'as the "NEXTVAL of "' + self._schema + '"."' + seqName + '"...';

                                                                  generateError(err4Msg, sql);
                                                                  resolveCreateSequence();
                                                              } else {
                                                                  let success = '\t--[createSequence] Sequence "' + self._schema + '"."' + seqName + '" is created...';
                                                                  log(success, self._dicTables[tableName].tableLogPath);
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

        Promise.all(createSequencePromises).then(() => resolve());
    });
}

/**
 * Create primary key and indices.
 *
 * @param   {String} tableName
 * @returns {Promise}
 */
function processIndexAndKey(tableName) {
    return connect().then(() => {
        return new Promise(resolveProcessIndexAndKey => {
            self._mysql.getConnection((error, connection) => {
                if (error) {
                    // The connection is undefined.
                    generateError('\t--[processIndexAndKey] Cannot connect to MySQL server...\n\t' + error);
                    resolveProcessIndexAndKey();
                } else {
                    let sql = 'SHOW INDEX FROM `' + tableName + '`;';
                    connection.query(sql, (err, arrIndices) => {
                        connection.release();

                        if (err) {
                            generateError('\t--[processIndexAndKey] ' + err, sql);
                            resolveProcessIndexAndKey();
                        } else {
                            let objPgIndices               = Object.create(null);
                            let cnt                        = 0;
                            let indexType                  = '';
                            let processIndexAndKeyPromises = [];

                            for (let i = 0; i < arrIndices.length; ++i) {
                                if (arrIndices[i].Key_name in objPgIndices) {
                                    objPgIndices[arrIndices[i].Key_name].column_name.push('"' + arrIndices[i].Column_name + '"');
                                } else {
                                    objPgIndices[arrIndices[i].Key_name] = {
                                        is_unique   : arrIndices[i].Non_unique === 0 ? true : false,
                                        column_name : ['"' + arrIndices[i].Column_name + '"'],
                                        Index_type  : ' USING ' + (arrIndices[i].Index_type === 'SPATIAL' ? 'GIST' : arrIndices[i].Index_type)
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
                                                generateError(msg);
                                                resolveProcessIndexAndKeySql();
                                            } else {
                                                if (attr.toLowerCase() === 'primary') {
                                                    indexType = 'PK';
                                                    sql       = 'ALTER TABLE "' + self._schema + '"."' + tableName + '" '
                                                              + 'ADD PRIMARY KEY(' + objPgIndices[attr].column_name.join(',') + ');';

                                                } else {
                                                    // "schema_idxname_{integer}_idx" - is NOT a mistake.
                                                    let columnName = objPgIndices[attr].column_name[0].slice(1, -1) + cnt++;
                                                    indexType      = 'index';
                                                    sql            = 'CREATE ' + (objPgIndices[attr].is_unique ? 'UNIQUE ' : '') + 'INDEX "'
                                                                   + self._schema + '_' + tableName + '_' + columnName + '_idx" ON "'
                                                                   + self._schema + '"."' + tableName + '" '
                                                                   + objPgIndices[attr].Index_type + ' (' + objPgIndices[attr].column_name.join(',') + ');';
                                                }

                                                pgClient.query(sql, err2 => {
                                                    done();

                                                    if (err2) {
                                                        generateError('\t--[processIndexAndKey] ' + err2, sql);
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
                                let success = '\t--[processIndexAndKey] "' + self._schema + '"."' + tableName + '": PK/indices are successfully set...';
                                log(success, self._dicTables[tableName].tableLogPath);
                                resolveProcessIndexAndKey();
                            });
                        }
                    });
                }
            });
        });
    });
}

/**
 * Create comments.
 *
 * @param   {String} tableName
 * @returns {Promise}
 */
function processComment(tableName) {
    return new Promise(resolve => {
        log('\t--[processComment] Creates comments for table "' + self._schema + '"."' + tableName + '"...', self._dicTables[tableName].tableLogPath);
        let arrCommentPromises = [];

        for (let i = 0; i < self._dicTables[tableName].arrTableColumns.length; ++i) {
            if (self._dicTables[tableName].arrTableColumns[i].Comment !== '') {
                arrCommentPromises.push(
                    new Promise(resolveComment => {
                        pg.connect(self._targetConString, (error, client, done) => {
                            if (error) {
                                done();
                                let msg = '\t--[processComment] Cannot connect to PostgreSQL server...\n' + error;
                                generateError(msg);
                                resolveComment();
                            } else {
                                let sql = 'COMMENT ON COLUMN "' + self._schema + '"."' + tableName + '"."'
                                        + self._dicTables[tableName].arrTableColumns[i].Field
                                        + '" IS \'' + self._dicTables[tableName].arrTableColumns[i].Comment + '\';';

                                client.query(sql, err => {
                                    done();

                                    if (err) {
                                        let msg = '\t--[processComment] Error while processing comment for "' + self._schema + '"."'
                                                + tableName + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '"...\n' + err;

                                        generateError(msg, sql);
                                        resolveComment();
                                    } else {
                                        let success = '\t--[processComment] Set comment for "' + self._schema + '"."' + tableName
                                                      + '" column: "' + self._dicTables[tableName].arrTableColumns[i].Field + '"...';

                                        log(success, self._dicTables[tableName].tableLogPath);
                                        resolveComment();
                                    }
                                });
                            }
                        });
                    })
                );
            }
        }

        Promise.all(arrCommentPromises).then(() => resolve());
    });
}

/**
 * Processes current table before data loading.
 *
 * @param   {String} tableName
 * @returns {Promise}
 */
function processTableBeforeDataLoading(tableName) {
    return connect().then(
        () => {
            return createTable(tableName);
        },
        () => {
            generateError('\t--[processTableBeforeDataLoading] Cannot establish DB connections...');
        }
    ).then(
        () => {
            return prepareArrayOfTablesAndChunkOffsets(tableName);
        },
        () => {
            generateError('\t--[processTableBeforeDataLoading] Cannot create table "' + self._schema + '"."' + tableName + '"...');
        }
    ).catch(() => {
        generateError('\t--[processTableBeforeDataLoading] Cannot establish DB connections...');
    });
}

/**
 * Closes DB connections.
 *
 * @returns {Promise}
 */
function closeConnections() {
    return new Promise(resolve => {
        if (self._mysql) {
            self._mysql.end(error => {
                if (error) {
                    log('\t--[closeConnections] ' + error);
                }

                log('\t--[closeConnections] All DB connections to both MySQL and PostgreSQL servers have been closed...');
                pg.end();
                resolve();
            });
        } else {
            log('\t--[closeConnections] All DB connections to both MySQL and PostgreSQL servers have been closed...');
            pg.end();
            resolve();
        }
    });
}

/**
 * Closes DB connections and removes the "./temporary_directory".
 *
 * @returns {Promise}
 */
function cleanup() {
    log('\t--[cleanup] Cleanup resources...');
    return removeTemporaryDirectory().then(
        closeConnections
    ).then(() => {
        return new Promise(resolve => {
            log('\t--[cleanup] Cleanup finished...');
            resolve();
        });
    });
}

/**
 * Generates a summary report.
 *
 * @param   {String} endMsg
 * @returns {undefined}
 */
function generateReport(endMsg) {
    let differenceSec = ((new Date()) - self._timeBegin) / 1000;
    let seconds       = Math.floor(differenceSec % 60);
    differenceSec     = differenceSec / 60;
    let minutes       = Math.floor(differenceSec % 60);
    let hours         = Math.floor(differenceSec / 60);
    hours             = hours < 10 ? '0' + hours : hours;
    minutes           = minutes < 10 ? '0' + minutes : minutes;
    seconds           = seconds < 10 ? '0' + seconds : seconds;
    let output        = '\t--[generateReport] ' + endMsg
                      + '\n\t--[generateReport] Total time: ' + hours + ':' + minutes + ':' + seconds
                      + '\n\t--[generateReport] (hours:minutes:seconds)';

    log(output);
    process.exit();
}

/**
 * Prepares an array of tables and chunk offsets.
 *
 * @param   {String} tableName
 * @returns {Promise}
 */
function prepareArrayOfTablesAndChunkOffsets(tableName) {
    return connect().then(
        () => {
            return new Promise(resolve => {
                self._mysql.getConnection((error, connection) => {
                    if (error) {
                        // The connection is undefined.
                        generateError('\t--[prepareArrayOfTablesAndChunkOffsets] Cannot connect to MySQL server...\n\t' + error);
                        resolve();
                    } else {
                        // Determine current table size, apply "chunking".
                        let sql = "SELECT ((data_length + index_length) / 1024) AS size_in_kb "
                                + "FROM information_schema.TABLES "
                                + "WHERE table_schema = '" + self._mySqlDbName + "' "
                                + "AND table_name = '" + tableName + "';";

                        connection.query(sql, (err, rows) => {
                            if (err) {
                                connection.release();
                                generateError('\t--[prepareArrayOfTablesAndChunkOffsets] ' + err, sql);
                                resolve();
                            } else {
                                let tableSizeInKb      = rows[0].size_in_kb;
                                tableSizeInKb          = tableSizeInKb < 1 ? 1 : tableSizeInKb;
                                rows                   = null;
                                let strSelectFieldList = arrangeColumnsData(self._dicTables[tableName].arrTableColumns);
                                sql                    = 'SELECT COUNT(1) AS rows_count FROM `' + tableName + '`;';

                                connection.query(sql, (err2, rows2) => {
                                    connection.release();

                                    if (err2) {
                                        generateError('\t--[prepareArrayOfTablesAndChunkOffsets] ' + err2, sql);
                                        resolve();
                                    } else {
                                        let rowsCnt             = rows2[0].rows_count;
                                        rows2                   = null;
                                        let chunksCnt           = tableSizeInKb / self._dataChunkSize;
                                        chunksCnt               = chunksCnt < 1 ? 1 : chunksCnt;
                                        let rowsInChunk         = Math.ceil(rowsCnt / chunksCnt);
                                        let arrDataPoolPromises = [];
                                        let msg                 = '\t--[prepareArrayOfTablesAndChunkOffsets] Total rows to insert into '
                                                                + '"' + self._schema + '"."' + tableName + '": ' + rowsCnt;

                                        log(msg, self._dicTables[tableName].tableLogPath);

                                        for (let offset = 0; offset < rowsCnt; offset += rowsInChunk) {
                                            arrDataPoolPromises.push(new Promise(resolveDataUnit => {
                                                pg.connect(self._targetConString, (error, client, done) => {
                                                    if (error) {
                                                        done();
                                                        generateError('\t--[prepareArrayOfTablesAndChunkOffsets] Cannot connect to PostgreSQL server...\n' + error);
                                                        resolveDataUnit();
                                                    } else {
                                                        let strJson = '{"_tableName":"' + tableName
                                                                    + '","_selectFieldList":"' + strSelectFieldList + '",'
                                                                    + '"_offset":' + offset + ','
                                                                    + '"_rowsInChunk":' + rowsInChunk + ','
                                                                    + '"_rowsCnt":' + rowsCnt + '}';

                                                        let sql = 'INSERT INTO "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '" VALUES($1);';
                                                        client.query(sql, [strJson], err => {
                                                            done();

                                                            if (err) {
                                                                generateError('\t--[prepareArrayOfTablesAndChunkOffsets] INSERT failed...\n' + err, sql);
                                                            }

                                                            resolveDataUnit();
                                                        });
                                                    }
                                                });
                                            }));
                                        }

                                        Promise.all(arrDataPoolPromises).then(() => resolve());
                                    }
                                });
                            }
                        });
                    }
                });
            });
        },
        () => generateError('\t--[prepareArrayOfTablesAndChunkOffsets] Cannot establish DB connections...')
    );
}

/**
 * Reads temporary table, and generates Data-pool.
 *
 * @returns {Promise}
 */
function readDataPool() {
    return new Promise((resolve, reject) => {
        pg.connect(self._targetConString, (error, client, done) => {
            if (error) {
                done();
                generateError('\t--[readDataPool] Cannot connect to PostgreSQL server...\n' + error);
                reject();
            } else {
                let sql = 'SELECT json AS json FROM "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '";';
                client.query(sql, (err, arrDataPool) => {
                    done();

                    if (err) {
                        generateError('\t--[readDataPool] ' + err, sql);
                        return reject();
                    }

                    for (let i = 0; i < arrDataPool.rows.length; ++i) {
                        self._dataPool.push(JSON.parse(arrDataPool.rows[i].json));
                    }

                    log('\t--[readDataPool] Data-Pool is loaded...');
                    resolve();
                });
            }
        });
    });
}

/**
 * Instructs loadData() function which DataUnits should be load.
 *
 * @returns {undefined}
 */
function dataPipe() {
    if (self._dataPool.length === 0) {
        return continueProcessAfterDataLoading();
    }

    let intProcessedDataUnits = 0;
    loadData(self._dataPool.slice(intProcessedDataUnits, self._dataPool.length - (self._dataPool.length - self._pipeWidth)));

    eventEmitter.on('processed', () => {
        intProcessedDataUnits += self._pipeWidth;

        if (intProcessedDataUnits < self._dataPool.length) {
            let intEnd = self._dataPool.length - (self._dataPool.length  - self._pipeWidth - intProcessedDataUnits);
            loadData(self._dataPool.slice(intProcessedDataUnits, intEnd));
        } else {
            return continueProcessAfterDataLoading();
        }
    });
}

/**
 * Loads DataUnits chunk into PostgreSQL server.
 *
 * @param   {Array} arrDataUnitsChunk
 * @returns {undefined}
 */
function loadData(arrDataUnitsChunk) {
    let arrPromises = [];

    for (let i = 0; i < arrDataUnitsChunk.length; ++i) {
        arrPromises.push(populateTableWorker(
            arrDataUnitsChunk[i]._tableName,
            arrDataUnitsChunk[i]._selectFieldList,
            arrDataUnitsChunk[i]._offset,
            arrDataUnitsChunk[i]._rowsInChunk,
            arrDataUnitsChunk[i]._rowsCnt
        ));
    }

    Promise.all(arrPromises).then(() => eventEmitter.emit('processed'));
}

/**
 * Continues the process after data loading.
 *
 * @returns {undefined}
 */
function continueProcessAfterDataLoading() {
    let promises = [];

    for (let i = 0; i < self._tablesToMigrate.length; ++i) {
        let tableName = self._tablesToMigrate[i];
        promises.push(
            processEnum(tableName).then(() => {
                processNull(tableName);
            }).then(() => {
                processDefault(tableName);
            }).then(() => {
                createSequence(tableName);
            }).then(() => {
                processIndexAndKey(tableName);
            }).then(() => {
                processComment(tableName);
            })
        );
    }

    Promise.all(promises).then(() => {
        processForeignKey().then(
            dropDataPoolTable
        ).then(
            processView
        ).then(
            runVacuumFullAndAnalyze
        ).then(
            cleanup
        ).then(
            () => generateReport('NMIG migration is accomplished.')
        );
    });
}

/**
 * Runs migration according to user's configuration.
 *
 * @param   {Object} config
 * @returns {undefined}
 */
module.exports = function(config) {
    self._config = config;
    boot().then(
        createLogsDirectory,
        () => {
            // Braces are essential. Without them promises-chain will continue execution.
            console.log('\t--[FromMySQL2PostgreSQL] Failed to boot migration');
        }
    ).then(
        createTemporaryDirectory,
        () => {
            // Braces are essential. Without them promises-chain will continue execution.
            log('\t--[FromMySQL2PostgreSQL] Logs directory was not created...');
        }
    ).then(
        createSchema,
        () => {
            let msg = '\t--[FromMySQL2PostgreSQL] The temporary directory [' + self._tempDirPath + '] already exists...'
                    + '\n\t  Please, remove this directory and rerun NMIG...';

            log(msg);
        }
    ).then(
        createDataPoolTable,
        () => {
            log('\t--[FromMySQL2PostgreSQL] Cannot create new DB schema...');
            cleanup();
        }
    ).then(
        loadStructureToMigrate,
        () => {
            log('\t--[FromMySQL2PostgreSQL] Cannot create data-pool...');
            cleanup();
        }
    ).then(
        readDataPool,
        () => {
            log('\t--[FromMySQL2PostgreSQL] NMIG cannot load source database structure...');
            cleanup();
        }
    ).then(
        dataPipe,
        () => {
            log('\t--[FromMySQL2PostgreSQL] NMIG failed to load Data-Units pool...');
            cleanup();
        }
    );
};
