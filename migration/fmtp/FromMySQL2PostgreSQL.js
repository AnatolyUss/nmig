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

const fs                  = require('fs');
const childProcess        = require('child_process');
const generateView        = require('./ViewGenerator');
const arrangeColumnsData  = require('./ColumnsDataArranger');
const readDataTypesMap    = require('./DataTypesMapReader');
const log                 = require('./Logger');
const generateError       = require('./ErrorGenerator');
const connect             = require('./Connector');
const Table               = require('./Table');
const Conversion          = require('./Conversion');
const MessageToDataLoader = require('./MessageToDataLoader');

let self                  = null;
let intProcessedDataUnits = 0;

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

        if ('enum' === strDataType || 'set' === strDataType) {
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
 * Creates temporary directory.
 *
 * @returns {Promise}
 */
function createTemporaryDirectory() {
    return new Promise((resolve, reject) => {
        log(self, '\t--[createTemporaryDirectory] Creating temporary directory...');
        fs.stat(self._tempDirPath, (directoryDoesNotExist, stat) => {
            if (directoryDoesNotExist) {
                fs.mkdir(self._tempDirPath, self._0777, e => {
                    if (e) {
                        let msg = '\t--[createTemporaryDirectory] Cannot perform a migration due to impossibility to create '
                                + '"temporary_directory": ' + self._tempDirPath;

                        log(self, msg);
                        reject();
                    } else {
                        log(self, '\t--[createTemporaryDirectory] Temporary directory is created...');
                        resolve();
                    }
                });
            } else if (!stat.isDirectory()) {
                log(self, '\t--[createTemporaryDirectory] Cannot perform a migration due to unexpected error');
                reject();
            } else {
                resolve();
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
        fs.readdir(self._tempDirPath, (err, arrContents) => {
            let msg = '';

            if (err) {
                msg = '\t--[removeTemporaryDirectory] Note, TemporaryDirectory located at "'
                    + self._tempDirPath + '" is not removed \n\t--[removeTemporaryDirectory] ' + err;

                log(self, msg);
                resolve();

            } else {
                let promises = [];

                for (let i = 0; i < arrContents.length; ++i) {
                    promises.push(new Promise(resolveUnlink => {
                        fs.unlink(self._tempDirPath + '/' + arrContents[i], () => resolveUnlink());
                    }));
                }

                Promise.all(promises).then(() => {
                    fs.rmdir(self._tempDirPath, error => {
                        if (error) {
                            msg = '\t--[removeTemporaryDirectory] Note, TemporaryDirectory located at "'
                                + self._tempDirPath + '" is not removed \n\t--[removeTemporaryDirectory] ' + error;
                        } else {
                            msg = '\t--[removeTemporaryDirectory] TemporaryDirectory located at "'
                                + self._tempDirPath + '" is removed';
                        }

                        log(self, msg);
                        resolve();
                    });
                });
            }
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
                        log(self, '\t--[createLogsDirectory] Logs directory is created...');
                        resolve();
                    }
                });
            } else if (!stat.isDirectory()) {
                console.log('\t--[createLogsDirectory] Cannot perform a migration due to unexpected error');
                reject();
            } else {
                log(self, '\t--[createLogsDirectory] Logs directory already exists...');
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
                    log(self, '\t--[logNotCreatedView] ' + e);
                } else {
                    log(self, '\t--[logNotCreatedView] "not_created_views" directory is created...');
                    // "not_created_views" directory is created. Can write the log...
                    fs.open(self._notCreatedViewsPath + '/' + viewName + '.sql', 'w', self._0777, (error, fd) => {
                        if (error) {
                            log(self, error);
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
            log(self, '\t--[logNotCreatedView] Cannot write the log due to unexpected error');
        } else {
            // "not_created_views" directory already exists. Can write the log...
            fs.open(self._notCreatedViewsPath + '/' + viewName + '.sql', 'w', self._0777, (error, fd) => {
                if (error) {
                    log(self, error);
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
 * Create a new database schema.
 * Insure a uniqueness of a new schema name.
 *
 * @returns {Promise}
 */
function createSchema() {
    return connect(self).then(() => {
        return new Promise((resolve, reject) => {
            self._pg.connect((error, client, done) => {
                if (error) {
                    generateError(self, '\t--[createSchema] Cannot connect to PostgreSQL server...\n' + error);
                    reject();
                } else {
                    let sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '" + self._schema + "';";
                    client.query(sql, (err, result) => {
                        if (err) {
                            done();
                            generateError(self, '\t--[createSchema] ' + err, sql);
                            reject();
                        } else if (result.rows.length === 0) {
                            sql = 'CREATE SCHEMA "' + self._schema + '";';
                            client.query(sql, err => {
                                done();

                                if (err) {
                                    generateError(self, '\t--[createSchema] ' + err, sql);
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
    });
}

/**
 * Get state-log.
 *
 * @param   {String} param
 * @returns {Promise}
 */
function getStatelog(param) {
    return connect(self).then(() => {
        return new Promise(resolve => {
            self._pg.connect((error, client, done) => {
                if (error) {
                    generateError(self, '\t--[getStateLog] Cannot connect to PostgreSQL server...\n' + error);
                    resolve(false);
                } else {
                    let sql = 'SELECT ' + param + ' FROM "' + self._schema + '"."state_logs_' + self._schema + self._mySqlDbName + '";';
                    client.query(sql, (err, data) => {
                        done();

                        if (err) {
                            generateError(self, '\t--[getStateLog] ' + err, sql);
                            resolve(false);
                        } else {
                            resolve(data.rows[0][param]);
                        }
                    });
                }
            });
        });
    });
}

/**
 * Update the state-log.
 *
 * @param   {String} param
 * @returns {Promise}
 */
function updateStatelog(param) {
    return connect(self).then(() => {
        return new Promise(resolve => {
            self._pg.connect((error, client, done) => {
                if (error) {
                    generateError(self, '\t--[updateStateLog] Cannot connect to PostgreSQL server...\n' + error);
                    resolve();
                } else {
                    let sql = 'UPDATE "' + self._schema + '"."state_logs_'
                            + self._schema + self._mySqlDbName + '" SET ' + param + ' = TRUE;';

                    client.query(sql, err => {
                        done();

                        if (err) {
                            generateError(self, '\t--[updateStateLog] ' + err, sql);
                        }

                        resolve();
                    });
                }
            });
        });
    });
}

/**
 * Create the "{schema}"."state_logs_{self._schema + self._mySqlDbName} temporary table."
 *
 * @returns {Promise}
 */
function createStateLogsTable() {
    return connect(self).then(() =>{
        return new Promise((resolve, reject) => {
            self._pg.connect((error, client, done) => {
                if (error) {
                    generateError(self, '\t--[createStateLogsTable] Cannot connect to PostgreSQL server...\n' + error);
                    reject();
                } else {
                    let sql = 'CREATE TABLE IF NOT EXISTS "' + self._schema + '"."state_logs_' + self._schema + self._mySqlDbName
                            + '"('
                            + '"tables_loaded" BOOLEAN,'
                            + '"per_table_constraints_loaded" BOOLEAN,'
                            + '"foreign_keys_loaded" BOOLEAN,'
                            + '"views_loaded" BOOLEAN'
                            + ');';

                    client.query(sql, err => {
                        if (err) {
                            done();
                            generateError(self, '\t--[createStateLogsTable] ' + err, sql);
                            reject();
                        } else {
                            sql = 'SELECT COUNT(1) AS cnt FROM "' + self._schema + '"."state_logs_' + self._schema + self._mySqlDbName + '";';
                            client.query(sql, (errorCount, result) => {
                                if (errorCount) {
                                    done();
                                    generateError(self, '\t--[createStateLogsTable] ' + errorCount, sql);
                                    reject();
                                } else if (+result.rows[0].cnt === 0) {
                                    sql = 'INSERT INTO "' + self._schema + '"."state_logs_' + self._schema + self._mySqlDbName
                                        + '" VALUES(FALSE, FALSE, FALSE, FALSE);';

                                    client.query(sql, errorInsert => {
                                        done();

                                        if (errorInsert) {
                                            generateError(self, '\t--[createStateLogsTable] ' + errorInsert, sql);
                                            reject();
                                        } else {
                                            let msg = '\t--[createStateLogsTable] table "' + self._schema + '"."state_logs_'
                                                    + self._schema + self._mySqlDbName + '" is created...';

                                            log(self, msg);
                                            resolve();
                                        }
                                    });
                                } else {
                                    let msg = '\t--[createStateLogsTable] table "' + self._schema + '"."state_logs_'
                                            + self._schema + self._mySqlDbName + '" is created...';

                                    log(self, msg);
                                    resolve();
                                }
                            });
                        }
                    });
                }
            });
        });
    });
}

/**
 * Drop the "{schema}"."state_logs_{self._schema + self._mySqlDbName} temporary table."
 *
 * @returns {Promise}
 */
function dropStateLogsTable() {
    return connect(self).then(() => {
        return new Promise(resolve => {
            self._pg.connect((error, client, done) => {
                if (error) {
                    generateError(self, '\t--[dropStateLogsTable] Cannot connect to PostgreSQL server...\n' + error);
                    resolve();
                } else {
                    let sql = 'DROP TABLE "' + self._schema + '"."state_logs_' + self._schema + self._mySqlDbName + '";';
                    client.query(sql, err => {
                        done();

                        if (err) {
                            generateError(self, '\t--[dropStateLogsTable] ' + err, sql);
                        } else {
                            log(self, '\t--[dropStateLogsTable] table "' + self._schema + '"."state_logs_' + self._schema + self._mySqlDbName + '" is dropped...');
                        }

                        resolve();
                    });
                }
            });
        });
    });
}

/**
 * Create the "{schema}"."data_pool_{self._schema + self._mySqlDbName} temporary table."
 *
 * @returns {Promise}
 */
function createDataPoolTable() {
    return connect(self).then(() => {
        return new Promise((resolve, reject) => {
            self._pg.connect((error, client, done) => {
                if (error) {
                    generateError(self, '\t--[createDataPoolTable] Cannot connect to PostgreSQL server...\n' + error);
                    reject();
                } else {
                    let sql = 'CREATE TABLE IF NOT EXISTS "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName
                            + '"("id" BIGSERIAL, "json" TEXT);';

                    client.query(sql, err => {
                        done();

                        if (err) {
                            generateError(self, '\t--[createDataPoolTable] ' + err, sql);
                            reject();
                        } else {
                            log(self, '\t--[createDataPoolTable] table "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '" is created...');
                            resolve();
                        }
                    });
                }
            });
        });
    });
}

/**
 * Drop the "{schema}"."data_pool_{self._schema + self._mySqlDbName} temporary table."
 *
 * @returns {Promise}
 */
function dropDataPoolTable() {
    return connect(self).then(() => {
        return new Promise(resolve => {
            self._pg.connect((error, client, done) => {
                if (error) {
                    generateError(self, '\t--[dropDataPoolTable] Cannot connect to PostgreSQL server...\n' + error);
                    resolve();
                } else {
                    let sql = 'DROP TABLE "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '";';
                    client.query(sql, err => {
                        done();

                        if (err) {
                            generateError(self, '\t--[dropDataPoolTable] ' + err, sql);
                        } else {
                            log(self, '\t--[dropDataPoolTable] table "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '" is dropped...');
                        }

                        resolve();
                    });
                }
            });
        });
    });
}

/**
 * Load source tables and views, that need to be migrated.
 *
 * @returns {Promise}
 */
function loadStructureToMigrate() {
    return getStatelog('tables_loaded').then(stateLog => {
        return new Promise((resolve, reject) => {
            self._mysql.getConnection((error, connection) => {
                if (error) {
                    // The connection is undefined.
                    generateError(self, '\t--[loadStructureToMigrate] Cannot connect to MySQL server...\n' + error);
                    reject();
                } else {
                    let sql = 'SHOW FULL TABLES IN `' + self._mySqlDbName + '`;';
                    connection.query(sql, (strErr, rows) => {
                        connection.release();

                        if (strErr) {
                            generateError(self, '\t--[loadStructureToMigrate] ' + strErr, sql);
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
                                    processTablePromises.push(processTableBeforeDataLoading(relationName, stateLog));
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

                            log(self, message);

                            Promise.all(processTablePromises).then(
                                () => {
                                    updateStatelog('tables_loaded');
                                    resolve();
                                },
                                () => reject()
                            );
                        }
                    });
                }
            });
        });
    });
}

/**
 * Attempts to convert MySQL view to PostgreSQL view.
 *
 * @returns {Promise}
 */
function processView() {
    return getStatelog('views_loaded').then(stateLog => {
        return new Promise(resolve => {
            let createViewPromises = [];

            if (!stateLog) {
                for (let i = 0; i < self._viewsToMigrate.length; ++i) {
                    createViewPromises.push(
                        new Promise(resolveProcessView2 => {
                            self._mysql.getConnection((error, connection) => {
                                if (error) {
                                    // The connection is undefined.
                                    generateError(self, '\t--[processView] Cannot connect to MySQL server...\n' + error);
                                    resolveProcessView2();
                                } else {
                                    let sql = 'SHOW CREATE VIEW `' + self._viewsToMigrate[i] + '`;';
                                    connection.query(sql, (strErr, rows) => {
                                        connection.release();

                                        if (strErr) {
                                            generateError(self, '\t--[processView] ' + strErr, sql);
                                            resolveProcessView2();
                                        } else {
                                            self._pg.connect((error, client, done) => {
                                                if (error) {
                                                    generateError(self, '\t--[processView] Cannot connect to PostgreSQL server...');
                                                    resolveProcessView2();
                                                } else {
                                                    sql  = generateView(self._schema, self._viewsToMigrate[i], rows[0]['Create View']);
                                                    rows = null;
                                                    client.query(sql, err => {
                                                        done();

                                                        if (err) {
                                                            generateError(self, '\t--[processView] ' + err, sql);
                                                            logNotCreatedView(self._viewsToMigrate[i], sql);
                                                            resolveProcessView2();
                                                        } else {
                                                            log(self, '\t--[processView] View "' + self._schema + '"."' + self._viewsToMigrate[i] + '" is created...');
                                                            resolveProcessView2();
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

            Promise.all(createViewPromises).then(() => resolve());
        });
    });
}

/**
 * Starts a process of foreign keys creation.
 *
 * @returns {Promise}
 */
function processForeignKey() {
    return getStatelog('foreign_keys_loaded').then(stateLog => {
        return new Promise(resolve => {
            let fkPromises = [];

            if (!stateLog) {
                for (let i = 0; i < self._tablesToMigrate.length; ++i) {
                    let tableName = self._tablesToMigrate[i];
                    log(self, '\t--[processForeignKey] Search foreign keys for table "' + self._schema + '"."' + tableName + '"...');
                    fkPromises.push(
                        new Promise(fkResolve => {
                            self._mysql.getConnection((error, connection) => {
                                if (error) {
                                    // The connection is undefined.
                                    generateError(self, '\t--[processForeignKey] Cannot connect to MySQL server...\n' + error);
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
                                              generateError(self, self, '\t--[processForeignKey] ' + err, sql);
                                              fkResolve();
                                          } else {
                                              processForeignKeyWorker(tableName, rows).then(() => {
                                                  log(self, '\t--[processForeignKey] Foreign keys for table "' + self._schema + '"."' + tableName + '" are set...');
                                                  fkResolve();
                                              });
                                          }
                                      });
                                  }
                            });
                        })
                    );
                }
            }

            Promise.all(fkPromises).then(() => resolve());
        });
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
                    self._pg.connect((error, client, done) => {
                        if (error) {
                            objConstraints[attr] = null;
                            generateError(self, '\t--[processForeignKeyWorker] Cannot connect to PostgreSQL server...');
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
                                    generateError(self, '\t--[processForeignKeyWorker] ' + err, sql);
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
    return connect(self).then(() => {
        return new Promise(resolve => {
            let vacuumPromises = [];

            for (let i = 0; i < self._tablesToMigrate.length; ++i) {
                if (self._noVacuum.indexOf(self._tablesToMigrate[i]) === -1) {
                    let msg = '\t--[runVacuumFullAndAnalyze] Running "VACUUM FULL and ANALYZE" query for table "'
                            + self._schema + '"."' + self._tablesToMigrate[i] + '"...';

                    log(self, msg);
                    vacuumPromises.push(
                        new Promise(resolveVacuum => {
                            self._pg.connect((error, client, done) => {
                                if (error) {
                                    generateError(self, '\t--[runVacuumFullAndAnalyze] Cannot connect to PostgreSQL server...');
                                    resolveVacuum();
                                } else {
                                    let sql = 'VACUUM (FULL, ANALYZE) "' + self._schema + '"."' + self._tablesToMigrate[i] + '";';
                                    client.query(sql, err => {
                                        done();

                                        if (err) {
                                            generateError(self, '\t--[runVacuumFullAndAnalyze] ' + err, sql);
                                            resolveVacuum();
                                        } else {
                                            let msg2 = '\t--[runVacuumFullAndAnalyze] Table "' + self._schema + '"."' + self._tablesToMigrate[i] + '" is VACUUMed...';
                                            log(self, msg2);
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
    });
}

/**
 * Migrates structure of a single table to PostgreSql server.
 *
 * @param   {String} tableName
 * @returns {Promise}
 */
function createTable(tableName) {
    return connect(self).then(() => {
        return new Promise((resolveCreateTable, rejectCreateTable) => {
            log(self, '\t--[createTable] Currently creating table: `' + tableName + '`', self._dicTables[tableName].tableLogPath);
            self._mysql.getConnection((error, connection) => {
                if (error) {
                    // The connection is undefined.
                    generateError(self, '\t--[createTable] Cannot connect to MySQL server...\n' + error);
                    rejectCreateTable();
                } else {
                    let sql = 'SHOW FULL COLUMNS FROM `' + tableName + '`;';
                    connection.query(sql, (err, rows) => {
                        connection.release();

                        if (err) {
                            generateError(self, '\t--[createTable] ' + err, sql);
                            rejectCreateTable();
                        } else {
                            self._pg.connect((error, client, done) => {
                                if (error) {
                                    generateError(self, '\t--[createTable] Cannot connect to PostgreSQL server...\n' + error, sql);
                                    rejectCreateTable();
                                } else {
                                    self._dicTables[tableName].arrTableColumns = rows;
                                    sql                                        = 'CREATE TABLE IF NOT EXISTS "'
                                                                               + self._schema + '"."' + tableName + '"(';

                                    for (let i = 0; i < rows.length; ++i) {
                                        let strConvertedType  = mapDataTypes(self._dataTypesMap, rows[i].Type);
                                        sql                  += '"' + rows[i].Field + '" ' + strConvertedType + ',';
                                    }

                                    rows = null;
                                    sql  = sql.slice(0, -1) + ');';
                                    client.query(sql, err => {
                                        done();

                                        if (err) {
                                            generateError(self, '\t--[createTable] ' + err, sql);
                                            rejectCreateTable();
                                        } else {
                                            log(self,
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
    });
}

/**
 * Define which columns of the given table are of type "enum".
 * Set an appropriate constraint, if need.
 *
 * @param   {String} tableName
 * @returns {Promise}
 */
function processEnum(tableName) {
    return connect(self).then(() => {
        return new Promise(resolve => {
            log(self, '\t--[processEnum] Defines "ENUMs" for table "' + self._schema + '"."' + tableName + '"', self._dicTables[tableName].tableLogPath);
            let processEnumPromises = [];

            for (let i = 0; i < self._dicTables[tableName].arrTableColumns.length; ++i) {
                if (self._dicTables[tableName].arrTableColumns[i].Type.indexOf('(') !== -1) {
                    let arrType = self._dicTables[tableName].arrTableColumns[i].Type.split('(');

                    if (arrType[0] === 'enum') {
                        processEnumPromises.push(
                            new Promise(resolveProcessEnum => {
                                self._pg.connect((error, client, done) => {
                                    if (error) {
                                        let msg = '\t--[processEnum] Cannot connect to PostgreSQL server...\n' + error;
                                        generateError(self, msg);
                                        resolveProcessEnum();
                                    } else {
                                        let sql = 'ALTER TABLE "' + self._schema + '"."' + tableName + '" '
                                                + 'ADD CHECK ("' + self._dicTables[tableName].arrTableColumns[i].Field + '" IN (' + arrType[1] + ');';

                                        client.query(sql, err => {
                                            done();

                                            if (err) {
                                                let msg = '\t--[processEnum] Error while setting ENUM for "' + self._schema + '"."'
                                                        + tableName + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '"...\n' + err;

                                                generateError(self, msg, sql);
                                                resolveProcessEnum();
                                            } else {
                                                let success = '\t--[processEnum] Set "ENUM" for "' + self._schema + '"."' + tableName
                                                            + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '"...';

                                                log(self, success, self._dicTables[tableName].tableLogPath);
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
    return connect(self).then(() => {
        return new Promise(resolve => {
            log(self, '\t--[processNull] Defines "NULLs" for table: "' + self._schema + '"."' + tableName + '"', self._dicTables[tableName].tableLogPath);
            let processNullPromises = [];

            for (let i = 0; i < self._dicTables[tableName].arrTableColumns.length; ++i) {
                if (self._dicTables[tableName].arrTableColumns[i].Null.toLowerCase() === 'no') {
                    processNullPromises.push(
                        new Promise(resolveProcessNull => {
                            self._pg.connect((error, client, done) => {
                                if (error) {
                                    let msg = '\t--[processNull] Cannot connect to PostgreSQL server...\n' + error;
                                    generateError(self, msg);
                                    resolveProcessNull();
                                } else {
                                    let sql = 'ALTER TABLE "' + self._schema + '"."' + tableName
                                            + '" ALTER COLUMN "' + self._dicTables[tableName].arrTableColumns[i].Field + '" SET NOT NULL;';

                                    client.query(sql, err => {
                                        done();

                                        if (err) {
                                            let msg = '\t--[processNull] Error while setting NULL for "' + self._schema + '"."'
                                                    + tableName + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '"...\n' + err;

                                            generateError(self, msg, sql);
                                            resolveProcessNull();
                                        } else {
                                            let success = '\t--[processNull] Set NULL for "' + self._schema + '"."' + tableName
                                                        + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '"...';

                                            log(self, success, self._dicTables[tableName].tableLogPath);
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
    });
}

/**
 * Returns the default value for a column, with the needed transformations.
 *
 * @param   {Conversion}    self
 * @param   {RowDataPacket} column
 * @returns {String}
 */
function columnDefault(self, column) {
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

    let reservedValue = sqlReservedValues[column.Default];
    if (reservedValue) {
        return reservedValue;
    } else if (self._convertTinyintToBoolean && column.Type.indexOf('tinyint') !== -1) {
        let value = parseInt(column.Default);
        return value === 0 ? 'FALSE' : 'TRUE';
    } else if (isFloatNumeric(column.Default)) {
        return column.Default;
    } else {
        return  "'" + column.Default + "'";
    }
}

/**
 * Define which columns of the given table have default value.
 * Set default values, if need.
 *
 * @param   {String} tableName
 * @returns {Promise}
 */
function processDefault(tableName) {
    return connect(self).then(() => {
        return new Promise(resolve => {
            log(self, '\t--[processDefault] Defines default values for table: "' + self._schema + '"."' + tableName + '"', self._dicTables[tableName].tableLogPath);
            let processDefaultPromises = [];

            for (let i = 0; i < self._dicTables[tableName].arrTableColumns.length; ++i) {
                if (self._dicTables[tableName].arrTableColumns[i].Default !== null) {
                    processDefaultPromises.push(
                        new Promise(resolveProcessDefault => {
                            self._pg.connect((error, client, done) => {
                                if (error) {
                                    let msg = '\t--[processDefault] Cannot connect to PostgreSQL server...\n' + error;
                                    generateError(self, msg);
                                    resolveProcessDefault();
                                } else {
                                    let sql = 'ALTER TABLE "' + self._schema + '"."' + tableName
                                            + '" ' + 'ALTER COLUMN "' + self._dicTables[tableName].arrTableColumns[i].Field
                                            + '" SET DEFAULT ' + columnDefault(self, self._dicTables[tableName].arrTableColumns[i]) + ';';

                                    client.query(sql, err => {
                                        done();

                                        if (err) {
                                            let msg = '\t--[processDefault] Error occurred when tried to set default value for "'
                                                    + self._schema + '"."' + tableName
                                                    + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '"...\n' + err;

                                            generateError(self, msg, sql);
                                            resolveProcessDefault();
                                        } else {
                                            let success = '\t--[processDefault] Set default value for "' + self._schema + '"."' + tableName
                                                        + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '"...';

                                            log(self, success, self._dicTables[tableName].tableLogPath);
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
    return connect(self).then(() => {
        return new Promise(resolve => {
            let createSequencePromises = [];

            for (let i = 0; i < self._dicTables[tableName].arrTableColumns.length; ++i) {
                if (self._dicTables[tableName].arrTableColumns[i].Extra === 'auto_increment') {
                    createSequencePromises.push(
                        new Promise(resolveCreateSequence => {
                            let seqName = tableName + '_' + self._dicTables[tableName].arrTableColumns[i].Field + '_seq';
                            log(self, '\t--[createSequence] Trying to create sequence : "' + self._schema + '"."' + seqName + '"', self._dicTables[tableName].tableLogPath);
                            self._pg.connect((error, client, done) => {
                                if (error) {
                                    let msg = '\t--[createSequence] Cannot connect to PostgreSQL server...\n' + error;
                                    generateError(self, msg);
                                    resolveCreateSequence();
                                } else {
                                    let sql = 'CREATE SEQUENCE "' + self._schema + '"."' + seqName + '";';
                                    client.query(sql, err => {
                                        if (err) {
                                            done();
                                            let errMsg = '\t--[createSequence] Failed to create sequence "' + self._schema + '"."' + seqName + '"';
                                            generateError(self, errMsg, sql);
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

                                                     generateError(self, err2Msg, sql);
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

                                                                generateError(self, err3Msg, sql);
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

                                                                      generateError(self, err4Msg, sql);
                                                                      resolveCreateSequence();
                                                                  } else {
                                                                      let success = '\t--[createSequence] Sequence "' + self._schema + '"."' + seqName + '" is created...';
                                                                      log(self, success, self._dicTables[tableName].tableLogPath);
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
    });
}

/**
 * Create primary key and indices.
 *
 * @param   {String} tableName
 * @returns {Promise}
 */
function processIndexAndKey(tableName) {
    return connect(self).then(() => {
        return new Promise(resolveProcessIndexAndKey => {
            self._mysql.getConnection((error, connection) => {
                if (error) {
                    // The connection is undefined.
                    generateError(self, '\t--[processIndexAndKey] Cannot connect to MySQL server...\n\t' + error);
                    resolveProcessIndexAndKey();
                } else {
                    let sql = 'SHOW INDEX FROM `' + tableName + '`;';
                    connection.query(sql, (err, arrIndices) => {
                        connection.release();

                        if (err) {
                            generateError(self, '\t--[processIndexAndKey] ' + err, sql);
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
                                        self._pg.connect((pgError, pgClient, done) => {
                                            if (pgError) {
                                                let msg = '\t--[processIndexAndKey] Cannot connect to PostgreSQL server...\n' + pgError;
                                                generateError(self, msg);
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
                                                        generateError(self, '\t--[processIndexAndKey] ' + err2, sql);
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
                                log(self, success, self._dicTables[tableName].tableLogPath);
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
    return connect(self).then(() => {
        return new Promise(resolve => {
            log(self, '\t--[processComment] Creates comments for table "' + self._schema + '"."' + tableName + '"...', self._dicTables[tableName].tableLogPath);
            let arrCommentPromises = [];

            for (let i = 0; i < self._dicTables[tableName].arrTableColumns.length; ++i) {
                if (self._dicTables[tableName].arrTableColumns[i].Comment !== '') {
                    arrCommentPromises.push(
                        new Promise(resolveComment => {
                            self._pg.connect((error, client, done) => {
                                if (error) {
                                    let msg = '\t--[processComment] Cannot connect to PostgreSQL server...\n' + error;
                                    generateError(self, msg);
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

                                            generateError(self, msg, sql);
                                            resolveComment();
                                        } else {
                                            let success = '\t--[processComment] Set comment for "' + self._schema + '"."' + tableName
                                                          + '" column: "' + self._dicTables[tableName].arrTableColumns[i].Field + '"...';

                                            log(self, success, self._dicTables[tableName].tableLogPath);
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
    });
}

/**
 * Processes current table before data loading.
 *
 * @param   {String}  tableName
 * @param   {Boolean} stateLog
 * @returns {Promise}
 */
function processTableBeforeDataLoading(tableName, stateLog) {
    return connect(self).then(() => {
        return createTable(tableName);
    }).then(() => {
        return prepareArrayOfTablesAndChunkOffsets(tableName, stateLog);
    }).catch(() => {
        generateError(self, '\t--[processTableBeforeDataLoading] Cannot create table "' + self._schema + '"."' + tableName + '"...');
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
                    log(self, '\t--[closeConnections] ' + error);
                }

                log(self, '\t--[closeConnections] All DB connections to both MySQL and PostgreSQL servers have been closed...');
                self._pg = null;
                resolve();
            });
        } else {
            log(self, '\t--[closeConnections] All DB connections to both MySQL and PostgreSQL servers have been closed...');
            self._pg = null;
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
    log(self, '\t--[cleanup] Cleanup resources...');
    return removeTemporaryDirectory().then(
        closeConnections
    ).then(() => {
        return new Promise(resolve => {
            log(self, '\t--[cleanup] Cleanup finished...');
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

    log(self, output);
    process.exit();
}

/**
 * Prepares an array of tables and chunk offsets.
 *
 * @param   {String}  tableName
 * @param   {Boolean} stateLog
 * @returns {Promise}
 */
function prepareArrayOfTablesAndChunkOffsets(tableName, stateLog) {
    return connect(self).then(() => {
        return new Promise(resolve => {
            if (stateLog) {
                return resolve();
            }

            self._mysql.getConnection((error, connection) => {
                if (error) {
                    // The connection is undefined.
                    generateError(self, '\t--[prepareArrayOfTablesAndChunkOffsets] Cannot connect to MySQL server...\n\t' + error);
                    resolve();
                } else {
                    // Determine current table size, apply "chunking".
                    let sql = "SELECT ((data_length + index_length) / 1024 / 1024) AS size_in_mb "
                            + "FROM information_schema.TABLES "
                            + "WHERE table_schema = '" + self._mySqlDbName + "' "
                            + "AND table_name = '" + tableName + "';";

                    connection.query(sql, (err, rows) => {
                        if (err) {
                            connection.release();
                            generateError(self, '\t--[prepareArrayOfTablesAndChunkOffsets] ' + err, sql);
                            resolve();
                        } else {
                            let tableSizeInMb      = +rows[0].size_in_mb;
                            tableSizeInMb          = tableSizeInMb < 1 ? 1 : tableSizeInMb;
                            rows                   = null;
                            let strSelectFieldList = arrangeColumnsData(self._dicTables[tableName].arrTableColumns);
                            sql                    = 'SELECT COUNT(1) AS rows_count FROM `' + tableName + '`;';

                            connection.query(sql, (err2, rows2) => {
                                connection.release();

                                if (err2) {
                                    generateError(self, '\t--[prepareArrayOfTablesAndChunkOffsets] ' + err2, sql);
                                    resolve();
                                } else {
                                    let rowsCnt             = rows2[0].rows_count;
                                    rows2                   = null;
                                    let chunksCnt           = tableSizeInMb / self._dataChunkSize;
                                    chunksCnt               = chunksCnt < 1 ? 1 : chunksCnt;
                                    let rowsInChunk         = Math.ceil(rowsCnt / chunksCnt);
                                    let arrDataPoolPromises = [];
                                    let msg                 = '\t--[prepareArrayOfTablesAndChunkOffsets] Total rows to insert into '
                                                            + '"' + self._schema + '"."' + tableName + '": ' + rowsCnt;

                                    log(self, msg, self._dicTables[tableName].tableLogPath);

                                    for (let offset = 0; offset < rowsCnt; offset += rowsInChunk) {
                                        arrDataPoolPromises.push(new Promise(resolveDataUnit => {
                                            self._pg.connect((error, client, done) => {
                                                if (error) {
                                                    generateError(self, '\t--[prepareArrayOfTablesAndChunkOffsets] Cannot connect to PostgreSQL server...\n' + error);
                                                    resolveDataUnit();
                                                } else {
                                                    let strJson = '{"_tableName":"' + tableName
                                                                + '","_selectFieldList":"' + strSelectFieldList + '",'
                                                                + '"_offset":' + offset + ','
                                                                + '"_rowsInChunk":' + rowsInChunk + ','
                                                                + '"_rowsCnt":' + rowsCnt + '}';

                                                    let sql = 'INSERT INTO "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '"("json") VALUES($1);';
                                                    client.query(sql, [strJson], err => {
                                                        done();

                                                        if (err) {
                                                            generateError(self, '\t--[prepareArrayOfTablesAndChunkOffsets] INSERT failed...\n' + err, sql);
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
    });
}

/**
 * Reads temporary table, and generates Data-pool.
 *
 * @returns {Promise}
 */
function readDataPool() {
    return connect(self).then(() => {
        return new Promise((resolve, reject) => {
            self._pg.connect((error, client, done) => {
                if (error) {
                    generateError(self, '\t--[readDataPool] Cannot connect to PostgreSQL server...\n' + error);
                    reject();
                } else {
                    let sql = 'SELECT id AS id, json AS json FROM "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '";';
                    client.query(sql, (err, arrDataPool) => {
                        done();

                        if (err) {
                            generateError(self, '\t--[readDataPool] ' + err, sql);
                            return reject();
                        }

                        for (let i = 0; i < arrDataPool.rows.length; ++i) {
                            let obj = JSON.parse(arrDataPool.rows[i].json);
                            obj._id = arrDataPool.rows[i].id;
                            self._dataPool.push(obj);
                        }

                        log(self, '\t--[readDataPool] Data-Pool is loaded...');
                        resolve();
                    });
                }
            });
        });
    });
}

/**
 * Instructs DataLoader which DataUnits should be load.
 * No need to check the state-log.
 * If dataPool's length is zero, then nmig will proceed to the next step.
 *
 * @returns {undefined}
 */
function dataPipe() {
    if (self._dataPool.length === 0) {
        return continueProcessAfterDataLoading();
    }

    let strDataLoaderPath = __dirname + '/DataLoader.js';
    let options           = self._loaderMaxOldSpaceSize === 'DEFAULT' ? {} : { execArgv: ['--max-old-space-size=' + self._loaderMaxOldSpaceSize] };
    let loaderProcess     = childProcess.fork(strDataLoaderPath, options);

    loaderProcess.on('message', signal => {
        if (typeof signal === 'object') {
            self._dicTables[signal.tableName].totalRowsInserted += signal.rowsInserted;
            let msg = '\t--[dataPipe]  For now inserted: ' + self._dicTables[signal.tableName].totalRowsInserted + ' rows, '
                    + 'Total rows to insert into "' + self._schema + '"."' + signal.tableName + '": ' + signal.totalRowsToInsert;

            log(self, msg);
        } else {
            killProcess(loaderProcess.pid);
            intProcessedDataUnits += self._pipeWidth;
            return intProcessedDataUnits < self._dataPool.length ? dataPipe() : continueProcessAfterDataLoading();
        }
    });

    let intEnd  = self._dataPool.length - (self._dataPool.length - self._pipeWidth - intProcessedDataUnits);
    let message = new MessageToDataLoader(self._config, self._dataPool.slice(intProcessedDataUnits, intEnd));
    loaderProcess.send(message);
}

/**
 * Kill a process specified by the pid.
 *
 * @param   {Number} pid
 * @returns {undefined}
 */
function killProcess(pid) {
    try {
        process.kill(pid);
    } catch (killError) {
        generateError(self, '\t--[killProcess] ' + killError);
    }
}

/**
 * Continues the process after data loading.
 *
 * @returns {undefined}
 */
function continueProcessAfterDataLoading() {
    getStatelog('per_table_constraints_loaded').then(stateLog => {
        let promises = [];

        if (!stateLog) {
            for (let i = 0; i < self._tablesToMigrate.length; ++i) {
                let tableName = self._tablesToMigrate[i];
                promises.push(
                    processEnum(tableName).then(() => {
                        return processNull(tableName);
                    }).then(() => {
                        return processDefault(tableName);
                    }).then(() => {
                        return createSequence(tableName);
                    }).then(() => {
                        return processIndexAndKey(tableName);
                    }).then(() => {
                        return processComment(tableName);
                    })
                );
            }
        }

        Promise.all(promises).then(() => {
            updateStatelog('per_table_constraints_loaded').then(
                processForeignKey
            ).then(() => {
                return updateStatelog('foreign_keys_loaded');
            }).then(
                dropDataPoolTable
            ).then(
                processView
            ).then(() => {
                return updateStatelog('views_loaded');
            }).then(
                runVacuumFullAndAnalyze
            ).then(
                dropStateLogsTable
            ).then(
                cleanup
            ).then(
                () => generateReport('NMIG migration is accomplished.')
            );
        });
    });
}

/**
 * Runs migration according to user's configuration.
 *
 * @param   {Object} config
 * @returns {undefined}
 */
module.exports = function(config) {
    console.log('\n\tNMIG - the database migration tool\n\tCopyright 2016 Anatoly Khaytovich <anatolyuss@gmail.com>\n\t Boot...');
    self = new Conversion(config);

    readDataTypesMap(self).then(
        createLogsDirectory,
        () => {
            // Braces are essential. Without them promises-chain will continue execution.
            console.log('\t--[FromMySQL2PostgreSQL] Failed to boot migration');
        }
    ).then(
        createTemporaryDirectory,
        () => {
            // Braces are essential. Without them promises-chain will continue execution.
            log(self, '\t--[FromMySQL2PostgreSQL] Logs directory was not created...');
        }
    ).then(
        createSchema,
        () => {
            let msg = '\t--[FromMySQL2PostgreSQL] The temporary directory [' + self._tempDirPath + '] already exists...'
                    + '\n\t  Please, remove this directory and rerun NMIG...';

            log(self, msg);
        }
    ).then(
        createStateLogsTable,
        () => {
            generateError(self, '\t--[FromMySQL2PostgreSQL] Cannot create new DB schema...');
            cleanup();
        }
    ).then(
        createDataPoolTable,
        () => {
            generateError(self, '\t--[FromMySQL2PostgreSQL] Cannot create execution_logs table...');
            cleanup();
        }
    ).then(
        loadStructureToMigrate,
        () => {
            generateError(self, '\t--[FromMySQL2PostgreSQL] Cannot create data-pool...');
            cleanup();
        }
    ).then(
        readDataPool,
        () => {
            generateError(self, '\t--[FromMySQL2PostgreSQL] NMIG cannot load source database structure...');
            cleanup();
        }
    ).then(
        dataPipe,
        () => {
            generateError(self, '\t--[FromMySQL2PostgreSQL] NMIG failed to load Data-Units pool...');
            cleanup();
        }
    );
};
