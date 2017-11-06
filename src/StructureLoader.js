/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright (C) 2016 - present, Anatoly Khaytovich <anatolyuss@gmail.com>
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

const Table                 = require('./Classes/Table');
const { createTable }       = require('./TableProcessor');
const connect               = require('./Connector');
const log                   = require('./Logger');
const generateError         = require('./ErrorGenerator');
const prepareDataChunks     = require('./DataChunksProcessor');
const migrationStateManager = require('./MigrationStateManager');
const extraConfigProcessor  = require('./ExtraConfigProcessor');

/**
 * Processes current table before data loading.
 *
 * @param {Conversion} self
 * @param {String}     tableName
 * @param {Boolean}    stateLog
 *
 * @returns {Promise}
 */
const processTableBeforeDataLoading = (self, tableName, stateLog) => {
    return connect(self).then(() => {
        return createTable(self, tableName);
    }).then(() => {
        return prepareDataChunks(self, tableName, stateLog);
    }).catch(() => {
        generateError(self, '\t--[processTableBeforeDataLoading] Cannot create table "' + self._schema + '"."' + tableName + '"...');
    });
}

/**
 * Get the MySQL version.
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
const getMySqlVersion = self => {
    return connect(self).then(() => {
        return new Promise(resolve => {
            self._mysql.getConnection((error, connection) => {
                if (error) {
                    // The connection is undefined.
                    generateError(self, '\t--[getMySqlVersion] Cannot connect to MySQL server...\n' + error);
                    resolve();
                } else {
                    const sql = 'SELECT VERSION() AS mysql_version;';
                    connection.query(sql, (err, rows) => {
                        connection.release();

                        if (err) {
                            generateError(self, '\t--[getMySqlVersion] ' + err, sql);
                            resolve();
                        } else {
                            const arrVersion   = rows[0].mysql_version.split('.');
                            const majorVersion = arrVersion[0];
                            const minorVersion = arrVersion.slice(1).join('');
                            self._mysqlVersion = +(majorVersion + '.' + minorVersion);
                            resolve();
                        }
                    });
                }
            });
        });
    });
}

/**
 * Load source tables and views, that need to be migrated.
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
module.exports = self => {
    return getMySqlVersion(self).then(() => {
        return migrationStateManager.get(self, 'tables_loaded').then(haveTablesLoaded => {
            return new Promise(resolve => {
                self._mysql.getConnection((error, connection) => {
                    if (error) {
                        // The connection is undefined.
                        generateError(self, '\t--[loadStructureToMigrate] Cannot connect to MySQL server...\n' + error);
                        process.exit();
                    } else {
                        let sql = 'SHOW FULL TABLES IN `' + self._mySqlDbName + '`';

                        if (self._includeTables.length) {
                            sql = sql + ' WHERE Tables_in_' + self._mySqlDbName + ' IN (' + self._includeTables.map(function (item) {return '"' + item + '"'}).join(',') + ')';
                        }
                        sql = sql + ';';

                        connection.query(sql, (strErr, rows) => {
                            connection.release();

                            if (strErr) {
                                generateError(self, '\t--[loadStructureToMigrate] ' + strErr, sql);
                                process.exit();
                            } else {
                                let tablesCnt              = 0;
                                let viewsCnt               = 0;
                                const processTablePromises = [];

                                for (let i = 0; i < rows.length; ++i) {
                                    let relationName = rows[i]['Tables_in_' + self._mySqlDbName];

                                    if (rows[i].Table_type === 'BASE TABLE' && self._excludeTables.indexOf(relationName) === -1) {
                                        relationName = extraConfigProcessor.getTableName(self, relationName, false);
                                        self._tablesToMigrate.push(relationName);
                                        self._dicTables[relationName] = new Table(self._logsDirPath + '/' + relationName + '.log');
                                        processTablePromises.push(processTableBeforeDataLoading(self, relationName, haveTablesLoaded));
                                        tablesCnt++;
                                    } else if (rows[i].Table_type === 'VIEW') {
                                        self._viewsToMigrate.push(relationName);
                                        viewsCnt++;
                                    }
                                }

                                rows        = null;
                                let message = '\t--[loadStructureToMigrate] Source DB structure is loaded...\n'
                                    + '\t--[loadStructureToMigrate] Tables to migrate: ' + tablesCnt + '\n'
                                    + '\t--[loadStructureToMigrate] Views to migrate: ' + viewsCnt;

                                log(self, message);

                                Promise.all(processTablePromises).then(
                                    () => {
                                        migrationStateManager.set(self, 'tables_loaded').then(() => resolve(self));
                                    },
                                    () => process.exit()
                                );
                            }
                        });
                    }
                });
            });
        });
    });
};
