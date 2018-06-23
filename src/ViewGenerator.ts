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
import * as fs from 'fs';
import * as path from 'path';
import log from './Logger';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import DBVendors from './DBVendors';
import * as extraConfigProcessor from './ExtraConfigProcessor';

/**
 * Attempts to convert MySQL view to PostgreSQL view.
 */
function generateView(schema: string, viewName: string, mysqlViewCode: string): string {
    mysqlViewCode = mysqlViewCode.split('`').join('"');
    const queryStart: number = mysqlViewCode.indexOf('AS');
    mysqlViewCode = mysqlViewCode.slice(queryStart);
    const arrMysqlViewCode: string[] = mysqlViewCode.split(' ');

    arrMysqlViewCode.forEach((str: string, index: number) => {
        if (str.toLowerCase() === 'from' || str.toLowerCase() === 'join' && index + 1 < arrMysqlViewCode.length) {
            arrMysqlViewCode[index + 1] = `"${ schema }".${ arrMysqlViewCode[index + 1] }`;
        }
    });

    return `CREATE OR REPLACE VIEW "${ schema }"."${ viewName }" ${ arrMysqlViewCode.join(' ') };`;
}

/**
 * Writes a log, containing a view code.
 *
 * @param {Conversion} self
 * @param {String}     viewName
 * @param {String}     sql
 *
 * @returns {undefined} TODO: refactor to return Promise<void>!!!
 */
const logNotCreatedView = (conversion: Conversion, viewName: string, sql: string) => {
    fs.stat(self._notCreatedViewsPath, (directoryDoesNotExist, stat) => {
        if (directoryDoesNotExist) {
            fs.mkdir(self._notCreatedViewsPath, self._0777, e => {
                if (e) {
                    log(self, '\t--[logNotCreatedView] ' + e);
                } else {
                    log(self, '\t--[logNotCreatedView] "not_created_views" directory is created...');
                    // "not_created_views" directory is created. Can write the log...
                    fs.open(path.join(self._notCreatedViewsPath, viewName + '.sql'), 'w', self._0777, (error, fd) => {
                        if (error) {
                            log(self, error);
                        } else {
                            const buffer = Buffer.from(sql, self._encoding);
                            fs.write(fd, buffer, 0, buffer.length, null, () => {
                                fs.close(fd, () => {
                                    // Each async function MUST have a callback (according to Node.js >= 7).
                                });
                            });
                        }
                    });
                }
            });
        } else if (!stat.isDirectory()) {
            log(self, '\t--[logNotCreatedView] Cannot write the log due to unexpected error');
        } else {
            // "not_created_views" directory already exists. Can write the log...
            fs.open(path.join(self._notCreatedViewsPath, viewName + '.sql'), 'w', self._0777, (error, fd) => {
                if (error) {
                    log(self, error);
                } else {
                    const buffer = Buffer.from(sql, self._encoding);
                    fs.write(fd, buffer, 0, buffer.length, null, () => {
                        fs.close(fd, () => {
                            // Each async function MUST have a callback (according to Node.js >= 7).
                        });
                    });
                }
            });
        }
    });
}

/**
 * Attempts to convert MySQL view to PostgreSQL view.
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
module.exports = self => {
    return migrationStateManager.get(self, 'views_loaded').then(hasViewsLoaded => {
        return new Promise(resolve => {
            const createViewPromises = [];

            if (!hasViewsLoaded) {
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
                                                            logNotCreatedView(self, self._viewsToMigrate[i], sql);
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
