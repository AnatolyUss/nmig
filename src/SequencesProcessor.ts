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
import log from './Logger';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import DBVendors from './DBVendors';
import * as extraConfigProcessor from './ExtraConfigProcessor';

/**
 * Set sequence value.
 *
 * @param {Conversion} self
 * @param {String}     tableName
 *
 * @returns {Promise}
 */
export async function setSequenceValue(conversion: Conversion, tableName: string): Promise<void> {
    let hasAutoIncrementColumnFound: boolean = false;
    const originalTableName: string = extraConfigProcessor.getTableName(conversion, tableName, true);

    conversion._dicTables[tableName].arrTableColumns.forEach(async (column: any) => {
        //
    });



    return connect(self).then(() => {
        return new Promise(resolve => {
            let hasAutoIncrementColumnFound = false;
            const originalTableName         = extraConfigProcessor.getTableName(self, tableName, true);

            for (let i = 0; i < self._dicTables[tableName].arrTableColumns.length; ++i) {
                if (self._dicTables[tableName].arrTableColumns[i].Extra === 'auto_increment') {
                    hasAutoIncrementColumnFound = true;
                    self._pg.connect((error, client, done) => {
                        if (error) {
                            const msg = '\t--[setSequenceValue] Cannot connect to PostgreSQL server...\n' + error;
                            generateError(self, msg);
                            resolve();
                        } else {
                            const columnName = extraConfigProcessor.getColumnName(
                                self,
                                originalTableName,
                                self._dicTables[tableName].arrTableColumns[i].Field,
                                false
                            );

                            const seqName = tableName + '_' + columnName + '_seq';
                            const sql     = 'SELECT SETVAL(\'"' + self._schema + '"."' + seqName + '"\', '
                                + '(SELECT MAX("' + columnName + '") FROM "'
                                + self._schema + '"."' + tableName + '"));';

                            client.query(sql, err => {
                               done();

                               if (err) {
                                   const errMsg = '\t--[setSequenceValue] Failed to set max-value of "' + self._schema + '"."'
                                       + tableName + '"."' + columnName + '" '
                                       + 'as the "NEXTVAL of "' + self._schema + '"."' + seqName + '"...';

                                   generateError(self, errMsg, sql);
                                   resolve();
                               } else {
                                   const success = '\t--[setSequenceValue] Sequence "' + self._schema + '"."' + seqName + '" is created...';
                                   log(self, success, self._dicTables[tableName].tableLogPath);
                                   resolve();
                               }
                           });
                        }
                    });

                    break; // The AUTO_INCREMENTed column was just processed.
                }
            }

            if (!hasAutoIncrementColumnFound) {
                resolve();
            }
        });
    });
}

/**
 * Define which column in given table has the "auto_increment" attribute.
 * Create an appropriate sequence.
 *
 * @param {Conversion} self
 * @param {String}     tableName
 *
 * @returns {Promise}
 */
module.exports.createSequence = (self, tableName) => {
    return connect(self).then(() => {
        return new Promise(resolve => {
            const createSequencePromises = [];
            const originalTableName      = extraConfigProcessor.getTableName(self, tableName, true);

            for (let i = 0; i < self._dicTables[tableName].arrTableColumns.length; ++i) {
                if (self._dicTables[tableName].arrTableColumns[i].Extra === 'auto_increment') {
                    createSequencePromises.push(
                        new Promise(resolveCreateSequence => {
                            const columnName = extraConfigProcessor.getColumnName(
                                self,
                                originalTableName,
                                self._dicTables[tableName].arrTableColumns[i].Field,
                                false
                            );

                            const seqName = tableName + '_' + columnName + '_seq';
                            log(self, '\t--[createSequence] Trying to create sequence : "' + self._schema + '"."' + seqName + '"', self._dicTables[tableName].tableLogPath);
                            self._pg.connect((error, client, done) => {
                                if (error) {
                                    const msg = '\t--[createSequence] Cannot connect to PostgreSQL server...\n' + error;
                                    generateError(self, msg);
                                    resolveCreateSequence();
                                } else {
                                    let sql = 'CREATE SEQUENCE "' + self._schema + '"."' + seqName + '";';
                                    client.query(sql, err => {
                                        if (err) {
                                            done();
                                            const errMsg = '\t--[createSequence] Failed to create sequence "' + self._schema + '"."' + seqName + '"';
                                            generateError(self, errMsg, sql);
                                            resolveCreateSequence();
                                        } else {
                                             sql = 'ALTER TABLE "' + self._schema + '"."' + tableName + '" '
                                                 + 'ALTER COLUMN "' + columnName + '" '
                                                 + 'SET DEFAULT NEXTVAL(\'"' + self._schema + '"."' + seqName + '"\');';

                                             client.query(sql, err2 => {
                                                 if (err2) {
                                                     done();
                                                     const err2Msg = '\t--[createSequence] Failed to set default value for "' + self._schema + '"."'
                                                         + tableName + '"."' + columnName + '"...'
                                                         + '\n\t--[createSequence] Note: sequence "' + self._schema + '"."' + seqName + '" was created...';

                                                     generateError(self, err2Msg, sql);
                                                     resolveCreateSequence();
                                                 } else {
                                                       sql = 'ALTER SEQUENCE "' + self._schema + '"."' + seqName + '" '
                                                           + 'OWNED BY "' + self._schema + '"."' + tableName + '"."' + columnName + '";';

                                                       client.query(sql, err3 => {
                                                            if (err3) {
                                                                done();
                                                                const err3Msg = '\t--[createSequence] Failed to relate sequence "' + self._schema + '"."' + seqName + '" to '
                                                                    + '"' + self._schema + '"."' + tableName + '"."' + columnName + '"...';

                                                                generateError(self, err3Msg, sql);
                                                                resolveCreateSequence();
                                                            } else {
                                                               sql = 'SELECT SETVAL(\'"' + self._schema + '"."' + seqName + '"\', '
                                                                   + '(SELECT MAX("' + columnName + '") FROM "'
                                                                   + self._schema + '"."' + tableName + '"));';

                                                               client.query(sql, err4 => {
                                                                  done();

                                                                  if (err4) {
                                                                      const err4Msg = '\t--[createSequence] Failed to set max-value of "' + self._schema + '"."'
                                                                          + tableName + '"."' + columnName + '" '
                                                                          + 'as the "NEXTVAL of "' + self._schema + '"."' + seqName + '"...';

                                                                      generateError(self, err4Msg, sql);
                                                                      resolveCreateSequence();
                                                                  } else {
                                                                      const success = '\t--[createSequence] Sequence "' + self._schema + '"."' + seqName + '" is created...';
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

                    break; // The AUTO_INCREMENTed column was just processed.
                }
            }

            Promise.all(createSequencePromises).then(() => resolve());
        });
    });
}
