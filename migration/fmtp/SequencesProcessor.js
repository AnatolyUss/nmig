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

const connect       = require('./Connector');
const log           = require('./Logger');
const generateError = require('./ErrorGenerator');

/**
 * Define which column in given table has the "auto_increment" attribute.
 * Create an appropriate sequence.
 *
 * @param {Conversion} self
 * @param {String}     tableName
 *
 * @returns {Promise}
 */
module.exports = function(self, tableName) {
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
};
