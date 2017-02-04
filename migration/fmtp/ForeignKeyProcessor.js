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

const log                   = require('./Logger');
const generateError         = require('./ErrorGenerator');
const migrationStateManager = require('./MigrationStateManager');
const extraConfigProcessor  = require('./ExtraConfigProcessor');

/**
 * Creates foreign keys for given table.
 *
 * @param {Conversion} self
 * @param {String}     tableName
 * @param {Array}      rows
 *
 * @returns {Promise}
 */
const processForeignKeyWorker = (self, tableName, rows) => {
    return new Promise(resolve => {
        const constraintsPromises = [];
        const objConstraints      = Object.create(null);
        const originalTableName   = extraConfigProcessor.getTableName(self, tableName, true);

        for (let i = 0; i < rows.length; ++i) {
            const currentColumnName           = extraConfigProcessor.getColumnName(self, originalTableName, rows[i].COLUMN_NAME, false);
            const currentReferencedTableName  = extraConfigProcessor.getTableName(self, rows[i].REFERENCED_TABLE_NAME, false);
            const originalReferencedTableName = extraConfigProcessor.getTableName(self, rows[i].REFERENCED_TABLE_NAME, true);
            const currentReferencedColumnName = extraConfigProcessor.getColumnName(
                self,
                originalReferencedTableName,
                rows[i].REFERENCED_COLUMN_NAME,
                false
            );

            if (rows[i].CONSTRAINT_NAME in objConstraints) {
                objConstraints[rows[i].CONSTRAINT_NAME].column_name.push('"' + currentColumnName + '"');
                objConstraints[rows[i].CONSTRAINT_NAME].referenced_column_name.push('"' + currentReferencedColumnName + '"');
            } else {
                objConstraints[rows[i].CONSTRAINT_NAME]                        = Object.create(null);
                objConstraints[rows[i].CONSTRAINT_NAME].column_name            = ['"' + currentColumnName + '"'];
                objConstraints[rows[i].CONSTRAINT_NAME].referenced_column_name = ['"' + currentReferencedColumnName + '"'];
                objConstraints[rows[i].CONSTRAINT_NAME].referenced_table_name  = currentReferencedTableName;
                objConstraints[rows[i].CONSTRAINT_NAME].update_rule            = rows[i].UPDATE_RULE;
                objConstraints[rows[i].CONSTRAINT_NAME].delete_rule            = rows[i].DELETE_RULE;
            }
        }

        rows = null;

        for (const attr in objConstraints) {
            constraintsPromises.push(
                new Promise(resolveConstraintPromise => {
                    self._pg.connect((error, client, done) => {
                        if (error) {
                            objConstraints[attr] = null;
                            generateError(self, '\t--[processForeignKeyWorker] Cannot connect to PostgreSQL server...');
                            resolveConstraintPromise();
                        } else {
                            const sql = 'ALTER TABLE "' + self._schema + '"."' + tableName + '" ADD FOREIGN KEY ('
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
 * Starts a process of foreign keys creation.
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
module.exports = self => {
    return migrationStateManager.get(self, 'foreign_keys_loaded').then(isForeignKeysProcessed => {
        return new Promise(resolve => {
            const fkPromises = [];

            if (!isForeignKeysProcessed) {
                for (let i = 0; i < self._tablesToMigrate.length; ++i) {
                    const tableName = self._tablesToMigrate[i];
                    log(self, '\t--[processForeignKey] Search foreign keys for table "' + self._schema + '"."' + tableName + '"...');
                    fkPromises.push(
                        new Promise(fkResolve => {
                            self._mysql.getConnection((error, connection) => {
                                if (error) {
                                    // The connection is undefined.
                                    generateError(self, '\t--[processForeignKey] Cannot connect to MySQL server...\n' + error);
                                    fkResolve();
                                } else {
                                    const sql = "SELECT cols.COLUMN_NAME, refs.REFERENCED_TABLE_NAME, refs.REFERENCED_COLUMN_NAME, "
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
                                        + "AND cols.TABLE_NAME = '" + extraConfigProcessor.getTableName(self, tableName, true) + "';";

                                      connection.query(sql, (err, rows) => {
                                          connection.release();

                                          if (err) {
                                              generateError(self, '\t--[processForeignKey] ' + err, sql);
                                              fkResolve();
                                          } else {
                                              processForeignKeyWorker(self, tableName, rows).then(() => {
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
};
