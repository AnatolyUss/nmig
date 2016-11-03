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
 * Migrates structure of a single table to PostgreSql server.
 *
 * @param   {Conversion} self
 * @param   {String}     tableName
 * @returns {Promise}
 */
module.exports = function(self, tableName) {
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
                            self._dicTables[tableName].arrTableColumns = rows;

                            if (self._migrateOnlyData) {
                                return resolveCreateTable();
                            }

                            self._pg.connect((error, client, done) => {
                                if (error) {
                                    generateError(self, '\t--[createTable] Cannot connect to PostgreSQL server...\n' + error, sql);
                                    rejectCreateTable();
                                } else {
                                    sql = 'CREATE TABLE IF NOT EXISTS "' + self._schema + '"."' + tableName + '"(';
                                    
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
};
