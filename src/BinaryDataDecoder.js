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

const generateError = require('./ErrorGenerator');
const log           = require('./Logger');
const connect       = require('./Connector');

/**
 * Decodes binary data from from textual representation in string.
 *
 * @param {Conversion} conversion
 *
 * @returns {Promise<Conversion>}
 */
module.exports = conversion => {
    log(conversion, '\t--[decodeBinaryData] Decodes binary data from textual representation in string.');

    return connect(conversion).then(() => {
        return new Promise(resolve => {
            conversion._pg.connect((error, client, release) => {
                if (error) {
                    generateError(conversion, '\t--[decodeBinaryData] Cannot connect to PostgreSQL server...');
                    return resolve(conversion);
                }

                const sql = `SELECT table_name, column_name 
                    FROM information_schema.columns
                    WHERE table_catalog = '${ conversion._targetConString.database }' 
                      AND table_schema = '${ conversion._schema }' 
                      AND data_type IN ('bytea', 'geometry');`;

                client.query(sql, (err, data) => {
                    release();

                    if (err) {
                        generateError(conversion, `\t--[decodeBinaryData] ${ err }`, sql);
                        return resolve(conversion);
                    }

                    const decodePromises = [];

                    for (let i = 0; i < data.rows.length; ++i) {
                        decodePromises.push(new Promise(resolveDecode => {
                            conversion._pg.connect((connectionError, pgClient, clientRelease) => {
                                if (connectionError) {
                                    generateError(conversion, '\t--[decodeBinaryData] Cannot connect to PostgreSQL server...');
                                    return resolveDecode();
                                }

                                const tableName  = data.rows[i].table_name;
                                const columnName = data.rows[i].column_name;
                                const sqlDecode  = `UPDATE ${ conversion._schema }.${ tableName }
                                                    SET ${ columnName } = DECODE(ENCODE(${ columnName }, 'escape'), 'hex');`;

                                pgClient.query(sqlDecode, decodeError => {
                                    clientRelease();

                                    if (decodeError) {
                                        generateError(conversion, `\t--[decodeBinaryData] ${ decodeError }`, sqlDecode);
                                    }

                                    resolveDecode();
                                });
                            });
                        }));
                    }

                    Promise.all(decodePromises).then(() => resolve(conversion));
                });
            });
        });
    });
};
