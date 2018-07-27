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
import * as path from 'path';
import * as csvStringify from './CsvStringifyModified';
import log from './Logger';
import generateError from './ErrorGenerator';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import DBAccessQueryResult from './DBAccessQueryResult';
import DBVendors from './DBVendors';
import MessageToMaster from './MessageToMaster';
import { enforceConsistency } from './ConsistencyEnforcer';
import * as extraConfigProcessor from './ExtraConfigProcessor';
import BufferStream from './BufferStream';
const { from } = require('pg-copy-streams'); // No declaration file for module "pg-copy-streams".

process.on('message', async (signal: any) => {
    const conv: Conversion = new Conversion(signal.config);
    log(conv, '\t--[loadData] Loading the data...');

    const promises: Promise<void>[] = signal.chunks.map(async (chunk: any) => {
        const isNormalFlow: boolean = await enforceConsistency(conv, chunk);

        return isNormalFlow
            ? populateTableWorker(conv, chunk._tableName, chunk._selectFieldList, chunk._offset, chunk._rowsInChunk, chunk._rowsCnt, chunk._id)
            : deleteChunk(conv, chunk._id);
    });

    await Promise.all(promises);
    process.send('processed');
});

/**
 * Deletes given record from the data-pool.
 */
async function deleteChunk(conv: Conversion, dataPoolId: number): Promise<void> {
    const sql: string = `DELETE FROM "${ conv._schema }"."data_pool_${ conv._schema }${ conv._mySqlDbName }" WHERE id = ${ dataPoolId };`;
    const dbAccess: DBAccess = new DBAccess(conv);
    await dbAccess.query('DataLoader::deleteChunk', sql, DBVendors.PG, false, false);
}

/**
 * Builds a MySQL query to retrieve the chunk of data.
 */
function buildChunkQuery(tableName: string, selectFieldList: string, offset: number, rowsInChunk: number): string {
    return `SELECT ${ selectFieldList } FROM \`${ tableName }\` LIMIT ${ offset },${ rowsInChunk };`;
}

/**
 * Processes data-loading error.
 */
function processDataError(
    conv: Conversion,
    streamError: string,
    sql: string,
    sqlCopy: string,
    tableName: string,
    dataPoolId: number
): Promise<void> {
    generateError(conv, `\t--[populateTableWorker] ${ streamError }`, sqlCopy);
    const rejectedData: string = `\t--[populateTableWorker] Error loading table data:\n${ sql }\n`;
    log(conv, rejectedData, path.join(conv._logsDirPath, `${ tableName }.log`));
    return deleteChunk(conv, dataPoolId);
}

/**
 * Loads a chunk of data using "PostgreSQL COPY".
 */
async function populateTableWorker(
    conv: Conversion,
    tableName: string,
    strSelectFieldList: string,
    offset: number,
    rowsInChunk: number,
    rowsCnt: number,
    dataPoolId: number
): Promise<void> {
    //


    return new Promise(resolvePopulateTableWorker => {
        self._mysql.getConnection((error, connection) => {
            if (error) {
                // The connection is undefined.
                generateError(self, '\t--[populateTableWorker] Cannot connect to MySQL server...\n\t' + error);
                resolvePopulateTableWorker();
            } else {
                const originalTableName = extraConfigProcessor.getTableName(self, tableName, true);
                const sql               = buildChunkQuery(originalTableName, strSelectFieldList, offset, rowsInChunk);

                connection.query(sql, (err, rows) => {
                    connection.release();

                    if (err) {
                        generateError(self, '\t--[populateTableWorker] ' + err, sql);
                        resolvePopulateTableWorker();
                    } else {
                        rowsInChunk                                                             = rows.length;
                        rows[0][self._schema + '_' + originalTableName + '_data_chunk_id_temp'] = dataPoolId;

                        csvStringify(rows, (csvError, csvString) => {
                            rows = null;

                            if (csvError) {
                                generateError(self, '\t--[populateTableWorker] ' + csvError);
                                resolvePopulateTableWorker();
                            } else {
                                const buffer = Buffer.from(csvString, self._encoding);
                                csvString  = null;

                                self._pg.connect((error, client, done) => {
                                    if (error) {
                                        generateError(self, '\t--[populateTableWorker] Cannot connect to PostgreSQL server...\n' + error, sql);
                                        resolvePopulateTableWorker();
                                    } else {
                                        const sqlCopy      = 'COPY "' + self._schema + '"."' + tableName + '" FROM STDIN DELIMITER \'' + self._delimiter + '\' CSV;';
                                        const copyStream   = client.query(from(sqlCopy));
                                        const bufferStream = new BufferStream(buffer);

                                        copyStream.on('end', () => {
                                            /*
                                             * COPY FROM STDIN does not return the number of rows inserted.
                                             * But the transactional behavior still applies (no records inserted if at least one failed).
                                             * That is why in case of 'on end' the rowsInChunk value is actually the number of records inserted.
                                             */
                                            process.send(new MessageToMaster(tableName, rowsInChunk, rowsCnt));
                                            deleteChunk(self, dataPoolId, client, done).then(() => resolvePopulateTableWorker());
                                        });

                                        copyStream.on('error', copyStreamError => {
                                            processDataError(
                                                self,
                                                copyStreamError,
                                                sql,
                                                sqlCopy,
                                                tableName,
                                                dataPoolId,
                                                client,
                                                done,
                                                resolvePopulateTableWorker
                                            );
                                        });

                                        bufferStream.on('error', bufferStreamError => {
                                            processDataError(
                                                self,
                                                bufferStreamError,
                                                sql,
                                                sqlCopy,
                                                tableName,
                                                dataPoolId,
                                                client,
                                                done,
                                                resolvePopulateTableWorker
                                            );
                                        });

                                        bufferStream
                                            .setEncoding(self._encoding)
                                            .pipe(copyStream);
                                    }
                                });
                            }
                        }, self._encoding);
                    }
                });
            }
        });
    });
}
