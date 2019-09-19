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
import * as csvStringify from './CsvStringifyModified';
import { log, generateError } from './FsOps';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import DBAccessQueryResult from './DBAccessQueryResult';
import DBVendors from './DBVendors';
import MessageToMaster from './MessageToMaster';
import { enforceConsistency } from './ConsistencyEnforcer';
import * as extraConfigProcessor from './ExtraConfigProcessor';
import BufferStream from './BufferStream';
import * as path from 'path';
import { PoolClient, QueryResult } from 'pg';
const { from } = require('pg-copy-streams'); // No declaration file for module "pg-copy-streams".

process.on('message', async (signal: any) => {
    const conv: Conversion = new Conversion(signal.config);
    log(conv, '\t--[loadData] Loading the data...');

    const promises: Promise<void>[] = signal.chunks.map(async (chunk: any) => {
        const isNormalFlow: boolean = await enforceConsistency(conv, chunk);

        if (isNormalFlow) {
            return populateTableWorker(conv, chunk._tableName, chunk._selectFieldList, chunk._offset, chunk._rowsInChunk, chunk._rowsCnt, chunk._id);
        }

        const dbAccess: DBAccess = new DBAccess(conv);
        const client: PoolClient = await dbAccess.getPgClient();
        return deleteChunk(conv, chunk._id, client);
    });

    await Promise.all(promises);
    processSend('processed');
});

/**
 * Wraps "process.send" method to avoid "cannot invoke an object which is possibly undefined" warning.
 */
function processSend(x: any): void {
    if (process.send) {
        process.send(x);
    }
}

/**
 * Deletes given record from the data-pool.
 */
async function deleteChunk(
    conv: Conversion,
    dataPoolId: number,
    client: PoolClient,
    originalSessionReplicationRole: string | null = null
): Promise<void> {
    const sql: string = `DELETE FROM "${ conv._schema }"."data_pool_${ conv._schema }${ conv._mySqlDbName }" WHERE id = ${ dataPoolId };`;
    const dbAccess: DBAccess = new DBAccess(conv);

    try {
        await client.query(sql);

        if (originalSessionReplicationRole) {
            await enableTriggers(conv, client, <string>originalSessionReplicationRole);
        }
    } catch (error) {
        await generateError(conv, `\t--[DataLoader::deleteChunk] ${ error }`, sql);
    } finally {
        dbAccess.releaseDbClient(client);
    }
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
async function processDataError(
    conv: Conversion,
    streamError: string,
    sql: string,
    sqlCopy: string,
    tableName: string,
    dataPoolId: number,
    client: PoolClient,
    originalSessionReplicationRole: string | null
): Promise<void> {
    await generateError(conv, `\t--[populateTableWorker] ${ streamError }`, sqlCopy);
    const rejectedData: string = `\t--[populateTableWorker] Error loading table data:\n${ sql }\n`;
    log(conv, rejectedData, path.join(conv._logsDirPath, `${ tableName }.log`));
    return deleteChunk(conv, dataPoolId, client, originalSessionReplicationRole);
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
    return new Promise<void>(async resolvePopulateTableWorker => {
        const originalTableName: string = extraConfigProcessor.getTableName(conv, tableName, true);
        const sql: string = buildChunkQuery(originalTableName, strSelectFieldList, offset, rowsInChunk);
        const dbAccess: DBAccess = new DBAccess(conv);
        const logTitle: string = 'DataLoader::populateTableWorker';
        const result: DBAccessQueryResult = await dbAccess.query(logTitle, sql, DBVendors.MYSQL, false, false);

        if (result.error) {
            return resolvePopulateTableWorker();
        }

        rowsInChunk = result.data.length;
        result.data[0][`${ conv._schema }_${ originalTableName }_data_chunk_id_temp`] = dataPoolId;

        csvStringify(result.data, async (csvError: any, csvString: string) => {
            if (csvError) {
                await generateError(conv, `\t--[${ logTitle }] ${ csvError }`);
                return resolvePopulateTableWorker();
            }

            const buffer: Buffer = Buffer.from(csvString, conv._encoding);
            const sqlCopy: string = `COPY "${ conv._schema }"."${ tableName }" FROM STDIN DELIMITER '${ conv._delimiter }' CSV;`;
            const client: PoolClient = await dbAccess.getPgClient();
            let originalSessionReplicationRole: string | null = null;

            if (conv.shouldMigrateOnlyDataFor(tableName)) {
                originalSessionReplicationRole = await disableTriggers(conv, client);
            }

            const copyStream: any = client.query(from(sqlCopy));
            const bufferStream: BufferStream = new BufferStream(buffer);

            copyStream.on('end', () => {
                // COPY FROM STDIN does not return the number of rows inserted.
                // But the transactional behavior still applies (no records inserted if at least one failed).
                // That is why in case of 'on end' the rowsInChunk value is actually the number of records inserted.
                processSend(new MessageToMaster(tableName, rowsInChunk, rowsCnt));
                return deleteChunk(conv, dataPoolId, client, originalSessionReplicationRole).then(() => resolvePopulateTableWorker());
            });

            copyStream.on('error', (copyStreamError: string) => {
                return processDataError(conv, copyStreamError, sql, sqlCopy, tableName, dataPoolId, client, originalSessionReplicationRole)
                    .then(() => resolvePopulateTableWorker());
            });

            bufferStream.on('error', (bufferStreamError: string) => {
                return processDataError(conv, bufferStreamError, sql, sqlCopy, tableName, dataPoolId, client, originalSessionReplicationRole)
                    .then(() => resolvePopulateTableWorker());
            });

            bufferStream.setEncoding(conv._encoding).pipe(copyStream);
        }, conv._encoding);
    });
}

/**
 * Disables all triggers and rules for current database session.
 * !!!DO NOT release the client, it will be released after current data-chunk deletion.
 */
async function disableTriggers(conversion: Conversion, client: PoolClient): Promise<string> {
    let sql: string = `SHOW session_replication_role;`;
    let originalSessionReplicationRole: string = 'origin';

    try {
        const queryResult: QueryResult = await client.query(sql);
        originalSessionReplicationRole = queryResult.rows[0].session_replication_role;
        sql = 'SET session_replication_role = replica;';
        await client.query(sql);
    } catch (error) {
        await generateError(conversion, `\t--[DataLoader::disableTriggers] ${ error }`, sql);
    }

    return originalSessionReplicationRole;
}

/**
 * Enables all triggers and rules for current database session.
 * !!!DO NOT release the client, it will be released after current data-chunk deletion.
 */
async function enableTriggers(conversion: Conversion, client: PoolClient, originalSessionReplicationRole: string): Promise<void> {
    const sql: string = `SET session_replication_role = ${ originalSessionReplicationRole };`;

    try {
        await client.query(sql);
    } catch (error) {
        await generateError(conversion, `\t--[DataLoader::enableTriggers] ${ error }`, sql);
    }
}
