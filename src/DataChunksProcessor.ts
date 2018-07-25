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
import arrangeColumnsData from './ColumnsDataArranger';
import * as extraConfigProcessor from './ExtraConfigProcessor';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import DBAccessQueryResult from './DBAccessQueryResult';
import DBVendors from './DBVendors';

/**
 * Prepares an array of tables and chunk offsets.
 */
export default async (conversion: Conversion, tableName: string, haveDataChunksProcessed: boolean): Promise<void> => {
    if (haveDataChunksProcessed) {
        return;
    }

    // Determine current table size, apply "chunking".
    const originalTableName: string = extraConfigProcessor.getTableName(conversion, tableName, true);
    let sql: string = `SELECT (data_length / 1024 / 1024) AS size_in_mb FROM information_schema.tables 
        WHERE table_schema = '${ conversion._mySqlDbName }' AND table_name = '${ originalTableName }';`;

    const dbAccess: DBAccess = new DBAccess(conversion);
    const sizeQueryResult: DBAccessQueryResult = await dbAccess.query(
        'DataChunksProcessor::default',
        sql,
        DBVendors.MYSQL,
        true,
        true
    );

    const tableSizeInMb: number = +sizeQueryResult.data[0].size_in_mb;
    const strSelectFieldList: string = arrangeColumnsData(conversion._dicTables[tableName].arrTableColumns, conversion._mysqlVersion);
    sql = `SELECT COUNT(1) AS rows_count FROM \`${ originalTableName }\`;`;
    const countResult: DBAccessQueryResult = await dbAccess.query(
        'DataChunksProcessor::default',
        sql,
        DBVendors.MYSQL,
        true,
        false,
        sizeQueryResult.client
    );

    const rowsCnt: number = countResult.data[0].rows_count;
    let chunksCnt: number = tableSizeInMb / conversion._dataChunkSize;
    chunksCnt = chunksCnt < 1 ? 1 : chunksCnt;
    const rowsInChunk: number = Math.ceil(rowsCnt / chunksCnt);
    const arrDataPoolPromises: Promise<void>[] = [];
    const msg: string = `\t--[prepareDataChunks] Total rows to insert into "${ conversion._schema }"."${ tableName }": ${ rowsCnt }`;
    log(conversion, msg, conversion._dicTables[tableName].tableLogPath);

    for (let offset: number = 0; offset < rowsCnt; offset += rowsInChunk) {
        arrDataPoolPromises.push(new Promise<void>(async resolveDataUnit => {
            const strJson: string = `{"_tableName":"${ tableName }","_selectFieldList":"${ strSelectFieldList }",
                "_offset":${ offset },"_rowsInChunk":${ rowsInChunk },"_rowsCnt":${ rowsCnt }`;

            // Define current data chunk size in MB.
            // If there is only one chunk, then its size is equal to the table size.
            // If there are more than one chunk,
            // then a size of each chunk besides the last one is equal to "data_chunk_size",
            // and a size of the last chunk is either "data_chunk_size" or tableSizeInMb % chunksCnt.
            let currentChunkSizeInMb: number = 0;

            if (chunksCnt === 1) {
                currentChunkSizeInMb = tableSizeInMb;
            } else if (offset + rowsInChunk >= rowsCnt) {
                currentChunkSizeInMb = tableSizeInMb % chunksCnt;
                currentChunkSizeInMb = currentChunkSizeInMb || conversion._dataChunkSize;
            } else {
                currentChunkSizeInMb = conversion._dataChunkSize;
            }

            sql = `INSERT INTO "${ conversion._schema }"."data_pool_${ conversion._schema }${ conversion._mySqlDbName }"
                ("is_started", "json", "size_in_mb") VALUES (FALSE, '${ strJson }', ${ currentChunkSizeInMb });`;

            // TODO: convert to prepared statement.
            await dbAccess.query('DataChunksProcessor::default', sql, DBVendors.PG,false,false);
            resolveDataUnit();
        }));
    }

    await Promise.all(arrDataPoolPromises);
}
