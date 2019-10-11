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
import { log } from './FsOps';
import arrangeColumnsData from './ColumnsDataArranger';
import * as extraConfigProcessor from './ExtraConfigProcessor';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import DBAccessQueryResult from './DBAccessQueryResult';
import DBVendors from './DBVendors';

/**
 * Prepares an array of tables metadata.
 */
export default async (conversion: Conversion, tableName: string, haveDataChunksProcessed: boolean): Promise<void> => {
    if (haveDataChunksProcessed) {
        return;
    }

    const originalTableName: string = extraConfigProcessor.getTableName(conversion, tableName, true);
    const logTitle: string = 'DataChunksProcessor::default';
    const dbAccess: DBAccess = new DBAccess(conversion);
    const strSelectFieldList: string = arrangeColumnsData(conversion._dicTables[tableName].arrTableColumns, conversion._mysqlVersion);
    const sqlRowsCnt: string = `SELECT COUNT(1) AS rows_count FROM \`${ originalTableName }\`;`;
    const countResult: DBAccessQueryResult = await dbAccess.query(
        logTitle,
        sqlRowsCnt,
        DBVendors.MYSQL,
        false,
        false
    );

    const rowsCnt: number = countResult.data[0].rows_count;
    const msg: string = `\t--[prepareDataChunks] Total rows to insert into "${ conversion._schema }"."${ tableName }": ${ rowsCnt }`;
    log(conversion, msg, conversion._dicTables[tableName].tableLogPath);
    const metadata: string = `{"_tableName":"${ tableName }","_selectFieldList":"${ strSelectFieldList }","_rowsCnt":${ rowsCnt }}`;
    const sql: string = `INSERT INTO "${ conversion._schema }"."data_pool_${ conversion._schema }${ conversion._mySqlDbName }"("metadata") VALUES ($1);`;

    await dbAccess.query(
        logTitle,
        sql,
        DBVendors.PG,
        false,
        false,
        undefined,
        [metadata]
    );
}
