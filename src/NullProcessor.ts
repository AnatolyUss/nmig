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
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import DBVendors from './DBVendors';
import DBAccessQueryResult from './DBAccessQueryResult';
import IDBAccessQueryParams from './IDBAccessQueryParams';
import * as extraConfigProcessor from './ExtraConfigProcessor';

/**
 * Defines which columns of the given table can contain the "NULL" value.
 * Sets an appropriate constraint, if need.
 */
export default async (conversion: Conversion, tableName: string): Promise<void> => {
    const logTitle: string = 'NullProcessor::default';
    const msg: string = `\t--[${ logTitle }] Defines "NOT NULLs" for table: "${ conversion._schema }"."${ tableName }"`;
    log(conversion, msg, conversion._dicTables[tableName].tableLogPath);
    const originalTableName: string = extraConfigProcessor.getTableName(conversion, tableName, true);

    const promises: Promise<void>[] = conversion._dicTables[tableName].arrTableColumns.map(async (column: any) => {
        if (column.Null.toLowerCase() === 'no') {
            const columnName: string = extraConfigProcessor.getColumnName(conversion, originalTableName, column.Field, false);
            const params: IDBAccessQueryParams = {
                conversion: conversion,
                caller: logTitle,
                sql: `ALTER TABLE "${ conversion._schema }"."${ tableName }" ALTER COLUMN "${ columnName }" SET NOT NULL;`,
                vendor: DBVendors.PG,
                processExitOnError: false,
                shouldReturnClient: false
            };

            const result: DBAccessQueryResult = await DBAccess.query(params);

            if (!result.error) {
                const successMsg: string = `\t--[${ logTitle }] Set NOT NULL for "${ conversion._schema }"."${ tableName }"."${ columnName }"...`;
                log(conversion, successMsg, conversion._dicTables[tableName].tableLogPath);
            }
        }
    });

    await Promise.all(promises);
};
