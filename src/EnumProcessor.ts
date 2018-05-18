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
 * Defines which columns of the given table are of type "enum".
 * Sets an appropriate constraint, if need.
 */
export default async function(conversion: Conversion, tableName: string): Promise<void> {
    const dbAccess: DBAccess = new DBAccess(conversion);
    const msg: string = `\t--[EnumProcessor] Defines "ENUMs" for table "${ conversion._schema }"."${ tableName }"`;
    log(conversion, msg, conversion._dicTables[tableName].tableLogPath);
    const originalTableName: string = extraConfigProcessor.getTableName(conversion, tableName, true);

    const processEnumPromises: Promise<void>[] = conversion._dicTables[tableName].arrTableColumns.map(async (column: any) => {
        if (column.Type.indexOf('(') !== -1) {
            const arrType: string[] = column.Type.split('(');

            if (arrType[0] === 'enum') {
                const columnName: string = extraConfigProcessor.getColumnName(
                    conversion,
                    originalTableName,
                    column.Field,
                    false
                );

                const sql: string = `ALTER TABLE "${ conversion._schema }"."${ tableName }" ADD CHECK ("${ columnName }" IN (${ arrType[1] });`;
                await dbAccess.query('EnumProcessor', sql, DBVendors.PG, false, false);
                const successMsg: string = `\t--[EnumProcessor] Set "ENUM" for "${ conversion._schema }"."${ tableName }"."${ columnName }"...`;
                log(conversion, successMsg, conversion._dicTables[tableName].tableLogPath);
            }
        }
    });

    await Promise.all(processEnumPromises);
}
