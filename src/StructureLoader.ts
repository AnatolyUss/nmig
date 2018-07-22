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
import DBAccess from './DBAccess';
import DBAccessQueryResult from './DBAccessQueryResult';
import DBVendors from './DBVendors';
import log from './Logger';
import Conversion from './Conversion';
import Table from './Table';
import { createTable } from './TableProcessor';
import prepareDataChunks from './DataChunksProcessor';
import * as migrationStateManager from './MigrationStateManager';
import * as extraConfigProcessor from './ExtraConfigProcessor';

/**
 * Processes current table before data loading.
 */
async function processTableBeforeDataLoading(conversion: Conversion, tableName: string, stateLog: boolean): Promise<void> {
    await createTable(conversion, tableName);
    await prepareDataChunks(conversion, tableName, stateLog);
}

/**
 * Retrieves the source db (MySQL) version.
 */
async function getMySqlVersion(conversion: Conversion): Promise<void> {
    const dbAccess: DBAccess = new DBAccess(conversion);
    const sql: string = 'SELECT VERSION() AS mysql_version;';
    const result: DBAccessQueryResult = await dbAccess.query('StructureLoader::getMySqlVersion', sql, DBVendors.MYSQL, false, false);

    if (result.error) {
        return;
    }

    const arrVersion: string[] = result.data[0].mysql_version.split('.');
    const majorVersion: string = arrVersion[0];
    const minorVersion: string = arrVersion.slice(1).join('');
    conversion._mysqlVersion = +(`${ majorVersion }.${ minorVersion }`);
}

/**
 * Loads source tables and views, that need to be migrated.
 */
export default async (conversion: Conversion): Promise<Conversion> => {
    await getMySqlVersion(conversion);
    const dbAccess: DBAccess = new DBAccess(conversion);
    const haveTablesLoaded: boolean = await migrationStateManager.get(conversion, 'tables_loaded');
    const sql: string = `SHOW FULL TABLES IN \`${ conversion._mySqlDbName }\`;`;
    const result: DBAccessQueryResult = await dbAccess.query('StructureLoader::default', sql, DBVendors.MYSQL, true, false);
    let tablesCnt: number = 0;
    let viewsCnt: number = 0;
    const processTablePromises: Promise<void>[] = [];

    result.data.forEach((row: any) => {
        let relationName: string = row[`Tables_in_${ conversion._mySqlDbName }`];

        if (row.Table_type === 'BASE TABLE' && conversion._excludeTables.indexOf(relationName) === -1) {
            relationName = extraConfigProcessor.getTableName(conversion, relationName, false);
            conversion._tablesToMigrate.push(relationName);
            conversion._dicTables[relationName] = new Table(`${ conversion._logsDirPath }/${ relationName }.log`);
            processTablePromises.push(processTableBeforeDataLoading(conversion, relationName, haveTablesLoaded));
            tablesCnt++;
        } else if (row.Table_type === 'VIEW') {
            conversion._viewsToMigrate.push(relationName);
            viewsCnt++;
        }
    });

    const message: string = `\t--[loadStructureToMigrate] Source DB structure is loaded...\n
        \t--[loadStructureToMigrate] Tables to migrate: ${ tablesCnt }\n
        \t--[loadStructureToMigrate] Views to migrate: ${ viewsCnt }`;

    log(conversion, message);
    await Promise.all(processTablePromises);
    await migrationStateManager.set(conversion, 'tables_loaded');
    return conversion;
}
