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
import { log } from './FsOps';
import Conversion from './Conversion';
import { createTable } from './TableProcessor';
import prepareDataChunks from './DataChunksProcessor';
import * as migrationStateManager from './MigrationStateManager';
import * as extraConfigProcessor from './ExtraConfigProcessor';
import { DBAccessQueryParams, DBAccessQueryResult, DBVendors } from './Types';

/**
 * Processes current table before data loading.
 */
const processTableBeforeDataLoading = async (
    conversion: Conversion,
    tableName: string,
    stateLog: boolean,
): Promise<void> => {
    await createTable(conversion, tableName);
    await prepareDataChunks(conversion, tableName, stateLog);
};

/**
 * Retrieves the source db (MySQL) version.
 */
const setMySqlVersion = async (conversion: Conversion): Promise<void> => {
    const params: DBAccessQueryParams = {
        conversion: conversion,
        caller: 'StructureLoader::setMySqlVersion',
        sql: 'SELECT VERSION() AS mysql_version;',
        vendor: DBVendors.MYSQL,
        processExitOnError: false,
        shouldReturnClient: false,
    };

    const result: DBAccessQueryResult = await DBAccess.query(params);

    if (result.error) {
        return;
    }

    const arrVersion: string[] = result.data[0].mysql_version.split('.');
    const majorVersion: string = arrVersion[0];
    const minorVersion: string = arrVersion.slice(1).join('');
    conversion._mysqlVersion = `${majorVersion}.${minorVersion}`;
};

/**
 * Loads source tables and views, that need to be migrated.
 */
export default async (conversion: Conversion): Promise<Conversion> => {
    const logTitle = 'StructureLoader::default';
    await setMySqlVersion(conversion);
    const haveTablesLoaded: boolean = await migrationStateManager.get(conversion, 'tables_loaded');
    let sql = `SHOW FULL TABLES IN \`${conversion._mySqlDbName}\` WHERE 1 = 1`;

    if (conversion._includeTables.length !== 0) {
        const tablesToInclude: string = conversion._includeTables
            .map((table: string): string => `'${table}'`)
            .join(',');

        sql += ` AND Tables_in_${conversion._mySqlDbName} IN (${tablesToInclude})`;
    }

    if (conversion._excludeTables.length !== 0) {
        const tablesToExclude: string = conversion._excludeTables
            .map((table: string): string => `'${table}'`)
            .join(',');

        sql += ` AND Tables_in_${conversion._mySqlDbName} NOT IN (${tablesToExclude})`;
    }

    const params: DBAccessQueryParams = {
        conversion: conversion,
        caller: logTitle,
        sql: `${sql};`,
        vendor: DBVendors.MYSQL,
        processExitOnError: true,
        shouldReturnClient: false,
    };

    const result: DBAccessQueryResult = await DBAccess.query(params);
    let tablesCnt = 0;
    let viewsCnt = 0;
    const processTablePromises: Promise<void>[] = [];

    result.data.forEach((row: any) => {
        let relationName: string = row[`Tables_in_${conversion._mySqlDbName}`];

        if (
            row.Table_type === 'BASE TABLE' &&
            conversion._excludeTables.indexOf(relationName) === -1
        ) {
            relationName = extraConfigProcessor.getTableName(conversion, relationName, false);
            conversion._tablesToMigrate.push(relationName);

            conversion._dicTables.set(relationName, {
                tableLogPath: `${conversion._logsDirPath}/${relationName}.log`,
                arrTableColumns: [],
            });

            processTablePromises.push(
                processTableBeforeDataLoading(conversion, relationName, haveTablesLoaded),
            );
            tablesCnt++;
        } else if (row.Table_type === 'VIEW') {
            conversion._viewsToMigrate.push(relationName);
            viewsCnt++;
        }
    });

    const message = `\t--[${logTitle}] Source DB structure is loaded...\n
        \t--[${logTitle}] Tables to migrate: ${tablesCnt}\n
        \t--[${logTitle}] Views to migrate: ${viewsCnt}`;

    await log(conversion, message);
    await Promise.all(processTablePromises);
    await migrationStateManager.set(conversion, 'tables_loaded');
    // make the table dictionary serializable for the DefaultReader
    conversion._config._dicTables = [...conversion._dicTables];
    return conversion;
};
