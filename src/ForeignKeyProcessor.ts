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
import * as migrationStateManager from './MigrationStateManager';
import { log } from './FsOps';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import DBVendors from './DBVendors';
import DBAccessQueryResult from './DBAccessQueryResult';
import IDBAccessQueryParams from './IDBAccessQueryParams';
import * as extraConfigProcessor from './ExtraConfigProcessor';

/**
 * Creates foreign keys for given table.
 */
const processForeignKeyWorker = async (conversion: Conversion, tableName: string, rows: any[]): Promise<void> => {
    const objConstraints: any = Object.create(null);
    const originalTableName: string = extraConfigProcessor.getTableName(conversion, tableName, true);
    const logTitle: string = 'ForeignKeyProcessor::processForeignKeyWorker';

    rows.forEach((row: any) => {
        const currentColumnName: string = extraConfigProcessor.getColumnName(conversion, originalTableName, row.COLUMN_NAME, false);
        const currentReferencedTableName: string  = extraConfigProcessor.getTableName(conversion, row.REFERENCED_TABLE_NAME, false);
        const originalReferencedTableName: string = extraConfigProcessor.getTableName(conversion, row.REFERENCED_TABLE_NAME, true);
        const currentReferencedColumnName: string = extraConfigProcessor.getColumnName(
            conversion,
            originalReferencedTableName,
            row.REFERENCED_COLUMN_NAME,
            false
        );

        if (row.CONSTRAINT_NAME in objConstraints) {
            objConstraints[row.CONSTRAINT_NAME].column_name.push(`"${ currentColumnName }"`);
            objConstraints[row.CONSTRAINT_NAME].referenced_column_name.push(`"${ currentReferencedColumnName }"`);
            return;
        }

        objConstraints[row.CONSTRAINT_NAME] = Object.create(null);
        objConstraints[row.CONSTRAINT_NAME].column_name = [`"${ currentColumnName }"`];
        objConstraints[row.CONSTRAINT_NAME].referenced_column_name = [`"${ currentReferencedColumnName }"`];
        objConstraints[row.CONSTRAINT_NAME].referenced_table_name = currentReferencedTableName;
        objConstraints[row.CONSTRAINT_NAME].update_rule = row.UPDATE_RULE;
        objConstraints[row.CONSTRAINT_NAME].delete_rule = row.DELETE_RULE;
    });

    const params: IDBAccessQueryParams = {
        conversion: conversion,
        caller: logTitle,
        sql: '',
        vendor: DBVendors.PG,
        processExitOnError: false,
        shouldReturnClient: false
    };

    const constraintsPromises: Promise<void>[] = Object.keys(objConstraints).map(async (attr: string) => {
        params.sql = `ALTER TABLE "${ conversion._schema }"."${ tableName }" 
            ADD FOREIGN KEY (${ objConstraints[attr].column_name.join(',') }) 
            REFERENCES "${ conversion._schema }"."${ objConstraints[attr].referenced_table_name }" 
            (${ objConstraints[attr].referenced_column_name.join(',') }) 
            ON UPDATE ${ objConstraints[attr].update_rule } 
            ON DELETE ${ objConstraints[attr].delete_rule };`;

        await DBAccess.query(params);
    });

    await Promise.all(constraintsPromises);
};

/**
 * Starts a process of foreign keys creation.
 */
export default async (conversion: Conversion): Promise<void> => {
    const logTitle: string = 'ForeignKeyProcessor::default';
    const isForeignKeysProcessed: boolean = await migrationStateManager.get(conversion, 'foreign_keys_loaded');

    if (isForeignKeysProcessed) {
        return;
    }

    const params: IDBAccessQueryParams = {
        conversion: conversion,
        caller: logTitle,
        sql: '',
        vendor: DBVendors.MYSQL,
        processExitOnError: false,
        shouldReturnClient: false
    };

    const fkPromises: Promise<void>[] = conversion._tablesToMigrate.map(async (tableName: string) => {
        log(conversion, `\t--[${ logTitle }] Search foreign keys for table "${ conversion._schema }"."${ tableName }"...`);
        params.sql = `SELECT cols.COLUMN_NAME, refs.REFERENCED_TABLE_NAME, refs.REFERENCED_COLUMN_NAME,
            cRefs.UPDATE_RULE, cRefs.DELETE_RULE, cRefs.CONSTRAINT_NAME 
            FROM INFORMATION_SCHEMA.\`COLUMNS\` AS cols 
            INNER JOIN INFORMATION_SCHEMA.\`KEY_COLUMN_USAGE\` AS refs 
            ON refs.TABLE_SCHEMA = cols.TABLE_SCHEMA 
            AND refs.REFERENCED_TABLE_SCHEMA = cols.TABLE_SCHEMA 
            AND refs.TABLE_NAME = cols.TABLE_NAME 
            AND refs.COLUMN_NAME = cols.COLUMN_NAME 
            LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS cRefs 
            ON cRefs.CONSTRAINT_SCHEMA = cols.TABLE_SCHEMA 
            AND cRefs.CONSTRAINT_NAME = refs.CONSTRAINT_NAME 
            LEFT JOIN INFORMATION_SCHEMA.\`KEY_COLUMN_USAGE\` AS links 
            ON links.TABLE_SCHEMA = cols.TABLE_SCHEMA 
            AND links.REFERENCED_TABLE_SCHEMA = cols.TABLE_SCHEMA 
            AND links.REFERENCED_TABLE_NAME = cols.TABLE_NAME 
            AND links.REFERENCED_COLUMN_NAME = cols.COLUMN_NAME 
            LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS cLinks 
            ON cLinks.CONSTRAINT_SCHEMA = cols.TABLE_SCHEMA 
            AND cLinks.CONSTRAINT_NAME = links.CONSTRAINT_NAME 
            WHERE cols.TABLE_SCHEMA = '${ conversion._mySqlDbName }' 
            AND cols.TABLE_NAME = '${ extraConfigProcessor.getTableName(conversion, tableName, true) }';`;

        const result: DBAccessQueryResult = await DBAccess.query(params);

        if (result.error) {
            return;
        }

        const extraRows: any[] = extraConfigProcessor.parseForeignKeys(conversion, tableName);
        const fullRows: any[] = (result.data || []).concat(extraRows); // Prevent failure if "result.data" is undefined.
        await processForeignKeyWorker(conversion, tableName, fullRows);
        log(conversion, `\t--[${ logTitle }] Foreign keys for table "${ conversion._schema }"."${ tableName }" are set...`);
    });

    await Promise.all(fkPromises);
};
