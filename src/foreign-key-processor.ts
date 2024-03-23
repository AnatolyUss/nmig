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
import * as migrationStateManager from './migration-state-manager';
import { log } from './fs-ops';
import Conversion from './conversion';
import DbAccess from './db-access';
import * as extraConfigProcessor from './extra-config-processor';
import { DBAccessQueryParams, DBAccessQueryResult, DBVendors } from './types';

/**
 * Creates foreign keys for given table.
 */
const processForeignKeyWorker = async (
    conversion: Conversion,
    tableName: string,
    rows: Record<string, any>[],
): Promise<void> => {
    const objConstraints: Record<string, any> = Object.create(null);
    const originalTableName: string = extraConfigProcessor.getTableName(
        conversion,
        tableName,
        true,
    );

    rows.forEach((row: Record<string, any>): void => {
        const currentColumnName: string = extraConfigProcessor.getColumnName(
            conversion,
            originalTableName,
            row.COLUMN_NAME,
            false,
        );

        const currentReferencedTableName: string = extraConfigProcessor.getTableName(
            conversion,
            row.REFERENCED_TABLE_NAME,
            false,
        );

        const originalReferencedTableName: string = extraConfigProcessor.getTableName(
            conversion,
            row.REFERENCED_TABLE_NAME,
            true,
        );

        const currentReferencedColumnName: string = extraConfigProcessor.getColumnName(
            conversion,
            originalReferencedTableName,
            row.REFERENCED_COLUMN_NAME,
            false,
        );

        if (row.CONSTRAINT_NAME in objConstraints) {
            objConstraints[row.CONSTRAINT_NAME].column_name.add(`"${currentColumnName}"`);
            objConstraints[row.CONSTRAINT_NAME].referenced_column_name.add(
                `"${currentReferencedColumnName}"`,
            );
            return;
        }

        objConstraints[row.CONSTRAINT_NAME] = Object.create(null);
        objConstraints[row.CONSTRAINT_NAME].column_name = new Set<string>([
            `"${currentColumnName}"`,
        ]);
        objConstraints[row.CONSTRAINT_NAME].referenced_column_name = new Set<string>([
            `"${currentReferencedColumnName}"`,
        ]);

        objConstraints[row.CONSTRAINT_NAME].referenced_table_name = currentReferencedTableName;
        objConstraints[row.CONSTRAINT_NAME].update_rule = row.UPDATE_RULE;
        objConstraints[row.CONSTRAINT_NAME].delete_rule = row.DELETE_RULE;
    });

    const params: DBAccessQueryParams = {
        conversion: conversion,
        caller: processForeignKeyWorker.name,
        sql: '',
        vendor: DBVendors.PG,
        processExitOnError: false,
        shouldReturnClient: false,
    };

    const _cb = async (attr: string): Promise<void> => {
        params.sql = `ALTER TABLE "${conversion._schema}"."${tableName}" 
            ADD FOREIGN KEY (${[...objConstraints[attr].column_name].join(',')}) 
            REFERENCES "${conversion._schema}"."${objConstraints[attr].referenced_table_name}" 
            (${[...objConstraints[attr].referenced_column_name].join(',')}) 
            ON UPDATE ${objConstraints[attr].update_rule} 
            ON DELETE ${objConstraints[attr].delete_rule};`;

        await DbAccess.query(params);
    };

    const constraintsPromises: Promise<void>[] = Object.keys(objConstraints).map(_cb);
    await Promise.all(constraintsPromises);
};

/**
 * Starts a process of foreign keys creation.
 */
export default async (conversion: Conversion): Promise<void> => {
    const logTitle = 'ForeignKeyProcessor::default';
    const isForeignKeysProcessed: boolean = await migrationStateManager.get(
        conversion,
        'foreign_keys_loaded',
    );

    if (isForeignKeysProcessed) {
        return;
    }

    const params: DBAccessQueryParams = {
        conversion: conversion,
        caller: logTitle,
        sql: '',
        vendor: DBVendors.MYSQL,
        processExitOnError: false,
        shouldReturnClient: false,
    };

    const _cb = async (tableName: string): Promise<void> => {
        await log(
            conversion,
            `\t--[${logTitle}] Search foreign keys for table "${conversion._schema}"."${tableName}"...`,
        );

        const colsTableName: string = extraConfigProcessor.getTableName(
            conversion,
            tableName,
            true,
        );

        params.sql = `
            SELECT 
                cols.COLUMN_NAME, refs.REFERENCED_TABLE_NAME, refs.REFERENCED_COLUMN_NAME,
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
            WHERE cols.TABLE_SCHEMA = '${conversion._mySqlDbName}' AND cols.TABLE_NAME = '${colsTableName}';`;

        const result: DBAccessQueryResult = await DbAccess.query(params);

        if (result.error) {
            return;
        }

        const extraRows: Record<string, any>[] = extraConfigProcessor.parseForeignKeys(
            conversion,
            tableName,
        );
        const fullRows: Record<string, any>[] = (result.data || []).concat(extraRows); // Prevent failure if "result.data" is undefined.
        await processForeignKeyWorker(conversion, tableName, fullRows);
        await log(
            conversion,
            `\t--[${logTitle}] Foreign keys for table "${conversion._schema}"."${tableName}" are set...`,
        );
    };

    const fkPromises: Promise<void>[] = conversion._tablesToMigrate.map(_cb);
    await Promise.all(fkPromises);
};
