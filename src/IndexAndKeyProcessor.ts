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
import { DBAccessQueryParams, DBAccessQueryResult, DBVendors, Table, Index } from './Types';
import * as extraConfigProcessor from './ExtraConfigProcessor';
import { getUniqueIdentifier } from './Utils';

/**
 * Returns PostgreSQL index type, that correlates to given MySQL index type.
 */
const getIndexType = (conversion: Conversion, indexType: string): string => {
    return indexType in conversion._indexTypesMap ? conversion._indexTypesMap[indexType] : 'BTREE';
};

/**
 * Creates primary key and indices.
 */
export default async (conversion: Conversion, tableName: string): Promise<void> => {
    const logTitle: string = 'IndexAndKeyProcessor::default';
    const originalTableName: string = extraConfigProcessor.getTableName(conversion, tableName, true);
    const params: DBAccessQueryParams = {
        conversion: conversion,
        caller: logTitle,
        sql: `SHOW INDEX FROM \`${ originalTableName }\`;`,
        vendor: DBVendors.MYSQL,
        processExitOnError: false,
        shouldReturnClient: false,
    };

    const showIndexResult: DBAccessQueryResult = await DBAccess.query(params);

    if (showIndexResult.error) {
        return;
    }

    const pgIndices: Map<string, Index> = new Map<string, Index>();

    showIndexResult.data.forEach((index: any) => {
        const pgColumnName: string = extraConfigProcessor.getColumnName(
            conversion,
            originalTableName,
            index.Column_name,
            false,
        );

        if (pgIndices.has(index.Key_name)) {
            (pgIndices.get(index.Key_name) as Index).column_name.push(`"${ pgColumnName }"`);
            return; // Continue to the next iteration.
        }

        pgIndices.set(index.Key_name, {
            is_unique: index.Non_unique === 0,
            column_name: [`"${ pgColumnName }"`],
            index_type: ` USING ${ getIndexType(conversion, index.Index_type) }`,
        });
    });

    const _cb = async (index: string): Promise<void> => {
        let sqlAddIndex: string = '';
        const currentIndex: Index = pgIndices.get(index) as Index;

        if (index.toLowerCase() === 'primary') {
            sqlAddIndex = `ALTER TABLE "${ conversion._schema }"."${ tableName }" 
                ADD PRIMARY KEY(${ currentIndex.column_name.join(',') });`;
        } else {
            const columnName: string = currentIndex.column_name
                .map((colName: string) => colName.slice(1, -1))
                .join('_');

            const indexName: string = getUniqueIdentifier(
                `${ tableName }_${ columnName }_idx`,
                '_idx',
            );

            sqlAddIndex = `CREATE ${ (currentIndex.is_unique ? 'UNIQUE' : '') } INDEX "${ indexName }" 
            ON "${ conversion._schema }"."${ tableName }" 
            ${ currentIndex.index_type } (${ currentIndex.column_name.join(',') });`;
        }

        params.vendor = DBVendors.PG;
        params.sql = sqlAddIndex;
        await DBAccess.query(params);
    };

    const addIndexPromises: Promise<void>[] = Array.from(pgIndices.keys()).map(_cb);
    await Promise.all(addIndexPromises);
    log(
        conversion,
        `\t--[${ logTitle }] "${ conversion._schema }"."${ tableName }": PK/indices are successfully set...`,
        (conversion._dicTables.get(tableName) as Table).tableLogPath,
    );
};
