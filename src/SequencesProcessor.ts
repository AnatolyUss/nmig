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
import { PoolClient } from 'pg';
import * as extraConfigProcessor from './ExtraConfigProcessor';

/**
 * Sets sequence value.
 */
export const setSequenceValue = async (conversion: Conversion, tableName: string): Promise<void> => {
    const originalTableName: string = extraConfigProcessor.getTableName(conversion, tableName, true);
    const autoIncrementedColumn: any = conversion._dicTables[tableName].arrTableColumns.find((column: any) => column.Extra === 'auto_increment');

    if (!autoIncrementedColumn) {
        // No auto-incremented column found.
        return;
    }

    const logTitle: string = 'SequenceProcessor::setSequenceValue';
    const columnName: string = extraConfigProcessor.getColumnName(conversion, originalTableName, autoIncrementedColumn.Field, false);
    const seqName: string = `${ tableName }_${ columnName }_seq`;
    const sql: string = `SELECT SETVAL(\'"${ conversion._schema }"."${ seqName }"\', 
                (SELECT MAX("${ columnName }") FROM "${ conversion._schema }"."${ tableName }"));`;

    const params: IDBAccessQueryParams = {
        conversion: conversion,
        caller: logTitle,
        sql: sql,
        vendor: DBVendors.PG,
        processExitOnError: false,
        shouldReturnClient: false
    };

    const result: DBAccessQueryResult = await DBAccess.query(params);

    if (!result.error) {
        const successMsg: string = `\t--[${ logTitle }] Sequence "${ conversion._schema }"."${ seqName }" is created...`;
        log(conversion, successMsg, conversion._dicTables[tableName].tableLogPath);
    }
};

/**
 * Defines which column in given table has the "auto_increment" attribute.
 * Creates an appropriate sequence.
 */
export const createSequence = async (conversion: Conversion, tableName: string): Promise<void> => {
    const originalTableName: string = extraConfigProcessor.getTableName(conversion, tableName, true);
    const autoIncrementedColumn: any = conversion._dicTables[tableName].arrTableColumns.find((column: any) => column.Extra === 'auto_increment');

    if (!autoIncrementedColumn) {
        // No auto-incremented column found.
        return;
    }

    const columnName: string = extraConfigProcessor.getColumnName(conversion, originalTableName, autoIncrementedColumn.Field, false);
    const logTitle: string = 'SequencesProcessor::createSequence';
    const seqName: string = `${ tableName }_${ columnName }_seq`;
    const params: IDBAccessQueryParams = {
        conversion: conversion,
        caller: logTitle,
        sql: `CREATE SEQUENCE "${ conversion._schema }"."${ seqName }";`,
        vendor: DBVendors.PG,
        processExitOnError: false,
        shouldReturnClient: true
    };

    const createSequenceResult: DBAccessQueryResult = await DBAccess.query(params);

    if (createSequenceResult.error) {
        await DBAccess.releaseDbClient(conversion, <PoolClient>createSequenceResult.client);
        return;
    }

    params.client = createSequenceResult.client;
    params.sql = `ALTER TABLE "${ conversion._schema }"."${ tableName }" ALTER COLUMN "${ columnName }" 
        SET DEFAULT NEXTVAL('${ conversion._schema }.${ seqName }');`;

    const setNextValResult: DBAccessQueryResult = await DBAccess.query(params);

    if (setNextValResult.error) {
        await DBAccess.releaseDbClient(conversion, <PoolClient>setNextValResult.client);
        return;
    }

    params.client = setNextValResult.client;
    params.sql = `ALTER SEQUENCE "${ conversion._schema }"."${ seqName }" OWNED BY "${ conversion._schema }"."${ tableName }"."${ columnName }";`;

    const setSequenceOwnerResult: DBAccessQueryResult = await DBAccess.query(params);

    if (setSequenceOwnerResult.error) {
        await DBAccess.releaseDbClient(conversion, <PoolClient>setSequenceOwnerResult.client);
        return;
    }

    params.client = setSequenceOwnerResult.client;
    params.shouldReturnClient = false;
    params.sql = `SELECT SETVAL(\'"${ conversion._schema }"."${ seqName }"\', (SELECT MAX("${ columnName }") FROM "${ conversion._schema }"."${ tableName }"));`;

    const sqlSetSequenceValueResult: DBAccessQueryResult = await DBAccess.query(params);

    if (!sqlSetSequenceValueResult.error) {
        const successMsg: string = `\t--[${ logTitle }] Sequence "${ conversion._schema }"."${ seqName }" is created...`;
        log(conversion, successMsg, conversion._dicTables[tableName].tableLogPath);
    }
};
