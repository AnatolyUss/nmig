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
import * as extraConfigProcessor from './ExtraConfigProcessor';
import IDBAccessQueryParams from './IDBAccessQueryParams';

/**
 * Escapes quotes inside given string.
 */
const escapeQuotes = (str: string): string => {
    const regexp: RegExp = new RegExp(`'`, 'g');
    return str.replace(regexp, `''`);
};

/**
 * Creates table comments.
 */
const processTableComments = async (conversion: Conversion, tableName: string): Promise<void> => {
    const logTitle: string = 'CommentsProcessor::processTableComments';
    const sqlSelectComment: string = `SELECT table_comment AS table_comment FROM information_schema.tables 
        WHERE table_schema = '${ conversion._mySqlDbName }' 
        AND table_name = '${ extraConfigProcessor.getTableName(conversion, tableName, true) }';`;

    const params: IDBAccessQueryParams = {
        conversion: conversion,
        caller: logTitle,
        sql: sqlSelectComment,
        vendor: DBVendors.MYSQL,
        processExitOnError: false,
        shouldReturnClient: false
    };

    const resultSelectComment: DBAccessQueryResult = await DBAccess.query(params);

    if (resultSelectComment.error) {
        return;
    }

    const comment: string = escapeQuotes(resultSelectComment.data[0].table_comment);
    params.sql = `COMMENT ON TABLE "${ conversion._schema }"."${ tableName }" IS '${ comment }';`;
    params.vendor = DBVendors.PG;
    const createCommentResult: DBAccessQueryResult = await DBAccess.query(params);

    if (createCommentResult.error) {
        return;
    }

    const successMsg: string = `\t--[${ logTitle }] Successfully set comment for table "${ conversion._schema }"."${ tableName }"`;
    log(conversion, successMsg, conversion._dicTables[tableName].tableLogPath);
};

/**
 * Creates columns comments.
 */
const processColumnsComments = async (conversion: Conversion, tableName: string): Promise<void> => {
    const logTitle: string = 'CommentsProcessor::processColumnsComments';
    const originalTableName: string = extraConfigProcessor.getTableName(conversion, tableName, true);

    const commentPromises: Promise<void>[] = conversion._dicTables[tableName].arrTableColumns.map(async (column: any) => {
        if (column.Comment === '') {
            return;
        }

        const columnName: string = extraConfigProcessor.getColumnName(conversion, originalTableName, column.Field, false);
        const comment = escapeQuotes(column.Comment);
        const params: IDBAccessQueryParams = {
            conversion: conversion,
            caller: logTitle,
            sql: `COMMENT ON COLUMN "${ conversion._schema }"."${ tableName }"."${ columnName }" IS '${ comment }';`,
            vendor: DBVendors.PG,
            processExitOnError: false,
            shouldReturnClient: false
        };

        const createCommentResult: DBAccessQueryResult = await DBAccess.query(params);

        if (createCommentResult.error) {
            return;
        }

        const successMsg: string = `\t--[${ logTitle }] Set comment for "${ conversion._schema }"."${ tableName }" column: "${ columnName }"...`;
        log(conversion, successMsg, conversion._dicTables[tableName].tableLogPath);
    });

    await Promise.all(commentPromises);
};

/**
 * Migrates comments.
 */
export default async (conversion: Conversion, tableName: string): Promise<void> => {
    const logTitle: string = 'CommentsProcessor::default';
    const msg: string = `\t--[${ logTitle }] Creates comments for table "${ conversion._schema }"."${ tableName }"...`;
    log(conversion, msg, conversion._dicTables[tableName].tableLogPath);
    await Promise.all([
        processTableComments(conversion, tableName),
        processColumnsComments(conversion, tableName)
    ]);
};
