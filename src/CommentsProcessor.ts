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
import DBAccessQueryResult from './DBAccessQueryResult';
import * as extraConfigProcessor from './ExtraConfigProcessor';
import generateError from './ErrorGenerator';

/**
 * Escapes quotes inside given string.
 */
function escapeQuotes(str: string): string {
    const regexp: RegExp = new RegExp(`'`, 'g');
    return str.replace(regexp, `''`);
}

/**
 * Creates table comments.
 */
async function processTableComments(conversion: Conversion, tableName: string): Promise<void> {
    const logTitle: string = 'CommentsProcessor::processTableComments';
    const dbAccess: DBAccess = new DBAccess(conversion);
    const sqlSelectComment: string = `SELECT table_comment AS table_comment FROM information_schema.tables 
        WHERE table_schema = '${ conversion._mySqlDbName }' 
        AND table_name = '${ extraConfigProcessor.getTableName(conversion, tableName, true) }';`;

    const resultSelectComment: DBAccessQueryResult = await dbAccess.query(logTitle, sqlSelectComment, DBVendors.MYSQL, false, false);

    if (resultSelectComment.error) {
        generateError(conversion, `\t--[${ logTitle }] ${ resultSelectComment.error }`, sqlSelectComment);
        return;
    }

    const comment: string = escapeQuotes(resultSelectComment.data[0].table_comment);
    const sqlCreateComment: string = `COMMENT ON TABLE "${ conversion._schema }"."${ tableName }" IS '${ comment }';`;
    const createCommentResult: DBAccessQueryResult = await dbAccess.query(logTitle, sqlCreateComment, DBVendors.PG, false, false);

    if (createCommentResult.error) {
        const msg: string = `\t--[${ logTitle }] Error while processing comment for 
            "${ conversion._schema }"."${ tableName }"...\n${ createCommentResult.error }`;

        generateError(conversion, msg, sqlCreateComment);
        return;
    }

    const successMsg: string = `\t--[${ logTitle }] Successfully set comment for table "${ conversion._schema }"."${ tableName }"`;
    log(conversion, successMsg, conversion._dicTables[tableName].tableLogPath);
}

/**
 * Creates columns comments.
 */
async function processColumnsComments(conversion: Conversion, tableName: string): Promise<void> {
    const logTitle: string = 'CommentsProcessor::processColumnsComments';
    const originalTableName: string = extraConfigProcessor.getTableName(conversion, tableName, true);
    const dbAccess: DBAccess = new DBAccess(conversion);
    const commentPromises: Promise<void>[] = conversion._dicTables[tableName].arrTableColumns.map(async (column: any) => {
        if (column.Comment === '') {
            return;
        }

        const columnName: string = extraConfigProcessor.getColumnName(conversion, originalTableName, column.Field, false);
        const comment = escapeQuotes(column.Comment);
        const sqlCreateComment: string = `COMMENT ON COLUMN "${ conversion._schema }"."${ tableName }"."${ columnName }" IS '${ comment }';`;
        const createCommentResult: DBAccessQueryResult = await dbAccess.query(logTitle, sqlCreateComment, DBVendors.PG, false, false);

        if (createCommentResult.error) {
            const msg: string = `\t--[${ logTitle }] Error while processing comment for 
            "${ conversion._schema }"."${ tableName }"...\n${ createCommentResult.error }`;

            generateError(conversion, msg, sqlCreateComment);
            return;
        }

        const successMsg: string = `\t--[${ logTitle }] Set comment for "${ conversion._schema }"."${ tableName }" column: "${ columnName }"...`;
        log(conversion, successMsg, conversion._dicTables[tableName].tableLogPath);
    });

    await Promise.all(commentPromises);
}

/**
 * Migrates comments.
 */
export default async function(conversion: Conversion, tableName: string): Promise<void> {
    const logTitle: string = 'CommentsProcessor::default';
    const msg: string = `\t--[${ logTitle }] Creates comments for table "${ conversion._schema }"."${ tableName }"...`;
    log(conversion, msg, conversion._dicTables[tableName].tableLogPath);
    await Promise.all([
        processTableComments(conversion, tableName),
        processColumnsComments(conversion, tableName)
    ]);
}
