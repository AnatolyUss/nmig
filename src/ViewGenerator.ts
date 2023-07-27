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
import * as fs from 'node:fs';
import * as path from 'node:path';

import { log } from './FsOps';
import Conversion from './Conversion';
import * as migrationStateManager from './MigrationStateManager';
import DBAccess from './DBAccess';
import { DBAccessQueryParams, DBAccessQueryResult, DBVendors } from './Types';

/**
 * Attempts to convert MySQL view to PostgreSQL view.
 */
const generateView = (schema: string, viewName: string, mysqlViewCode: string): string => {
    mysqlViewCode = mysqlViewCode.split('`').join('"');
    const queryStart: number = mysqlViewCode.indexOf('AS');
    mysqlViewCode = mysqlViewCode.slice(queryStart);
    const arrMysqlViewCode: string[] = mysqlViewCode.split(' ');

    arrMysqlViewCode.forEach((str: string, index: number) => {
        if (
            str.toLowerCase() === 'from' ||
            (str.toLowerCase() === 'join' && index + 1 < arrMysqlViewCode.length)
        ) {
            arrMysqlViewCode[index + 1] = `"${schema}".${arrMysqlViewCode[index + 1]}`;
        }
    });

    return `CREATE OR REPLACE VIEW "${schema}"."${viewName}" ${arrMysqlViewCode.join(' ')};`;
};

/**
 * Writes a log, containing a view code.
 */
const logNotCreatedView = (
    conversion: Conversion,
    viewName: string,
    sql: string,
): Promise<void> => {
    return new Promise<void>(resolve => {
        const viewFilePath: string = path.join(conversion._notCreatedViewsPath, `${viewName}.sql`);
        fs.open(
            viewFilePath,
            'w',
            conversion._0777,
            async (error: NodeJS.ErrnoException | null, fd: number): Promise<void> => {
                if (error) {
                    await log(conversion, error);
                    return resolve();
                }

                const buffer: Buffer = Buffer.from(sql, conversion._encoding);
                fs.write(fd, buffer, 0, buffer.length, null, () => {
                    fs.close(fd, () => {
                        return resolve();
                    });
                });
            },
        );
    });
};

/**
 * Attempts to convert MySQL view to PostgreSQL view.
 */
export default async (conversion: Conversion): Promise<void> => {
    const hasViewsLoaded: boolean = await migrationStateManager.get(conversion, 'views_loaded');

    if (hasViewsLoaded) {
        return;
    }

    const logTitle = 'ViewGenerator::default';

    const _cb = async (view: string): Promise<void> => {
        const params: DBAccessQueryParams = {
            conversion: conversion,
            caller: logTitle,
            sql: `SHOW CREATE VIEW \`${view}\`;`,
            vendor: DBVendors.MYSQL,
            processExitOnError: false,
            shouldReturnClient: false,
        };

        const showCreateViewResult: DBAccessQueryResult = await DBAccess.query(params);

        if (showCreateViewResult.error) {
            return;
        }

        params.sql = generateView(
            conversion._schema,
            view,
            showCreateViewResult.data[0]['Create View'],
        );
        params.vendor = DBVendors.PG;
        const createPgViewResult: DBAccessQueryResult = await DBAccess.query(params);

        if (createPgViewResult.error) {
            await logNotCreatedView(conversion, view, params.sql);
            return;
        }

        await log(
            conversion,
            `\t--[${logTitle}] View "${conversion._schema}"."${view}" is created...`,
        );
    };

    const createViewPromises: Promise<void>[] = conversion._viewsToMigrate.map(_cb);
    await Promise.all(createViewPromises);
};
