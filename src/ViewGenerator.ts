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
import * as fs from 'fs';
import { Stats } from 'fs';
import * as path from 'path';
import log from './Logger';
import generateError from './ErrorGenerator';
import Conversion from './Conversion';
import * as migrationStateManager from './MigrationStateManager';
import DBAccess from './DBAccess';
import DBVendors from './DBVendors';
import DBAccessQueryResult from './DBAccessQueryResult';

/**
 * Attempts to convert MySQL view to PostgreSQL view.
 */
function generateView(schema: string, viewName: string, mysqlViewCode: string): string {
    mysqlViewCode = mysqlViewCode.split('`').join('"');
    const queryStart: number = mysqlViewCode.indexOf('AS');
    mysqlViewCode = mysqlViewCode.slice(queryStart);
    const arrMysqlViewCode: string[] = mysqlViewCode.split(' ');

    arrMysqlViewCode.forEach((str: string, index: number) => {
        if (str.toLowerCase() === 'from' || str.toLowerCase() === 'join' && index + 1 < arrMysqlViewCode.length) {
            arrMysqlViewCode[index + 1] = `"${ schema }".${ arrMysqlViewCode[index + 1] }`;
        }
    });

    return `CREATE OR REPLACE VIEW "${ schema }"."${ viewName }" ${ arrMysqlViewCode.join(' ') };`;
}

/**
 * Writes a log, containing a view code.
 */
function logNotCreatedView(conversion: Conversion, viewName: string, sql: string): Promise<void> {
    return new Promise<void>((resolve) => {
        fs.stat(conversion._notCreatedViewsPath, (directoryDoesNotExist: NodeJS.ErrnoException, stat: Stats) => {
            if (directoryDoesNotExist) {
                fs.mkdir(conversion._notCreatedViewsPath, conversion._0777, (e: NodeJS.ErrnoException) => {
                    if (e) {
                        log(conversion, `\t--[logNotCreatedView] ${ e }`);
                        return resolve();
                    }

                    log(conversion, '\t--[logNotCreatedView] "not_created_views" directory is created...');
                    // "not_created_views" directory is created. Can write the log...
                    fs.open(
                        path.join(conversion._notCreatedViewsPath, `${ viewName }.sql`),
                        'w',
                        conversion._0777,
                        (error: NodeJS.ErrnoException, fd: number
                    ) => {
                        if (error) {
                            log(conversion, error);
                            return resolve();
                        }

                        const buffer = Buffer.from(sql, conversion._encoding);
                        fs.write(fd, buffer, 0, buffer.length, null, () => {
                            fs.close(fd, () => {
                                return resolve();
                            });
                        });
                    });
                });
            }

            if (!stat.isDirectory()) {
                log(conversion, '\t--[logNotCreatedView] Cannot write the log due to unexpected error');
                return resolve();
            }

            // "not_created_views" directory already exists. Can write the log...
            fs.open(
                path.join(conversion._notCreatedViewsPath, `${ viewName }.sql`),
                'w',
                conversion._0777,
                (error: NodeJS.ErrnoException, fd: number
            ) => {
                if (error) {
                    log(conversion, error);
                    return resolve();
                }

                const buffer = Buffer.from(sql, conversion._encoding);
                fs.write(fd, buffer, 0, buffer.length, null, () => {
                    fs.close(fd, () => {
                        return resolve();
                    });
                });
            });
        });
    });
}

/**
 * Attempts to convert MySQL view to PostgreSQL view.
 */
export default async function(conversion: Conversion): Promise<void> {
    const hasViewsLoaded: boolean = await migrationStateManager.get(conversion, 'views_loaded');

    if (hasViewsLoaded) {
        return;
    }

    const createViewPromises: Promise<void>[] = conversion._viewsToMigrate.map(async (view: string) => {
        const sqlShowCreateView: string = `SHOW CREATE VIEW \`${ view }\`;`;
        const logTitle: string = 'ViewGenerator';
        const dbAccess: DBAccess = new DBAccess(conversion);
        const showCreateViewResult: DBAccessQueryResult = await dbAccess.query(logTitle, sqlShowCreateView, DBVendors.MYSQL, false, false);

        if (showCreateViewResult.error) {
            generateError(conversion, `\t--[${ logTitle }] ${ showCreateViewResult.error }`, sqlShowCreateView);
            return;
        }

        const sqlCreatePgView: string = generateView(conversion._schema, view, showCreateViewResult.data[0]['Create View']);
        const createPgViewResult: DBAccessQueryResult = await dbAccess.query(logTitle, sqlCreatePgView, DBVendors.PG, false, false);

        if (createPgViewResult.error) {
            generateError(conversion, `\t--[${ logTitle }] ${ createPgViewResult.error }`, sqlCreatePgView);
            return logNotCreatedView(conversion, view, sqlCreatePgView);
        }

        log(conversion, `\t--[${ logTitle }] View "${ conversion._schema }"."${ view }" is created...`);
    });

    await Promise.all(createViewPromises);
}
