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

import Conversion from './Conversion';
import { LogMessage, LogMessageType } from './Types';

/**
 * Writes a detailed error message to the "/errors-only.log" file.
 */
const _generateErrorInBackground = (
    conversion: Conversion,
    message: string,
    sql: string = '',
): Promise<void> => {
    return new Promise<void>(async resolve => {
        message += sql !== '' ? `\n\n\tSQL: ${sql}\n\n` : sql;
        const buffer: Buffer = Buffer.from(message, conversion._encoding);
        await _logInBackground(conversion, message);

        fs.open(conversion._errorLogsPath, 'a', conversion._0777, (error: NodeJS.ErrnoException | null, fd: number) => {
            if (error) {
                console.error(error);
                return resolve();
            }

            fs.write(fd, buffer, 0, buffer.length, null, (fsWriteError: NodeJS.ErrnoException | null): void => {
                if (fsWriteError) {
                    console.error(fsWriteError);
                    // !!!Note, still must close current "fd", since recent "fs.open" has definitely succeeded.
                }

                fs.close(fd, () => resolve());
            });
        });
    });
};

/**
 * Outputs given log.
 * Writes given log to the "/all.log" file.
 * If necessary, writes given log to the "/{tableName}.log" file.
 */
const _logInBackground = (
    conversion: Conversion,
    log: string | NodeJS.ErrnoException,
    tableLogPath?: string,
): Promise<void> => {
    return new Promise<void>(resolve => {
        console.log(log);
        const buffer: Buffer = Buffer.from(`${ log }\n\n`, conversion._encoding);

        fs.open(conversion._allLogsPath, 'a', conversion._0777, (err: NodeJS.ErrnoException | null, fd: number) => {
            if (err) {
                console.error(err);
                return resolve();
            }

            fs.write(fd, buffer, 0, buffer.length, null, (fsWriteError: NodeJS.ErrnoException | null): void => {
                if (fsWriteError) {
                    console.error(fsWriteError);
                    // !!!Note, still must close current "fd", since recent "fs.open" has definitely succeeded.
                }

                fs.close(fd, () => {
                    if (tableLogPath) {
                        fs.open(tableLogPath, 'a', conversion._0777, (error: NodeJS.ErrnoException | null, fd: number) => {
                            if (error) {
                                console.error(error);
                                return resolve();
                            } else {
                                fs.write(fd, buffer, 0, buffer.length, null, (fsWriteError: NodeJS.ErrnoException | null): void => {
                                    if (fsWriteError) {
                                        console.error(fsWriteError);
                                        // !!!Note, still must close current "fd", since recent "fs.open" has definitely succeeded.
                                    }

                                    fs.close(fd, () => resolve());
                                });
                            }
                        });
                    } else {
                        return resolve();
                    }
                });
            });
        });
    });
};

/**
 * Conversion instance, used in a context of logger process.
 */
let conv: Conversion;

/**
 * Process incoming logs in a context of logger process.
 */
process.on('message', async (_log: LogMessage): Promise<void> => {
    try {
        if (_log.type === LogMessageType.CONFIG) {
            // Create Conversion instance, but avoid recursion,
            // which might lead to redundant logger processes creation.
            const avoidLogger: boolean = true;
            conv = conv || new Conversion(_log.config, avoidLogger);
        } else if (_log.type === LogMessageType.LOG) {
            await _logInBackground(conv, _log.message as string, _log.tableLogPath);
        } else if (_log.type === LogMessageType.ERROR) {
            await _generateErrorInBackground(conv, _log.message as string, _log.sql);
        } else if (_log.type === LogMessageType.EXIT) {
            // Migration has been just finished.
            // All resources must be released.
            await _logInBackground(conv, _log.message as string, _log.tableLogPath);
            process.exit(0);
        }
    } catch (error) {
        console.log(`\n\t--[LogsProcessor] Logger error: ${JSON.stringify(error)}\n`);
    }
});
