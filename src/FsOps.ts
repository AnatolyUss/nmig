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
import { ChildProcess } from 'node:child_process';

import Conversion from './Conversion';
import {
    LogMessage,
    LogMessageType,
} from './Types';

/**
 * Sends error-log to dedicated logger process.
 */
export const generateError = async (
    conversion: Conversion,
    message: string,
    sql: string = '',
): Promise<void> => {
    if (conversion.logger) {
        const logMessage: LogMessage = {
            type: LogMessageType.ERROR,
            message: message,
            sql: sql,
        };

        (conversion.logger as ChildProcess).send(logMessage);
        return;
    }

    await generateErrorInBackground(conversion, message, sql);
};

/**
 * Sends log to dedicated logger process.
 */
export const log = async (
    conversion: Conversion,
    message: string | NodeJS.ErrnoException,
    tableLogPath?: string,
    isConcluding: boolean = false,
): Promise<void> => {
    if (conversion.logger) {
        const type: LogMessageType = isConcluding ? LogMessageType.EXIT : LogMessageType.LOG;
        const logMessage: LogMessage = { type, message, tableLogPath };
        (conversion.logger as ChildProcess).send(logMessage);
        return;
    }

    await logInBackground(conversion, message, tableLogPath);
};

/**
 * Writes a detailed error message to the "/errors-only.log" file.
 */
export const generateErrorInBackground = (
    conversion: Conversion,
    message: string,
    sql: string = '',
): Promise<void> => {
    return new Promise<void>(async resolve => {
        message += sql !== '' ? `\n\n\tSQL: ${sql}\n\n` : sql;
        const buffer: Buffer = Buffer.from(message, conversion._encoding);
        await logInBackground(conversion, message);

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
export const logInBackground = (
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
 * Reads and parses JOSN file under given path.
 */
const readAndParseJsonFile = (pathToFile: string): Promise<any> => {
    return new Promise<any>(resolve => {
        fs.readFile(pathToFile, (error: NodeJS.ErrnoException | null, data: Buffer) => {
            if (error) {
                console.log(`\n\t--Cannot run migration\nCannot read configuration info from  ${ pathToFile }`);
                process.exit(1);
            }

            const config: any = JSON.parse(data.toString());
            resolve(config);
        });
    });
};

/**
 * Reads the configuration file.
 */
export const readConfig = async (
    confPath: string,
    logsPath: string,
    configFileName: string = 'config.json',
): Promise<any> => {
    const pathToConfig: string = path.join(confPath, configFileName);
    const config: any = await readAndParseJsonFile(pathToConfig);
    config.logsDirPath = path.join(logsPath, 'logs_directory');
    config.dataTypesMapAddr = path.join(confPath, 'data_types_map.json');
    config.indexTypesMapAddr = path.join(confPath, 'index_types_map.json');
    return config;
};

/**
 * Reads the extra configuration file, if necessary.
 */
export const readExtraConfig = async (
    config: any,
    confPath: string,
    extraConfigFileName: string = 'extra_config.json',
): Promise<any> => {
    if (config.enable_extra_config !== true) {
        config.extraConfig = null;
        return config;
    }

    const pathToExtraConfig: string = path.join(confPath, extraConfigFileName);
    config.extraConfig = await readAndParseJsonFile(pathToExtraConfig);
    return config;
};

/**
 * Reads both "./config/data_types_map.json" and "./config/index_types_map.json"
 * and converts its json content to js object.
 */
export const readDataAndIndexTypesMap = async (conversion: Conversion): Promise<Conversion> => {
    const logTitle: string = 'FsOps::readDataAndIndexTypesMap';
    conversion._dataTypesMap = await readAndParseJsonFile(conversion._dataTypesMapAddr);
    conversion._indexTypesMap = await readAndParseJsonFile(conversion._indexTypesMapAddr);
    await log(conversion, `\t--[${ logTitle }] Data and Index Types Maps are loaded...`);
    return conversion;
};

/**
 * Creates logs directory.
 */
export const createLogsDirectory = async (conversion: Conversion): Promise<Conversion> => {
    const logTitle: string = 'FsOps::createLogsDirectory';
    await createDirectory(conversion, conversion._logsDirPath, logTitle);
    await createDirectory(conversion, conversion._notCreatedViewsPath, logTitle);
    return conversion;
};

/**
 * Creates a directory at the specified path.
 */
const createDirectory = (
    conversion: Conversion,
    directoryPath: string,
    logTitle: string,
): Promise<void> => {
    return new Promise<void>(resolve => {
        console.log(`\t--[${ logTitle }] Creating directory ${ directoryPath }...`);

        fs.stat(directoryPath, async (directoryDoesNotExist: NodeJS.ErrnoException | null, stat: fs.Stats) => {
            if (directoryDoesNotExist) {
                fs.mkdir(directoryPath, conversion._0777, async e => {
                    if (e) {
                        console.log(`\t--[${ logTitle }] Cannot perform migration.`);
                        console.log(`\t--[${ logTitle }] Failed to create directory ${ directoryPath }`);
                        process.exit(1);
                    }

                    await log(conversion, `\t--[${ logTitle }] Directory ${ directoryPath } is created...`);
                    resolve();
                });

                return;
            }

            if (!stat.isDirectory()) {
                console.log(`\t--[${ logTitle }] Cannot perform a migration due to unexpected error`);
                process.exit(1);
            }

            await log(conversion, `\t--[${ logTitle }] Directory ${ directoryPath } already exists...`);
            resolve();
        });
    });
};
