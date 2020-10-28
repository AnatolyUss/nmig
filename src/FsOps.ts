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
import * as path from 'path';
import Conversion from './Conversion';
import ErrnoException = NodeJS.ErrnoException;

/**
 * Writes a detailed error message to the "/errors-only.log" file.
 */
export const generateError = (conversion: Conversion, message: string, sql: string = ''): Promise<void> => {
    return new Promise<void>(resolve => {
        message += `\n\n\tSQL: ${sql}\n\n`;
        const buffer: Buffer = Buffer.from(message, conversion._encoding);
        log(conversion, message, undefined);

        fs.open(conversion._errorLogsPath, 'a', conversion._0777, (error: ErrnoException | null, fd: number) => {
            if (error) {
                return resolve();
            }

            fs.write(fd, buffer, 0, buffer.length, null, () => {
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
export const log = (conversion: Conversion, log: string | NodeJS.ErrnoException, tableLogPath?: string, callback?: Function): void => {
    console.log(log);
    const buffer: Buffer = Buffer.from(`${ log }\n\n`, conversion._encoding);

    fs.open(conversion._allLogsPath, 'a', conversion._0777, (error: ErrnoException | null, fd: number) => {
        if (!error) {
            fs.write(fd, buffer, 0, buffer.length, null, () => {
                fs.close(fd, () => {
                    if (tableLogPath) {
                        fs.open(tableLogPath, 'a', conversion._0777, (error: ErrnoException | null, fd: number) => {
                            if (!error) {
                                fs.write(fd, buffer, 0, buffer.length, null, () => {
                                    fs.close(fd, () => {
                                        // Each async function MUST have a callback (according to Node.js >= 7).
                                        if (callback) {
                                            callback();
                                        }
                                    });
                                });
                            } else  if (callback) {
                                callback(error);
                            }
                        });
                    } else if (callback) {
                        callback();
                    }
                });
            });
        } else if (callback) {
            callback(error);
        }
    });
};

/**
 * Reads and parses JOSN file under given path.
 */
const readAndParseJsonFile = (pathToFile: string): Promise<any> => {
    return new Promise<any>(resolve => {
        fs.readFile(pathToFile, (error: ErrnoException | null, data: Buffer) => {
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
export const readConfig = async (confPath: string, logsPath: string, configFileName: string = 'config.json'): Promise<any> => {
    const pathToConfig = path.join(confPath, configFileName);
    const config: any = await readAndParseJsonFile(pathToConfig);
    config.logsDirPath = path.join(logsPath, 'logs_directory');
    config.dataTypesMapAddr = path.join(confPath, 'data_types_map.json');
    config.indexTypesMapAddr = path.join(confPath, 'index_types_map.json');
    return config;
};

/**
 * Reads the extra configuration file, if necessary.
 */
export const readExtraConfig = async (config: any, confPath: string, extraConfigFileName: string = 'extra_config.json'): Promise<any> => {
    if (config.enable_extra_config !== true) {
        config.extraConfig = null;
        return config;
    }

    const pathToExtraConfig = path.join(confPath, extraConfigFileName);
    config.extraConfig = await readAndParseJsonFile(pathToExtraConfig);
    return config;
};

/**
 * Reads both "./config/data_types_map.json" and "./config/index_types_map.json" and converts its json content to js object.
 */
export const readDataAndIndexTypesMap = async (conversion: Conversion): Promise<Conversion> => {
    const logTitle: string = 'FsOps::readDataAndIndexTypesMap';
    conversion._dataTypesMap = await readAndParseJsonFile(conversion._dataTypesMapAddr);
    conversion._indexTypesMap = await readAndParseJsonFile(conversion._indexTypesMapAddr);
    log(conversion, `\t--[${ logTitle }] Data and Index Types Maps are loaded...`);
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
const createDirectory = (conversion: Conversion, directoryPath: string, logTitle: string): Promise<void> => {
    return new Promise<void>(resolve => {
        console.log(`\t--[${ logTitle }] Creating directory ${ directoryPath }...`);

        fs.stat(directoryPath, (directoryDoesNotExist: ErrnoException | null, stat: fs.Stats) => {
            if (directoryDoesNotExist) {
                fs.mkdir(directoryPath, conversion._0777, e => {
                    if (e) {
                        console.log(`\t--[${ logTitle }] Cannot perform a migration due to impossibility to create directory: ${ directoryPath }`);
                        process.exit(1);
                    } else {
                        log(conversion, `\t--[${ logTitle }] Directory ${ directoryPath } is created...`);
                        resolve();
                    }
                });
            } else if (!stat.isDirectory()) {
                console.log(`\t--[${ logTitle }] Cannot perform a migration due to unexpected error`);
                process.exit(1);
            } else {
                log(conversion, `\t--[${ logTitle }] Directory ${ directoryPath } already exists...`);
                resolve();
            }
        });
    });
};
