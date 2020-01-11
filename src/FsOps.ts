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
 * Writes given buffer to an appropriate log file.
 */
function _writeLog(conversion: Conversion, logFilePath: string, bufferedLog: Buffer): void {
    const fd: number = fs.openSync(logFilePath, 'a', conversion._0777);
    fs.writeSync(fd, bufferedLog, 0, bufferedLog.length, null);
    fs.closeSync(fd);
}

/**
 * Writes a detailed error message to the "/errors-only.log" file.
 */
export function generateError(conversion: Conversion, message: string, sql: string = ''): void {
    message += `\n\n\tSQL: ${sql}\n\n`;
    const buffer: Buffer = Buffer.from(message, conversion._encoding);
    log(conversion, message, undefined);
    _writeLog(conversion, conversion._errorLogsPath, buffer);
}

/**
 * Outputs given log.
 * Writes given log to the "/all.log" file.
 * If necessary, writes given log to the "/{tableName}.log" file.
 */
export function log(conversion: Conversion, log: string | NodeJS.ErrnoException, tableLogPath?: string): void {
    const buffer: Buffer = Buffer.from(`${ log }\n\n`, conversion._encoding);
    _writeLog(conversion, conversion._allLogsPath, buffer);

    if (tableLogPath) {
        _writeLog(conversion, tableLogPath, buffer);
    }
}

/**
 * Reads the configuration file.
 */
export function readConfig(baseDir: string, configFileName: string = 'config.json'): Promise<any> {
    return new Promise<any>(resolve => {
        const strPathToConfig = path.join(baseDir, 'config', configFileName);

        fs.readFile(strPathToConfig, (error: ErrnoException | null, data: Buffer) => {
            if (error) {
                console.log(`\n\t--Cannot run migration\nCannot read configuration info from  ${ strPathToConfig }`);
                process.exit(1);
            }

            const config: any = JSON.parse(data.toString());
            config.logsDirPath = path.join(baseDir, 'logs_directory');
            config.dataTypesMapAddr = path.join(baseDir, 'config', 'data_types_map.json');
            resolve(config);
        });
    });
}

/**
 * Reads the extra configuration file, if necessary.
 */
export function readExtraConfig(config: any, baseDir: string): Promise<any> {
    return new Promise<any>(resolve => {
        if (config.enable_extra_config !== true) {
            config.extraConfig = null;
            return resolve(config);
        }

        const strPathToExtraConfig = path.join(baseDir, 'config', 'extra_config.json');

        fs.readFile(strPathToExtraConfig, (error: ErrnoException | null, data: Buffer) => {
            if (error) {
                console.log(`\n\t--Cannot run migration\nCannot read configuration info from ${ strPathToExtraConfig }`);
                process.exit(1);
            }

            config.extraConfig = JSON.parse(data.toString());
            resolve(config);
        });
    });
}

/**
 * Creates logs directory.
 */
export async function createLogsDirectory(conversion: Conversion): Promise<Conversion> {
    const logTitle: string = 'FsOps::createLogsDirectory';
    await createDirectory(conversion, conversion._logsDirPath, logTitle);
    await createDirectory(conversion, conversion._notCreatedViewsPath, logTitle);
    return conversion;
}

/**
 * Creates a directory at the specified path.
 */
function createDirectory(conversion: Conversion, directoryPath: string, logTitle: string): Promise<void> {
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
}

/**
 * Reads "./config/data_types_map.json" and converts its json content to js object.
 */
export function readDataTypesMap(conversion: Conversion): Promise<Conversion> {
    return new Promise<Conversion>(resolve => {
        fs.readFile(conversion._dataTypesMapAddr, (error: ErrnoException | null, data: Buffer) => {
            const logTitle: string = 'FsOps::readDataTypesMap';

            if (error) {
                console.log(`\t--[${ logTitle }] Cannot read "DataTypesMap" from ${conversion._dataTypesMapAddr}`);
                process.exit(1);
            }

            conversion._dataTypesMap = JSON.parse(data.toString());
            console.log(`\t--[${ logTitle }] Data Types Map is loaded...`);
            resolve(conversion);
        });
    });
}
