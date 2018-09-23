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
import log from './Logger';

/**
 * Reads the configuration file.
 */
export function readConfig(baseDir: string, configFileName: string = 'config.json'): Promise<any> {
    return new Promise<any>(resolve => {
        const strPathToConfig = path.join(baseDir, 'config', configFileName);

        fs.readFile(strPathToConfig, (error: Error, data: Buffer) => {
            if (error) {
                console.log(`\n\t--Cannot run migration\nCannot read configuration info from  ${ strPathToConfig }`);
                process.exit();
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

        fs.readFile(strPathToExtraConfig, (error: Error, data: Buffer) => {
            if (error) {
                console.log(`\n\t--Cannot run migration\nCannot read configuration info from ${ strPathToExtraConfig }`);
                process.exit();
            }

            config.extraConfig = JSON.parse(data.toString());
            resolve(config);
        });
    });
}

/**
 * Creates logs directory.
 */
export function createLogsDirectory(conversion: Conversion): Promise<Conversion> {
    return new Promise<Conversion>(resolve => {
        const logTitle: string = 'FsOps::createLogsDirectory';
        console.log(`\t--[${ logTitle }] Creating logs directory...`);

        fs.stat(conversion._logsDirPath, (directoryDoesNotExist: Error, stat: fs.Stats) => {
            if (directoryDoesNotExist) {
                fs.mkdir(conversion._logsDirPath, conversion._0777, e => {
                    if (e) {
                        console.log(`\t--[${ logTitle }] Cannot perform a migration due to impossibility to create "logs_directory": ${ conversion._logsDirPath }`);
                        process.exit();
                    } else {
                        log(conversion, '\t--[logTitle] Logs directory is created...');
                        resolve(conversion);
                    }
                });
            } else if (!stat.isDirectory()) {
                console.log(`\t--[${ logTitle }] Cannot perform a migration due to unexpected error`);
                process.exit();
            } else {
                log(conversion, `\t--[${ logTitle }] Logs directory already exists...`);
                resolve(conversion);
            }
        });
    });
}

/**
 * Reads "./config/data_types_map.json" and converts its json content to js object.
 */
export function readDataTypesMap(conversion: Conversion): Promise<Conversion> {
    return new Promise<Conversion>(resolve => {
        fs.readFile(conversion._dataTypesMapAddr, (error: Error, data: Buffer) => {
            const logTitle: string = 'FsOps::readDataTypesMap';

            if (error) {
                console.log(`\t--[${ logTitle }] Cannot read "DataTypesMap" from ${conversion._dataTypesMapAddr}`);
                process.exit();
            }

            conversion._dataTypesMap = JSON.parse(data.toString());
            console.log(`\t--[${ logTitle }] Data Types Map is loaded...`);
            resolve(conversion);
        });
    });
}
