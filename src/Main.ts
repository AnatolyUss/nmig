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
import readDataTypesMap from './DataTypesMapReader';
import Conversion from './Classes/Conversion';
import SchemaProcessor from './SchemaProcessor';
import loadStructureToMigrate from './StructureLoader';
import pipeData from './DataPipeManager';
import boot from './BootProcessor';
import { createStateLogsTable } from './MigrationStateManager';
import { createDataPoolTable, readDataPool } from './DataPoolManager';
import log from './Logger';

const Main = class {

    /**
     * Read the configuration file.
     */
    readConfig(baseDir: string, configFileName: string = 'config.json'): Promise<any> {
        return new Promise(resolve => {
            const strPathToConfig = path.join(baseDir, 'config', configFileName);

            fs.readFile(strPathToConfig, (error: Error, data: any) => {
                if (error) {
                    console.log(`\n\t--Cannot run migration\nCannot read configuration info from  ${ strPathToConfig }`);
                    process.exit();
                }

                const config            = JSON.parse(data);
                config.logsDirPath      = path.join(baseDir, 'logs_directory');
                config.dataTypesMapAddr = path.join(baseDir, 'config', 'data_types_map.json');
                resolve(config);
            });
        });
    }

    /**
     * Read the extra configuration file, if necessary.
     */
    readExtraConfig(config: any, baseDir: string): Promise<any> {
        return new Promise(resolve => {
            if (config.enable_extra_config !== true) {
                config.extraConfig = null;
                return resolve(config);
            }

            const strPathToExtraConfig = path.join(baseDir, 'config', 'extra_config.json');

            fs.readFile(strPathToExtraConfig, (error: Error, data: any) => {
                if (error) {
                    console.log(`\n\t--Cannot run migration\nCannot read configuration info from ${ strPathToExtraConfig }`);
                    process.exit();
                }

                config.extraConfig = JSON.parse(data);
                resolve(config);
            });
        });
    }

    /**
     * Initialize Conversion instance.
     */
    initializeConversion(config: any): Promise<Conversion> {
        return Promise.resolve(new Conversion(config));
    }

    /**
     * Creates logs directory.
     */
    createLogsDirectory(self: Conversion): Promise<Conversion> {
        return new Promise(resolve => {
            console.log('\t--[DirectoriesManager.createLogsDirectory] Creating logs directory...');
            fs.stat(self._logsDirPath, (directoryDoesNotExist, stat) => {
                if (directoryDoesNotExist) {
                    fs.mkdir(self._logsDirPath, self._0777, e => {
                        if (e) {
                            const msg = `\t--[DirectoriesManager.createLogsDirectory] Cannot perform a migration due to impossibility to create 
                                "logs_directory": ${ self._logsDirPath }`;

                            console.log(msg);
                            process.exit();
                        } else {
                            log(self, '\t--[DirectoriesManager.createLogsDirectory] Logs directory is created...');
                            resolve(self);
                        }
                    });
                } else if (!stat.isDirectory()) {
                    console.log('\t--[DirectoriesManager.createLogsDirectory] Cannot perform a migration due to unexpected error');
                    process.exit();
                } else {
                    log(self, '\t--[DirectoriesManager.createLogsDirectory] Logs directory already exists...');
                    resolve(self);
                }
            });
        });
    }
};

module.exports = Main;
const app      = new Main();
const baseDir  = path.join(__dirname, '..', '..');

app.readConfig(baseDir)
    .then(config => {
        return app.readExtraConfig(config, baseDir);
    })
    .then(app.initializeConversion)
    .then(boot)
    .then(readDataTypesMap)
    .then(app.createLogsDirectory)
    .then(conversion => {
        return (new SchemaProcessor(conversion)).createSchema();
    })
    .then(createStateLogsTable)
    .then(createDataPoolTable)
    .then(loadStructureToMigrate)
    .then(readDataPool)
    .then(pipeData)
    .catch(error => console.log(error));
