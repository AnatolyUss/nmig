/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright 2016 Anatoly Khaytovich <anatolyuss@gmail.com>
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
'use strict';

const fs                      = require('fs');
const childProcess            = require('child_process');
const processViews            = require('./ViewGenerator');
const readDataTypesMap        = require('./DataTypesMapReader');
const log                     = require('./Logger');
const generateError           = require('./ErrorGenerator');
const MessageToDataLoader     = require('./MessageToDataLoader');
const Conversion              = require('./Conversion');
const generateReport          = require('./ReportGenerator');
const processComments         = require('./CommentsProcessor');
const migrationStateManager   = require('./MigrationStateManager');
const processIndexAndKey      = require('./IndexAndKeyProcessor');
const processForeignKey       = require('./ForeignKeyProcessor');
const createSequence          = require('./SequencesProcessor');
const runVacuumFullAndAnalyze = require('./VacuumProcessor');
const processEnum             = require('./EnumProcessor');
const processNull             = require('./NullProcessor');
const processDefault          = require('./DefaultProcessor');
const createSchema            = require('./SchemaProcessor');
const cleanup                 = require('./CleanupProcessor');
const dataPoolManager         = require('./DataPoolManager');
const directoriesManager      = require('./DirectoriesManager');
const loadStructureToMigrate  = require('./StructureLoader');

let self                  = null;
let intProcessedDataUnits = 0;

/**
 * Kill a process specified by the pid.
 *
 * @param {Number} pid
 *
 * @returns {undefined}
 */
function killProcess(pid) {
    try {
        process.kill(pid);
    } catch (killError) {
        generateError(self, '\t--[killProcess] ' + killError);
    }
}

/**
 * Instructs DataLoader which DataUnits should be loaded.
 * No need to check the state-log.
 * If dataPool's length is zero, then nmig will proceed to the next step.
 *
 * @returns {undefined}
 */
function pipeData() {
    if (self._dataPool.length === 0) {
        return continueProcessAfterDataLoading();
    }

    let strDataLoaderPath = __dirname + '/DataLoader.js';
    let options           = self._loaderMaxOldSpaceSize === 'DEFAULT' ? {} : { execArgv: ['--max-old-space-size=' + self._loaderMaxOldSpaceSize] };
    let loaderProcess     = childProcess.fork(strDataLoaderPath, options);

    loaderProcess.on('message', signal => {
        if (typeof signal === 'object') {
            self._dicTables[signal.tableName].totalRowsInserted += signal.rowsInserted;
            let msg = '\t--[pipeData]  For now inserted: ' + self._dicTables[signal.tableName].totalRowsInserted + ' rows, '
                    + 'Total rows to insert into "' + self._schema + '"."' + signal.tableName + '": ' + signal.totalRowsToInsert;

            log(self, msg);
        } else {
            killProcess(loaderProcess.pid);
            intProcessedDataUnits += self._pipeWidth;
            return intProcessedDataUnits < self._dataPool.length ? pipeData() : continueProcessAfterDataLoading();
        }
    });

    let intEnd  = self._dataPool.length - (self._dataPool.length - self._pipeWidth - intProcessedDataUnits);
    let message = new MessageToDataLoader(self._config, self._dataPool.slice(intProcessedDataUnits, intEnd));
    loaderProcess.send(message);
}

/**
 * Continues migration process after data loading.
 *
 * @returns {undefined}
 */
function continueProcessAfterDataLoading() {
    if (self._migrateOnlyData) {
        dataPoolManager.dropDataPoolTable(self).then(() => {
            return runVacuumFullAndAnalyze(self);
        }).then(() => {
            return migrationStateManager.dropStateLogsTable(self);
        }).then(() => {
            return cleanup(self);
        }).then(
            () => generateReport(self, 'NMIG migration is accomplished.')
        );

    } else {
        migrationStateManager.get(self, 'per_table_constraints_loaded').then(isTableConstraintsLoaded => {
            let promises = [];

            if (!isTableConstraintsLoaded) {
                for (let i = 0; i < self._tablesToMigrate.length; ++i) {
                    let tableName = self._tablesToMigrate[i];
                    promises.push(
                        processEnum(self, tableName).then(() => {
                            return processNull(self, tableName);
                        }).then(() => {
                            return processDefault(self, tableName);
                        }).then(() => {
                            return createSequence(self, tableName);
                        }).then(() => {
                            return processIndexAndKey(self, tableName);
                        }).then(() => {
                            return processComments(self, tableName);
                        })
                    );
                }
            }

            Promise.all(promises).then(() => {
                migrationStateManager.set(self, 'per_table_constraints_loaded').then(() => {
                    return processForeignKey(self);
                }).then(() => {
                    return migrationStateManager.set(self, 'foreign_keys_loaded');
                }).then(() => {
                    return dataPoolManager.dropDataPoolTable(self);
                }).then(() => {
                    return processViews(self);
                }).then(() => {
                    return migrationStateManager.set(self, 'views_loaded');
                }).then(() => {
                    return runVacuumFullAndAnalyze(self);
                }).then(() => {
                    return migrationStateManager.dropStateLogsTable(self);
                }).then(() => {
                    return cleanup(self);
                }).then(
                    () => generateReport(self, 'NMIG migration is accomplished.')
                );
            });
        });
    }
}

/**
 * Runs migration according to user's configuration.
 *
 * @param {Object} config
 *
 * @returns {undefined}
 */
module.exports = function(config) {
    console.log('\n\tNMIG - the database migration tool\n\tCopyright 2016 Anatoly Khaytovich <anatolyuss@gmail.com>\n\t Boot...');
    self = new Conversion(config);

    readDataTypesMap(self).then(
        () => {
            return directoriesManager.createLogsDirectory(self);
        },
        () => {
            // Braces are essential. Without them promises-chain will continue execution.
            console.log('\t--[Main] Failed to boot migration');
        }
    ).then(
        () => {
            return directoriesManager.createTemporaryDirectory(self);
        },
        () => {
            // Braces are essential. Without them promises-chain will continue execution.
            log(self, '\t--[Main] Logs directory was not created...');
        }
    ).then(
        () => {
            return createSchema(self);
        },
        () => {
            let msg = '\t--[Main] The temporary directory [' + self._tempDirPath + '] already exists...'
                    + '\n\t  Please, remove this directory and rerun NMIG...';

            log(self, msg);
        }
    ).then(
        () => {
            return migrationStateManager.createStateLogsTable(self);
        },
        () => {
            generateError(self, '\t--[Main] Cannot create new DB schema...');
            return cleanup(self);
        }
    ).then(
        () => {
            return dataPoolManager.createDataPoolTable(self);
        },
        () => {
            generateError(self, '\t--[Main] Cannot create execution_logs table...');
            return cleanup(self);
        }
    ).then(
        () => {
            return loadStructureToMigrate(self);
        },
        () => {
            generateError(self, '\t--[Main] Cannot create data-pool...');
            return cleanup(self);
        }
    ).then(
        () => {
            return dataPoolManager.readDataPool(self);
        },
        () => {
            generateError(self, '\t--[Main] NMIG cannot load source database structure...');
            return cleanup(self);
        }
    ).then(
        pipeData,
        () => {
            generateError(self, '\t--[Main] NMIG failed to load Data-Units pool...');
            return cleanup(self);
        }
    );
};
