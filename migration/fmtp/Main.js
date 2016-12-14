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

const readDataTypesMap        = require('./DataTypesMapReader');
const log                     = require('./Logger');
const generateError           = require('./ErrorGenerator');
const Conversion              = require('./Conversion');
const migrationStateManager   = require('./MigrationStateManager');
const createSchema            = require('./SchemaProcessor');
const cleanup                 = require('./CleanupProcessor');
const dataPoolManager         = require('./DataPoolManager');
const directoriesManager      = require('./DirectoriesManager');
const loadStructureToMigrate  = require('./StructureLoader');
const pipeData                = require('./DataPipeManager');
const boot                    = require('./BootProcessor');

/**
 * Runs migration according to user's configuration.
 *
 * @param {Object} config
 *
 * @returns {undefined}
 */
module.exports = function(config) {
    const self = new Conversion(config);
    boot(self).then(() => {
        return readDataTypesMap(self);
    }).then(
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
            const msg = '\t--[Main] The temporary directory [' + self._tempDirPath + '] already exists...'
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
        () => {
            pipeData(self);
        },
        () => {
            generateError(self, '\t--[Main] NMIG failed to load Data-Units pool...');
            return cleanup(self);
        }
    );
};
