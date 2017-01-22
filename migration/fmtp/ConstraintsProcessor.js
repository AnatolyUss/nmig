/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright (C) 2016 - 2017 Anatoly Khaytovich <anatolyuss@gmail.com>
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

const sequencesProcessor      = require('./SequencesProcessor');
const dataPoolManager         = require('./DataPoolManager');
const runVacuumFullAndAnalyze = require('./VacuumProcessor');
const migrationStateManager   = require('./MigrationStateManager');
const cleanup                 = require('./CleanupProcessor');
const generateReport          = require('./ReportGenerator');
const processEnum             = require('./EnumProcessor');
const processNull             = require('./NullProcessor');
const processDefault          = require('./DefaultProcessor');
const processIndexAndKey      = require('./IndexAndKeyProcessor');
const processComments         = require('./CommentsProcessor');
const processForeignKey       = require('./ForeignKeyProcessor');
const processViews            = require('./ViewGenerator');

/**
 * Continues migration process after data loading, when migrate_only_data is true.
 *
 * @param {Conversion} self
 *
 * @returns {undefined}
 */
const continueProcessAfterDataLoadingShort = self => {
    const promises = [];

    for (let i = 0; i < self._tablesToMigrate.length; ++i) {
        const tableName = self._tablesToMigrate[i];
        promises.push(sequencesProcessor.setSequenceValue(self, tableName));
    }

    Promise.all(promises).then(() => {
        return dataPoolManager.dropDataPoolTable(self);
    }).then(() => {
        return runVacuumFullAndAnalyze(self);
    }).then(() => {
        return migrationStateManager.dropStateLogsTable(self);
    }).then(() => {
        return cleanup(self);
    }).then(
        () => generateReport(self, 'NMIG migration is accomplished.')
    );
}

/**
 * Continues migration process after data loading, when migrate_only_data is false.
 *
 * @param {Conversion} self
 *
 * @returns {undefined}
 */
const continueProcessAfterDataLoadingLong = self => {
    migrationStateManager.get(self, 'per_table_constraints_loaded').then(isTableConstraintsLoaded => {
        const promises = [];

        if (!isTableConstraintsLoaded) {
            for (let i = 0; i < self._tablesToMigrate.length; ++i) {
                const tableName = self._tablesToMigrate[i];
                promises.push(
                    processEnum(self, tableName).then(() => {
                        return processNull(self, tableName);
                    }).then(() => {
                        return processDefault(self, tableName);
                    }).then(() => {
                        return sequencesProcessor.createSequence(self, tableName);
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

/**
 * Continues migration process after data loading.
 *
 * @param {Conversion} self
 *
 * @returns {undefined}
 */
module.exports = self => {
    if (self._migrateOnlyData) {
        continueProcessAfterDataLoadingShort(self);
    } else {
        continueProcessAfterDataLoadingLong(self);
    }
};
