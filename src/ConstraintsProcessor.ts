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
import * as sequencesProcessor from './SequencesProcessor';
import * as dataPoolManager from './DataPoolManager';
import runVacuumFullAndAnalyze from './VacuumProcessor';
import * as migrationStateManager from './MigrationStateManager';
import generateReport from './ReportGenerator';
import processEnum from './EnumProcessor';
import processNull from './NullProcessor';
import processDefault from './DefaultProcessor';
import processIndexAndKey from './IndexAndKeyProcessor';
import processComments from './CommentsProcessor';
import processForeignKey from './ForeignKeyProcessor';
import processViews from './ViewGenerator';
import { dropDataChunkIdColumn } from './ConsistencyEnforcer';
import Conversion from './Conversion';

/**
 * Continues migration process after data loading.
 */
export default async function(conversion: Conversion): Promise<void> {
    const isTableConstraintsLoaded: boolean = await migrationStateManager.get(conversion, 'per_table_constraints_loaded');

    const promises: Promise<void>[] = conversion._tablesToMigrate.map(async (tableName: string) => {
        if (!isTableConstraintsLoaded) {
            if (conversion.shouldMigrateOnlyDataFor(tableName)) {
                await dropDataChunkIdColumn(conversion, tableName);
                return sequencesProcessor.setSequenceValue(conversion, tableName);
            }

            await dropDataChunkIdColumn(conversion, tableName);
            await processEnum(conversion, tableName);
            await processNull(conversion, tableName);
            await processDefault(conversion, tableName);
            await sequencesProcessor.createSequence(conversion, tableName);
            await processIndexAndKey(conversion, tableName);
            await processComments(conversion, tableName);
        }
    });

    await Promise.all(promises);
    await migrationStateManager.set(conversion, 'per_table_constraints_loaded');
    await processForeignKey(conversion);
    await migrationStateManager.set(conversion, 'foreign_keys_loaded');
    await dataPoolManager.dropDataPoolTable(conversion);
    await processViews(conversion);
    await migrationStateManager.set(conversion, 'views_loaded');
    await runVacuumFullAndAnalyze(conversion);
    await migrationStateManager.dropStateLogsTable(conversion);
    generateReport(conversion, 'NMIG migration is accomplished.');
}
