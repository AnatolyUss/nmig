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
import * as sequencesProcessor from './sequences-processor';
import * as migrationStateManager from './migration-state-manager';
import processEnum from './enum-processor';
import processNull from './null-processor';
import processDefault from './default-processor';
import processIndexAndKey from './index-and-key-processor';
import processComments from './comments-processor';
import processForeignKey from './foreign-key-processor';
import processViews from './view-generator';
import Conversion from './conversion';

/**
 * Continues migration process after data loading.
 */
export const processConstraints = async (conversion: Conversion): Promise<Conversion> => {
  const isTableConstraintsLoaded: boolean = await migrationStateManager.get(
    conversion,
    'per_table_constraints_loaded',
  );

  const migrateOnlyData: boolean = conversion.shouldMigrateOnlyData();

  if (!isTableConstraintsLoaded) {
    const _cb = async (tableName: string): Promise<void> => {
      await processConstraintsPerTable(conversion, tableName, migrateOnlyData);
    };

    const promises: Promise<void>[] = conversion._tablesToMigrate.map(_cb);
    await Promise.all(promises);
  }

  if (migrateOnlyData) {
    await migrationStateManager.set(
      conversion,
      'per_table_constraints_loaded',
      'foreign_keys_loaded',
      'views_loaded',
    );
  } else {
    await migrationStateManager.set(conversion, 'per_table_constraints_loaded');
    await processForeignKey(conversion);
    await migrationStateManager.set(conversion, 'foreign_keys_loaded');
    await processViews(conversion);
    await migrationStateManager.set(conversion, 'views_loaded');
  }

  return conversion;
};

/**
 * Processes given table's constraints.
 */
export const processConstraintsPerTable = async (
  conversion: Conversion,
  tableName: string,
  migrateOnlyData: boolean,
): Promise<void> => {
  if (migrateOnlyData) {
    return sequencesProcessor.setSequenceValue(conversion, tableName);
  }

  await processEnum(conversion, tableName);
  await processNull(conversion, tableName);
  await processDefault(conversion, tableName);
  await sequencesProcessor.createIdentity(conversion, tableName);
  await processIndexAndKey(conversion, tableName);
  await processComments(conversion, tableName);
};
