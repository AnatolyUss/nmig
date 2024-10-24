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
import Conversion from './conversion';
import createSchema from './schema-processor';
import loadStructureToMigrate from './structure-loader';
import DataPipeManager from './data-pipe-manager';
import decodeBinaryData from './binary-data-decoder';
import generateReport from './report-generator';
import DbAccess from './db-access';
import { dropDataPoolTable } from './data-pool-manager';
import { processConstraints } from './constraints-processor';
import { getConfAndLogsPaths, boot } from './boot-processor';
import { createStateLogsTable, dropStateLogsTable } from './migration-state-manager';
import { createDataPoolTable, readDataPool } from './data-pool-manager';
import {
  readConfig,
  readExtraConfig,
  createLogsDirectory,
  readDataAndIndexTypesMap,
} from './fs-ops';

const { confPath, logsPath } = getConfAndLogsPaths();

readConfig(confPath, logsPath)
  .then(config => readExtraConfig(config, confPath))
  .then(Conversion.initializeConversion)
  .then(createLogsDirectory)
  .then(readDataAndIndexTypesMap)
  .then(boot)
  .then(createSchema)
  .then(createStateLogsTable)
  .then(createDataPoolTable)
  .then(loadStructureToMigrate)
  .then(readDataPool)
  .then(DataPipeManager.runDataPipe)
  .then(decodeBinaryData)
  .then(processConstraints)
  .then(dropDataPoolTable)
  .then(dropStateLogsTable)
  .then(DbAccess.closeConnectionPools)
  .then(generateReport)
  .catch((error: Error) => console.log(`\t--[Main] error: ${error}`));
