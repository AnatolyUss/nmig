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
import Conversion from './Conversion';
import createSchema from './SchemaProcessor';
import loadStructureToMigrate from './StructureLoader';
import pipeData from './DataPipeManager';
import decodeBinaryData from './BinaryDataDecoder';
import generateReport from './ReportGenerator';
import DBAccess from './DBAccess';
import { dropDataPoolTable } from './DataPoolManager';
import { processConstraints } from './ConstraintsProcessor';
import { getConfAndLogsPaths, boot } from './BootProcessor';
import { createStateLogsTable, dropStateLogsTable } from './MigrationStateManager';
import { createDataPoolTable, readDataPool } from './DataPoolManager';
import { readConfig, readExtraConfig, createLogsDirectory, readDataAndIndexTypesMap } from './FsOps';

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
    .then(pipeData)
    .then(decodeBinaryData)
    .then(processConstraints)
    .then(dropDataPoolTable)
    .then(dropStateLogsTable)
    .then(DBAccess.closeConnectionPools)
    .then(generateReport);
