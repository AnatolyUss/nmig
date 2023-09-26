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
import { Writable } from 'node:stream';
import * as streamPromises from 'node:stream/promises';

import { PoolClient } from 'pg';
import { from } from 'pg-copy-streams';

import { log } from './FsOps';
import { MessageToDataWriter } from './Types';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import DataPipeManager from './DataPipeManager';

/**
 * After accepting the message, initializes data streaming from current process stdin into PostgreSQL (via COPY).
 */
process.on('message', async (signal: MessageToDataWriter): Promise<void> => {
    const { config, chunk, copyStreamSerializableParams } = signal;

    // Create Conversion instance, but avoid creating a separate logger process.
    const avoidLogger = true;
    const conv: Conversion = new Conversion(config, avoidLogger);

    const fullTableName = `"${conv._schema}"."${chunk._tableName}"`;
    await log(conv, `\t--[NMIG DataWriter] Loading the data into ${fullTableName} table...`);

    const { sqlCopy, sql, tableName, dataPoolId, originalSessionReplicationRole } =
        copyStreamSerializableParams;

    const client: PoolClient = await DBAccess.getPgClient(conv);

    if (conv.shouldMigrateOnlyData()) {
        await DataPipeManager.disablePgTriggers(conv, client);
    }

    const copyStream: Writable = client.query(from(sqlCopy));

    try {
        await streamPromises.pipeline(process.stdin, copyStream);
    } catch (pipelineError) {
        await DataPipeManager.processDataError(
            conv,
            pipelineError as string,
            sql,
            sqlCopy,
            tableName,
            dataPoolId,
            client,
            originalSessionReplicationRole,
        );
    }

    process.exit(0);
});
