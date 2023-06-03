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
import { Writable, promises as streamPromises } from 'node:stream';

import { PoolClient } from 'pg';
const { from } = require('pg-copy-streams'); // No declaration file for module "pg-copy-streams".

import { log } from './FsOps';
// import { getCopyStream } from './DataReader'; // TODO: DO NOT import DataReader!!!
import { MessageToDataWriter, CopyStreamSerializableParams } from './Types';
import Conversion from './Conversion';
import DBAccess from './DBAccess';

/**
 * Returns new PostgreSQL copy stream object.
 */
export const getCopyStream = (
    conv: Conversion,
    client: PoolClient,
    copyStreamSerializableParams: CopyStreamSerializableParams,
): Writable => {
    const {
        sqlCopy,
        sql,
        tableName,
        dataPoolId,
        originalSessionReplicationRole,
    } = copyStreamSerializableParams;

    const copyStream: Writable = client.query(from(sqlCopy));

    // copyStream.on('error', async (copyStreamError: string): Promise<void> => {
    //     await processDataError(
    //         conv,
    //         copyStreamError,
    //         sql,
    //         sqlCopy,
    //         tableName,
    //         dataPoolId,
    //         client,
    //         originalSessionReplicationRole,
    //     );
    // });

    return copyStream;
};

/**
 * TODO: add description.
 */
process.on('message', async (signal: MessageToDataWriter): Promise<void> => {
    const { config, chunk, copyStreamSerializableParams } = signal;
    const conv: Conversion = new Conversion(config);
    const fullTableName: string = `"${ conv._schema }"."${ chunk._tableName }"`;
    log(conv, `\t--[NMIG DataWriter] Loading the data into ${ fullTableName } table...`);
    const client: PoolClient = await DBAccess.getPgClient(conv);
    const copyStream: Writable = getCopyStream(conv, client, copyStreamSerializableParams);
    // process.stdin.pipe(copyStream);
    // TODO: should I apply errors-handling using "catch"?
    await streamPromises.pipeline(process.stdin, copyStream);
    process.exit(0); // TODO: probably unnecessary...
});
