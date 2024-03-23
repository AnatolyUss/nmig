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
import { PoolConnection } from 'mysql2';
import { PoolClient, PoolConfig } from 'pg';

import Conversion from './conversion';

export interface PgPoolConfig extends PoolConfig {
    // Unfortunately, client_encoding is missing in the original PoolConfig, yet exists in pg module's code.
    client_encoding?: string;
}

export type Encoding =
    | 'ascii'
    | 'utf8'
    | 'utf-8'
    | 'utf16le'
    | 'ucs2'
    | 'ucs-2'
    | 'base64'
    | 'latin1'
    | 'binary'
    | 'hex';

export enum DBVendors {
    MYSQL,
    PG,
}

export enum LogMessageType {
    LOG,
    ERROR,
    CONFIG,
    EXIT,
}

export type LogMessage = {
    readonly type: LogMessageType;
    readonly message?: string | NodeJS.ErrnoException;
    readonly sql?: string;
    readonly tableLogPath?: string;
    readonly config?: Record<string, any>;
};

export type CopyStreamSerializableParams = {
    readonly sqlCopy: string;
    readonly sql: string;
    readonly tableName: string;
    readonly dataPoolId: number;
    readonly originalSessionReplicationRole: string | null;
};

type MessageToDataLoader = {
    readonly config: Record<string, any>;
};

export type MessageToDataReader = MessageToDataLoader & {
    readonly chunk: Record<string, any>;
};

export type MessageToDataWriter = MessageToDataLoader & {
    readonly chunk: Record<string, any>;
    readonly copyStreamSerializableParams: CopyStreamSerializableParams;
};

export type MessageToMaster = {
    readonly tableName: string;
    readonly totalRowsToInsert: number;
};

export type DBAccessQueryParams = {
    conversion: Conversion;
    caller: string;
    sql: string;
    vendor: DBVendors;
    processExitOnError: boolean;
    shouldReturnClient: boolean;
    client?: PoolConnection | PoolClient;
    bindings?: any[];
};

export type DBAccessQueryResult = {
    readonly client?: PoolConnection | PoolClient;
    readonly data?: any;
    readonly error?: any;
};

export type ConfAndLogsPaths = {
    readonly confPath: string;
    readonly logsPath: string;
};

export type Table = {
    readonly tableLogPath: string;
    arrTableColumns: Record<string, any>[];
};

export type Index = {
    readonly is_unique: boolean;
    readonly column_name: string[];
    readonly index_type: string;
};
