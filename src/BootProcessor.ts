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
import * as path from 'path';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import DBAccessQueryResult from './DBAccessQueryResult';
import DBVendors from './DBVendors';
import IDBAccessQueryParams from './IDBAccessQueryParams';
import IConfAndLogsPaths from './IConfAndLogsPaths';
import { getStateLogsTableName } from './MigrationStateManager';
import { generateError, log } from './FsOps';

/**
 * Checks correctness of connection details of both MySQL and PostgreSQL.
 */
export const checkConnection = async (conversion: Conversion): Promise<string> => {
    let resultMessage: string = '';
    const params: IDBAccessQueryParams = {
        conversion: conversion,
        caller: 'BootProcessor::checkConnection',
        sql: 'SELECT 1;',
        vendor: DBVendors.MYSQL,
        processExitOnError: false,
        shouldReturnClient: false
    };

    const mySqlResult: DBAccessQueryResult = await DBAccess.query(params);
    resultMessage += mySqlResult.error ? `\tMySQL connection error: ${ JSON.stringify(mySqlResult.error) }\n` : '';

    params.vendor = DBVendors.PG;
    const pgResult: DBAccessQueryResult = await DBAccess.query(params);
    resultMessage += pgResult.error ? `\tPostgreSQL connection error: ${ JSON.stringify(pgResult.error) }` : '';
    return resultMessage;
};

/**
 * Returns Nmig's logo.
 */
export const getLogo = (): string => {
    return '\n\t/\\_  |\\  /\\/\\ /\\___'
        + '\n\t|  \\ | |\\ | | | __'
        + '\n\t| |\\\\| || | | | \\_ \\'
        + '\n\t| | \\| || | | |__/ |'
        + '\n\t\\|   \\/ /_|/______/'
        + '\n\n\tNMIG - the database migration tool'
        + '\n\tCopyright (C) 2016 - present, Anatoly Khaytovich <anatolyuss@gmail.com>\n\n'
        + '\t--[boot] Configuration has been just loaded.';
};

/**
 * Boots the migration.
 */
export const boot = async (conversion: Conversion): Promise<Conversion> => {
    const connectionErrorMessage = await checkConnection(conversion);
    const logo: string = getLogo();
    const logTitle: string = 'BootProcessor::boot';

    if (connectionErrorMessage) {
        await generateError(conversion, `\t--[${ logTitle }]\n ${ logo } \n ${ connectionErrorMessage }`);
        process.exit(1);
    }

    const sql: string = `SELECT EXISTS(SELECT 1 FROM information_schema.tables`
        + ` WHERE table_schema = '${ conversion._schema }' AND table_name = '${ getStateLogsTableName(conversion, true) }');`;

    const params: IDBAccessQueryParams = {
        conversion: conversion,
        caller: 'BootProcessor::boot',
        sql: sql,
        vendor: DBVendors.PG,
        processExitOnError: true,
        shouldReturnClient: false
    };

    const result: DBAccessQueryResult = await DBAccess.query(params);
    const isExists: boolean = !!result.data.rows[0].exists;
    const message: string = `${ (isExists
        ? '\n\t--[boot] NMIG is restarting after some failure.\n\t--[boot] Consider checking log files at the end of migration.\n'
        : '\n\t--[boot] NMIG is starting.') } \n`;

    log(conversion, `\t--[${ logTitle }] ${ logo }${ message }`);
    return conversion;
};

/**
 * Parses CLI input arguments, if given.
 * Returns an object containing paths to configuration files and to logs directory.
 *
 * Sample:
 * npm start -- --conf-dir='C:\Users\anatolyuss\Documents\projects\nmig_config' --logs-dir='C:\Users\anatolyuss\Documents\projects\nmig_logs'
 * npm test -- --conf-dir='C:\Users\anatolyuss\Documents\projects\nmig_config' --logs-dir='C:\Users\anatolyuss\Documents\projects\nmig_logs'
 */
export const getConfAndLogsPaths = (): IConfAndLogsPaths => {
    const baseDir: string = path.join(__dirname, '..', '..');
    const _parseInputArguments = (paramName: string): string | undefined => {
        const _path: string | undefined = process.argv.find((arg: string) => arg.startsWith(paramName));
        return _path ? _path.split('=')[1] : undefined;
    };

    return {
        confPath: _parseInputArguments('--conf-dir') || path.join(baseDir, 'config'),
        logsPath: _parseInputArguments('--logs-dir') || baseDir
    };
};
