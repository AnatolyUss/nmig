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
import DBAccess from './DBAccess';
import DBAccessQueryResult from './DBAccessQueryResult';
import DBVendors from './DBVendors';

/**
 * Checks correctness of connection details of both MySQL and PostgreSQL.
 */
async function checkConnection(conversion: Conversion, dbAccess: DBAccess): Promise<string> {
    const logTitle: string = 'BootProcessor::checkConnection';
    let resultMessage: string = '';
    const sql: string = 'SELECT 1;';

    const mySqlResult: DBAccessQueryResult = await dbAccess.query(logTitle, sql, DBVendors.MYSQL, false, false);
    resultMessage += mySqlResult.error ? `\tMySQL connection error: ${ JSON.stringify(mySqlResult.error) }\n` : '';

    const pgResult: DBAccessQueryResult = await dbAccess.query(logTitle, sql, DBVendors.PG, false, false);
    resultMessage += pgResult.error ? `\tPostgreSQL connection error: ${ JSON.stringify(pgResult.error) }` : '';
    return resultMessage;
}

/**
 * Returns Nmig's logo.
 */
function getLogo(): string {
    return '\n\t/\\_  |\\  /\\/\\ /\\___'
        + '\n\t|  \\ | |\\ | | | __'
        + '\n\t| |\\\\| || | | | \\_ \\'
        + '\n\t| | \\| || | | |__/ |'
        + '\n\t\\|   \\/ /_|/______/'
        + '\n\n\tNMIG - the database migration tool'
        + '\n\tCopyright (C) 2016 - present, Anatoly Khaytovich <anatolyuss@gmail.com>\n\n'
        + '\t--[boot] Configuration has been just loaded.';
}

/**
 * Boots the migration.
 */
export default(conversion: Conversion): Promise<Conversion> => {
    return new Promise<Conversion>(async resolve => {
        const dbAccess: DBAccess = new DBAccess(conversion);
        const connectionErrorMessage = await checkConnection(conversion, dbAccess);
        const logo: string = getLogo();

        if (connectionErrorMessage) {
            console.log(`${ logo } \n ${ connectionErrorMessage }`);
            process.exit();
        }

        const sql: string = `SELECT EXISTS(SELECT 1 FROM information_schema.tables 
        WHERE table_schema = '${ conversion._schema }'
            AND table_name = 'state_logs_${ conversion._schema }${ conversion._mySqlDbName }');`;

        const result: DBAccessQueryResult = await dbAccess.query('Boot', sql, DBVendors.PG, true, false);
        const isExists: boolean = !!result.data.rows[0].exists;
        const message: string = `${ (isExists
            ? '\n\t--[boot] NMIG is ready to restart after some failure.\n\t--[boot] Consider checking log files at the end of migration.'
            : '\n\t--[boot] NMIG is ready to start.') } \n\t--[boot] Proceed? [Y/n]`;

        console.log(logo + message);

        process
            .stdin
            .resume()
            .setEncoding(conversion._encoding)
            .on('data', (stdin: string) => {
                if (stdin.indexOf('n') !== -1) {
                    console.log('\t--[boot] Migration aborted.\n');
                    process.exit();
                } else if (stdin.indexOf('Y') !== -1) {
                    return resolve(conversion);
                } else {
                    const hint: string = `\t--[boot] Unexpected input ${ stdin }\n
                    \t--[boot] Expected input is upper case Y\n
                    \t--[boot] or lower case n\n${ message }`;

                    console.log(hint);
                }
            });
    });
}
