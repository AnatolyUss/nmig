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
import log from './Logger';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import DBVendors from './DBVendors';
import DBAccessQueryResult from './DBAccessQueryResult';
import * as extraConfigProcessor from './ExtraConfigProcessor';

/**
 * Runs "vacuum full" and "analyze".
 */
export default async function(conversion: Conversion): Promise<void> {
    const dbAccess: DBAccess = new DBAccess(conversion);

    const vacuumPromises: Promise<void>[] = conversion._tablesToMigrate.map(async (table: string) => {
        if (conversion._noVacuum.indexOf(extraConfigProcessor.getTableName(conversion, table, true)) === -1) {
            const msg: string = `\t--[runVacuumFullAndAnalyze] Running "VACUUM FULL and ANALYZE" query for table 
                "${ conversion._schema }"."${ table }"...`;

            log(conversion, msg);
            const sql: string = `VACUUM (FULL, ANALYZE) "${ conversion._schema }"."${ table }";`;
            const result: DBAccessQueryResult = await dbAccess.query('runVacuumFullAndAnalyze', sql, DBVendors.PG, false, false);

            if (!result.error) {
                const msgSuccess: string = `\t--[runVacuumFullAndAnalyze] Table "${ conversion._schema }"."${ table }" is VACUUMed...`;
                log(conversion, msgSuccess);
            }
        }
    });

    await Promise.all(vacuumPromises);
}
