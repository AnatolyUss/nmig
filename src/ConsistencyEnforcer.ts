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
import DBAccessQueryResult from './DBAccessQueryResult';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import DBVendors from './DBVendors';

/**
 * Enforces consistency before processing a chunk of data.
 * Ensures there are no any data duplications.
 * In case of normal execution - it is a good practice.
 * In case of rerunning Nmig after unexpected failure - it is absolutely mandatory.
 */
export async function dataTransferred(conversion: Conversion, dataPoolId: number): Promise<boolean> {
    const logTitle: string = 'ConsistencyEnforcer::dataTransferred';
    const dataPoolTable: string = `"${ conversion._schema }"."data_pool_${ conversion._schema }${ conversion._mySqlDbName }"`;
    const sqlGetMetadata: string = `SELECT metadata AS metadata FROM ${ dataPoolTable } WHERE id = ${ dataPoolId };`;
    const dbAccess: DBAccess = new DBAccess(conversion);

    const result: DBAccessQueryResult = await dbAccess.query(
        logTitle,
        sqlGetMetadata,
        DBVendors.PG,
        true,
        true
    );

    const metadata: any = JSON.parse(result.data.rows[0].metadata);
    const targetTableName: string = `"${ conversion._schema }"."${ metadata._tableName }"`;
    const sqlGetFirstRow: string = `SELECT * FROM ${ targetTableName } LIMIT 1 OFFSET 0;`;

    const probe: DBAccessQueryResult = await dbAccess.query(
        logTitle,
        sqlGetFirstRow,
        DBVendors.PG,
        true,
        false,
        result.client
    );

    return probe.data.rows.length !== 0;
}
