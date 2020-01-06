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
import { PoolClient } from 'pg';
import { PoolConnection } from 'mysql';

export default class DBAccessQueryResult {
    /**
     * MySQL's or PostgreSQL's client instance.
     * The client may be undefined.
     */
    public readonly client?: PoolConnection | PoolClient;

    /**
     * Query result.
     * The data may be undefined.
     */
    public readonly data?: any;

    /**
     * Query error.
     * The data may be undefined.
     */
    public readonly error?: any;

    /**
     * Constructor.
     */
    public constructor(client?: PoolConnection | PoolClient, data?: any, error?: any) {
        this.client = client;
        this.data = data;
        this.error = error;
    }
}
