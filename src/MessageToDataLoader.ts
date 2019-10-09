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
export default class MessageToDataLoader {
    /**
     * Parsed Nmig's configuration object.
     */
    public readonly config: any;

    /**
     * Data chunk.
     */
    public readonly chunk: any;

    /**
     * Representation of a message of the master process to DataLoader process.
     * Contains migration's configuration and a chunk of data.
     */
    public constructor(config: any, chunk: any) {
        this.config = config;
        this.chunk = chunk;
    }
}
