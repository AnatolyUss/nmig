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
import { v4 as uuidV4 } from 'uuid';

import Conversion from './Conversion';
import { generateError } from './FsOps';

/**
 * PostgreSQL has a limit of 63 characters for identifier names.
 * This function substitutes a postfix of possibly long identifiers with UUID string to ensure identifier uniqueness.
 */
export const getUniqueIdentifier = (identifier: string, mandatoryPostfix: string = ''): string => {
    const MAX_PG_IDENTIFIER_LENGTH: number = 63;

    if (identifier.length > MAX_PG_IDENTIFIER_LENGTH) {
        const mandatoryPostfixLength: number = mandatoryPostfix.length;
        const uuidSliceStart: number = mandatoryPostfixLength === 0
            ? mandatoryPostfixLength
            : mandatoryPostfixLength - 1;

        const uuid: string = uuidV4().slice(uuidSliceStart) + mandatoryPostfix;
        return identifier.slice(0, (MAX_PG_IDENTIFIER_LENGTH - uuid.length)) + uuid;
    }

    return identifier;
};

/**
 * Kills a process specified by the pid.
 */
export const killProcess = (pid: number, conversion: Conversion): void => {
    try {
        process.kill(pid);
    } catch (killError) {
        generateError(conversion, `\t--[killProcess] pid: ${pid} killError: ${killError}`);
    }
};
