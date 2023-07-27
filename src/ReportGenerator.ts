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
import { EventEmitter } from 'node:events';

import { log } from './FsOps';
import Conversion from './Conversion';

/**
 * Generates a summary report.
 */
export default async (conversion: Conversion): Promise<void> => {
    if (conversion._runsInTestMode) {
        (conversion._eventEmitter as EventEmitter).emit(conversion._migrationCompletedEvent);
        return;
    }

    let differenceSec = (new Date().getTime() - (conversion._timeBegin as Date).getTime()) / 1000;
    const seconds = Math.floor(differenceSec % 60);
    differenceSec = differenceSec / 60;
    const minutes = Math.floor(differenceSec % 60);
    const hours = Math.floor(differenceSec / 60);
    const formattedHours = hours < 10 ? `0${hours}` : `${hours}`;
    const formattedMinutes = minutes < 10 ? `0${minutes}` : `${minutes}`;
    const formattedSeconds = seconds < 10 ? `0${seconds}` : `${seconds}`;
    const endMsg = 'NMIG migration is accomplished.';
    const output = `\t--[generateReport] ${endMsg}
        \n\t--[generateReport] Total time: ${formattedHours}:${formattedMinutes}:${formattedSeconds}
        \n\t--[generateReport] (hours:minutes:seconds)`;

    await log(conversion, output, undefined, true);
    process.exit(0);
};
