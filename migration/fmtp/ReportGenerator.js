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
'use strict';

const log = require('./Logger');

/**
 * Generates a summary report.
 *
 * @param {Conversion} self
 * @param {String}     endMsg
 *
 * @returns {undefined}
 */
module.exports = (self, endMsg) => {
    let differenceSec = ((new Date()) - self._timeBegin) / 1000;
    let seconds       = Math.floor(differenceSec % 60);
    differenceSec     = differenceSec / 60;
    let minutes       = Math.floor(differenceSec % 60);
    let hours         = Math.floor(differenceSec / 60);
    hours             = hours < 10 ? '0' + hours : hours;
    minutes           = minutes < 10 ? '0' + minutes : minutes;
    seconds           = seconds < 10 ? '0' + seconds : seconds;
    const output      = '\t--[generateReport] ' + endMsg
        + '\n\t--[generateReport] Total time: ' + hours + ':' + minutes + ':' + seconds
        + '\n\t--[generateReport] (hours:minutes:seconds)';

    log(self, output);
    process.exit();
};
