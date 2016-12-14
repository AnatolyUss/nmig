/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright 2016 Anatoly Khaytovich <anatolyuss@gmail.com>
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

/**
 * Boot the migration.
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
module.exports = function(self) {
    return new Promise(resolve => {
        const logo = '\n\t/\\_  |\\  /\\/\\ /\\___'
            + '\n\t|  \\ | |\\ | | | __'
            + '\n\t| |\\\\| || | | | \\_ \\'
            + '\n\t| | \\| || | | |__/ |'
            + '\n\t\\|   \\/ /_|/______/'
            + '\n\n\tNMIG - the database migration tool'
            + '\n\tCopyright 2016 Anatoly Khaytovich <anatolyuss@gmail.com>\n\n'
            + '\t--[boot] The configuration has been just loaded.'
            + '\n\t--[boot] NMIG is ready to start.\n\t--[boot] Proceed? [Y/n]';

        console.log(logo);
        process
            .stdin
            .resume()
            .setEncoding(self._encoding)
            .on('data', stdin => {
                if (stdin.indexOf('Y') !== -1) {
                    resolve();
                } else {
                    console.log('\t--[boot] Migration aborted.\n');
                    process.exit();
                }
            });
    });
};
