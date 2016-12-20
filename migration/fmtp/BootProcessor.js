/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright (C) 2016 - 2017 Anatoly Khaytovich <anatolyuss@gmail.com>
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

const connect = require('./Connector');

/**
 * Boot the migration.
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
module.exports = function(self) {
    return connect(self).then(() => {
        return new Promise(resolve => {
            self._pg.connect((error, client, done) => {
                if (error) {
                    console.log('\t--[boot] Cannot connect to PostgreSQL server...\n' + error);
                    done();
                    process.exit();
                } else {
                    const sql = 'SELECT EXISTS(SELECT 1 FROM information_schema.tables '
                        + 'WHERE table_schema = \'' + self._schema
                        + '\' AND table_name = \'state_logs_' + self._schema + self._mySqlDbName + '\');';

                    client.query(sql, (err, result) => {
                        done();

                        if (err) {
                            console.log('\t--[boot] Error when executed query:\n' + sql + '\nError message:\n' + err);
                            process.exit();
                        } else {
                            const isExists = !!result.rows[0].exists;
                            const logo     = '\n\t/\\_  |\\  /\\/\\ /\\___'
                                + '\n\t|  \\ | |\\ | | | __'
                                + '\n\t| |\\\\| || | | | \\_ \\'
                                + '\n\t| | \\| || | | |__/ |'
                                + '\n\t\\|   \\/ /_|/______/'
                                + '\n\n\tNMIG - the database migration tool'
                                + '\n\tCopyright (C) 2016 - 2017 Anatoly Khaytovich <anatolyuss@gmail.com>\n\n'
                                + '\t--[boot] Configuration has been just loaded.'
                                + (isExists
                                    ? '\n\t--[boot] NMIG is ready to restart after some failure.'
                                          + '\n\t--[boot] Consider checking log files at the end of migration.'
                                    : '\n\t--[boot] NMIG is ready to start.')
                                + '\n\t--[boot] Proceed? [Y/n]';

                            console.log(logo);
                            process
                                .stdin
                                .resume()
                                .setEncoding(self._encoding)
                                .on('data', stdin => {
                                    if (stdin.indexOf('n') !== -1) {
                                        console.log('\t--[boot] Migration aborted.\n');
                                        process.exit();
                                    }

                                    if (stdin.indexOf('Y') !== -1) {
                                        resolve();
                                    }
                                });
                        }
                    });
                }
            });
        });
    });
};
