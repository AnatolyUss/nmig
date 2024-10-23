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
import { logInBackground, generateErrorInBackground } from './FsOps';
import { LogMessage, LogMessageType } from './Types';

/**
 * Conversion instance, used in a context of logger process.
 */
let conv: Conversion;

/**
 * Process incoming logs in a context of logger process.
 */
process.on('message', async (logMessage: LogMessage): Promise<void> => {
  try {
    if (logMessage.type === LogMessageType.CONFIG) {
      // Create Conversion instance, but avoid recursion,
      // which might lead to redundant logger processes creation.
      const avoidLogger = true;
      conv = conv || new Conversion(logMessage.config, avoidLogger);
    } else if (logMessage.type === LogMessageType.LOG) {
      await logInBackground(conv, logMessage.message as string, logMessage.tableLogPath);
    } else if (logMessage.type === LogMessageType.ERROR) {
      await generateErrorInBackground(conv, logMessage.message as string, logMessage.sql);
    } else if (logMessage.type === LogMessageType.EXIT) {
      // Migration has been just finished.
      // All resources must be released.
      await logInBackground(conv, logMessage.message as string, logMessage.tableLogPath);
      process.exit(0);
    }
  } catch (error) {
    console.log(`\n\t--[LogsProcessor] Logger error: ${JSON.stringify(error)}\n`);
  }
});
