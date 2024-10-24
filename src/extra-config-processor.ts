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
import Conversion from './conversion';

/**
 * Retrieves current table's name.
 */
export const getTableName = (
  conversion: Conversion,
  currentTableName: string,
  shouldGetOriginal: boolean,
): string => {
  if (conversion._extraConfig !== null && 'tables' in conversion._extraConfig) {
    for (let i = 0; i < conversion._extraConfig.tables.length; ++i) {
      const tableName: string = shouldGetOriginal
        ? conversion._extraConfig.tables[i].name.new
        : conversion._extraConfig.tables[i].name.original;

      if (tableName === currentTableName) {
        return shouldGetOriginal
          ? conversion._extraConfig.tables[i].name.original
          : conversion._extraConfig.tables[i].name.new;
      }
    }
  }

  return currentTableName;
};

/**
 * Retrieves current column's name.
 *
 * !!!Note, 're-namings' defined in extra-config under 'tables' -> 'columns'
 * will take precedence over 'lowerCaseAllColumnNames: true'.
 */
export const getColumnName = (
  conversion: Conversion,
  originalTableName: string,
  currentColumnName: string,
  shouldGetOriginal: boolean,
): string => {
  if (conversion._extraConfig) {
    if ('tables' in conversion._extraConfig) {
      for (let i = 0; i < conversion._extraConfig.tables.length; ++i) {
        const tableFound: boolean =
          conversion._extraConfig.tables[i].name.original === originalTableName &&
          'columns' in conversion._extraConfig.tables[i];

        if (tableFound) {
          for (
            let columnsCount = 0;
            columnsCount < conversion._extraConfig.tables[i].columns.length;
            ++columnsCount
          ) {
            const columnFound: boolean =
              conversion._extraConfig.tables[i].columns[columnsCount].original ===
              currentColumnName;

            if (columnFound) {
              return shouldGetOriginal
                ? conversion._extraConfig.tables[i].columns[columnsCount].original
                : conversion._extraConfig.tables[i].columns[columnsCount].new;
            }
          }
        }
      }
    }

    if (conversion._extraConfig.lowerCaseAllColumnNames) {
      return currentColumnName.toLowerCase();
    }
  }

  return currentColumnName;
};

/**
 * Parses the extra_config foreign_keys attributes and generate
 * an output array required by ForeignKeyProcessor::processForeignKeyWorker.
 */
export const parseForeignKeys = (conversion: Conversion, tableName: string): any[] => {
  const retVal: any[] = [];

  if (conversion._extraConfig !== null && 'foreign_keys' in conversion._extraConfig) {
    for (let i = 0; i < conversion._extraConfig.foreign_keys.length; ++i) {
      if (conversion._extraConfig.foreign_keys[i].table_name === tableName) {
        // There may be several FKs in a single table.
        const objFk: any = Object.create(null);

        for (const attribute of conversion._extraConfig.foreign_keys[i]) {
          objFk[attribute.toUpperCase()] = conversion._extraConfig.foreign_keys[i][attribute];
        }

        retVal.push(objFk);
      }
    }
  }

  return retVal;
};
