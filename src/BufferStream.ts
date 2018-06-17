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
import { Readable } from 'stream';

export class BufferStream extends Readable {
    /**
     * The Buffer, that contains the data to load.
     */
    private _source?: Buffer;

    /**
     * Indicator of the offset, from which the data should be read into underlying stream buffer.
     */
    private _offset?: number;

    /**
     * BufferStream constructor.
     */
    public constructor(source: Buffer) {
        super();
        this._source = source;
        this._offset = 0;

        // When source buffer consumed entirely, the 'end' event is emitted.
        this.on('end', this._destruct.bind(this));
    }

    /**
     * BufferStream destructor.
     */
    private _destruct(): void {
        this._source = undefined;
        this._offset = undefined;
    }

    /**
     * Reads chunks from the source buffer into the underlying stream buffer.
     */
    public _read(size: number): void {
        // Push the next chunk onto the internal stream buffer.
        if ((<number>this._offset) < (<Buffer>this._source).length) {
            this.push((<Buffer>this._source).slice((<number>this._offset), (<number>this._offset) + size));
            (<number>this._offset) += size;
            return;
        }

        // When the source ends, the EOF - signaling `null` chunk should be pushed.
        this.push(null);
    }
}
