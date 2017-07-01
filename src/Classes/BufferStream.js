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

const { Readable } = require('stream');

module.exports = class BufferStream extends Readable {
    /**
     * BufferStream constructor.
     *
     * @param {Buffer} source
     */
    constructor(source) {
        super();
        this._source = source;
        this._offset = 0;
        this.on('end', this.destroy.bind(this));
    }

    /**
     * BufferStream destructor.
     *
     * @returns {undefined}
     */
    destroy() {
        this._source = null;
        this._offset = null;
    }

    /**
     * Read chunks from the source buffer into the underlying stream buffer.
     *
     * @param {Number} size
     *
     * @returns {undefined}
     */
    read(size) {
        // Push the next chunk onto the internal stream buffer.
        if (this._offset < this._source.length) {
            this.push(this._source.slice(this._offset, this._offset + size));
            this._offset += size;
            return;
        }

        // Source buffer consumed entirely, readable stream must be closed.
        this.push(null);
    }
};
