// This file is part of OpenTSDB.
// Copyright (C) 2010-2016  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package fi.iki.yak.ts.compression.gorilla.nettybuff;

import java.nio.ByteBuffer;

import com.heliosapm.utils.buffer.BufferManager;
import com.heliosapm.utils.io.NIOHelper;

import fi.iki.yak.ts.compression.gorilla.BitInput;
import io.netty.buffer.ByteBuf;

/**
 * <p>Title: ByteBufBitInput</p>
 * <p>Description: An implementation of BitInput that parses the data from a Netty ByteBuf.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>fi.iki.yak.ts.compression.gorilla.nettybuff.ByteBufBitInput</code></p>
 */

public class ByteBufBitInput implements BitInput {
	/** The shared buffer manager */
	private static final BufferManager bufferManager = BufferManager.getInstance();
	
	/** The buffer for this input */
	private final ByteBuf buffer;
    private byte b;
    private int bitsLeft = 0;
	
    public void release() {
    	buffer.release();
    }

	/**
	 * Creates a new ByteBufBitInput using the default allocation size
	 */
	public ByteBufBitInput() {
		this(bufferManager.buffer());
	}
	
	/**
	 * Creates a new ByteBufBitInput with a ByteBuf of the specified size in bytes
	 * @param initialSize The initial size in bytes
	 */
	public ByteBufBitInput(final int initialSize) {
		this(bufferManager.buffer(validate(initialSize)));
	}
	
	
	/**
	 * Creates a new ByteBufBitInput from the passed byte array
	 * @param bytes The byte array
	 */
	public ByteBufBitInput(final byte[] bytes) {
		this(bufferManager.wrap(validate(bytes)));
	}
	
	/**
	 * Creates a new ByteBufBitInput using the bytes in the passed ByteBuffer.
	 * The ByteBuffer is then deallocated.
	 * @param bb The ByteBuffer to initialize with
	 */
	public ByteBufBitInput(final ByteBuffer bb) {
		this(bufferManager.wrap(validate(bb)));
		NIOHelper.clean(bb);
	}
	
	/**
	 * Creates a new ByteBufBitInput from an existing ByteBuf
	 * @param buffer the existing ByteBuf
	 */
	public ByteBufBitInput(final ByteBuf buffer) {
		if(buffer==null) throw new IllegalArgumentException("The passed buffer was null");
		if(buffer.isReadOnly()) throw new IllegalArgumentException("The passed buffer was read only");
		this.buffer = buffer;
		flipByte();
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see fi.iki.yak.ts.compression.gorilla.BitInput#readBit()
	 */
	@Override
	public boolean readBit() {
        boolean bit = ((b >> (bitsLeft - 1)) & 1) == 1;
        bitsLeft--;
        flipByte();
        return bit;
	}

	/**
	 * {@inheritDoc}
	 * @see fi.iki.yak.ts.compression.gorilla.BitInput#getLong(int)
	 */
	@Override
	public long getLong(int bits) {
        long value = 0;
        while(bits > 0) {
            if(bits > bitsLeft || bits == Byte.SIZE) {
                // Take only the bitsLeft "least significant" bits
                byte d = (byte) (b & ((1<<bitsLeft) - 1));
                value = (value << bitsLeft) + (d & 0xFF);
                bits -= bitsLeft;
                bitsLeft = 0;
            } else {
                // Shift to correct position and take only least significant bits
                byte d = (byte) ((b >>> (bitsLeft - bits)) & ((1<<bits) - 1));
                value = (value << bits) + (d & 0xFF);
                bitsLeft -= bits;
                bits = 0;
            }
            flipByte();
        }
        return value;
	}

	/**
	 * {@inheritDoc}
	 * @see fi.iki.yak.ts.compression.gorilla.BitInput#nextClearBit(int)
	 */
	@Override
	public int nextClearBit(final int maxBits) {
        int val = 0x00;

        for(int i = 0; i < maxBits; i++) {
            val <<= 1;
            boolean bit = readBit();

            if(bit) {
                val |= 0x01;
            } else {
                break;
            }
        }
        return val;
	}
	
    private void flipByte() {
        if (bitsLeft == 0) {
            b = buffer.readByte();
            bitsLeft = Byte.SIZE;
        }
    }
	
	private static int validate(final int i) {
		if(i < 0) throw new IllegalArgumentException("Invalid initial size [" + i + "]");
		return i;
	}

	private static byte[] validate(final byte[] bytes) {
		if(bytes==null) throw new IllegalArgumentException("The passed byte array was null");
		return bytes;
	}

	private static ByteBuffer validate(final ByteBuffer bb) {
		if(bb==null) throw new IllegalArgumentException("The passed ByteBuffer was null");
		return bb;
	}

}
