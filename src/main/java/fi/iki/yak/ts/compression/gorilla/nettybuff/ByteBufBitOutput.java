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

import fi.iki.yak.ts.compression.gorilla.BitOutput;
import io.netty.buffer.ByteBuf;

/**
 * <p>Title: ByteBufBitOutput</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>fi.iki.yak.ts.compression.gorilla.nettybuff.ByteBufBitOutput</code></p>
 */

public class ByteBufBitOutput implements BitOutput {
	/** The shared buffer manager */
	private static final BufferManager bufferManager = BufferManager.getInstance();
	
    /** The default allocation size */
    public static final int DEFAULT_ALLOCATION = 4096;

    private ByteBuf bb;
    private byte b;
    private int bitsLeft = Byte.SIZE;
    
    public void release() {
    	bb.release();
    }
    
    /**
     * Creates a new ByteBufferBitOutput with a default allocated size of 4096 bytes.
     */
    public ByteBufBitOutput() {
        this(DEFAULT_ALLOCATION);
    }

    /**
     * Give an initialSize different than DEFAULT_ALLOCATIONS. Recommended to use values which are dividable by 4096.
     *
     * @param initialSize New initialsize to use
     */
    public ByteBufBitOutput(int initialSize) {
        bb = bufferManager.buffer(initialSize);
        b = bb.getByte(bb.writerIndex());
    }


	/**
	 * {@inheritDoc}
	 * @see fi.iki.yak.ts.compression.gorilla.BitOutput#writeBit()
	 */
	@Override
	public void writeBit() {
        b |= (1 << (bitsLeft - 1));
        bitsLeft--;
        flipByte();
	}

	/**
	 * {@inheritDoc}
	 * @see fi.iki.yak.ts.compression.gorilla.BitOutput#skipBit()
	 */
	@Override
	public void skipBit() {
        bitsLeft--;
        flipByte();
	}

	/**
	 * {@inheritDoc}
	 * @see fi.iki.yak.ts.compression.gorilla.BitOutput#writeBits(long, int)
	 */
	@Override
	public void writeBits(final long value, int bits) {
        while(bits > 0) {
            int bitsToWrite = (bits > bitsLeft) ? bitsLeft : bits;
            if(bits > bitsLeft) {
                int shift = bits - bitsLeft;
                b |= (byte) ((value >> shift) & ((1 << bitsLeft) - 1));
            } else {
                int shift = bitsLeft - bits;
                b |= (byte) (value << shift);
            }
            bits -= bitsToWrite;
            bitsLeft -= bitsToWrite;
            flipByte();
        }
	}

	/**
	 * {@inheritDoc}
	 * @see fi.iki.yak.ts.compression.gorilla.BitOutput#flush()
	 */
	@Override
	public void flush() {
        bitsLeft = 0;
        flipByte(); // Causes write to the ByteBuf
	}
	
	/**
	 * Returns the inner ByteBuf
	 * @return the inner ByteBuf
	 */
	public ByteBuf getByteBuf() {
		return bb;
	}

    private void flipByte() {
        if(bitsLeft == 0) {
            bb.writeByte(b);
            if(!bb.isReadable()) {
            	throw new RuntimeException("OUT OF SPACE");
            }
            try {
            	b = bb.getByte(bb.readerIndex());
            } catch (Exception ex) {
            	throw new RuntimeException("OUT OF SPACE", ex);
            }
            bitsLeft = Byte.SIZE;
        }
    }
	
}
