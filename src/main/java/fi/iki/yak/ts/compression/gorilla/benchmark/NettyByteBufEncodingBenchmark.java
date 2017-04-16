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
package fi.iki.yak.ts.compression.gorilla.benchmark;

import fi.iki.yak.ts.compression.gorilla.Compressor;
import fi.iki.yak.ts.compression.gorilla.Decompressor;
import fi.iki.yak.ts.compression.gorilla.GorillaCompressor;
import fi.iki.yak.ts.compression.gorilla.GorillaDecompressor;
import fi.iki.yak.ts.compression.gorilla.LongArrayInput;
import fi.iki.yak.ts.compression.gorilla.LongArrayOutput;
import fi.iki.yak.ts.compression.gorilla.Pair;
import fi.iki.yak.ts.compression.gorilla.benchmark.EncodingBenchmark.DataGenerator;
import fi.iki.yak.ts.compression.gorilla.nettybuff.*;
import io.netty.buffer.ByteBuf;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import com.heliosapm.utils.buffer.BufferManager;
import com.heliosapm.utils.jmx.JMXHelper;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>Title: NettyByteBufEncodingBenchmark</p>
 * <p>Description: Benchmark for Netty ByteBuf compression</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>fi.iki.yak.ts.compression.gorilla.benchmark.NettyByteBufEncodingBenchmark</code></p>
 */
@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10) // Reduce the amount of iterations if you start to see GC interference
public class NettyByteBufEncodingBenchmark {
	
	static {
		JMXHelper.fireUpJMXMPServer(1934);
	}
	
	/** The shared buffer manager */
	private static final BufferManager bufferManager = BufferManager.getInstance();

    @State(Scope.Benchmark)
    public static class DataGenerator {
        public List<Pair> insertList;

        @Param({"100000"})
        public int amountOfPoints;

        public long blockStart;

        public long[] uncompressedTimestamps;
        public long[] uncompressedValues;
        public double[] uncompressedDoubles;
        public long[] compressedArray;

        public ByteBuf uncompressedBuffer;
        public ByteBuf compressedBuffer;

        public List<Pair> pairs;
        
        @TearDown
        public void tearDown() {
        	if(uncompressedBuffer!=null) uncompressedBuffer.release();
        	if(compressedBuffer!=null) compressedBuffer.release();
        }

        @Setup(Level.Trial)
        public void setup() {
            blockStart = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)
                    .toInstant(ZoneOffset.UTC).toEpochMilli();

            long now = blockStart + 60;
            uncompressedTimestamps = new long[amountOfPoints];
            uncompressedDoubles = new double[amountOfPoints];
            uncompressedValues = new long[amountOfPoints];

            insertList = new ArrayList<>(amountOfPoints);

            ByteBuf bb = bufferManager.buffer(amountOfPoints * 2*Long.BYTES);

            pairs = new ArrayList<>(amountOfPoints);

            for(int i = 0; i < amountOfPoints; i++) {
                now += 60;
                bb.writeLong(now);
                bb.writeDouble(i);
                uncompressedTimestamps[i] = now;
                uncompressedDoubles[i] = i;
                uncompressedValues[i] = i;
                pairs.add(new Pair(now, i));
//                bb.putLong(i);
            }

//            if (bb.hasArray()) {
//                uncompressedBuffer = bb.duplicate();
//                uncompressedBuffer.flip();
//            }
            ByteBufBitOutput output = new ByteBufBitOutput();
            LongArrayOutput arrayOutput = new LongArrayOutput(amountOfPoints);

            Compressor c = new Compressor(blockStart, output);
            GorillaCompressor gc = new GorillaCompressor(blockStart, arrayOutput);
            if(compressedBuffer!=null) compressedBuffer.release();
//            bb.flip();

            for(int j = 0; j < amountOfPoints; j++) {
//                c.addValue(bb.getLong(), bb.getLong());
                c.addValue(bb.readLong(), bb.readDouble());
                gc.addValue(uncompressedTimestamps[j], uncompressedDoubles[j]);
            }

            gc.close();
            c.close();

            ByteBuf byteBuf = output.getByteBuf();
            if(compressedBuffer!=null) compressedBuffer.release();
            compressedBuffer = byteBuf;

            compressedArray = arrayOutput.getLongArray();
        }
    }

//    @Benchmark
    @OperationsPerInvocation(100000)
    public void encodingBenchmark(DataGenerator dg) {
        ByteBufBitOutput output = new ByteBufBitOutput();
        Compressor c = new Compressor(dg.blockStart, output);

        for(int j = 0; j < dg.amountOfPoints; j++) {
            c.addValue(dg.uncompressedBuffer.readLong(), dg.uncompressedBuffer.readDouble());
        }
        c.close();
        dg.uncompressedBuffer.clear();
        output.release();
    }

    @Benchmark
    @OperationsPerInvocation(100000)
    public void decodingBenchmark(DataGenerator dg, Blackhole bh) throws Exception {
        ByteBuf duplicate = dg.compressedBuffer.copy();
        ByteBufBitInput input = new ByteBufBitInput(duplicate);
        Decompressor d = new Decompressor(input);
        Pair pair;
        while((pair = d.readPair()) != null) {
            bh.consume(pair);
        }
        input.release();
    }

    @Benchmark
    @OperationsPerInvocation(100000)
    public void encodingGorillaBenchmark(DataGenerator dg) {
        LongArrayOutput output = new LongArrayOutput();
        GorillaCompressor c = new GorillaCompressor(dg.blockStart, output);

        for(int j = 0; j < dg.amountOfPoints; j++) {
            c.addValue(dg.uncompressedTimestamps[j], dg.uncompressedDoubles[j]);
        }
        c.close();
    }

    @Benchmark
    @OperationsPerInvocation(100000)
    public void encodingGorillaBenchmarkLong(DataGenerator dg) {
        LongArrayOutput output = new LongArrayOutput();
        GorillaCompressor c = new GorillaCompressor(dg.blockStart, output);

        for(int j = 0; j < dg.amountOfPoints; j++) {
            c.addValue(dg.uncompressedTimestamps[j], dg.uncompressedValues[j]);
        }
        c.close();
    }

//    @Benchmark
//    @OperationsPerInvocation(100000)
//    public void encodingGorillaStreamBenchmark(DataGenerator dg) {
//        LongArrayOutput output = new LongArrayOutput();
//        GorillaCompressor c = new GorillaCompressor(dg.blockStart, output);
//
//        c.compressLongStream(dg.pairs.stream());
//        c.close();
//    }

    @Benchmark
    @OperationsPerInvocation(100000)
    public void decodingGorillaBenchmark(DataGenerator dg, Blackhole bh) throws Exception {
        LongArrayInput input = new LongArrayInput(dg.compressedArray);
        GorillaDecompressor d = new GorillaDecompressor(input);
        Pair pair;
        while((pair = d.readPair()) != null) {
            bh.consume(pair);
        }
    }

}
