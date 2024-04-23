/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.codecs.lucene90.DeflateWithPresetDictCompressionMode;
import org.apache.lucene.codecs.lucene90.LZ4WithPresetDictCompressionMode;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsReader;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Objects;

/**
 * Stored field format used by pluggable codec
 */
public class Lucene99CoreStoredFieldsFormat extends StoredFieldsFormat {

    /**
     * A key that we use to map to a mode
     */
    public static final String MODE_KEY = Lucene99CoreStoredFieldsFormat.class.getSimpleName() + ".mode";

    private final Lucene95Codec.Mode mode;
    private final Integer lz4BlockSize;
    private final Integer zlibBlockSize;
    private final Integer noopCompressionSize;
    private final boolean enableHybridCompression;

    /**
     * default constructor
     */
    public Lucene99CoreStoredFieldsFormat() {
        this(Lucene95Codec.Mode.BEST_SPEED, 8, 48, 100, true);
    }

    /**
     * Creates a new instance.
     *
     * @param mode The mode represents ZSTD or ZSTDNODICT
     */

    public Lucene99CoreStoredFieldsFormat(Lucene95Codec.Mode mode, Integer lz4Block, Integer zlibBlockSize, Integer noopCompressionSize, Boolean enableHybridCompression) {
        this.mode = Objects.requireNonNull(mode);
        this.lz4BlockSize = lz4Block;
        this.zlibBlockSize = zlibBlockSize;
        this.noopCompressionSize = noopCompressionSize;
        this.enableHybridCompression = enableHybridCompression;
    }

    /**
     * Returns a {@link StoredFieldsReader} to load stored fields.
     *
     * @param directory The index directory.
     * @param si        The SegmentInfo that stores segment information.
     * @param fn        The fieldInfos.
     * @param context   The IOContext that holds additional details on the merge/search context.
     */
    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
        if (si.getAttribute(MODE_KEY) != null) {
            String value = si.getAttribute(MODE_KEY);
            Lucene95Codec.Mode mode = Lucene95Codec.Mode.valueOf(value);
            return impl(mode, si, context).fieldsReader(directory, si, fn, context);
        } else {
            throw new IllegalStateException("missing value for " + MODE_KEY + " for segment: " + si.name);
        }

    }

    /**
     * Returns a {@link StoredFieldsReader} to write stored fields.
     *
     * @param directory The index directory.
     * @param si        The SegmentInfo that stores segment information.
     * @param context   The IOContext that holds additional details on the merge/search context.
     */

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
        String previous = si.putAttribute(MODE_KEY, mode.name());
        if (previous != null && previous.equals(mode.name()) == false) {
            throw new IllegalStateException(
                    "found existing value for " + MODE_KEY + " for segment: " + si.name + "old=" + previous + ", new=" + mode.name()
            );
        }
        return impl(mode, si, context).fieldsWriter(directory, si, context);
    }

    StoredFieldsFormat impl(Lucene95Codec.Mode mode, SegmentInfo si, IOContext context) {
        switch (mode) {
            case BEST_SPEED:
                return getLZ4CompressingStoredFieldsFormat(si, context);
            case BEST_COMPRESSION:
                return getZlibCompressingStoredFieldsFormat(si, context);
            default:
                throw new AssertionError();
        }
    }

    public Lucene95Codec.Mode getMode() {
        return mode;
    }

    // Shoot for 10 sub blocks of 48kB each.
    private static final int BEST_COMPRESSION_BLOCK_LENGTH = 10 * 1024 * 1024;

    /**
     * Compression mode for {@link Lucene90StoredFieldsFormat.Mode#BEST_COMPRESSION}
     */
    public static final CompressionMode BEST_COMPRESSION_MODE = new DeflateWithPresetDictCompressionMode();

    // Shoot for 10 sub blocks of 8kB each.
    private static final int BEST_SPEED_BLOCK_LENGTH = 10 * 1024 * 1024;

    /**
     * Compression mode for {@link Lucene90StoredFieldsFormat.Mode#BEST_SPEED}
     */
    public static final CompressionMode BEST_SPEED_MODE = new LZ4WithPresetDictCompressionMode();

    private StoredFieldsFormat getLZ4CompressingStoredFieldsFormat(SegmentInfo si, IOContext ioContext) {

        if (enableHybridCompression) {
            if ( ioContext.mergeInfo != null && ioContext.mergeInfo.estimatedMergeBytes > this.noopCompressionSize * 1024 * 1024) {
                return getLZ4Mode();
            }
            return getNoCompressionMode();
        }
        return getLZ4Mode();
    }

    public StoredFieldsFormat getNoCompressionMode(){
        return new Lucene90CompressingStoredFieldsFormat(
                "Lucene90StoredFieldsFastData",
                NO_COMPRESSION_MODE,
                BEST_SPEED_BLOCK_LENGTH * this.lz4BlockSize,
                1024,
                10
        );
    }

    public StoredFieldsFormat getLZ4Mode(){
        return new Lucene90CompressingStoredFieldsFormat(
                "Lucene90StoredFieldsFastData",
                BEST_SPEED_MODE,
                BEST_SPEED_BLOCK_LENGTH * this.lz4BlockSize,
                1024,
                10
        );
    }

    public StoredFieldsFormat getZlibMode(){
        return new Lucene90CompressingStoredFieldsFormat(
                "Lucene90StoredFieldsHighData",
                BEST_COMPRESSION_MODE,
                BEST_COMPRESSION_BLOCK_LENGTH * this.zlibBlockSize,
                4096,
                10
        );
    }


    private StoredFieldsFormat getZlibCompressingStoredFieldsFormat(SegmentInfo si, IOContext ioContext) {

        if (enableHybridCompression) {
            if (ioContext.mergeInfo != null && ioContext.mergeInfo.estimatedMergeBytes > this.noopCompressionSize * 1024 * 1024) {
                return getZlibMode();
            }
            return getNoCompressionMode();
        }

        return getZlibMode();

    }

    static final CompressionMode NO_COMPRESSION_MODE = new CompressionMode() {
        public Compressor newCompressor() {
            return new Compressor() {
                public void close() throws IOException {
                }

                public void compress(ByteBuffersDataInput buffersInput, DataOutput out) throws IOException {
                    out.copyBytes(buffersInput, buffersInput.size());
                }
            };
        }

        public Decompressor newDecompressor() {
            return new Decompressor() {
                public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
                    bytes.bytes = ArrayUtil.growNoCopy(bytes.bytes, length);
                    in.skipBytes(offset);
                    in.readBytes(bytes.bytes, 0, length);
                    bytes.offset = 0;
                    bytes.length = length;
                }

                public Decompressor clone() {
                    return this;
                }
            };
        }
    };

}
