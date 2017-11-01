package org.rocksdb.ycsb;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RocksDBYcsbBinding extends DB {
    private static final String DB_PATH = "/data1/rocksdb-ycsb";
    private static final int BYTE_BUFFER_SIZE = 4096;
	private static final int BATCH_SIZE = 32;
	private static final Object DB_LOCK = new Object();

    private static RocksDB db = null;
    private static Options options;
	private static ReadOptions read_options;
	private static WriteOptions write_options;
	private WriteBatch write_batch = null;
	private static Statistics stats;
	private static AtomicInteger count = new AtomicInteger(0);

	@Override
    public void init() throws DBException {
		System.out.println("Initializing RocksDB...");
		synchronized (DB_LOCK) {
			if (null == db)	{
				String db_path = DB_PATH;
				options = new Options();
				read_options = new ReadOptions();
				write_options = new WriteOptions();

				options.setCreateIfMissing(true)
						.createStatistics()
						.setWriteBufferSize(64 * SizeUnit.MB)
						.setMaxWriteBufferNumber(50)
						.setMaxBackgroundFlushes(4)
						.setMaxBackgroundCompactions(16)
						.setCompressionType(CompressionType.SNAPPY_COMPRESSION)
						.setCompactionStyle(CompactionStyle.LEVEL)
						.setUseDirectIoForFlushAndCompaction(false)
						.setUseDirectReads(false)
						.setMaxOpenFiles(500000)
						.setMaxBytesForLevelBase(256 * SizeUnit.MB)
						.setInplaceUpdateSupport(false)
						.setAllowConcurrentMemtableWrite(true);
				//		.setMemtablePrefixBloomSizeRatio(0.2)
				//		.setBloomLocality(10000)
				//		.useCappedPrefixExtractor(20);
				options.setMaxSubcompactions(4);
				stats = options.statisticsPtr();

				assert(options.createIfMissing() == true);
				assert(options.writeBufferSize() == 64 * SizeUnit.MB);
				assert(options.maxWriteBufferNumber() == 50);
				assert(options.maxBackgroundFlushes() == 4);
				assert(options.maxBackgroundCompactions() == 16);
				assert(options.compressionType() == CompressionType.SNAPPY_COMPRESSION);
				assert(options.compactionStyle() == CompactionStyle.LEVEL);
				assert(options.useDirectIoForFlushAndCompaction() == false);
				assert(options.useDirectReads() == false);
				assert(options.maxOpenFiles() == 500000);
				assert(options.maxSubcompactions() == 4);
				assert(options.maxBytesForLevelBase() == 256 * SizeUnit.MB);
				assert(options.inplaceUpdateSupport() == false);
				assert(options.allowConcurrentMemtableWrite() == true);
				//assert(options.memtablePrefixBloomSizeRatio() == 0.2);
				//assert(options.bloomLocality() == 10000);

				assert(options.memTableFactoryName().equals("SkipListFactory"));

/*        options.setMemTableConfig(
                new HashSkipListMemTableConfig()
                        .setHeight(4)
                        .setBranchingFactor(4)
                        .setBucketCount(2000000));
        assert(options.memTableFactoryName().equals("HashSkipListRepFactory"));

        options.setMemTableConfig(
                new HashLinkedListMemTableConfig()
                        .setBucketCount(100000));
        assert(options.memTableFactoryName().equals("HashLinkedListRepFactory"));

        options.setMemTableConfig(
                new VectorMemTableConfig().setReservedSize(10000));
        assert(options.memTableFactoryName().equals("VectorRepFactory"));

        options.setMemTableConfig(new SkipListMemTableConfig());
        assert(options.memTableFactoryName().equals("SkipListFactory"));
*/

//        options.setTableFormatConfig(new PlainTableConfig());
//        // Plain-Table requires mmap read
//        options.setAllowMmapReads(true);
//        assert(options.tableFactoryName().equals("PlainTable"));
//
//        options.setRateLimiterConfig(new GenericRateLimiterConfig(10000000,
//                10000, 10));
//        options.setRateLimiterConfig(new GenericRateLimiterConfig(10000000));
//
				Filter bloomFilter = new BloomFilter(10, true);
				BlockBasedTableConfig table_options = new BlockBasedTableConfig();
				table_options.setBlockCacheSize(50 * SizeUnit.GB)
						.setBlockSize(32 * SizeUnit.KB)
						.setFilter(bloomFilter)
						.setCacheNumShardBits(6)
						.setBlockSizeDeviation(5)
						.setBlockRestartInterval(16)
						.setHashIndexAllowCollision(false);
//						.setBlockCacheCompressedSize(64 * SizeUnit.KB)
//						.setBlockCacheCompressedNumShardBits(10)
//						.setCacheIndexAndFilterBlocks(true)
//						.setPinL0FilterAndIndexBlocksInCache(true);

				assert(table_options.blockCacheSize() == 50 * SizeUnit.GB);
				assert(table_options.blockSize() == 32 * SizeUnit.KB);
				assert(table_options.cacheNumShardBits() == 6);
				assert(table_options.blockSizeDeviation() == 5);
				assert(table_options.blockRestartInterval() == 16);
				//assert(table_options.cacheIndexAndFilterBlocks() == true);
				assert(table_options.hashIndexAllowCollision() == false);
//				assert(table_options.blockCacheCompressedSize() == 64 * SizeUnit.KB);
//				assert(table_options.blockCacheCompressedNumShardBits() == 10);
//				assert(table_options.cacheIndexAndFilterBlocks() == true);
//				assert(table_options.pinL0FilterAndIndexBlocksInCache() == true);

				options.setTableFormatConfig(table_options);
				assert(options.tableFactoryName().equals("BlockBasedTable"));

				read_options.setVerifyChecksums(true);

				write_options.setDisableWAL(false);

				try {
					db = RocksDB.open(options, db_path);
					db.put("hello".getBytes(), "world".getBytes());
					byte[] value = db.get("hello".getBytes());
					assert("world".equals(new String(value)));
					String str = db.getProperty("rocksdb.stats");
					assert(str != null && str != "");
				} catch (RocksDBException e) {
					System.out.format("[ERROR] caught the unexpceted exception -- %s\n", e);
					assert(db == null);
					assert(false);
				}

				System.out.println("Initializing RocksDB is over");
			}
		}
		if (write_batch == null) {
			write_batch = new WriteBatch();
		}
    }

    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        try {
            byte[] value = db.get(read_options, key.getBytes());
            HashMap<String, ByteIterator> deserialized = deserialize(value);
            result.putAll(deserialized);
        } catch (RocksDBException e) {
            System.out.format("[ERROR] caught the unexpceted exception -- %s\n", e);
            assert(false);
        }
        return Status.OK;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
			RocksIterator rocks_iter = db.newIterator(read_options);
			rocks_iter.seek(startkey.getBytes());
			if (rocks_iter.isValid()) {
				HashMap<String, ByteIterator> deserialized = deserialize(rocks_iter.value());
				result.addElement(deserialized);
			}
			int i = 1;
			for (rocks_iter.next(); rocks_iter.isValid() && i < recordcount; ++i) {
				HashMap<String, ByteIterator> deserialized = deserialize(rocks_iter.value());
				result.addElement(deserialized);
			}
        return Status.OK;
    }

    @Override
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            byte[] serialized = serialize(values);
			write_batch.put(key.getBytes(), serialized);
            if (write_batch.count() >= BATCH_SIZE) {
				db.write(write_options,	write_batch);
				write_batch.clear();
//				assert(write_batch.count() == 0);
			}
        } catch (RocksDBException e) {
            System.out.format("[ERROR] caught the unexpceted exception -- %s\n", e);
            assert(false);
        }
        return Status.OK;
    }

    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        return update(table, key, values);
    }

    @Override
    public Status delete(String table, String key) {
        try {
            db.delete(key.getBytes());
        } catch (RocksDBException e) {
            System.out.format("[ERROR] caught the unexpceted exception -- %s\n", e);
            assert(false);
        }
        return Status.OK;
    }

    @Override
    public void cleanup() throws DBException {
		try {
			if (write_batch.count() != 0) {
				db.write(write_options,	write_batch);
			}
		} catch (RocksDBException e) {
			throw new DBException("Error while trying to flush write_batch in cleanup");
		}
        super.cleanup();
        try {
            String str = db.getProperty("rocksdb.stats");
            System.out.println(str);
			if (count.getAndIncrement() == 0)
				stats_to_string();
        } catch (RocksDBException e) {
            throw new DBException("Error while trying to print RocksDB statistics");
        }

        System.out.println("Cleaning up RocksDB database...");
//        db.close();
//        options.dispose();
        // Why does it cause error? : "pointer being freed was not allocated"
    }

	private void stats_to_string() {
		System.out.println("=======================Ticker Count=================================");
		for (TickerType ticker : TickerType.values()) {
			if (ticker.toString() == "TICKER_ENUM_MAX")
				continue;
			System.out.println(ticker.toString() + ": " +
					stats.getTickerCount(ticker));
		}
		System.out.println("=======================Ticker Count end=================================");
	}

    private byte[] serialize(HashMap<String, ByteIterator> values) {
        ByteBuffer buf = ByteBuffer.allocate(BYTE_BUFFER_SIZE);
        // Number of elements in HashMap (int)
        buf.put((byte) values.size());
        for (HashMap.Entry<String, ByteIterator> entry : values.entrySet()) {
            // Key string length (int)
            buf.put((byte) entry.getKey().length());
            // Key bytes
            buf.put(entry.getKey().getBytes());
            // Value bytes length (long)
            buf.put((byte) entry.getValue().bytesLeft());
            // Value bytes
            buf.put((entry.getValue().toArray()));
        }

        byte[] result = new byte[buf.position()];
        buf.get(result, 0, buf.position());
        return result;
    }

    private HashMap<String, ByteIterator> deserialize(byte[] bytes) {
        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int count = buf.getInt();
        for (int i = 0; i < count; i++) {
            int keyLength = buf.getInt();
            byte[] keyBytes = new byte[keyLength];
            buf.get(keyBytes, buf.position(), keyLength);

            int valueLength = buf.getInt();
            byte[] valueBytes = new byte[valueLength];
            buf.get(valueBytes, buf.position(), valueLength);

            result.put(new String(keyBytes), new ByteArrayByteIterator(valueBytes));
        }
        return result;
    }
}
