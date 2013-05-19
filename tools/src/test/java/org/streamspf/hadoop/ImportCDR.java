package org.streamspf.hadoop;

import org.streamspf.hadoop.hbase.DefaultHBaseDao;
import org.streamspf.hadoop.hbase.HBaseDao;
import org.streamspf.hadoop.hbase.exceptions.HBaseDaoException;
import org.streamspf.hadoop.hbase.pool.HTablePoolManager;
import java.io.File;
import java.util.ArrayList;

public class ImportCDR {

	private static volatile int threadNum = 0;
	private static volatile int targetNum = 0;

	public static void main(String[] args)
			throws HBaseDaoException {
		System.out.println("Usage: importcdr [quorum port batchNum threadNum]");

		String quorum = args.length > 0 ? args[0] : "127.0.0.1";
		String port = args.length > 1 ? args[1] : "2181";
		int batchNum = args.length > 2 ? Integer.valueOf(args[2]) : 1000;
		threadNum = args.length > 3 ? Integer.valueOf(args[3]) : 10;

		HTablePoolManager.getHTablePool("default", quorum, port);

		RecordsGenerator recordsGenerator = new RecordsGenerator();

		CallRecordDetail randomRecord = recordsGenerator.randomRecord(null);

		HBaseDao hdao = new DefaultHBaseDao("MYHBASE");
		ArrayList records = new ArrayList(batchNum);

		CdrBatch cdrBatch = new CdrBatch();
		long startTime = System.currentTimeMillis();
		long c = 0L;
		File stopFile = new File("stop");

		for (long k = 0L; k < Long.MAX_VALUE; k += 1L) {
			if (c == 0L) {
				cdrBatch.setTimestamp(randomRecord.getTimestamp());
				cdrBatch.setStart(randomRecord.getDesc());
				hdao.put(cdrBatch);
				System.out.print("Timestamp:" + randomRecord.getTimestamp() + ", Start:" + randomRecord.getDesc());
			}

			randomRecord = recordsGenerator.randomRecord(randomRecord);
			records.add(randomRecord);
			c += 1L;
			if (c == batchNum) {
				c = 0L;
				hdao.put(records);
				records.clear();

				long endTime = System.currentTimeMillis();
				System.out.println(", " + batchNum + " Reords, End:" + randomRecord.getDesc() + ", Cost:" + (endTime - startTime));
				cdrBatch.setEnd(randomRecord.getDesc());
				hdao.put(cdrBatch);
				startTime = System.currentTimeMillis();

				if (stopFile.exists()) {
					stopFile.deleteOnExit();
					break;
				}
			}
		}

		System.out.println("Exited! ");
	}
}
