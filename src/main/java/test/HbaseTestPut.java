package test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import normal.dao.HbaseConnectionGenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.TableNameOrBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.hbase.util.Pair;

public class HbaseTestPut {

	public static HConnection getConnection() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		ExecutorService pool = Executors.newFixedThreadPool(10);
		HConnection connection = HConnectionManager
				.createConnection(conf, pool);
		HTableInterface table = connection.getTable("testtable");

		Put put1 = new Put(Bytes.toBytes("0010"));
		put1.add(Bytes.toBytes("cf"), Bytes.toBytes("q2"), 3,
				Bytes.toBytes("我爱你"));

		Put put3 = new Put(Bytes.toBytes("0011"));
		put3.add(Bytes.toBytes("cf"), Bytes.toBytes("q2"), 3,
				Bytes.toBytes("是不是真的"));

		Put put2 = new Put(Bytes.toBytes("0019"));
		put2.add(Bytes.toBytes("cf"), Bytes.toBytes("q2"), 3,
				Bytes.toBytes("真的"));
		put2.add(Bytes.toBytes("cf"), Bytes.toBytes("q2"),
				Bytes.toBytes("真的是真的"));

		List<Put> lstPut = new ArrayList<Put>();

		lstPut.add(put1);
		lstPut.add(put3);
		lstPut.add(put2);
		try {
			table.put(lstPut);

		} catch (Exception e) {
			HTable table1 = (HTable) table;
			System.out.println(table1.getWriteBuffer());
			// table.flushCommits();
		}
		System.out.println(lstPut.size());
		table.close();
		// Put put4 = new Put(Bytes.toBytes("3008"));
		// put4.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"),
		// System.currentTimeMillis(), Bytes.toBytes("v21222222"));

		// HTableInterface table2=connection.getTable("testtable");
		// table2.put(put4);
		// System.out.println("------------------");
		// HTable table1=(HTable)table;
		// System.out.println(table1.getWriteBuffer());
		// HTable table3=(HTable)table2;
		// System.out.println(table3.getWriteBuffer());
		// // table.batchCallback(lstPut, callback)
		//
		return connection;
	}

	public static void test1() throws Exception {

		// HTable hTable = new HTable(HBaseConfiguration.create(), "test1");
		Put put = new Put(Bytes.toBytes("test01"));
		put.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"),
				System.currentTimeMillis(), Bytes.toBytes("v1"));
		TimeUnit.SECONDS.sleep(2);
		put.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"),
				System.currentTimeMillis(), Bytes.toBytes("v2"));
		put.add(Bytes.toBytes("cf"), Bytes.toBytes("q2"), Bytes.toBytes("v3"));

		List<Cell> list = put.get(Bytes.toBytes("cf"), Bytes.toBytes("q1"));
		// NavigableMap<byte[], List<KeyValue>> familyMap = put.getFamilyMap();

		NavigableMap<byte[], List<Cell>> list2 = put.getFamilyCellMap();
		System.out.println(list);
		for (Cell cell : list) {
			System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
			System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
			System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
			System.out.println(cell.getTimestamp());
			// cell.getTagsLength()

		}
		System.out.println("------------------------");
		System.out.println(list2);
		System.out.println(Bytes.toString(list2.keySet().iterator().next()));
		System.out.println(put.has(Bytes.toBytes("cf"), Bytes.toBytes("q1")));

		System.out.println(put.getRow());
		System.out.println(put.getId());
		// System.out.println(put.getWriteToWAL());
		System.out.println(put.getDurability());
		System.out.println(put.getTimeStamp());
		System.out.println(put.heapSize());
		System.out.println(put.isEmpty());
		System.out.println(put.numFamilies());
		System.out.println(put.size());

	}

	public static void test2() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, "testtable");
		table.setAutoFlush(false, false);
		// table.setAutoFlushTo(autoFlush);
		// System.out.println(table.isAutoFlush());
		// table.flushCommits();

		// table.setWriteBufferSize(11111);
		// table.getWriteBufferSize();
		// table.getWriteBuffer();//不建议再进行读取了

		Put put1 = new Put(Bytes.toBytes("2000"));
		put1.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"),
				Bytes.toBytes("v19999"));

		Put put2 = new Put(Bytes.toBytes("2001"));
		put2.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("v2"));

		Put put3 = new Put(Bytes.toBytes("2002"));
		// put3.add(Bytes.toBytes("cfbug"), Bytes.toBytes("q1"),
		// Bytes.toBytes("v2"));

		List<Put> lst = new ArrayList<Put>();
		lst.add(put1);
		lst.add(put3);
		lst.add(put2);

		try {
			table.put(lst);
			table.flushCommits();
		} catch (Exception e) {
			System.out.println(table.getWriteBuffer());// 该数据结构已经不再建议进行获取和修改了
			table.flushCommits();
			System.out.println(table.getWriteBuffer());
		}

	}

	public static void testBatch() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HConnection connection = HConnectionManager.createConnection(conf);
		HTableInterface table = connection.getTable(Bytes.toBytes("testtable"));

		List<Row> batch = new ArrayList<Row>();

		byte[] ROW1 = Bytes.toBytes("10000");
		byte[] ROW2 = Bytes.toBytes("10001");
		byte[] COLFAM1 = Bytes.toBytes("cf");
		byte[] QUAL1 = Bytes.toBytes("company");

		Put put = new Put(ROW1);
		put.add(COLFAM1, QUAL1, 66, Bytes.toBytes("val8888"));
		batch.add(put);
		Get get1 = new Get(ROW1);
		get1.addColumn(COLFAM1, QUAL1);
		batch.add(get1);
		// Put put1 = new Put(ROW2);
		// batch.add(put1);

		Put put2 = new Put(ROW2);
		put2.add(COLFAM1, QUAL1, 4, Bytes.toBytes("val66666"));
		batch.add(put2);

		Get get2 = new Get(ROW2);
		get2.addFamily(Bytes.toBytes("BOGUS"));
		batch.add(get2);
		// Delete delete = new Delete(ROW1);
		// delete.deleteColumn(COLFAM1, QUAL1);
		// batch.add(delete);

		Object[] results = new Object[batch.size()];

		try {
			table.batchCallback(batch, results, new Batch.Callback<Object>() {
				@Override
				public void update(byte[] region, byte[] row, Object result) {
					try {
						System.out.println("Received callback for row["
								+ Bytes.toString(row) + "] -> " + result);
					} catch (Exception e) {
						System.err.println(1111111111);
					}
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(table.isAutoFlush());
		}

	}

	public static void test3() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("testtable"));
		Get get = new Get(Bytes.toBytes("3000"));
		get.setMaxVersions();
		System.out.println(get.getTimeRange());
		get.setCacheBlocks(false);
		System.out.println(get.getCacheBlocks());
		Result result = table.get(get);
		System.out.println(result.size());// 包含多少个cell
		System.out.println(result.isEmpty());
		System.out.println(result.rawCells());
		System.out.println(result.listCells());
		System.out.println(result.getColumnCells(Bytes.toBytes("cf"),
				Bytes.toBytes("q1")));
		System.out.println(result.getMap());
		table.close();
	}

	public static void test4() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("testtable"));
		Get get1 = new Get(Bytes.toBytes("3000"));
		get1.setMaxVersions();
		get1.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q1"));

		Get get2 = new Get(Bytes.toBytes("3001"));
		get2.setMaxVersions();
		// get2.addColumn(Bytes.toBytes("cfBug"), Bytes.toBytes("company"));
		Get get3 = new Get(Bytes.toBytes("3008"));

		List<Get> getLst = new ArrayList<Get>();
		getLst.add(get1);
		getLst.add(get2);
		getLst.add(get3);

		try {
			Result[] results = table.get(getLst);

			for (Result result : results) {
				System.out.println(result.getValue(Bytes.toBytes("cf"),
						Bytes.toBytes("q1")));
				System.out.println(result.getColumnCells(Bytes.toBytes("cf"),
						Bytes.toBytes("q1")));
			}
		} catch (Exception e) {
			System.out.println(getLst.size());
		} finally {
			table.close();
		}

	}

	public static void test5() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("testtable"));

		Delete del1 = new Delete(Bytes.toBytes("3000"));
		del1.deleteColumn(Bytes.toBytes("cf"), Bytes.toBytes("q1"));

		Delete del2 = new Delete(Bytes.toBytes("3001"));
		del2.deleteColumns(Bytes.toBytes("cfBug"), Bytes.toBytes("q1"));

		Delete del3 = new Delete(Bytes.toBytes("3008"));

		List<Delete> dLst = new ArrayList<Delete>();

		dLst.add(del1);
		dLst.add(del2);
		dLst.add(del3);

		try {
			table.delete(dLst);
		} catch (Exception e) {

		} finally {
			System.out.println(dLst.size());
			System.out.println(dLst);
		}

	}

	public static void test6() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("testtable"));

		Put put1 = new Put(Bytes.toBytes("3006"));
		put1.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"),
				System.currentTimeMillis(), Bytes.toBytes("v55555555"));

		Put put2 = new Put(Bytes.toBytes("3007"));
		put2.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"),
				System.currentTimeMillis(), Bytes.toBytes("v55555555"));

		Get get = new Get(Bytes.toBytes("3001"));
		// get.addColumn(Bytes.toBytes("BUG"), null);
		Delete delete = new Delete(Bytes.toBytes("3000"));

		List<Row> bath = new ArrayList<Row>();

		bath.add(put1);
		bath.add(put2);
		bath.add(get);
		bath.add(delete);

		Object[] results = new Object[bath.size()];

		try {
			table.batch(bath, results);
			// table.batch(bath);//过期了，不建议在使用的，因为报错的时候结果数组里面没有任何的返回信息
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			for (Object obj : results) {
				System.out.println(obj);
			}
			table.close();
		}

	}

	public static void test7() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("testtable"));
		Scan scan = new Scan();
		// scan.setBatch(1);
		System.out.println(scan.getBatch());
		scan.setCaching(6);
		ResultScanner scanner = table.getScanner(scan);

		for (Result result : scanner) {
			System.out.println(result.listCells().size());
		}
		System.out.println(table.getConfiguration());
		System.out.println(table.getTableDescriptor());
		// HTable.isTableEnabled("testtable");
		byte[][] startKeys = table.getStartKeys();
		byte[][] endKeys = table.getEndKeys();
		// Pair<byte[][], byte[][]> startEndKeys = table.getStartEndKeys();
		for (byte[] b : startKeys) {
			System.out.println(Bytes.toStringBinary(b));
		}
		System.out.println(table.getRegionLocation("10000"));
		// HTable.setRegionCachePrefetch(tableName, enable);
		table.close();

	}

	public static void test8() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HBaseAdmin hadmin = new HBaseAdmin(conf);
		System.out.println(hadmin.isTableEnabled("testtable"));
		System.out.println(hadmin.getTableRegions(Bytes.toBytes("testtable")));
		hadmin.close();
	}

	public static void test9() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("testtable"));
		Scan scan = new Scan();
		// scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q3"));

		RowFilter filter1 = new RowFilter(
				CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(
						Bytes.toBytes("3001")));
		RowFilter fileter2 = new RowFilter(CompareFilter.CompareOp.EQUAL,
				new RegexStringComparator("3.*"));
		RowFilter filter3 = new RowFilter(CompareFilter.CompareOp.EQUAL,
				new SubstringComparator("01"));

		FamilyFilter filter4 = new FamilyFilter(
				CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(
						Bytes.toBytes("cf")));
		QualifierFilter filter5 = new QualifierFilter(
				CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(
						Bytes.toBytes("q2")));
		ValueFilter filter6 = new ValueFilter(CompareFilter.CompareOp.EQUAL,
				new SubstringComparator("55"));
		scan.setFilter(filter6);
		ResultScanner scanner = table.getScanner(scan);
		for (Result res : scanner) {
			System.out.println(res);
			for (Cell cell : res.listCells()) {
				System.out.println("value_: "
						+ Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		scanner.close();

		// Get get1 = new Get(Bytes.toBytes("2001"));
		// get1.setFilter(filter4);
		// Result result = table.get(get1);
		// System.out.println("================================");
		// System.out.println(result);

		table.close();
	}

	private static void filter(boolean drop, CompareFilter.CompareOp operator,
			ByteArrayComparable comparator) throws Exception {
		Filter filter;
		if (comparator != null) {
			filter = new DependentColumnFilter(Bytes.toBytes("cf"),
					Bytes.toBytes("q1"), drop, operator, comparator);
		} else {
			filter = new DependentColumnFilter(Bytes.toBytes("cf"),
					Bytes.toBytes("q1"), drop);
		}
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("testtable"));
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);

		for (Result res : scanner) {
			for (Cell cell : res.listCells()) {
				System.out.println("Cell:" + cell + ",Value:"
						+ Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}

		scanner.close();
		System.out.println("---------------------");
		Get get = new Get(Bytes.toBytes("8003"));
		get.setFilter(filter);
		Result result = table.get(get);
		for (Cell cell : result.listCells()) {
			System.out.println("Cell:" + cell + ",Value:"
					+ Bytes.toString(CellUtil.cloneValue(cell)));
		}

		table.close();
	}

	public static void test10() throws Exception {
		// filter(true, CompareFilter.CompareOp.NO_OP, null);
		// filter(false,CompareFilter.CompareOp.NO_OP, null);
		filter(false, CompareFilter.CompareOp.EQUAL,
				new BinaryPrefixComparator(Bytes.toBytes("v55")));

	}

	public static void test11() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("testtable"));

		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				Bytes.toBytes("cf"), Bytes.toBytes("q2"), CompareOp.EQUAL,
				new SubstringComparator("test"));

		filter.setFilterIfMissing(true);
		filter.setLatestVersionOnly(false);
		// filter.setReversed(true);//在这里没什么用

		Scan scan = new Scan();
		scan.setFilter(filter);
		// scan.setTimeStamp(2);

		ResultScanner scanner = table.getScanner(scan);
		for (Result res : scanner) {
			for (Cell cell : res.listCells()) {
				System.out.println("Cell:" + cell + ",Value:"
						+ Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}

		scanner.close();
		table.close();

	}

	public static void test12() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("testtable"));
		PrefixFilter filter = new PrefixFilter(Bytes.toBytes("30"));
		Scan scan = new Scan();
		scan.setFilter(filter);

		ResultScanner scanner = table.getScanner(scan);

		for (Result res : scanner) {
			System.out.println("result:" + res);
			for (Cell cell : res.listCells()) {
				System.out.println("---------------------");
				System.out.println("Value:"
						+ Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}

		scanner.close();
		table.close();

	}

	static final byte[] POSTFIX = new byte[] { 0x00 };

	public static void test13() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("testtable"));
		PageFilter pageFilter = new PageFilter(3);

		int totalRow = 0;
		byte[] lastRow = null;

		while (true) {
			Scan scan = new Scan();
			scan.setFilter(pageFilter);
			if (lastRow != null) {
				byte[] startRow = Bytes.add(lastRow, POSTFIX);
				System.out.println("start row: "
						+ Bytes.toStringBinary(startRow));
				scan.setStartRow(startRow);
			}

			ResultScanner scanner = table.getScanner(scan);

			int localRows = 0;

			Result result;
			while ((result = scanner.next()) != null) {
				System.out.println(localRows++ + ":" + result);
				totalRow++;
				lastRow = result.getRow();
			}
			scanner.close();
			if (localRows == 0)
				break;
		}

		System.out.println("total rows: " + totalRow);

		table.close();
	}

	public static void test14() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("testtable"));
		// PrefixFilter filter = new PrefixFilter(Bytes.toBytes("30"));
		KeyOnlyFilter filter = new KeyOnlyFilter(true);
		FirstKeyOnlyFilter filter2 = new FirstKeyOnlyFilter();
		Scan scan = new Scan();
		scan.setFilter(filter2);

		ResultScanner scanner = table.getScanner(scan);

		for (Result res : scanner) {
			System.out.println("result:" + res);
			for (Cell cell : res.listCells()) {
				System.out.println("---------------------");
				System.out.println("Value:"
						+ Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}

		scanner.close();
		table.close();
	}

	public static void test15() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("testtable"));

		InclusiveStopFilter filter = new InclusiveStopFilter(
				Bytes.toBytes("0008"));

		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes("0002"));
		scan.setMaxVersions();
		// scan.setStopRow(Bytes.toBytes("0008"));
		scan.setFilter(filter);

		ResultScanner scanner = table.getScanner(scan);

		for (Result res : scanner) {
			System.out.println("result:" + res);
			for (Cell cell : res.listCells()) {
				System.out.println("---------------------");
				System.out.println("Value:"
						+ Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}

		scanner.close();
		table.close();
	}

	public static void test16() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("testtable"));

		List<Long> timeLst = new ArrayList<Long>();

		// timeLst.add(3L);
		timeLst.add(1485164610318L);

		TimestampsFilter filter = new TimestampsFilter(timeLst);

		Scan scan = new Scan();
		// scan.setMaxVersions();
		scan.setFilter(filter);

		ResultScanner scanner = table.getScanner(scan);

		for (Result res : scanner) {
			System.out.println("result:" + res);
			for (Cell cell : res.listCells()) {
				System.out.println("---------------------");
				System.out.println("Value:"
						+ Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}

		scanner.close();
		table.close();
	}

	public static void test17() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("testtable"));

		RandomRowFilter filter = new RandomRowFilter(0.1F);

		Scan scan = new Scan();
		// scan.setMaxVersions();
		scan.setFilter(filter);

		ResultScanner scanner = table.getScanner(scan);

		for (Result res : scanner) {
			System.out.println("result:" + res);
			for (Cell cell : res.listCells()) {
				System.out.println("---------------------");
				System.out.println("Value:"
						+ Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}

		scanner.close();
		table.close();
	}

	public static void test18() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("testtable"));

		// ColumnRangeFilter filter = new ColumnRangeFilter(Bytes.toBytes("q2"),
		// true, Bytes.toBytes("q3"), true);
		Scan scan = new Scan();
		// scan.setMaxVersions();
		// scan.setFilter(filter);
		scan.setStartRow(Bytes.toBytes("20_20170216170802"));
		scan.setStopRow(Bytes.add(Bytes.toBytes("20_20170216173221"),
				new byte[] { 0x00 }));
		scan.addColumn(Bytes.toBytes("cf"),
				Bytes.toBytes("0e98c284cd164f22b3cd27fbb778621f_value"));

		ResultScanner scanner = table.getScanner(scan);

		for (Result res : scanner) {
			System.out.println("result:" + res);
			for (Cell cell : res.listCells()) {
				System.out.println("---------------------");
				System.out.println("Value:"
						+ Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}

		scanner.close();
		table.close();
	}

	public static void test19() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("testtable"));

		List<Pair<byte[], byte[]>> lst = new ArrayList<Pair<byte[], byte[]>>();
		Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>(
				Bytes.toBytes("?001"), new byte[] { 0x01, 0x00, 0x00, 0x00 });
		System.out.println(pair.getFirst().length + "++++++-----"
				+ pair.getSecond().length);
		lst.add(pair);

		FuzzyRowFilter filter = new FuzzyRowFilter(lst);

		Scan scan = new Scan();
		// scan.setMaxVersions();
		scan.setFilter(filter);

		ResultScanner scanner = table.getScanner(scan);

		for (Result res : scanner) {
			// System.out.println("result:" + res);
			for (Cell cell : res.listCells()) {
				System.out.println("---------------------");
				System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
			}
		}
		scanner.close();
		table.close();
	}

	public static void test20() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("testtable"));

		List<RowRange> list = new ArrayList<RowRange>();

		list.add(new RowRange(Bytes.toBytes("0002"), true, Bytes
				.toBytes("0009"), true));
		list.add(new RowRange(Bytes.toBytes("4000"), true, Bytes
				.toBytes("6002"), true));

		MultiRowRangeFilter filter = new MultiRowRangeFilter(list);

		Scan scan = new Scan();
		// scan.setMaxVersions();
		scan.setFilter(filter);

		ResultScanner scanner = table.getScanner(scan);

		for (Result res : scanner) {
			// System.out.println("result:" + res);
			for (Cell cell : res.listCells()) {
				System.out.println("---------------------");
				System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
			}
		}

		scanner.close();
		table.close();
	}

	public static void test21() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("counters"));

		long cnt1 = table.incrementColumnValue(Bytes.toBytes("20170206"),
				Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1);

		long cnt2 = table.incrementColumnValue(Bytes.toBytes("20170206"),
				Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1);

		long current = table.incrementColumnValue(Bytes.toBytes("20170206"),
				Bytes.toBytes("daily"), Bytes.toBytes("hits"), 0);

		long cnt3 = table.incrementColumnValue(Bytes.toBytes("20170206"),
				Bytes.toBytes("daily"), Bytes.toBytes("hits"), -1);

		System.out.println(cnt1);
		System.out.println(cnt2);
		System.out.println(current);
		System.out.println(cnt3);

		table.close();
	}

	public static void test22() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HTable table = new HTable(conf, Bytes.toBytes("counters"));

		Increment increment = new Increment(Bytes.toBytes("20170206"));

		increment.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("clicks"), 1);
		increment.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1);
		increment.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("clicks"),
				10);
		increment.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("hits"), 10);

		Result result = table.increment(increment);

		for (Cell cell : result.listCells()) {
			System.out.println("CELL:" + cell + " VALUE:"
					+ Bytes.toLong(CellUtil.cloneValue(cell)));
		}

		table.close();
	}

	public static void test23() throws Exception {
		HColumnDescriptor d1 = new HColumnDescriptor("test");
		d1.setTimeToLive(55);// 设置TTL
		d1.setBlocksize(12);// hfile的大小？待确认
		d1.setInMemory(true);
		d1.setCompactionCompressionType(Compression.Algorithm.GZ);
		d1.setBloomFilterType(BloomType.ROW);
		d1.setScope(1);
	}

	public static void test24() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		HConnection connection = HbaseConnectionGenerator.getConnection();
		HBaseAdmin admin = new HBaseAdmin(connection);

		System.out.println(admin.isMasterRunning());
		System.out.println(admin.getConnection());
		System.out.println(admin.getConfiguration());

		admin.close();

		HBaseAdmin admin1 = new HBaseAdmin(connection);

		System.out.println(admin1.isMasterRunning());
		System.out.println(admin1.getConnection());
		System.out.println(admin1.getConfiguration());

		admin1.close();
		HTableInterface table = connection.getTable("testtable");
		Scan scan = new Scan();
		ResultScanner result = table.getScanner(scan);
		for (Result r : result) {
			for (Cell cell : r.listCells()) {
				System.out.println(cell);
			}
		}
		table.close();

		HTableInterface table1 = connection.getTable("testtable");
		Scan scan1 = new Scan();
		ResultScanner result1 = table.getScanner(scan);
		for (Result r : result1) {
			for (Cell cell : r.listCells()) {
				System.out.println(cell);
			}
		}
		table1.close();

		HConnection connection1 = HbaseConnectionGenerator.getConnection();

	}

	public static void test25() throws Exception {
		HConnection connection = HbaseConnectionGenerator.getConnection();
		HBaseAdmin admin = new HBaseAdmin(connection);

		HTableDescriptor desc = new HTableDescriptor(
				TableName.valueOf("testcreate"));

		HColumnDescriptor coldef = new HColumnDescriptor(
				Bytes.toBytes("colfam1"));

		desc.addFamily(coldef);

		admin.createTable(desc);

		System.out.println(admin.isTableAvailable(Bytes.toBytes("testcreate")));

		admin.close();

	}

	private static void printTableRegions(String tableName) throws Exception {
		System.out.println("Printing regions of table: " + tableName);
		HConnection connection = HbaseConnectionGenerator.getConnection();
		HTableInterface table = connection.getTable(Bytes.toBytes(tableName));
		Pair<byte[][], byte[][]> pair = ((HTable) table).getStartEndKeys();
		for (int n = 0; n < pair.getFirst().length; n++) {
			byte[] sk = pair.getFirst()[n];
			byte[] ek = pair.getSecond()[n];

			System.out.println("["
					+ (n + 1)
					+ "]"
					+ " start key: "
					+ (sk.length == 8 ? Bytes.toLong(sk) : Bytes
							.toStringBinary(sk))
					+ " ,end key: "
					+ (ek.length == 8 ? Bytes.toLong(ek) : Bytes
							.toStringBinary(ek)));
		}
	}

	public static void test26() throws Exception {
		// HConnection connection = HbaseConnectionGenerator.getConnection();
		// HBaseAdmin admin = new HBaseAdmin(connection);
		//
		// HTableDescriptor desc = new
		// HTableDescriptor(TableName.valueOf("testtable2"));
		// HColumnDescriptor coldef = new
		// HColumnDescriptor(Bytes.toBytes("colfam1"));
		// desc.addFamily(coldef);
		//
		// admin.createTable(desc, Bytes.toBytes(1L), Bytes.toBytes(100L), 10);
		printTableRegions("testtable1");

		// byte[][] regions=new byte[][]{
		// Bytes.toBytes("A"),
		// Bytes.toBytes("D"),
		// Bytes.toBytes("G"),
		// Bytes.toBytes("K"),
		// Bytes.toBytes("O"),
		// Bytes.toBytes("T")
		// };
		//
		// admin.createTableAsync(desc, regions);
		printTableRegions("testtable2");

		// admin.close();

	}

	public static void test27() throws Exception {
		HConnection connection = HbaseConnectionGenerator.getConnection();
		HBaseAdmin admin = new HBaseAdmin(connection);
		System.out.println(admin.tableExists(Bytes.toBytes("testtable")));

		HTableDescriptor[] htds = admin.listTables();

		for (HTableDescriptor htd : htds) {
			System.out.println(htd);
		}

		HTableDescriptor htd1 = admin.getTableDescriptor(TableName
				.valueOf("testtable1"));
		System.out.println(htd1);

		HTableDescriptor htd2 = admin.getTableDescriptor(TableName
				.valueOf("testtable10"));
		System.out.println(htd2);

		admin.close();
	}

	public static void test28() throws Exception {
		HConnection connection = HbaseConnectionGenerator.getConnection();
		HBaseAdmin admin = new HBaseAdmin(connection);

		byte[] name = Bytes.toBytes("testtable3");

		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(name));
		HColumnDescriptor coldef1 = new HColumnDescriptor(
				Bytes.toBytes("colfam1"));
		desc.addFamily(coldef1);

		admin.createTable(desc);

		HTableDescriptor htd1 = admin.getTableDescriptor(TableName
				.valueOf(name));
		HColumnDescriptor coldef2 = new HColumnDescriptor(
				Bytes.toBytes("colfam2"));
		htd1.addFamily(coldef2);
		htd1.setMaxFileSize(1024 * 1024 * 1024L);

		admin.disableTable(TableName.valueOf(name));
		admin.modifyTable(TableName.valueOf(name), htd1);
		admin.enableTable(TableName.valueOf(name));

		HTableDescriptor htd2 = admin.getTableDescriptor(TableName
				.valueOf(name));
		System.out.println("Equals: " + htd1.equals(htd2));
		System.out.println("New schema: " + htd2);

		admin.close();
	}

	public static void generateOutletTable() throws Exception {
		HConnection connection = HbaseConnectionGenerator.getConnection();
		HBaseAdmin admin = new HBaseAdmin(connection);

		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(Bytes
				.toBytes("verify_info")));

		HColumnDescriptor coldef1 = new HColumnDescriptor(Bytes.toBytes("cf"));
		coldef1.setMaxVersions(1);
		coldef1.setInMemory(true);

		htd.addFamily(coldef1);

		admin.createTable(htd);

		admin.close();

	}

	public static void putOutletInfo() throws Exception {
		HConnection connection = HbaseConnectionGenerator.getConnection();
		HTableInterface table = connection.getTable(Bytes
				.toBytes("outlet_info"));
		Put put1 = new Put(
				Bytes.toBytes("1000100100090005_10010001000100021003_64444a6e76ed4cb18f383ad958024409_99010004_77667004acb54db9aade465ccf3479b9"));
		put1.add(Bytes.toBytes("base_info"), Bytes.toBytes("category"),
				Bytes.toBytes("1"));
		put1.add(Bytes.toBytes("base_info"), Bytes.toBytes("name"),
				Bytes.toBytes("繁峙的排口"));
		put1.add(Bytes.toBytes("base_info"), Bytes.toBytes("longitude"),
				Bytes.toBytes(113.117595));
		put1.add(Bytes.toBytes("base_info"), Bytes.toBytes("latitude"),
				Bytes.toBytes(36.229825));
		put1.add(Bytes.toBytes("base_info"), Bytes.toBytes("running_state"),
				Bytes.toBytes("1"));
		put1.add(Bytes.toBytes("base_info"), Bytes.toBytes("emission_access"),
				Bytes.toBytes("1"));
		put1.add(Bytes.toBytes("base_info"), Bytes.toBytes("is_delete"),
				Bytes.toBytes("0"));
		put1.add(Bytes.toBytes("base_info"), Bytes.toBytes("type"),
				Bytes.toBytes("10040001"));
		put1.add(Bytes.toBytes("base_info"), Bytes.toBytes("enter_name"),
				Bytes.toBytes("大铁棍子医院"));
		put1.add(Bytes.toBytes("base_info"), Bytes.toBytes("eara_name"),
				Bytes.toBytes("山西省忻州市繁峙县"));
		put1.add(Bytes.toBytes("base_info"), Bytes.toBytes("mn_code"),
				Bytes.toBytes("99010004"));

		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("e6500d8760ca43a894b77de2226bd192_parentName"),
				Bytes.toBytes("PH"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("e6500d8760ca43a894b77de2226bd192_status"),
				Bytes.toBytes("4"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("e6500d8760ca43a894b77de2226bd192_name"),
				Bytes.toBytes("PH"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("e6500d8760ca43a894b77de2226bd192_type"),
				Bytes.toBytes("10040002"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("e6500d8760ca43a894b77de2226bd192_isPollutant"),
				Bytes.toBytes("1"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("e6500d8760ca43a894b77de2226bd192_realTimeCode"),
				Bytes.toBytes("001-Rtd"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("e6500d8760ca43a894b77de2226bd192_realTimeUnit"),
				Bytes.toBytes("mg/m3"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("e6500d8760ca43a894b77de2226bd192_outlierMax"),
				Bytes.toBytes(0.6));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("e6500d8760ca43a894b77de2226bd192_outlierMin"),
				Bytes.toBytes(0));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("e6500d8760ca43a894b77de2226bd192_max"),
				Bytes.toBytes(0.5));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("e6500d8760ca43a894b77de2226bd192_min"),
				Bytes.toBytes(0));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("e6500d8760ca43a894b77de2226bd192_maxOpt"),
				Bytes.toBytes(">="));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("e6500d8760ca43a894b77de2226bd192_minOpt"),
				Bytes.toBytes("<="));

		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee0146315ecc8018b6_parentName"),
				Bytes.toBytes("污水流量"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee0146315ecc8018b6_status"),
				Bytes.toBytes("0"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee0146315ecc8018b6_name"),
				Bytes.toBytes("污水流量"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee0146315ecc8018b6_type"),
				Bytes.toBytes("10040002"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee0146315ecc8018b6_isPollutant"),
				Bytes.toBytes("1"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee0146315ecc8018b6_realTimeCode"),
				Bytes.toBytes("B01-Rtd"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee0146315ecc8018b6_outlierMax"),
				Bytes.toBytes(0.3));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee0146315ecc8018b6_outlierMin"),
				Bytes.toBytes(0));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee0146315ecc8018b6_max"),
				Bytes.toBytes(0.2));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee0146315ecc8018b6_min"),
				Bytes.toBytes(0));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee0146315ecc8018b6_maxOpt"),
				Bytes.toBytes(">="));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee0146315ecc8018b6_minOpt"),
				Bytes.toBytes("<="));

		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee01463163ee411b6d_parentName"),
				Bytes.toBytes("COD"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee01463163ee411b6d_status"),
				Bytes.toBytes("0"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee01463163ee411b6d_name"),
				Bytes.toBytes("化学需氧量(COD cr )"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee01463163ee411b6d_type"),
				Bytes.toBytes("10040002"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee01463163ee411b6d_isPollutant"),
				Bytes.toBytes("1"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee01463163ee411b6d_realTimeCode"),
				Bytes.toBytes("011-Rtd"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee01463163ee411b6d_outlierMax"),
				Bytes.toBytes(0.9));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee01463163ee411b6d_outlierMin"),
				Bytes.toBytes(0));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee01463163ee411b6d_max"),
				Bytes.toBytes(0.2));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee01463163ee411b6d_min"),
				Bytes.toBytes(0));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee01463163ee411b6d_maxOpt"),
				Bytes.toBytes(">="));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("40288120463145ee01463163ee411b6d_minOpt"),
				Bytes.toBytes("<="));

		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("0e98c284cd164f22b3cd27fbb778621f_parentName"),
				Bytes.toBytes("氨氮"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("0e98c284cd164f22b3cd27fbb778621f_status"),
				Bytes.toBytes("0"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("0e98c284cd164f22b3cd27fbb778621f_name"),
				Bytes.toBytes("氨氮"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("0e98c284cd164f22b3cd27fbb778621f_type"),
				Bytes.toBytes("10040002"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("0e98c284cd164f22b3cd27fbb778621f_isPollutant"),
				Bytes.toBytes("1"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("0e98c284cd164f22b3cd27fbb778621f_realTimeCode"),
				Bytes.toBytes("060-Rtd"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("0e98c284cd164f22b3cd27fbb778621f_realTimeUnit"),
				Bytes.toBytes("mg/m3|kg"));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("0e98c284cd164f22b3cd27fbb778621f_outlierMax"),
				Bytes.toBytes(0.9));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("0e98c284cd164f22b3cd27fbb778621f_outlierMin"),
				Bytes.toBytes(0));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("0e98c284cd164f22b3cd27fbb778621f_max"),
				Bytes.toBytes(0.2));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("0e98c284cd164f22b3cd27fbb778621f_min"),
				Bytes.toBytes(0));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("0e98c284cd164f22b3cd27fbb778621f_maxOpt"),
				Bytes.toBytes(">="));
		put1.add(Bytes.toBytes("monitor_info"),
				Bytes.toBytes("0e98c284cd164f22b3cd27fbb778621f_minOpt"),
				Bytes.toBytes("<="));

		table.put(put1);
		table.close();

	}

	public static void test29() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
		// HBaseAdmin.checkHBaseAvailable(conf);
		// HBaseAdmin admin = new HBaseAdmin(conf);
		HTable hTable = new HTable(conf, Bytes.toBytes("testtable"));
		HRegionLocation regionLocation = hTable.getRegionLocation(Bytes
				.toBytes("2001"));
		HRegionInfo regionInfo = regionLocation.getRegionInfo();
		System.out.println(regionInfo.getEncodedName());
		System.out.println(regionInfo.getShortNameToLog());
		System.out.println(regionInfo.getRegionId());
		System.out.println(regionInfo.getRegionNameAsString());
		System.out.println(regionInfo.getTable());
		System.out.println("----------------------------");
		System.out.println(regionLocation.getHostname());
		System.out.println(regionLocation.getHostnamePort());
		System.out.println(regionLocation.getPort());
		System.out.println(regionLocation.getSeqNum());
		System.out.println(regionLocation.getServerName());
		// System.out.println(admin.getClusterStatus());

	}

	public static void testInsert() throws Exception {
		Class.forName("com.mysql.jdbc.Driver");
		String url = "jdbc:mysql://192.168.1.200:3306/zaixianv4.2";
		Connection conn = DriverManager.getConnection(url, "root", "root1234");
		Statement stmt = conn.createStatement();
		String sql = "select * from v_effective_collection_factor_info";
		ResultSet rs = stmt.executeQuery(sql);
		List<Put> putLst = new ArrayList<Put>();
		while (rs.next()) {
			String pkId = rs.getString(1);
			String pkName=rs.getString(2);
			String pkType=rs.getString(3);
			String company_name=rs.getString(4);
			String company_id=rs.getString(5);
			String area_code=rs.getString(6);
			String MN = rs.getString(7);
			String monitorFactorId = rs.getString(9);
			String statuss = rs.getString(11);
			String realtimeCod = rs.getString(12);
			String realtimeUnit = rs.getString(13);
			String minuteCode = rs.getString(14);
			String minuteUnit = rs.getString(15);
			String hourCode = rs.getString(16);
			String hourUnit = rs.getString(17);
			String dayCode = rs.getString(18);
			String dayUnit = rs.getString(19);
			String outlierMax = rs.getString(20);
			String outlierMin = rs.getString(21);
			String max = rs.getString(22);
			String min = rs.getString(23);
			String isUp=rs.getString(27);

			System.out.println(pkId);
			System.out.println(MN);
			System.out.println(monitorFactorId);
			System.out.println(realtimeCod);
			System.out.println(realtimeUnit);
			System.out.println(minuteCode);
			System.out.println(minuteUnit);
			System.out.println(hourCode);
			System.out.println(hourUnit);
			System.out.println(dayCode);
			System.out.println(dayUnit);
			System.out.println(outlierMax);
			System.out.println(outlierMin);
			System.out.println(max);
			System.out.println(min);

			System.out.println("-----------");

			Put put1 = new Put(Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes
					.toBytes(MN + "_R"))));
			Put put2 = new Put(Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes
					.toBytes(MN + "_M"))));
			Put put3 = new Put(Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes
					.toBytes(MN + "_H"))));
			Put put4 = new Put(Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes
					.toBytes(MN + "_D"))));

			if (null != outlierMax && !outlierMax.equals("")) {
				put1.add(Bytes.toBytes("cf"),
						Bytes.toBytes(realtimeCod + "_outmax"),
						Bytes.toBytes(outlierMax));
				put2.add(Bytes.toBytes("cf"),
						Bytes.toBytes(minuteCode + "_outmax"),
						Bytes.toBytes(outlierMax));
				put3.add(Bytes.toBytes("cf"),
						Bytes.toBytes(hourCode + "_outmax"),
						Bytes.toBytes(outlierMax));
				put4.add(Bytes.toBytes("cf"),
						Bytes.toBytes(dayCode + "_outmax"),
						Bytes.toBytes(outlierMax));
			}

			if (null != outlierMin && !outlierMin.equals("")) {
				put1.add(Bytes.toBytes("cf"),
						Bytes.toBytes(realtimeCod + "_outmin"),
						Bytes.toBytes(outlierMin));
				put2.add(Bytes.toBytes("cf"),
						Bytes.toBytes(minuteCode + "_outmin"),
						Bytes.toBytes(outlierMin));
				put3.add(Bytes.toBytes("cf"),
						Bytes.toBytes(hourCode + "_outmin"),
						Bytes.toBytes(outlierMin));
				put4.add(Bytes.toBytes("cf"),
						Bytes.toBytes(dayCode + "_outmin"),
						Bytes.toBytes(outlierMin));
			}

			if (null != max && !max.equals("")) {
				put1.add(Bytes.toBytes("cf"),
						Bytes.toBytes(realtimeCod + "_max"), Bytes.toBytes(max));
				put2.add(Bytes.toBytes("cf"),
						Bytes.toBytes(minuteCode + "_max"), Bytes.toBytes(max));
				put3.add(Bytes.toBytes("cf"), Bytes.toBytes(hourCode + "_max"),
						Bytes.toBytes(max));
				put4.add(Bytes.toBytes("cf"), Bytes.toBytes(dayCode + "_max"),
						Bytes.toBytes(max));
			}

			if (null != min && !min.equals("")) {
				put1.add(Bytes.toBytes("cf"),
						Bytes.toBytes(realtimeCod + "_min"), Bytes.toBytes(min));
				put2.add(Bytes.toBytes("cf"),
						Bytes.toBytes(minuteCode + "_min"), Bytes.toBytes(min));
				put3.add(Bytes.toBytes("cf"), Bytes.toBytes(hourCode + "_min"),
						Bytes.toBytes(min));
				put4.add(Bytes.toBytes("cf"), Bytes.toBytes(dayCode + "_min"),
						Bytes.toBytes(min));
			}

			put1.add(Bytes.toBytes("cf"), Bytes.toBytes("outlet_id"),
					Bytes.toBytes(pkId));
			put2.add(Bytes.toBytes("cf"), Bytes.toBytes("outlet_id"),
					Bytes.toBytes(pkId));
			put3.add(Bytes.toBytes("cf"), Bytes.toBytes("outlet_id"),
					Bytes.toBytes(pkId));
			put4.add(Bytes.toBytes("cf"), Bytes.toBytes("outlet_id"),
					Bytes.toBytes(pkId));
			
			/*排口名称*/
			if(null!=pkName){
				put1.add(Bytes.toBytes("cf"), Bytes.toBytes("outlet_name"),
						Bytes.toBytes(pkName));
				put2.add(Bytes.toBytes("cf"), Bytes.toBytes("outlet_name"),
						Bytes.toBytes(pkName));
				put3.add(Bytes.toBytes("cf"), Bytes.toBytes("outlet_name"),
						Bytes.toBytes(pkName));
				put4.add(Bytes.toBytes("cf"), Bytes.toBytes("outlet_name"),
						Bytes.toBytes(pkName));
				
			}
		
			/*排口类型*/
			if(null!=pkType){
				put1.add(Bytes.toBytes("cf"), Bytes.toBytes("outlet_type"),
						Bytes.toBytes(pkType));
				put2.add(Bytes.toBytes("cf"), Bytes.toBytes("outlet_type"),
						Bytes.toBytes(pkType));
				put3.add(Bytes.toBytes("cf"), Bytes.toBytes("outlet_type"),
						Bytes.toBytes(pkType));
				put4.add(Bytes.toBytes("cf"), Bytes.toBytes("outlet_type"),
						Bytes.toBytes(pkType));
			}
			
			/*企业名称*/
			if(null!=company_name){
				put1.add(Bytes.toBytes("cf"), Bytes.toBytes("company_name"),
						Bytes.toBytes(company_name));
				put2.add(Bytes.toBytes("cf"), Bytes.toBytes("company_name"),
						Bytes.toBytes(company_name));
				put3.add(Bytes.toBytes("cf"), Bytes.toBytes("company_name"),
						Bytes.toBytes(company_name));
				put4.add(Bytes.toBytes("cf"), Bytes.toBytes("company_name"),
						Bytes.toBytes(company_name));
			}
			/*企业ID*/
			if(null!=company_id){
				put1.add(Bytes.toBytes("cf"), Bytes.toBytes("company_id"),
						Bytes.toBytes(company_id));
				put2.add(Bytes.toBytes("cf"), Bytes.toBytes("company_id"),
						Bytes.toBytes(company_id));
				put3.add(Bytes.toBytes("cf"), Bytes.toBytes("company_id"),
						Bytes.toBytes(company_id));
				put4.add(Bytes.toBytes("cf"), Bytes.toBytes("company_id"),
						Bytes.toBytes(company_id));
			}
			/*区域code*/
			if(null!=area_code){
				put1.add(Bytes.toBytes("cf"), Bytes.toBytes("area_code"),
						Bytes.toBytes(area_code));
				put2.add(Bytes.toBytes("cf"), Bytes.toBytes("area_code"),
						Bytes.toBytes(area_code));
				put3.add(Bytes.toBytes("cf"), Bytes.toBytes("area_code"),
						Bytes.toBytes(area_code));
				put4.add(Bytes.toBytes("cf"), Bytes.toBytes("area_code"),
						Bytes.toBytes(area_code));
			}
			
			/*是否上传排放量*/
			if(null!=isUp){
				put1.add(Bytes.toBytes("cf"), Bytes.toBytes("emission_access"),
						Bytes.toBytes(isUp));
				put2.add(Bytes.toBytes("cf"), Bytes.toBytes("emission_access"),
						Bytes.toBytes(isUp));
				put3.add(Bytes.toBytes("cf"), Bytes.toBytes("emission_access"),
						Bytes.toBytes(isUp));
				put4.add(Bytes.toBytes("cf"), Bytes.toBytes("emission_access"),
						Bytes.toBytes(isUp));
			}
			
			put1.add(Bytes.toBytes("cf"),
					Bytes.toBytes(realtimeCod + "_status"),
					Bytes.toBytes(statuss));
			put2.add(Bytes.toBytes("cf"),
					Bytes.toBytes(minuteCode + "_status"),
					Bytes.toBytes(statuss));
			put3.add(Bytes.toBytes("cf"), Bytes.toBytes(hourCode + "_status"),
					Bytes.toBytes(statuss));
			put4.add(Bytes.toBytes("cf"), Bytes.toBytes(dayCode + "_status"),
					Bytes.toBytes(statuss));

			if (null != realtimeCod && !realtimeCod.equals(""))
				put1.add(Bytes.toBytes("cf"),
						Bytes.toBytes(realtimeCod + "_monitor"),
						Bytes.toBytes(monitorFactorId));
			put1.add(Bytes.toBytes("cf"), Bytes.toBytes(realtimeCod + "_code"),
					Bytes.toBytes(realtimeCod));
			if (null != realtimeUnit && !realtimeUnit.equals(""))
				put1.add(Bytes.toBytes("cf"),
						Bytes.toBytes(realtimeCod + "_unit"),
						Bytes.toBytes(realtimeUnit));

			if (null != minuteCode && !minuteCode.equals(""))
				put2.add(Bytes.toBytes("cf"),
						Bytes.toBytes(minuteCode + "_monitor"),
						Bytes.toBytes(monitorFactorId));
			put2.add(Bytes.toBytes("cf"), Bytes.toBytes(minuteCode + "_code"),
					Bytes.toBytes(minuteCode));
			if (null != minuteUnit && !minuteUnit.equals(""))
				put2.add(Bytes.toBytes("cf"),
						Bytes.toBytes(minuteCode + "_unit"),
						Bytes.toBytes(minuteUnit));

			if (null != hourCode && !hourCode.equals(""))
				put3.add(Bytes.toBytes("cf"),
						Bytes.toBytes(hourCode + "_monitor"),
						Bytes.toBytes(monitorFactorId));
			put3.add(Bytes.toBytes("cf"), Bytes.toBytes(hourCode + "_code"),
					Bytes.toBytes(hourCode));
			if (null != hourUnit && !hourUnit.equals(""))
				put3.add(Bytes.toBytes("cf"),
						Bytes.toBytes(hourCode + "_unit"),
						Bytes.toBytes(hourUnit));

			if (null != dayCode && !dayCode.equals(""))
				put4.add(Bytes.toBytes("cf"),
						Bytes.toBytes(dayCode + "_monitor"),
						Bytes.toBytes(monitorFactorId));
			put4.add(Bytes.toBytes("cf"), Bytes.toBytes(dayCode + "_code"),
					Bytes.toBytes(dayCode));
			if (null != dayUnit && !dayUnit.equals(""))
				put4.add(Bytes.toBytes("cf"), Bytes.toBytes(dayCode + "_unit"),
						Bytes.toBytes(dayUnit));
			putLst.add(put4);
			putLst.add(put1);
			putLst.add(put2);
			putLst.add(put3);
		}
		rs.close();
		stmt.close();
		conn.close();

		HConnection connection = HbaseConnectionGenerator.getConnection();
		HTableInterface table = connection.getTable(Bytes
				.toBytes("verify_info"));
		table.put(putLst);
		table.close();
	}

	public static void getnertable1() throws Exception {
		HConnection connection = HbaseConnectionGenerator.getConnection();
		HBaseAdmin admin = new HBaseAdmin(connection);

		HTableDescriptor hd = new HTableDescriptor(TableName.valueOf(Bytes
				.toBytes("equipment_running_state")));

		HColumnDescriptor cdf = new HColumnDescriptor(Bytes.toBytes("cf"));
		cdf.setMaxVersions(1);
		cdf.setInMemory(true);
		hd.addFamily(cdf);

		admin.createTable(hd);
		admin.close();
	}

	public static void main(String[] args) throws Exception {
		
		HConnection connection = HbaseConnectionGenerator.getConnection();
		HTableInterface table = connection.getTable(Bytes.toBytes("log_count"));
		//Result rowOrBefore = table.getRowOrBefore(Bytes.toBytes("2017-03-02_hour"), Bytes.toBytes("cf"));
		//System.out.println(rowOrBefore);
		
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes("2017-03-01_hour"));
		//scan.setReversed(true);
		
		ResultScanner scanner = table.getScanner(scan);
		System.out.println(scanner.next());
		
		
		//getnertable1()
		//testInsert();
		//generateOutletTable()
		//		Configuration conf = HBaseConfiguration.create();
//		conf.set("hbase.zookeeper.quorum",
//				"hadoop5:2181,hadoop6:2181,hadoop7:2181");
//		HTable table = new HTable(conf, Bytes.toBytes("testtable"));
//
//		SingleColumnValueFilter filter = new SingleColumnValueFilter(
//				Bytes.toBytes("cf"), Bytes.toBytes("q2"), CompareOp.EQUAL,
//				new SubstringComparator("test"));
//		SingleColumnValueExcludeFilter f1 = new SingleColumnValueExcludeFilter(Bytes.toBytes("cf"),
//				Bytes.toBytes("q2"), CompareOp.EQUAL, new SubstringComparator(
//						"test"));
//		filter.setFilterIfMissing(true);
//		filter.setLatestVersionOnly(false);
//		// filter.setReversed(true);//在这里没什么用
//
//		Scan scan = new Scan();
//		scan.setFilter(f1);
//		// scan.setTimeStamp(2);
//
//		ResultScanner scanner = table.getScanner(scan);
//		for (Result res : scanner) {
//			for (Cell cell : res.listCells()) {
//				System.out.println("Cell:" + cell + ",Value:"
//						+ Bytes.toString(CellUtil.cloneValue(cell)));
//			}
//		}
//
//		scanner.close();
//		table.close();

		// HConnection connection = HbaseConnectionGenerator.getConnection();
		// HTableInterface table = connection.getTable(Bytes.toBytes("mtable"));

		// Put put = new Put(Bytes.toBytes("0001"));
		// put.add(Bytes.toBytes("cf"), Bytes.toBytes("name"), 1,
		// Bytes.toBytes("wwww"));
		// table.put(put);

		// Get get = new Get(Bytes.toBytes("0001"));
		// get.setMaxVersions();
		// get.setTimeStamp(1L);
		// Result result = table.get(get);
		// for(Cell cell:result.listCells()){
		// System.out.println(cell);
		// System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
		// }
		// table.close();

		// getnertable1();
		// HConnection connection = HbaseConnectionGenerator.getConnection();
		// HTableInterface table =
		// connection.getTable(Bytes.toBytes("verify_info"));
		// Result result = table.get(new Get(Bytes.toBytes("03502050800831")));
		//
		// for(Cell cell:result.listCells()){
		// System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell))+": "+Bytes.toString(CellUtil.cloneValue(cell)));
		// }
		// testInsert();
		// putOutletInfo();
		// test29();
		// generateOutletTable();
		// test28();
		// test27();
		// test26();
		// test25();
		// test24();
		// test22()
		// test21();
		// test20();
		// test19()
		// test18();
		// test17();
		// test16();
		// test15();
		// test14();
		// test13();
		// test12();
		// test11();
		// test10();
		// test9();
		// test2();
		// getConnection();
		// test3();
		// test4();
		// test5();
		// test6();
		// test8();
		// test7();
		// System.out.println(new
		// SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new
		// Date(1485160367644L)));
		;
	}
}
