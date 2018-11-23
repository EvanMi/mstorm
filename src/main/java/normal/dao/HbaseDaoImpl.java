package normal.dao;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDaoImpl implements HbaseDao {
	Configuration conf = null;

	public HbaseDaoImpl() {
		conf = HBaseConfiguration.create();
		String zkHost = "hadoop5:2181,hadoop6:2181,hadoop7:2181";
		conf.set("hbase.zookeeper.quorum", zkHost);

	}

	@Override
	public void save(String tableName, String rowKey, String famliy,
			String qualifer, byte[] value) {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(famliy), Bytes.toBytes(qualifer), value);
		save(put, tableName);
	}

	@Override
	public void save(Put put, String tableName) {
		HTableInterface table = null;

		try {
			table = HbaseConnectionGenerator.getConnection()
					.getTable(tableName);
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	@Override
	public void save(List<Put> puts, String tableName) {
		HTableInterface table = null;

		try {
			table = HbaseConnectionGenerator.getConnection()
					.getTable(tableName);
			table.put(puts);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public Result getOneRow(String tableName, String rowKey) {
		HTableInterface table = null;
		Result result = null;
		try {
			table = HbaseConnectionGenerator.getConnection()
					.getTable(tableName);
			Get get = new Get(Bytes.toBytes(rowKey));
			result = table.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	@Override
	public List<Result> getRows(String tableName, String rowKey_like) {
		HTableInterface table = null;
		List<Result> list = new ArrayList<Result>();
		try {
			table = HbaseConnectionGenerator.getConnection()
					.getTable(tableName);
			PrefixFilter filter = new PrefixFilter(Bytes.toBytes(rowKey_like));
			Scan scan = new Scan();
			scan.setFilter(filter);
			ResultScanner scanResult = table.getScanner(scan);
			for (Result rs : scanResult) {
				list.add(rs);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	@Override
	public void generateTable(String tableName, String... famlies) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			
			TableName tName = TableName.valueOf(tableName);
			HTableDescriptor td = new HTableDescriptor(tName);
			for (String famliy : famlies) {
				//td.SPLIT_POLICY.codePointAt(1);
				HColumnDescriptor cd = new HColumnDescriptor(famliy);
				cd.setMaxVersions(10);
				td.addFamily(cd);
				//td.setRegionSplitPolicyClassName(clazz);
			}
			admin.createTable(td);
			admin.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void dropTable(String tableName) {
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			admin.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void delete(String tableName, String rowKey, String family,
			String... qualifers) {
		HTableInterface table = null;

		try {
			table = HbaseConnectionGenerator.getConnection()
					.getTable(tableName);
			Delete del = new Delete(Bytes.toBytes(rowKey));
			for (String qualifer : qualifers) {
				del.deleteColumns(Bytes.toBytes(family),
						Bytes.toBytes(qualifer));
			}
			table.delete(del);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	@Override
	public void save(String tableName, String rowKey, String famliy,
			Map<String, Object> map) {

		HTableInterface table = null;
		Put put = new Put(Bytes.toBytes(rowKey));

		Iterator<Entry<String, Object>> ito = map.entrySet().iterator();
		while (ito.hasNext()) {
			Entry<String, Object> entry = ito.next();
			put.add(Bytes.toBytes(famliy), Bytes.toBytes(entry.getKey()),
					Bytes.toBytes((String) entry.getValue()));
		}

		try {
			table = HbaseConnectionGenerator.getConnection()
					.getTable(tableName);
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public List<Result> getRows(String tableName, String rowKey_like,
			String family, String... qualifers) {
		HTableInterface table = null;
		List<Result> list = new ArrayList<Result>();
		try {
			table = HbaseConnectionGenerator.getConnection()
					.getTable(tableName);
			PrefixFilter filter = new PrefixFilter(Bytes.toBytes(rowKey_like));
			Scan scan = new Scan();
			scan.setFilter(filter);
			for (String qualifer : qualifers) {
				scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifer));
			}
			ResultScanner scanResult = table.getScanner(scan);
			for (Result rs : scanResult) {
				list.add(rs);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	@Override
	public List<Result> getRows(String tableName, String startRow,
			String stopRow) {
		HTableInterface table = null;
		List<Result> list = new ArrayList<Result>();
		try {
			table = HbaseConnectionGenerator.getConnection()
					.getTable(tableName);
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes(startRow));
			scan.setStopRow(Bytes.toBytes(stopRow));
			ResultScanner scanResult = table.getScanner(scan);
			for (Result rs : scanResult) {
				list.add(rs);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	public static void main(String[] args) {
		HbaseDao dao = new HbaseDaoImpl();

	Calendar cal = Calendar.getInstance();
	cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH) - 1);
	cal.set(Calendar.HOUR_OF_DAY, 0);
	cal.set(Calendar.MINUTE, 0);
	cal.set(Calendar.SECOND, 0);
	cal.set(Calendar.MILLISECOND, 0);
	
	SimpleDateFormat sdfDay=new SimpleDateFormat("yyyy-MM-dd");
	String rowKey = sdfDay.format(cal.getTime());



		
	int tests=0;
		for (int i = 0; i < 24; i++) {
			int hour = cal.get(Calendar.HOUR_OF_DAY);
		int minute = cal.get(Calendar.MINUTE);
			int sec = cal.get(Calendar.SECOND);

		// 总秒数
			int curSecNum = hour * 3600 + minute * 60 + sec;

			Double xValue = (double) curSecNum / 3600;

		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm");
		
		SimpleDateFormat sdfMinute = new SimpleDateFormat("yyyyMMddHHmm");

			String columPrefix = sdfMinute.format(cal.getTime());

			/* { hour + ":" + minute, xValue.toString() } */
		String[] qualifers = { columPrefix + "_xValue",
					columPrefix + "_timeShow", columPrefix + "_value_",
				"now_xValue", "now_timeShow", "now_value" };
			
		
			
		byte[][] values = { Bytes.toBytes(xValue.toString()), Bytes.toBytes(dateFormat.format(cal.getTime())),
					Bytes.toBytes(tests + ""), Bytes.toBytes(xValue.toString()),
					Bytes.toBytes(dateFormat.format(cal.getTime())), Bytes.toBytes(tests + "") };
		dao.save("log_count", rowKey, "cf", qualifers, values);
			System.out.println("xValue:"+xValue+" timeShow:"+dateFormat.format(cal.getTime())+" "+columPrefix+" value:"+tests);
		tests+=new Random().nextInt(456);
		cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY)+1);
		}

		

		


		// for (int i = 1; i <= 5; i++) {
		// dao.delete("area_order", "2016-09-28_" + i, "cf", "order_amt");
		// }

		// dao.dropTable("SYSTEM.CATALOG");
		// dao.dropTable("SYSTEM.FUNCTION");
		// dao.dropTable("SYSTEM.SEQUENCE");
		// dao.dropTable("SYSTEM.STATS");
		// dao.dropTable("verify_info");

		// dao.generateTable("log_count", "cf");

		// Result result = dao.getOneRow("log_count", "2016-11-07");
		// for (Cell cell : result.listCells()) {
		// System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell))
		// + ":" + Bytes.toString(CellUtil.cloneValue(cell)));
		// }

		/*
		 * Result oneRow = dao.getOneRow("person_info", "rk_001");
		 * System.out.println(Bytes.toString(oneRow.getValue(
		 * Bytes.toBytes("base_info"), Bytes.toBytes("name"))));
		 * System.out.println("=========================="); for (Cell cell :
		 * oneRow.listCells()) { System.out.println("family:" +
		 * Bytes.toString(CellUtil.cloneFamily(cell)));
		 * System.out.println("qualifier:" +
		 * Bytes.toString(CellUtil.cloneQualifier(cell)));
		 * System.out.println("value:" +
		 * Bytes.toString(CellUtil.cloneValue(cell)));
		 * System.out.println("----------------------"); }
		 */

		// List<Result> rows = dao.getRows("person_info", "rk_001", "rk_007");
		// for (Result result : rows) {
		// for (Cell cell : result.listCells()) {
		// System.out.println("row:"
		// + Bytes.toString(CellUtil.cloneRow(cell)));
		// System.out.println("family:"
		// + Bytes.toString(CellUtil.cloneFamily(cell)));
		// System.out.println("qualifier:"
		// + Bytes.toString(CellUtil.cloneQualifier(cell)));
		// System.out.println("value:"
		// + Bytes.toString(CellUtil.cloneValue(cell)));
		// System.out.println("------------------");
		// }
		// }

		//dao.generateTable("log_count", "cf");
		//dao.dropTable("log_count");
		// String tem ="123\tdddd\tfff";
		// System.out.println(StringUtils.split(tem, "\\t")[1]);
	}

	@Override
	public void save(String tableName, String rowKey, String family,
			String[] qualifers, byte[][] values) {
		HTableInterface table = null;
		try {
			table = HbaseConnectionGenerator.getConnection()
					.getTable(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			for (int i = 0; i < qualifers.length; i++) {
				put.add(Bytes.toBytes(family), Bytes.toBytes(qualifers[i]),
						values[i]);
			}
			table.put(put);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	@Override
	public Result getOneRow(String tableName, String rowKey, String family,
			String... qualifers) {
		HTableInterface table = null;

		Result result = null;
		try {
			table = HbaseConnectionGenerator.getConnection()
					.getTable(tableName);
			Get get = new Get(Bytes.toBytes(rowKey));
			for (String str : qualifers) {
				get.addColumn(Bytes.toBytes(family), Bytes.toBytes(str));
			}
			result = table.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}
}
