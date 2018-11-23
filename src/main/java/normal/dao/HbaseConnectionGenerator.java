package normal.dao;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;


public class HbaseConnectionGenerator {

	private HbaseConnectionGenerator() {

	}

	private static HConnection connection = null;
	public static HConnection getConnection() {
		if (null == connection) {
			Configuration conf = HBaseConfiguration.create();
			String zkHost = "hadoop5:2181,hadoop6:2181,hadoop7:2181";
			conf.set("hbase.zookeeper.quorum", zkHost);
			ExecutorService pool = Executors.newFixedThreadPool(10);
			try {
				connection = HConnectionManager.createConnection(conf, pool);
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("------------");
		}
		System.out.println("=================");
		return connection;

	}
}
