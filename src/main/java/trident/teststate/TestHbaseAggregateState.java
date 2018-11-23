package trident.teststate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.JSONNonTransactionalSerializer;
import org.apache.storm.trident.state.JSONOpaqueSerializer;
import org.apache.storm.trident.state.JSONTransactionalSerializer;
import org.apache.storm.trident.state.Serializer;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.map.CachedMap;
import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.storm.trident.state.map.MapState;
import org.apache.storm.trident.state.map.NonTransactionalMap;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.apache.storm.trident.state.map.SnapshottableMap;
import org.apache.storm.trident.state.map.TransactionalMap;
import org.apache.storm.tuple.Values;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestHbaseAggregateState<T> implements IBackingMap<T> {
	private String tableName;
	private Serializer serializer;

	public TestHbaseAggregateState(String tableName, Serializer serializer) {
		this.tableName = tableName;
		this.serializer = serializer;
	}

	@Override
	public List<T> multiGet(List<List<Object>> list) {
		HTableInterface table = null;
		try {
			table = HbaseConnectionGenerater.generateHbaseConnection()
					.getTable(TableName.valueOf(this.tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<Get> gets = new ArrayList<Get>(list.size());
		for (List<Object> l : list) {
			Get get = new Get(Bytes.toBytes((String) l.get(0)));
			get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes((String) l.get(1)));
			gets.add(get);
		}
		Result[] results = null;
		try {
			results = table.get(gets);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		List<T> rnt = new ArrayList<T>();
		for (int i = 0; i < results.length; i++) {
			byte[] cq = Bytes.toBytes((String) list.get(i).get(1));
			if (results[i].isEmpty()) {
				rnt.add(null);
			} else {
				rnt.add((T) this.serializer.deserialize(results[i].getValue(
						Bytes.toBytes("cf"), cq)));
			}
		}

		return rnt;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<T> values) {
		HTableInterface table = null;
		try {
			table = HbaseConnectionGenerater.generateHbaseConnection()
					.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}

		List<Put> puts = new ArrayList<Put>(values.size());

		for (int i = 0; i < values.size(); i++) {
			Put put = new Put(Bytes.toBytes((String) keys.get(i).get(0)));
			put.add(Bytes.toBytes("cf"),
					Bytes.toBytes((String) keys.get(i).get(1)),
					serializer.serialize(values.get(i)));
			puts.add(put);
		}

		try {
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

	public static StateFactory nonTransactional(String tableName) {
		return new TestHbaseAggregateFactory(StateType.NON_TRANSACTIONAL,
				tableName);
	}

	public static StateFactory transactional(String tableName) {
		return new TestHbaseAggregateFactory(StateType.TRANSACTIONAL, tableName);
	}

	public static StateFactory opaque(String tableName) {
		return new TestHbaseAggregateFactory(StateType.OPAQUE, tableName);
	}

	protected static class TestHbaseAggregateFactory implements StateFactory {

		private static final long serialVersionUID = 6319570645642302481L;

		private int cacheSize = 1000;
		private StateType stateType = null;
		private String tableName;

		public TestHbaseAggregateFactory(StateType stateType, String tableName) {
			this.stateType = stateType;
			this.tableName = tableName;
		}

		@Override
		public State makeState(Map conf, IMetricsContext metrics,
				int partitionIndex, int numPartitions) {
			Serializer serializer = null;
			if (stateType == StateType.NON_TRANSACTIONAL) {
				serializer = new JSONNonTransactionalSerializer();
			} else if (stateType == StateType.TRANSACTIONAL) {
				serializer = new JSONTransactionalSerializer();
			} else if (stateType == StateType.OPAQUE) {
				serializer = new JSONOpaqueSerializer();
			} else {
				throw new RuntimeException("Unkonwn state type:" + stateType);
			}
			TestHbaseAggregateState state = new TestHbaseAggregateState(
					this.tableName, serializer);

			CachedMap cachedMap = new CachedMap(state, this.cacheSize);
			MapState mapState = null;

			if (stateType == StateType.NON_TRANSACTIONAL) {
				mapState = NonTransactionalMap.build(cachedMap);
			} else if (stateType == StateType.TRANSACTIONAL) {
				mapState = TransactionalMap.build(cachedMap);
			} else if (stateType == StateType.OPAQUE) {
				mapState = OpaqueMap.build(cachedMap);
			} else {
				throw new RuntimeException("Unkonwn state type:" + stateType);
			}

			return new SnapshottableMap(mapState, new Values("$GLOBAL$"));
		}

	}

}
