package trident.myhbasestate;

import java.util.Map;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.map.CachedMap;
import org.apache.storm.trident.state.map.MapState;
import org.apache.storm.trident.state.map.NonTransactionalMap;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.apache.storm.trident.state.map.SnapshottableMap;
import org.apache.storm.trident.state.map.TransactionalMap;
import org.apache.storm.tuple.Values;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class MyHbaseAggregateStateFactory implements StateFactory {

	private static final long serialVersionUID = 6504244622155615883L;

	private StateType stateType;
	private String tableName;
	private int stateCacheSize = 1000;

	public MyHbaseAggregateStateFactory(String tableName, StateType stateType) {

		this.tableName = tableName;
		this.stateType = stateType;
	}

	@Override
	public State makeState(Map conf, IMetricsContext metrics,
			int partitionIndex, int numPartitions) {

		MyHbaseAggregateState myHbaseAggregateState = new MyHbaseAggregateState(
				tableName, stateType);
		CachedMap cachedMap = new CachedMap(myHbaseAggregateState,
				stateCacheSize);

		MapState mapState = null;
		if (stateType.equals(StateType.NON_TRANSACTIONAL)) {
			mapState = NonTransactionalMap.build(cachedMap);

		} else if (stateType.equals(StateType.TRANSACTIONAL)) {

			mapState = TransactionalMap.build(cachedMap);

		} else if (stateType.equals(StateType.OPAQUE)) {

			mapState = OpaqueMap.build(cachedMap);

		}

		return new SnapshottableMap(mapState, new Values("$GLOBAL$"));
	}

}
