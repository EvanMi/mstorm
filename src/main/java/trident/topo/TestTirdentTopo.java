package trident.topo;

import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;

import trident.teststate.TestHbaseAggregateState;
import trident.testtool.TestSplitAmt;
import trident.testtool.TestUpper;

public class TestTirdentTopo {

	public static StormTopology getTopo(LocalDRPC drpc) {

		StateFactory state = TestHbaseAggregateState.transactional("state");


		// order_id order_amt create_time province_id
		String topic = "mytri";
		ZkHosts hosts = new ZkHosts("mpc5:2181,mpc6:2181,mpc7:2181");

		TridentKafkaConfig config = new TridentKafkaConfig(hosts, topic);
		config.ignoreZkOffsets=false;
		config.scheme = new SchemeAsMultiScheme(new StringScheme());

		TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(
				config);

		TridentTopology topo = new TridentTopology();

		topo.newStream("spout", spout)
				.parallelismHint(2)
				.each(new Fields(StringScheme.STRING_SCHEME_KEY),
						new TestSplitAmt("\\t"),
						new Fields("order_id", "order_amt", "create_date",
								"province_id"))
				.shuffle()
				.partitionPersist(state,
						new Fields("province_id", "order_amt"), new TestUpper())
		// .groupBy(new Fields("create_date", "province_id"))
		// .persistentAggregate(state, new Fields("order_amt"), new Sum(),
		// new Fields("sum_amt"))
		;

		return topo.build();
	}

}
