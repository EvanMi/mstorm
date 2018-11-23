package trident.topo;

import org.apache.hadoop.net.NetworkTopology.InvalidTopologyException;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;

import trident.hbasestate.HbaseAggregateState;
import trident.hbasestate.TridentConfig;
import trident.tools.OrderAmtSplit;
import trident.tools.OrderNumSplit;
import trident.tools.Split;
import trident.tools.SplitBy;

public class TridentTopo {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static StormTopology buildTopo(LocalDRPC drpc) {
		TridentConfig tridentConfig = new TridentConfig("state");
		StateFactory state = HbaseAggregateState.transactional(tridentConfig);

		// StateFactory state = new MyHbaseAggregateState("state",
		// StateType.TRANSACTIONAL).getFactory();

		ZkHosts zkHosts = new ZkHosts("mpc5:2181,mpc6:2181,mpc7:2181");
		String topStr = "mytri3";
		TridentKafkaConfig config = new TridentKafkaConfig(zkHosts, topStr);

		config.ignoreZkOffsets=false;
		config.scheme = new SchemeAsMultiScheme(new StringScheme());
		// config.fetchSizeBytes=10;

		TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(
				config);

		TridentTopology topo = new TridentTopology();

		TridentState amtState = topo
				.newStream("spout", spout)
				.parallelismHint(2)
				.each(new Fields(StringScheme.STRING_SCHEME_KEY),
						new OrderAmtSplit("\t"),
						new Fields("order_id", "order_amt", "create_date",
								"province_id"))
				.shuffle()
				.groupBy(new Fields("create_date", "province_id"))
				.persistentAggregate(state, new Fields("order_amt"), new Sum(),
						new Fields("sum_amt"));

		topo.newDRPCStream("getOrderAmt", drpc)
				.each(new Fields("args"), new Split(" "), new Fields("arg"))
				.each(new Fields("arg"), new SplitBy("\\:"),
						new Fields("create_date", "province_id"))
				.groupBy(new Fields("create_date", "province_id"))
				.stateQuery(amtState, new Fields("create_date", "province_id"),
						new MapGet(), new Fields("sum_amt"))
				.applyAssembly(new FirstN(5, "sum_amt", true));

		TridentState orderState = topo
				.newStream("orderSpout", spout)
				.parallelismHint(2)
				.each(new Fields(StringScheme.STRING_SCHEME_KEY),
						new OrderNumSplit("\t"),
						new Fields("order_id", "order_amt", "create_date",
								"province_id"))
				.shuffle()
				.groupBy(new Fields("create_date", "province_id"))
				.persistentAggregate(state, new Fields("province_id"),
						new Count(), new Fields("order_num"));

		topo.newDRPCStream("getOrderNum", drpc)
				.each(new Fields("args"), new Split(" "), new Fields("arg"))
				.each(new Fields("arg"), new SplitBy("\\:"),
						new Fields("create_date", "province_id"))
				.groupBy(new Fields("create_date", "province_id"))
				.stateQuery(orderState,
						new Fields("create_date", "province_id"), new MapGet(),
						new Fields("order_num"))
		// .applyAssembly(new FirstN(5, "order_num", true))
		;

		return topo.build();

	}

	public static void main(String[] args) {

		LocalDRPC drpc = new LocalDRPC();

		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(4);

		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf, buildTopo(null));
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			} catch (org.apache.storm.generated.InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (AuthorizationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {

			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf, buildTopo(drpc));
		}

		// while (true) {
		// System.err
		// .println(drpc
		// .execute(
		// "getOrderAmt",
		// "2016-09-30:1 2016-09-30:2 2016-09-30:3 2016-09-30:4 2016-09-30:5 2016-09-30:6 2016-09-30:7 2016-09-30:8 2016-09-30:9"));
		// System.out
		// .println(drpc
		// .execute(
		// "getOrderNum",
		// "2016-09-30:1 2016-09-30:2 2016-09-30:3 2016-09-30:4 2016-09-30:5 2016-09-30:6 2016-09-30:7 2016-09-30:8 2016-09-30:9"));
		// Utils.sleep(2000);
		// }

	}
}
