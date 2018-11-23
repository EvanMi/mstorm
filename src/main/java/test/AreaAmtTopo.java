package test;

import org.apache.hadoop.net.NetworkTopology.InvalidTopologyException;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

public class AreaAmtTopo {

	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		BrokerHosts zkHosts = new ZkHosts("");
		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, "",
				"/storm-project", "KafkaSpout");
		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());

		builder.setSpout("KafkaSpout", new KafkaSpout(spoutConfig), 5);

		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(2);

		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf,
						builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			} catch (org.apache.storm.generated.InvalidTopologyException e) {
				e.printStackTrace();
			} catch (AuthorizationException e) {
				e.printStackTrace();
			}
		} else {

			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf,
					builder.createTopology());
		}

	}

}
