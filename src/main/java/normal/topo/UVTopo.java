package normal.topo;

import normal.bolt.FormatAndSplitBolt;
import normal.bolt.UVBolt;
import normal.bolt.UVSumBolt;
import normal.spout.StaticUVSpout;

import org.apache.hadoop.net.NetworkTopology.InvalidTopologyException;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class UVTopo {

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("uvSpout", new StaticUVSpout(), 1);
		builder.setBolt("formatBolt", new FormatAndSplitBolt(), 4)
				.shuffleGrouping("uvSpout");
		builder.setBolt("uvBolt", new UVBolt(), 6).fieldsGrouping("formatBolt",
				new Fields("sessionId", "createDate"));
		builder.setBolt("sumBolt", new UVSumBolt(), 1)
				.shuffleGrouping("uvBolt");

		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumAckers(0);
		conf.setNumWorkers(4);/*在本地启动的时候要注释掉哦*/

		if (args.length == 0) {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("test", conf, builder.createTopology());
		} else {
			try {
				StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
						builder.createTopology());
			} catch (org.apache.storm.generated.InvalidTopologyException e) {
				e.printStackTrace();
			} catch (AuthorizationException e) {
				e.printStackTrace();
			}
		}
	}
}
