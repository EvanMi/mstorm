package normal.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FormatAndSplitBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		if (null != tuple) {
			try {
				String emitValue = tuple.getString(0);
				if (null == emitValue || "".equals(emitValue))
					throw new Exception("the value is null or empty !");
				emitValue = emitValue.replaceAll("(\r\n|\r|\n|\n\r)", "");
				String[] fields = emitValue.split("\\t");
				if (3 != fields.length)
					throw new Exception(
							"required three fields,but actully there are "
									+ fields.length);
				collector.emit(new Values(fields[0], fields[1], fields[2]));
			} catch (Exception e) {
				//System.err.println(e.getMessage());
			}
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url", "sessionId", "createDate"));
	}

}
