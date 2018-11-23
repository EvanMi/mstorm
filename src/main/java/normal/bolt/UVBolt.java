package normal.bolt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class UVBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	String today = null;
	Set<String> emited = null;
	Map<String, Long> pvMap = null;;

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);

		today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		emited = new HashSet<String>();
		pvMap = new HashMap<String, Long>();
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		if (null != tuple) {
			try {

				//String url = tuple.getString(0);
				String sessionId = tuple.getString(1);
				String createDate = tuple.getString(2);
				String key = sessionId + "_" + createDate;
				if (!today.equals(createDate)
						&& today.compareTo(createDate) < 0) {
					today = createDate;
					pvMap.clear();
					emited.clear();
				}

				if (emited.contains(key))
					throw new Exception("already emited:" + key);

				Long count = pvMap.get(key);

				if (null == count) {
					count = 0L;
				}
				count++;

				pvMap.put(key, count);

				if (count >= 2) {
					emited.add(key);
					// www.mpc.com c7da534b80804bbabb0a245e94cad1a4_2016-11-04
					collector.emit(new Values(key));
					// System.err.println(key);
				}

			} catch (Exception e) {
				//System.err.println(e.getMessage());
			}
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("logKey"));
	}

}
