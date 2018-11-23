package normal.spout;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;



public class StaticUVSpout extends BaseRichSpout {
	SpoutOutputCollector collector = null;
	SimpleDateFormat sdf = null;
	List<String> sessionIds = null;
	String url = null;
	private static final long serialVersionUID = 1L;

	@Override
	public void nextTuple() {
		int flag = new Random().nextInt(10);
		String sessionId = null;
		if (flag > 2 && sessionIds.size() > 0) {
			sessionId = sessionIds.get(0);
			if (flag == 5 || flag == 9 || flag == 6 || flag == 7)
				sessionIds.remove(sessionId);
		} else {
			sessionId = UUIDGenerater.generateUUId();
			sessionIds.add(sessionId);
		}
		String emitValue = url + "\t" + sessionId + "\t"
				+ sdf.format(new Date(System.currentTimeMillis()));
		collector.emit(new Values(emitValue));
		Utils.sleep(100);
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.sdf = new SimpleDateFormat("yyyy-MM-dd");
		sessionIds = new ArrayList<String>();
		url = "www.mpc.com";
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("record"));
	}

}
