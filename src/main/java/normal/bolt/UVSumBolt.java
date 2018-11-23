package normal.bolt;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import normal.dao.HbaseDao;
import normal.dao.HbaseDaoImpl;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;



public class UVSumBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	private String nowDate = null;
	private SimpleDateFormat sdfDay = null;
	private SimpleDateFormat sdfMinute = null;
	private SimpleDateFormat sdfHour = null;
	private String hourFieldStr = null;
	private Map<String, Long> countMap = null;
	private long preSaveTime = 0;
	private int nowHour = 0;
	private Long hourData = 0L;
	private HbaseDao dao = null;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
		this.sdfDay = new SimpleDateFormat("yyyy-MM-dd");
		this.sdfMinute = new SimpleDateFormat("yyyyMMddHHmm");
		this.sdfHour = new SimpleDateFormat("HH");
		this.hourFieldStr = sdfHour.format(new Date());
		nowDate = sdfDay.format(new Date());
		countMap = new HashMap<String, Long>();
		preSaveTime = System.currentTimeMillis();
		dao = new HbaseDaoImpl();
		nowHour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
		hourData = 0L;

		Result oneRow = dao.getOneRow("log_count", nowDate, "cf", "now_value");
		if (!oneRow.isEmpty()) {
			byte[] value = oneRow.getValue(Bytes.toBytes("cf"),
					Bytes.toBytes("now_value"));
			countMap.put(nowDate, Long.parseLong(Bytes.toString(value)));
		}

	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		try {
			if (null != tuple) {
				String key = tuple.getString(0);
				String[] keyFields = key.split("_");
				if (keyFields.length != 2)
					throw new Exception("key " + key + "_的格式错误");
				String date = keyFields[1];

				if (!nowDate.equals(date) && nowDate.compareTo(date) < 0) {
					nowDate = date;
					countMap.clear();
				}

				int temHour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);

				if (temHour != nowHour) {
					nowHour = temHour;
					hourData = 0L;
					this.hourFieldStr = sdfHour.format(new Date());
				}

				String countKey = date;

				Long count = countMap.get(countKey);

				if (null == count)
					count = 0L;

				count++;
				hourData++;

				countMap.put(countKey, count);

				long nowSaveTime = System.currentTimeMillis();
				if (nowSaveTime - preSaveTime > 5000) {

					String rowKey = date;

					String[] end = getXValueStr();

					String columPrefix = sdfMinute.format(new Date());

					/* { hour + ":" + minute, xValue.toString() } */
					String[] qualifers = { columPrefix + "_xValue",
							columPrefix + "_timeShow", columPrefix + "_value_",
							"now_xValue", "now_timeShow", "now_value" };

					byte[][] values = { Bytes.toBytes(end[1]),
							Bytes.toBytes(end[0]), Bytes.toBytes(count + ""),
							Bytes.toBytes(end[1]), Bytes.toBytes(end[0]),
							Bytes.toBytes(count + "") };

					dao.save("log_count", rowKey, "cf", qualifers, values);

					dao.save("log_count", rowKey + "_hour", "cf", hourFieldStr
							+ "_value", Bytes.toBytes(hourData + ""));

					preSaveTime = System.currentTimeMillis();
				}

			}
		} catch (Exception e) {
			//System.err.println(e.getMessage());
		}

	}

	public String[] getXValueStr() {
		Calendar c = Calendar.getInstance();
		int hour = c.get(Calendar.HOUR_OF_DAY);
		int minute = c.get(Calendar.MINUTE);
		int sec = c.get(Calendar.SECOND);

		int curSecNum = hour * 3600 + minute * 60 + sec;

		Double xValue = (double) curSecNum / 3600;

		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm");

		String[] end = { dateFormat.format(c.getTime()), xValue.toString() };

		return end;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
