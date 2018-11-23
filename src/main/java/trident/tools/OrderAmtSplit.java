package trident.tools;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class OrderAmtSplit extends BaseFunction {

	private static final long serialVersionUID = 8416255310638151814L;

	String partten = null;

	public OrderAmtSplit(String partten) {

		this.partten = partten;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {

		if (!tuple.isEmpty()) {
			String msg = tuple.getString(0);
			msg = msg.replaceAll("(\r\n|\r|\n|\n\r)", "");
			String values[] = StringUtils.split(msg, this.partten);
			// order_id order_amt create_time province_id
			collector.emit(new Values(values[0], Double.parseDouble(values[1]),
					values[2], "amt_" + values[3]));

		}
	}

}
