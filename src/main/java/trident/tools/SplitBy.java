package trident.tools;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;



public class SplitBy extends BaseFunction {

	private static final long serialVersionUID = -1301925768704884634L;
	String partten = null;

	public SplitBy(String partten) {

		this.partten = partten;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {

		if (!tuple.isEmpty()) {
			String msg = tuple.getString(0);
			String[] values = StringUtils.split(msg, this.partten);

			collector.emit(new Values(values[0], values[1]));

		}
	}

}
