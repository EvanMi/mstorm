package trident.tools;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;


public class Print extends BaseFunction {

	private static final long serialVersionUID = -705869775976703437L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		if (!tuple.isEmpty()) {
			String message = tuple.getString(0);
			System.out.println(message);
		}
	}

}
