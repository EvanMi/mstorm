package trident.testtool;

import java.util.List;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;

import trident.teststate.TestState;

public class TestQuery<T> extends BaseQueryFunction<TestState, T> {

	private static final long serialVersionUID = -939965312987586742L;

	@Override
	public List<T> batchRetrieve(TestState state, List<TridentTuple> tuples) {
		return null;
	}

	@Override
	public void execute(TridentTuple tuple, T value, TridentCollector collector) {

	}

}
