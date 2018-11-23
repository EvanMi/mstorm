package trident.teststate;

import java.util.List;

import org.apache.storm.trident.state.ValueUpdater;
import org.apache.storm.trident.state.map.MapState;

public class TestState<T> implements MapState<T> {

	@Override
	public List<T> multiGet(List<List<Object>> arg0) {
		return null;
	}

	@Override
	public void beginCommit(Long arg0) {

	}

	@Override
	public void commit(Long arg0) {

	}

	@Override
	public void multiPut(List<List<Object>> arg0, List<T> arg1) {

	}

	@Override
	public List<T> multiUpdate(List<List<Object>> arg0, List<ValueUpdater> arg1) {
		return null;
	}

}
