package test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class MessageScheme implements Scheme {

	private static final long serialVersionUID = 3029557435066271451L;

	@Override
	public Fields getOutputFields() {
		return new Fields("order");
		
	}

	@Override
	public List<Object> deserialize(ByteBuffer ser) {
		try {
			String msg = new String(ser.array(), "UTF-8");
			return new Values(msg);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

}
