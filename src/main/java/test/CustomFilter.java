package test;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class CustomFilter extends FilterBase {

	private byte[] value = null;
	private boolean filterRow = true;

	public CustomFilter() {
		super();
	}

	public CustomFilter(byte[] value) {
		this.value = value;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	public boolean isFilterRow() {
		return filterRow;
	}

	@Override
	public void reset() throws IOException {
		this.filterRow = true;
	}

	@Override
	public ReturnCode filterKeyValue(Cell cell) throws IOException {
		if (Bytes.compareTo(value, CellUtil.cloneValue(cell)) == 0) {
			filterRow = false;
		}
		return ReturnCode.INCLUDE;
	}

	@Override
	public boolean filterRow() throws IOException {
		return filterRow;
	}

	@Override
	public byte[] toByteArray() throws IOException {
		CustomFilterProto.CustomFilter.Builder builder = CustomFilterProto.CustomFilter
				.newBuilder();
		if (this.value != null)
			builder.setValue(ByteString.copyFrom(this.value));
		builder.setFilterRow(this.filterRow);
		return builder.build().toByteArray();
	}

	public static CustomFilter parseFrom(final byte[] pbBytes)
			throws DeserializationException {
		CustomFilterProto.CustomFilter proto = null;
		try {
			proto = CustomFilterProto.CustomFilter.parseFrom(pbBytes);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		return new CustomFilter(proto.getValue().toByteArray());
	}

	public static CustomFilter createFilterFromArguments(
			ArrayList<byte[]> filterArguments) {
		Preconditions.checkArgument(filterArguments.size() == 1,
				"Expected 1 but got: %s", filterArguments.size());
		byte[] value = ParseFilter.removeQuotesFromByteArray(filterArguments
				.get(0));
		return new CustomFilter(value);
	}

}
