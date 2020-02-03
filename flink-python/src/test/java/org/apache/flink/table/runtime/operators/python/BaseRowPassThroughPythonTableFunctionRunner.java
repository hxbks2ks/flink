package org.apache.flink.table.runtime.operators.python;

import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.binaryrow;

public class BaseRowPassThroughPythonTableFunctionRunner extends AbstractPassThroughPythonTableFunctionRunner<BaseRow> {
	BaseRowPassThroughPythonTableFunctionRunner(FnDataReceiver<BaseRow> resultReceiver) {
		super(resultReceiver);
	}

	@Override
	public BaseRow copy(BaseRow element) {
		BaseRow row = binaryrow(element.getLong(0));
		row.setHeader(element.getHeader());
		return row;
	}

	@Override
	public void finishBundle() throws Exception {
		Preconditions.checkState(bundleStarted);
		bundleStarted = false;

		for (BaseRow element : bufferedElements) {
			resultReceiver.accept(element);
			resultReceiver.accept(new BinaryRow(0));
		}
		bufferedElements.clear();
	}
}
