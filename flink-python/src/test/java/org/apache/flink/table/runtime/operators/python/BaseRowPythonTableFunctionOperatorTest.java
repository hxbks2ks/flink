package org.apache.flink.table.runtime.operators.python;

import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.util.BaseRowHarnessAssertor;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collection;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.baserow;

public class BaseRowPythonTableFunctionOperatorTest
	extends PythonTableFunctionOperatorTestBase<BaseRow, BaseRow, BaseRow, BaseRow> {

	private final BaseRowHarnessAssertor assertor = new BaseRowHarnessAssertor(new TypeInformation[]{
		Types.STRING,
		Types.STRING,
		Types.LONG,
		Types.LONG
	});

	@Override
	public BaseRow newRow(boolean accumulateMsg, Object... fields) {
		if (accumulateMsg) {
			return baserow(fields);
		} else {
			return BaseRowUtil.setRetract(baserow(fields));
		}
	}

	@Override
	public void assertOutputEquals(String message, Collection<Object> expected, Collection<Object> actual) {
		assertor.assertOutputEquals(message, expected, actual);
	}

	@Override
	public StreamTableEnvironment createTableEnvironment(StreamExecutionEnvironment env) {
		return StreamTableEnvironment.create(
			env,
			EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build());
	}

	@Override
	public AbstractPythonTableFunctionOperator<BaseRow, BaseRow, BaseRow, BaseRow> getTestOperator(
		Configuration config,
		PythonFunctionInfo tableFunction,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets) {
		return new BaseRowPassThroughPythonTableFunctionOperator(
			config, tableFunction, inputType, outputType, udfInputOffsets);
	}

	private static class BaseRowPassThroughPythonTableFunctionOperator extends BaseRowPythonTableFunctionOperator {

		BaseRowPassThroughPythonTableFunctionOperator(
			Configuration config,
			PythonFunctionInfo tableFunction,
			RowType inputType,
			RowType outputType,
			int[] udfInputOffsets) {
			super(config, tableFunction, inputType, outputType, udfInputOffsets);
		}

		@Override
		public PythonFunctionRunner<BaseRow> createPythonFunctionRunner(
			FnDataReceiver<BaseRow> resultReceiver,
			PythonEnvironmentManager pythonEnvironmentManager) {
			return new BaseRowPassThroughPythonTableFunctionRunner(resultReceiver);
		}
	}
}
