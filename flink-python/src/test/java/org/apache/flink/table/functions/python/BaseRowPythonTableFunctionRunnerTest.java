package org.apache.flink.table.functions.python;

import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.python.env.ProcessPythonEnvironmentManager;
import org.apache.flink.python.env.PythonDependencyInfo;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.runners.python.AbstractPythonTableFunctionRunner;
import org.apache.flink.table.runtime.runners.python.BaseRowPythonTableFunctionRunner;
import org.apache.flink.table.runtime.typeutils.serializers.python.BaseRowTableSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link BaseRowPythonTableFunctionRunner}. These test that
 * the input data type and output data type are properly constructed.
 */
public class BaseRowPythonTableFunctionRunnerTest extends AbstractPythonTableFunctionRunnerTest<BaseRow, BaseRow> {

	@Test
	public void testInputOutputDataTypeConstructedProperlyForSingleUDTF() throws Exception {
		final AbstractPythonTableFunctionRunner<BaseRow, BaseRow> runner = createUDTFRunner();

		// check input TypeSerializer
		TypeSerializer inputTypeSerializer = runner.getInputTypeSerializer();
		assertTrue(inputTypeSerializer instanceof BaseRowTableSerializer);

		assertEquals(1, ((BaseRowTableSerializer) inputTypeSerializer).getBaseRowSerializer().getArity());

		// check output TypeSerializer
		TypeSerializer outputTypeSerializer = runner.getOutputTypeSerializer();
		assertTrue(outputTypeSerializer instanceof BaseRowTableSerializer);
		assertEquals(1, ((BaseRowTableSerializer) outputTypeSerializer).getBaseRowSerializer().getArity());
	}


	@Override
	public AbstractPythonTableFunctionRunner<BaseRow, BaseRow> createPythonTableFunctionRunner(
		PythonFunctionInfo pythonFunctionInfo,
		RowType inputType,
		RowType outputType) throws Exception {
		final FnDataReceiver<BaseRow> dummyReceiver = input -> {
			// ignore the execution results
		};

		final PythonEnvironmentManager environmentManager =
			new ProcessPythonEnvironmentManager(
				new PythonDependencyInfo(new HashMap<>(), null, null, new HashMap<>(), null),
				new String[]{System.getProperty("java.io.tmpdir")},
				null,
				new HashMap<>());

		return new BaseRowPythonTableFunctionRunner(
			"testPythonRunner",
			dummyReceiver,
			pythonFunctionInfo,
			environmentManager,
			inputType,
			outputType);
	}
}
