/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.runners.python.table.PythonTableFunctionRunner;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.calcite.rel.core.JoinRelType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The {@link RichFlatMapFunction} used to invoke Python {@link TableFunction} functions for the
 * old planner.
 */
@Internal
public final class PythonTableFunctionFlatMap extends AbstractPythonStatelessFunctionFlatMap {

	private static final long serialVersionUID = 1L;

	/**
	 * The Python {@link TableFunction} to be executed.
	 */
	private final PythonFunctionInfo tableFunction;

	/**
	 * The correlate join type.
	 */
	private final JoinRelType joinType;

	public PythonTableFunctionFlatMap(
		Configuration config,
		PythonFunctionInfo tableFunction,
		RowType inputType,
		RowType outputType,
		int[] udtfInputOffsets,
		JoinRelType joinType) {
		super(config, inputType, outputType, udtfInputOffsets);
		this.tableFunction = Preconditions.checkNotNull(tableFunction);
		Preconditions.checkArgument(
			joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT,
			"The join type should be inner join or left join");
		this.joinType = joinType;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		RowTypeInfo forwardedInputTypeInfo = new RowTypeInfo(TypeConversions.fromDataTypeToLegacyInfo(
			TypeConversions.fromLogicalToDataType(inputType)));
		forwardedInputSerializer = forwardedInputTypeInfo.createSerializer(getRuntimeContext().getExecutionConfig());

		List<RowType.RowField> udtfOutputDataFields = new ArrayList<>(
			outputType.getFields().subList(inputType.getFieldCount(), outputType.getFieldCount()));
		userDefinedFunctionOutputType = new RowType(udtfOutputDataFields);

		super.open(parameters);
	}

	@Override
	public PythonEnv getPythonEnv() {
		return tableFunction.getPythonFunction().getPythonEnv();
	}

	@Override
	public PythonFunctionRunner<Row> createPythonFunctionRunner() throws IOException {
		FnDataReceiver<byte[]> userDefinedFunctionResultReceiver = input -> {
			// handover to queue, do not block the result receiver thread
			userDefinedFunctionResultQueue.put(input);
		};

		return new PythonTableFunctionRunner(
			getRuntimeContext().getTaskName(),
			userDefinedFunctionResultReceiver,
			tableFunction,
			createPythonEnvironmentManager(),
			userDefinedFunctionInputType,
			userDefinedFunctionOutputType,
			jobOptions,
			getFlinkMetricContainer());
	}

	@Override
	public void bufferInput(Row input) {
		if (getRuntimeContext().getExecutionConfig().isObjectReuseEnabled()) {
			input = forwardedInputSerializer.copy(input);
		}
		forwardedInputQueue.add(input);
	}

	@Override
	public void emitResults() throws IOException {
		Row input = null;
		byte[] rawUdtfResult;
		boolean lastIsFinishResult = true;
		while ((rawUdtfResult = userDefinedFunctionResultQueue.poll()) != null) {
			if (input == null) {
				input = forwardedInputQueue.poll();
			}
			boolean isFinishResult = isFinishResult(rawUdtfResult);
			if (isFinishResult && (!lastIsFinishResult || joinType == JoinRelType.INNER)) {
				input = forwardedInputQueue.poll();
			} else if (input != null) {
				if (!isFinishResult) {
					bais.setBuffer(rawUdtfResult, 0, rawUdtfResult.length);
					Row udtfResult = userDefinedFunctionTypeSerializer.deserialize(baisWrapper);
					this.resultCollector.collect(Row.join(input, udtfResult));
				} else {
					Row udtfResult = new Row(userDefinedFunctionOutputType.getFieldCount());
					for (int i = 0; i < udtfResult.getArity(); i++) {
						udtfResult.setField(0, null);
					}
					this.resultCollector.collect(Row.join(input, udtfResult));
					input = forwardedInputQueue.poll();
				}
			}
			lastIsFinishResult = isFinishResult;
		}
	}

	private boolean isFinishResult(byte[] rawUdtfResult) {
		return rawUdtfResult.length == 1 && rawUdtfResult[0] == 0x00;
	}
}
