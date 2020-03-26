#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# cython: language_level = 3
# cython: infer_types = True
# cython: profile=True

import datetime

import cloudpickle
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker import operation_specs
from pyflink.table.udf import DelegatingScalarFunction

from pyflink.fn_execution import flink_fn_execution_pb2
from pyflink.serializers import PickleSerializer

SCALAR_FUNCTION_URN = "flink:transform:scalar_function:v1"
TABLE_FUNCTION_URN = "flink:transform:table_function:v1"

cdef class WrapperFuncInputStream:
    def __cinit__(self, func, wrapper_input_element):
        self.func = func
        self.wrapper_input_element = wrapper_input_element


cdef class StatelessFunctionOperation(Operation):
    """
    Base class of stateless function operation that will execute ScalarFunction or TableFunction for
    each input element.
    """

    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(StatelessFunctionOperation, self).__init__(name, spec, counter_factory, sampler)
        self.consumer = consumers['output'][0]
        self._value_coder_impl = self.consumer.windowed_coder.wrapped_value_coder.get_impl()

        self.variable_dict = {}
        self.user_defined_funcs = []
        self._func_num = 0
        self._constant_num = 0
        self.func = self.generate_func(self.spec.serialized_fn)
        for user_defined_func in self.user_defined_funcs:
            user_defined_func.open(None)

    cpdef start(self):
        with self.scoped_start_state:
            super(StatelessFunctionOperation, self).start()

    cpdef finish(self):
        super(StatelessFunctionOperation, self).finish()

    cpdef teardown(self):
        for user_defined_func in self.user_defined_funcs:
            user_defined_func.close(None)

    cpdef generate_func(self, udfs):
        pass

    cpdef str _extract_user_defined_function(self, user_defined_function_proto):
        """
        Extracts user-defined-function from the proto representation of a
        :class:`UserDefinedFunction`.

        :param user_defined_function_proto: the proto representation of the Python
        :class:`UserDefinedFunction`
        """
        user_defined_func = cloudpickle.loads(user_defined_function_proto.payload)
        func_name = 'f%s' % self._func_num
        self._func_num += 1
        if isinstance(user_defined_func, DelegatingScalarFunction):
            self.variable_dict[func_name] = user_defined_func.func
        else:
            self.variable_dict[func_name] = user_defined_func.eval
        self.user_defined_funcs.append(user_defined_func)
        func_args = self._extract_user_defined_function_args(user_defined_function_proto.inputs)
        return "%s(%s)" % (func_name, func_args)

    cpdef str _extract_user_defined_function_args(self, args):
        args_str = []
        for arg in args:
            if arg.HasField("udf"):
                # for chaining Python UDF input: the input argument is a Python ScalarFunction
                args_str.append(self._extract_user_defined_function(arg.udf))
            elif arg.HasField("inputOffset"):
                # the input argument is a column of the input row
                args_str.append("value[%s]" % arg.inputOffset)
            else:
                # the input argument is a constant value
                args_str.append(self._parse_constant_value(arg.inputConstant))
        return ",".join(args_str)

    cpdef str _parse_constant_value(self, constant_value):
        j_type = constant_value[0]
        serializer = PickleSerializer()
        pickled_data = serializer.loads(constant_value[1:])
        # the type set contains
        # TINYINT,SMALLINT,INTEGER,BIGINT,FLOAT,DOUBLE,DECIMAL,CHAR,VARCHAR,NULL,BOOLEAN
        # the pickled_data doesn't need to transfer to anther python object
        if j_type == 0:
            parsed_constant_value = pickled_data
        # the type is DATE
        elif j_type == 1:
            parsed_constant_value = \
                datetime.date(year=1970, month=1, day=1) + datetime.timedelta(days=pickled_data)
        # the type is TIME
        elif j_type == 2:
            seconds, milliseconds = divmod(pickled_data, 1000)
            minutes, seconds = divmod(seconds, 60)
            hours, minutes = divmod(minutes, 60)
            parsed_constant_value = datetime.time(hours, minutes, seconds, milliseconds * 1000)
        # the type is TIMESTAMP
        elif j_type == 3:
            parsed_constant_value = \
                datetime.datetime(year=1970, month=1, day=1, hour=0, minute=0, second=0) \
                + datetime.timedelta(milliseconds=pickled_data)
        else:
            raise Exception("Unknown type %s, should never happen" % str(j_type))

        constant_value_name = 'c%s' % self._constant_num
        self._constant_num += 1
        self.variable_dict[constant_value_name] = parsed_constant_value
        return constant_value_name

cdef class ScalarFunctionOperation(StatelessFunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(ScalarFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers)

    cpdef generate_func(self, udfs):
        """
        Generates a lambda function based on udfs.
        :param udfs: a list of the proto representation of the Python :class:`ScalarFunction`
        :return: the generated lambda function
        """
        scalar_functions = [self._extract_user_defined_function(udf) for udf in udfs]
        # from Cython.Build import Inline
        # return Inline.cython_inline('return lambda value: [%s]' % ','.join(scalar_functions),
        #                             locals=self.variable_dict, globals={})
        return eval('lambda value: [%s]' % ','.join(scalar_functions), self.variable_dict)

    cpdef process(self, WindowedValue o):
        output_stream = self.consumer.output_stream
        cdef WrapperFuncInputStream wrapper
        wrapper = WrapperFuncInputStream(self.func, o.value)
        self._value_coder_impl.encode_to_stream(wrapper, output_stream, True)
        output_stream.maybe_flush()

cdef class TableFunctionOperation(StatelessFunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(TableFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers)

    cpdef generate_func(self, udtfs):
        """
        Generates a lambda function based on udtfs.
        :param udtfs: a list of the proto representation of the Python :class:`TableFunction`
        :return: the generated lambda function
        """
        table_function = self._extract_user_defined_function(udtfs[0])
        return eval('lambda value: %s' % table_function, self.variable_dict)

    cpdef process(self, WindowedValue o):
        output_stream = self.consumer.output_stream
        for result in self._create_result(o.value):
            self._value_coder_impl.encode_to_stream(result, output_stream, True)
        output_stream.maybe_flush()

    def _create_result(self, list value):
        result = self.func(value)
        if result is not None:
            yield from result
        yield None

@bundle_processor.BeamTransformFactory.register_urn(
    SCALAR_FUNCTION_URN, flink_fn_execution_pb2.UserDefinedFunctions)
def create_scalar_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter.udfs, ScalarFunctionOperation)

@bundle_processor.BeamTransformFactory.register_urn(
    TABLE_FUNCTION_URN, flink_fn_execution_pb2.UserDefinedFunctions)
def create_table_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter.udfs, TableFunctionOperation)

def _create_user_defined_function_operation(factory, transform_proto, consumers, udfs_proto,
                                            operation_cls):
    output_tags = list(transform_proto.outputs.keys())
    output_coders = factory.get_output_coders(transform_proto)
    spec = operation_specs.WorkerDoFn(
        serialized_fn=udfs_proto,
        output_tags=output_tags,
        input=None,
        side_inputs=None,
        output_coders=[output_coders[tag] for tag in output_tags])

    return operation_cls(
        transform_proto.unique_name,
        spec,
        factory.counter_factory,
        factory.state_sampler,
        consumers)
