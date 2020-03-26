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
# cython: language_level=3

cimport cython
cimport libc.stdint

from apache_beam.utils.windowed_value cimport WindowedValue
from apache_beam.runners.worker.operations cimport Operation
from apache_beam.coders.coder_impl cimport StreamCoderImpl, CoderImpl, OutputStream, InputStream
from pyflink.fn_execution.fast_coder_impl cimport WrapperInputElement

cdef class WrapperFuncInputStream:
    cdef readonly object func
    cdef WrapperInputElement wrapper_input_element

cdef class StatelessFunctionOperation(Operation):
    cdef Operation consumer
    cdef StreamCoderImpl _value_coder_impl
    cdef dict variable_dict
    cdef list user_defined_funcs
    cdef libc.stdint.int32_t _func_num
    cdef libc.stdint.int32_t _constant_num
    cdef object func

    cpdef generate_func(self, udfs)
    @cython.locals(func_args=str, func_name=str)
    cpdef str _extract_user_defined_function(self, user_defined_function_proto)
    @cython.locals(args_str=list)
    cpdef str _extract_user_defined_function_args(self, args)
    @cython.locals(j_type=libc.stdint.int32_t, constant_value_name=str)
    cpdef str _parse_constant_value(self, constant_value)

cdef class ScalarFunctionOperation(StatelessFunctionOperation):
    pass

cdef class TableFunctionOperation(StatelessFunctionOperation):
    pass