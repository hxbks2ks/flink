################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
# cython: language_level=3

cimport libc.stdint

from apache_beam.coders.coder_impl cimport StreamCoderImpl, OutputStream, InputStream
from pyflink.fn_execution.fast_operations cimport WrapperFuncInputStream

cdef class WrapperInputElement:
    cdef InputStream input_stream
    cdef list input_field_coders
    cdef unsigned char* input_field_type
    cdef libc.stdint.int32_t input_field_count
    cdef libc.stdint.int32_t input_leading_complete_bytes_num
    cdef libc.stdint.int32_t input_remaining_bits_num
    cdef size_t input_buffer_size

cdef class FlattenRowCoderImpl(StreamCoderImpl):
    cdef list _input_field_coders
    cdef list _output_field_coders
    cdef unsigned char* _input_field_type
    cdef unsigned char* _output_field_type
    cdef libc.stdint.int32_t _input_field_count
    cdef libc.stdint.int32_t _output_field_count
    cdef libc.stdint.int32_t _input_leading_complete_bytes_num
    cdef libc.stdint.int32_t _output_leading_complete_bytes_num
    cdef libc.stdint.int32_t _input_remaining_bits_num
    cdef libc.stdint.int32_t _output_remaining_bits_num
    cdef bint*_null_mask
    cdef unsigned char*_null_byte_search_table
    cdef char* _output_data
    cdef char* _output_row_data
    cdef size_t _output_buffer_size
    cdef size_t _output_row_buffer_size
    cdef size_t _output_pos
    cdef size_t _output_row_pos
    cdef size_t _input_pos
    cdef size_t _input_buffer_size
    cdef char* _input_data
    cdef list row
    cpdef _init_attribute(self)
    cdef _consume_input_data(self, WrapperInputElement wrapper_input_element, size_t size)
    cpdef _write_null_mask(self, value)
    cdef _read_null_mask(self)
    cdef _copy_before_data(self, WrapperFuncInputStream wrapper_stream, OutputStream out_stream)
    cdef _copy_after_data(self, OutputStream out_stream)
    cpdef _dump_field(self, unsigned char field_type, CoderType field_coder, item)
    cdef _dump_row(self)
    cdef _dump_byte(self, unsigned char val)
    cdef _dump_smallint(self, libc.stdint.int16_t v)
    cdef _dump_int(self, libc.stdint.int32_t v)
    cdef _dump_bigint(self, libc.stdint.int64_t v)
    cdef _dump_float(self, float v)
    cdef _dump_double(self, double v)
    cdef _dump_bytes(self, char*b)
    cpdef _load_row(self)
    cpdef _load_field(self, unsigned char field_type, CoderType field_coder)
    cdef unsigned char _load_byte(self) except? -1
    cdef libc.stdint.int16_t _load_smallint(self) except? -1
    cdef libc.stdint.int32_t _load_int(self) except? -1
    cdef libc.stdint.int64_t _load_bigint(self) except? -1
    cdef float _load_float(self) except? -1
    cdef double _load_double(self) except? -1
    cdef bytes _load_bytes(self)

cdef class PassThroughLengthPrefixCoderImpl(StreamCoderImpl):
    cdef StreamCoderImpl _value_coder

# cdef class RowCoderImpl(FlattenRowCoderImpl):
#     pass

cdef class TableFunctionRowCoderImpl(StreamCoderImpl):
    cdef FlattenRowCoderImpl _flatten_row_coder
    cdef libc.stdint.int32_t _field_count

cdef class CoderType:
    cpdef unsigned char value(self)

cdef class ArrayCoderImpl(CoderType):
    cdef readonly CoderType elem_coder

cdef class MapCoderImpl(CoderType):
    cdef readonly CoderType key_coder
    cdef readonly CoderType value_coder

cdef class TinyIntCoderImpl(CoderType):
    pass

cdef class SmallIntCoderImpl(CoderType):
    pass

cdef class IntCoderImpl(CoderType):
    pass

cdef class BigIntCoderImpl(CoderType):
    pass

cdef class BooleanCoderImpl(CoderType):
    pass

cdef class FloatCoderImpl(CoderType):
    pass

cdef class DoubleCoderImpl(CoderType):
    pass

cdef class DecimalCoderImpl(CoderType):
    cdef readonly object context
    cdef readonly object scale_format

cdef class BinaryCoderImpl(CoderType):
    pass

cdef class CharCoderImpl(CoderType):
    pass

cdef class DateCoderImpl(CoderType):
    pass

cdef class TimeCoderImpl(CoderType):
    pass

cdef class TimestampCoderImpl(CoderType):
    cdef readonly bint is_compact
