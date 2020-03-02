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

from apache_beam.coders.coder_impl cimport StreamCoderImpl, CoderImpl, OutputStream, InputStream

cdef class FlattenRowCoderImpl(StreamCoderImpl):
    cdef list _field_coders
    cdef readonly libc.stdint.int32_t _filed_count
    cdef libc.stdint.int32_t _leading_complete_bytes_num
    cdef libc.stdint.int32_t _remaining_bits_num
    cdef bint*null_mask
    cdef unsigned char*null_byte_search_table
    cdef OutputStream data_out_stream
    cpdef write_null_mask(self, value, OutputStream out_stream)
    cdef read_null_mask(self, InputStream in_stream)
    cdef _init_null_byte_search_table(self)

cdef class CustomLengthPrefixCoderImpl(StreamCoderImpl):
    cdef StreamCoderImpl _value_coder
    pass

cdef class RowCoderImpl(FlattenRowCoderImpl):
    pass

cdef class TableFunctionRowCoderImpl(StreamCoderImpl):
    cdef FlattenRowCoderImpl _flatten_row_coder
    cdef libc.stdint.int32_t _field_count

cdef class ArrayCoderImpl(StreamCoderImpl):
    cdef CoderImpl _elem_coder

cdef class MapCoderImpl(StreamCoderImpl):
    cdef CoderImpl _key_coder
    cdef CoderImpl _value_coder

cdef class TinyIntCoderImpl(StreamCoderImpl):
    cdef void write_bigendian_int8(self, libc.stdint.int8_t signed_v, OutputStream out_stream)
    cdef libc.stdint.int8_t read_bigendian_int8(self, InputStream in_stream) except? -1

cdef class SmallIntImpl(StreamCoderImpl):
    cdef void write_bigendian_int16(self, libc.stdint.int16_t signed_v, OutputStream out_stream)
    cdef libc.stdint.int16_t read_bigendian_int16(self, InputStream in_stream) except? -1

cdef class IntCoderImpl(StreamCoderImpl):
    pass

cdef class BigIntCoderImpl(StreamCoderImpl):
    pass

cdef class BooleanCoderImpl(StreamCoderImpl):
    pass

cdef class FloatCoderImpl(StreamCoderImpl):
    cdef void write_bigendian_float(self, float d, OutputStream out_stream)
    cdef float read_bigendian_float(self, InputStream in_stream) except? -1

cdef class DoubleCoderImpl(StreamCoderImpl):
    pass

cdef class DecimalCoderImpl(StreamCoderImpl):
    cdef object context
    cdef object scale_format

cdef class BinaryCoderImpl(StreamCoderImpl):
    pass

cdef class CharCoderImpl(StreamCoderImpl):
    pass

cdef class DateCoderImpl(StreamCoderImpl):
    cdef libc.stdint.int32_t EPOCH_ORDINAL  # datetime.datetime(1970, 1, 1).toordinal()
    cdef libc.stdint.int32_t internal_to_date(self, libc.stdint.int32_t value)
    cdef libc.stdint.int32_t date_to_internal(self, libc.stdint.int32_t value)

cdef class TimeCoderImpl(StreamCoderImpl):
    pass

cdef class TimestampCoderImpl(StreamCoderImpl):
    cdef bint is_compact
