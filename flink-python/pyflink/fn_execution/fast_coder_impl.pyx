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
# cython: language_level = 3
# cython: infer_types = True
# cython: profile=True

cimport libc.stdlib
from libc.string cimport strlen

import datetime
import decimal
from pyflink.table import Row

cdef class WrapperInputElement:
    def __cinit__(self, input_stream):
        self.input_stream = input_stream

cdef class FlattenRowCoderImpl(StreamCoderImpl):
    def __cinit__(self, field_coders):
        self._output_field_coders = field_coders
        self._output_field_count = len(self._output_field_coders)
        self._output_field_type = <unsigned char*> libc.stdlib.malloc(
            self._output_field_count * sizeof(unsigned char))
        self._output_leading_complete_bytes_num = self._output_field_count // 8
        self._output_remaining_bits_num = self._output_field_count % 8
        self._output_row_buffer_size = 1024
        self._output_row_pos = 0
        self._output_row_data = <char*> libc.stdlib.malloc(self._output_row_buffer_size)
        self._null_byte_search_table = <unsigned char*> libc.stdlib.malloc(
            8 * sizeof(unsigned char))
        self._init_attribute()

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        cdef WrapperInputElement wrapper_input_element
        wrapper_input_element = WrapperInputElement(in_stream)
        self._consume_input_data(wrapper_input_element, in_stream.size())
        return wrapper_input_element

    cpdef encode_to_stream(self, wrapper_stream, OutputStream out_stream, bint nested):
        cdef libc.stdint.int32_t i
        cdef list result
        self._copy_before_data(wrapper_stream, out_stream)
        self.row = [None for _ in range(self._input_field_count)]
        func = wrapper_stream.func
        while self._input_buffer_size > self._input_pos:
            self._load_row()
            result = func(self.row)
            self._write_null_mask(result)
            for i in range(self._output_field_count):
                item = result[i]
                if item is not None:
                    self._dump_field(self._output_field_type[i], self._output_field_coders[i], item)
            self._dump_row()
        self._copy_after_data(out_stream)

    cpdef _init_attribute(self):
        self._null_byte_search_table[0] = 0x80
        self._null_byte_search_table[1] = 0x40
        self._null_byte_search_table[2] = 0x20
        self._null_byte_search_table[3] = 0x10
        self._null_byte_search_table[4] = 0x08
        self._null_byte_search_table[5] = 0x04
        self._null_byte_search_table[6] = 0x02
        self._null_byte_search_table[7] = 0x01
        for i in range(self._output_field_count):
            self._output_field_type[i] = self._output_field_coders[i].value()

    cdef _consume_input_data(self, WrapperInputElement wrapper_input_element, size_t size):
        wrapper_input_element.input_field_coders = self._output_field_coders
        wrapper_input_element.input_remaining_bits_num = self._output_remaining_bits_num
        wrapper_input_element.input_leading_complete_bytes_num = \
            self._output_leading_complete_bytes_num
        wrapper_input_element.input_field_count = self._output_field_count
        wrapper_input_element.input_field_type = self._output_field_type
        wrapper_input_element.input_stream.pos = size
        wrapper_input_element.input_buffer_size = size

    cdef _read_null_mask(self):
        cdef libc.stdint.int32_t field_pos
        cdef unsigned char b
        field_pos = 0
        null_byte_search_table = self._null_byte_search_table
        for _ in range(self._input_leading_complete_bytes_num):
            b = self._load_byte()
            for i in range(8):
                self._null_mask[field_pos] = (b & null_byte_search_table[i]) > 0
                field_pos += 1

        if self._input_remaining_bits_num:
            b = self._load_byte()
            for i in range(self._input_remaining_bits_num):
                self._null_mask[field_pos] = (b & null_byte_search_table[i]) > 0
                field_pos += 1

    cpdef _load_row(self):
        cdef libc.stdint.int32_t i
        cdef unsigned char byte
        byte = self._load_byte()
        while byte & 0x80:
            byte = self._load_byte()
        self._read_null_mask()
        for i in range(self._input_field_count):
            if self._null_mask[i]:
                self.row[i] = None
            else:
                self.row[i] = self._load_field(self._input_field_type[i],
                                               self._input_field_coders[i])

    cpdef _load_field(self, unsigned char field_type, CoderType field_coder):
        cdef libc.stdint.int32_t value, nanoseconds, microseconds, minutes, seconds, hours, length
        cdef libc.stdint.int64_t milliseconds
        cdef CoderType value_coder, key_coder
        cdef unsigned char value_type, key_type

        if field_type == 0:
            # tinyint
            return self._load_byte()
        elif field_type == 1:
            # smallint
            return self._load_smallint()
        elif field_type == 2:
            # int
            return self._load_int()
        elif field_type == 3:
            # bigint
            return self._load_bigint()
        elif field_type == 4:
            # boolean
            return not not self._load_byte()
        elif field_type == 5:
            # float
            return self._load_float()
        elif field_type == 6:
            # double
            return self._load_double()
        elif field_type == 7:
            # bytes
            return self._load_bytes()
        elif field_type == 8:
            # str
            return self._load_bytes().decode("utf-8")
        elif field_type == 9:
            # decimal
            user_context = decimal.getcontext()
            decimal.setcontext((<DecimalCoderImpl>field_coder).context)
            result = decimal.Decimal((self._load_bytes()).decode("utf-8")).quantize(
                (<DecimalCoderImpl> field_coder).scale_format)
            decimal.setcontext(user_context)
            return result
        elif field_type == 10:
            # Date
            return datetime.date.fromordinal(self._load_int() + 719163)
        elif field_type == 11:
            # Time
            value = self._load_int()
            seconds = value // 1000
            milliseconds = value % 1000
            minutes = seconds // 60
            seconds %= 60
            hours = minutes // 60
            minutes %= 60
            return datetime.time(hours, minutes, seconds, milliseconds * 1000)
        elif field_type == 12:
            # Timestamp
            if (<TimeCoderImpl>field_coder).is_compact:
                milliseconds = self._load_bigint()
                nanoseconds = 0
            else:
                milliseconds = self._load_bigint()
                nanoseconds = self._load_int()
            seconds, microseconds = (milliseconds // 1000,
                                     milliseconds % 1000 * 1000 + nanoseconds // 1000)
            return datetime.datetime.utcfromtimestamp(seconds).replace(microsecond=microseconds)
        elif field_type == 13:
            # Array
            length = self._load_int()
            value_coder = (<ArrayCoderImpl> field_coder).elem_coder
            value_type = value_coder.value()
            return [self._load_field(value_type, value_coder) if self._load_byte()
                    else None for _ in range(length)]
        elif field_type == 14:
            # Map
            key_coder = (<MapCoderImpl> field_coder).key_coder
            key_type = key_coder.value()
            value_coder = (<MapCoderImpl> field_coder).value_coder
            value_type = value_coder.value()
            length = self._load_int()
            map_value = {}
            for _ in range(length):
                key = self._load_field(key_type, key_coder)
                if self._load_byte():
                    map_value[key] = None
                else:
                    map_value[key] = self._load_field(value_type, value_coder)
            return map_value

    cdef unsigned char _load_byte(self) except? -1:
        self._input_pos += 1
        return <unsigned char> self._input_data[self._input_pos - 1]

    cdef libc.stdint.int16_t _load_smallint(self) except? -1:
        self._input_pos += 2
        return (<unsigned char> self._input_data[self._input_pos - 1]
                | <libc.stdint.uint32_t> <unsigned char> self._input_data[self._input_pos - 2] << 8)

    cdef libc.stdint.int32_t _load_int(self) except? -1:
        self._input_pos += 4
        return (<unsigned char> self._input_data[self._input_pos - 1]
                | <libc.stdint.uint32_t> <unsigned char> self._input_data[self._input_pos - 2] << 8
                | <libc.stdint.uint32_t> <unsigned char> self._input_data[self._input_pos - 3] << 16
                | <libc.stdint.uint32_t> <unsigned char> self._input_data[self._input_pos - 4] << 24)

    cdef libc.stdint.int64_t _load_bigint(self) except? -1:
        self._input_pos += 8
        return (<unsigned char> self._input_data[self._input_pos - 1]
                | <libc.stdint.uint64_t> <unsigned char> self._input_data[self._input_pos - 2] << 8
                | <libc.stdint.uint64_t> <unsigned char> self._input_data[self._input_pos - 3] << 16
                | <libc.stdint.uint64_t> <unsigned char> self._input_data[self._input_pos - 4] << 24
                | <libc.stdint.uint64_t> <unsigned char> self._input_data[self._input_pos - 5] << 32
                | <libc.stdint.uint64_t> <unsigned char> self._input_data[self._input_pos - 6] << 40
                | <libc.stdint.uint64_t> <unsigned char> self._input_data[self._input_pos - 7] << 48
                | <libc.stdint.uint64_t> <unsigned char> self._input_data[self._input_pos - 8] << 56)

    cdef float _load_float(self) except? -1:
        cdef libc.stdint.int32_t as_long = self._load_int()
        return (<float*> <char*> &as_long)[0]

    cdef double _load_double(self) except? -1:
        cdef libc.stdint.int64_t as_long = self._load_bigint()
        return (<double*> <char*> &as_long)[0]

    cdef bytes _load_bytes(self):
        cdef libc.stdint.int32_t size = self._load_int()
        self._input_pos += size
        return self._input_data[self._input_pos - size: self._input_pos]

    cdef _copy_before_data(self, WrapperFuncInputStream wrapper_stream, OutputStream out_stream):
        self._output_data = out_stream.data
        self._output_pos = out_stream.pos
        self._output_buffer_size = out_stream.buffer_size

        self._input_data = wrapper_stream.wrapper_input_element.input_stream.allc
        self._input_buffer_size = wrapper_stream.wrapper_input_element.input_buffer_size
        self._input_field_count = wrapper_stream.wrapper_input_element.input_field_count
        self._input_leading_complete_bytes_num = wrapper_stream.wrapper_input_element.input_leading_complete_bytes_num
        self._input_remaining_bits_num = wrapper_stream.wrapper_input_element.input_remaining_bits_num
        self._input_field_type = wrapper_stream.wrapper_input_element.input_field_type
        self._input_field_coders = wrapper_stream.wrapper_input_element.input_field_coders
        self._null_mask = <bint*> libc.stdlib.malloc(self._input_field_count * sizeof(bint))
        self._input_pos = 0

    cdef _copy_after_data(self, OutputStream out_stream):
        out_stream.data = self._output_data
        out_stream.pos = self._output_pos
        out_stream.buffer_size = self._output_buffer_size

    cpdef _dump_field(self, unsigned char field_type, CoderType field_coder, item):
        cdef libc.stdint.int32_t hour, minute, seconds, microsecond, milliseconds, nanoseconds, \
            microseconds_of_second, length
        cdef libc.stdint.int64_t timestamp_milliseconds, timestamp_seconds
        cdef CoderType value_coder, key_coder
        cdef unsigned char value_type, key_type
        if field_type == 0:
            # tinyint
            self._dump_byte(item)
        elif field_type == 1:
            # smallint
            self._dump_smallint(item)
        elif field_type == 2:
            # int
            self._dump_int(item)
        elif field_type == 3:
            # bigint
            self._dump_bigint(item)
        elif field_type == 4:
            # boolean
            self._dump_byte(item)
        elif field_type == 5:
            # float
            self._dump_float(item)
        elif field_type == 6:
            # double
            self._dump_double(item)
        elif field_type == 7:
            # bytes
            self._dump_bytes(item)
        elif field_type == 8:
            # str
            self._dump_bytes(item.encode('utf-8'))
        elif field_type == 9:
            # decimal
            user_context = decimal.getcontext()
            decimal.setcontext((<DecimalCoderImpl>field_coder).context)
            bytes_value = str(item.quantize((<DecimalCoderImpl>field_coder).scale_format)).encode("utf-8")
            self._dump_bytes(bytes_value)
            decimal.setcontext(user_context)
        elif field_type == 10:
            # Date
            self._dump_int(item.toordinal() - 719163)
        elif field_type == 11:
            # Time
            hour = item.hour
            minute = item.minute
            seconds = item.second
            microsecond = item.microsecond
            milliseconds = hour * 3600000 + minute * 60000 + seconds * 1000 + microsecond // 1000
            self._dump_int(milliseconds)
        elif field_type == 12:
            # Timestamp
            timestamp_seconds = <libc.stdint.int64_t> (
                item.replace(tzinfo=datetime.timezone.utc).timestamp())
            microseconds_of_second = item.microsecond
            timestamp_milliseconds = timestamp_seconds * 1000 + microseconds_of_second // 1000
            nanoseconds = microseconds_of_second % 1000 * 1000
            if (<TimestampCoderImpl>field_coder).is_compact:
                self._dump_bigint(timestamp_milliseconds)
            else:
                self._dump_bigint(timestamp_milliseconds)
                self._dump_int(nanoseconds)
        elif field_type == 13:
            # Array
            length = len(item)
            value_coder = (<ArrayCoderImpl> field_coder).elem_coder
            value_type = value_coder.value()
            self._dump_int(length)
            for i in range(length):
                value = item[i]
                if value is None:
                    self._dump_byte(False)
                else:
                    self._dump_byte(True)
                    self._dump_field(value_type, value_coder, value)
        elif field_type == 14:
            # Map
            length = len(item)
            self._dump_int(length)
            iter_items = item.items()
            key_coder = (<MapCoderImpl> field_coder).key_coder
            key_type = key_coder.value()
            value_coder = (<MapCoderImpl> field_coder).value_coder
            value_type = value_coder.value()
            for iter_item in iter_items:
                key = iter_item[0]
                value = iter_item[1]
                self._dump_field(key_type, key_coder, key)
                if value is None:
                    self._dump_byte(True)
                else:
                    self._dump_byte(False)
                    self._dump_field(value_type, value_coder, value)

    cdef _dump_row(self):
        cdef size_t size
        cdef size_t i
        cdef bint is_realloc
        cdef char bits
        if self._output_buffer_size < self._output_pos + self._output_row_pos + 9:
            self._output_buffer_size += self._output_row_buffer_size + 9
            self._output_data = <char*> libc.stdlib.realloc(self._output_data, self._output_buffer_size)
        size = self._output_row_pos
        while size:
            bits = size & 0x7F
            size >>= 7
            if size:
                bits |= 0x80
            self._output_data[self._output_pos] = bits
            self._output_pos += 1
        if self._output_row_pos < 8:
            for i in range(self._output_row_pos):
                self._output_data[self._output_pos + i] = self._output_row_data[i]
        else:
            libc.string.memcpy(self._output_data + self._output_pos, self._output_row_data, self._output_row_pos)
        self._output_pos += self._output_row_pos
        self._output_row_pos = 0

    cdef _dump_byte(self, unsigned char val):
        if self._output_row_buffer_size < self._output_row_pos + 1:
            self._output_row_buffer_size *= 2
            self._output_row_data = <char*> libc.stdlib.realloc(self._output_row_data, self._output_row_buffer_size)
        self._output_row_data[self._output_row_pos] = val
        self._output_row_pos += 1

    cdef _dump_smallint(self, libc.stdint.int16_t v):
        if self._output_row_buffer_size < self._output_row_pos + 2:
            self._output_row_buffer_size *= 2
            self._output_row_data = <char*> libc.stdlib.realloc(self._output_row_data, self._output_row_buffer_size)
        self._output_row_data[self._output_row_pos] = <unsigned char> (v >> 8)
        self._output_row_data[self._output_row_pos + 1] = <unsigned char> (v)
        self._output_row_pos += 2

    cdef _dump_int(self, libc.stdint.int32_t v):
        if self._output_row_buffer_size < self._output_row_pos + 4:
            self._output_row_buffer_size *= 2
            self._output_row_data = <char*> libc.stdlib.realloc(self._output_row_data, self._output_row_buffer_size)
        self._output_row_data[self._output_row_pos] = <unsigned char> (v >> 24)
        self._output_row_data[self._output_row_pos + 1] = <unsigned char> (v >> 16)
        self._output_row_data[self._output_row_pos + 2] = <unsigned char> (v >> 8)
        self._output_row_data[self._output_row_pos + 3] = <unsigned char> (v)
        self._output_row_pos += 4

    cdef _dump_bigint(self, libc.stdint.int64_t v):
        if self._output_row_buffer_size < self._output_row_pos + 8:
            self._output_row_buffer_size *= 2
            self._output_row_data = <char*> libc.stdlib.realloc(self._output_row_data, self._output_row_buffer_size)
        self._output_row_data[self._output_row_pos] = <unsigned char> (v >> 56)
        self._output_row_data[self._output_row_pos + 1] = <unsigned char> (v >> 48)
        self._output_row_data[self._output_row_pos + 2] = <unsigned char> (v >> 40)
        self._output_row_data[self._output_row_pos + 3] = <unsigned char> (v >> 32)
        self._output_row_data[self._output_row_pos + 4] = <unsigned char> (v >> 24)
        self._output_row_data[self._output_row_pos + 5] = <unsigned char> (v >> 16)
        self._output_row_data[self._output_row_pos + 6] = <unsigned char> (v >> 8)
        self._output_row_data[self._output_row_pos + 7] = <unsigned char> (v)
        self._output_row_pos += 8

    cdef _dump_float(self, float v):
        self._dump_int((<libc.stdint.int32_t*> <char*> &v)[0])

    cdef _dump_double(self, double v):
        self._dump_bigint((<libc.stdint.int64_t*> <char*> &v)[0])

    cdef _dump_bytes(self, char*b):
        cdef libc.stdint.int32_t length = strlen(b)
        self._dump_int(length)
        if self._output_row_buffer_size < self._output_row_pos + length:
            self._output_row_buffer_size *= 2
            self._output_row_data = <char*> libc.stdlib.realloc(self._output_row_data, self._output_row_buffer_size)
        if length < 8:
            for i in range(length):
                self._output_row_data[self._output_row_pos + i] = b[i]
        else:
            libc.string.memcpy(self._output_row_data + self._output_row_pos, b, length)
        self._output_row_pos += length

    cpdef _write_null_mask(self, value):
        cdef libc.stdint.int32_t field_pos
        cdef libc.stdint.int32_t remaining_bits_num
        cdef libc.stdint.int32_t leading_complete_bytes_num
        cdef unsigned char*null_byte_search_table
        cdef unsigned char b
        field_pos = 0
        remaining_bits_num = self._output_remaining_bits_num
        leading_complete_bytes_num = self._output_leading_complete_bytes_num
        null_byte_search_table = self._null_byte_search_table
        for _ in range(leading_complete_bytes_num):
            b = 0x00
            for i in range(8):
                if value[field_pos + i] is None:
                    b |= null_byte_search_table[i]
            field_pos += 8
            self._dump_byte(b)

        if remaining_bits_num:
            b = 0x00
            for i in range(remaining_bits_num):
                if value[field_pos + i] is None:
                    b |= null_byte_search_table[i]
            self._dump_byte(b)

    def __dealloc__(self):
        if self.null_mask:
            libc.stdlib.free(self._null_mask)
        if self.null_byte_search_table:
            libc.stdlib.free(self._null_byte_search_table)
        if self._output_row_data:
            libc.stdlib.free(self._output_row_data)

cdef class PassThroughLengthPrefixCoderImpl(StreamCoderImpl):
    def __cinit__(self, value_coder):
        self._value_coder = value_coder

    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        self._value_coder.encode_to_stream(value, out_stream, False)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        return self._value_coder.decode_from_stream(in_stream, nested)

    cpdef get_estimated_size_and_observables(self, value, bint nested=False):
        return 0, []

# cdef class RowCoderImpl(FlattenRowCoderImpl):
#     cpdef decode_from_stream(self, InputStream in_stream, bint nested):
#         return Row(*super(RowCoderImpl, self).decode_from_stream(in_stream, nested))

cdef class TableFunctionRowCoderImpl(StreamCoderImpl):
    def __cinit__(self, flatten_row_coder):
        self._flatten_row_coder = flatten_row_coder
        self._field_count = flatten_row_coder._field_count

    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        if value is None:
            out_stream.write_byte(0x00)
        else:
            self._flatten_row_coder.encode_to_stream(value if self._field_count != 1 else (value,),
                                                     out_stream, nested)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        return self._flatten_row_coder.decode_from_stream(in_stream, nested)

cdef class CoderType:
    cpdef unsigned char value(self):
        return -1

cdef class ArrayCoderImpl(CoderType):
    def __cinit__(self, elem_coder):
        self.elem_coder = elem_coder

    cpdef unsigned char value(self):
        return 13

cdef class MapCoderImpl(CoderType):
    def __cinit__(self, key_coder, value_coder):
        self.key_coder = key_coder
        self.value_coder = value_coder

    cpdef unsigned char value(self):
        return 14

cdef class TinyIntCoderImpl(CoderType):
    cpdef unsigned char value(self):
        return 0

cdef class SmallIntCoderImpl(CoderType):
    cpdef unsigned char value(self):
        return 1

cdef class IntCoderImpl(CoderType):
    cpdef unsigned char value(self):
        return 2

cdef class BigIntCoderImpl(CoderType):
    cpdef unsigned char value(self):
        return 3

cdef class BooleanCoderImpl(CoderType):
    cpdef unsigned char value(self):
        return 4

cdef class FloatCoderImpl(CoderType):
    cpdef unsigned char value(self):
        return 5

cdef class DoubleCoderImpl(CoderType):
    cpdef unsigned char value(self):
        return 6

cdef class BinaryCoderImpl(CoderType):
    cpdef unsigned char value(self):
        return 7

cdef class CharCoderImpl(CoderType):
    cpdef unsigned char value(self):
        return 8

cdef class DecimalCoderImpl(CoderType):
    def __cinit__(self, precision, scale):
        self.context = decimal.Context(prec=precision)
        self.scale_format = decimal.Decimal(10) ** -scale

    cpdef unsigned char value(self):
        return 9

cdef class DateCoderImpl(CoderType):
    cpdef unsigned char value(self):
        return 10

cdef class TimeCoderImpl(CoderType):
    cpdef unsigned char value(self):
        return 11

cdef class TimestampCoderImpl(CoderType):
    def __cinit__(self, precision):
        self.is_compact = precision <= 3

    cpdef unsigned char value(self):
        return 12
