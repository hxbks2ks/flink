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

import datetime
import decimal
from pyflink.table import Row

cdef class FlattenRowCoderImpl(StreamCoderImpl):
    def __cinit__(self, field_coders):
        self._field_coders = field_coders
        self._filed_count = len(self._field_coders)
        self.null_mask = <bint*> libc.stdlib.malloc(self._filed_count * sizeof(bint))
        self._leading_complete_bytes_num = self._filed_count // 8
        self._remaining_bits_num = self._filed_count % 8
        self.null_byte_search_table = <unsigned char*> libc.stdlib.malloc(8 * sizeof(unsigned char))
        self._init_null_byte_search_table()
        self.data_out_stream = OutputStream()

    # def encode_data(self, iter_value, out_stream):
    #     for value in iter_value:
    #         self.encode_to_stream(value, out_stream, True)

    cpdef encode_to_stream(self, iter_value, OutputStream out_stream, bint nested):
        cdef libc.stdint.int32_t i
        for value in iter_value:
            self.write_null_mask(value, self.data_out_stream)
            for i in range(self._filed_count):
                item = value[i]
                if item is not None:
                    self._field_coders[i].encode_to_stream(item, self.data_out_stream, nested)
            out_stream.write_var_int64(self.data_out_stream.size())
            out_stream.write(self.data_out_stream.get())
            self.data_out_stream._clear()

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        in_stream.read_var_int64()
        self.read_null_mask(in_stream)
        return [None if self.null_mask[idx] else self._field_coders[idx].decode_from_stream(
            in_stream, nested) for idx in range(self._filed_count)]

    def yield_result(self, in_stream):
        while in_stream.size() > 0:
            yield self.decode_from_stream(in_stream, False)

    cpdef write_null_mask(self, value, OutputStream out_stream):
        cdef libc.stdint.int32_t field_pos
        cdef libc.stdint.int32_t remaining_bits_num
        cdef libc.stdint.int32_t leading_complete_bytes_num
        cdef unsigned char* null_byte_search_table
        cdef unsigned char b
        field_pos = 0
        remaining_bits_num = self._remaining_bits_num
        leading_complete_bytes_num = self._leading_complete_bytes_num
        null_byte_search_table = self.null_byte_search_table
        for _ in range(leading_complete_bytes_num):
            b = 0x00
            for i in range(8):
                if value[field_pos + i] is None:
                    b |= null_byte_search_table[i]
            field_pos += 8
            out_stream.write_byte(b)

        if remaining_bits_num:
            b = 0x00
            for i in range(remaining_bits_num):
                if value[field_pos + i] is None:
                    b |= null_byte_search_table[i]
            out_stream.write_byte(b)

    cdef read_null_mask(self, InputStream in_stream):
        cdef libc.stdint.int32_t field_pos
        cdef libc.stdint.int32_t num_pos
        cdef libc.stdint.int32_t byte_pos
        cdef libc.stdint.int32_t null_mask_pos
        cdef unsigned char b
        field_pos = 0
        null_byte_search_table = self.null_byte_search_table
        for _ in range(self._leading_complete_bytes_num):
            b = in_stream.read_byte()
            for i in range(8):
                self.null_mask[field_pos] = (b & null_byte_search_table[i]) > 0
                field_pos +=1

        if self._remaining_bits_num:
            b = in_stream.read_byte()
            for i in range(self._remaining_bits_num):
                self.null_mask[field_pos] = (b & null_byte_search_table[i]) > 0
                field_pos +=1

    cdef _init_null_byte_search_table(self):
        self.null_byte_search_table[0] = 0x80
        self.null_byte_search_table[1] = 0x40
        self.null_byte_search_table[2] = 0x20
        self.null_byte_search_table[3] = 0x10
        self.null_byte_search_table[4] = 0x08
        self.null_byte_search_table[5] = 0x04
        self.null_byte_search_table[6] = 0x02
        self.null_byte_search_table[7] = 0x01

    def __dealloc__(self):
        if self.null_mask:
            libc.stdlib.free(self.null_mask)
        if self.null_byte_search_table:
            libc.stdlib.free(self.null_byte_search_table)

cdef class CustomLengthPrefixCoderImpl(StreamCoderImpl):
    def __cinit__(self, value_coder):
        self._value_coder = value_coder

    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        # self._value_coder.encode_data(value, out_stream)
        self._value_coder.encode_to_stream(value, out_stream, False)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        return self._value_coder.yield_result(in_stream)

    cpdef get_estimated_size_and_observables(self, value, bint nested=False):
        return 0, []

    cpdef estimate_size(self, value, bint nested=False):
        return 0


cdef class RowCoderImpl(FlattenRowCoderImpl):
    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        return Row(*super(RowCoderImpl, self).decode_from_stream(in_stream, nested))


cdef class TableFunctionRowCoderImpl(StreamCoderImpl):

    def __cinit__(self, flatten_row_coder):
        self._flatten_row_coder = flatten_row_coder
        self._field_count = flatten_row_coder._filed_count

    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        if value is None:
            out_stream.write_byte(0x00)
        else:
            self._flatten_row_coder.encode_to_stream(value if self._field_count != 1 else (value,),
                                                     out_stream, nested)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        return self._flatten_row_coder.decode_from_stream(in_stream, nested)

cdef class ArrayCoderImpl(StreamCoderImpl):
    def __cinit__(self, elem_coder):
        self._elem_coder = elem_coder

    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        cdef libc.stdint.int32_t size
        size = len(value)
        out_stream.write_bigendian_int32(size)
        for i in range(size):
            elem = value[i]
            if elem is None:
                out_stream.write_byte(False)
            else:
                out_stream.write_byte(True)
                self._elem_coder.encode_to_stream(elem, out_stream, nested)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        cdef libc.stdint.int32_t size
        size = in_stream.read_bigendian_int32()
        elements = [self._elem_coder.decode_from_stream(in_stream, nested)
                    if in_stream.read_byte() else None for _ in range(size)]
        return elements

cdef class MapCoderImpl(StreamCoderImpl):
    def __cinit__(self, key_coder, value_coder):
        self._key_coder = key_coder
        self._value_coder = value_coder

    cpdef encode_to_stream(self, map_value, OutputStream out_stream, bint nested):
        cdef libc.stdint.int32_t size
        size = len(map_value)
        out_stream.write_bigendian_int32(size)
        iter_items = map_value.items()
        for iter_item in iter_items:
            key = iter_item[0]
            value = iter_item[1]
            self._key_coder.encode_to_stream(key, out_stream, nested)
            if value is None:
                out_stream.write_byte(True)
            else:
                out_stream.write_byte(False)
                self._value_coder.encode_to_stream(value, out_stream, nested)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        cdef libc.stdint.int32_t size
        size = in_stream.read_bigendian_int32()
        map_value = {}
        for _ in range(size):
            key = self._key_coder.decode_from_stream(in_stream, nested)
            if in_stream.read_byte():
                map_value[key] = None
            else:
                value = self._value_coder.decode_from_stream(in_stream, nested)
                map_value[key] = value
        return map_value

cdef class TinyIntCoderImpl(StreamCoderImpl):
    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        self.write_bigendian_int8(value, out_stream)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        return self.read_bigendian_int8(in_stream)

    cdef void write_bigendian_int8(self, libc.stdint.int8_t signed_v, OutputStream out_stream):
        cdef libc.stdint.uint8_t v = signed_v
        while out_stream.buffer_size < out_stream.pos + 1:
            out_stream.buffer_size *= 2
        out_stream.data[out_stream.pos] = <unsigned char> v
        out_stream.pos += 1

    cdef libc.stdint.int8_t read_bigendian_int8(self, InputStream in_stream) except? -1:
        in_stream.pos += 1
        return <unsigned char> in_stream.allc[in_stream.pos - 1]

cdef class SmallIntImpl(StreamCoderImpl):
    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        self.write_bigendian_int16(value, out_stream)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        return self.read_bigendian_int16(in_stream)

    cdef void write_bigendian_int16(self, libc.stdint.int16_t signed_v, OutputStream out_stream):
        cdef libc.stdint.uint16_t v = signed_v
        while out_stream.buffer_size < out_stream.pos + 2:
            out_stream.buffer_size *= 2
        out_stream.data[out_stream.pos] = <unsigned char> (v >> 8)
        out_stream.data[out_stream.pos + 1] = <unsigned char> v
        out_stream.pos += 2

    cdef libc.stdint.int16_t read_bigendian_int16(self, InputStream in_stream) except? -1:
        in_stream.pos += 2
        return (<unsigned char> in_stream.allc[in_stream.pos - 1]
                | <libc.stdint.uint16_t> <unsigned char> in_stream.allc[in_stream.pos - 2] << 8)

cdef class IntCoderImpl(StreamCoderImpl):
    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        out_stream.write_bigendian_int32(value)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        return in_stream.read_bigendian_int32()

cdef class BigIntCoderImpl:
    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        out_stream.write_bigendian_int64(value)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        return in_stream.read_bigendian_int64()

cdef class BooleanCoderImpl(StreamCoderImpl):
    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        out_stream.write_byte(value)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        return in_stream.read_byte() > 0

cdef class FloatCoderImpl(StreamCoderImpl):
    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        self.write_bigendian_float(value, out_stream)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        return self.read_bigendian_float(in_stream)

    cdef void write_bigendian_float(self, float d, OutputStream out_stream):
        out_stream.write_bigendian_int32((<libc.stdint.int32_t*> <char*> &d)[0])

    cdef float read_bigendian_float(self, InputStream in_stream) except? -1:
        cdef libc.stdint.int32_t as_long = in_stream.read_bigendian_int32()
        return (<float*> <char*> &as_long)[0]

cdef class DoubleCoderImpl(StreamCoderImpl):
    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        out_stream.write_bigendian_double(value)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        return in_stream.read_bigendian_double()

cdef class DecimalCoderImpl(StreamCoderImpl):
    def __cinit__(self, precision, scale):
        self.context = decimal.Context(prec=precision)
        self.scale_format = decimal.Decimal(10) ** -scale

    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        cdef bytes bytes_value
        user_context = decimal.getcontext()
        decimal.setcontext(self.context)
        value = value.quantize(self.scale_format)
        bytes_value = str(value).encode("utf-8")
        out_stream.write_bigendian_int32(len(bytes_value))
        out_stream.write(bytes_value, False)
        decimal.setcontext(user_context)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        cdef libc.stdint.int32_t size
        user_context = decimal.getcontext()
        decimal.setcontext(self.context)
        size = in_stream.read_bigendian_int32()
        value = decimal.Decimal(in_stream.read(size).decode("utf-8")).quantize(self.scale_format)
        decimal.setcontext(user_context)
        return value

cdef class BinaryCoderImpl(StreamCoderImpl):
    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        out_stream.write_bigendian_int32(len(value))
        out_stream.write(value, False)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        cdef libc.stdint.int32_t size
        size = in_stream.read_bigendian_int32()
        return in_stream.read(size)

cdef class CharCoderImpl(StreamCoderImpl):
    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        cdef bytes py_byte_string = value.encode("utf-8")
        out_stream.write_bigendian_int32(len(py_byte_string))
        out_stream.write(py_byte_string, False)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        cdef libc.stdint.int32_t size
        size = in_stream.read_bigendian_int32()
        return in_stream.read(size).decode("utf-8")

cdef class DateCoderImpl(StreamCoderImpl):
    def __cinit__(self):
        self.EPOCH_ORDINAL = 719163

    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        out_stream.write_bigendian_int32(self.date_to_internal(value.toordinal()))

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        return datetime.date.fromordinal(self.internal_to_date(in_stream.read_bigendian_int32()))

    cdef libc.stdint.int32_t date_to_internal(self, libc.stdint.int32_t d):
        return d - self.EPOCH_ORDINAL

    cdef libc.stdint.int32_t internal_to_date(self, libc.stdint.int32_t v):
        return v + self.EPOCH_ORDINAL

cdef class TimeCoderImpl(StreamCoderImpl):
    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        cdef libc.stdint.int32_t hour, minute, second, microsecond
        hour = value.hour
        minute = value.minute
        second = value.second
        microsecond = value.microsecond
        milliseconds = hour * 3600000 + minute * 60000 + second * 1000 + microsecond // 1000
        out_stream.write_bigendian_int32(milliseconds)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        cdef libc.stdint.int32_t value, milliseconds, minutes, seconds, hours
        value = in_stream.read_bigendian_int32()
        seconds = value // 1000
        milliseconds = value % 1000
        minutes = seconds // 60
        seconds %= 60
        hours = minutes // 60
        minutes %= 60
        return datetime.time(hours, minutes, seconds, milliseconds * 1000)

cdef class TimestampCoderImpl(StreamCoderImpl):
    def __cinit__(self, precision):
        self.is_compact = precision <= 3

    cpdef encode_to_stream(self, value, OutputStream out_stream, bint nested):
        cdef libc.stdint.int32_t microseconds_of_second, nanoseconds
        cdef libc.stdint.int64_t seconds, milliseconds
        seconds = <libc.stdint.int64_t>(value.replace(tzinfo=datetime.timezone.utc).timestamp())
        microseconds_of_second = value.microsecond
        milliseconds = seconds * 1000 + microseconds_of_second // 1000
        nanoseconds = microseconds_of_second % 1000 * 1000
        if self.is_compact:
            out_stream.write_bigendian_int64(milliseconds)
        else:
            out_stream.write_bigendian_int64(milliseconds)
            out_stream.write_bigendian_int32(nanoseconds)

    cpdef decode_from_stream(self, InputStream in_stream, bint nested):
        cdef libc.stdint.int32_t nanoseconds, second, microsecond
        cdef libc.stdint.int64_t milliseconds
        if self.is_compact:
            milliseconds = in_stream.read_bigendian_int64()
            nanoseconds = 0
        else:
            milliseconds = in_stream.read_bigendian_int64()
            nanoseconds = in_stream.read_bigendian_int32()
        second, microsecond = (milliseconds // 1000,
                               milliseconds % 1000 * 1000 + nanoseconds // 1000)
        return datetime.datetime.utcfromtimestamp(second).replace(microsecond=microsecond)
