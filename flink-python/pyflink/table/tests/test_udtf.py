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
from pyflink.table import DataTypes
from pyflink.table.udf import TableFunction, udtf, ScalarFunction, udf
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase,\
    PyFlinkBlinkStreamTableTestCase, PyFlinkBlinkBatchTableTestCase


class UserDefinedTableFunctionTests(PyFlinkStreamTableTestCase):

    def test_table_function(self):
        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c', 'd'],
            [DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)
        self.t_env.register_function(
            "multi_emit", udtf(MultiEmit(), [DataTypes.BIGINT(), DataTypes.BIGINT()],
                               [DataTypes.BIGINT(), DataTypes.BIGINT()]))
        self.t_env.register_function(
            "multi_num", udf(MultiNum(), [DataTypes.BIGINT()],
                             DataTypes.BIGINT()))

        t = self.t_env.from_elements([(1, 1, 3), (2, 1, 6), (3, 2, 9)], ['a', 'b', 'c'])
        t.join_lateral("multi_emit(a, multi_num(b)) as (x, y)") \
            .select("a, b, x, y") \
            .insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["1,1,1,0", "1,1,1,1", "2,1,2,0", "2,1,2,1", "3,2,3,0",
                                    "3,2,3,1", "3,2,3,2", "3,2,3,3"])

    def test_table_function_merge(self):
        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b', 'c', 'd', 'e'],
            [DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT(),
             DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)

        self.t_env.register_function("multi_emit", multi_emit)
        self.t_env.register_function("split_str",
                                     udtf(SplitStr(), DataTypes.STRING(), DataTypes.STRING()))

        t = self.t_env.from_elements([(1, 1, 3), (2, 1, 6), (3, 2, 9)], ['a', 'b', 'c'])
        t.join_lateral("multi_emit(a, a) as (x, y, z)") \
            .join_lateral("multi_emit(b, x) as (m, n, o)") \
            .join_lateral("multi_emit(x, n) as (p, q, r)") \
            .select("a, b, y, n, p") \
            .insert_into("Results")
        self.t_env.execute("test")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["2,1,0,1,2", "2,1,1,1,2", "3,2,0,2,3", "3,2,0,2,3", "3,2,2,2,3",
                                    "3,2,2,2,3", "3,2,0,1,3", "3,2,1,2,3", "3,2,1,2,3", "3,2,2,1,3",
                                    "3,2,1,1,3"])


class MultiEmit(TableFunction):
    def eval(self, x, y):
        for i in range(y):
            yield x, i


@udtf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()],
      result_types=[DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()])
def multi_emit(x, y):
    for i in range(y):
        yield x, i, i + 1


class SplitStr(TableFunction):
    def eval(self, s):
        for ele in s.split("#"):
            yield ele


class MultiNum(ScalarFunction):
    def eval(self, x):
        return x * 2


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
