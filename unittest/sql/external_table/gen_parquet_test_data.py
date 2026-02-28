#!/bin/env python
# -*- coding: UTF-8 -*-

"""
@author caizhi()
@brief


"""
import datetime
from decimal import Decimal
import hashlib
import random
import os
import os.path
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.types import *

def copy_parquet_file(table_dir, file_name, mysql_test_name=None):
    parquet_files = [f for f in os.listdir(table_dir) if f.endswith('.parquet')]
    if len(parquet_files) != 1:
        raise Exception('found %d files in %s, should be 1: %s' % (len(parquet_files), table_dir, parquet_files))
    cwd = os.path.dirname(os.path.abspath(__file__))
    src_file = os.path.join(table_dir, parquet_files[0])
    dst_file = os.path.join(cwd, file_name) + '.parquet'
    shutil.copyfile(src_file, dst_file)
    print('generated %s' % dst_file)
    if mysql_test_name:
        code_root = os.path.dirname(os.path.dirname(os.path.dirname(cwd)))
        dst_dir = os.path.join(code_root, 'tools/deploy/mysql_test/test_suite/external_table/data', mysql_test_name)
        os.makedirs(dst_dir, exist_ok=True)
        dst_file = os.path.join(dst_dir, file_name) + '.parquet'
        shutil.copyfile(src_file, dst_file)
        print('generated %s' % dst_file)
    return dst_file

# bloom_filter_smoking_test.parquet
def gen_bloom_filter_smoking_test(spark, tmp_dir, table_name, mysql_test_name=None):
    table_dir = os.path.join(tmp_dir, table_name)
    schema = StructType([
        StructField("i_no_bf", LongType(), True),
        StructField("i_has_bf", LongType(), True),
        StructField('s_no_bf', StringType(), True),
        StructField('s_has_bf', StringType(), True),
        ])
    rows = [(1, 1, 'uno', 'uno'), (2, 2, 'dos', 'dos'), (4, 4, 'cuatro', 'cuatro')]
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(rows, schema).repartition(1)
    df.show()
    df.write.option("parquet.bloom.filter.enabled#s_has_bf", "true") \
            .option("parquet.bloom.filter.enabled#i_has_bf", "true") \
            .mode('overwrite') \
            .parquet(table_dir)
    copy_parquet_file(table_dir, table_name, mysql_test_name)

# bloom_filter_all_numerics.parquet
def gen_bloom_filter_all_numerics(spark, tmp_dir, table_name):
    table_dir = os.path.join(tmp_dir, table_name)

    type_name_and_values = [
            (ByteType(),         'int8',          (1, 2, 4)),
            (ShortType(),        'int16',         (1, 2, 4)),
            (IntegerType(),      'int32',         (1, 2, 4)),
            (LongType(),         'int64',         (1, 2, 4)),
            (FloatType(),        'float',         (1.0, 2.0, 4.0)),
            (DoubleType(),       'double',        (1.0, 2.0, 4.0)),
            (DecimalType(10, 3), 'short_decimal', (Decimal('1.0'), Decimal('2.0'), Decimal('4.0'))),
            (DecimalType(20, 3), 'long_decimal',  (Decimal('1.0'), Decimal('2.0'), Decimal('4.0'))),
            ]

    col_types = []
    col_values = []
    for t, n, v in type_name_and_values:
        col_types.append(StructField(n + '_no_bf', t, True))
        col_values.append(v)
        col_types.append(StructField(n + '_has_bf', t, True))
        col_values.append(v)

    schema = StructType(col_types)
    rows = zip(*col_values)
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(rows, schema).repartition(1)
    df.show()

    x = df.write
    for _, n, _ in type_name_and_values:
        x = x.option('parquet.bloom.filter.enabled#' + n + '_has_bf', 'true')
    x.mode('overwrite').parquet(table_dir)
    copy_parquet_file(table_dir, table_name, 'parquet_bloom_filter_all_numerics')

# bloom_filter_all_other_types.parquet
def gen_bloom_filter_all_other_types(spark, tmp_dir, table_name):
    table_dir = os.path.join(tmp_dir, table_name)
    # all_other_types + all_numercis = all the data types in spark
    type_name_and_values = [
            (StringType(),      'str',       ('hola', 'bonjour', 'ciao')),
            # [BUG] logical planould not haveput of char/varchar type
            #(VarcharType(20),  'vcharn',    ('hola', 'bonjour', 'ciao')),
            #(CharType(20),     'charn',     ('hola', 'bonjour', 'ciao')),
            (BinaryType(),      'binary',    (b'hola', b'bonjour', b'ciao')),
            (BooleanType(),     'bool',      (True, True, True)),
            (DateType(),        'date',      (datetime.date(1970,1,1), datetime.date(1969,12,31), datetime.date(2025,7,16))),
            # TODO: +0800 tz/int96_t
            (TimestampType(),   'ts',        (datetime.datetime(1970,1,1,0,0,0), datetime.datetime(1969,12,31,23,59,59), datetime.datetime(2025,7,16,9,0,0))),
            ]

    col_types = []
    col_values = []
    for t, n, v in type_name_and_values:
        col_types.append(StructField(n + '_no_bf', t, True))
        col_values.append(v)
        # timestamp not supported
        if n != 'ts':
            col_types.append(StructField(n + '_has_bf', t, True))
            col_values.append(v)

    schema = StructType(col_types)
    rows = zip(*col_values)
    df = spark.createDataFrame(rows, schema).repartition(1)
    df.show()

    x = df.write
    for _, n, _ in type_name_and_values:
        x = x.option('parquet.bloom.filter.enabled#' + n + '_has_bf', 'true')
    x.mode('overwrite').parquet(table_dir)
    copy_parquet_file(table_dir, table_name, 'parquet_bloom_filter_all_other_types')

# simple types: int32(no bf), int32, double, text, binary
def gen_multi_stripes_with_simple_types(spark, tmp_dir, table_name):
    table_dir = os.path.join(tmp_dir, table_name)
    rows_per_row_group_plan = 2000
    # we'll fill the last unexpected ones with the last expected value
    rows_per_row_group_expect = 2107
    total_row_groups_expect = 6
    max_row_group_size = 70 * 1024
    """ expect layout: total 6 strips, note that parquet may not write bloom filter if ndv is low..
row_group |  min |  max | note                     | has_bf
-----------------------------------------------------------
        0 |    0 | 3998 | even numbers 0, 2, 4...  |      Y
        1 |    1 | 3999 | odd numbers 1, 3, 5..    |      Y
        2 | 1110 | 1114 | NDV=3: 1110,1111,1114    |      N
        3 |    0 | 1999 | sequence numbers         |      Y
        4 | 4000 | 5999 | sequence numbers         |      Y
        5 | 1110 | 1114 | NDV=3: 1110,1111,1114    |      N

find 1112:
row_group |  min |  max | has_bf | filter by min-max | filter by bf | num_rows
-------------------------------- ---------------------------------------------
        0 |    0 | 3998 |      Y |                 N |            N |        1
        1 |    1 | 3999 |      Y |                 N |            Y |        0
        2 | 1110 | 1114 |      N |                 N |            - |        0
        3 |    0 | 1999 |      Y |                 N |            N |        1
        4 | 4000 | 5999 |      Y |                 Y |            - |        0
        5 | 1110 | 1114 |      N |                 N |            - |        0
    """
    def make_row(i):
        s = str(i).zfill(4)
        return [
                i,  # int_no_bf
                i,  # int_has_bf
                i * 1.0,  # double_has_bf
                s, # text_has_bf
                #s.encode() + hashlib.md5(s.encode()).hexdigest().encode(), # binary_has_bf
                (s + hashlib.md5(s.encode()).hexdigest()[:2]).encode(), # binary_has_bf
                ]
    random.seed(76543)
    def fill_last_and_shuffle(r):
        r = r + [r[-1] for _ in range(rows_per_row_group_expect - rows_per_row_group_plan)]
        random.shuffle(r)
        return r
    expect_min_max_hasbf = []
    # row group[0]: even numbers 0 ~ 3998
    rows = fill_last_and_shuffle([make_row(i*2) for i in range(0, 2000)])
    expect_min_max_hasbf.append(('0000', '3998', True))
    # row group[1]: even numbers 1 ~ 3999
    rows += fill_last_and_shuffle([make_row(i*2+1) for i in range(0, 2000)])
    expect_min_max_hasbf.append(('0001', '3999', True))
    # row group[2]: NDV=3: 1110,1111,1114
    rows += fill_last_and_shuffle([make_row(1110 + [0,1,4][i % 3]) for i in range(2000)])
    expect_min_max_hasbf.append(('1110', '1114', False))
    # row group[3]: sequence numbers 0~1999
    rows += fill_last_and_shuffle([make_row(i) for i in range(2000)])
    expect_min_max_hasbf.append(('0000', '1999', False))
    # row group[4]: sequence numbers 4000~5999
    rows += fill_last_and_shuffle([make_row(i) for i in range(4000,6000)])
    expect_min_max_hasbf.append(('4000', '5999', True))
    # row group[5]: NDV=3: 1110,1111,1114
    rows += fill_last_and_shuffle([make_row(1110 + [0,1,4][i % 3]) for i in range(2000)])
    expect_min_max_hasbf.append(('1110', '1114', False))

    assert(len(rows) == total_row_groups_expect * rows_per_row_group_expect)
    assert(len(expect_min_max_hasbf) == total_row_groups_expect)

    schema = StructType([
        StructField("int_no_bf", IntegerType(), True),
        StructField("int_has_bf", IntegerType(), True),
        StructField('double_has_bf', DoubleType(), True),
        StructField('text_has_bf', StringType(), True),
        StructField('binary_has_bf', BinaryType(), True),
    ])

    df = spark.createDataFrame(rows, schema).repartition(1)
    df.show(3)
    df.write.option("parquet.bloom.filter.enabled#int_has_bf", "true") \
            .option("parquet.bloom.filter.enabled#double_has_bf", "true") \
            .option("parquet.bloom.filter.enabled#text_has_bf", "true") \
            .option("parquet.bloom.filter.enabled#binary_has_bf", "true") \
            .option('parquet.bloom.filter.max.bytes', 1048576 * 10) \
            .option('parquet.block.size', str(max_row_group_size)) \
            .mode('overwrite').parquet(table_dir)
    dst_file = copy_parquet_file(table_dir, table_name, 'parquet_bloom_filter_multi_row_group')
    # check parquet file structure
    import parquet_tools.parquet.reader
    metadata = parquet_tools.parquet.reader.get_filemetadata(dst_file)
    assert(len(metadata.row_groups) == total_row_groups_expect)
    print('%s has %d row groups, as expected.' % (dst_file, len(metadata.row_groups)))
    for row_group, (str_min, str_max, has_bf) in zip(metadata.row_groups, expect_min_max_hasbf):
        print('Row group rows[%d]' % row_group.num_rows)
        assert(row_group.num_rows == rows_per_row_group_expect)
        has_text_has_bf = False
        for c in row_group.columns:
            name = c.meta_data.path_in_schema[0]
            if name.endswith('no_bf'):
                continue
            stat = c.meta_data.statistics
            offset = c.meta_data.bloom_filter_offset
            print('>>> %s: min[%s] max[%s] bloom_filter_offset[%s]' % (
                name.ljust(20), stat.min_value, stat.max_value, offset))
            if has_bf:
                assert(offset and offset > 0)
            if name == 'text_has_bf':
                has_text_has_bf = True
                assert(str_min.encode() == stat.min_value)
                assert(str_max.encode() == stat.max_value)
        assert(has_text_has_bf)


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    tmp_dir = '/tmp/gen_parquet_test_data_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    print('using %s as tmp dir' % tmp_dir)

    import pyspark
    if pyspark.__version__ == '4.0.0':
        gen_bloom_filter_smoking_test(spark, tmp_dir, 'bloom_filter_smoking_test_pyspark4.0')
    elif pyspark.__version__ == '3.5.6':
        gen_bloom_filter_smoking_test(spark, tmp_dir, 'bloom_filter_smoking_test', 'parquet_bloom_filter_smoking')
        gen_bloom_filter_all_numerics(spark, tmp_dir, 'bloom_filter_all_numerics')
        gen_bloom_filter_all_other_types(spark, tmp_dir, 'bloom_filter_all_other_types')
        gen_multi_stripes_with_simple_types(spark, tmp_dir, 'bloom_filter_multi_stripes_with_simple_types')
    else:
        raise Exception('invalidate pyspark version[%s] should be either 4.0.0 or 3.5.6!' % (pyspark.__version__))
