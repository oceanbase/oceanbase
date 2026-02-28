#!/bin/env python
# -*- coding: UTF-8 -*-

"""
@author caizhi()
@brief


"""
import datetime
from decimal import Decimal
import random
import os
import os.path
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.types import *

def copy_orc_file(table_dir, file_name, mysql_test_name=None):
    orc_files = [f for f in os.listdir(table_dir) if f.endswith('.orc')]
    if len(orc_files) != 1:
        raise Exception('found %d files in %s, should be 1: %s' % (len(orc_files), table_dir, orc_files))
    cwd = os.path.dirname(os.path.abspath(__file__))
    src_file = os.path.join(table_dir, orc_files[0])
    dst_file = os.path.join(cwd, file_name) + '.orc'
    shutil.copyfile(src_file, dst_file)
    print('generated %s' % dst_file)
    if mysql_test_name:
        code_root = os.path.dirname(os.path.dirname(os.path.dirname(cwd)))
        dst_dir = os.path.join(code_root, 'tools/deploy/mysql_test/test_suite/external_table/data', mysql_test_name)
        os.makedirs(dst_dir, exist_ok=True)
        dst_file = os.path.join(dst_dir, file_name) + '.orc'
        shutil.copyfile(src_file, dst_file)
        print('generated %s' % dst_file)
    return dst_file

# bloom_filter_smoking_test.orc
def gen_bloom_filter_smoking_test(spark, tmp_dir, table_name):
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
    df.write.option('orc.bloom.filter.columns', 's_has_bf,i_has_bf').mode('overwrite').orc(table_dir)
    copy_orc_file(table_dir, table_name, 'orc_bloom_filter_smoking')

# bloom_filter_all_numerics.orc
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

    orc_columns = ','.join(['%s_has_bf' % n for _, n, _ in type_name_and_values])
    df.write.option('orc.bloom.filter.columns', orc_columns).mode('overwrite').orc(table_dir)
    copy_orc_file(table_dir, table_name, 'orc_bloom_filter_all_numerics')

# bloom_filter_all_other_types.orc
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
        col_types.append(StructField(n + '_has_bf', t, True))
        col_values.append(v)

    schema = StructType(col_types)
    rows = zip(*col_values)
    df = spark.createDataFrame(rows, schema).repartition(1)
    df.show()

    orc_columns = ','.join(['%s_has_bf' % n for _, n, _ in type_name_and_values])
    df.write.option('orc.bloom.filter.columns', orc_columns).mode('overwrite').orc(table_dir)
    copy_orc_file(table_dir, table_name, 'orc_bloom_filter_all_other_types')

# simple types: int32(no bf), int32, double, decimal, text, date, timestamp
def gen_multi_stripes_with_simple_types(spark, tmp_dir, table_name):
    table_dir = os.path.join(tmp_dir, table_name)
    rows_per_index = 2000
    rows_per_stripe = rows_per_index * 2
    rows_per_file = rows_per_stripe * 4
    """ expect layout: total 4 strips, each has 2 index.
strip_no | page_no |  min |  max | note                     |
-------------------------------------------------------------
       0 |       0 |    0 | 3998 | even numbers 0, 2, 4...  |
         |       1 |    1 | 3999 | odd numbers 1, 3, 5..    |
       1 |       0 | 2000 | 3999 | sequence numbers         |
         |       1 | 1110 | 1114 | NDV=3: 1110,1111,1114    |
       2 |       0 | 4000 | 5999 | sequence numbers         |
         |       1 |    0 | 1999 | sequence numbers         |
       3 |       0 | 1110 | 1114 | NDV=3: 1110,1111,1114    |
         |       1 | 1110 | 1114 | NDV=3: 1110,1111,1114    |

find 1112:
strip_no | page_no |  min |  max | filter by row group | filter by page | filter by bf | num rows
------------------------------------------------------------------------------------------------
       0 |       0 |    0 | 3998 |                   N |              N |            N |      1
         |       1 |    1 | 3999 |                     |              N |            Y |      0
       1 |       0 | 2000 | 3999 |                   N |              Y |            - |      0
         |       1 | 1110 | 1114 |                     |              N |            Y |      0
       2 |       0 | 4000 | 5999 |                   N |              Y |            - |      0
         |       1 |    0 | 1999 |                     |              N |            N |      1
       3 |       0 | 1110 | 1114 |                   N |              N |            Y |      0
         |       1 | 1110 | 1114 |                     |              N |            Y |      0
find 1121:
strip_no | page_no |  min |  max | filter by row group | filter by page | filter by bf | num rows
------------------------------------------------------------------------------------------------
       0 |       0 |    0 | 3998 |                   N |              N |            Y |      0
         |       1 |    1 | 3999 |                     |              N |            N |      1
       1 |       0 | 2000 | 3999 |                   N |              Y |            - |      0
         |       1 | 1110 | 1114 |                     |              Y |            - |      0
       2 |       0 | 4000 | 5999 |                   N |              Y |            - |      0
         |       1 |    0 | 1999 |                     |              N |            N |      1
       3 |       0 | 1110 | 1114 |                   N |              Y |            - |      0
         |       1 | 1110 | 1114 |                     |              Y |            - |      0
    """
    def make_row(i):
        return [
                i,  # int_no_bf
                i,  # int_has_bf
                i * 1.0,  # double_has_bf
                Decimal('%d.123' % i), # decimal_has_bf
                str(i).zfill(4), # text_has_bf
                (datetime.datetime(2000, 1, 1) + datetime.timedelta(days=i)), # date_has_bf
                (datetime.datetime(2000, 1, 1, 1, 23, 59) + datetime.timedelta(days=i)) # timestamp_has_bf
                ]
    random.seed(76543)
    # strip[0] row_index[0]: even numbers 0 ~ 3998
    rows = [make_row(i*2) for i in range(0, 2000)]
    random.shuffle(rows)
    # strip[0] row_index[1]: odd numbers 1 ~ 3999
    r = [make_row(i*2+1) for i in range(0, 2000)]
    random.shuffle(r)
    rows += r
    # strip[1] row_index[0]: sequence numbers 2000 ~ 3999
    r = [make_row(i) for i in range(2000, 4000)]
    random.shuffle(r)
    rows += r
    # strip[1] row_index[1]: NDV=3: 1110,1111,1114
    r = [make_row(1110 + [0,1,4][i % 3]) for i in range(2000)]
    random.shuffle(r)
    rows += r
    # strip[2] row_index[0]: sequence numbers 4000 ~ 5999
    r = [make_row(i) for i in range(4000, 6000)]
    random.shuffle(r)
    rows += r
    # strip[2] row_index[1]: sequence numbers 0 ~ 1999
    r = [make_row(i) for i in range(0, 2000)]
    random.shuffle(r)
    rows += r
    # strip[3] row_index[0]: NDV=3: 1110,1111,1114
    r = [make_row(1110 + [0,1,4][i % 3]) for i in range(2000)]
    random.shuffle(r)
    rows += r
    # strip[3] row_index[1]: NDV=3: 1110,1111,1114
    r = [make_row(1110 + [0,1,4][i % 3]) for i in range(2000)]
    random.shuffle(r)
    rows += r
    assert(len(rows) == rows_per_file)

    schema = StructType([
        StructField("int_no_bf", IntegerType(), True),
        StructField("int_has_bf", IntegerType(), True),
        StructField('double_has_bf', DoubleType(), True),
        StructField('decimal_has_bf', DecimalType(20, 3), True),
        StructField('text_has_bf', StringType(), True),
        StructField('date_has_bf', DateType(), True),
        StructField('timestamp_has_bf', TimestampType(), True),
    ])

    df = spark.createDataFrame(rows, schema).repartition(1)
    df.show(3)
    orc_columns = 'int_has_bf,double_has_bf,decimal_has_bf,text_has_bf,date_has_bf,timestamp_has_bf'
    df.write.option('orc.bloom.filter.columns', orc_columns)\
            .option('orc.row.index.stride', rows_per_index)\
            .option('orc.stripe.row.count', rows_per_stripe)\
            .mode('overwrite').orc(table_dir)
    copy_orc_file(table_dir, table_name, 'orc_bloom_filter_multi_row_group')

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    tmp_dir = '/tmp/gen_orc_test_data_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    print('using %s as tmp dir' % tmp_dir)

    gen_bloom_filter_smoking_test(spark, tmp_dir, 'bloom_filter_smoking_test')
    gen_bloom_filter_all_numerics(spark, tmp_dir, 'bloom_filter_all_numerics')
    gen_bloom_filter_all_other_types(spark, tmp_dir, 'bloom_filter_all_other_types')
    gen_multi_stripes_with_simple_types(spark, tmp_dir, 'bloom_filter_multi_stripes_with_simple_types')
