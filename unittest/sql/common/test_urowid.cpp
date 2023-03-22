/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL
#include "lib/rowid/ob_urowid.h"
#include "lib/container/ob_array.h"
#include "lib/number/ob_number_v2.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/encode/ob_base64_encode.h"
#include "common/object/ob_object.h"
#include "share/ob_errno.h"

#include <gtest/gtest.h>
#include <cstdlib>
#include <iostream>
#include <string>
#include <cstring>

using std::cout;
using std::endl;

namespace oceanbase
{
namespace common
{
class TestAllocator: public ObIAllocator
{
public:
  virtual void *alloc(const int64_t size) override
  {
    return std::malloc(size);
  }
  virtual void free(void *ptr) override
  {
    return std::free(ptr);
  }
  virtual void *alloc(const int64_t size, const ObMemAttr &/* not used */)
  {
    return std::malloc(size);
  }
};
class TestURowID
{
public:
  // void SetUp() override {}
  // void TearDown() override {}

  void multi_set_and_get()
  {
    ObObj pk_val;
    ObArray<ObObj> pk_vals;
    TestAllocator allocator;

    pk_val.set_null();
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));
    pk_val.set_tinyint(1);
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));
    pk_val.set_smallint(2);
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));
    pk_val.set_mediumint(322);
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));
    pk_val.set_int32(4333);
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));
    pk_val.set_int(512345678);
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));
    pk_val.set_float(1.2345678);
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));
    pk_val.set_double(2345.231312411);
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));

    number::ObNumber test_number;
    test_number.from("123456789987654321.12345678987654321", allocator);
    pk_val.set_number(test_number);
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));

    const char *test_str = "hello, this is a test";
    pk_val.set_char(ObString(strlen(test_str), test_str));
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));

    pk_val.set_varchar(ObString(strlen(test_str), test_str));
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));

    pk_val.set_nchar(ObString(strlen(test_str), test_str));
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));

    pk_val.set_nvarchar2(ObString(strlen(test_str), test_str));
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));

    ObTimeZoneInfo time_zone_info;
    time_zone_info.set_timezone("+8:00");
    ObTimeConvertCtx time_convert_ctx(&time_zone_info,
                                      ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT,
                                      true);

    ObOTimestampData otime_data;
    ObScale scale;
    ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp("2020-02-24 21:37:25",
                                                             time_convert_ctx,
                                                             ObTimestampTZType,
                                                             otime_data,
                                                             scale));
    pk_val.set_timestamp_tz(otime_data);
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));

    ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp("2020-02-24 21:37:25",
                                                             time_convert_ctx,
                                                             ObTimestampNanoType,
                                                             otime_data,
                                                             scale));
    pk_val.set_timestamp_nano(otime_data);
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));

    ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp("2020-02-24 21:37:25",
                                                             time_convert_ctx,
                                                             ObTimestampNanoType,
                                                             otime_data,
                                                             scale));
    pk_val.set_timestamp_nano(otime_data);
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));

    scale = 9;
    ObIntervalYMValue ym_value;
    ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_interval_ym("000000019-11", ym_value, scale));
    pk_val.set_interval_ym(ym_value);
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));

    scale = 99;
    ObIntervalDSValue ds_value;
    ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_interval_ds("000000012 11:11:11",
                                                              ds_value, scale));
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));

    ObURowIDData urowid_data;
    ASSERT_EQ(OB_SUCCESS, urowid_data.set_rowid_content(pk_vals, ObURowIDData::PK_ROWID_VERSION, allocator));

    int64_t pos = urowid_data.get_pk_content_offset();
    int i = 0;
    ObObj tmp_obj;
    for(; pos < urowid_data.rowid_len_;) {
      ASSERT_EQ(urowid_data.get_pk_type(pos), pk_vals.at(i).get_type());
      ASSERT_EQ(OB_SUCCESS, urowid_data.get_pk_value(pk_vals.at(i).get_type(), pos, tmp_obj));
      ASSERT_EQ(tmp_obj, pk_vals.at(i));
      i++;
    }
    test_encode_decode(pk_vals, urowid_data);
  }

  void set_and_get_pk()
  {
#define TEST_SET_AND_GET_VAL(val, val_len)                                    \
  do {                                                                        \
    pk_vals.reset();                                                          \
    urowid_data.reset();                                                      \
    ObObj tmp_obj;                                                            \
    int64_t urowid_version = ObURowIDData::PK_ROWID_VERSION;                  \
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(pk_val));                         \
    ASSERT_EQ(OB_SUCCESS, urowid_data.set_rowid_content(pk_vals, ObURowIDData::PK_ROWID_VERSION, allocator)); \
    int64_t tmp_pos = urowid_data.get_pk_content_offset();                    \
    ASSERT_EQ(urowid_version, urowid_data.get_version());                     \
    ASSERT_TRUE(urowid_data.is_valid_urowid());                               \
    ASSERT_EQ(val_len + 1, urowid_data.get_obj_size(val));                    \
    ASSERT_EQ(tmp_pos + val_len + 1, urowid_data.get_buf_len());              \
    ASSERT_EQ(urowid_data.get_pk_type(tmp_pos), val.get_type());              \
    ASSERT_EQ(tmp_pos, urowid_data.get_pk_content_offset() + 1);              \
    ASSERT_EQ(OB_SUCCESS,                                                     \
              urowid_data.get_pk_value(val.get_type(), tmp_pos, tmp_obj));    \
    ASSERT_EQ(tmp_obj, val);                                                  \
    test_encode_decode(pk_vals, urowid_data);                                 \
                                                                              \
    ObObj urowid_obj;                                                         \
    ObURowIDData urowid2;                                                     \
    urowid_obj.set_urowid(urowid_data);                                       \
    pk_vals.reset();                                                          \
    tmp_obj.reset();                                                          \
    ASSERT_EQ(OB_SUCCESS, pk_vals.push_back(urowid_obj));                     \
    ASSERT_EQ(OB_SUCCESS, urowid2.set_rowid_content(pk_vals, ObURowIDData::PK_ROWID_VERSION, allocator));     \
    ASSERT_TRUE(urowid2.is_valid_urowid());                                   \
    tmp_pos = urowid2.get_pk_content_offset();                                \
    ASSERT_EQ(ObURowIDType, urowid2.get_pk_type(tmp_pos));                    \
    ASSERT_EQ(OB_SUCCESS,                                                     \
              urowid2.get_pk_value(urowid_obj.get_type(), tmp_pos, tmp_obj)); \
    ASSERT_TRUE(urowid_data == tmp_obj.get_urowid());                         \
  } while (0);

    ObURowIDData urowid_data;
    ObArray<ObObj> pk_vals;
    TestAllocator allocator;

    ObObj pk_val;
    pk_val.set_null();

    ObObj val;
    val.set_null();
    // int64_t val_len = 0;
    TEST_SET_AND_GET_VAL(pk_val, 0);

    pk_val.set_tinyint(1);
    TEST_SET_AND_GET_VAL(pk_val, 1);

    pk_val.set_smallint(2);
    TEST_SET_AND_GET_VAL(pk_val, 2);

    pk_val.set_mediumint(3);
    TEST_SET_AND_GET_VAL(pk_val, 4);

    pk_val.set_int32(4);
    TEST_SET_AND_GET_VAL(pk_val, 4);

    pk_val.set_int(5);
    TEST_SET_AND_GET_VAL(pk_val, 8);

    pk_val.set_float(1.2345678);
    TEST_SET_AND_GET_VAL(pk_val, sizeof(float));

    pk_val.set_double(2345.231312411);
    TEST_SET_AND_GET_VAL(pk_val, sizeof(double));

    pk_val.set_datetime(1582569960);
    TEST_SET_AND_GET_VAL(pk_val, 8);

    number::ObNumber test_number;
    test_number.from("123456789987654321.12345678987654321", allocator);
    pk_val.set_number(test_number);
    TEST_SET_AND_GET_VAL(pk_val, 4 + sizeof(uint32_t) * test_number.get_desc().len_);

    pk_val.set_number_float(test_number);
    TEST_SET_AND_GET_VAL(pk_val, 4 + sizeof(uint32_t) * test_number.get_desc().len_);

    const char *test_str = "hello, this is a test";
    pk_val.set_char(ObString(strlen(test_str), test_str));
    TEST_SET_AND_GET_VAL(pk_val, 5 + strlen(test_str));

    pk_val.set_varchar(ObString(strlen(test_str), test_str));
    TEST_SET_AND_GET_VAL(pk_val, 5 + strlen(test_str));

    pk_val.set_nchar(ObString(strlen(test_str), test_str));
    TEST_SET_AND_GET_VAL(pk_val, 5 + strlen(test_str));

    pk_val.set_nvarchar2(ObString(strlen(test_str), test_str));
    TEST_SET_AND_GET_VAL(pk_val, 5 + strlen(test_str));

    ObTimeZoneInfo time_zone_info;
    time_zone_info.set_timezone("+8:00");
    ObTimeConvertCtx time_convert_ctx(&time_zone_info,
                                      ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT,
                                      true);

    ObOTimestampData otime_data;
    ObScale scale;
    ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp("2020-02-24 21:37:25",
                                                             time_convert_ctx,
                                                             ObTimestampTZType,
                                                             otime_data,
                                                             scale));
    pk_val.set_timestamp_tz(otime_data);
    TEST_SET_AND_GET_VAL(pk_val, 12);

    ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp("2020-02-24 21:37:25",
                                                             time_convert_ctx,
                                                             ObTimestampLTZType,
                                                             otime_data,
                                                             scale));
    pk_val.set_timestamp_ltz(otime_data);
    TEST_SET_AND_GET_VAL(pk_val, 10);

    ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp("2020-02-24 21:37:25",
                                                             time_convert_ctx,
                                                             ObTimestampNanoType,
                                                             otime_data,
                                                             scale));
    pk_val.set_timestamp_nano(otime_data);
    TEST_SET_AND_GET_VAL(pk_val, 10);

    scale = 9;
    ObIntervalYMValue ym_value;
    ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_interval_ym("000000019-11", ym_value, scale));
    pk_val.set_interval_ym(ym_value);
    TEST_SET_AND_GET_VAL(pk_val, 8);

    scale = 99;
    ObIntervalDSValue ds_value;
    ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_interval_ds("000000012 11:11:11",
                                                              ds_value, scale));
    pk_val.set_interval_ds(ds_value);
    TEST_SET_AND_GET_VAL(pk_val, 12);
  }

  void test_encode_decode(ObIArray<ObObj> &pk_vals, ObURowIDData &urowid_data)
  {
    TestAllocator allocator;
    const int64_t encode_buf_len = urowid_data.needed_base64_buffer_size();
    char *encode_buf = (char *)allocator.alloc(encode_buf_len);
    int64_t encoded_len = 0;
    ASSERT_EQ(OB_SUCCESS, urowid_data.get_base64_str(encode_buf, encode_buf_len, encoded_len));
    ASSERT_EQ(encoded_len, encode_buf_len);
    std::cout << std::string(encode_buf, encoded_len) << '\n';
    // const int64_t decode_buf_len = ObURowIDData::needed_urowid_buf_size(encoded_len);
    // char *decode_buf = (char *)allocator.alloc(decode_buf_len);
    ObURowIDData decode_data;
    ASSERT_EQ(OB_SUCCESS, ObURowIDData::decode2urowid(encode_buf, encoded_len,
                                                      allocator, decode_data));
    ASSERT_EQ(decode_data, urowid_data);

    int64_t decode_pos = decode_data.get_pk_content_offset();
    int64_t i = 0;
    for(; decode_pos < decode_data.rowid_len_; ) {
      ObObjType obj_type = decode_data.get_pk_type(decode_pos);
      ASSERT_EQ(obj_type, pk_vals.at(i).get_type());
      ObObj tmp_obj;
      ASSERT_EQ(OB_SUCCESS, decode_data.get_pk_value(obj_type, decode_pos, tmp_obj));
      ASSERT_EQ(tmp_obj, pk_vals.at(i));
      i += 1;
    }
  }

  void test_urowid_cmp()
  {
    TestAllocator allocator;
    ObURowIDData urowid1, urowid2;
    ObObj obj;
    number::ObNumber num;
    ObArray<ObObj> pk_vals;
    num.from("123456.123456", allocator);
    obj.set_int(123456);
    pk_vals.push_back(obj);
    obj.set_number(num);
    pk_vals.push_back(obj);
    obj.set_varchar("hello");
    pk_vals.push_back(obj);
    ASSERT_EQ(OB_SUCCESS, urowid1.set_rowid_content(pk_vals, ObURowIDData::PK_ROWID_VERSION, allocator));

    pk_vals.reset();
    num.from("123456.124356", allocator);
    obj.set_int(123456);
    pk_vals.push_back(obj);
    obj.set_number(num);
    pk_vals.push_back(obj);
    ASSERT_EQ(OB_SUCCESS, urowid2.set_rowid_content(pk_vals, ObURowIDData::PK_ROWID_VERSION, allocator));

    ASSERT_TRUE(-1 == urowid1.compare(urowid2));
    ASSERT_TRUE(1 == urowid2.compare(urowid1));

    pk_vals.reset();
    num.from("123456.123456", allocator);
    obj.set_int(123456);
    pk_vals.push_back(obj);
    obj.set_number(num);
    pk_vals.push_back(obj);
    obj.set_varchar("hella");
    pk_vals.push_back(obj);
    ASSERT_EQ(OB_SUCCESS, urowid2.set_rowid_content(pk_vals, ObURowIDData::PK_ROWID_VERSION, allocator));

    ASSERT_TRUE(1 == urowid1.compare(urowid2));
    ASSERT_TRUE(-1 == urowid2.compare(urowid1));
  }
};

TEST(TestURowID, single_urowid_test) {
  TestAllocator alloc;
  // bug:
  const char *str = "*AAEWLlcAAABnYXJuaXNoIG51bWJlciB0ZWFyaW5nIG1hbnRsaW5nIGFjY3VzdG9tcyBleHRyZW1lcyBiYXNlbHkgY3JhemlseSBlbGRlciB0YXJkaWVzIHByZW1pc2UtCwAAAAACCkcywAAAAAAA";
  ObURowIDData dec_data;
  ASSERT_EQ(OB_SUCCESS, ObURowIDData::decode2urowid(str, strlen(str), alloc, dec_data));
}

TEST(TestURowID, set_and_get_pk) {
  TestURowID urowid_test;
  urowid_test.set_and_get_pk();
}

void decode_output()
{
  TestAllocator alloc;
  // const char *str = "*AAIPAQEAwHsAAAAACgEAAAAAAAAA";
  const char *str = "*AAEWLlcAAABnYXJuaXNoIG51bWJlciB0ZWFyaW5nIG1hbnRsaW5nIGFjY3VzdG9tcyBleHRyZW1lcyBiYXNlbHkgY3JhemlseSBlbGRlciB0YXJkaWVzIHByZW1pc2UtCwAAAAACCkcywAAAAAAA";
  ObURowIDData dec_data;
  ASSERT_EQ(OB_SUCCESS, ObURowIDData::decode2urowid(str, strlen(str), alloc, dec_data));

  ObArray<ObObj> pk_vals;
  LOG_INFO("dba len: ", K(dec_data.get_guess_dba_len()));
  LOG_INFO("ver: ", K(dec_data.get_version()));

  ASSERT_EQ(OB_SUCCESS, dec_data.get_pk_vals(pk_vals));
  LOG_INFO("cnt: ", K(pk_vals.count()));
  for (int i = 0; i < pk_vals.count(); ++i) {
    LOG_INFO("pk", K(pk_vals.at(i)));
  }
}

TEST(TestURowID, multi_set_and_get) {
  TestURowID urowid_test;
  urowid_test.multi_set_and_get();
}

TEST(TestURowID, cmp) {
  TestURowID urowid_test;
  urowid_test.test_urowid_cmp();
}
} // end namesapce common
} // end namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  system("rm -rf test_urowid.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_urowid.log", true);
  oceanbase::common::decode_output();
  return RUN_ALL_TESTS();
}
