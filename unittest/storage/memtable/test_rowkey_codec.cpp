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

#include "storage/memtable/ob_rowkey_codec.h"

#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "common/rowkey/ob_rowkey.h"

#include "utils_rowkey_builder.h"

#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::memtable;

class ObBuffer
{
public:
  ObBuffer() : buf_(NULL), cap_(0), pos_(0) {}
  ~ObBuffer()
  {
    if (NULL != buf_)
    {
      delete[] buf_;
      buf_ = NULL;
    }
  }
public:
  void set_buf(const int64_t cap)
  {
    buf_ = new(std::nothrow) char[cap];
    assert(NULL != buf_);
    cap_ = cap;
    pos_ = 0;
  }
  void reset()
  {
    if (NULL != buf_)
    {
      delete[] buf_;
      buf_ = NULL;
    }
    cap_ = 0;
    pos_ = 0;
  }
public:
  char *get_data() const { return buf_; }
  int64_t get_capacity() const { return cap_; }
  int64_t &get_capacity() { return cap_; }
  int64_t &get_position() { return pos_; }
private:
  char *buf_;
  int64_t cap_;
  int64_t pos_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

void test_decode_encode(const ObRowkeyWrapper &rkw)
{
  static const int64_t VALID_BUFFER_SIZE = 1<<21;
  static const int64_t LOOP_TIMES = 100;

  ObBuffer src_buffer;
  ObBuffer dst_buffer;
  int ret = OB_SUCCESS;

  src_buffer.set_buf(VALID_BUFFER_SIZE);
  for (int64_t i = 0; i < LOOP_TIMES; ++i)
  {
    int64_t encode_orig_pos = src_buffer.get_position();

    ret = encode_table_id(i, src_buffer);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = encode_rowkey_len(static_cast<uint8_t>(rkw.get_obj_cnt()), src_buffer);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = encode_compact_rowkey(rkw.get_rowkey(), src_buffer);
    if (OB_FAIL(ret)) abort();
    ASSERT_EQ(OB_SUCCESS, ret);

    dst_buffer.set_buf(VALID_BUFFER_SIZE);
    for (int64_t j = 0; j < LOOP_TIMES; ++j)
    {
      int64_t decode_orig_pos = src_buffer.get_position();
      src_buffer.get_position() = encode_orig_pos;

      uint64_t table_id = 0;
      ret = decode_table_id(table_id, src_buffer);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(static_cast<int64_t>(table_id), i);

      uint8_t rk_len = 0;
      ret = decode_rowkey_len(rk_len, src_buffer);
      ASSERT_EQ(OB_SUCCESS, ret);
      ObRowkeyWrapper decode_rkw(rk_len);

      ret = decode_compact_rowkey(decode_rkw.get_rowkey(), src_buffer, dst_buffer);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(rkw.get_rowkey(), decode_rkw.get_rowkey());
      ASSERT_EQ(decode_orig_pos, src_buffer.get_position());
      if (rkw.get_rowkey() != decode_rkw.get_rowkey())
      {
        TRANS_LOG(WARN, "NE::", "\nrk1", rkw.get_rowkey(), "\nrk2", decode_rkw.get_rowkey());
      }
    }
  }
}

TEST(TestObRowkeyCodec, test_encode_decode_normal)
{
  test_decode_encode(
      RK(
        V("hello", 5),
        V(NULL, 0),
        I(1024),
        N("3.14"),
        //B(true),
        //T(::oceanbase::common::ObTimeUtility::current_time()),
        U(),
        OBMIN(),
        OBMAX(),
        V("world", 5),
        V(NULL, 0),
        I(-1024),
        N("-3.14"),
        //B(false),
        //T(-::oceanbase::common::ObTimeUtility::current_time()),
        U(),
        OBMIN(),
        OBMAX()
        ));

  int64_t v = 0x0100abcd01001234;
  test_decode_encode(
      RK(
        V(NULL, 0),
        V(reinterpret_cast<char *>(&v), 1),
        V(reinterpret_cast<char *>(&v), 2),
        V(reinterpret_cast<char *>(&v), 3),
        V(reinterpret_cast<char *>(&v), 4),
        V(reinterpret_cast<char *>(&v), 5),
        V(reinterpret_cast<char *>(&v), 6),
        V(reinterpret_cast<char *>(&v), 7),
        V(reinterpret_cast<char *>(&v), 8)
        ));

  test_decode_encode(
      RK(
        N("3.14"),
        N("-3.14"),
        N("0"),
        N("999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
        N("-999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
        N("0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"),
        N("-0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"),
        N("1.23456789123456789123456789123456789123456789123456789"),
        N("-1.23456789123456789123456789123456789123456789123456789"),
        N("12345678912345678912345678912345678912345678.9123456789"),
        N("-12345678912345678912345678912345678912345678.9123456789")
        ));
}

TEST(TestObRowkeyCodec, test_encode_decode_abnormal)
{
  ObBuffer src_buffer;
  ObBuffer dst_buffer;
  int ret = OB_SUCCESS;

  RK rkw_e(E(RF_DELETE));
  uint8_t rk_len = 1;
  uint64_t table_id = 1000;

  RK rkw_v(V("hello", 5));
  RK rkw_n(N("3.141593"));
  RK rkw_i(I(1024));
  //RK rkw_t(T(::oceanbase::common::ObTimeUtility::current_time()));
  //RK rkw_b(B(true));

  // null pointer, invalid param
  src_buffer.reset();
  ret = encode_compact_rowkey(rkw_v.get_rowkey(), src_buffer);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  // buffer not enough
  src_buffer.set_buf(3);
  src_buffer.get_position() = 0;
  ret = encode_compact_rowkey(rkw_v.get_rowkey(), src_buffer);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);
  EXPECT_EQ(0, src_buffer.get_position());
  src_buffer.get_position() = 3;
  ret = encode_compact_rowkey(rkw_v.get_rowkey(), src_buffer);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);

  // null pointer, invalid param
  src_buffer.reset();
  ret = encode_compact_rowkey(rkw_n.get_rowkey(), src_buffer);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  // buffer not enough
  src_buffer.set_buf(3);
  src_buffer.get_position() = 0;
  ret = encode_compact_rowkey(rkw_n.get_rowkey(), src_buffer);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);
  EXPECT_EQ(0, src_buffer.get_position());
  src_buffer.get_position() = 3;
  ret = encode_compact_rowkey(rkw_n.get_rowkey(), src_buffer);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);

  // null pointer, invalid param
  src_buffer.reset();
  ret = encode_compact_rowkey(rkw_i.get_rowkey(), src_buffer);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  // buffer not enough
  src_buffer.set_buf(3);
  src_buffer.get_position() = 0;
  ret = encode_compact_rowkey(rkw_i.get_rowkey(), src_buffer);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);
  EXPECT_EQ(0, src_buffer.get_position());
  src_buffer.get_position() = 3;
  ret = encode_compact_rowkey(rkw_i.get_rowkey(), src_buffer);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);

  //// null pointer, invalid param
  //src_buffer.reset();
  //ret = encode_compact_rowkey(rkw_i.get_rowkey(), src_buffer);
  //EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  //// buffer not enough
  //src_buffer.set_buf(3);
  //src_buffer.get_position() = 0;
  //ret = encode_compact_rowkey(rkw_t.get_rowkey(), src_buffer);
  //EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);
  //EXPECT_EQ(0, src_buffer.get_position());
  //src_buffer.get_position() = 3;
  //ret = encode_compact_rowkey(rkw_t.get_rowkey(), src_buffer);
  //EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);

  //// null pointer, invalid param
  //src_buffer.reset();
  //ret = encode_compact_rowkey(rkw_b.get_rowkey(), src_buffer);
  //EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  //// buffer not enough
  //src_buffer.set_buf(1);
  //src_buffer.get_position() = 0;
  //ret = encode_compact_rowkey(rkw_b.get_rowkey(), src_buffer);
  //EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);
  //EXPECT_EQ(0, src_buffer.get_position());
  //src_buffer.get_position() = 1;
  //ret = encode_compact_rowkey(rkw_b.get_rowkey(), src_buffer);
  //EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);

  // not supported
  ret = encode_compact_rowkey(rkw_e.get_rowkey(), src_buffer);
  EXPECT_EQ(OB_NOT_SUPPORTED, ret);

  //////////////////////////////

  // null pointer, invalid param
  src_buffer.reset();
  ret = encode_rowkey_len(rk_len, src_buffer);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);

  // buffer not enough
  src_buffer.set_buf(1);
  src_buffer.get_position() = 1;
  ret = encode_rowkey_len(rk_len, src_buffer);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);

  // null pointer, invalid param
  src_buffer.reset();
  ret = encode_table_id(table_id, src_buffer);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);

  // buffer not enough
  src_buffer.set_buf(3);
  src_buffer.get_position() = 1;
  ret = encode_table_id(table_id, src_buffer);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);
  EXPECT_EQ(1, src_buffer.get_position());

  // decode rowkey len
  src_buffer.set_buf(2);
  ret = encode_rowkey_len(rk_len, src_buffer);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1, src_buffer.get_position());

  src_buffer.get_position() = 0;
  src_buffer.get_capacity() = 0;
  ret = decode_rowkey_len(rk_len, src_buffer);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);
  EXPECT_EQ(0, src_buffer.get_position());

  // decode table id
  src_buffer.set_buf(3);
  ret = encode_table_id(table_id, src_buffer);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(3, src_buffer.get_position());

  // src buffer not integrity
  src_buffer.get_position() = 0;
  src_buffer.get_capacity() = 1;
  ret = decode_table_id(table_id, src_buffer);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);
  EXPECT_EQ(0, src_buffer.get_position());

  // src buffer not integrity
  src_buffer.get_position() = 0;
  src_buffer.get_capacity() = 2;
  ret = decode_table_id(table_id, src_buffer);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);
  EXPECT_EQ(0, src_buffer.get_position());

  //////////////////////////////

  ObRowkeyWrapper decode_rkw(1);
  int64_t v = 0x0100abcddcba0001;
  RK rkw_v4decode(V(reinterpret_cast<char *>(&v), 3));

  src_buffer.set_buf(7);
  ret = encode_compact_rowkey(rkw_v4decode.get_rowkey(), src_buffer);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(7, src_buffer.get_position());

  // check position after decode
  dst_buffer.set_buf(7);
  src_buffer.get_position() = 0;
  src_buffer.get_capacity() = 7;
  ret = decode_compact_rowkey(decode_rkw.get_rowkey(), src_buffer, dst_buffer);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(7, src_buffer.get_position());
  EXPECT_EQ(3, dst_buffer.get_position());

  // dst buffer not enough
  dst_buffer.set_buf(2);
  src_buffer.get_position() = 0;
  src_buffer.get_capacity() = 7;
  ret = decode_compact_rowkey(decode_rkw.get_rowkey(), src_buffer, dst_buffer);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);
  EXPECT_EQ(0, src_buffer.get_position());
  EXPECT_EQ(0, dst_buffer.get_position());

  // src buffer not integrity
  dst_buffer.set_buf(7);
  src_buffer.get_position() = 0;
  src_buffer.get_capacity() = 1;
  ret = decode_compact_rowkey(decode_rkw.get_rowkey(), src_buffer, dst_buffer);
  EXPECT_EQ(OB_ERR_UNEXPECTED, ret);
  EXPECT_EQ(0, src_buffer.get_position());
  EXPECT_EQ(0, dst_buffer.get_position());

  // src buffer not integrity
  dst_buffer.set_buf(7);
  src_buffer.get_position() = 0;
  src_buffer.get_capacity() = 2;
  ret = decode_compact_rowkey(decode_rkw.get_rowkey(), src_buffer, dst_buffer);
  EXPECT_EQ(OB_ERR_UNEXPECTED, ret);
  EXPECT_EQ(0, src_buffer.get_position());
  EXPECT_EQ(0, dst_buffer.get_position());

  // src buffer break
  dst_buffer.set_buf(7);
  src_buffer.get_position() = 0;
  src_buffer.get_capacity() = 7;
  src_buffer.get_data()[6] = '\2';
  ret = decode_compact_rowkey(decode_rkw.get_rowkey(), src_buffer, dst_buffer);
  EXPECT_EQ(OB_ERR_UNEXPECTED, ret);
  EXPECT_EQ(0, src_buffer.get_position());
  EXPECT_EQ(0, dst_buffer.get_position());

  // invalid type
  dst_buffer.set_buf(7);
  src_buffer.get_position() = 0;
  src_buffer.get_capacity() = 7;
  src_buffer.get_data()[0] = static_cast<char>(0xff - 1);
  ret = decode_compact_rowkey(decode_rkw.get_rowkey(), src_buffer, dst_buffer);
  EXPECT_EQ(OB_NOT_SUPPORTED, ret);
  EXPECT_EQ(0, src_buffer.get_position());
  EXPECT_EQ(0, dst_buffer.get_position());

  //////////////////////////////

  RK rkw_i4decode(I(0x0100abcd));

  src_buffer.set_buf(6);
  ret = encode_compact_rowkey(rkw_i4decode.get_rowkey(), src_buffer);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(6, src_buffer.get_position());

  // check position after decode
  dst_buffer.set_buf(6);
  src_buffer.get_position() = 0;
  src_buffer.get_capacity() = 6;
  ret = decode_compact_rowkey(decode_rkw.get_rowkey(), src_buffer, dst_buffer);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(6, src_buffer.get_position());
  EXPECT_EQ(0, dst_buffer.get_position());

  // src buffer not integrity
  dst_buffer.set_buf(6);
  src_buffer.get_position() = 0;
  src_buffer.get_capacity() = 1;
  ret = decode_compact_rowkey(decode_rkw.get_rowkey(), src_buffer, dst_buffer);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);
  EXPECT_EQ(0, src_buffer.get_position());
  EXPECT_EQ(0, dst_buffer.get_position());

  // src buffer not integrity
  dst_buffer.set_buf(6);
  src_buffer.get_position() = 0;
  src_buffer.get_capacity() = 2;
  ret = decode_compact_rowkey(decode_rkw.get_rowkey(), src_buffer, dst_buffer);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);
  EXPECT_EQ(0, src_buffer.get_position());
  EXPECT_EQ(0, dst_buffer.get_position());
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_rowkey_codec.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
