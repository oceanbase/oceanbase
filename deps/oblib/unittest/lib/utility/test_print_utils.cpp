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

#include <gtest/gtest.h>
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common;
/*
TEST(print_utility, hex_print)
{
  const int64_t data_size = 10;
  char data[10];
  for (int64_t i = 0; i < 10; ++i) {
    data[i] = (char)(random()%255);
  }
  char buff_h[21];
  char buff_p[21];
  int64_t pos = 0;
  int ret = hex_print(data, data_size, buff_h, 20, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(buff_h[20], '\0');
  ASSERT_EQ(strlen(buff_h), 20);
  char *bf = buff_p;
  int64_t len = 0;
  for (int64_t i = 0; i < 10; ++i) {
    len = snprintf(bf, 20 - i*2, "%02X", (unsigned char)data[i]);
    bf += len;
  }
  ASSERT_EQ(buff_p[19], '\0');
  pos = 0;
  ret = hex_print(data, data_size, buff_h, 21, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(buff_h[20], '\0');
  bf = buff_p;
  for (int64_t i = 0; i < 10; ++i) {
    len = snprintf(bf, 21 - i*2, "%02X", (unsigned char)data[i]);
    bf += len;
  }
  ASSERT_EQ(strcasecmp(buff_h, buff_p), 0);

  pos = 0;
  ret = hex_print(NULL, data_size, buff_h, 21, pos);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  pos = 0;
  ret = hex_print(data, -1, buff_h, 21, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
}*/

template <typename ...T>
class ObTuple
{
public:
  template <typename ...Args>
  ObTuple(Args &&...args) : tuple_(std::forward<Args>(args)...) {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    print_<0>(buf, buf_len, pos);
    return pos;
  }
  template <int N>
  int64_t print_(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    if (N == 0) {
      databuff_printf(buf, buf_len, pos, "{");
    }
    databuff_printf(buf, buf_len, pos, std::get<N>(tuple_));
    databuff_printf(buf, buf_len, pos, ",");
    print_<N+1>(buf, buf_len, pos);
    return pos;
  }
  template <>
  int64_t print_<sizeof...(T) - 1>(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    databuff_printf(buf, buf_len, pos, std::get<sizeof...(T) - 1>(tuple_));
    databuff_printf(buf, buf_len, pos, "}");
    return pos;
  }
private:
  std::tuple<T...> tuple_;
};

class ObSimpleObj
{
public:
  ObSimpleObj(int _n, char _ch) : n(_n), ch(_ch) {}
  virtual int64_t to_string(char *buf, const int64_t buf_len) const
  {
    return snprintf(buf, buf_len, "{n=%d, ch=%c}", n, ch);
  }

private:
  int n;
  char ch;
};

class ObComplexObj
{
public:
  ObComplexObj(ObSimpleObj *_p_simple_obj = nullptr) : data_len(0), p_simple_obj(_p_simple_obj)
  {
    memset(data, 0, sizeof(data));
  }
  void set_data(const char *src)
  {
    if (src) {
      data_len = snprintf(data, sizeof(data), "%s", src);
    }
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "{data=%s, data_len=%hu, simple_obj=", data, data_len);
    // call  int databuff_printf(char *buf, int64_t buf_len, int64_t &pos, T *p_obj)
    databuff_printf(buf, buf_len, pos, p_simple_obj);
    databuff_printf(buf, buf_len, pos, "}");
    return pos;
  }

private:
  char data[32];
  uint8_t data_len;
  const ObSimpleObj *p_simple_obj;
};

template <int64_t N, char CH>
class RepetitiveLetter
{
public:
  RepetitiveLetter()
  {
    if (N > 0) {
        memset(ch_, CH, N);
    }
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int pos = 0;
    if (nullptr != buf && buf_len > 0) {
        int64_t len = buf_len < N ? buf_len : N;
        // not ending with '\0'
        memset(buf, CH, len);
        pos = len;
    }
    return pos;
  }
  bool is_same_with(const char *buf, const int64_t buf_len) const
  {
    bool is_same = true;
    if (nullptr != buf && buf_len > 0 && buf_len == N) {
        is_same = (0 == strncmp(ch_, buf, buf_len));
    } else {
        is_same = false;
    }
    return is_same;
  }

private:
char ch_[N];
};

template <uint64_t N>
class SimpleCString
{
public:
  SimpleCString()
  {
    memset(buf_, 0, N);
  }
  void fill(char ch)
  {
    memset(buf_, ch, N - 1);
    buf_[N - 1] = '\0';
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "%s", buf_);
    return pos;
  }
private:
  char buf_[N];
};

TEST(print_utility, convert)
{
  typedef ObTuple<ObString,int64_t> MyTuple;
  const int size = 1300;
  const int number = 10;
  char data[size * number];
  char *buffer = (char*)ob_malloc(sizeof(MyTuple) * number, ObNewModIds::TEST);
  MyTuple *tuples[number];
  for (int n = 0; n < number; ++n) {
    memset(&data[n * size], 'a' + n, size - 1);
    data[size * (n+1) - 1] = '\0';
    tuples[n] = new (buffer + sizeof(MyTuple) * n) MyTuple(data + size * n, n);
  }
  // mutiply call helper.convert at the same time
  ObCStringHelper helper;
  _OB_LOG(INFO, "print tuple string, {%s}, {%s}, {%s}",
      helper.convert(*tuples[0]), helper.convert(*tuples[1]), helper.convert(*tuples[2]));
  _OB_LOG(INFO, "print tuple string, {%s}, {%s}, {%s}, {%s}, {%s}, {%s}, {%s}",
      helper.convert(*tuples[3]), helper.convert(*tuples[4]), helper.convert(*tuples[5]),
      helper.convert(*tuples[6]), helper.convert(*tuples[7]), helper.convert(*tuples[8]),
      helper.convert(*tuples[9]));
  // the performance of convert when observer reach memory limit
  EventItem item;
  item.trigger_freq_ = 1;
  item.error_code_ = OB_ALLOCATE_MEMORY_FAILED;
  ::oceanbase::common::EventTable::instance().set_event(EventTable::EN_4, item);
  _OB_LOG(INFO, "print tuple string, {%s}\n", helper.convert(*tuples[0]));
}

TEST(print_utility, print_functions)
{
  int ret = OB_SUCCESS;
  unsigned long long num = 100ULL;
  ObAddr addr1;
  ObAddr addr2(ObAddr::IPV4, "255.255.255.255", 16673);
  ObSimpleObj simple_obj(10, 'A');
  ObComplexObj complex_obj_1(&simple_obj);
  complex_obj_1.set_data("Oceanbase");
  ObComplexObj complex_obj_2(nullptr);


  int64_t pos = 0;
  char buf[1024] = {'\0'};
  int64_t buf_len = sizeof(buf);
  ret = databuff_printf(buf, buf_len, pos, "INFO: {num=%llu, addr1=", num);
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, addr1);
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", addr2=");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, addr2);
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", complex_obj_1=");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, complex_obj_1);
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", complex_obj_2=");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, complex_obj_2);
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, "}");
  ASSERT_EQ(ret, OB_SUCCESS);
  OB_LOG(INFO, "in the case of buf enough: ", K(buf), K(pos), K(ret));

  pos = 0;
  char short_buf[64] = {'\0'};
  buf_len = sizeof(short_buf);
  ret = databuff_printf(short_buf, buf_len, pos, "INFO: {num=%llu, addr1=", num);
  OB_SUCCESS != ret ? : ret = databuff_printf(short_buf, buf_len, pos, addr1);
  OB_SUCCESS != ret ? : ret = databuff_printf(short_buf, buf_len, pos, ", addr2=");
  OB_SUCCESS != ret ? : ret = databuff_printf(short_buf, buf_len, pos, addr2);
  OB_SUCCESS != ret ? : ret = databuff_printf(short_buf, buf_len, pos, ", complex_obj_1=");
  OB_SUCCESS != ret ? : ret = databuff_printf(short_buf, buf_len, pos, complex_obj_1);
  OB_SUCCESS != ret ? : ret = databuff_printf(short_buf, buf_len, pos, ", complex_obj_2=");
  OB_SUCCESS != ret ? : ret = databuff_printf(short_buf, buf_len, pos, complex_obj_2);
  OB_SUCCESS != ret ? : ret = databuff_printf(short_buf, buf_len, pos, "}");
  ASSERT_EQ(ret, OB_SIZE_OVERFLOW);
  OB_LOG(INFO, "in the case of buf not enough: ", K(short_buf), K(pos), K(ret));

  // test truncate
  char thin_buf[8] = {'\0'};
  RepetitiveLetter<7, 'A'> rl7;
  pos = 0;
  ret = databuff_printf(thin_buf, sizeof(thin_buf), pos, rl7);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(7, pos);
  ASSERT_EQ(0, strcmp(thin_buf, "AAAAAAA"));
  RepetitiveLetter<8, 'A'> rl8;
  pos = 0;
  memset(thin_buf, 0, sizeof(thin_buf));
  ret = databuff_printf(thin_buf, sizeof(thin_buf), pos, rl8);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);
  ASSERT_EQ(7, pos);
  ASSERT_EQ(0, strcmp(thin_buf, "AAAAAAA"));

  // test boolean
  bool flag1 = true;
  bool flag2 = false;
  pos = 0;
  buf_len = sizeof(buf);
  ret = databuff_printf(buf, buf_len, pos, "{flag1=");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, flag1);
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", flag2=");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, flag2);
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", (3 > 1)=");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, 3 > 1);
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", (3 < 1)=");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, 3 < 1);
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, "}");
  ASSERT_EQ(ret, OB_SUCCESS);
  OB_LOG(INFO, "test boolean: ", K(buf), K(pos), K(ret));

  // test pointer
  const ObComplexObj *ptr_complex_obj = &complex_obj_1;
  const ObComplexObj *ptr_nullptr = nullptr;
  pos = 0;
  buf_len = sizeof(buf);
  ret = databuff_printf(buf, buf_len, pos, "{ptr_complex_obj=");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ptr_complex_obj);
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", ptr_nullptr=");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ptr_nullptr);
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, "}");
  ASSERT_EQ(ret, OB_SUCCESS);
  OB_LOG(INFO, "test pointer: ", K(buf), K(pos), K(ret));

  // test databuff_print_multi_objs
  pos = 0;
  buf_len = sizeof(buf);
  uint64_t num_u64 = 0xffffffffffffffff;
  int64_t num_i64 = 0x7fffffffffffffff;
  uint32_t num_u32 = 0xffffffff;
  int32_t num_i32 = 0x7fffffff;
  uint16_t num_u16 = 0xffff;
  int16_t num_i16 = 0x7fff;
  uint8_t num_u8 = 0xff;
  int8_t num_i8 = 0x7f;
  char ch = 'A';
  double num_double = 1000000 / 3;
  float num_float = (float)(1000000/3);
  if (OB_FAIL(databuff_print_multi_objs(buf, buf_len, pos,
      "{num_u64=", num_u64, ", num_i64=", num_i64, ", num_u32=", num_u32, ", num_i32=", num_i32,
      ", num_u16=", num_u16, ", num_i16=", num_i16, ", num_u8=", num_u8, ", num_i8=", num_i8,
      ", ch=", ch, ", num_double=", num_double, ", num_float=", num_float))) {
    OB_LOG(WARN, "call databuff_print_multi_objs fail", K(buf), K(pos), K(ret));
  } else if (OB_FAIL(databuff_print_multi_objs(buf, buf_len, pos,
      ", flag1=", flag1, ", flag2=", flag2, ", (3 > 1)=", 3 > 1, ", (3 < 1)=", 3 < 1))) {
    OB_LOG(WARN, "call databuff_print_multi_objs fail", K(buf), K(pos), K(ret));
  } else if (OB_FAIL(databuff_print_multi_objs(buf, buf_len, pos,
      ", ptr_complex_obj=", ptr_complex_obj, ", ptr_nullptr=", ptr_nullptr))) {
    OB_LOG(WARN, "call databuff_print_multi_objs fail", K(buf), K(pos), K(ret));
  } else if (OB_FAIL(databuff_print_multi_objs(buf, buf_len, pos,
      ", addr1=", addr1, ", addr2=", addr2,
      ", complex_obj_1=", complex_obj_1, ", complex_obj_2=", complex_obj_2, "}"))) {
    OB_LOG(WARN, "call databuff_print_multi_objs fail", K(buf), K(pos), K(ret));
  } else {
    OB_LOG(INFO, "test databuff_print_multi_objs: ", K(buf), K(pos), K(ret));
  }
  ASSERT_EQ(ret, OB_SUCCESS);

  // test ObCStringHelper
  ObCStringHelper helper;
  OB_LOG(INFO, "test ObCStringHelper: ", "addr1", helper.convert(addr1),
      "addr2", helper.convert(addr2), "ptr_complex_obj", helper.convert(ptr_complex_obj),
      "ptr_nullptr", helper.convert(ptr_nullptr), "complex_obj_1", helper.convert(complex_obj_1),
      "complex_obj_2", helper.convert(complex_obj_2));
  // test ObCStringHelper (bad case)
  ObCStringHelper helper_bad;
  RepetitiveLetter<1023, 'A'> rl;
  for (int i = 0; i < (64 << 11); ++i)
  {
    const char *ptr = helper_bad.convert(rl);
    if (nullptr != ptr) {
      ASSERT_EQ(true, rl.is_same_with(ptr, strlen(ptr)));
    } else {
      break;
    }
  }
  const char *ptr = helper_bad.convert(rl);
  ASSERT_EQ(nullptr == ptr, true);
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, helper_bad.get_ob_errno()); // test get_ob_errno

  // test ObCStringHelper (over 1024)
  ObCStringHelper helper_scs;
  SimpleCString<1026> scs1;
  scs1.fill('A');
  const char *scs_ptr_1 = helper_scs.convert(scs1);
  ASSERT_EQ(nullptr != scs_ptr_1, true);
  ASSERT_EQ(1025, strlen(scs_ptr_1));
  ASSERT_EQ(OB_SUCCESS, helper.get_ob_errno());

  // test ObCStringHelper (reset)
  helper_scs.reset();
  scs_ptr_1 = helper_scs.convert(scs1);
  ASSERT_EQ(nullptr != scs_ptr_1, true);
  ASSERT_EQ(1025, strlen(scs_ptr_1));
  ASSERT_EQ(OB_SUCCESS, helper.get_ob_errno());

  // test ObCStringHelper (convert that returns errno)
  helper_scs.reset();
  scs_ptr_1 = nullptr;
  ASSERT_EQ(OB_SUCCESS, helper_scs.convert(scs1, scs_ptr_1));
  ASSERT_EQ(nullptr != scs_ptr_1, true);
  ASSERT_EQ(1025, strlen(scs_ptr_1));
  ASSERT_EQ(OB_SUCCESS, helper.get_ob_errno());
}

int main(int argc, char **argv)
{
  system("rm -rf test_print_utility.log");

  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_print_utility.log", true);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
