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
#include <tuple>
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
    databuff_printf(buf, buf_len, pos, "%s,", to_cstring(std::get<N>(tuple_)));
    print_<N+1>(buf, buf_len, pos);
    return pos;
  }
  template <>
  int64_t print_<sizeof...(T) - 1>(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    databuff_printf(buf, buf_len, pos, "%s}", to_cstring(std::get<sizeof...(T) - 1>(tuple_)));
    return pos;
  }
private:
  std::tuple<T...> tuple_;
};
TEST(print_utility, to_cstring)
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
  // mutiply call to_cstring at the same time
  _OB_LOG(INFO, "print tuple string, {%s}, {%s}, {%s}",
               to_cstring(*tuples[0]), to_cstring(*tuples[1]), to_cstring(*tuples[2]));
  _OB_LOG(INFO, "print tuple string, {%s}, {%s}, {%s}, {%s}, {%s}, {%s}, {%s}", to_cstring(*tuples[3]), to_cstring(*tuples[4]),
               to_cstring(*tuples[5]), to_cstring(*tuples[6]), to_cstring(*tuples[7]), to_cstring(*tuples[8]), to_cstring(*tuples[9]));
  // the performance of to_cstring when observer reach memory limit
  EventItem item;
  item.trigger_freq_ = 1;
  item.error_code_ = OB_ALLOCATE_MEMORY_FAILED;
  ::oceanbase::common::EventTable::instance().set_event(EventTable::EN_4, item);
  _OB_LOG(INFO, "print tuple string, {%s}\n", to_cstring(*tuples[0]));
}

int main(int argc, char **argv)
{
  system("rm -rf test_print_utility.log");

  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_print_utility.log", true);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}