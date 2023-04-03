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
#include <thread>
#include "lib/oblog/ob_base_log_writer.h"
#include "lib/ob_errno.h"

//using namespace ::oblib;

namespace oceanbase
{
namespace common
{
class ObTLogItem : public ObIBaseLogItem
{
public:
  ObTLogItem() {}
  virtual ~ObTLogItem() {}
  virtual char *get_buf() { return a_; }
  virtual const char *get_buf() const { return NULL; }
  virtual int64_t get_buf_size() const { return 0; }
  virtual int64_t get_data_len() const { return 0; }
  char a_[16];
};

class ObTLogWriter : public ObBaseLogWriter
{
public:
  ObTLogWriter() : process_cnt_(0) {}
  virtual ~ObTLogWriter() {}
  int64_t process_cnt_;
protected:
  virtual void process_log_items(ObIBaseLogItem **items, const int64_t item_cnt, int64_t &finish_cnt);
};

void ObTLogWriter::process_log_items(ObIBaseLogItem **items, const int64_t item_cnt, int64_t &finish_cnt)
{
  if (NULL != items) {
    finish_cnt = item_cnt;
    process_cnt_ += item_cnt;
    ObTLogItem* a = (ObTLogItem*)(items[finish_cnt - 1]);
    (a->get_buf())[15] = '@';
  }
}

TEST(ObBaseLogWriter, normal)
{
  int ret = OB_SUCCESS;
  ObTLogWriter writer;
  ObBaseLogWriterCfg cfg;
  ObTLogItem log_item;
  int64_t process_cnt = 0;

  //invoke when not init
  ret = writer.append_log(log_item);
  ASSERT_NE(OB_SUCCESS, ret);

  //normal init
  cfg = ObBaseLogWriterCfg(512 << 10, 500000, 1, 4);
  ret = writer.init(cfg);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.start();

  //repeat init
  ret = writer.init(cfg);
  ASSERT_NE(OB_SUCCESS, ret);

  //normal append
  ret = writer.append_log(log_item, 10000000);
  //ASSERT_EQ(OB_SUCCESS, ret);
  ++process_cnt;

  //multi append
  //constexpr int cnt = 64;
  //std::thread threads[cnt];
  //for (auto j = 0; j < cnt; ++j) {
  //  threads[j] = std::thread([&]() {
  //    for (int64_t i = 0; i < 100000; ++i) {
  //      ret = writer.append_log(log_item, 10000000);
  //      //ASSERT_EQ(OB_SUCCESS, ret);
  //      process_cnt++;
  //    }
  //  });
  //}

  //run multi append again
  for (int64_t i = 0; i < 100000; ++i) {
    ret = writer.append_log(log_item, 10000000);
    //ASSERT_EQ(OB_SUCCESS, ret);
    process_cnt++;
  }
  //for (auto j = 0; j < cnt; ++j) {
  //  threads[j].join();
  //}

  //destroy and init
  writer.stop();
  writer.wait();
  writer.destroy();
  ret = writer.init(cfg);
  ASSERT_EQ(OB_SUCCESS, ret);

  //repeat destroy
  writer.destroy();
  writer.destroy();
}


}
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
