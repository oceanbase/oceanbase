/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "lib/oblog/ob_base_log_writer.h"

#define private public
#include "lib/oblog/ob_log.h"
#undef private

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

TEST(ObLogger, alloc_log_item)
{
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);
  ObPLogItem *log_item = NULL;
  const int64_t buf_size = ObLogger::BASE_LOG_SIZE + 1024;
  OB_LOGGER.alloc_log_item(OB_LOG_LEVEL_INFO, buf_size + ObLogger::LOG_ITEM_SIZE, log_item);
  ASSERT_EQ(true, log_item != NULL);
  ASSERT_EQ(false, log_item->is_large_item());
  ASSERT_EQ(true, ObLogger::BASE_LOG_SIZE >= log_item->get_buf_size());
}


}
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
