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
#include "lib/oblog/ob_base_log_writer.h"
#include "lib/ob_errno.h"

using namespace ::oblib;

namespace oceanbase {
namespace common {
class ObTLogItem : public ObIBaseLogItem {
public:
  ObTLogItem()
  {}
  virtual ~ObTLogItem()
  {}
  virtual char* get_buf()
  {
    return NULL;
  }
  virtual const char* get_buf() const
  {
    return NULL;
  }
  virtual int64_t get_buf_size() const
  {
    return 0;
  }
  virtual int64_t get_data_len() const
  {
    return 0;
  }
};

class ObTLogWriter : public ObBaseLogWriter {
public:
  ObTLogWriter() : process_cnt_(0)
  {}
  virtual ~ObTLogWriter()
  {}
  int64_t process_cnt_;

protected:
  virtual void process_log_items(ObIBaseLogItem** items, const int64_t item_cnt, int64_t& finish_cnt);
};

void ObTLogWriter::process_log_items(ObIBaseLogItem** items, const int64_t item_cnt, int64_t& finish_cnt)
{
  if (NULL != items) {
    finish_cnt = item_cnt;
    process_cnt_ += item_cnt;
  }
}

TEST(ObBaseLogWriter, normal)
{
  int ret = OB_SUCCESS;
  ObTLogWriter writer;
  ObBaseLogWriterCfg cfg;
  ObTLogItem log_item;
  int64_t process_cnt = 0;

  // invalid argument
  ret = writer.init(cfg);
  ASSERT_NE(OB_SUCCESS, ret);

  // invoke when not init
  ret = writer.append_log(log_item);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = writer.reconfig(cfg);
  ASSERT_NE(OB_SUCCESS, ret);

  // normal init
  cfg.group_commit_max_wait_us_ = 1;
  cfg.group_commit_min_item_cnt_ = 1;
  cfg.group_commit_max_item_cnt_ = 1;
  ret = writer.init(cfg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // repeat init
  ret = writer.init(cfg);
  ASSERT_NE(OB_SUCCESS, ret);

  // normal append
  ret = writer.append_log(log_item, 10000000);
  ASSERT_EQ(OB_SUCCESS, ret);
  ++process_cnt;

  // multi append
  for (int64_t i = 0; i < 10000; ++i) {
    ret = writer.append_log(log_item, 10000000);
    ASSERT_EQ(OB_SUCCESS, ret);
    process_cnt++;
  }

  // invalid reconfig
  cfg.group_commit_max_item_cnt_ = 0;
  ret = writer.reconfig(cfg);
  ASSERT_NE(OB_SUCCESS, ret);

  // normal reconfig
  cfg.group_commit_min_item_cnt_ = cfg.max_buffer_item_cnt_;
  cfg.group_commit_max_item_cnt_ = cfg.max_buffer_item_cnt_;
  ret = writer.reconfig(cfg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // run multi append again
  for (int64_t i = 0; i < 100000; ++i) {
    ret = writer.append_log(log_item, 10000000);
    ASSERT_EQ(OB_SUCCESS, ret);
    process_cnt++;
  }

  // destroy and init
  writer.destroy();
  ret = writer.init(cfg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // repeat destroy
  writer.destroy();
  writer.destroy();
}

}  // namespace common
}  // namespace oceanbase

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
