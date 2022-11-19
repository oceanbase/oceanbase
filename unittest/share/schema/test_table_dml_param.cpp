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

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#define private public
#include "share/schema/ob_table_dml_param.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace schema;

TEST(ObTableSchemaParam, test_serialize)
{
  ObArenaAllocator allocator1;
  ObArenaAllocator allocator2;
  ObTableSchemaParam old_param1(allocator1);
  ObTableSchemaParam new_param1(allocator1);
  ObTableSchemaParam new_param2(allocator2);

  old_param1.table_id_ = 1;
  old_param1.index_status_ = INDEX_STATUS_AVAILABLE;
  ObArenaAllocator allocator;
  int64_t buf_size = 1024;
  char *buf = (char*)allocator.alloc(buf_size);
  int64_t pos = 0;

  LOG_INFO("dump param", K(old_param1));
  ASSERT_EQ(OB_SUCCESS, old_param1.serialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  ASSERT_EQ(OB_SUCCESS, old_param1.serialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_param1.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(new_param1), K(pos), K(buf_size));
  ASSERT_EQ(OB_SUCCESS, new_param2.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(new_param2), K(pos), K(buf_size));
  ASSERT_EQ(old_param1.table_id_, new_param1.table_id_);
  ASSERT_EQ(old_param1.table_id_, new_param2.table_id_);

  new_param1.table_id_ = 2;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_param1.serialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  ASSERT_EQ(OB_SUCCESS, new_param2.serialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_param1.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  ASSERT_EQ(new_param1.table_id_, old_param1.table_id_);
  ASSERT_EQ(OB_SUCCESS, old_param1.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(old_param1), K(pos), K(buf_size));
  ASSERT_EQ(new_param2.table_id_, old_param1.table_id_);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_param2.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(new_param2), K(pos), K(buf_size));
  ASSERT_EQ(OB_SUCCESS, new_param2.deserialize(buf, buf_size, pos));
  LOG_INFO("dump param", K(new_param2), K(pos), K(buf_size));

}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
