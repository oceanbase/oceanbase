/**
 * Copyright (c) 2026 OceanBase
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

#include "share/table/ob_table.h"
#include "share/table/ob_table_object.h"

using namespace oceanbase::common;
using namespace oceanbase::table;

TEST(TestHbaseDeserialize, reject_missing_table_objects)
{
  char null_obj[] = {static_cast<char>(ObTableNullType)};
  ObObj obj;
  int64_t pos = sizeof(null_obj);
  EXPECT_EQ(OB_DESERIALIZE_ERROR,
            ObTableSerialUtil::deserialize(null_obj, sizeof(null_obj), pos, obj));
  EXPECT_EQ(sizeof(null_obj), pos);

  pos = -1;
  EXPECT_EQ(OB_DESERIALIZE_ERROR,
            ObTableSerialUtil::deserialize(null_obj, sizeof(null_obj), pos, obj));
  EXPECT_EQ(-1, pos);

  pos = 0;
  EXPECT_EQ(OB_DESERIALIZE_ERROR,
            ObTableSerialUtil::deserialize(nullptr, sizeof(null_obj), pos, obj));
  EXPECT_EQ(0, pos);

  pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            ObTableSerialUtil::deserialize(null_obj, sizeof(null_obj), pos, obj));
  EXPECT_EQ(sizeof(null_obj), pos);
  EXPECT_TRUE(obj.is_null());

  char entity_buf[32];
  int64_t entity_len = 0;
  ASSERT_EQ(OB_SUCCESS,
            serialization::encode_vi64(entity_buf, sizeof(entity_buf), entity_len, 0));
  ASSERT_EQ(OB_SUCCESS,
            serialization::encode_vi64(
                entity_buf, sizeof(entity_buf), entity_len, INT64_C(1) << 62));

  ObSEArray<ObString, 1> names;
  ObTableSingleOpEntity entity;
  entity.set_dictionary(&names, &names);
  pos = 0;
  EXPECT_EQ(OB_DESERIALIZE_ERROR, entity.deserialize_(entity_buf, entity_len, pos));
  EXPECT_EQ(entity_len, pos);
  EXPECT_EQ(0, entity.get_rowkey_size());
  EXPECT_EQ(0, entity.get_properties_count());
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_hbase_deserialize.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
