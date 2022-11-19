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
#include "common/ob_range.h"
#include "common/ob_store_range.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_define.h"

using namespace oceanbase::common;

TEST(TestObRange, test_to_store_range)
{
  ObArenaAllocator allocator;
  ObNewRange range;
  ObStoreRange store_range;
  int ret = OB_SUCCESS;
  int rowkey_cnt = 2;
  ObObj start_objs[3];
  ObObj end_objs[3];

  ObSEArray<ObOrderType, OB_MAX_ROWKEY_COLUMN_NUMBER> all_asc_orders;
  ObSEArray<ObOrderType, OB_MAX_ROWKEY_COLUMN_NUMBER> asc_desc_orders;
  all_asc_orders.push_back(ObOrderType::ASC);
  all_asc_orders.push_back(ObOrderType::ASC);
  asc_desc_orders.push_back(ObOrderType::ASC);
  asc_desc_orders.push_back(ObOrderType::DESC);

  //invalid keys cannot be converted
  //ret = range.to_store_range(all_asc_orders, rowkey_cnt, store_range, allocator);
  //ASSERT_EQ(ret, OB_INVALID_ARGUMENT);

  // cannot be converted if range key length does not match rowkey_cnt
  range.start_key_.assign(start_objs, 3);
  range.end_key_.assign(end_objs, 3);
  //ret = range.to_store_range(all_asc_orders, rowkey_cnt, store_range, allocator);
  //ASSERT_EQ(ret, OB_INVALID_ARGUMENT);

  start_objs[0].set_int(1);
  start_objs[1].set_min_value();
  range.start_key_.assign(start_objs, rowkey_cnt);
  end_objs[0].set_int(9);
  end_objs[1].set_max_value();
  range.end_key_.assign(end_objs, rowkey_cnt);
  range.table_id_ = 999;
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();

  //ret = range.to_store_range(all_asc_orders, rowkey_cnt, store_range, allocator);
  //ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(store_range.get_table_id(), 999);
  ASSERT_TRUE(store_range.get_start_key().is_valid());
  ASSERT_EQ(1, store_range.get_start_key().get_obj_ptr()[0].v_.int64_);
  ASSERT_TRUE(store_range.get_start_key().get_obj_ptr()[1].is_min_value());
  ASSERT_TRUE(store_range.get_end_key().is_valid());
  ASSERT_EQ(9, store_range.get_end_key().get_obj_ptr()[0].v_.int64_);
  ASSERT_TRUE(store_range.get_end_key().get_obj_ptr()[1].is_max_value());
  ASSERT_FALSE(store_range.get_border_flag().inclusive_start());
  ASSERT_TRUE(store_range.get_border_flag().inclusive_end());


  //ret = range.to_store_range(asc_desc_orders, rowkey_cnt, store_range, allocator);
  //ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(store_range.get_table_id(), 999);
  ASSERT_TRUE(store_range.get_start_key().is_valid());
  ASSERT_EQ(1, store_range.get_start_key().get_obj_ptr()[0].v_.int64_);
  ASSERT_TRUE(store_range.get_start_key().get_obj_ptr()[1].is_max_value());
  ASSERT_TRUE(store_range.get_end_key().is_valid());
  ASSERT_EQ(9, store_range.get_end_key().get_obj_ptr()[0].v_.int64_);
  ASSERT_TRUE(store_range.get_end_key().get_obj_ptr()[1].is_min_value());
  ASSERT_TRUE(store_range.get_border_flag().inclusive_start());
  ASSERT_FALSE(store_range.get_border_flag().inclusive_end());
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}
