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
#include "storage/blocksstable/ob_data_buffer.h"
#define protected public
#define private public
#include "storage/blocksstable/ob_macro_block_common_header.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_micro_block_header.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/ob_i_table.h"
#include "storage/ob_storage_struct.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
namespace unittest
{

class TestMockSSTable
{
public:
  static void generate_mock_sstable(
      const int64_t start_log_ts,
      const int64_t end_log_ts,
      ObSSTable &sstable)
  {
    sstable.key_.table_type_ = ObITable::MINOR_SSTABLE;
    sstable.key_.tablet_id_ = 1;
    sstable.key_.log_ts_range_.start_log_ts_ = start_log_ts;
    sstable.key_.log_ts_range_.end_log_ts_ = end_log_ts;
  }
};




//normal condition
//sstable1 log_ts: [0,100)
//sstable2 log_ts: [100, 200)
//sstable3 log_ts: [200,300)
TEST(ObTabletTableStore, sstable_log_ts_range_no_cross_and_continue)
{
  int ret = OB_SUCCESS;
  ObTabletTableStore tablet_table_store;

  ObSSTable sstable1;
  TestMockSSTable::generate_mock_sstable(0, 100, sstable1);

  ObSSTable sstable2;
  TestMockSSTable::generate_mock_sstable(100, 200, sstable2);

  ObSSTable sstable3;
  TestMockSSTable::generate_mock_sstable(200, 300, sstable3);

  ObArray<ObITable *> minor_sstables;
  ret = minor_sstables.push_back(&sstable1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_table_store.cut_ha_sstable_log_ts_range_(minor_sstables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(0, sstable1.key_.log_ts_range_.start_log_ts_);
  ASSERT_EQ(100, sstable1.key_.log_ts_range_.end_log_ts_);

  ASSERT_EQ(100, sstable2.key_.log_ts_range_.start_log_ts_);
  ASSERT_EQ(200, sstable2.key_.log_ts_range_.end_log_ts_);

  ASSERT_EQ(200, sstable3.key_.log_ts_range_.start_log_ts_);
  ASSERT_EQ(300, sstable3.key_.log_ts_range_.end_log_ts_);
}


//sstable log ts is not continue with other sstable log ts
//sstable1 log_ts: [0,100)
//sstable2 log_ts: [200, 300)
//sstable3 log_ts: [300,500)

TEST(ObTabletTableStore, sstable_log_ts_range_is_not_continue)
{
  int ret = OB_SUCCESS;
  ObTabletTableStore tablet_table_store;

  ObSSTable sstable1;
  TestMockSSTable::generate_mock_sstable(0, 100, sstable1);

  ObSSTable sstable2;
  TestMockSSTable::generate_mock_sstable(200, 300, sstable2);

  ObSSTable sstable3;
  TestMockSSTable::generate_mock_sstable(300, 500, sstable3);

  ObArray<ObITable *> minor_sstables;
  ret = minor_sstables.push_back(&sstable1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_table_store.cut_ha_sstable_log_ts_range_(minor_sstables);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
}


//sstable log ts contain other sstable log ts
//sstable1 log_ts: [0,100)
//sstable2 log_ts: [0, 200)
//sstable3 log_ts: [200,500)

TEST(ObTabletTableStore, sstable_log_ts_range_contain)
{
  int ret = OB_SUCCESS;
  ObTabletTableStore tablet_table_store;

  ObSSTable sstable1;
  TestMockSSTable::generate_mock_sstable(0, 100, sstable1);

  ObSSTable sstable2;
  TestMockSSTable::generate_mock_sstable(0, 200, sstable2);

  ObSSTable sstable3;
  TestMockSSTable::generate_mock_sstable(200, 500, sstable3);

  ObArray<ObITable *> minor_sstables;
  ret = minor_sstables.push_back(&sstable1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_table_store.cut_ha_sstable_log_ts_range_(minor_sstables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(0, sstable1.key_.log_ts_range_.start_log_ts_);
  ASSERT_EQ(100, sstable1.key_.log_ts_range_.end_log_ts_);

  ASSERT_EQ(100, sstable2.key_.log_ts_range_.start_log_ts_);
  ASSERT_EQ(200, sstable2.key_.log_ts_range_.end_log_ts_);

  ASSERT_EQ(200, sstable3.key_.log_ts_range_.start_log_ts_);
  ASSERT_EQ(500, sstable3.key_.log_ts_range_.end_log_ts_);

}

//sstable log ts contain other sstable log ts
//sstable1 log_ts: [0,100)
//sstable2 log_ts: [50, 200)
//sstable3 log_ts: [200,500)

TEST(ObTabletTableStore, sstable_log_ts_range_has_overlap)
{
  int ret = OB_SUCCESS;
  ObTabletTableStore tablet_table_store;

  ObSSTable sstable1;
  TestMockSSTable::generate_mock_sstable(0, 100, sstable1);

  ObSSTable sstable2;
  TestMockSSTable::generate_mock_sstable(50, 200, sstable2);

  ObSSTable sstable3;
  TestMockSSTable::generate_mock_sstable(200, 500, sstable3);

  ObArray<ObITable *> minor_sstables;
  ret = minor_sstables.push_back(&sstable1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_table_store.cut_ha_sstable_log_ts_range_(minor_sstables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(0, sstable1.key_.log_ts_range_.start_log_ts_);
  ASSERT_EQ(100, sstable1.key_.log_ts_range_.end_log_ts_);

  ASSERT_EQ(100, sstable2.key_.log_ts_range_.start_log_ts_);
  ASSERT_EQ(200, sstable2.key_.log_ts_range_.end_log_ts_);

  ASSERT_EQ(200, sstable3.key_.log_ts_range_.start_log_ts_);
  ASSERT_EQ(500, sstable3.key_.log_ts_range_.end_log_ts_);
}


}//blocksstable
}//oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40L << 30);
  return RUN_ALL_TESTS();
}

