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
#define private public
#include "storage/access/ob_multiple_merge.h"
#include "storage/blocksstable/ob_sstable.h"
#undef private

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace ::testing;

namespace unittest
{
class ObMultipleMergeTest : public ::testing::Test
{
public:
  ObMultipleMergeTest() = default;
  virtual ~ObMultipleMergeTest() = default;
  bool check_table_continues(const common::ObIArray<ObITable *> &tables);
};

TEST_F(ObMultipleMergeTest, test_sort_sstables_overlap)
{
  int ret = OB_SUCCESS;
  ObTablesHandle tables_handle;
  const int64_t TABLES_CNT = 8;
  ObSSTable table[TABLES_CNT];
  ObITable::TableKey table_key;
  int64_t table_id = combine_id(1, 3001);
  table_key.table_type_ = ObITable::MAJOR_SSTABLE;
  table_key.tablet_id_ = table_id;
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.snapshot_version_ = 10;
  ASSERT_EQ(OB_SUCCESS, table[0].init(table_key));
  ASSERT_EQ(OB_SUCCESS, table[1].init(table_key));
  table_key.trans_version_range_.base_version_ = 8;
  table_key.trans_version_range_.snapshot_version_ = 30;
  ASSERT_EQ(OB_SUCCESS, table[2].init(table_key));
  ASSERT_EQ(OB_SUCCESS, table[3].init(table_key));
  table_key.trans_version_range_.base_version_ = 10;
  table_key.trans_version_range_.snapshot_version_ = 20;
  ASSERT_EQ(OB_SUCCESS, table[4].init(table_key));
  ASSERT_EQ(OB_SUCCESS, table[5].init(table_key));
  table_key.trans_version_range_.base_version_ = 30;
  table_key.trans_version_range_.snapshot_version_ = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, table[6].init(table_key));
  ASSERT_EQ(OB_SUCCESS, table[7].init(table_key));
  for (int64_t i = 0; i < TABLES_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&table[i]));
  }
  ObMultipleMerge::ObTableCompartor compartor(ret);
  std::sort(tables_handle.table_begin(), tables_handle.table_end(), compartor);
  const ObIArray<ObITable *> &tables = tables_handle.get_tables();
  int64_t pos = tables.count() - 1;
  const ObITable *last_table = tables.at(pos--);
  const ObITable *cur_table = NULL;

  for (; OB_SUCC(ret) && pos >= 0; --pos) {
    cur_table = tables.at(pos);
    if (OB_ISNULL(cur_table)
        || OB_ISNULL(last_table)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "cur_table or last_table is NULL", K(ret), KP(cur_table), KP(last_table));
    } else if (OB_UNLIKELY(last_table->get_base_version() > cur_table->get_snapshot_version())) {

      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "tables' version not match", K(ret),
          "last table base version", last_table->get_base_version(),
          "cur table snapshot version", cur_table->get_snapshot_version());
    }
    last_table = cur_table;
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
