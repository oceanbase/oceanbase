/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "rootserver/ddl_task/ob_index_build_task.h"
#include "share/ob_cluster_version.h"

#define LOG_FILE_PATH "./test_fts_rowkey_doc_snapshot_hold.log"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::share::schema;

static const uint64_t TEST_ROWKEY_DOC_TABLE_ID = 200001;
static const uint64_t TEST_DATA_TABLET_ID = 300001;
static const uint64_t TEST_TARGET_TABLET_ID = 300002;

TEST(TestFtsRowkeyDocSnapshotHold, route_fts_children_to_rowkey_doc_snapshot_lookup)
{
  ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);

  ObTableSchema data_table_schema;
  ASSERT_EQ(OB_SUCCESS, data_table_schema.add_simple_index_info(
      ObAuxTableMetaInfo(TEST_ROWKEY_DOC_TABLE_ID,
                         USER_INDEX,
                         INDEX_TYPE_ROWKEY_DOC_ID_LOCAL)));

  ObIndexBuildTask task;
  // Invalid tenant id makes ObDDLUtil::get_tablets() return deterministically,
  // so the test can observe whether the production path reaches rowkey-doc lookup
  // without adding a test-only injection interface to ObIndexBuildTask.
  task.tenant_id_ = OB_INVALID_ID;
  const ObIndexType fts_child_index_types[] = {
    INDEX_TYPE_DOC_ID_ROWKEY_LOCAL,
    INDEX_TYPE_FTS_INDEX_LOCAL,
    INDEX_TYPE_FTS_DOC_WORD_LOCAL
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(fts_child_index_types); ++i) {
    ObSEArray<ObTabletID, 3> snapshot_tablet_ids;
    ASSERT_EQ(OB_SUCCESS, snapshot_tablet_ids.push_back(ObTabletID(TEST_DATA_TABLET_ID)));
    ASSERT_EQ(OB_SUCCESS, snapshot_tablet_ids.push_back(ObTabletID(TEST_TARGET_TABLET_ID)));
    task.create_index_arg_.index_type_ = fts_child_index_types[i];

    EXPECT_EQ(OB_INVALID_ARGUMENT,
              task.append_rowkey_doc_snapshot_tablets(data_table_schema, snapshot_tablet_ids));
    EXPECT_EQ(2, snapshot_tablet_ids.count());
  }

  const ObIndexType unrelated_index_types[] = {
    INDEX_TYPE_NORMAL_LOCAL,
    INDEX_TYPE_ROWKEY_DOC_ID_LOCAL
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(unrelated_index_types); ++i) {
    ObSEArray<ObTabletID, 3> snapshot_tablet_ids;
    ASSERT_EQ(OB_SUCCESS, snapshot_tablet_ids.push_back(ObTabletID(TEST_DATA_TABLET_ID)));
    ASSERT_EQ(OB_SUCCESS, snapshot_tablet_ids.push_back(ObTabletID(TEST_TARGET_TABLET_ID)));
    task.create_index_arg_.index_type_ = unrelated_index_types[i];

    EXPECT_EQ(OB_SUCCESS,
              task.append_rowkey_doc_snapshot_tablets(data_table_schema, snapshot_tablet_ids));
    EXPECT_EQ(2, snapshot_tablet_ids.count());
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WDIAG");
  oceanbase::common::ObLogger::get_logger().set_file_name(LOG_FILE_PATH, true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
