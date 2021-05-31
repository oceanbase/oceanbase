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

#include "storage/ob_partition_merge_task.h"
#include "storage/ob_build_index_task.h"
#include "storage/ob_partition_migrator.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/ob_partition_split_task.h"
#include "storage/ob_sstable_row_iterator.h"
#include <gtest/gtest.h>

namespace oceanbase {
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace share::schema;
using namespace ::testing;
using namespace share;
namespace unittest {

class TestDagSize : public ::testing::Test {};

TEST_F(TestDagSize, test_size)
{
  ObDagScheduler& scheduler = ObDagScheduler::get_instance();
  ObAddr addr(1, 1);
  ASSERT_EQ(OB_SUCCESS, scheduler.init(addr));
  ObBuildIndexDag* build_index_dag = NULL;
  ASSERT_EQ(OB_SUCCESS, scheduler.alloc_dag(build_index_dag));
  scheduler.free_dag(*build_index_dag);
  ObMigrateDag* migrate_dag = NULL;
  ASSERT_EQ(OB_SUCCESS, scheduler.alloc_dag(migrate_dag));
  scheduler.free_dag(*migrate_dag);
  ObSSTableMajorMergeDag* major_merge_dag = NULL;
  ASSERT_EQ(OB_SUCCESS, scheduler.alloc_dag(major_merge_dag));
  scheduler.free_dag(*major_merge_dag);
  ObSSTableMinorMergeDag* minor_merge_dag = NULL;
  ASSERT_EQ(OB_SUCCESS, scheduler.alloc_dag(minor_merge_dag));
  scheduler.free_dag(*minor_merge_dag);
  ObSSTableSplitDag* split_dag = NULL;
  ASSERT_EQ(OB_SUCCESS, scheduler.alloc_dag(split_dag));
  scheduler.free_dag(*split_dag);
  ObUniqueCheckingDag* unique_check_dag = NULL;
  ASSERT_EQ(OB_SUCCESS, scheduler.alloc_dag(unique_check_dag));
  scheduler.free_dag(*unique_check_dag);

  STORAGE_LOG(INFO, "micro block row getter size", K(sizeof(ObMicroBlockRowGetter)));
  STORAGE_LOG(INFO, "micro block row scanner size", K(sizeof(ObMicroBlockRowScanner)));
  STORAGE_LOG(INFO, "micro block reader size", K(sizeof(ObMicroBlockReader)));
  STORAGE_LOG(INFO, "multiple scan merge", K(sizeof(ObMultipleScanMerge)));
  scheduler.destroy();
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
