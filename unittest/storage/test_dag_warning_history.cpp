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
#define protected public

#include "storage/ob_dag_warning_history_mgr.h"
#include "share/scheduler/ob_dag_scheduler.h"

namespace oceanbase {
using namespace common;
using namespace storage;

namespace unittest {
class TestDagWarningHistory : public ::testing::Test {
public:
  TestDagWarningHistory()
  {}
  virtual ~TestDagWarningHistory()
  {}
};

TEST_F(TestDagWarningHistory, init)
{
  ASSERT_EQ(OB_SUCCESS, ObDagWarningHistoryManager::get_instance().init());
}

TEST_F(TestDagWarningHistory, simple_add)
{
  int ret = OB_SUCCESS;
  ObDagWarningHistoryManager& manager = ObDagWarningHistoryManager::get_instance();

  ObDagWarningInfo* info = NULL;
  const int64_t key = 8888;
  ret = manager.alloc_and_add(key, info);
  ASSERT_EQ(OB_SUCCESS, ret);

  info->dag_ret_ = -4016;
  info->dag_status_ = ODS_WARNING;
  info->dag_type_ = share::ObIDag::DAG_TYPE_SSTABLE_MINOR_MERGE;
  strcpy(info->warning_info_, "table_id=1101710651081571, partition_id=66, mini merge error");

  ObDagWarningInfo* ret_info = NULL;
  ret = manager.get(key + 1, ret_info);
  ASSERT_EQ(OB_HASH_NOT_EXIST, ret);
  ASSERT_EQ(NULL, ret_info);

  ret = manager.get(key, ret_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, *ret_info == *info);
  STORAGE_LOG(INFO, "ret", KPC(ret_info));

  // duplicated key
  ret = manager.alloc_and_add(key, info);
  ASSERT_EQ(OB_HASH_EXIST, ret);
}

TEST_F(TestDagWarningHistory, simple_del)
{
  int ret = OB_SUCCESS;
  ObDagWarningHistoryManager& manager = ObDagWarningHistoryManager::get_instance();
  manager.clear();

  ObDagWarningInfo* info = NULL;
  const int64_t key = 8888;
  ASSERT_EQ(OB_SUCCESS, manager.alloc_and_add(key, info));
  info->dag_ret_ = -4016;
  info->dag_status_ = ODS_WARNING;
  info->dag_type_ = share::ObIDag::DAG_TYPE_SSTABLE_MINOR_MERGE;
  strcpy(info->warning_info_, "table_id=1101710651081571, partition_id=66, mini merge error");

  ASSERT_EQ(OB_HASH_NOT_EXIST, manager.del(key + 1));

  ASSERT_EQ(OB_SUCCESS, manager.del(key));

  ObDagWarningInfo* ret_info = NULL;
  ret = manager.get(key, ret_info);
  ASSERT_EQ(OB_HASH_NOT_EXIST, ret);
}

TEST_F(TestDagWarningHistory, simple_loop_get)
{
  int ret = OB_SUCCESS;
  ObDagWarningHistoryManager& manager = ObDagWarningHistoryManager::get_instance();
  manager.clear();

  ObDagWarningInfo basic_info;
  basic_info.dag_ret_ = -4016;
  basic_info.dag_status_ = ODS_WARNING;
  basic_info.dag_type_ = share::ObIDag::DAG_TYPE_SSTABLE_MINOR_MERGE;
  strcpy(basic_info.warning_info_, "table_id=1101710651081571, partition_id=66, mini merge error");

  const int64_t max_cnt = 20;
  ObDagWarningInfo* info = NULL;
  int64_t key = 0;
  for (int i = 0; i < max_cnt; ++i) {
    key = 8888 + i;
    ASSERT_EQ(OB_SUCCESS, manager.alloc_and_add(key, info));
    info->dag_ret_ = -4016 + i;
    info->dag_status_ = ODS_WARNING;
    info->dag_type_ = share::ObIDag::DAG_TYPE_SSTABLE_MINOR_MERGE;
    strcpy(info->warning_info_, "table_id=1101710651081571, partition_id=66, mini merge error");
  }
  ObDagWarningInfoIterator iterator;
  iterator.open();
  ObDagWarningInfo read_info;
  int i = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iterator.get_next_info(read_info))) {
      ASSERT_EQ(OB_ITER_END, ret);
    } else {
      basic_info.dag_ret_ = -4016 + i;
      STORAGE_LOG(DEBUG, "print info", K(ret), K(read_info), K(basic_info));
      ASSERT_EQ(true, read_info == basic_info);
      ++i;
    }
  }

  for (int i = 0; i < max_cnt; i += 2) {
    key = 8888 + i;
    ASSERT_EQ(OB_SUCCESS, manager.del(key));
  }

  iterator.reset();
  iterator.open();
  i = 0;
  while (OB_SUCC(ret)) {
    if (OB_SUCC(iterator.get_next_info(read_info))) {
      basic_info.dag_ret_ = -4016 + i;
      ASSERT_EQ(true, read_info == basic_info);
      STORAGE_LOG(DEBUG, "print info", K(ret), K(read_info), KPC(info));
      i += 2;
    }
  }

  manager.clear();
  ASSERT_EQ(0, manager.size());
}

TEST_F(TestDagWarningHistory, test_rebuild)
{
  int ret = OB_SUCCESS;
  ObDagWarningHistoryManager& manager = ObDagWarningHistoryManager::get_instance();
  manager.clear();

  const int64_t max_cnt = 20;
  ObDagWarningInfo basic_info;
  basic_info.dag_ret_ = -4016;
  basic_info.dag_status_ = ODS_WARNING;
  basic_info.dag_type_ = share::ObIDag::DAG_TYPE_SSTABLE_MINOR_MERGE;
  strcpy(basic_info.warning_info_, "table_id=1101710651081571, partition_id=66, mini merge error");

  ObDagWarningInfo* info = NULL;
  int64_t key = 0;
  for (int i = 0; i < max_cnt; ++i) {
    key = 8888 + i;
    ASSERT_EQ(OB_SUCCESS, manager.alloc_and_add(key, info));
    info->dag_ret_ = -4016 + i;
    info->dag_status_ = ODS_WARNING;
    info->dag_type_ = share::ObIDag::DAG_TYPE_SSTABLE_MINOR_MERGE;
    strcpy(info->warning_info_, "table_id=1101710651081571, partition_id=66, mini merge error");
    STORAGE_LOG(DEBUG, "print info", K(ret), K(i), K(key), KPC(info));
  }

  ASSERT_EQ(max_cnt, manager.size());

  for (int i = 0; i < max_cnt; i += 2) {
    key = 8888 + i;
    ASSERT_EQ(OB_SUCCESS, manager.del(key));
  }

  ASSERT_EQ(max_cnt / 2, manager.size());

  ObDagWarningInfoIterator iterator;
  iterator.open();
  ObDagWarningInfo read_info;
  int i = 1;
  while (OB_SUCC(ret)) {
    if (OB_SUCC(iterator.get_next_info(read_info))) {
      basic_info.dag_ret_ = -4016 + i;
      STORAGE_LOG(DEBUG, "print info", K(ret), K(read_info), K(basic_info));
      ASSERT_EQ(true, read_info == basic_info);
      i += 2;
    }
  }

  manager.clear();
  ASSERT_EQ(0, manager.size());
}

TEST_F(TestDagWarningHistory, simple_destory)
{
  ObDagWarningHistoryManager& manager = ObDagWarningHistoryManager::get_instance();
  manager.destory();
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_dag_warning_history.log*");
  OB_LOGGER.set_file_name("test_dag_warning_history.log");
  CLOG_LOG(INFO, "begin unittest: test_dag_warning_history");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
