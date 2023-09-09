/**
 * Copyright (c) 2022 OceanBase
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
#include <gmock/gmock.h>
#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "share/transfer/ob_transfer_task_operator.h"

namespace oceanbase
{
using namespace unittest;
namespace share
{
using namespace schema;
using namespace common;

class TestTransferTaskOperator : public ObSimpleClusterTestBase
{
public:
  TestTransferTaskOperator() : ObSimpleClusterTestBase("test_transfer_task_operator") {}
  virtual void SetUp();
public:
  uint64_t tenant_id_;
  ObTransferTaskID task_id_;
  ObLSID src_ls_;
  ObLSID dest_ls_;
  ObTransferPartList part_list_;
  ObTransferPartList not_exist_part_list_;
  ObTransferPartList lock_conflict_part_list_;
  ObDisplayTabletList table_lock_tablet_list_;
  ObTransferTabletList tablet_list_;
  share::SCN start_scn_;
  share::SCN finish_scn_;
  ObTransferStatus status_;
  ObCurTraceId::TraceId trace_id_;
  ObTransferTask task_;
  transaction::tablelock::ObTableLockOwnerID lock_owner_id_;
};

void TestTransferTaskOperator::SetUp()
{
  tenant_id_ = 1002;
  task_id_ = 111;
  src_ls_ = 1001;
  dest_ls_ = 1002;
  status_ = ObTransferStatus::INIT;
  start_scn_.convert_for_inner_table_field(1666844202200632);
  finish_scn_.convert_for_inner_table_field(1666844202208490);
  ObString trace_id_str = "YCDC56458724D-0005EBECD3F9DB9D-0-0";
  trace_id_.parse_from_buf(trace_id_str.ptr());
  lock_owner_id_ = 999;
  for(int64_t i = 0; i < 100; ++i) {
    ObTransferPartInfo part;
    ObTransferTabletInfo tablet;
    ASSERT_EQ(OB_SUCCESS, part.init(20000 + i, 25000 + i));
    ASSERT_EQ(OB_SUCCESS, part_list_.push_back(part));
    ASSERT_EQ(OB_SUCCESS, tablet.init(ObTabletID(10000 + i), i));
    ASSERT_EQ(OB_SUCCESS, tablet_list_.push_back(tablet));
    ASSERT_EQ(OB_SUCCESS, table_lock_tablet_list_.push_back(ObDisplayTabletID(ObTabletID(10000 + i))));
    if (i < 2) {
      ASSERT_EQ(OB_SUCCESS, not_exist_part_list_.push_back(part));
    } else if (i < 4) {
      ASSERT_EQ(OB_SUCCESS, lock_conflict_part_list_.push_back(part));
    }
  }
  ASSERT_EQ(OB_SUCCESS, task_.init(task_id_, src_ls_, dest_ls_, part_list_, status_, trace_id_,
      ObBalanceTaskID(12)));
  LOG_INFO("tranfer task init", K(task_));
}

TEST_F(TestTransferTaskOperator, test_basic_func)
{
  int ret = OB_SUCCESS;

  // ObTransferTabletInfo
  ObTransferTabletInfo tablet_info;
  ObTransferTabletInfo invalid_tablet_info;
  char buf[100];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, tablet_info.init(ObTabletID(20001), 1));
  ASSERT_EQ(OB_SUCCESS, tablet_info.to_display_str(buf, sizeof(buf), pos));
  ASSERT_TRUE(0 == strcmp(buf, "20001:1"));
  LOG_INFO("tablet_info str", K(buf), K(tablet_info));
  tablet_info.reset();
  ASSERT_EQ(OB_SUCCESS, tablet_info.parse_from_display_str(ObString::make_string(buf)));
  ASSERT_TRUE((tablet_info.tablet_id().id() == 20001) && (tablet_info.transfer_seq() == 1));

  memset(buf, 0, sizeof(buf));
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_tablet_info.init(ObTabletID(), 1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_tablet_info.init(ObTabletID(1), -1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_tablet_info.to_display_str(buf, sizeof(buf), pos));
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_tablet_info.parse_from_display_str(ObString::make_string("")));
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_tablet_info.parse_from_display_str(ObString::make_string("a:1")));
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_tablet_info.parse_from_display_str(ObString::make_string("123,1")));
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_tablet_info.parse_from_display_str(ObString::make_string("123,123456789012345678901234567890")));

  // ObTransferPartInfo
  ObTransferPartInfo part_info;
  ObTransferPartInfo invalid_part_info;
  char part_info_buf[100];
  memset(part_info_buf, 0, sizeof(part_info_buf));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, part_info.init(123456, 654321));
  ASSERT_EQ(OB_SUCCESS, part_info.to_display_str(part_info_buf, sizeof(part_info_buf), pos));
  ASSERT_TRUE(0 == strcmp(part_info_buf, "123456:654321"));
  LOG_INFO("part_info str", K(part_info_buf), K(part_info));
  part_info.reset();
  ASSERT_EQ(OB_SUCCESS, part_info.parse_from_display_str(ObString::make_string(part_info_buf)));
  ASSERT_TRUE((part_info.table_id() == 123456) && (part_info.part_object_id() == 654321));

  memset(part_info_buf, 0, sizeof(part_info_buf));
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_part_info.init(OB_INVALID_ID, 1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_part_info.init(1, OB_INVALID_ID));
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_part_info.to_display_str(part_info_buf, sizeof(part_info_buf), pos));
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_part_info.parse_from_display_str(ObString::make_string("")));
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_part_info.parse_from_display_str(ObString::make_string("a:1")));
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_part_info.parse_from_display_str(ObString::make_string("123,1")));
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_tablet_info.parse_from_display_str(ObString::make_string("123,123456789012345678901234567890")));

  // to_display_str and parse_str
  common::ObArenaAllocator allocator;
  ObString part_list_str;
  ObTransferPartList new_part_list;
  ASSERT_EQ(OB_SUCCESS, part_list_.to_display_str(allocator, part_list_str));
  LOG_INFO("ObTransferPartList to str", K(part_list_str));
  ASSERT_EQ(OB_SUCCESS, new_part_list.parse_from_display_str(part_list_str));
  LOG_INFO("parse str into ObTransferPartList", K(new_part_list));
  ASSERT_TRUE(!new_part_list.empty());
  ARRAY_FOREACH_N(part_list_, idx, cnt) {
    ASSERT_TRUE(part_list_.at(idx) == new_part_list.at(idx));
  }

  ObString tablet_list_str;
  ObString table_lock_tablet_list_str;
  ObTransferTabletList new_tablet_list;
  ASSERT_EQ(OB_SUCCESS, tablet_list_.to_display_str(allocator, tablet_list_str));
  LOG_INFO("ObTransferTabletList to str", K(tablet_list_str));
  ASSERT_EQ(OB_SUCCESS, table_lock_tablet_list_.to_display_str(allocator, table_lock_tablet_list_str));
  ASSERT_EQ(OB_SUCCESS, new_tablet_list.parse_from_display_str(tablet_list_str));
  LOG_INFO("parse str into ObTransferTabletList", K(new_tablet_list));
  ASSERT_TRUE(!new_tablet_list.empty());
  ARRAY_FOREACH_N(tablet_list_, idx, cnt) {
    ASSERT_TRUE(tablet_list_.at(idx) == new_tablet_list.at(idx));
  }

  // empty list and str
  ObString empty_str;
  new_part_list.reset();
  ASSERT_EQ(OB_SUCCESS, new_part_list.to_display_str(allocator, empty_str));
  ASSERT_EQ(OB_SUCCESS, new_part_list.parse_from_display_str(empty_str));
  ASSERT_TRUE(new_part_list.empty() && empty_str.empty());

  // invalid str
  ObString invalid_str_1 = "123,12,123,21";
  ObString invalid_str_2 = "aa:12,123:bb";
  ObString invalid_str_3 = "1:1,1:123456789012345678901234567890";
  ObString invalid_str_4 = "123:12!321:21";
  ASSERT_EQ(OB_INVALID_ARGUMENT, new_part_list.parse_from_display_str(invalid_str_1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, new_part_list.parse_from_display_str(invalid_str_2));
  new_part_list.reset();
  ASSERT_EQ(OB_SUCCESS, new_part_list.parse_from_display_str(invalid_str_3));
  ASSERT_TRUE(new_part_list.count() == 2 && new_part_list.at(1).part_object_id() == OB_INVALID_ID);
  new_part_list.reset();
  ASSERT_EQ(OB_SUCCESS, new_part_list.parse_from_display_str(invalid_str_4));
  ASSERT_TRUE(new_part_list.count() == 1);


  // ObTransferTask other init
  ObTransferTask task;
  ASSERT_TRUE(!task.is_valid());
  ASSERT_EQ(OB_SUCCESS, task.init(task_id_, src_ls_, dest_ls_, part_list_str, empty_str, empty_str, table_lock_tablet_list_str, tablet_list_str,
      start_scn_, finish_scn_, status_, trace_id_, OB_SUCCESS, ObTransferTaskComment::EMPTY_COMMENT,
      ObBalanceTaskID(2), lock_owner_id_));
  LOG_INFO("tranfer task other init", K(task));
  ASSERT_TRUE(task.is_valid());
  task.reset();
  ASSERT_EQ(OB_SUCCESS, task.assign(task_));
  ASSERT_EQ(task.get_task_id(), task_.get_task_id());

  // ObTrasnferTaskComment
  ASSERT_TRUE(0 == strcmp(transfer_task_comment_to_str(ObTransferTaskComment::EMPTY_COMMENT), ""));
  ASSERT_TRUE(0 == strcmp(transfer_task_comment_to_str(ObTransferTaskComment::WAIT_FOR_MEMBER_LIST), "Wait for member list to be same"));
  ASSERT_TRUE(0 == strcmp(transfer_task_comment_to_str(ObTransferTaskComment::TASK_COMPLETED_AS_NO_VALID_PARTITION), "Task completed as no valid partition"));
  ASSERT_TRUE(0 == strcmp(transfer_task_comment_to_str(ObTransferTaskComment::TASK_CANCELED), "Task canceled"));
  ASSERT_TRUE(0 == strcmp(transfer_task_comment_to_str(ObTransferTaskComment::MAX_COMMENT), "Unknow"));

  ASSERT_TRUE(ObTransferTaskComment::WAIT_FOR_MEMBER_LIST == str_to_transfer_task_comment("Wait for member list to be same"));
  ASSERT_TRUE(ObTransferTaskComment::TASK_COMPLETED_AS_NO_VALID_PARTITION == str_to_transfer_task_comment("Task completed as no valid partition"));
  ASSERT_TRUE(ObTransferTaskComment::TASK_CANCELED == str_to_transfer_task_comment("Task canceled"));
  ASSERT_TRUE(ObTransferTaskComment::EMPTY_COMMENT == str_to_transfer_task_comment(""));
  ASSERT_TRUE(ObTransferTaskComment::MAX_COMMENT == str_to_transfer_task_comment("Unknow"));
  ASSERT_TRUE(ObTransferTaskComment::MAX_COMMENT == str_to_transfer_task_comment("XXXXX"));
}

TEST_F(TestTransferTaskOperator, test_operator)
{
  int ret = OB_SUCCESS;
  ObSimpleClusterTestBase::SetUp();
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_));
  LOG_INFO("new tenant_id", K(tenant_id_));
  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();

  // get_all_task_status empty
  ObArray<ObTransferTask::TaskStatus> task_status;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObTransferTaskOperator::get_all_task_status(sql_proxy, tenant_id_, task_status));
  ASSERT_TRUE(task_status.empty());

  // get_max_task_id_from_history when history is empty
  ObTransferTaskID max_task_id;
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get_max_task_id_from_history(sql_proxy, tenant_id_, max_task_id));
  ASSERT_TRUE(!max_task_id.is_valid());

  // insert
  ObTransferTask other_task;
  ObTransferTaskID other_task_id(222);
  ASSERT_EQ(OB_SUCCESS, other_task.init(other_task_id, ObLSID(1003), ObLSID(1004), part_list_,
      ObTransferStatus(ObTransferStatus::INIT), trace_id_, ObBalanceTaskID(2)));
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::insert(sql_proxy, tenant_id_, other_task));
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::insert(sql_proxy, tenant_id_, task_));
  ASSERT_EQ(OB_ENTRY_EXIST, ObTransferTaskOperator::insert(sql_proxy, tenant_id_, task_));

  // update comment
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::update_comment(sql_proxy, tenant_id_, task_id_, ObTransferTaskComment::TASK_CANCELED));

  // get
  ObTransferTask task;
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get(sql_proxy, tenant_id_, task_id_, false, task, 0/*group_id*/));
  ASSERT_TRUE(task.get_task_id() == task_id_);
  ASSERT_TRUE(task.get_tablet_list().empty());
  ASSERT_TRUE(0 == strcmp(transfer_task_comment_to_str(task.get_comment()), "Task canceled"));
  LOG_INFO("get from table", K(task));
  task.reset();
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get(sql_proxy, tenant_id_, task_id_, true, task, 0/*group_id*/));
  LOG_INFO("get from table", K(task));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObTransferTaskOperator::get(sql_proxy, tenant_id_,
      ObTransferTaskID(555), false, task, 0/*group_id*/));

  // get_task_with_time
  int64_t create_time = OB_INVALID_TIMESTAMP;
  int64_t finish_time = OB_INVALID_TIMESTAMP;
  task.reset();
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get_task_with_time(sql_proxy, tenant_id_, task_id_, true, task, create_time, finish_time));
  ASSERT_TRUE(OB_INVALID_TIMESTAMP != create_time);
  ASSERT_TRUE(OB_INVALID_TIMESTAMP != finish_time);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObTransferTaskOperator::get_task_with_time(sql_proxy, tenant_id_,
      ObTransferTaskID(555), false, task, create_time, finish_time));

  // get by status
  ObArray<ObTransferTask> tasks;
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get_by_status(sql_proxy, tenant_id_, status_, tasks));
  ASSERT_TRUE(2 == tasks.count());
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObTransferTaskOperator::get_by_status(sql_proxy, tenant_id_, ObTransferStatus(ObTransferStatus::ABORTED), tasks));

  // get by dest_ls
  task.reset();
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get_by_dest_ls(sql_proxy, tenant_id_, dest_ls_, task, 0/*group_id*/));
  ASSERT_TRUE(task_id_ == task.get_task_id());
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObTransferTaskOperator::get_by_dest_ls(sql_proxy, tenant_id_, ObLSID(555), task, 0/*group_id*/));
  ObTransferTask dup_dest_ls_task;
  ASSERT_EQ(OB_SUCCESS, dup_dest_ls_task.init(ObTransferTaskID(2223), ObLSID(1003), ObLSID(1004),
      part_list_, ObTransferStatus(ObTransferStatus::INIT), trace_id_, ObBalanceTaskID(2)));
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::insert(sql_proxy, tenant_id_, dup_dest_ls_task));
  task.reset();
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObTransferTaskOperator::get_by_dest_ls(sql_proxy, tenant_id_, ObLSID(1004), task, 0/*group_id*/));

  // update_to_start_status
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::update_to_start_status(sql_proxy, tenant_id_,task_id_,
      ObTransferStatus(ObTransferStatus::INIT), part_list_, not_exist_part_list_, lock_conflict_part_list_, table_lock_tablet_list_, tablet_list_, ObTransferStatus(ObTransferStatus::START), lock_owner_id_));
  task.reset();
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get(sql_proxy, tenant_id_, task_id_, false, task, 0/*group_id*/));
  ASSERT_TRUE(!task.get_tablet_list().empty());
  LOG_INFO("update to start status", K(task));
  ASSERT_EQ(OB_STATE_NOT_MATCH, ObTransferTaskOperator::update_to_start_status(sql_proxy, tenant_id_,task_id_,
      ObTransferStatus(ObTransferStatus::ABORTED), part_list_, not_exist_part_list_, lock_conflict_part_list_, table_lock_tablet_list_, tablet_list_, ObTransferStatus(ObTransferStatus::START), lock_owner_id_));

  // update start_scn
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::update_start_scn(sql_proxy, tenant_id_, task_id_, ObTransferStatus(ObTransferStatus::START), start_scn_, 0/*group_id*/));
  task.reset();
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get(sql_proxy, tenant_id_, task_id_, false, task, 0/*group_id*/));
  ASSERT_TRUE(task.get_start_scn() == start_scn_);
  //ASSERT_EQ(OB_STATE_NOT_MATCH, ObTransferTaskOperator::update_start_scn(sql_proxy, tenant_id_, task_id_, ObTransferStatus(ObTransferStatus::ABORTED), start_scn_));

  // update status
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::update_status_and_result(sql_proxy, tenant_id_, task_id_,
      ObTransferStatus(ObTransferStatus::START), ObTransferStatus(ObTransferStatus::DOING), OB_SUCCESS, 0/*group_id*/));
  task.reset();
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get(sql_proxy, tenant_id_, task_id_, false, task, 0/*group_id*/));
  ASSERT_TRUE(task.get_status().status() == ObTransferStatus::DOING);
  //ASSERT_EQ(OB_STATE_NOT_MATCH, ObTransferTaskOperator::update_status(sql_proxy, tenant_id_, task_id_,
  //    ObTransferStatus(ObTransferStatus::ABORTED), ObTransferStatus(ObTransferStatus::ABORTED)));

  // finish task
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::finish_task(sql_proxy, tenant_id_, other_task_id,
      ObTransferStatus(ObTransferStatus::INIT), ObTransferStatus(ObTransferStatus::COMPLETED), OB_SUCCESS, ObTransferTaskComment::EMPTY_COMMENT, 0/*group_id*/));
  task.reset();
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get(sql_proxy, tenant_id_, other_task_id, false, task, 0/*group_id*/));
  ASSERT_TRUE(task.get_status().status() == ObTransferStatus::COMPLETED);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ObTransferTaskOperator::finish_task(sql_proxy, tenant_id_, other_task_id,
      ObTransferStatus(ObTransferStatus::ABORTED), ObTransferStatus(ObTransferStatus::ABORTED), OB_SUCCESS, ObTransferTaskComment::EMPTY_COMMENT, 0/*group_id*/));

  // finish task from init
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::finish_task_from_init(sql_proxy, tenant_id_, dup_dest_ls_task.get_task_id(),
      ObTransferStatus(ObTransferStatus::INIT), part_list_, not_exist_part_list_, lock_conflict_part_list_, ObTransferStatus(ObTransferStatus::COMPLETED), OB_SUCCESS, ObTransferTaskComment::TASK_COMPLETED_AS_NO_VALID_PARTITION));
  task.reset();
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get(sql_proxy, tenant_id_, other_task_id, false, task, 0/*group_id*/));
  ASSERT_TRUE(task.get_status().status() == ObTransferStatus::COMPLETED);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ObTransferTaskOperator::finish_task_from_init(sql_proxy, tenant_id_, dup_dest_ls_task.get_task_id(),
      ObTransferStatus(ObTransferStatus::ABORTED), part_list_, not_exist_part_list_, lock_conflict_part_list_, ObTransferStatus(ObTransferStatus::COMPLETED), OB_SUCCESS, ObTransferTaskComment::TASK_COMPLETED_AS_NO_VALID_PARTITION));

  // update finish_scn
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::update_finish_scn(sql_proxy, tenant_id_, task_id_, ObTransferStatus(ObTransferStatus::DOING), finish_scn_, 0/*group_id*/));
  task.reset();
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get(sql_proxy, tenant_id_, task_id_, false, task, 0/*group_id*/));
  ASSERT_TRUE(task.get_finish_scn() == finish_scn_);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ObTransferTaskOperator::update_finish_scn(sql_proxy, tenant_id_, task_id_, ObTransferStatus(ObTransferStatus::ABORTED), finish_scn_, 0/*group_id*/));

  // get_all_task_status
  task_status.reset();
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get_all_task_status(sql_proxy, tenant_id_, task_status));
  ASSERT_TRUE(task_status.count() == 3);
  bool is_same = false;
  ARRAY_FOREACH(task_status, idx) {
    const ObTransferTask::TaskStatus task_stat = task_status.at(idx);
    if (task_stat.get_task_id() == task_id_) {
      is_same = ObTransferStatus::DOING == task_stat.get_status();
    } else if (task_stat.get_task_id() == other_task_id) {
      is_same = ObTransferStatus::COMPLETED == task_stat.get_status();
    } else if (task_stat.get_task_id() == dup_dest_ls_task.get_task_id()) {
      is_same = ObTransferStatus::COMPLETED == task_stat.get_status();
    } else {
      is_same = false;
    }
  }
  ASSERT_TRUE(is_same);

  // remove task
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::remove(sql_proxy, tenant_id_, task_id_));
  task.reset();
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObTransferTaskOperator::remove(sql_proxy, tenant_id_, task_id_));

  // insert history
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::insert_history(sql_proxy, tenant_id_, task_, 111, 222));
  ASSERT_EQ(OB_ENTRY_EXIST, ObTransferTaskOperator::insert_history(sql_proxy, tenant_id_, task_, 111, 222));

  // get history task
  ObTransferTask history_task;
  create_time = OB_INVALID_TIMESTAMP;
  finish_time = OB_INVALID_TIMESTAMP;
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get_history_task(sql_proxy, tenant_id_, task_id_, history_task, create_time, finish_time));
  ObArenaAllocator allocator;
  ObString part_list_str;
  ObString history_part_list_str;
  ObString not_exist_part_list_str;
  ObString history_not_exist_part_list_str;
  ObString lock_conflict_part_list_str;
  ObString history_lock_conflict_part_list_str;
  ASSERT_TRUE(task_.get_task_id() == history_task.get_task_id());
  ASSERT_TRUE(task_.get_status() == history_task.get_status());
  ASSERT_EQ(OB_SUCCESS, task_.get_part_list().to_display_str(allocator, part_list_str));
  ASSERT_EQ(OB_SUCCESS, history_task.get_part_list().to_display_str(allocator, history_part_list_str));
  ASSERT_EQ(OB_SUCCESS, task_.get_not_exist_part_list().to_display_str(allocator, not_exist_part_list_str));
  ASSERT_EQ(OB_SUCCESS, history_task.get_not_exist_part_list().to_display_str(allocator, history_not_exist_part_list_str));
  ASSERT_EQ(OB_SUCCESS, task_.get_lock_conflict_part_list().to_display_str(allocator, lock_conflict_part_list_str));
  ASSERT_EQ(OB_SUCCESS, history_task.get_lock_conflict_part_list().to_display_str(allocator, history_lock_conflict_part_list_str));
  ASSERT_TRUE(0 == compare(part_list_str, history_part_list_str));
  ASSERT_TRUE(0 == compare(not_exist_part_list_str, history_not_exist_part_list_str));
  ASSERT_TRUE(0 == compare(lock_conflict_part_list_str, history_lock_conflict_part_list_str));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObTransferTaskOperator::get_history_task(sql_proxy,
      tenant_id_, ObTransferTaskID(555), history_task, create_time, finish_time));

  max_task_id.reset();
  ASSERT_EQ(OB_SUCCESS, ObTransferTaskOperator::get_max_task_id_from_history(sql_proxy, tenant_id_, max_task_id));
  ASSERT_TRUE(max_task_id == task_id_);
}

} // namespace share
} // namespace oceanbase
int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
