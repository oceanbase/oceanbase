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
#include <stdlib.h>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "lib/container/ob_tuple.h"
#include "lib/ob_define.h"
#include "ob_tablet_id.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_ls_id.h"
#include "env/ob_simple_cluster_test_base.h"
#include "env/ob_simple_server_restart_helper.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "logservice/rcservice/ob_role_change_service.h"
#include "logservice/ob_ls_adapter.h"
#include "storage/access/ob_rows_info.h"
#include "storage/checkpoint/ob_data_checkpoint.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/ls/ob_freezer.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_meta.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/ob_relative_table.h"
#include "storage/ob_storage_table_guard.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/multi_data_source/runtime_utility/mds_tenant_service.h"
#include <fstream>

#undef private
#undef protected

static const char *TEST_FILE_NAME = "test_mds_recover";
static const char *BORN_CASE_NAME = "ObTestMdsBeforeRecover";
static const char *RESTART_CASE_NAME = "ObTestMdsAfterRecover";
static const char *BEFORE_RECOVER_RESULT_FILE = "/tmp/1.txt";
static const char *AFTER_RECOVER_RESULT_FILE = "/tmp/2.txt";

using namespace std;

ObSimpleServerRestartHelper *helper_ptr = nullptr;

oceanbase::share::ObLSID TEST_LS_ID;
oceanbase::common::ObTabletID TEST_TABLET_ID;
oceanbase::share::SCN LAST_SCN, REPLAY_BASE_SCN;
using VirtualTableResult = oceanbase::common::ObArray<ObTuple<int64_t/*unit_id*/, ObStringHolder/*user_key*/, int64_t/*version_idx*/, ObStringHolder/*writer_type*/,
                           int64_t/*writer_id*/, int64_t/*seq_no*/, uint64_t/*redo_scn*/, uint64_t/*end_scn*/, uint64_t/*trans_version*/,
                           ObStringHolder/*node_type*/, ObStringHolder/*state*/, ObStringHolder/*user_data*/, ObStringHolder/*position*/>>;

VirtualTableResult RESULT;

namespace oceanbase
{
using namespace transaction;
using namespace storage;
using namespace share;
namespace unittest
{
class ObTestMdsBeforeRecover : public ObSimpleClusterTestBase
{
public:
  ObTestMdsBeforeRecover() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
};

void wait_user()
{
  std::cout << "Enter anything to continue..." << std::endl;
  char a = '\0';
  std::cin >> a;
}

void create_or_find_test_table(const char *table_name, share::ObLSID &ls_id, ObTabletID &tablet_id, bool create)
{
  if (create) {
    int64_t _;
    char create_table_sql[512] = { 0 };
    databuff_printf(create_table_sql, 512, "create table %s(a int)", table_name);
    // 1. 新建一个tablet
    ASSERT_EQ(OB_SUCCESS, GCTX.sql_proxy_->write(OB_SYS_TENANT_ID, create_table_sql, _));
  }
  // 2. 从表名拿到它的tablet_id
  char where_condition1[512] = { 0 };
  databuff_printf(where_condition1, 512, "where table_name = '%s'", table_name);
  ASSERT_EQ(OB_SUCCESS, ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID,
                                                            {"tablet_id"},
                                                            OB_ALL_TABLE_TNAME,
                                                            where_condition1,
                                                            tablet_id));
  // 3. 从tablet_id拿到它的ls_id
  char where_condition2[512] = {  0 };
  databuff_printf(where_condition2, 512, "where tablet_id = %ld", tablet_id.id());
  ASSERT_EQ(OB_SUCCESS, ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID,
                                                            {"ls_id"},
                                                            OB_ALL_TABLET_TO_LS_TNAME,
                                                            where_condition2,
                                                            ls_id));
  // 4. 从ls_id找到ls
  storage::ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(storage::ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
}

void insert_row_to_write_inc_seq(const char *table_name)
{
  int64_t _;
  char insert_sql[512] = { 0 };
  databuff_printf(insert_sql, 512, "insert into %s values(0)", table_name);
  // 1. 插入数据
  ASSERT_EQ(OB_SUCCESS, GCTX.sql_proxy_->write(OB_SYS_TENANT_ID, insert_sql, _));
}

void add_column_to_write_dll_info(const char *table_name, share::ObLSID &ls_id, ObTabletID &tablet_id)
{
  int64_t _;
  char insert_sql[512] = { 0 };
  databuff_printf(insert_sql, 512, "alter table %s add b int after a", table_name);
  // 1. 加列
  ASSERT_EQ(OB_SUCCESS, GCTX.sql_proxy_->write(OB_SYS_TENANT_ID, insert_sql, _));
  // 2. 从表名拿到它的tablet_id
  char where_condition1[512] = { 0 };
  databuff_printf(where_condition1, 512, "where table_name = '%s'", table_name);
  ASSERT_EQ(OB_SUCCESS, ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID,
                                                            {"tablet_id"},
                                                            OB_ALL_TABLE_TNAME,
                                                            where_condition1,
                                                            tablet_id));
  // 3. 从tablet_id拿到它的ls_id
  char where_condition2[512] = { 0 };
  databuff_printf(where_condition2, 512, "where tablet_id = %ld", tablet_id.id());
  ASSERT_EQ(OB_SUCCESS, ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID,
                                                            {"ls_id"},
                                                            OB_ALL_TABLET_TO_LS_TNAME,
                                                            where_condition2,
                                                            ls_id));
  // 4. 从ls_id找到ls
  storage::ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(storage::ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
}

void do_major_to_write_medium_info()
{
  int64_t _;
  // 1. 插入数据
  ASSERT_EQ(OB_SUCCESS, GCTX.sql_proxy_->write(OB_SYS_TENANT_ID, "alter system minor freeze", _));
}

void do_flush_mds_table(share::ObLSID ls_id, ObTabletID tablet_id)
{
  // 1. 从ls_id找到ls
  storage::ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(storage::ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
  // 2. 从ls拿到tablet handle
  storage::ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  // 3. 从tablet handle拿到tablet pointer
  const ObTablet::ObTabletPointerHandle &pointer_handle = tablet_handle.get_obj()->get_pointer_handle();
  ObMetaPointer<oceanbase::storage::ObTablet> *resource_ptr = pointer_handle.get_resource_ptr();
  ObTabletPointer *tablet_pointer = dynamic_cast<ObTabletPointer *>(resource_ptr);
  // 4. 做flush动作
  mds::MdsTableHandle handle;
  ASSERT_EQ(OB_SUCCESS, tablet_pointer->get_mds_table(handle));
  ASSERT_EQ(OB_SUCCESS, handle.flush(share::SCN::max_scn()));
  ASSERT_EQ(true, handle.p_mds_table_base_->flushing_scn_.is_valid());
  // 5. 等flush完成
  share::SCN rec_scn = share::SCN::min_scn();
  while (!rec_scn.is_max()) {
    ASSERT_EQ(OB_SUCCESS, handle.get_rec_scn(rec_scn));
    sleep(1);
    OCCAM_LOG(INFO, "waiting node dump", K(rec_scn));
  }
}

void do_recycle_and_gc_mds_table(share::ObLSID ls_id, ObTabletID tablet_id)
{
  int ret = OB_SUCCESS;
  // 1. 从ls_id找到ls
  storage::ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(storage::ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
  // 2. 从ls拿到tablet handle
  storage::ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  // 3. gc mds table
  mds::ObTenantMdsTimer timer;
  while (OB_FAIL(timer.process_with_tablet_(*tablet_handle.get_obj()))) {
    sleep(1);
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

void get_latest_scn(share::ObLSID ls_id, ObTabletID tablet_id, uint64_t &latest_scn)
{
  int ret = OB_SUCCESS;
  char where_condition[512] = { 0 };
  databuff_printf(where_condition, 512, "where tenant_id=%ld and ls_id =%ld and tablet_id=%ld and end_scn != 18446744073709551615 order by end_scn desc limit 1",
                                         MTL_ID(), ls_id.id(), tablet_id.id());
  ret = ObTableAccessHelper::read_single_row(MTL_ID(),
                                            {"end_scn"},
                                            OB_ALL_VIRTUAL_MDS_NODE_STAT_TNAME,
                                            where_condition,
                                            latest_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void read_virtual_mds_stat(share::ObLSID ls_id, ObTabletID tablet_id, VirtualTableResult &result) {
  int ret = OB_SUCCESS;
  char where_condition[512] = { 0 };
  result.reset();
  databuff_printf(where_condition, 512, "where tenant_id=%ld and ls_id =%ld and tablet_id=%ld and position='DISK'",
                                         MTL_ID(), ls_id.id(), tablet_id.id());
  int retry_time = 0;
  do {
    if (ret == OB_TABLE_NOT_EXIST) {
      sleep(1);
    }
    ret =  ObTableAccessHelper::read_multi_row(MTL_ID(),
                                              {"unit_id", "user_key", "version_idx", "writer_type", "writer_id", "seq_no",
                                              "redo_scn", "end_scn", "trans_version", "node_type", "state", "user_data",
                                              "position"},
                                              OB_ALL_VIRTUAL_MDS_NODE_STAT_TNAME,
                                              where_condition,
                                              result);
  } while (ret == OB_TABLE_NOT_EXIST && ++retry_time < 20);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void advance_checkpoint(share::ObLSID ls_id, share::SCN aim_scn)
{
  int ret = OB_SUCCESS;
  // 1. 从ls_id找到ls
  storage::ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(storage::ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::TRANS_MOD));
  int64_t retry_times = 60;
  SCN checkpoint = SCN::min_scn();
  // 2. 等max_decided_scn推过目标点
  SCN max_decided_scn;
  do {
    ret = ls_handle.get_ls()->get_max_decided_scn(max_decided_scn);
    ::sleep(1);
    fprintf(stdout,
            "waiting advance max decided scn, max_decided_scn = %lu aim_scn = %lu\n",
            max_decided_scn.get_val_for_inner_table_field(),
            aim_scn.get_val_for_inner_table_field());
  } while (OB_SUCC(ret) && max_decided_scn <= aim_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GT(max_decided_scn, aim_scn);
  // 3. 推checkpoint并且等checkpoint推过
  while (--retry_times > 0) {
    checkpoint = ls_handle.get_ls()->get_clog_checkpoint_scn();
    if (checkpoint > aim_scn) {
      fprintf(stdout,
              "checkpoint scn is higher than aim scn, checkpoint_scn:%ld, aim_scn:%ld\n",
              checkpoint.get_val_for_inner_table_field(),
              aim_scn.get_val_for_inner_table_field());
      break;
    } else {
      ::sleep(1);
      fprintf(stdout,
              "waiting advance checkpoint, checkpoint = %lu aim_scn = %lu\n",
              checkpoint.get_val_for_inner_table_field(),
              aim_scn.get_val_for_inner_table_field());
      ls_handle.get_ls()->checkpoint_executor_.advance_checkpoint_by_flush(SCN::scn_inc(aim_scn));
    }
  }
  ASSERT_GT(ls_handle.get_ls()->get_clog_checkpoint_scn(), aim_scn);
  // 4. 等CLOG回收日志
  SCN min_start_scn = SCN::min_scn();
  SCN keep_alive_scn =  SCN::min_scn();
  MinStartScnStatus status = MinStartScnStatus::UNKOWN;
  retry_times = 40;
  while (--retry_times > 0) {
    ls_handle.get_ls()->get_min_start_scn(min_start_scn, keep_alive_scn, status);
    if (MinStartScnStatus::HAS_CTX == status) {
      break;
    } else {
      ::sleep(1);
      fprintf(stdout,
              "waiting min_start_scn has ctx min_start_scn = %ld keep_alive_scn = %ld status = %d\n",
              min_start_scn.get_val_for_inner_table_field(),
              keep_alive_scn.get_val_for_inner_table_field(),
              status);
    }
  }
  ASSERT_NE(SCN::min_scn(), min_start_scn);
  ASSERT_NE(SCN::min_scn(), keep_alive_scn);
  ASSERT_EQ(MinStartScnStatus::HAS_CTX, status);
  fprintf(stdout,
          "advance checkpoint done, checkpoint = %lu aim_scn = %lu min_start_scn = %lu keep_alive_scn = %lu\n",
          checkpoint.get_val_for_inner_table_field(),
          aim_scn.get_val_for_inner_table_field(),
          min_start_scn.get_val_for_inner_table_field(),
          keep_alive_scn.get_val_for_inner_table_field());
}

void write_result_to_file(const char *file_name)
{
  char buffer[2048] = { 0 };
  databuff_printf(buffer, 2048, "result from virtual table:%s", to_cstring(RESULT));
  std::ofstream file(file_name);
  file << TEST_LS_ID.id() << std::endl
        << TEST_TABLET_ID.id() << std::endl
        << LAST_SCN.val_ << std::endl
        << buffer << std::endl;
}

TEST_F(ObTestMdsBeforeRecover, before_recover_test)
{
  ObLSID ls_id;
  common::ObTabletID tablet_id;
  share::SCN aim_checkpoint;
  uint64_t scn_from_table;
  int ret = OB_SUCCESS;
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    create_or_find_test_table("test_a", ls_id, tablet_id, true);
    insert_row_to_write_inc_seq("test_a");
    add_column_to_write_dll_info("test_a", TEST_LS_ID, TEST_TABLET_ID);
    do_flush_mds_table(TEST_LS_ID, TEST_TABLET_ID);
    do_recycle_and_gc_mds_table(TEST_LS_ID, TEST_TABLET_ID);
    get_latest_scn(TEST_LS_ID, TEST_TABLET_ID, scn_from_table);
    read_virtual_mds_stat(TEST_LS_ID, TEST_TABLET_ID, RESULT);
    ASSERT_EQ(OB_SUCCESS, LAST_SCN.convert_for_inner_table_field(scn_from_table));
    advance_checkpoint(TEST_LS_ID, LAST_SCN);
    write_result_to_file(BEFORE_RECOVER_RESULT_FILE);
  }
}

class ObTestMdsAfterRecover : public ObSimpleClusterTestBase
{
public:
  ObTestMdsAfterRecover() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
};

bool compare_before_and_after_recover_results(const char *file1, const char *file2)
{
  ifstream file1_stream(file1);
  ifstream file2_stream(file2);
  cout << file1 << " context:" << endl << file1_stream.rdbuf() << endl;
  cout << file2 << " context:" << endl << file2_stream.rdbuf() << endl;
  char c1, c2;
  while (file1_stream.get(c1) && file2_stream.get(c2)) {
    if (c1 != c2) {
      return false;
    }
  }
  return true;
}

TEST_F(ObTestMdsAfterRecover, after_recover_test)
{
  int ret = OB_SUCCESS;
  {
    std::ifstream file(BEFORE_RECOVER_RESULT_FILE);
    file >> TEST_LS_ID.id_ >> TEST_TABLET_ID.id_ >> LAST_SCN.val_;
    ASSERT_GE(REPLAY_BASE_SCN, LAST_SCN);
  }
  MTL_SWITCH(OB_SYS_TENANT_ID)
  {
    read_virtual_mds_stat(TEST_LS_ID, TEST_TABLET_ID, RESULT);// to fill cache
    read_virtual_mds_stat(TEST_LS_ID, TEST_TABLET_ID, RESULT);
    write_result_to_file(AFTER_RECOVER_RESULT_FILE);
    ASSERT_EQ(true, compare_before_and_after_recover_results(BEFORE_RECOVER_RESULT_FILE, AFTER_RECOVER_RESULT_FILE));
  }
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char **argv)
{
  int c = 0;
  int time_sec = 0;
  int concurrency = 1;
  char *log_level = (char *)"INFO";
  while (EOF != (c = getopt(argc, argv, "t:l:"))) {
    switch (c) {
      case 't':
        time_sec = atoi(optarg);
        break;
      case 'l':
        log_level = optarg;
        oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
        break;
      case 'c':
        concurrency = atoi(optarg);
        break;
      default:
        break;
    }
  }
  std::string gtest_file_name = std::string(TEST_FILE_NAME) + "_gtest.log";
  oceanbase::unittest::init_gtest_output(gtest_file_name);

  int ret = 0;
  ObSimpleServerRestartHelper restart_helper(argc, argv, TEST_FILE_NAME, BORN_CASE_NAME,
                                             RESTART_CASE_NAME);
  helper_ptr = &restart_helper;
  restart_helper.set_sleep_sec(time_sec);
  restart_helper.run();

  return ret;
}

namespace oceanbase {
namespace logservice
{
int ObReplayStatus::enable(const LSN &base_lsn, const SCN &base_scn)
{
  /****************************************************************************/
  REPLAY_BASE_SCN = base_scn;
  /****************************************************************************/
  int ret = OB_SUCCESS;
  if (is_enabled()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "replay status already enable", K(ret));
  } else {
    WLockGuard wlock_guard(rwlock_);
    if (OB_FAIL(enable_(base_lsn, base_scn))) {
      CLOG_LOG(WARN, "enable replay status failed", K(ret), K(base_lsn), K(base_scn), K(ls_id_));
    } else {
      CLOG_LOG(INFO, "enable replay status success", K(ret), K(base_lsn), K(base_scn), K(ls_id_));
    }
  }
  return ret;
}
}
}  // namespace oceanbase