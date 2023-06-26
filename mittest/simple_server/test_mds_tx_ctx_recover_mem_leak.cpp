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
#include "lib/time/ob_time_utility.h"
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

static const char *TEST_FILE_NAME = "test_mds_tx_ctx_recover_mem_leak";
static const char *BORN_CASE_NAME = "ObTestMdsTxCtxRecoverMemLeakBeforeRecover";
static const char *RESTART_CASE_NAME = "ObTestMdsTxCtxRecoverMemLeakAfterRecover";

using namespace std;

ObSimpleServerRestartHelper *helper_ptr = nullptr;

namespace oceanbase
{
using namespace transaction;
using namespace storage;
using namespace share;
namespace unittest
{
class ObTestMdsTxCtxRecoverMemLeakBeforeRecover : public ObSimpleClusterTestBase
{
public:
  ObTestMdsTxCtxRecoverMemLeakBeforeRecover() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
};

void wait_user()
{
  std::cout << "Enter anything to continue..." << std::endl;
  char a = '\0';
  std::cin >> a;
}

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int time_sec_ = 0;
};

TestRunCtx RunCtx;

TEST_F(ObTestMdsTxCtxRecoverMemLeakBeforeRecover, add_tenant)
{
  OCCAM_LOG(INFO, "step 1: 创建普通租户tt1");
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  OCCAM_LOG(INFO, "step 2: 获取租户tt1的tenant_id");
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  OCCAM_LOG(INFO, "step 3: 初始化普通租户tt1的sql proxy");
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

TEST_F(ObTestMdsTxCtxRecoverMemLeakBeforeRecover, create_table_then_insert_then_minor_freeze)
{
  int ret = OB_SUCCESS;
  OCCAM_LOG(INFO, "step 4: 使用普通租户tt1的proxy");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  OCCAM_LOG(INFO, "step 5: 创建无主键表");
  int64_t affected_rows = 0;
  sleep(3);
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create table test(a int)", affected_rows));
  OCCAM_LOG(INFO, "step 6: 开启事务");
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("begin", affected_rows));
  OCCAM_LOG(INFO, "step 7: 插入一条数据，此时会修改多源数据auto_inc");
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("insert into test values(1)", affected_rows));
  int64_t do_freeze_time = ObTimeUtility::current_time();
  OCCAM_LOG(INFO, "step 8: 执行minor freeze，确保tx_ctx_table转储下去了(带着mds ctx一起)");
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system minor freeze", affected_rows));
  OCCAM_LOG(INFO, "step 9: 从表名拿到它的tablet_id");
  ObTabletID tablet_id;
  ObLSID ls_id;
  char where_condition[512] = { 0 };
  databuff_printf(where_condition, 512, "where table_name = '%s'", "test");
  ASSERT_EQ(OB_SUCCESS, ObTableAccessHelper::read_single_row(RunCtx.tenant_id_,
                                                            {"tablet_id"},
                                                            OB_ALL_TABLE_TNAME,
                                                            where_condition,
                                                            tablet_id));
  OCCAM_LOG(INFO, "step 10: 从tablet_id拿到它的ls_id");
  databuff_printf(where_condition, 512, "where tablet_id = %ld", tablet_id.id());
  ASSERT_EQ(OB_SUCCESS, ObTableAccessHelper::read_single_row(RunCtx.tenant_id_,
                                                            {"ls_id"},
                                                            OB_ALL_TABLET_TO_LS_TNAME,
                                                            where_condition,
                                                            ls_id));
  int64_t tx_ctx_table_compaction_time = 0;
  do {
    databuff_printf(where_condition, 512, "where tenant_id = %ld and ls_id = %ld and tablet_id = 49401 order by finish_time desc limit 1", RunCtx.tenant_id_, ls_id.id());
    ret = ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID, {"TIME_TO_USEC(finish_time)"}, OB_ALL_VIRTUAL_TABLET_COMPACTION_HISTORY_TNAME, where_condition, tx_ctx_table_compaction_time);
    OCCAM_LOG(INFO, "read compaction history", KR(ret), K(tx_ctx_table_compaction_time), K(RunCtx.tenant_id_), K(ls_id), K(tablet_id));
    ASSERT_EQ(true, OB_SUCCESS == ret || OB_ITER_END == ret);
  } while (tx_ctx_table_compaction_time <= do_freeze_time);
}

class ObTestMdsTxCtxRecoverMemLeakAfterRecover : public ObSimpleClusterTestBase
{
public:
  ObTestMdsTxCtxRecoverMemLeakAfterRecover() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
};

TEST_F(ObTestMdsTxCtxRecoverMemLeakAfterRecover, after_recover_test)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  OCCAM_LOG(INFO, "step 10: before delete tenant");
  ASSERT_EQ(OB_SUCCESS, delete_tenant("tt1"));
  OCCAM_LOG(INFO, "step 11: after delete tenant");
  int64_t result_num = 0;
  // 等待租户的MTL组建析构，此时会检查内存泄露
  do {
    char where_condition[512] = { 0 };
    databuff_printf(where_condition, 512, "where event = 'remove_tenant' and value1 = 1002");
    ASSERT_EQ(OB_SUCCESS, ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID, {"count(*)"}, OB_ALL_ROOTSERVICE_EVENT_HISTORY_TNAME, where_condition, result_num));
    std::this_thread::sleep_for(std::chrono::seconds(1));
    OCCAM_LOG(INFO, "step 12: wait MTL destroy");
  } while (result_num == 0);
  OCCAM_LOG(INFO, "step 13: MTL destroy done");
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char **argv)
{
  int c = 0;
  int time_sec = 0;
  int concurrency = 1;
  char *log_level = (char *)"INFO";
  system("rm -rf ./test_mds_tx_ctx_recover_mem_leak_*");
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