/**
 * Copyright (c) 2025 OceanBase
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
#include <thread>
#include <iostream>
#define protected public
#define private public

#include "common/ob_role.h"
#include "storage/incremental/sslog/ob_sslog_table_proxy.h"
#include "storage/incremental/sslog/ob_i_sslog_proxy.h"
#include "storage/ls/ob_ls.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "lib/string/ob_string.h"
#include "lib/ob_define.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/scn.h"
#include "mittest/simple_server/env/ob_simple_server_restart_helper.h"
#include "close_modules/shared_storage/storage/incremental/sslog/notify/ob_sslog_notify_adapter.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_i_sslog_proxy.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_proxy.h"

static const char *TEST_FILE_NAME = "test_sslog_basic";
static const char *BORN_CASE_NAME = "ObTestSSLogBasic";
static const char *RESTART_CASE_NAME = "ObSSLogAfterRestartTest";

oceanbase::unittest::ObMockPalfKV PALF_KV;

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
namespace sslog
{

int get_sslog_table_guard(const ObSSLogTableType type,
                          const int64_t tenant_id,
                          ObSSLogProxyGuard &guard)
{
  int ret = OB_SUCCESS;

  switch (type)
  {
    case ObSSLogTableType::SSLOG_TABLE: {
      void *proxy = share::mtl_malloc(sizeof(ObSSLogTableProxy), "ObSSLogTable");
      if (nullptr == proxy) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObSSLogTableProxy *sslog_table_proxy = new (proxy) ObSSLogTableProxy(tenant_id);
        if (OB_FAIL(sslog_table_proxy->init())) {
          SSLOG_LOG(WARN, "fail to inint", K(ret));
        } else {
          guard.set_sslog_proxy((ObISSLogProxy *)proxy);
        }
      }
      break;
    }
    case ObSSLogTableType::SSLOG_PALF_KV: {
      void *proxy = share::mtl_malloc(sizeof(ObSSLogKVProxy), "ObSSLogTable");
      if (nullptr == proxy) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObSSLogKVProxy *sslog_kv_proxy = new (proxy) ObSSLogKVProxy(&PALF_KV);
        // if (OB_FAIL(sslog_kv_proxy->init(GCONF.cluster_id, tenant_id))) {
        //   SSLOG_LOG(WARN, "init palf kv failed", K(ret));
        // } else {
          guard.set_sslog_proxy((ObISSLogProxy *)proxy);
        // }
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      SSLOG_LOG(WARN, "invalid sslog type", K(type));
      break;
    }
  }

  return ret;
}



int ObSSLogNotifyAdapter::get_is_finish_flag_from_extra_info_column(const char *,
                                                                    const int64_t,
                                                                    bool &is_finish_flag)
{
  is_finish_flag = false;
  return OB_SUCCESS;
}
}
char *shared_storage_info = NULL;

namespace common {
bool is_shared_storage_sslog_table(const uint64_t tid)
{
  return OB_ALL_SSLOG_TABLE_TID == tid;
}

bool is_shared_storage_sslog_exist()
{
  return true;
}
}

namespace unittest
{

using namespace common;

#define EXE_SQL(sql_str)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define EXE_SQL_FMT(...)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__));               \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));


class ObTestSSLogBasic : public ObSimpleClusterTestBase
{
public:
  ObTestSSLogBasic() : ObSimpleClusterTestBase(TEST_FILE_NAME, "50G", "50G", "50G") {}
  void wait_sys_to_leader()
  {
    share::ObTenantSwitchGuard tenant_guard;
    int ret = OB_ERR_UNEXPECTED;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(1));
    ObLS *ls = nullptr;
    ObLSID ls_id(ObLSID::SYS_LS_ID);
    ObLSHandle handle;
    ObLSService *ls_svr = MTL(ObLSService *);
    EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
    ls = handle.get_ls();
    ASSERT_NE(nullptr, ls);
    ASSERT_EQ(ls_id, ls->get_ls_id());
    for (int i=0; i<100; i++) {
      ObRole role;
      int64_t proposal_id = 0;
      ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_role(role, proposal_id));
      if (role == ObRole::LEADER) {
        ret = OB_SUCCESS;
        break;
      }
      ob_usleep(10 * 1000);
    }
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  void create_test_tenant(uint64_t &tenant_id)
  {
    TRANS_LOG(INFO, "create_tenant start");
    wait_sys_to_leader();
    int ret = OB_SUCCESS;
    int retry_cnt = 0;
    do {
      if (OB_FAIL(create_tenant("tt1"))) {
        TRANS_LOG(WARN, "create_tenant fail, need retry", K(ret));
        ob_usleep(15 * 1000 * 1000); // 15s
      }
      retry_cnt++;
    } while (OB_FAIL(ret) && retry_cnt < 10);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  }
};

#define MULTI_VERSION_READ_INIT                                         \
  ObSSLogProxyGuard guard;                                              \
  char *key_buf = (char *)mtl_malloc(meta_key1.length()+1, "MITTEST");  \
  key_buf[meta_key1.length()] = '\0';                                   \
  ASSERT_EQ(true, key_buf != NULL);                                     \
  ASSERT_EQ(OB_SUCCESS, sslog::get_sslog_table_guard(ObSSLogTableType::SSLOG_TABLE, MTL_ID(), guard)); \
  sslog::ObISSLogProxy *proxy_ptr = guard.get_sslog_proxy();            \
  MEMCPY(key_buf, meta_key1.ptr(), meta_key1.length());                 \
  const common::ObString meta_key_malloc(meta_key1.length(), key_buf);  \
  ObSSLogIteratorGuard iter(false/*read_unfinish*/, true/*read_init_value*/, true/*read_mark_delete*/);

#define SSLOG_TABLE_READ_INIT                                           \
  ObSSLogIteratorGuard iter(true/*read_unfinish*/, true/*read_init_value*/, true/*read_mark_delete*/);

TEST_F(ObTestSSLogBasic, test_sslog_basic)
{
  if (is_shared_storage_sslog_table(542)) {
    TRANS_LOG(INFO, "qcc debug");
  }

  int ret = OB_SUCCESS;
  TRANS_LOG(INFO, "create tenant start");
  uint64_t tenant_id = 0;
  create_test_tenant(tenant_id);
  TRANS_LOG(INFO, "create tenant end", K(tenant_id));

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  ObSSLogProxyGuard guard;
  ASSERT_EQ(OB_SUCCESS, sslog::get_sslog_table_guard(ObSSLogTableType::SSLOG_TABLE,
                                                     tenant_id,
                                                     guard));
  sslog::ObISSLogProxy *sslog_table_proxy = guard.get_sslog_proxy();

  // ========== Example 1 ==========
  int64_t affected_rows = 0;
  const sslog::ObSSLogMetaType meta_type1 = sslog::ObSSLogMetaType::SSLOG_TABLET_META;
  // key format example: tenant_id;ls_id;tablet_id
  const common::ObString meta_key1 = "1002;1001;200001";
  const common::ObString meta_value1 = "qc_example1";
  const common::ObString extra_info1= "qc_extra1";
  const common::ObString meta_value1_new = "qc_example1_new";
  const common::ObString meta_value1_new_2 = "qc_example1_new_2";
  const common::ObString meta_value1_new_3 = "qc_example1_new_3";
  const common::ObString meta_value1_new_4 = "qc_example1_new_4";
  const common::ObString extra_info1_new = "qc_extra1_new";
  const common::ObString extra_info1_new_2 = "qc_extra1_new_2";
  const common::ObString extra_info1_new_3 = "qc_extra1_new_3";
  const common::ObString extra_info1_new_4 = "qc_extra1_new_4";

  // ========== Example 2 ==========
  const sslog::ObSSLogMetaType meta_type2 = sslog::ObSSLogMetaType::SSLOG_TABLET_META;
  // key format example: tenant_id;ls_id;tablet_id
  const common::ObString meta_key2 = "1002;1001;200002";
  const common::ObString meta_value2 = "qc_example2";
  const common::ObString extra_info2= "qc_extra2";

  // ========== Universal ==========
  common::ObString meta_key_ret;
  common::ObString meta_value_ret;
  common::ObString extra_info_ret;
  SCN row_scn_ret;
  const common::ObString meta_key_prefix = "1002;1001;";

  // ========== Test 1: insert one row =========
  ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->insert_row(meta_type1,
                                                     meta_key1,
                                                     meta_value1,
                                                     extra_info1,
                                                     affected_rows));
  ASSERT_EQ(1, affected_rows);

  // ========== Test 2: read the row =========
  {
    SSLOG_TABLE_READ_INIT
    sslog::ObSSLogReadParam param(false/*read_local*/, false/*read_row*/);
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                     meta_type1,
                                                     meta_key1,
                                                     iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  // ========== Test 3: insert another row =========
  ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->insert_row(meta_type2,
                                                     meta_key2,
                                                     meta_value2,
                                                     extra_info2,
                                                     affected_rows));
  ASSERT_EQ(1, affected_rows);

  // ========== Test 4: read the prefix =========
  {
    SSLOG_TABLE_READ_INIT
    sslog::ObSSLogReadParam param(false/*read_local*/, true/*read_row*/);
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                     meta_type1,
                                                     meta_key_prefix,
                                                     iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value2, meta_value_ret);
    ASSERT_EQ(extra_info2, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }
  {
    SSLOG_TABLE_READ_INIT
    sslog::ObSSLogReadParam param(false/*read_local*/, true/*read_row*/);
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                      meta_type1,
                                                      meta_key_prefix,
                                                      iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_key_v2(row_scn_ret, meta_key_ret, extra_info_ret));
    ASSERT_EQ(meta_key1, meta_key_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_key_v2(row_scn_ret, meta_key_ret, extra_info_ret));
    ASSERT_EQ(meta_key2, meta_key_ret);
    ASSERT_EQ(extra_info2, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_key_v2(row_scn_ret, meta_key_ret, extra_info_ret));
  }


  int64_t current_time = ObTimeUtility::current_time();
  share::SCN snapshot_version1;
  ASSERT_EQ(OB_SUCCESS, snapshot_version1.convert_from_ts(current_time));
  ob_usleep(1_s);

  // ========== Test 5: update and read the row =========
  {
    ObSSLogWriteParam write_param(false, false, ObString());
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->update_row(write_param,
                                                       meta_type1,
                                                       meta_key1,
                                                       meta_value1_new,
                                                       extra_info1_new,
                                                       affected_rows));
  }

  {
    SSLOG_TABLE_READ_INIT
    sslog::ObSSLogReadParam param(false/*read_local*/, false/*read_row*/);
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                     meta_type1,
                                                     meta_key1,
                                                     iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new, meta_value_ret);
    ASSERT_EQ(extra_info1_new, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  // ========== Test 6: delete and read the prefix =========
  ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->delete_row(meta_type2,
                                                     meta_key2,
                                                     affected_rows));
  ASSERT_EQ(1, affected_rows);

  {
    SSLOG_TABLE_READ_INIT
    sslog::ObSSLogReadParam param(false/*read_local*/, true/*read_row*/);
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                        meta_type1,
                                                        meta_key_prefix,
                                                        iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new, meta_value_ret);
    ASSERT_EQ(extra_info1_new, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  // ========== Test 7: read the snapshot =========
  {
    SSLOG_TABLE_READ_INIT
    sslog::ObSSLogReadParam param(false/*read_local*/, false/*read_row*/, snapshot_version1);
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                     meta_type1,
                                                     meta_key1,
                                                     iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  {
    SSLOG_TABLE_READ_INIT
    sslog::ObSSLogReadParam param(false/*read_local*/, true/*read_row*/, snapshot_version1);
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                     meta_type1,
                                                     meta_key_prefix,
                                                     iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value2, meta_value_ret);
    ASSERT_EQ(extra_info2, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  // ========== Test 8: read the multi_version =========
  current_time = ObTimeUtility::current_time();
  share::SCN snapshot_version2;
  ASSERT_EQ(OB_SUCCESS, snapshot_version2.convert_from_ts(current_time));
  ob_usleep(1_s);
  ObSSLogWriteParam write_param8(false, false, ObString());

  ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->update_row(write_param8,
                                                      meta_type1,
                                                      meta_key1,
                                                      meta_value1_new_2,
                                                      extra_info1_new_2,
                                                      affected_rows));
  ASSERT_EQ(1, affected_rows);

  ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->update_row(write_param8,
                                                      meta_type1,
                                                      meta_key1,
                                                      meta_value1_new_3,
                                                      extra_info1_new_3,
                                                      affected_rows));
  ASSERT_EQ(1, affected_rows);

  {
    MULTI_VERSION_READ_INIT
    ObSSLogMultiVersionReadParam param(share::SCN::invalid_scn(),
                                       share::SCN::invalid_scn(),
                                       false /*include_last_version*/);
    ASSERT_EQ(OB_SUCCESS, proxy_ptr->read_multi_version_row(param,
                                                            meta_type1,
                                                            meta_key_malloc,
                                                            iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_3, meta_value_ret);
    ASSERT_EQ(extra_info1_new_3, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_2, meta_value_ret);
    ASSERT_EQ(extra_info1_new_2, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new, meta_value_ret);
    ASSERT_EQ(extra_info1_new, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  {
    MULTI_VERSION_READ_INIT
    ObSSLogMultiVersionReadParam param(share::SCN::invalid_scn(),
                                       snapshot_version2,
                                       false /*include_last_version*/);
    ASSERT_EQ(OB_SUCCESS, proxy_ptr->read_multi_version_row(param,
                                                            meta_type1,
                                                            meta_key_malloc,
                                                            iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_3, meta_value_ret);
    ASSERT_EQ(extra_info1_new_3, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_2, meta_value_ret);
    ASSERT_EQ(extra_info1_new_2, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  // {
  //   MULTI_VERSION_READ_INIT
  //   ObSSLogMultiVersionReadParam param(share::SCN::invalid_scn(),
  //                                      snapshot_version2,
  //                                      true /*include_last_version*/);
  //   ASSERT_EQ(OB_SUCCESS, proxy_ptr->read_multi_version_row(param,
  //                                                           meta_type1,
  //                                                           meta_key_malloc,
  //                                                           iter));
  //   ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  //   ASSERT_EQ(meta_value1_new_3, meta_value_ret);
  //   ASSERT_EQ(extra_info1_new_3, extra_info_ret);
  //   ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  //   ASSERT_EQ(meta_value1_new_2, meta_value_ret);
  //   ASSERT_EQ(extra_info1_new_2, extra_info_ret);
  //   ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  // }

  {
    MULTI_VERSION_READ_INIT
    ObSSLogMultiVersionReadParam param(snapshot_version2,
                                       share::SCN::invalid_scn(),
                                       false /*include_last_version*/);
    ASSERT_EQ(OB_SUCCESS, proxy_ptr->read_multi_version_row(param,
                                                            meta_type1,
                                                            meta_key_malloc,
                                                            iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new, meta_value_ret);
    ASSERT_EQ(extra_info1_new, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  // ========== Test 9: only update extra info =========
  {
    ObSSLogWriteParam write_param(true, false, ObString());
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->update_row(write_param,
                                                      meta_type1,
                                                      meta_key1,
                                                      ObString(),
                                                      extra_info1_new_4,
                                                      affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  {
    SSLOG_TABLE_READ_INIT
    sslog::ObSSLogReadParam param(false/*read_local*/, false/*read_row*/);
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                     meta_type1,
                                                     meta_key1,
                                                     iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_3, meta_value_ret);
    ASSERT_EQ(extra_info1_new_4, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  // ========== Test 10: update with extra info check =========
  {
    ObSSLogWriteParam write_param(false, true, extra_info1_new_4);
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->update_row(write_param,
                                                        meta_type1,
                                                        meta_key1,
                                                        meta_value1_new_4,
                                                        extra_info1_new_4,
                                                        affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  {
    SSLOG_TABLE_READ_INIT
    sslog::ObSSLogReadParam param(false/*read_local*/, false/*read_row*/);
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                     meta_type1,
                                                     meta_key1,
                                                     iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_4, meta_value_ret);
    ASSERT_EQ(extra_info1_new_4, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }


  // ========== Test 11: update with larger than 2M =========
  const sslog::ObSSLogMetaType meta_type3 = sslog::ObSSLogMetaType::SSLOG_TABLET_META;
  const int64_t meta_value3_size = 1.5 * 1024 * 1024;
  char *meta_value3_buffer = (char *)ob_malloc(meta_value3_size, "qcc debug");
  memset(meta_value3_buffer, 'a', meta_value3_size);
  const common::ObString meta_key3 = "1002;1001;200003";
  common::ObString meta_value3;
  meta_value3.assign_ptr(meta_value3_buffer, meta_value3_size);
  const common::ObString extra_info3 = "qc_extra3";

  ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->insert_row(meta_type3,
                                                      meta_key3,
                                                      meta_value3,
                                                      extra_info3,
                                                      affected_rows));
  ASSERT_EQ(1, affected_rows);

  {
    SSLOG_TABLE_READ_INIT
    sslog::ObSSLogReadParam param(false/*read_local*/, false/*read_row*/);
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                      meta_type3,
                                                      meta_key3,
                                                      iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value3, meta_value_ret);
    ASSERT_EQ(extra_info3, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  {
    ObSSLogWriteParam write_param(false, false, ObString());
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->update_row(write_param,
                                                        meta_type3,
                                                        meta_key3,
                                                        meta_value2,
                                                        extra_info2,
                                                        affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  {
    SSLOG_TABLE_READ_INIT
      sslog::ObSSLogReadParam param(false/*read_local*/, false/*read_row*/);
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                      meta_type3,
                                                      meta_key3,
                                                      iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value2, meta_value_ret);
    ASSERT_EQ(extra_info2, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  {
    ObSSLogWriteParam write_param(false, false, ObString());
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->update_row(write_param,
                                                        meta_type3,
                                                        meta_key3,
                                                        meta_value3,
                                                        extra_info3,
                                                        affected_rows));
    ASSERT_EQ(1, affected_rows);
  }

  {
    SSLOG_TABLE_READ_INIT
      sslog::ObSSLogReadParam param(false/*read_local*/, false/*read_row*/);
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                      meta_type3,
                                                      meta_key3,
                                                      iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value3, meta_value_ret);
    ASSERT_EQ(extra_info3, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

    {
    MULTI_VERSION_READ_INIT
    ObSSLogMultiVersionReadParam param(share::SCN::invalid_scn(),
                                       share::SCN::invalid_scn(),
                                       false /*include_last_version*/);
    ASSERT_EQ(OB_SUCCESS, proxy_ptr->read_multi_version_row(param,
                                                            meta_type3,
                                                            meta_key3,
                                                            iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value3, meta_value_ret);
    ASSERT_EQ(extra_info3, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value2, meta_value_ret);
    ASSERT_EQ(extra_info2, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value3, meta_value_ret);
    ASSERT_EQ(extra_info3, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }
}

TEST_F(ObTestSSLogBasic, test_sslog_schema)
{
  int64_t affected_rows = 0;
  ObSqlString sql;

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(1));

  // prepare system variables
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    EXE_SQL("delete from oceanbase.__all_table_history where table_id = 542");
    EXE_SQL("delete from oceanbase.__all_table where table_id = 542");
  }
}

class ObSSLogAfterRestartTest : public ObSimpleClusterTestBase
{
public:
  ObSSLogAfterRestartTest() : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
};

TEST_F(ObSSLogAfterRestartTest, test_sslog_schema)
{
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  TRANS_LOG(INFO, "get_tenant end", K(tenant_id));

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  ObSSLogProxyGuard guard;
  ASSERT_EQ(OB_SUCCESS, sslog::get_sslog_table_guard(ObSSLogTableType::SSLOG_TABLE,
                                                     tenant_id,
                                                     guard));
  sslog::ObISSLogProxy *sslog_table_proxy = guard.get_sslog_proxy();

  // ========== Example 1 ==========
  int64_t affected_rows = 0;
  const sslog::ObSSLogMetaType meta_type1 = sslog::ObSSLogMetaType::SSLOG_TABLET_META;
  // key format example: tenant_id;ls_id;tablet_id
  const common::ObString meta_key1 = "1002;1001;200001";
  const common::ObString meta_value1 = "qc_example1";
  const common::ObString extra_info1= "qc_extra1";
  const common::ObString meta_value1_new = "qc_example1_new";
  const common::ObString meta_value1_new_2 = "qc_example1_new_2";
  const common::ObString meta_value1_new_3 = "qc_example1_new_3";
  const common::ObString meta_value1_new_4 = "qc_example1_new_4";
  const common::ObString extra_info1_new = "qc_extra1_new";
  const common::ObString extra_info1_new_2 = "qc_extra1_new_2";
  const common::ObString extra_info1_new_3 = "qc_extra1_new_3";
  const common::ObString extra_info1_new_4 = "qc_extra1_new_4";

  // ========== Example 2 ==========
  const sslog::ObSSLogMetaType meta_type2 = sslog::ObSSLogMetaType::SSLOG_TABLET_META;
  // key format example: tenant_id;ls_id;tablet_id
  const common::ObString meta_key2 = "1002;1001;200002";
  const common::ObString meta_value2 = "qc_example2";
  const common::ObString extra_info2= "qc_extra2";

  // ========== Universal ==========
  common::ObString meta_key_ret;
  common::ObString meta_value_ret;
  common::ObString extra_info_ret;
  SCN row_scn_ret;
  sslog::ObSSLogMetaType meta_type_ret;
  const common::ObString meta_key_prefix = "1002;1001;";

  // ============================== restart successfully ==============================
  {
    SSLOG_TABLE_READ_INIT
    sslog::ObSSLogReadParam param(false/*read_local*/, true/*read_row*/);
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                     meta_type1,
                                                     meta_key_prefix,
                                                     iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_row_v2(row_scn_ret,
                                                    meta_type_ret,
                                                    meta_key_ret,
                                                    meta_value_ret,
                                                    extra_info_ret));
    ASSERT_EQ(meta_type1, meta_type_ret);
    ASSERT_EQ(meta_key1, meta_key_ret);
    ASSERT_EQ(meta_value1_new_4, meta_value_ret);
    ASSERT_EQ(extra_info1_new_4, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }

  {
    MULTI_VERSION_READ_INIT
    ObSSLogMultiVersionReadParam param(share::SCN::invalid_scn(),
                                       share::SCN::invalid_scn(),
                                       false /*include_last_version*/);
    ASSERT_EQ(OB_SUCCESS, proxy_ptr->read_multi_version_row(param,
                                                            meta_type1,
                                                            meta_key_malloc,
                                                            iter));
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_4, meta_value_ret);
    ASSERT_EQ(extra_info1_new_4, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_3, meta_value_ret);
    ASSERT_EQ(extra_info1_new_4, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    TRANS_LOG(INFO, "jianyue debug", K(meta_value_ret), K(meta_value1_new_3));
    ASSERT_EQ(meta_value1_new_3, meta_value_ret);
    ASSERT_EQ(extra_info1_new_3, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new_2, meta_value_ret);
    ASSERT_EQ(extra_info1_new_2, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1_new, meta_value_ret);
    ASSERT_EQ(extra_info1_new, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, iter.get_next_meta_v2(row_scn_ret, meta_value_ret, extra_info_ret));
  }
}

TEST_F(ObTestSSLogBasic, test_sslog_guard)
{
  TRANS_LOG(INFO, "test_sslog_guard start");

  {
    uint64_t tenant_id = 0;
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
    TRANS_LOG(INFO, "get_tenant end", K(tenant_id));

    share::ObTenantSwitchGuard tenant_guard;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

    ObSSLogProxyGuard guard;
    ASSERT_EQ(OB_SUCCESS, sslog::get_sslog_table_guard(ObSSLogTableType::SSLOG_TABLE,
                                                       tenant_id,
                                                       guard));
    ASSERT_EQ(1, guard.get_sslog_proxy()->ref_);

    ObSSLogProxyGuard guard2;
    guard2.set_sslog_proxy(guard.get_sslog_proxy());
    ASSERT_EQ(2, guard.get_sslog_proxy()->ref_);
    ASSERT_EQ(2, guard2.get_sslog_proxy()->ref_);

    guard2.reset();
    ASSERT_EQ(1, guard.get_sslog_proxy()->ref_);

    guard2.set_sslog_proxy(guard.get_sslog_proxy());
    ASSERT_EQ(2, guard.get_sslog_proxy()->ref_);
    ASSERT_EQ(2, guard2.get_sslog_proxy()->ref_);
  }

  TRANS_LOG(INFO, "test_sslog_guard end");
}

} // unittest
} // oceanbase

int main(int argc, char **argv)
{
  int64_t c = 0;
  int64_t time_sec = 0;
  char *log_level = (char*)"INFO";
  char buf[1000];
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  memset(buf, 1000, sizeof(buf));
  databuff_printf(buf, sizeof(buf), "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=2000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns, oceanbase::unittest::S3_ENDPOINT, oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  while(EOF != (c = getopt(argc,argv,"t:l:"))) {
    switch(c) {
    case 't':
      time_sec = atoi(optarg);
      break;
    case 'l':
     log_level = optarg;
     oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
     break;
    default:
      break;
    }
  }
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(log_level);
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(log_level);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
