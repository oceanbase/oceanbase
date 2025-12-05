// owner: handora.qc
// owner group: transaction

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
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public
#include "env/ob_multi_replica_test_base.h"
#include "env/ob_multi_replica_util.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/incremental/sslog/ob_sslog_table_proxy.h"
#include "storage/tx_storage/ob_access_service.h"

#define CUR_TEST_CASE_NAME ObTestMultiSSLog

DEFINE_MULTI_ZONE_TEST_CASE_CLASS
MULTI_REPLICA_TEST_MAIN_FUNCTION(test_multi_sslog_);

int64_t qc_example = 0;

namespace oceanbase
{
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
namespace storage
{
int ObAccessService::check_read_allowed_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObStoreAccessType access_type,
    const ObTableScanParam &scan_param,
    ObTabletHandle &tablet_handle,
    ObStoreCtxGuard &ctx_guard,
    SCN user_specified_snapshot)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;

  LOG_TRACE("print check read allowed, scan param", K(ls_id), K(tablet_id), K(scan_param.fb_read_tx_uncommitted_));
  if (OB_FAIL(ctx_guard.init(ls_id))) {
    LOG_WARN("ctx_guard init fail", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_svr_->get_ls(ls_id, ctx_guard.get_ls_handle(), ObLSGetMod::DAS_MOD))) {
    LOG_WARN("get log stream failed.", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ctx_guard.get_ls_handle().get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), K(ls_id));
  } else {
    ObStoreCtx &ctx = ctx_guard.get_store_ctx();
    ctx.ls_ = ls;
    ctx.timeout_ = scan_param.timeout_;
    ctx.tablet_id_ = tablet_id;
    if (user_specified_snapshot.is_valid() && !scan_param.fb_read_tx_uncommitted_) {
      if (OB_FAIL(ls->get_read_store_ctx(user_specified_snapshot,
                                         scan_param.tx_lock_timeout_,
                                         ctx))) {
        LOG_WARN("get read store ctx fail", K(user_specified_snapshot), K(ls_id), K(ret));
      } else {
        if (OB_ALL_SSLOG_TABLE_TID == tablet_id.id() && 1001 == MTL_ID()) {
          qc_example += 1;
        }
        LOG_WARN("qc debug2", K(ret), K(user_specified_snapshot), K(ls_id), K(ctx),
                 K(scan_param.fb_read_tx_uncommitted_), K(scan_param.snapshot_));
      }
    } else {
      bool read_latest = access_type == ObStoreAccessType::READ_LATEST;
      if (user_specified_snapshot.is_valid()) {
        transaction::ObTxReadSnapshot spec_snapshot;
        if (OB_FAIL(spec_snapshot.assign(scan_param.snapshot_))) {
          LOG_WARN("copy snapshot fail", K(ret));
        } else if (FALSE_IT(spec_snapshot.specify_snapshot_scn(user_specified_snapshot))) {
        } else if (OB_FAIL(ls->get_read_store_ctx(spec_snapshot,
                                                  read_latest,
                                                  scan_param.tx_lock_timeout_,
                                                  ctx))) {
          LOG_WARN("get read store ctx fail", K(ret), K(read_latest),
                   K(scan_param.fb_read_tx_uncommitted_), K(scan_param.snapshot_),
                   K(spec_snapshot), K(user_specified_snapshot), K(ls_id));
        }
      } else if (OB_FAIL(ls->get_read_store_ctx(scan_param.snapshot_,
                                                read_latest,
                                                scan_param.tx_lock_timeout_,
                                                ctx,
                                                scan_param.trans_desc_))) {
        LOG_WARN("get read store ctx fail", K(ret), K(read_latest), K(scan_param.snapshot_), K(ls_id));
      }
      if (OB_FAIL(ret)) {
      } else if (read_latest) {
        if (!scan_param.tx_id_.is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("readlatest need scan_param.tx_id_ valid", K(ret));
        } else {
          ctx.mvcc_acc_ctx_.tx_id_ = scan_param.tx_id_;
        }
      }
    }

    // If this select is for foreign key check,
    // we should get tx_id and tx_desc for deadlock detection.
    if (OB_SUCC(ret)) {
      if (scan_param.is_for_foreign_check_) {
        if (scan_param.tx_id_.is_valid()) {
          ctx.mvcc_acc_ctx_.tx_id_ = scan_param.tx_id_;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("foreign key check need scan_param.tx_id_ valid", K(ret), K(scan_param.tx_id_));
        }
        if (OB_NOT_NULL(scan_param.trans_desc_) && scan_param.trans_desc_->is_valid()) {
          ctx.mvcc_acc_ctx_.tx_desc_ = scan_param.trans_desc_;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("foreign key check need scan_param.trans_desc_ valid", K(ret), KPC(scan_param.trans_desc_));
        }
      }
    }
    if (OB_FAIL(ret)) {
      if (OB_LS_NOT_EXIST == ret) {
        int tmp_ret = OB_SUCCESS;
        bool is_dropped = false;
        schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
        if (OB_ISNULL(schema_service)) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema_service is nullptr", "tmp_ret", tmp_ret);
        } else if (OB_SUCCESS != (tmp_ret = schema_service->check_if_tenant_has_been_dropped(MTL_ID(), is_dropped))) {
          LOG_WARN("check if tenant has been dropped fail", "tmp_ret", tmp_ret);
        } else {
          ret = is_dropped ? OB_TENANT_HAS_BEEN_DROPPED : ret;
        }
      }
    } else if (OB_FAIL(construct_store_ctx_other_variables_(*ls, tablet_id, scan_param.timeout_,
         ctx.mvcc_acc_ctx_.get_snapshot_version(), tablet_handle, ctx_guard))) {
      if (OB_SNAPSHOT_DISCARDED == ret && scan_param.fb_snapshot_.is_valid()) {
        ret = OB_TABLE_DEFINITION_CHANGED;
      } else {
        LOG_WARN("failed to check replica allow to read", K(ret), K(tablet_id), "timeout", scan_param.timeout_);
      }
    }
  }
  return ret;
}
}

namespace unittest
{

#define SSLOG_TABLE_READ_INIT                                           \
  sslog::ObSSLogIteratorGuard iter(true/*read_unfinish*/, true/*read_init_value*/, true/*read_mark_delete*/); \
  sslog::ObSSLogMetaValueIterator *raw_iter = dynamic_cast<sslog::ObSSLogMetaValueIterator *>(iter.get_sslog_iterator());

#define EXE_SQL(sql_str)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define EXE_SQL_FMT(...)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__));               \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));


void get_tablet_info_with_table_name(common::ObMySQLProxy &sql_proxy,
                                     const char *name,
                                     int64_t &table_id,
                                     int64_t &object_id,
                                     int64_t &tablet_id,
                                     int64_t &ls_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("SELECT table_id, object_id, tablet_id, ls_id FROM oceanbase.DBA_OB_TABLE_LOCATIONS WHERE TABLE_NAME= '%s';", name));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("table_id", table_id));
    ASSERT_EQ(OB_SUCCESS, result->get_int("object_id", object_id));
    ASSERT_EQ(OB_SUCCESS, result->get_int("tablet_id", tablet_id));
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", ls_id));
  }
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), create_test_env)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t tenant_id = 0;

  // ============================== Phase1. create tenant ==============================
  SERVER_LOG(INFO, "create_tenant start");
  ASSERT_EQ(OB_SUCCESS, create_tenant(DEFAULT_TEST_TENANT_NAME,
                                      "2G",
                                      "2G",
                                      false,
                                      "zone1"));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  SERVER_LOG(INFO, "create_tenant end", K(tenant_id));

  SERVER_LOG(INFO, "[ObMultiSSLog1] create test tenant success", K(tenant_id));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection_qc = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection_qc));
  ASSERT_NE(nullptr, connection_qc);

  // ============================== Phase2. create table ==============================
  TRANS_LOG(INFO, "create table qcc1 start");
  EXE_SQL("create table qcc1 (a int)");
  TRANS_LOG(INFO, "create_table qcc1 end");
  usleep(3 * 1000 * 1000);

  ObLSID loc1;
  ASSERT_EQ(0, SSH::select_table_loc(tenant_id, "qcc1", loc1));
  int64_t table1;
  int64_t object1;
  int64_t tablet1;
  int64_t ls1;
  get_tablet_info_with_table_name(sql_proxy, "qcc1", table1, object1, tablet1, ls1);
  fprintf(stdout, "ZONE1: qcc is created successfully, loc1: %ld, table1: %ld, tablet1: %ld, ls1: %ld\n",
          loc1.id(), table1, tablet1, ls1);

  // ============================== Phase5. start the user txn ==============================
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  ObSSLogProxyGuard guard;
  ASSERT_EQ(OB_SUCCESS, sslog::get_sslog_table_guard(ObSSLogTableType::SSLOG_TABLE,
                                                     tenant_id,
                                                     guard));
  sslog::ObISSLogProxy *sslog_table_proxy = guard.get_sslog_proxy();


  // ========== Example 1 ==========
  affected_rows = 0;
  const sslog::ObSSLogMetaType meta_type1 = sslog::ObSSLogMetaType::SSLOG_TABLET_META;
  // key format example: tenant_id;ls_id;tablet_id
  const common::ObString meta_key1 = "1002;1001;200001";
  const common::ObString meta_value1 = "qc_example1";
  const common::ObString meta_value1_new = "qc_example1_new";
  const common::ObString extra_info1= "qc_extra1";
  const common::ObString extra_info1_new = "qc_extra1_new";

  // ========== Example 2 ==========
  const sslog::ObSSLogMetaType meta_type2 = sslog::ObSSLogMetaType::SSLOG_TABLET_META;
  // key format example: tenant_id;ls_id;tablet_id
  const common::ObString meta_key2 = "1002;1001;200002";
  const common::ObString meta_value2 = "qc_example2";
  const common::ObString extra_info2= "qc_extra2";

  // ========== Universal ==========
  common::ObString meta_value_ret;
  common::ObString extra_info_ret;
  SCN row_scn;
  const common::ObString meta_key_prefix = "1002;1001;";

  // ========== Test 1: insert one row =========
  ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->insert_row(meta_type1,
                                                      meta_key1,
                                                      meta_value1,
                                                      extra_info1,
                                                      affected_rows));
  ASSERT_EQ(1, affected_rows);

  // ========== Test 2: insert another row =========
  ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->insert_row(meta_type2,
                                                      meta_key2,
                                                      meta_value2,
                                                      extra_info2,
                                                      affected_rows));
  ASSERT_EQ(1, affected_rows);

  ob_usleep(1_s);

  {
    sslog::ObSSLogReadParam param(true/*read_local*/, false/*read_row*/);
    SSLOG_TABLE_READ_INIT
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                      meta_type1,
                                                      meta_key1,
                                                      iter));
    ASSERT_EQ(OB_SUCCESS, raw_iter->get_next_meta_v2(row_scn, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, raw_iter->get_next_meta_v2(row_scn, meta_value_ret, extra_info_ret));
    ASSERT_EQ(qc_example, 1);
  }

  {
    sslog::ObSSLogReadParam param(true/*read_local*/, true/*read_row*/);
    SSLOG_TABLE_READ_INIT
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                      meta_type1,
                                                      meta_key_prefix,
                                                      iter));
    ASSERT_EQ(OB_SUCCESS, raw_iter->get_next_meta_v2(row_scn, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, raw_iter->get_next_meta_v2(row_scn, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value2, meta_value_ret);
    ASSERT_EQ(extra_info2, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, raw_iter->get_next_meta_v2(row_scn, meta_value_ret, extra_info_ret));
    ASSERT_EQ(qc_example, 2);
  }
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), create_test_env2)
{
  ObLSID loc1;
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 0;

  while (0 == tenant_id || OB_FAIL(SSH::select_table_loc(tenant_id, "qcc1", loc1))) {
    get_tenant_id(tenant_id, DEFAULT_TEST_TENANT_NAME);
    fprintf(stdout, "qcc is waiting %" PRIu64 ", %d\n", tenant_id, ret);
    ob_usleep(1_s);
  }

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  SERVER_LOG(INFO, "[ObMultiSSLog2] create test tenant success", K(tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  ASSERT_EQ(0, SSH::select_table_loc(tenant_id, "qcc1", loc1));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection_qc = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection_qc));
  ASSERT_NE(nullptr, connection_qc);
  int64_t table1;
  int64_t object1;
  int64_t tablet1;
  int64_t ls1;
  get_tablet_info_with_table_name(sql_proxy, "qcc1", table1, object1, tablet1, ls1);
  fprintf(stdout, "ZONE2: qcc is created successfully, loc1: %ld, table1: %ld, tablet1: %ld, ls1: %ld\n",
          loc1.id(), table1, tablet1, ls1);

    // ============================== Phase5. start the user txn ==============================

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
  const common::ObString meta_value1_new = "qc_example1_new";
  const common::ObString extra_info1= "qc_extra1";

  // ========== Example 2 ==========
  const sslog::ObSSLogMetaType meta_type2 = sslog::ObSSLogMetaType::SSLOG_TABLET_META;
  // key format example: tenant_id;ls_id;tablet_id
  const common::ObString meta_key2 = "1002;1001;200002";
  const common::ObString meta_value2 = "qc_example2";
  const common::ObString extra_info2= "qc_extra2";

  // ========== Universal ==========
  common::ObString meta_value_ret;
  common::ObString extra_info_ret;
  SCN row_scn;
  const common::ObString meta_key_prefix = "1002;1001;";


  ob_usleep(10_s);

  {
    sslog::ObSSLogReadParam param(true/*read_local*/, false/*read_row*/);
    SSLOG_TABLE_READ_INIT
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                      meta_type1,
                                                      meta_key1,
                                                      iter));
    ASSERT_EQ(OB_SUCCESS, raw_iter->get_next_meta_v2(row_scn, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, raw_iter->get_next_meta_v2(row_scn, meta_value_ret, extra_info_ret));
    ASSERT_EQ(qc_example, 1);
  }

  {
    sslog::ObSSLogReadParam param(true/*read_local*/, true/*read_row*/);
    SSLOG_TABLE_READ_INIT
    ASSERT_EQ(OB_SUCCESS, sslog_table_proxy->read_row(param,
                                                      meta_type1,
                                                      meta_key_prefix,
                                                      iter));
    ASSERT_EQ(OB_SUCCESS, raw_iter->get_next_meta_v2(row_scn, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value1, meta_value_ret);
    ASSERT_EQ(extra_info1, extra_info_ret);
    ASSERT_EQ(OB_SUCCESS, raw_iter->get_next_meta_v2(row_scn, meta_value_ret, extra_info_ret));
    ASSERT_EQ(meta_value2, meta_value_ret);
    ASSERT_EQ(extra_info2, extra_info_ret);
    ASSERT_EQ(OB_ITER_END, raw_iter->get_next_meta_v2(row_scn, meta_value_ret, extra_info_ret));
    ASSERT_EQ(qc_example, 2);
  }
}

} // namespace unittest
} // namespace oceanbase
