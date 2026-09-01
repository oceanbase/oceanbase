// owner: yeqiyi.yqy
// owner group: storage

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

#define USING_LOG_PREFIX STORAGE

#include <gmock/gmock.h>
#include <thread>
#include <mutex>
#include <atomic>
#include <vector>

#include "storage/test_tablet_helper.h"
#include "storage/test_dml_common.h"

#define protected public
#define private public

#include "storage/tablet/ob_sstable_truncate_filter.h"
#include "storage/tablet/ob_session_tablet_helper.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "storage/tablet/ob_session_tablet_info_map.h"
#include "storage/tablet/ob_tablet_to_global_temporary_table_operator.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/multi_data_source/ob_mds_table_merge_dag.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/tx/ob_ts_mgr.h"

using namespace oceanbase::unittest;

namespace oceanbase
{
namespace storage
{
using namespace share::schema;
using namespace common;
using namespace share;

static uint64_t g_tenant_id = OB_INVALID_ID;
static uint64_t g_gtt_table_id = OB_INVALID_ID;
static uint64_t g_gtt_table_id2 = OB_INVALID_ID;

static bool VERBOSE = true;
static FILE *VERBOSE_OUT = stdout;

static const char *tenant_name = "oracle_tenant";

#define ASSERT_SUCC(expr)                \
do {                                    \
    ASSERT_EQ(OB_SUCCESS, ret = (expr)); \
} while(0);                             \

#define EXPECT_SUCC(expr)               \
do {                                    \
    EXPECT_EQ(OB_SUCCESS, ret = (expr)); \
} while(0);                             \

#define LOG_AND_PRINT(level, fmt, args...) \
do {                                      \
  LOG_##level(fmt, args);                 \
  if (VERBOSE) {                          \
    fprintf(VERBOSE_OUT, fmt"\n");      \
  }                                       \
} while(0);                               \


std::string ob_str_to_std(const ObString &ob_str)
{
  return std::string(ob_str.ptr(), ob_str.length());
}

ObString std_str_to_ob(const std::string &std_str)
{
  return ObString(std_str.size(), std_str.data());
}

class TestTruncateOracleGTT : public unittest::ObSimpleClusterTestBase
{
public:
  struct DatumRow final
  {
  public:
    static void from_sql_result(common::sqlclient::ObMySQLResult &result, DatumRow &row)
    {
      int ret = OB_SUCCESS;
      int64_t id = 0;
      ObString name;
      ASSERT_SUCC(result.get_int("ID", id));
      ASSERT_SUCC(result.get_varchar("NAME", name));
      row.id_ = id;
      row.name_ = ob_str_to_std(name);
    }

    static void print(const vector<DatumRow> &rows)
    {
      if (rows.empty()) {
        fprintf(VERBOSE_OUT, "[EMPTY]\n");
      } else {
        fprintf(VERBOSE_OUT, "[\n");
        for (size_t i = 0; i < rows.size(); ++i) {
          fprintf(VERBOSE_OUT, " row[%lu]: %s,\n", i, rows[i].to_string().c_str());
        }
        fprintf(VERBOSE_OUT, "]\n");
      }
    }

  public:
    DatumRow()
      : id_(0),
        name_()
    {
    }
    DatumRow(const int64_t id, const string &name)
      : id_(id),
        name_(name)
    {
    }
    void reset()
    {
      id_ = 0;
      name_.clear();
    }
    std::string to_string() const
    {
      return string("{id:") + std::to_string(id_) + ", name:'" + name_ + string("'}");
    }
    bool operator==(const DatumRow &other) const
    {
      return id_ == other.id_ && name_ == other.name_;
    }

  public:
    int64_t id_;
    std::string name_;
  };

  struct DisableBackgroundSlogCkptGuard final
  {
  public:
    DisableBackgroundSlogCkptGuard(
      const uint64_t tenant_id)
      : tenant_guard_(),
        ret_(OB_SUCCESS)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(tenant_guard_.switch_to(tenant_id))) {
        LOG_AND_PRINT(WARN, "failed to switch tenant", K(ret), K(tenant_id));
      } else {
        bool &is_write_checkpoint = MTL(ObTenantStorageMetaService *)->ckpt_slog_handler_.is_writing_checkpoint_;
        while (!ATOMIC_BCAS(&is_write_checkpoint, false, true)) {
            ob_usleep(100 * 1000);
        }
        fprintf(VERBOSE_OUT, "succeed to disable background checkpoint.\n");
      }
      ret_ = ret;
    }
    ~DisableBackgroundSlogCkptGuard()
    {
      if (OB_SUCCESS == ret_) {
        bool &is_write_checkpoint = MTL(ObTenantStorageMetaService *)->ckpt_slog_handler_.is_writing_checkpoint_;
        ATOMIC_SET(&is_write_checkpoint, false);
        fprintf(VERBOSE_OUT, "succeed to enable background checkpoint.\n");
      }
    }
    int get_ret() const { return ret_; }

  public:
    ObTenantSwitchGuard tenant_guard_;
    int ret_;
  };

public:
  TestTruncateOracleGTT()
    : unittest::ObSimpleClusterTestBase("test_truncate_oracle_gtt"),
      tenant_id_(OB_INVALID_TENANT_ID)
  {
  }
  virtual ~TestTruncateOracleGTT() = default;

  int create_gtt_table(ObMySQLProxy &sql_proxy, const char *table_name, uint64_t &table_id, uint64_t tenant_id);
  int gen_new_schema_version(int64_t &schema_version);
  int get_session_infos(const uint64_t table_id, common::ObIArray<ObSessionTabletInfo> &infos);
  int get_table_id_from_db(const char *table_name, uint64_t &table_id, uint64_t tenant_id);
  int check_tablet_exists_in_tablet_to_ls(const ObTabletID &tablet_id, bool &exists, uint64_t tenant_id);
  int check_tablet_exists_in_gtt_operator(const ObTabletID &tablet_id, bool &exists, uint64_t tenant_id);
  void gen_sql_proxy(ObSingleMySQLConnectionPool &sql_conn_pool, ObMySQLProxy &sql_proxy);
  void clean_all_session_tablets();
  void select_from(ObMySQLProxy &sql_proxy, const char *table_name, vector<DatumRow> &rows);
  int insert_to(ObMySQLTransaction &trans, const char *table_name, const DatumRow &row);
  int insert_to(ObMySQLTransaction &trans, const char *table_name, const vector<DatumRow> &rows);
  int dump_tablet(const ObTabletMapKey &key);
  int trigger_minor_freeze();
  int trigger_mds_minor(const ObTabletMapKey &key);
  int trigger_tablet_medium_merge(
      const ObTabletMapKey &key,
      const share::SCN &expected_min_frozen_scn = share::SCN::min_scn(),
      const int64_t timeout_us = 600LL * 1000LL * 1000LL);
  void check_table_data(ObMySQLProxy &sql_proxy, const char *table_name, const vector<DatumRow> &expected_rows);
  int new_dummy_mds_sstable(const ObTabletMapKey &key);

public:
  uint64_t tenant_id_;
};

int TestTruncateOracleGTT::create_gtt_table(ObMySQLProxy &sql_proxy, const char *table_name, uint64_t &table_id, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  // Create global temporary table
  if (OB_FAIL(sql.assign_fmt(
    "CREATE GLOBAL TEMPORARY TABLE %s (id NUMBER PRIMARY KEY, name VARCHAR2(100)) ON COMMIT PRESERVE ROWS",
    table_name))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret));
  } else {
    // Get table id
    sleep(2); // wait for schema refresh
    if (OB_FAIL(get_table_id_from_db(table_name, table_id, tenant_id))) {
      LOG_WARN("failed to get table id", K(ret));
    }
  }
  return ret;
}

int TestTruncateOracleGTT::gen_new_schema_version(int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret), K_(tenant_id));
  } else if (OB_FAIL(GCTX.schema_service_->gen_new_schema_version(tenant_id_, schema_version))) {
    LOG_WARN("failed to generate schema version", K(ret), K_(tenant_id));
  }
  return ret;
}

int TestTruncateOracleGTT::get_session_infos(const uint64_t table_id, common::ObIArray<ObSessionTabletInfo> &infos)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTableID, 1> table_ids;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_AND_PRINT(WARN, "unexpected null sql_proxy", K(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(table_ids.push_back(table_id))) {
    LOG_AND_PRINT(WARN, "failed to push back table id", K(ret));
  } else if (OB_FAIL(ObTabletToGlobalTmpTableOperator::batch_get_by_table_ids(*GCTX.sql_proxy_, tenant_id_, table_ids, infos))) {
    LOG_AND_PRINT(WARN, "failed to get session tablet infos", K(ret), K(table_id));
  }
  return ret;
}

int TestTruncateOracleGTT::get_table_id_from_db(const char *table_name, uint64_t &table_id, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  table_id = OB_INVALID_ID;

  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sql proxy", K(ret), KP(GCTX.sql_proxy_));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(sql.assign_fmt(
        "SELECT table_id FROM oceanbase.__all_virtual_table WHERE table_name = '%s' AND tenant_id = %lu",
        table_name, tenant_id))) {
        LOG_WARN("failed to assign sql", K(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(result, sql.ptr()))) {
        LOG_WARN("failed to read sql", K(ret));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else {
        sqlclient::ObMySQLResult &res = *result.get_result();
        if (OB_FAIL(res.next())) {
          LOG_WARN("failed to get next result", K(ret));
        } else {
          EXTRACT_INT_FIELD_MYSQL(res, "table_id", table_id, uint64_t);
        }
      }
    }
  }
  return ret;
}

int TestTruncateOracleGTT::check_tablet_exists_in_tablet_to_ls(const ObTabletID &tablet_id, bool &exists, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  exists = false;
  ObSqlString sql;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sql proxy", K(ret), KP(GCTX.sql_proxy_));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(sql.assign_fmt(
        "SELECT tablet_id FROM oceanbase.__all_virtual_tablet_to_ls WHERE tablet_id = %lu AND tenant_id = %lu",
        tablet_id.id(), tenant_id))) {
        LOG_WARN("failed to assign sql", K(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(result, sql.ptr()))) {
        LOG_WARN("failed to read sql", K(ret));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else {
        sqlclient::ObMySQLResult &res = *result.get_result();
        if (OB_SUCCESS == res.next()) {
          exists = true;
        }
      }
    }
  }
  return ret;
}

int TestTruncateOracleGTT::check_tablet_exists_in_gtt_operator(const ObTabletID &tablet_id, bool &exists, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  exists = false;
  ObSqlString sql;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();

  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    if (OB_FAIL(sql.assign_fmt(
      "SELECT tablet_id FROM oceanbase.__all_virtual_tablet_to_global_temporary_table WHERE tablet_id = %lu AND tenant_id = %lu",
      tablet_id.id(), tenant_id))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(inner_sql_proxy.read(result, sql.ptr()))) {
      LOG_WARN("failed to read sql", K(ret));
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else {
      sqlclient::ObMySQLResult &res = *result.get_result();
      if (OB_SUCCESS == res.next()) {
        exists = true;
      }
    }
  }
  return ret;
}

void TestTruncateOracleGTT::gen_sql_proxy(
    ObSingleMySQLConnectionPool &sql_conn_pool,
    ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  std::string db_user = "sys@" + std::string(tenant_name);
  sql_conn_pool.set_db_param(db_user.c_str(), "", "SYS");
  ObConnPoolConfigParam param;
  param.sqlclient_wait_timeout_ = 1000;
  param.long_query_timeout_ = 300*1000*1000;
  param.connection_refresh_interval_ = 200*1000;
  param.connection_pool_warn_time_ = 10*1000*1000;
  param.sqlclient_per_observer_conn_limit_ = 1000;
  common::ObAddr db_addr;
  db_addr.set_ip_addr(get_curr_simple_server().get_local_ip().c_str(), get_curr_simple_server().get_mysql_port());
  ASSERT_SUCC(sql_conn_pool.init(db_addr, param));
  sql_conn_pool.set_mode(common::sqlclient::ObMySQLConnection::DEBUG_MODE);
  ASSERT_SUCC(sql_proxy.init(&sql_conn_pool));
}

void TestTruncateOracleGTT::clean_all_session_tablets()
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(is_valid_tenant_id(tenant_id_));
  ObTenantSwitchGuard tenant_guard;
  ASSERT_SUCC(tenant_guard.switch_to(tenant_id_));
  ASSERT_NE(nullptr, GCTX.sql_proxy_);
  common::ObSArray<ObLSID> ls_ids;
  ASSERT_NE(nullptr, MTL(ObLSService *));
  ASSERT_SUCC(MTL(ObLSService *)->get_ls_ids(ls_ids));

  if (ls_ids.empty()) {
    LOG_AND_PRINT(INFO, "no LS", K(ret), K(ls_ids));
  } else {
    common::ObSArray<ObSessionTabletInfo> session_tablet_infos;
    while (true) {
      session_tablet_infos.reset();
      ASSERT_SUCC(share::ObTabletToGlobalTmpTableOperator::batch_get_by_ls_ids(*GCTX.sql_proxy_, tenant_id_, ls_ids, session_tablet_infos));
      if (session_tablet_infos.empty()) {
        LOG_AND_PRINT(INFO, "no session tablets", K(ret), K(session_tablet_infos));
        break;
      } else {
        int tmp_ret = OB_SUCCESS;
        ObSessionTabletGCHelper helper(tenant_id_);
        if (OB_TMP_FAIL(helper.do_work())) {
          LOG_AND_PRINT(INFO, "failed to gc", K(tmp_ret));
          usleep(10_ms);
        }
      }
    }
  }
}

void TestTruncateOracleGTT::select_from(ObMySQLProxy &sql_proxy, const char *table_name, vector<DatumRow> &rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    if (OB_FAIL(sql.assign_fmt(
      "SELECT id, name FROM %s", table_name))) {
      LOG_AND_PRINT(WARN, "failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_proxy.read(result, sql.ptr()))) {
      LOG_AND_PRINT(WARN, "failed to read sql", K(ret));
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_AND_PRINT(WARN, "result is null", K(ret));
    } else {
      common::sqlclient::ObMySQLResult &res = *result.get_result();
      while (OB_SUCC(ret) && OB_SUCC(res.next())) {
        DatumRow row;
        DatumRow::from_sql_result(res, row);
        rows.push_back(row);
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  ASSERT_SUCC(ret);
}

int TestTruncateOracleGTT::insert_to(ObMySQLTransaction &trans, const char *table_name, const DatumRow &row)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(!trans.is_started())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_AND_PRINT(WARN, "trans not started", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s(id, name) VALUES(%ld, '%s')", table_name, row.id_, row.name_.c_str()))) {
    LOG_AND_PRINT(WARN, "failed to assign sql", K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(trans.write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
      LOG_AND_PRINT(WARN, "execute sql failed", K(ret), K(sql));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_AND_PRINT(WARN, "unexpected affected_rows", K(ret), K(sql), K(affected_rows));
    }
  }
  return ret;
}

int TestTruncateOracleGTT::insert_to(ObMySQLTransaction &trans, const char *table_name, const vector<DatumRow> &rows)
{
  int ret=  OB_SUCCESS;
  for (const DatumRow &row : rows) {
    if (OB_FAIL(insert_to(trans, table_name, row))) {
      LOG_AND_PRINT(WARN, "failed to insert row", K(ret));
      break;
    }
  }
  return ret;
}

static int get_mds_compaction_record_cnt(const uint64_t tenant_id, int64_t &cnt)
{
  int ret = OB_SUCCESS;
  cnt = 0;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(sql.assign_fmt(
          "SELECT count(*) AS cnt FROM oceanbase.__all_virtual_tablet_compaction_history"
          " WHERE tenant_id = %lu AND type like 'MDS%'",
          tenant_id))) {
      LOG_AND_PRINT(WARN, "failed to assign sql", K(ret));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
      LOG_AND_PRINT(WARN, "failed to read baseline mini merge count", K(ret), K(sql));
    } else if (OB_ISNULL(res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(res.get_result()->next())) {
      LOG_AND_PRINT(WARN, "failed to get next row", K(ret));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*res.get_result(), "cnt", cnt, int64_t);
    }
  }
  return ret;
};

int TestTruncateOracleGTT::dump_tablet(const ObTabletMapKey &key)
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_ts = ObTimeUtility::current_time() + 30_s;
  ObTenantFreezer *freezer = MTL(ObTenantFreezer *);
  ObTabletHandle tablet_hdl;
  ObTablet *tablet = nullptr;
  ObProtectedMemtableMgrHandle *memtable_mgr_handle = nullptr;
  share::SCN expected_clog_ckpt_scn;
  if (OB_ISNULL(freezer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_AND_PRINT(WARN, "tenant freezer is null", K(ret));
  } else if (OB_FAIL(freezer->tablet_freeze(
                 key.ls_id_,
                 key.tablet_id_,
                 true,                            // is_sync
                 abs_timeout_ts,
                 false,                           // need_rewrite_tablet_meta
                 ObFreezeSourceFlag::TEST_MODE))) {
    LOG_AND_PRINT(WARN, "failed to freeze tablet", K(ret), K(key), K(key));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_hdl))) {
    LOG_AND_PRINT(WARN, "failed to get tablet", K(ret), K(key));
  } else if (OB_ISNULL(tablet = tablet_hdl.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_AND_PRINT(WARN, "unexpected null tablet", K(ret), K(key), K(tablet_hdl));
  } else if (OB_FAIL(tablet->get_protected_memtable_mgr_handle(memtable_mgr_handle))) {
    LOG_AND_PRINT(WARN, "failed to get memtable mgr handle", K(ret));
  } else {
    ObTableHandleV2 table_handle;
    ObITabletMemtable *memtable = nullptr;
    if (OB_FAIL(memtable_mgr_handle->get_last_frozen_memtable(table_handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_AND_PRINT(WARN, "failed to get last frozen memtable", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(table_handle.get_tablet_memtable(memtable))) {
      LOG_AND_PRINT(WARN, "failed to get tablet memtable", K(ret), K(table_handle));
    } else {
      expected_clog_ckpt_scn = memtable->get_end_scn();
      if (VERBOSE) {
        fprintf(VERBOSE_OUT, "expected clog checkpoint scn: %s\n", ObCStringHelper().convert(expected_clog_ckpt_scn));
      }
    }
  }
  if(OB_SUCC(ret)) {
    // Step 2: submit MINI_MERGE dag
    compaction::ObTabletMergeDagParam param;
    param.ls_id_       = key.ls_id_;
    param.tablet_id_   = key.tablet_id_;
    param.merge_type_  = compaction::MINI_MERGE;
    param.merge_version_ = ObVersion::MIN_VERSION;
    if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_tablet_merge_dag(param))) {
      LOG_AND_PRINT(WARN, "failed to schedule mini merge dag", K(ret), K(param));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (expected_clog_ckpt_scn.is_valid()) {
    const int64_t abs_timeout_ts = ObClockGenerator::getClock() + 20LL * 1000LL * 1000LL;
    do {
      if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_hdl))) {
        LOG_AND_PRINT(WARN, "failed to get tablet", K(ret), K(key));
      } else if (OB_ISNULL(tablet = tablet_hdl.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_AND_PRINT(WARN, "unexpected null tablet", K(ret), K(key), K(tablet_hdl));
      } else if (tablet->get_clog_checkpoint_scn() >= expected_clog_ckpt_scn) {
        break;
      }

      if (OB_FAIL(ret)) {
      } else if (ObClockGenerator::getClock() > abs_timeout_ts) {
        ret = OB_TIMEOUT;
        LOG_AND_PRINT(WARN, "dump tablet timeout", K(ret), K(key), K(expected_clog_ckpt_scn),
          K(tablet->get_clog_checkpoint_scn()));
      }
    } while (OB_SUCC(ret));
  }
  return ret;
}

int TestTruncateOracleGTT::trigger_minor_freeze()
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_AND_PRINT(WARN, "unexpected null sql_proxy", K(ret), KP(GCTX.sql_proxy_));
  } else {
    LOG_AND_PRINT(INFO, "trigger tenant minor freeze", K(ret), K(tenant_id_));
    int64_t cnt_before = 0;
    if (OB_FAIL(get_mds_compaction_record_cnt(tenant_id_, cnt_before))) {
      LOG_AND_PRINT(WARN, "failed to get compaction record cnt", K(ret), K(tenant_id_));
    }
    MTL_SWITCH(tenant_id_) {
      int64_t affected_rows = -1;
      if (OB_FAIL(sql.assign_fmt("ALTER SYSTEM MINOR FREEZE TENANT = %s", tenant_name))) {
        LOG_AND_PRINT(WARN, "failed to assign sql", K(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
        LOG_AND_PRINT(WARN, "failed to write sql", K(ret), K(sql));
      }
    } else {
      LOG_AND_PRINT(WARN, "failed to switch tenant", K(ret), K(tenant_id_));
    }
    LOG_AND_PRINT(INFO, "waiting for minor freeze to finish", K(ret), K(cnt_before));
    const int64_t MAX_RETRY = 600;
    int64_t cnt_after = 0;
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < MAX_RETRY; ++i) {
      usleep(10_ms); // 10ms
      cnt_after = 0;
      if (OB_FAIL(get_mds_compaction_record_cnt(tenant_id_, cnt_after))) {
        LOG_AND_PRINT(WARN, "failed to get compaction record cnt", K(ret), K(tenant_id_));
      } else if (cnt_after > cnt_before) {
        LOG_AND_PRINT(INFO, "minor freeze finished", K(ret), K(cnt_after), K(cnt_before));
        break;
      }
    }
    if (MAX_RETRY == i && cnt_after <= cnt_before) {
      ret = OB_TIMEOUT;
      LOG_AND_PRINT(WARN, "failed to wait minor freeze finished", K(ret), K(i), K(cnt_after), K(cnt_before));
    }
  }
  return ret;
}

int TestTruncateOracleGTT::trigger_mds_minor(const ObTabletMapKey &key)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  const ObLSID ls_id = key.ls_id_;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_AND_PRINT(WARN, "invalid tablet key", K(ret), K(key));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_AND_PRINT(WARN, "failed to get ls", K(ret));
  } else {
    int times = 0;
    do
    {
      LOG_AND_PRINT(INFO, "try mds minor", K(times));
      if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
        LOG_AND_PRINT(WARN, "failed to get ls", K(ret), K(key));
      } else if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
        LOG_AND_PRINT(WARN, "failed to tablet handle", K(ret), K(key));
      // for pass schedule varify
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_AND_PRINT(WARN, "ls is null", K(ret), KP(ls));
      } else if (OB_FAIL(compaction::ObTenantTabletScheduler::schedule_tablet_minor_merge<mds::ObTabletMdsMinorMergeDag>(
        compaction::MDS_MINOR_MERGE, ls_handle, tablet_handle))) {
        LOG_AND_PRINT(WARN, "fail to schedule mds minor merge", K(ret));
      }
      // sleep
      ::ob_usleep(100_ms);
      ++times;
    } while (OB_EAGAIN == ret && times < 20);
  }
  return ret;
}

int TestTruncateOracleGTT::trigger_tablet_medium_merge(
    const ObTabletMapKey &key,
    const share::SCN &expected_min_frozen_scn,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  const bool has_expected = expected_min_frozen_scn.is_valid_and_not_min();
  const int64_t expected_val = has_expected ? expected_min_frozen_scn.get_val_for_tx() : 0;
  const int64_t abs_timeout_ts = ObTimeUtility::current_time() + timeout_us;
  int64_t old_snapshot = 0;

  // Step 1: read current major snapshot version of this tablet.
  {
    ObTabletHandle tablet_hdl;
    if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_hdl))) {
      LOG_AND_PRINT(WARN, "failed to get tablet", K(ret), K(key));
    } else {
      old_snapshot = tablet_hdl.get_obj()->get_last_major_snapshot_version();
    }
  }

  // Step 2: wait LS weak_read_ts to pass expected_val so that medium merge
  // picks a snapshot > truncate_commit_version (choose_scn_for_user_request
  // uses MAX(max_reserved_snapshot, weak_read_ts_)).
  if (OB_FAIL(ret)) {
  } else if (has_expected) {
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService *)->get_ls(key.ls_id_, ls_handle, ObLSGetMod::COMPACT_MODE))) {
      LOG_AND_PRINT(WARN, "failed to get ls", K(ret), K(key));
    } else {
      while (OB_SUCC(ret)) {
        const share::SCN wrs = ls_handle.get_ls()->get_ls_wrs_handler()->get_ls_weak_read_ts();
        if (wrs.is_valid_and_not_min() && wrs.get_val_for_tx() > expected_val) {
          LOG_AND_PRINT(INFO, "ls weak_read_ts passed expected_val", K(ret), K(key),
            K(expected_val), "weak_read_ts", wrs.get_val_for_tx());
          break;
        } else if (ObTimeUtility::current_time() > abs_timeout_ts) {
          ret = OB_TIMEOUT;
          LOG_AND_PRINT(WARN, "wait ls weak_read_ts pass expected_val timeout", K(ret), K(key),
            K(expected_val), "weak_read_ts", wrs.get_val_for_tx());
          break;
        }
        ::usleep(50 * 1000); // 50ms
      }
    }
  }

  // Step 3: request medium merge for this tablet.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(MTL(compaction::ObTenantTabletScheduler *)->user_request_schedule_medium_merge(
          key.ls_id_, key.tablet_id_, false/*is_rebuild_column_group*/))) {
    LOG_AND_PRINT(WARN, "failed to schedule medium merge", K(ret), K(key));
  } else {
    LOG_AND_PRINT(INFO, "triggered tablet medium merge", K(ret), K(key),
      K(old_snapshot), K(has_expected), K(expected_val));
  }

  // Step 4: wait until the tablet's last_major_snapshot_version advances past old_snapshot
  // and (if expected) past expected_val.
  while (OB_SUCC(ret)) {
    int64_t new_snapshot = 0;
    ObTabletHandle tablet_hdl;
    if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_hdl))) {
      LOG_AND_PRINT(WARN, "failed to get tablet", K(ret), K(key));
    } else {
      new_snapshot = tablet_hdl.get_obj()->get_last_major_snapshot_version();
    }
    if (OB_FAIL(ret)) {
      break;
    } else if (new_snapshot > old_snapshot
               && (!has_expected || new_snapshot > expected_val)) {
      LOG_AND_PRINT(INFO, "tablet medium merge finished", K(ret), K(key),
        K(old_snapshot), K(new_snapshot), K(expected_val));
      break;
    } else if (ObTimeUtility::current_time() > abs_timeout_ts) {
      ret = OB_TIMEOUT;
      LOG_AND_PRINT(WARN, "wait tablet medium merge timeout", K(ret), K(key),
        K(old_snapshot), K(new_snapshot), K(expected_val));
      break;
    } else if (REACH_TIME_INTERVAL(10_s)) {
      LOG_AND_PRINT(INFO, "waiting tablet medium merge finish...", K(ret), K(key),
        K(old_snapshot), K(new_snapshot), K(expected_val));
    }
    ::usleep(500 * 1000); // 500ms
  }

  return ret;
}

void TestTruncateOracleGTT::check_table_data(
     ObMySQLProxy &sql_proxy,
     const char *table_name,
     const vector<DatumRow> &expected_rows)
{
  vector<DatumRow> select_rows;
  select_from(sql_proxy, table_name, select_rows);
  DatumRow::print(select_rows);
  ASSERT_EQ(expected_rows.size(), select_rows.size());
  for (size_t i = 0; i < expected_rows.size(); ++i) {
    ASSERT_EQ(expected_rows[i], select_rows[i]);
  }
}

static int get_mds_sstable_cnt(const ObTabletMapKey &key, int64_t &cnt)
{
  int ret = OB_SUCCESS;
  cnt = 0;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
    LOG_AND_PRINT(WARN, "failed to get tablet", K(ret), K(key));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_AND_PRINT(WARN, "unexpected null tablet", K(ret), K(key), K(tablet_handle));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_AND_PRINT(WARN, "failed to fetch table store", K(ret), K(key), K(tablet_handle));
  } else if (OB_ISNULL(table_store_wrapper.get_member())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_AND_PRINT(WARN, "unexpected null table store", K(ret), K(key), K(table_store_wrapper));
  } else {
    cnt = table_store_wrapper.get_member()->mds_sstables_.count();
  }
  return ret;
}

static int wait_for_mds_sstable_cnt(
    const ObTabletMapKey &key,
    const int64_t expected_cnt,
    int64_t &actual_cnt,
    const int64_t timeout_us = 10_s)
{
  int ret = OB_SUCCESS;
  const int64_t deadline = ObTimeUtility::current_time() + timeout_us;
  bool finish = false;
  while (OB_SUCC(ret) && !finish) {
    if (OB_FAIL(get_mds_sstable_cnt(key, actual_cnt))) {
      LOG_AND_PRINT(WARN, "failed to get mds sstable cnt", K(ret), K(key));
    } else if (expected_cnt == actual_cnt) {
      finish = true;
    } else if (ObTimeUtility::current_time() >= deadline) {
      ret = OB_TIMEOUT;
      LOG_AND_PRINT(WARN, "wait mds sstable cnt timeout", K(ret), K(key),
        K(expected_cnt), K(actual_cnt));
    } else {
      ob_usleep(100_ms);
    }
  }
  return ret;
}

static int wait_for_mds_minor_finish(
    const ObTabletMapKey &key,
    const int64_t timeout_us = 60_s)
{
  int ret = OB_SUCCESS;
  const int64_t deadline = ObTimeUtility::current_time() + timeout_us;
  bool finish = false;
  while (OB_SUCC(ret) && !finish) {
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
    if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
      LOG_AND_PRINT(WARN, "failed to get tablet", K(ret), K(key));
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_AND_PRINT(WARN, "unexpected null tablet", K(ret), K(key), K(tablet_handle));
    } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
      LOG_AND_PRINT(WARN, "failed to fetch table store", K(ret), K(key), K(tablet_handle));
    } else if (OB_ISNULL(table_store_wrapper.get_member())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_AND_PRINT(WARN, "unexpected null table store", K(ret), K(key), K(table_store_wrapper));
    } else {
      const ObTabletTableStore *table_store = table_store_wrapper.get_member();
      finish = 1 == table_store->mds_sstables_.count()
          && table_store->mds_sstables_.at(0)->is_mds_minor_sstable();
      if (!finish && ObTimeUtility::current_time() >= deadline) {
        ret = OB_TIMEOUT;
        LOG_AND_PRINT(WARN, "wait mds minor merge timeout", K(ret), K(key),
          "mds_sstable_cnt", table_store->mds_sstables_.count());
      } else if (!finish) {
        ob_usleep(100_ms);
      }
    }
  }
  return ret;
}

static int wait_for_mds_table_flush(
    const ObTabletMapKey &key,
    int64_t &mds_sstable_cnt)
{
  int ret = OB_SUCCESS;
  share::SCN rec_scn = share::SCN::min_scn();
  int64_t abs_timeout_ts = ObTimeUtility::current_time() + 10_s; // 10s
  do {
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    mds::MdsTableHandle mds_table_hdl;
    rec_scn.set_min();
    if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
      LOG_AND_PRINT(WARN, "failed to get tablet", K(ret), K(key));
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_AND_PRINT(WARN, "unexpected null tablet", K(ret), K(key), K(tablet_handle));
    } else if (OB_FAIL(tablet->inner_get_mds_table(mds_table_hdl, false/*not_exist_create*/))) {
      LOG_AND_PRINT(WARN, "failed to get mds table", K(ret), K(key));
    } else if (OB_FAIL(mds_table_hdl.get_rec_scn(rec_scn))) {
      LOG_AND_PRINT(WARN, "failed to get mds table rec scn", K(ret), K(key), K(mds_table_hdl));
    } else if (rec_scn.is_max()) {
      ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
      if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
        LOG_AND_PRINT(WARN, "failed to fetch table store", K(ret), K(key), K(tablet_handle));
      } else if (OB_ISNULL(table_store_wrapper.get_member())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_AND_PRINT(WARN, "unexpected null table store", K(ret), K(key), K(table_store_wrapper));
      } else {
        mds_sstable_cnt = table_store_wrapper.get_member()->mds_sstables_.count();
        LOG_AND_PRINT(INFO, "mds table flush finished", K(ret), K(key), K(mds_table_hdl),
          K(mds_sstable_cnt));
      }
      break;
    } else if (ObTimeUtility::current_time() > abs_timeout_ts) {
      ret = OB_TIMEOUT;
      LOG_AND_PRINT(WARN, "wait mds table flush timeout", K(ret), K(key));
    } else {
      ob_usleep(100_us);
    }
  } while(OB_SUCC(ret));
  return ret;
}

int TestTruncateOracleGTT::new_dummy_mds_sstable(const ObTabletMapKey &key)
{
  int ret = OB_SUCCESS;

  int64_t mds_cnt_before = 0;
  if (OB_FAIL(get_mds_sstable_cnt(key, mds_cnt_before))) {
    LOG_AND_PRINT(WARN, "failed to get mds sstable cnt", K(ret), K(key));
  } else {
    ObTabletBindingMdsUserData binding_data;
    binding_data.set_default_value();
    mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(123 + mds_cnt_before + 1)));
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    ObLSService *ls_svr = nullptr;
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    share::SCN max_decided_scn;

    if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
      LOG_AND_PRINT(WARN, "failed to get tablet", K(ret), K(key));
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_AND_PRINT(WARN, "unexpected null tablet", K(ret), K(key), K(tablet_handle));
    } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_AND_PRINT(WARN, "unexpected null ls svr", K(ret), KP(ls_svr));
    } else if (OB_FAIL(ls_svr->get_ls(key.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_AND_PRINT(WARN, "failed to get ls", K(ret), K(key));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_AND_PRINT(WARN, "unexpected null ls", K(ret), K(key), K(ls_handle));
    } else if (OB_FAIL(ls->get_max_decided_scn(max_decided_scn))) {
      LOG_AND_PRINT(WARN, "failed to get max decided scn", K(ret), K(key), KPC(ls));
    } else if (OB_FAIL(tablet->set(binding_data, ctx))) {
      LOG_AND_PRINT(WARN, "failed to set tablet mds data", K(ret), K(key), K(binding_data));
    } else if (FALSE_IT(ctx.single_log_commit(max_decided_scn, max_decided_scn))) {
    } else if (OB_FAIL(tablet->mds_table_flush(share::SCN::plus(max_decided_scn, 10)))) {
      LOG_AND_PRINT(WARN, "failed to flush mds table", K(ret), K(key), K(max_decided_scn));
    }
  }
  int64_t mds_cnt_after = 0;
  if (FAILEDx(wait_for_mds_sstable_cnt(key, mds_cnt_before + 1, mds_cnt_after))) {
    LOG_AND_PRINT(WARN, "failed to wait mds sstable", K(ret), K(key));
  } else if (OB_UNLIKELY(mds_cnt_after != mds_cnt_before + 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_AND_PRINT(WARN, "unexpected mds sstable cnt after mds flush", K(ret),
      K(mds_cnt_before), K(mds_cnt_after));
  }
  return ret;
}

#define BEGIN_TEST_CASE \
  int ret = OB_SUCCESS;\
  ObTenantSwitchGuard tenant_guard;\
  ASSERT_SUCC(get_tenant_id(tenant_id_, tenant_name));\
  ASSERT_SUCC(tenant_guard.switch_to(tenant_id_));\
  ObTenantMetaMemMgr *t3m = nullptr;\
  ObLSService *ls_svr = nullptr;\
  ObTenantStorageMetaService *tsms = nullptr;\
  ASSERT_NE(nullptr, t3m = MTL(ObTenantMetaMemMgr *));\
  ASSERT_NE(nullptr, ls_svr = MTL(ObLSService *));\
  ASSERT_NE(nullptr, tsms = MTL(ObTenantStorageMetaService *));\

#define IN_TRANS_SCOPE(sql_proxy, code) \
  {\
    int ret = OB_SUCCESS;\
    ObMySQLTransaction trans;\
    const int64_t start_time = ObTimeUtility::current_time();\
    if (OB_ISNULL(sql_proxy)) {\
      ret = OB_ERR_UNEXPECTED;\
      LOG_AND_PRINT(WARN, "unepected null sql_proxy", K(ret), KP(sql_proxy));\
    } else if (OB_FAIL(trans.start(sql_proxy, tenant_id_))){\
      LOG_AND_PRINT(WARN, "failed to start trans", K(ret));\
    } else {\
      code\
    }\
    if (!trans.is_started()) {\
    } else if (OB_FAIL(trans.end(OB_SUCC(ret)))) {\
      LOG_AND_PRINT(WARN, "failed to end trans", K(ret));\
    } else if (VERBOSE_OUT) {\
      fprintf(VERBOSE_OUT, "trans timecost: %ldus\n", ObTimeUtility::current_time() - start_time);\
    }\
  }\

static int check_tablet_mds_after_truncate(
    ObTablet &tablet,
    ObTabletCreateDeleteMdsUserData *tablet_status = nullptr,
    ObTabletTruncateMdsUserData *truncate_data = nullptr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!tablet.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_AND_PRINT(WARN, "unexpected invalid tablet", K(ret), K(tablet));
  }
  // check tablet status
  if (OB_SUCC(ret)) {
    ObArenaAllocator tmp_allocator;
    ObTabletCreateDeleteMdsUserData data;

    if (tablet.tablet_meta_.last_persisted_committed_tablet_status_.is_valid()) {
      LOG_AND_PRINT(INFO, "read from sstable", K(ret), K(tablet.tablet_meta_.last_persisted_committed_tablet_status_));
      // bypass cache
      if (OB_FAIL((tablet.read_data_from_mds_sstable<mds::DummyKey, ObTabletCreateDeleteMdsUserData>(
                                                    tmp_allocator,
                                                    mds::DummyKey(),
                                                    tablet.tablet_meta_.mds_checkpoint_scn_,
                                                    ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
                                                    ReadTabletStatusOp(data))))) {
        LOG_AND_PRINT(WARN, "failed to get tablet status from mds sstable", K(ret));
      }
    } else if (OB_FAIL(tablet.get_latest_committed_tablet_status(data))) {
      LOG_AND_PRINT(WARN, "failed to get latest commited tablet status", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!data.is_valid()
                           || !data.create_commit_scn_.is_valid()
                           || OB_INVALID_VERSION == data.create_commit_version_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_AND_PRINT(WARN, "unexpected invalid tablet status", K(ret), K(data));
    } else if (VERBOSE) {
      fprintf(VERBOSE_OUT, "check tablet status:%s\n", ObCStringHelper().convert(data));
    }
    if (nullptr != tablet_status) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(tablet_status->assign(data))) {
        LOG_AND_PRINT(WARN, "failed to assign tablet status", K(tmp_ret), K(data));
      }
    }
  }
  // check truncate info
  if (OB_SUCC(ret)) {
    ObTabletTruncateMdsUserData data;
    if (OB_FAIL(tablet.get_truncate_mds_data(data))) {
      LOG_AND_PRINT(WARN, "failed to get truncate mds data", K(ret));
    } else if (OB_UNLIKELY(!data.is_valid()
                           || !data.truncate_commit_scn_.is_valid()
                           || OB_INVALID_VERSION == data.truncate_commit_version_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_AND_PRINT(WARN, "unexpected invalid truncate mds data", K(ret), K(data));
    } else if (VERBOSE) {
      fprintf(VERBOSE_OUT, "check truncate mds data:%s\n", ObCStringHelper().convert(data));
    }
    if (nullptr != truncate_data) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(truncate_data->assign(data))) {
        LOG_AND_PRINT(WARN, "failed to assign truncate data", K(tmp_ret), K(data));
      }
    }
  }
  return ret;
}

static void print_all_sstables(const ObTabletMapKey &key)
{
  if (VERBOSE) {
    int ret = OB_SUCCESS;
    ObTabletHandle tablet_hdl;
    ObTablet *tablet = nullptr;
    ASSERT_SUCC(ObTabletCreateDeleteHelper::get_tablet(key, tablet_hdl));
    ASSERT_NE(nullptr, tablet = tablet_hdl.get_obj());

    ObTableStoreIterator iter;
    ASSERT_SUCC(tablet->get_all_sstables(iter));
    int64_t ith = 0;
    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      ObSSTable *sstable = nullptr;
      if (OB_FAIL(iter.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_AND_PRINT(WARN, "failed to get next sstable", K(ret));
        } else {
          ret = OB_SUCCESS;
          fprintf(VERBOSE_OUT, "\n");
          break;
        }
      } else if (OB_ISNULL(sstable = static_cast<ObSSTable *>(table))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_AND_PRINT(WARN, "unexpected null sstable", K(ret), KPC(table));
      } else {
        if (ith > 0) {
          fprintf(VERBOSE_OUT, ",\n");
        }
        fprintf(VERBOSE_OUT, "sstable[%ld]: %s", ith, ObCStringHelper().convert(sstable->get_key()));
        ++ith;
      }
    }
    ASSERT_SUCC(ret);
    if (0 == ith) {
      fprintf(VERBOSE_OUT, "[EMPTY TABLE STORE]\n");
    }
  }
}

TEST_F(TestTruncateOracleGTT, create_tenant)
{
  int ret = OB_SUCCESS;
  ASSERT_SUCC(create_tenant_with_retry(tenant_name, "4G", "4G", true /*oracle_mode*/));
  ASSERT_SUCC(get_tenant_id(tenant_id_, tenant_name));
  ASSERT_TRUE(is_valid_tenant_id(tenant_id_));
}

TEST_F(TestTruncateOracleGTT, test_gtt_truncate_basic)
{
  BEGIN_TEST_CASE
  DisableBackgroundSlogCkptGuard slog_ckpt_guard(tenant_id_);
  ASSERT_SUCC(slog_ckpt_guard.get_ret());

  {
    const char *table_name = "test0";
    uint64_t table_id = 0;
    ObSingleMySQLConnectionPool conn_pool;
    ObMySQLProxy sql_proxy;
    gen_sql_proxy(conn_pool, sql_proxy);
    ASSERT_SUCC(create_gtt_table(sql_proxy, table_name, table_id, tenant_id_));

    vector<DatumRow> rows{{1, "name1"}, {2, "name2"}, {3, "name3"}};

    IN_TRANS_SCOPE(&sql_proxy, {
      if (OB_FAIL(insert_to(trans, table_name, rows))) {
        LOG_AND_PRINT(WARN, "failed to insert rows", K(ret));
      }
    })
    ASSERT_SUCC(ret);
    // dump in-memory data
    ASSERT_SUCC(trigger_minor_freeze());
    // insert rows to memtable
    vector<DatumRow> tmp_rows{{4, "name4"}, {5, "name5"}, {6, "name6"}};
    IN_TRANS_SCOPE(&sql_proxy, {
      if (OB_FAIL(insert_to(trans, table_name, tmp_rows))) {
        LOG_AND_PRINT(WARN, "failed to insert rows", K(ret));
      }
    })
    ASSERT_SUCC(ret);
    rows.insert(rows.end(), tmp_rows.cbegin(), tmp_rows.cend());
    // verify insert
    check_table_data(sql_proxy, table_name, rows);

    ObSArray<ObSessionTabletInfo> infos;
    ASSERT_SUCC(get_session_infos(table_id, infos));
    ASSERT_EQ(1, infos.size());
    const ObSessionTabletInfo &session_info = infos.at(0);
    const ObTabletMapKey key(session_info.ls_id_, session_info.tablet_id_);


    ObTabletHandle tablet_hdl_before_truncate;
    ObTablet *tablet_before_truncate = nullptr;

    // truncate tablet
    LOG_AND_PRINT(INFO, "start truncate tablet", K(ret), K(session_info));
    IN_TRANS_SCOPE(GCTX.sql_proxy_, {
      int64_t schema_version = OB_INVALID_VERSION;
      ASSERT_SUCC(gen_new_schema_version(schema_version));
      ObSessionTabletTruncateHelper helper(tenant_id_, session_info, trans, schema_version);
      if (OB_FAIL(helper.do_work())) {
        LOG_AND_PRINT(WARN, "failed to truncate tablet", K(ret));
      }

      // tablet cas should be disabled
      ASSERT_SUCC(ObTabletCreateDeleteHelper::get_tablet(key, tablet_hdl_before_truncate));
      ASSERT_NE(nullptr, tablet_before_truncate = tablet_hdl_before_truncate.get_obj());

      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      ObLSTabletService *ls_tablet_svr = nullptr;
      ASSERT_SUCC(ls_svr->get_ls(key.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD));
      ASSERT_NE(nullptr, ls = ls_handle.get_ls());
      ASSERT_NE(nullptr, ls_tablet_svr = ls->get_tablet_svr());
      ObTimeGuard time_guard;
      ASSERT_EQ(OB_EAGAIN, ls_tablet_svr->safe_update_cas_tablet(key,
                                                                 tablet_before_truncate->get_tablet_addr(),
                                                                 tablet_hdl_before_truncate,
                                                                 tablet_hdl_before_truncate,
                                                                 time_guard,
                                                                 share::SCN::min_scn()));
    })
    ASSERT_SUCC(ret);

    // tablet gc should wait if tablet is disked
    {
      ASSERT_NE(nullptr, tablet_before_truncate);
      if (tablet_before_truncate->get_tablet_addr().is_disked()) {
        ASSERT_GE(tablet_before_truncate->get_ref(), 1);
      }
    }

    LOG_AND_PRINT(INFO, "truncate finished", K(ret), K(session_info));

    // slog truncate should failed (OB_NEED_WAIT)
    //LOG_AND_PRINT(INFO, "slog checkpoint expect to be failed", K(ret), K(session_info));
    ObTenantCheckpointSlogHandler &ckpt_slog_handler = MTL(ObTenantStorageMetaService *)->ckpt_slog_handler_;
    //ASSERT_EQ(OB_NEED_WAIT, ObTenantSlogCheckpointWorkflow::execute(ObTenantSlogCheckpointWorkflow::FORCE, ckpt_slog_handler));

    check_table_data(sql_proxy, table_name, {});

    // check tablet mds
    {
      ObTabletHandle handle;
      ObTablet *tablet = nullptr;
      ObTabletTruncateMdsUserData truncate_data;
      ASSERT_SUCC(ObTabletCreateDeleteHelper::get_tablet(key, handle));
      ASSERT_NE(nullptr, tablet = handle.get_obj());
      ASSERT_SUCC(check_tablet_mds_after_truncate(
          *tablet, nullptr /* tablet_status */, &truncate_data));
      ASSERT_EQ(truncate_data.truncate_commit_scn_, tablet->get_clog_checkpoint_scn());
    }

    // insert rows with duplicate keys after truncate
    {
      vector<DatumRow> rows{{1, "name1"}, {2, "name2"}, {3, "name3"}};
      IN_TRANS_SCOPE(&sql_proxy, {
        if (OB_FAIL(insert_to(trans, table_name, rows))) {
          LOG_AND_PRINT(WARN, "failed to insert rows", K(ret));
        }
      })
      // verify result
      check_table_data(sql_proxy, table_name, rows);
      // dump tablet and do check
      ASSERT_SUCC(dump_tablet(key));
      check_table_data(sql_proxy, table_name, rows);
    }

    // tablet cas should be recovered
    {
      ObTabletHandle handle;
      ObTablet *tablet = nullptr;
      ASSERT_SUCC(ObTabletCreateDeleteHelper::get_tablet(key, handle));
      ASSERT_NE(nullptr, tablet = handle.get_obj());

      ObTabletStorageParam storage_param;
      storage_param.tablet_key_ = key;
      storage_param.original_addr_ = tablet->get_tablet_addr();
      bool skipped = false;
      ObArenaAllocator tmp_allocator;
      ASSERT_SUCC(ObTenantSlogCkptUtil::write_and_apply_tablet(storage_param,
                                                               *t3m,
                                                               *ls_svr,
                                                               *tsms,
                                                               tmp_allocator,
                                                               skipped));
      ASSERT_FALSE(skipped);
    }
    // check if tablet is disked
    {
      ObTabletHandle handle;
      ObTablet *tablet = nullptr;
      ASSERT_SUCC(ObTabletCreateDeleteHelper::get_tablet(key, handle));
      ASSERT_NE(nullptr, tablet = handle.get_obj());
      ASSERT_TRUE(tablet->get_tablet_addr().is_disked());
    }
    // checkpoint slog again, this time should be successful
    LOG_AND_PRINT(INFO, "slog checkpoint expect to be successful", K(ret), K(session_info));
    ASSERT_SUCC(ObTenantSlogCheckpointWorkflow::execute(ObTenantSlogCheckpointWorkflow::FORCE, ckpt_slog_handler));

    conn_pool.close_all_connection();
  }
  clean_all_session_tablets();
}

TEST_F(TestTruncateOracleGTT, test_continuous_truncate)
{
  BEGIN_TEST_CASE
  {
    const char *table_name = "test1";
    uint64_t table_id = 0;
    ObSingleMySQLConnectionPool conn_pool;
    ObMySQLProxy sql_proxy;
    gen_sql_proxy(conn_pool, sql_proxy);
    ASSERT_SUCC(create_gtt_table(sql_proxy, table_name, table_id, tenant_id_));


    vector<DatumRow> rows{{1, "name1"}, {2, "name2"}, {3, "name3"}};
    IN_TRANS_SCOPE(&sql_proxy, {
      if (OB_FAIL(insert_to(trans, table_name, rows))) {
        LOG_AND_PRINT(WARN, "failed to insert rows", K(ret));
      }
    })
    check_table_data(sql_proxy, table_name, rows);

    ASSERT_SUCC(trigger_minor_freeze()); // dump mds

    ObSArray<ObSessionTabletInfo> infos;
    ASSERT_SUCC(get_session_infos(table_id, infos));
    ASSERT_EQ(1, infos.size());
    const ObSessionTabletInfo &session_info = infos.at(0);
    const ObTabletMapKey key(session_info.ls_id_, session_info.tablet_id_);

    // truncate1
    LOG_AND_PRINT(INFO, "start truncate tablet(1)", K(ret), K(session_info));
    IN_TRANS_SCOPE(GCTX.sql_proxy_, {
      int64_t schema_version = OB_INVALID_VERSION;
      ASSERT_SUCC(gen_new_schema_version(schema_version));
      ObSessionTabletTruncateHelper helper(tenant_id_, session_info, trans, schema_version);
      if (OB_FAIL(helper.do_work())) {
        LOG_AND_PRINT(WARN, "failed to truncate tablet", K(ret));
      }
    })
    LOG_AND_PRINT(INFO, "truncate finished(1)", K(ret), K(session_info));

    check_table_data(sql_proxy, table_name, {});

    ASSERT_SUCC(trigger_minor_freeze()); // dump again

    check_table_data(sql_proxy, table_name, {}); // check result

    ObTabletTruncateMdsUserData data0;
    {
      ObTabletHandle tablet_hdl;
      ObTablet *tablet = nullptr;
      ASSERT_SUCC(ObTabletCreateDeleteHelper::get_tablet(key, tablet_hdl));
      ASSERT_NE(nullptr, tablet = tablet_hdl.get_obj());
      ASSERT_SUCC(check_tablet_mds_after_truncate(*tablet, nullptr/*tablet_status*/, &data0));
    }

    rows = {{1, "name1"}, {2, "name2"}, {3, "name3"}, {4, "name4"}, {5, "name5"}};
    IN_TRANS_SCOPE(&sql_proxy, {
      if (OB_FAIL(insert_to(trans, table_name, rows))) {
        LOG_AND_PRINT(WARN, "failed to insert rows", K(ret));
      }
    })
    check_table_data(sql_proxy, table_name, rows);

    // truncate2
    LOG_AND_PRINT(INFO, "start truncate tablet(2)", K(ret), K(session_info));
    IN_TRANS_SCOPE(GCTX.sql_proxy_, {
      int64_t schema_version = OB_INVALID_VERSION;
      ASSERT_SUCC(gen_new_schema_version(schema_version));
      ObSessionTabletTruncateHelper helper(tenant_id_, session_info, trans, schema_version);
      if (OB_FAIL(helper.do_work())) {
        LOG_AND_PRINT(WARN, "failed to truncate tablet", K(ret));
      }
    })
    LOG_AND_PRINT(INFO, "truncate finished(2)", K(ret), K(session_info));

    check_table_data(sql_proxy, table_name, {}); // check result

    ObTabletTruncateMdsUserData data1;
    {
      ObTabletHandle tablet_hdl;
      ObTablet *tablet = nullptr;
      ASSERT_SUCC(ObTabletCreateDeleteHelper::get_tablet(key, tablet_hdl));
      ASSERT_NE(nullptr, tablet = tablet_hdl.get_obj());
      ASSERT_SUCC(check_tablet_mds_after_truncate(*tablet, nullptr/*tablet_status*/, &data1));
    }

    ASSERT_GT(data1.truncate_commit_scn_, data0.truncate_commit_scn_);
    ASSERT_GT(data1.truncate_commit_version_, data0.truncate_commit_version_);


    check_table_data(sql_proxy, table_name, {});

    rows = {{123, "xxx"}, {298, "abc"}, {392, "zxc"}};
    IN_TRANS_SCOPE(&sql_proxy, {
      if (OB_FAIL(insert_to(trans, table_name, rows))) {
        LOG_AND_PRINT(WARN, "failed to insert rows", K(ret));
      }
    })
    ASSERT_SUCC(dump_tablet(key));
    check_table_data(sql_proxy, table_name, rows);

    conn_pool.close_all_connection();
  }
  clean_all_session_tablets();
}

TEST_F(TestTruncateOracleGTT, test_truncate_tx_abort)
{
  BEGIN_TEST_CASE

  auto get_truncate_mds_data = [](const ObTabletMapKey &key, ObTabletTruncateMdsUserData &data)->int
    {
      int ret = OB_SUCCESS;
      data.reset();
      ObTabletHandle tablet_hdl;
      ObTablet *tablet = nullptr;
      if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_hdl))) {
        LOG_AND_PRINT(WARN, "failed to get tablet", K(ret), K(key));
      } else if (OB_ISNULL(tablet = tablet_hdl.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_AND_PRINT(WARN, "unexpected null tablet", K(ret), K(key), K(tablet_hdl));
      } else if (OB_FAIL(tablet->get_truncate_mds_data(data))) {
        LOG_AND_PRINT(WARN, "failed to get truncate mds data", K(ret), K(key));
      }
      return ret;
    };
  {
    const char *table_name = "test2";
    uint64_t table_id = 0;
    ObSingleMySQLConnectionPool conn_pool;
    ObMySQLProxy sql_proxy;
    gen_sql_proxy(conn_pool, sql_proxy);
    ASSERT_SUCC(create_gtt_table(sql_proxy, table_name, table_id, tenant_id_));

    vector<DatumRow> rows{{1, "name1"}, {2, "name2"}, {3, "name3"}};
    IN_TRANS_SCOPE(&sql_proxy, {
      if (OB_FAIL(insert_to(trans, table_name, rows))) {
        LOG_AND_PRINT(WARN, "failed to insert rows", K(ret));
      }
    })
    ASSERT_SUCC(ret);
    check_table_data(sql_proxy, table_name, rows);

    ObSArray<ObSessionTabletInfo> infos;
    ASSERT_SUCC(get_session_infos(table_id, infos));
    ASSERT_EQ(1, infos.size());
    const ObSessionTabletInfo &session_info = infos.at(0);
    const ObTabletMapKey key(session_info.ls_id_, session_info.tablet_id_);

    ASSERT_SUCC(dump_tablet(key));

    // insert rows to memtable
    vector<DatumRow> tmp_rows{{4, "name4"}, {5, "name5"}, {6, "name6"}};
    IN_TRANS_SCOPE(&sql_proxy, {
      if (OB_FAIL(insert_to(trans, table_name, tmp_rows))) {
        LOG_AND_PRINT(WARN, "failed to insert rows", K(ret));
      }
    })
    ASSERT_SUCC(ret);
    rows.insert(rows.end(), tmp_rows.cbegin(), tmp_rows.cend());
    check_table_data(sql_proxy, table_name, rows);

    LOG_AND_PRINT(INFO, "start truncate tablet", K(ret), K(session_info));
    IN_TRANS_SCOPE(GCTX.sql_proxy_, {
      int64_t schema_version = OB_INVALID_VERSION;
      ASSERT_SUCC(gen_new_schema_version(schema_version));
      ObSessionTabletTruncateHelper helper(tenant_id_, session_info, trans, schema_version);
      if (OB_FAIL(helper.do_work())) {
        LOG_AND_PRINT(WARN, "failed to truncate tablet", K(ret));
      }

      ob_usleep(1_ms);
      if (OB_FAIL(OB_ERR_UNEXPECTED)) {
        LOG_AND_PRINT(WARN, "sim err to abort tx", K(ret));
      }
    })
    LOG_AND_PRINT(INFO, "truncate tablet finished", K(ret), K(session_info));
    ObTabletTruncateMdsUserData data;
    // tablet truncate mds data should be empty
    ASSERT_SUCC(get_truncate_mds_data(key, data));
    ASSERT_TRUE(data.is_default());

    // table data should not be truncated
    check_table_data(sql_proxy, table_name, rows);

    // dump again
    dump_tablet(key);

    print_all_sstables(key);

    LOG_AND_PRINT(INFO, "start truncate tablet", K(ret), K(session_info));
    IN_TRANS_SCOPE(GCTX.sql_proxy_, {
      int64_t schema_version = OB_INVALID_VERSION;
      ASSERT_SUCC(gen_new_schema_version(schema_version));
      ObSessionTabletTruncateHelper helper(tenant_id_, session_info, trans, schema_version);
      if (OB_FAIL(helper.do_work())) {
        LOG_AND_PRINT(WARN, "failed to truncate tablet", K(ret));
      }
    })
    LOG_AND_PRINT(INFO, "truncate tablet finished", K(ret), K(session_info));

    check_table_data(sql_proxy, table_name, {});

    ASSERT_SUCC(get_truncate_mds_data(key, data));

    ASSERT_TRUE(data.truncate_commit_scn_.is_valid());
    ASSERT_NE(OB_INVALID_VERSION, data.truncate_commit_version_);
    ASSERT_NE(OB_INVALID_VERSION, data.schema_version_);

    print_all_sstables(key);

    conn_pool.close_all_connection();
  }
  clean_all_session_tablets();
}

TEST_F(TestTruncateOracleGTT, test_truncate_mds_data_gc)
{
  BEGIN_TEST_CASE

  auto get_truncate_mds_data = [](const ObTabletMapKey &key, ObTabletTruncateMdsUserData &data)->int
    {
      int ret = OB_SUCCESS;
      data.reset();
      ObTabletHandle tablet_hdl;
      ObTablet *tablet = nullptr;
      if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_hdl))) {
        LOG_AND_PRINT(WARN, "failed to get tablet", K(ret), K(key));
      } else if (OB_ISNULL(tablet = tablet_hdl.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_AND_PRINT(WARN, "unexpected null tablet", K(ret), K(key), K(tablet_hdl));
      } else if (OB_FAIL(tablet->get_truncate_mds_data(data))) {
        LOG_AND_PRINT(WARN, "failed to get truncate mds data", K(ret), K(key));
      }
      return ret;
    };
  auto get_truncate_mds_data_from_sstable = [](const ObTabletMapKey &key, ObTabletTruncateMdsUserData &data)->int
    {
      int ret = OB_SUCCESS;
      ObArenaAllocator tmp_allocator;
      data.reset();
      ObTabletHandle tablet_hdl;
      ObTablet *tablet = nullptr;
      if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_hdl))) {
        LOG_AND_PRINT(WARN, "failed to get tablet", K(ret), K(key));
      } else if (OB_ISNULL(tablet = tablet_hdl.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_AND_PRINT(WARN, "unexpected null tablet", K(ret), K(key), K(tablet_hdl));
      } else if (OB_FAIL((tablet->read_data_from_mds_sstable<mds::DummyKey, ObTabletTruncateMdsUserData>(
          tmp_allocator,
          mds::DummyKey(),
          tablet->tablet_meta_.mds_checkpoint_scn_,
          ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
          ReadTabletTruncateDataOp(data))))) {
        LOG_AND_PRINT(WARN, "failed to get truncate mds data from mds sstable", K(ret));
      }
      return ret;
    };
  {
    const char *table_name = "test3";
    uint64_t table_id = 0;
    ObSingleMySQLConnectionPool conn_pool;
    ObMySQLProxy sql_proxy;
    gen_sql_proxy(conn_pool, sql_proxy);
    ASSERT_SUCC(create_gtt_table(sql_proxy, table_name, table_id, tenant_id_));

    vector<DatumRow> rows{{1, "name1"}, {2, "name2"}, {3, "name3"}};
    IN_TRANS_SCOPE(&sql_proxy, {
      if (OB_FAIL(insert_to(trans, table_name, rows))) {
        LOG_AND_PRINT(WARN, "failed to insert rows", K(ret));
      }
    })
    ASSERT_SUCC(ret);
    check_table_data(sql_proxy, table_name, rows);


    ObSArray<ObSessionTabletInfo> infos;
    ASSERT_SUCC(get_session_infos(table_id, infos));
    ASSERT_EQ(1, infos.size());
    const ObSessionTabletInfo &session_info = infos.at(0);
    const ObTabletMapKey key(session_info.ls_id_, session_info.tablet_id_);

    ASSERT_SUCC(dump_tablet(key));

    // insert rows to memtable
    vector<DatumRow> tmp_rows{{4, "name4"}, {5, "name5"}, {6, "name6"}};
    IN_TRANS_SCOPE(&sql_proxy, {
      if (OB_FAIL(insert_to(trans, table_name, tmp_rows))) {
        LOG_AND_PRINT(WARN, "failed to insert rows", K(ret));
      }
    })
    ASSERT_SUCC(ret);
    rows.insert(rows.end(), tmp_rows.cbegin(), tmp_rows.cend());
    check_table_data(sql_proxy, table_name, rows);

    LOG_AND_PRINT(INFO, "start truncate tablet", K(ret), K(session_info));
    IN_TRANS_SCOPE(GCTX.sql_proxy_, {
      int64_t schema_version = OB_INVALID_VERSION;
      ASSERT_SUCC(gen_new_schema_version(schema_version));
      ObSessionTabletTruncateHelper helper(tenant_id_, session_info, trans, schema_version);
      if (OB_FAIL(helper.do_work())) {
        LOG_AND_PRINT(WARN, "failed to truncate tablet", K(ret));
      }
    })
    LOG_AND_PRINT(INFO, "truncate tablet finished", K(ret), K(session_info));
    ObTabletTruncateMdsUserData data;
    // tablet truncate mds data should not be empty
    ASSERT_SUCC(get_truncate_mds_data(key, data));
    ASSERT_FALSE(data.is_default());

    // table data should not be truncated
    check_table_data(sql_proxy, table_name, {});

    rows = {{1, "new_name1"}, {2, "new_name2"}, {3, "new_name3"}};
    IN_TRANS_SCOPE(&sql_proxy, {
      if (OB_FAIL(insert_to(trans, table_name, rows))) {
        LOG_AND_PRINT(WARN, "failed to insert rows", K(ret));
      }
    })
    ASSERT_SUCC(ret);
    check_table_data(sql_proxy, table_name, rows);

    // dump tablet and trigger mds minor again
    ASSERT_SUCC(dump_tablet(key));
    // medium merge to scn(> truncate_commit_scn)
    share::SCN expected_scn;
    ASSERT_SUCC(expected_scn.convert_for_tx(data.truncate_commit_version_));
    ASSERT_SUCC(trigger_tablet_medium_merge(key, expected_scn));
    // check tablet last major snapshot version after medium merge finished.
    {
      ObTabletHandle tablet_hdl;
      ObTablet *tablet = nullptr;
      ASSERT_SUCC(ObTabletCreateDeleteHelper::get_tablet(key, tablet_hdl));
      ASSERT_NE(nullptr, tablet = tablet_hdl.get_obj());
      ASSERT_GT(tablet->get_last_major_snapshot_version(), data.truncate_commit_version_);
    }
    // fake a new mds sstable and trigger mds merge
    ASSERT_SUCC(new_dummy_mds_sstable(key));
    ASSERT_SUCC(trigger_mds_minor(key));
    // waiting for truncate mds data be GC
    {
      int tmp_ret = OB_SUCCESS;
      int retry_time = 100;
      for (int i = 0; i < retry_time; ++i) {
        if (OB_TMP_FAIL(get_truncate_mds_data_from_sstable(key, data))) {
          if (OB_ITER_END != tmp_ret) {
            LOG_AND_PRINT(WARN, "failed to get truncate mds data from sstable", K(ret), K(key));
          }
          break;
        } else {
          ob_usleep(100_ms);
        }
      }
      ASSERT_EQ(OB_ITER_END, tmp_ret);
    }
    fprintf(VERBOSE_OUT, "truncate data:%s\n", ObCStringHelper().convert(data));

    check_table_data(sql_proxy, table_name, rows);

    print_all_sstables(key);

    conn_pool.close_all_connection();
  }
  clean_all_session_tablets();
}

TEST_F(TestTruncateOracleGTT, test_mds_minor_keep_multi_version_truncate_chain)
{
  BEGIN_TEST_CASE

  auto get_truncate_mds_data = [](const ObTabletMapKey &key, ObTabletTruncateMdsUserData &data)->int
    {
      int ret = OB_SUCCESS;
      data.reset();
      ObTabletHandle tablet_hdl;
      ObTablet *tablet = nullptr;
      if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_hdl))) {
        LOG_AND_PRINT(WARN, "failed to get tablet", K(ret), K(key));
      } else if (OB_ISNULL(tablet = tablet_hdl.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_AND_PRINT(WARN, "unexpected null tablet", K(ret), K(key), K(tablet_hdl));
      } else if (OB_FAIL(tablet->get_truncate_mds_data(data))) {
        LOG_AND_PRINT(WARN, "failed to get truncate mds data", K(ret), K(key));
      }
      return ret;
    };
  auto get_truncate_mds_data_from_sstable = [](const ObTabletMapKey &key, ObTabletTruncateMdsUserData &data)->int
    {
      int ret = OB_SUCCESS;
      ObArenaAllocator tmp_allocator;
      data.reset();
      ObTabletHandle tablet_hdl;
      ObTablet *tablet = nullptr;
      if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_hdl))) {
        LOG_AND_PRINT(WARN, "failed to get tablet", K(ret), K(key));
      } else if (OB_ISNULL(tablet = tablet_hdl.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_AND_PRINT(WARN, "unexpected null tablet", K(ret), K(key), K(tablet_hdl));
      } else if (OB_FAIL((tablet->read_data_from_mds_sstable<mds::DummyKey, ObTabletTruncateMdsUserData>(
          tmp_allocator,
          mds::DummyKey(),
          tablet->tablet_meta_.mds_checkpoint_scn_,
          ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
          ReadTabletTruncateDataOp(data))))) {
        LOG_AND_PRINT(WARN, "failed to get truncate mds data from mds sstable", K(ret), K(key));
      }
      return ret;
    };
  {
    const char *table_name = "test_mds_keep_mv_truncate";
    uint64_t table_id = 0;
    ObSingleMySQLConnectionPool conn_pool;
    ObMySQLProxy sql_proxy;
    gen_sql_proxy(conn_pool, sql_proxy);
    ASSERT_SUCC(create_gtt_table(sql_proxy, table_name, table_id, tenant_id_));

    vector<DatumRow> rows{{1, "old_name1"}, {2, "old_name2"}};
    IN_TRANS_SCOPE(&sql_proxy, {
      if (OB_FAIL(insert_to(trans, table_name, rows))) {
        LOG_AND_PRINT(WARN, "failed to insert rows", K(ret));
      }
    })
    ASSERT_SUCC(ret);

    ObSArray<ObSessionTabletInfo> infos;
    ASSERT_SUCC(get_session_infos(table_id, infos));
    ASSERT_EQ(1, infos.size());
    const ObSessionTabletInfo &session_info = infos.at(0);
    const ObTabletMapKey key(session_info.ls_id_, session_info.tablet_id_);
    ASSERT_SUCC(dump_tablet(key));

    IN_TRANS_SCOPE(GCTX.sql_proxy_, {
      int64_t schema_version = OB_INVALID_VERSION;
      ASSERT_SUCC(gen_new_schema_version(schema_version));
      ObSessionTabletTruncateHelper helper(tenant_id_, session_info, trans, schema_version);
      if (OB_FAIL(helper.do_work())) {
        LOG_AND_PRINT(WARN, "failed to truncate tablet", K(ret));
      }
    })
    ASSERT_SUCC(ret);
    ObTabletTruncateMdsUserData old_data;
    ASSERT_SUCC(get_truncate_mds_data(key, old_data));
    ASSERT_FALSE(old_data.is_default());

    rows = {{1, "new_name1"}, {2, "new_name2"}};
    IN_TRANS_SCOPE(&sql_proxy, {
      if (OB_FAIL(insert_to(trans, table_name, rows))) {
        LOG_AND_PRINT(WARN, "failed to insert rows", K(ret));
      }
    })
    ASSERT_SUCC(ret);
    check_table_data(sql_proxy, table_name, rows);
    ASSERT_SUCC(dump_tablet(key));
    int64_t mds_sstable_cnt = 0;
    ASSERT_SUCC(get_mds_sstable_cnt(key, mds_sstable_cnt));
    ASSERT_EQ(0, mds_sstable_cnt);

    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    ASSERT_TRUE(tenant_config.is_valid());
    const int64_t original_mds_minor_compact_trigger = tenant_config->mds_minor_compact_trigger;
    DEFER(tenant_config->mds_minor_compact_trigger = original_mds_minor_compact_trigger);
    tenant_config->mds_minor_compact_trigger = 16;

    IN_TRANS_SCOPE(GCTX.sql_proxy_, {
      int64_t schema_version = OB_INVALID_VERSION;
      ASSERT_SUCC(gen_new_schema_version(schema_version));
      ObSessionTabletTruncateHelper helper(tenant_id_, session_info, trans, schema_version);
      if (OB_FAIL(helper.do_work())) {
        LOG_AND_PRINT(WARN, "failed to truncate tablet", K(ret));
      }
    })
    ASSERT_SUCC(ret);
    ObTabletTruncateMdsUserData new_data;
    ASSERT_SUCC(get_truncate_mds_data(key, new_data));
    ASSERT_GT(new_data.truncate_commit_scn_.get_val_for_tx(), old_data.truncate_commit_scn_.get_val_for_tx());
    ASSERT_GT(new_data.truncate_commit_version_, old_data.truncate_commit_version_ + 1);
    check_table_data(sql_proxy, table_name, {});

    // Keep T1 eligible for filtering while T2 is still newer than the injected major snapshot.
    const int64_t filter_last_major_snapshot = old_data.truncate_commit_version_ + 1;
    ASSERT_LT(filter_last_major_snapshot, new_data.truncate_commit_version_);
    ASSERT_SUCC(new_dummy_mds_sstable(key));
    ASSERT_SUCC(get_mds_sstable_cnt(key, mds_sstable_cnt));
    ASSERT_EQ(1, mds_sstable_cnt);

    ObTabletHandle filter_tablet_hdl;
    ObTablet *filter_tablet = nullptr;
    ASSERT_SUCC(ObTabletCreateDeleteHelper::get_tablet(key, filter_tablet_hdl));
    ASSERT_NE(nullptr, filter_tablet = filter_tablet_hdl.get_obj());
    ASSERT_GT(filter_tablet->get_clog_checkpoint_scn().get_val_for_tx(),
      old_data.truncate_commit_scn_.get_val_for_tx());
    filter_tablet->table_store_cache_.last_major_snapshot_version_ = filter_last_major_snapshot;
    ASSERT_EQ(filter_last_major_snapshot, filter_tablet->get_last_major_snapshot_version());
    tenant_config->mds_minor_compact_trigger = original_mds_minor_compact_trigger;
    ASSERT_SUCC(trigger_mds_minor(key));
    ASSERT_SUCC(wait_for_mds_minor_finish(key));

    ObTabletTruncateMdsUserData first_round_data;
    ASSERT_SUCC(get_truncate_mds_data_from_sstable(key, first_round_data));
    ASSERT_FALSE(first_round_data.is_default());
    ASSERT_EQ(new_data.truncate_commit_scn_.get_val_for_tx(), first_round_data.truncate_commit_scn_.get_val_for_tx());
    ASSERT_EQ(new_data.truncate_commit_version_, first_round_data.truncate_commit_version_);
    ASSERT_EQ(new_data.schema_version_, first_round_data.schema_version_);

    tenant_config->mds_minor_compact_trigger = 16;
    ASSERT_SUCC(new_dummy_mds_sstable(key));
    ASSERT_SUCC(get_mds_sstable_cnt(key, mds_sstable_cnt));
    ASSERT_GT(mds_sstable_cnt, 1);
    tenant_config->mds_minor_compact_trigger = original_mds_minor_compact_trigger;
    ASSERT_SUCC(trigger_mds_minor(key));
    ASSERT_SUCC(wait_for_mds_minor_finish(key));

    ObTabletTruncateMdsUserData second_round_data;
    ASSERT_SUCC(get_truncate_mds_data_from_sstable(key, second_round_data));
    ASSERT_FALSE(second_round_data.is_default());
    ASSERT_EQ(new_data.truncate_commit_scn_.get_val_for_tx(), second_round_data.truncate_commit_scn_.get_val_for_tx());
    ASSERT_EQ(new_data.truncate_commit_version_, second_round_data.truncate_commit_version_);
    ASSERT_EQ(new_data.schema_version_, second_round_data.schema_version_);

    conn_pool.close_all_connection();
  }
  clean_all_session_tablets();
}

struct MockSSTableBuilder final
{
  static void make(
      const ObITable::TableType type,
      const int64_t start_scn,
      const int64_t end_scn,
      const int64_t snapshot_version,
      blocksstable::ObSSTable &sstable,
      blocksstable::ObSSTableMeta &meta)
  {
    meta.basic_meta_.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
    meta.basic_meta_.latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
    meta.basic_meta_.status_ = SSTABLE_READY_FOR_READ;
    meta.basic_meta_.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    meta.data_root_info_.addr_.set_none_addr();
    meta.macro_info_.macro_meta_info_.addr_.set_none_addr();
    meta.is_inited_ = true;

    sstable.key_.table_type_ = type;
    sstable.key_.tablet_id_ = 1;
    if (ObITable::is_major_sstable(type)) {
      sstable.key_.version_range_.snapshot_version_ = snapshot_version;
    } else {
      sstable.key_.scn_range_.start_scn_.convert_for_gts(start_scn);
      sstable.key_.scn_range_.end_scn_.convert_for_gts(end_scn);
    }
    sstable.meta_ = &meta;
    sstable.valid_for_reading_ = true;
  }
};

TEST_F(TestTruncateOracleGTT, test_sstable_truncate_filter_check_sstable)
{
  int ret = OB_SUCCESS;
  // truncate boundary: T = scn(200), V = 100
  share::SCN scn_T;
  scn_T.convert_for_tx(200);
  const int64_t version_V = 100;

  ObSSTableTruncateFilter filter;
  ASSERT_SUCC(filter.init(scn_T, version_V));
  ASSERT_TRUE(filter.need_filter_());

  bool keep = true;

  // major: snapshot_version < V => drop
  {
    blocksstable::ObSSTable sst;
    blocksstable::ObSSTableMeta meta;
    MockSSTableBuilder::make(ObITable::MAJOR_SSTABLE, 0, 0, 99, sst, meta);
    ASSERT_SUCC(filter.check_sstable_(sst, keep));
    ASSERT_FALSE(keep);
  }
  // major: snapshot_version == V => keep
  {
    blocksstable::ObSSTable sst;
    blocksstable::ObSSTableMeta meta;
    MockSSTableBuilder::make(ObITable::MAJOR_SSTABLE, 0, 0, 100, sst, meta);
    ASSERT_SUCC(filter.check_sstable_(sst, keep));
    ASSERT_TRUE(keep);
  }
  // major: snapshot_version > V => keep
  {
    blocksstable::ObSSTable sst;
    blocksstable::ObSSTableMeta meta;
    MockSSTableBuilder::make(ObITable::MAJOR_SSTABLE, 0, 0, 150, sst, meta);
    ASSERT_SUCC(filter.check_sstable_(sst, keep));
    ASSERT_TRUE(keep);
  }
  // meta_major: drop by version
  {
    blocksstable::ObSSTable sst;
    blocksstable::ObSSTableMeta meta;
    MockSSTableBuilder::make(ObITable::META_MAJOR_SSTABLE, 0, 0, 50, sst, meta);
    ASSERT_SUCC(filter.check_sstable_(sst, keep));
    ASSERT_FALSE(keep);
  }

  // minor: end_scn <= T => drop (boundary equal)
  {
    blocksstable::ObSSTable sst;
    blocksstable::ObSSTableMeta meta;
    MockSSTableBuilder::make(ObITable::MINOR_SSTABLE, 100, 200, 0, sst, meta);
    ASSERT_SUCC(filter.check_sstable_(sst, keep));
    ASSERT_FALSE(keep);
  }
  // minor: start_scn >= T => keep (boundary equal)
  {
    blocksstable::ObSSTable sst;
    blocksstable::ObSSTableMeta meta;
    MockSSTableBuilder::make(ObITable::MINOR_SSTABLE, 200, 300, 0, sst, meta);
    ASSERT_SUCC(filter.check_sstable_(sst, keep));
    ASSERT_TRUE(keep);
  }
  // mini sstable shares minor semantics
  {
    blocksstable::ObSSTable sst;
    blocksstable::ObSSTableMeta meta;
    MockSSTableBuilder::make(ObITable::MINI_SSTABLE, 100, 200, 0, sst, meta);
    ASSERT_SUCC(filter.check_sstable_(sst, keep));
    ASSERT_FALSE(keep);
  }

  // ddl: same semantics as minor
  {
    blocksstable::ObSSTable sst;
    blocksstable::ObSSTableMeta meta;
    MockSSTableBuilder::make(ObITable::DDL_DUMP_SSTABLE, 100, 200, 0, sst, meta);
    ASSERT_SUCC(filter.check_sstable_(sst, keep));
    ASSERT_FALSE(keep);
  }

  // mds: never filtered
  {
    blocksstable::ObSSTable sst;
    blocksstable::ObSSTableMeta meta;
    MockSSTableBuilder::make(ObITable::MDS_MINOR_SSTABLE, 0, 100, 0, sst, meta);
    ASSERT_SUCC(filter.check_sstable_(sst, keep));
    ASSERT_TRUE(keep);
  }
  {
    blocksstable::ObSSTable sst;
    blocksstable::ObSSTableMeta meta;
    MockSSTableBuilder::make(ObITable::MDS_MINI_SSTABLE, 150, 250, 0, sst, meta);
    ASSERT_SUCC(filter.check_sstable_(sst, keep));
    ASSERT_TRUE(keep);
  }

  // need_filter() == false => everything keeps
  {
    ObSSTableTruncateFilter noop_filter;
    share::SCN scn_min;
    scn_min.set_min();
    ASSERT_SUCC(noop_filter.init(scn_min, 0));
    ASSERT_FALSE(noop_filter.need_filter_());

    blocksstable::ObSSTable sst;
    blocksstable::ObSSTableMeta meta;
    MockSSTableBuilder::make(ObITable::MINOR_SSTABLE, 0, 50, 0, sst, meta);
    ASSERT_SUCC(noop_filter.check_sstable_(sst, keep));
    ASSERT_TRUE(keep);
  }
}

} // end namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_new_oracle_gtt_truncate.log*");
  OB_LOGGER.set_file_name("test_new_oracle_gtt_truncate.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
