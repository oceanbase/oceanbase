/**
 * Copyright (c) 2023 OceanBase
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

#include "storage/mview/ob_mlog_purge.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_errno.h"
#include "share/schema/ob_mview_info.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/mview/ob_mview_refresh_helper.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace sql;

ObMLogPurger::ObMLogPurger()
  : ctx_(nullptr), is_oracle_mode_(false), need_purge_(false), is_inited_(false)
{
}

ObMLogPurger::~ObMLogPurger() {}

int ObMLogPurger::init(ObExecContext &ctx, const ObMLogPurgeParam &purge_param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMLogPurger init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == ctx.get_my_session() || nullptr == ctx.get_sql_proxy() ||
                         !purge_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(ctx), K(purge_param));
  } else {
    ctx_ = &ctx;
    purge_param_ = purge_param;
    is_inited_ = true;
  }
  return ret;
}

int ObMLogPurger::purge()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMLogPurger not init", KR(ret), KP(this));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("ctx can not be NULL", KR(ret));
  } else {
    if (OB_FAIL(trans_.start(ctx_->get_my_session(), ctx_->get_sql_proxy()))) {
      LOG_WARN("fail to start trans", KR(ret));
    } else if (OB_FAIL(prepare_for_purge())) {
      LOG_WARN("fail to prepare for purge", KR(ret));
    } else if (need_purge_ && OB_FAIL(do_purge())) {
      LOG_WARN("fail to do purge", KR(ret));
    }
    if (trans_.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans_.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
    LOG_INFO("mlog purge finish", KR(ret), K(purge_param_), K(mlog_info_), K(need_purge_),
             K(purge_scn_));
  }
  return ret;
}

int ObMLogPurger::prepare_for_purge()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = purge_param_.tenant_id_;
  const uint64_t master_table_id = purge_param_.master_table_id_;
  uint64_t mlog_table_id = OB_INVALID_ID;
  share::schema::ObSchemaGetterGuard schema_guard;
  SCN current_scn;
  const share::schema::ObTableSchema *master_table_schema = nullptr;
  const share::schema::ObTableSchema *mlog_table_schema = nullptr;
  ObArray<ObDependencyInfo> deps;
  uint64_t min_mview_refresh_scn = UINT64_MAX;
  // get refreshed schema and scn
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObMViewRefreshHelper::get_current_scn(current_scn))) {
    LOG_WARN("fail to get current scn", KR(ret));
  }
  // get master table schema
  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, master_table_id, master_table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(master_table_id));
    } else if (OB_ISNULL(master_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", KR(ret), K(tenant_id), K(master_table_id));
    } else if (OB_UNLIKELY(OB_INVALID_ID ==
                           (mlog_table_id = master_table_schema->get_mlog_tid()))) {
      ret = OB_ERR_TABLE_NO_MLOG;
      LOG_WARN("table does not have materialized view log", KR(ret), KPC(master_table_schema));
    } else if (OB_FAIL(
                 schema_guard.get_table_schema(tenant_id, mlog_table_id, mlog_table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(mlog_table_id));
    } else if (OB_ISNULL(mlog_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected mlog table schema not exist", KR(ret), K(tenant_id),
               KPC(master_table_schema));
    } else if (OB_UNLIKELY(!mlog_table_schema->is_mlog_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table type not mlog", KR(ret), K(tenant_id), KPC(mlog_table_schema));
    } else if (OB_UNLIKELY(!mlog_table_schema->is_available_mlog())) {
      ret = OB_ERR_TABLE_NO_MLOG;
      LOG_WARN("materialized view log is not available", KR(ret), KPC(mlog_table_schema));
    } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(
                 tenant_id, mlog_table_id, is_oracle_mode_))) {
      LOG_WARN("check if oracle mode failed", KR(ret), K(mlog_table_id));
    }
  }
  // fetch mlog info and dep objs
  if (OB_SUCC(ret)) {
    WITH_MVIEW_TRANS_INNER_MYSQL_GUARD(trans_)
    {
      if (OB_FAIL(ObMLogInfo::fetch_mlog_info(trans_, tenant_id, mlog_table_id, mlog_info_,
                                              true /*for_update*/))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          LOG_WARN("fail to fetch mlog info", KR(ret), K(mlog_table_id));
        } else {
          ret = OB_ERR_TABLE_NO_MLOG;
          LOG_WARN("materialized view log may dropped", KR(ret), K(mlog_table_id));
        }
      } else if (OB_FAIL(
                   ObDependencyInfo::collect_dep_infos(tenant_id, master_table_id, trans_, deps))) {
        LOG_WARN("fail to collect dep infos", KR(ret), K(master_table_id));
      }
    }
  }
  // collect min refresh scn of mviews
  if (OB_SUCC(ret)) {
    WITH_MVIEW_TRANS_INNER_MYSQL_GUARD(trans_)
    {
      for (int64_t i = 0; OB_SUCC(ret) && i < deps.count(); ++i) {
        const uint64_t table_id = deps.at(i).get_dep_obj_id();
        const ObTableSchema *table_schema = nullptr;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
          LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
        } else if (OB_NOT_NULL(table_schema) && table_schema->is_materialized_view()) {
          ObMViewInfo mview_info;
          if (OB_FAIL(ObMViewInfo::fetch_mview_info(trans_, tenant_id, table_id, mview_info))) {
            if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
              LOG_WARN("fail to fetch mview info", KR(ret), K(table_id));
            } else {
              ret = OB_SUCCESS;
            }
          } else if (OB_INVALID_SCN_VAL != mview_info.get_last_refresh_scn()) {
            min_mview_refresh_scn = MIN(min_mview_refresh_scn, mview_info.get_last_refresh_scn());
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (min_mview_refresh_scn != UINT64_MAX) {
      if (min_mview_refresh_scn <= mlog_info_.get_last_purge_scn()) {
        need_purge_ = false;
        LOG_INFO("mlog does not need purge", KR(ret), K(mlog_info_), K(min_mview_refresh_scn));
      } else if (OB_FAIL(purge_scn_.convert_for_inner_table_field(min_mview_refresh_scn))) {
        LOG_WARN("fail to convert for inner table field", KR(ret), K(min_mview_refresh_scn));
      } else {
        need_purge_ = true;
      }
    } else {
      purge_scn_ = current_scn;
      need_purge_ = true;
    }
  }
  if (OB_SUCC(ret) && need_purge_) {
    if (OB_FAIL(ObMViewRefreshHelper::generate_purge_mlog_sql(
          schema_guard, tenant_id, mlog_table_id, purge_scn_, purge_param_.purge_log_parallel_, purge_sql_))) {
      LOG_WARN("fail to generate purge mlog sql", KR(ret), K(mlog_table_id), K(purge_scn_));
    }
  }
  return ret;
}

int ObMLogPurger::do_purge()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = purge_param_.tenant_id_;
  // 1. execute purge sql
  const int64_t start_time = ObTimeUtil::current_time();
  int64_t affected_rows = 0;
  if (OB_FAIL(trans_.write(tenant_id, purge_sql_.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(purge_sql_));
  }
  const int64_t end_time = ObTimeUtil::current_time();
  // 2. update mlog last purge info
  if (OB_SUCC(ret)) {
    WITH_MVIEW_TRANS_INNER_MYSQL_GUARD(trans_)
    {
      mlog_info_.set_last_purge_scn(purge_scn_.get_val_for_inner_table_field());
      mlog_info_.set_last_purge_date(start_time);
      mlog_info_.set_last_purge_time((end_time - start_time) / 1000 / 1000);
      mlog_info_.set_last_purge_rows(affected_rows);
      if (OB_FAIL(mlog_info_.set_last_purge_trace_id(ObCurTraceId::get_trace_id_str()))) {
        LOG_WARN("fail to set last purge trace id", KR(ret));
      } else if (OB_FAIL(ObMLogInfo::update_mlog_last_purge_info(trans_, mlog_info_))) {
        LOG_WARN("fail to update mlog last purge info", KR(ret), K(mlog_info_));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
