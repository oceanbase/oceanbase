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

#include "storage/mview/ob_mview_refresh.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "sql/resolver/mv/ob_mv_provider.h"
#include "storage/mview/ob_mview_refresh_helper.h"
#include "storage/mview/ob_mview_refresh_stats_collect.h"
#include "storage/mview/ob_mview_transaction.h"
#include "storage/mview/ob_mview_mds.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace observer;
using namespace share;
using namespace share::schema;
using namespace sql;

/**
 * ObMViewRefresher
 */

ObMViewRefresher::ObMViewRefresher()
  : ctx_(nullptr), refresh_ctx_(nullptr), refresh_stats_collection_(nullptr), is_inited_(false)
{
}

ObMViewRefresher::~ObMViewRefresher() {}

int ObMViewRefresher::init(ObExecContext &ctx, ObMViewRefreshCtx &refresh_ctx,
                           const ObMViewRefreshParam &refresh_param,
                           ObMViewRefreshStatsCollection *refresh_stats_collection)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMViewRefresher init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == ctx.get_my_session() || nullptr == ctx.get_sql_proxy() ||
                         nullptr == refresh_ctx.trans_ || !refresh_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(ctx), K(refresh_ctx), K(refresh_param));
  } else {
    ctx_ = &ctx;
    refresh_ctx_ = &refresh_ctx;
    refresh_param_ = refresh_param;
    refresh_stats_collection_ = refresh_stats_collection;
    is_inited_ = true;
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_MVIEW_REFRESH)
int ObMViewRefresher::refresh()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewRefresher not init", KR(ret), KP(this));
  } else {
    const uint64_t tenant_id = refresh_param_.tenant_id_;
    const uint64_t mview_id = refresh_param_.mview_id_;
    if (OB_FAIL(lock_mview_for_refresh())) {
      LOG_WARN("fail to lock mview for refresh", KR(ret));
    } else if (OB_FAIL(prepare_for_refresh())) {
      LOG_WARN("fail to prepare for refresh", KR(ret));
    }
    // collect stats before refresh
    if (OB_SUCC(ret) && nullptr != refresh_stats_collection_) {
      if (OB_FAIL(refresh_stats_collection_->collect_before_refresh(*refresh_ctx_))) {
        LOG_WARN("fail to collect refresh stats before refresh", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const ObMVRefreshType refresh_type = refresh_ctx_->refresh_type_;
      ObMViewOpArg arg;
      arg.table_id_ =  mview_id;
      arg.parallel_ = refresh_ctx_->refresh_parallelism_;
      if (ObMVRefreshType::FAST == refresh_type) {
        arg.mview_op_type_ = MVIEW_OP_TYPE::FAST_REFRESH;
        arg.read_snapshot_ = refresh_ctx_->refresh_scn_range_.end_scn_.get_val_for_tx();
      } else if (ObMVRefreshType::COMPLETE == refresh_type) {
        // COMPLETE REFRESH is ddl task keep snapshot by ddl frame
        arg.mview_op_type_ = MVIEW_OP_TYPE::COMPLETE_REFRESH;
      }
      if (OB_FAIL(ObMViewMdsOpHelper::register_mview_mds(tenant_id, arg, *refresh_ctx_->trans_))) {
        LOG_WARN("register mview mds failed", KR(ret), K(tenant_id), K(arg));
      } else if (ObMVRefreshType::FAST == refresh_type) {
        if (OB_FAIL(fast_refresh())) {
          LOG_WARN("fail to fast refresh", KR(ret));
        }
      } else if (ObMVRefreshType::COMPLETE == refresh_type) {
        if (OB_FAIL(complete_refresh())) {
          LOG_WARN("fail to complete refresh", KR(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected refresh type", KR(ret), K(refresh_type));
      }
    }
    // collect stats after refresh
    if (OB_SUCC(ret) && nullptr != refresh_stats_collection_) {
      if (OB_FAIL(refresh_stats_collection_->collect_after_refresh(*refresh_ctx_))) {
        LOG_WARN("fail to collect refresh stats after refresh", KR(ret));
      }
    }
    LOG_INFO("mview refresh finish", KR(ret), K(refresh_param_));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret) && OB_FAIL(ERRSIM_MVIEW_REFRESH)) {
    LOG_WARN("errsim mview refresh", K(ret));
  }
#endif
  return ret;
}

int ObMViewRefresher::lock_mview_for_refresh()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = refresh_param_.tenant_id_;
  const uint64_t mview_id = refresh_param_.mview_id_;
  int64_t retries = 0;
  CK(OB_NOT_NULL(refresh_ctx_->trans_));
  while (OB_SUCC(ret) && OB_SUCC(ctx_->check_status())) {
    if (OB_FAIL(ObMViewRefreshHelper::lock_mview(*refresh_ctx_->trans_, tenant_id, mview_id,
                                                 true /*try_lock*/))) {
      if (OB_UNLIKELY(OB_TRY_LOCK_ROW_CONFLICT != ret)) {
        LOG_WARN("fail to lock mview for refresh", KR(ret), K(tenant_id), K(mview_id));
      } else {
        ret = OB_SUCCESS;
        ++retries;
        if (retries % 10 == 0) {
          LOG_WARN("retry too many times", K(retries), K(tenant_id), K(mview_id));
        }
        ob_usleep(100LL * 1000);
      }
    } else {
      break;
    }
  }
  return ret;
}

int ObMViewRefresher::prepare_for_refresh()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = refresh_param_.tenant_id_;
  const uint64_t mview_id = refresh_param_.mview_id_;
  ObMViewTransaction &trans = *refresh_ctx_->trans_;
  ObMViewInfo &mview_info = refresh_ctx_->mview_info_;
  ObMViewRefreshStatsParams &refresh_stats_params = refresh_ctx_->refresh_stats_params_;
  ObIArray<ObDependencyInfo> &dependency_infos = refresh_ctx_->dependency_infos_;
  ObScnRange &refresh_scn_range = refresh_ctx_->refresh_scn_range_;
  ObMVRefreshType &refresh_type = refresh_ctx_->refresh_type_;
  ObSQLSessionInfo *session_info = nullptr;
  ObSchemaGetterGuard schema_guard;
  SCN current_scn;
  uint64_t data_version = 0;
  const ObTableSchema *mview_table_schema = nullptr;
  if (OB_ISNULL(session_info = ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session info", KR(ret), KPC(ctx_));
  }
  // get refreshed schema and scn
  else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObMViewRefreshHelper::get_current_scn(current_scn))) {
    LOG_WARN("fail to get current scn", KR(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data_version", KR(ret));
  }
  // fetch mview info
  if (OB_SUCC(ret)) {
    WITH_MVIEW_TRANS_INNER_MYSQL_GUARD(trans)
    {
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, mview_id, mview_table_schema))) {
        LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(mview_id));
      } else if (OB_ISNULL(mview_table_schema)) {
        ret = OB_ERR_MVIEW_NOT_EXIST;
        LOG_WARN("mview not exist", KR(ret), K(tenant_id), K(mview_id));
      } else if (OB_UNLIKELY(!mview_table_schema->is_materialized_view())) {
        ret = OB_ERR_MVIEW_NOT_EXIST;
        LOG_WARN("table is not mview", KR(ret), K(tenant_id), K(mview_id));
      } else if (OB_FAIL(ObMViewInfo::fetch_mview_info(trans, tenant_id, mview_id, mview_info))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          LOG_WARN("fail to fetch mview info", KR(ret), K(tenant_id), K(mview_id));
        } else {
          ret = OB_ERR_MVIEW_NOT_EXIST;
          LOG_WARN("mview may dropped", KR(ret), K(tenant_id), K(mview_id));
        }
      } else if (OB_FAIL(ObMViewRefreshStatsParams::fetch_mview_refresh_stats_params(
                   trans, tenant_id, mview_id, refresh_stats_params, true /*with_sys_defaults*/))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          LOG_WARN("fail to fetch mview refresh stats params", KR(ret), K(tenant_id), K(mview_id));
        } else {
          ret = OB_ERR_MVIEW_NOT_EXIST;
          LOG_WARN("mview may dropped", KR(ret), K(tenant_id), K(mview_id));
        }
      } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(
                   tenant_id, mview_id, refresh_ctx_->is_oracle_mode_))) {
        LOG_WARN("check if oracle mode failed", KR(ret), K(mview_id));
      }
    }
  }
  // set refresh scn range
  if (OB_SUCC(ret)) {
    if (OB_INVALID_SCN_VAL != mview_info.get_last_refresh_scn() &&
        OB_FAIL(refresh_scn_range.start_scn_.convert_for_inner_table_field(
          mview_info.get_last_refresh_scn()))) {
      LOG_WARN("fail to convert for inner table field", KR(ret), K(mview_info));
    } else {
      refresh_scn_range.end_scn_ = current_scn;
    }
  }
  // check refresh type
  if (OB_SUCC(ret)) {
    ObMVRefreshMethod refresh_method = ObMVRefreshMethod::MAX == refresh_param_.refresh_method_
                                         ? mview_info.get_refresh_method()
                                         : refresh_param_.refresh_method_;
    ObMVProvider mv_provider(tenant_id, mview_id);
    bool can_fast_refresh = false;
    FastRefreshableNotes note;
    if (ObMVRefreshMode::NEVER == mview_info.get_refresh_mode()) {
      ret = OB_ERR_MVIEW_NEVER_REFRESH;
      LOG_WARN("mview never refresh", KR(ret), K(mview_info));
    } else if (OB_FAIL(mv_provider.init_mv_provider(refresh_scn_range.start_scn_,
                                                    refresh_scn_range.end_scn_,
                                                    &schema_guard,
                                                    session_info,
                                                    note))) {
      LOG_WARN("fail to init mv provider", KR(ret), K(tenant_id));
    } else if (OB_FAIL(mv_provider.get_mv_dependency_infos(dependency_infos))) {
      LOG_WARN("fail to get mv dependency infos", KR(ret), K(tenant_id));
    } else if (OB_FAIL(fetch_based_infos(schema_guard))) {
      LOG_WARN("fail to fetch based infos", KR(ret));
    } else if (OB_FAIL(mv_provider.check_mv_refreshable(can_fast_refresh))) {
      LOG_WARN("fail to check refresh type", KR(ret));
    } else if (ObMVRefreshMethod::COMPLETE == refresh_method ||
               (!can_fast_refresh && ObMVRefreshMethod::FORCE == refresh_method)) {
      refresh_type = ObMVRefreshType::COMPLETE;
    } else if (!can_fast_refresh && ObMVRefreshMethod::FAST == refresh_method) {
      ret = OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH;
      LOG_WARN("mv can not fast refresh", KR(ret));
      LOG_USER_ERROR(OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH, mview_table_schema->get_table_name(),
                     note.error_.ptr());
    } else if (OB_FAIL(check_fast_refreshable())) {
      if (ObMVRefreshMethod::FORCE == refresh_method &&
          OB_LIKELY(OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH == ret || OB_ERR_MLOG_IS_YOUNGER == ret)) {
        refresh_type = ObMVRefreshType::COMPLETE;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to check check fast refreshable", KR(ret));
      }
    } else {
      refresh_type = ObMVRefreshType::FAST;
    }
    if (OB_SUCC(ret) && ObMVRefreshType::FAST == refresh_type) {
      const ObIArray<ObString> *operators = nullptr;
      ObString fast_refresh_sql;
      if (OB_FAIL(mv_provider.get_fast_refresh_operators(operators))) {
        LOG_WARN("fail to get operators", KR(ret));
      } else if (OB_ISNULL(operators)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", KR(ret), K(operators));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < operators->count(); ++i) {
        const ObString &op_sql = operators->at(i);
        if (OB_FAIL(ob_write_string(refresh_ctx_->allocator_, op_sql, fast_refresh_sql, true))) {
          LOG_WARN("fail to copy string", KR(ret), K(i), K(op_sql));
        } else if (OB_FAIL(refresh_ctx_->refresh_sqls_.push_back(fast_refresh_sql))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
  }

  // calculate refresh parallelism
  if (OB_SUCC(ret) && data_version >= DATA_VERSION_4_3_5_1) {
    int64_t final_parallelism = 0;
    int64_t explict_parallelism = trans.is_inner_session() ? mview_info.get_refresh_dop() : refresh_param_.parallelism_;
    if (OB_FAIL(calc_mv_refresh_parallelism(explict_parallelism, ctx_->get_my_session(),
                                            final_parallelism))) {
      LOG_WARN("fail to calculate mv refresh parallelism", KR(ret), K(refresh_param_));
    } else {
      refresh_param_.parallelism_ = final_parallelism;
      refresh_ctx_->refresh_parallelism_ = final_parallelism;
    }
  }
  return ret;
}

int ObMViewRefresher::fetch_based_infos(ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = refresh_param_.tenant_id_;
  ObMViewTransaction &trans = *refresh_ctx_->trans_;
  const ObIArray<ObDependencyInfo> &dependency_infos = refresh_ctx_->dependency_infos_;
  ObIArray<ObBasedSchemaObjectInfo> &based_schema_object_infos =
    refresh_ctx_->based_schema_object_infos_;
  ObIArray<ObMLogInfo> &mlog_infos = refresh_ctx_->mlog_infos_;
  based_schema_object_infos.reset();
  mlog_infos.reset();
  WITH_MVIEW_TRANS_INNER_MYSQL_GUARD(trans)
  {
    for (int64_t i = 0; OB_SUCC(ret) && i < dependency_infos.count(); ++i) {
      const ObDependencyInfo &dep = dependency_infos.at(i);
      const ObTableSchema *based_table_schema = nullptr;
      if (OB_UNLIKELY(ObObjectType::TABLE != dep.get_ref_obj_type()
                      && ObObjectType::VIEW != dep.get_ref_obj_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("ref obj type is not table, not supported", KR(ret), K(dep));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "the ref obj type of materialized view not user table is");
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, dep.get_ref_obj_id(),
                                                       based_table_schema))) {
        LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(dep));
      } else if (OB_ISNULL(based_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("based table not exist", KR(ret), K(tenant_id), K(dep));
      } else {
        ObBasedSchemaObjectInfo based_info;
        based_info.schema_id_ = dep.get_ref_obj_id();
        based_info.schema_type_ = ObSchemaType::TABLE_SCHEMA;
        based_info.schema_version_ = based_table_schema->get_schema_version();
        based_info.schema_tenant_id_ = tenant_id;
        if (OB_FAIL(based_schema_object_infos.push_back(based_info))) {
          LOG_WARN("fail to push back base info", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        const uint64_t mlog_table_id = based_table_schema->get_mlog_tid();
        const ObTableSchema *mlog_table_schema = nullptr;
        ObMLogInfo mlog_info;
        if (OB_INVALID_ID != mlog_table_id) {
          if (OB_FAIL(schema_guard.get_table_schema(tenant_id, mlog_table_id, mlog_table_schema))) {
            LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(mlog_table_id));
          } else if (OB_ISNULL(mlog_table_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected mlog table schema not exist", KR(ret), K(tenant_id),
                     K(mlog_table_id));
          } else if (OB_UNLIKELY(!mlog_table_schema->is_mlog_table())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected table type not mlog", KR(ret), KPC(mlog_table_schema));
          } else if (!mlog_table_schema->is_available_mlog()) {
            // mlog is unavailable
          } else if (OB_FAIL(
                       ObMLogInfo::fetch_mlog_info(trans, tenant_id, mlog_table_id, mlog_info))) {
            if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
              LOG_WARN("fail to fetch mlog info", KR(ret), K(mlog_table_id));
            } else {
              // mlog may dropped
              ret = OB_SUCCESS;
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(mlog_infos.push_back(mlog_info))) {
            LOG_WARN("fail to push back mlog info", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObMViewRefresher::check_fast_refreshable()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = refresh_param_.tenant_id_;
  const uint64_t mview_id = refresh_param_.mview_id_;
  ObMViewTransaction &trans = *refresh_ctx_->trans_;
  const ObIArray<ObDependencyInfo> &dependency_infos = refresh_ctx_->dependency_infos_;
  const ObIArray<ObMLogInfo> &mlog_infos = refresh_ctx_->mlog_infos_;
  ObArray<ObDependencyInfo> previous_dependency_infos;
  WITH_MVIEW_TRANS_INNER_MYSQL_GUARD(trans)
  {
    if (OB_FAIL(ObDependencyInfo::collect_ref_infos(tenant_id, mview_id, trans,
                                                    previous_dependency_infos))) {
      LOG_WARN("fail to parse mview ref infos", KR(ret), K(tenant_id), K(mview_id));
    } else if (OB_UNLIKELY(previous_dependency_infos.count() != dependency_infos.count())) {
      ret = OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH;
      LOG_WARN("dependency num not match", KR(ret), K(dependency_infos),
               K(previous_dependency_infos));
    }
  }
  // check dependency consistent
  for (int64_t i = 0; OB_SUCC(ret) && i < dependency_infos.count(); ++i) {
    const ObDependencyInfo &dep = dependency_infos.at(i);
    const ObDependencyInfo &pre_dep = previous_dependency_infos.at(i);
    if (dep.get_ref_obj_id() != pre_dep.get_ref_obj_id()) {
      ret = OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH;
      LOG_WARN("dependency changed", KR(ret), K(i), K(dependency_infos),
               K(previous_dependency_infos));
    }
  }
  // check mlog
  for (int64_t i = 0; OB_SUCC(ret) && i < mlog_infos.count(); ++i) {
    const ObMLogInfo &mlog_info = mlog_infos.at(i);
    const ObDependencyInfo &dep = dependency_infos.at(i);
    if (ObObjectType::VIEW == dep.get_dep_obj_type()) {
      // bypass
    } else if (!mlog_info.is_valid()) {
      ret = OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH;
      LOG_WARN("table does not have mlog", KR(ret), K(i), K(dependency_infos));
    } else if (OB_UNLIKELY(mlog_info.get_last_purge_scn() >
                           refresh_ctx_->mview_info_.get_last_refresh_scn())) {
      ret = OB_ERR_MLOG_IS_YOUNGER;
      LOG_WARN("mlog is younger than last refresh", KR(ret), K(refresh_ctx_->mview_info_), K(i),
               K(mlog_info));
    }
  }
  return ret;
}

int ObMViewRefresher::complete_refresh()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = refresh_param_.tenant_id_;
  const uint64_t mview_id = refresh_param_.mview_id_;
  ObMViewTransaction &trans = *refresh_ctx_->trans_;
  ObMViewInfo &mview_info = refresh_ctx_->mview_info_;
  ObScnRange &refresh_scn_range = refresh_ctx_->refresh_scn_range_;
  ObSQLSessionInfo *session_info = nullptr;
  CK(OB_NOT_NULL(session_info = ctx_->get_my_session()));
  if (OB_SUCC(ret)) {
    obrpc::ObMViewCompleteRefreshArg arg;
    obrpc::ObMViewCompleteRefreshRes res;
    arg.tenant_id_ = tenant_id;
    arg.table_id_ = mview_id;
    arg.consumer_group_id_ = THIS_WORKER.get_group_id();
    arg.session_id_ = session_info->get_sessid_for_table();
    arg.parallelism_ = refresh_param_.parallelism_;
    arg.sql_mode_ = session_info->get_sql_mode();
    arg.last_refresh_scn_ = refresh_ctx_->refresh_scn_range_.start_scn_;
    arg.tz_info_ = session_info->get_tz_info_wrap().get_tz_info_offset();
    arg.nls_formats_[ObNLSFormatEnum::NLS_DATE] = session_info->get_local_nls_date_format();
    arg.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP] =
      session_info->get_local_nls_timestamp_format();
    arg.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP_TZ] =
      session_info->get_local_nls_timestamp_tz_format();
    arg.exec_tenant_id_ = tenant_id;
    if (OB_FAIL(arg.tz_info_wrap_.deep_copy(session_info->get_tz_info_wrap()))) {
      LOG_WARN("failed to deep copy tz_info_wrap", KR(ret));
    } else if (OB_FAIL(
                 arg.based_schema_object_infos_.assign(refresh_ctx_->based_schema_object_infos_))) {
      LOG_WARN("fail to assign based schema object infos", KR(ret));
    }
    if (OB_SUCC(ret)) {
      const int64_t DEFAULT_TIMEOUT_US = GCONF.internal_sql_execute_timeout;
      ObTimeoutCtx timeout_ctx;
      ObAddr rs_addr;
      if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, DEFAULT_TIMEOUT_US))) {
        LOG_WARN("fail to set default timeout ctx", KR(ret));
      } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
        LOG_WARN("fail to rootservice address", KR(ret));
      } else {
        LOG_INFO("mview complete refresh start", K(rs_addr), K(arg));
        if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr)
                      .timeout(timeout_ctx.get_timeout())
                      .mview_complete_refresh(arg, res))) {
          LOG_WARN("fail to mview complete refresh", KR(ret), K(arg));
        } else if (OB_FAIL(ObDDLExecutorUtil::wait_ddl_finish(tenant_id, res.task_id_, DDL_MVIEW_COMPLETE_REFRESH, session_info,
                                                              GCTX.rs_rpc_proxy_))) {
          LOG_WARN("fail to wait mview complete refresh finish", KR(ret));
        } else {
          LOG_INFO("mview complete refresh success", K(arg), K(res));
        }
      }
    }
  }
  // refetch mview info
  if (OB_SUCC(ret)) {
    WITH_MVIEW_TRANS_INNER_MYSQL_GUARD(trans)
    {
      if (OB_FAIL(ObMViewInfo::fetch_mview_info(trans, tenant_id, mview_id, mview_info))) {
        LOG_WARN("fail to fetch mview info", KR(ret), K(tenant_id), K(mview_id));
      } else if (OB_FAIL(refresh_scn_range.end_scn_.convert_for_inner_table_field(
                   mview_info.get_last_refresh_scn()))) {
        LOG_WARN("fail to convert for inner table field", KR(ret), K(mview_info));
      }
    }
  }
  return ret;
}

int ObMViewRefresher::fast_refresh()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = refresh_param_.tenant_id_;
  const uint64_t mview_id = refresh_param_.mview_id_;
  const int64_t parallelism = refresh_param_.parallelism_;
  const int64_t start_time = ObTimeUtil::current_time();
  ObMViewTransaction &trans = *refresh_ctx_->trans_;
  ObMViewInfo &mview_info = refresh_ctx_->mview_info_;
  const ObIArray<ObDependencyInfo> &dependency_infos = refresh_ctx_->dependency_infos_;
  const ObIArray<ObMLogInfo> &mlog_infos = refresh_ctx_->mlog_infos_;
  const ObScnRange &refresh_scn_range = refresh_ctx_->refresh_scn_range_;
  const ObIArray<ObString> &refresh_sqls = refresh_ctx_->refresh_sqls_;
  ObInnerSQLConnection *conn = nullptr;
  sql::ObSQLSessionInfo *exec_session_info = nullptr;
  int64_t affected_rows = 0;
  uint64_t data_version = 0;
  bool has_updated_dml_dop = false;
  uint64_t orig_dml_dop = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data_version", KR(ret));
  } else if (OB_ISNULL(conn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("connection can not be NULL", KR(ret));
  } else {
    exec_session_info = &conn->get_session();
  }
  // lock mview info record
  if (OB_SUCC(ret)) {
    WITH_MVIEW_TRANS_INNER_MYSQL_GUARD(trans)
    {
      if (OB_FAIL(ObMViewInfo::fetch_mview_info(trans, tenant_id, mview_id, mview_info,
                                                true /*for_update*/))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          LOG_WARN("fail to fetch mview info", KR(ret), K(tenant_id), K(mview_id));
        } else {
          ret = OB_ERR_MVIEW_NOT_EXIST;
          LOG_WARN("mview may dropped", KR(ret), K(tenant_id), K(mview_id));
        }
      } else if (OB_UNLIKELY(mview_info.get_last_refresh_scn() !=
                             refresh_scn_range.start_scn_.get_val_for_inner_table_field())) {
        ret = OB_VERSION_NOT_MATCH;
        LOG_WARN("mview version is old", KR(ret), K(refresh_scn_range), K(mview_info));
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(set_session_dml_dop_(tenant_id, data_version, exec_session_info,
                                                   trans, parallelism, has_updated_dml_dop,
                                                   orig_dml_dop))) {
    LOG_WARN("failed to set session dml dop", KR(ret));
  }

  // exec sqls
  for (int64_t i = 0; OB_SUCC(ret) && OB_SUCC(ctx_->check_status()) && i < refresh_sqls.count();
       ++i) {
    const ObString &fast_refresh_sql = refresh_sqls.at(i);
    const int64_t exec_start_time = ObTimeUtil::current_time();
    if (OB_FAIL(trans.write(tenant_id, fast_refresh_sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute write", KR(ret), K(fast_refresh_sql));
    }
    const int64_t exec_end_time = ObTimeUtil::current_time();
    // collect stmt stats
    if (OB_SUCC(ret) && nullptr != refresh_stats_collection_) {
      const int64_t execution_time = (exec_end_time - exec_start_time) / 1000 / 1000;
      if (OB_FAIL(refresh_stats_collection_->collect_stmt_stats(*refresh_ctx_, fast_refresh_sql,
                                                                execution_time))) {
        LOG_WARN("fail to collect stmt stats", KR(ret));
      }
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(restore_session_dml_dop_(tenant_id, data_version, has_updated_dml_dop,
                                           orig_dml_dop, trans))) {
    LOG_WARN("failed to restore session dml dop", KR(ret), K(has_updated_dml_dop), K(orig_dml_dop));
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }

  const int64_t end_time = ObTimeUtil::current_time();
  // check mlogs avaiable
  if (OB_SUCC(ret)) {
    WITH_MVIEW_TRANS_INNER_MYSQL_GUARD(trans)
    {
      for (int64_t i = 0; OB_SUCC(ret) && i < mlog_infos.count(); ++i) {
        const ObMLogInfo &mlog_info = mlog_infos.at(i);
        const ObDependencyInfo &dep = dependency_infos.at(i);
        ObMLogInfo new_mlog_info;
        if (ObObjectType::VIEW == dep.get_dep_obj_type()) {
          // bypass
        } else if (OB_UNLIKELY(!mlog_info.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected invalid mlog", KR(ret), K(i), K(mlog_infos));
        } else if (OB_FAIL(ObMLogInfo::fetch_mlog_info(trans, tenant_id, mlog_info.get_mlog_id(),
                                                       new_mlog_info))) {
          if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
            LOG_WARN("fail to fetch mlog info", KR(ret), K(mlog_info));
          } else {
            ret = OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH;
            LOG_WARN("mlog may dropped during refreshing", KR(ret), K(mlog_info));
          }
        } else if (OB_UNLIKELY(new_mlog_info.get_last_purge_scn() >
                               mview_info.get_last_refresh_scn())) {
          ret = OB_ERR_MLOG_IS_YOUNGER;
          LOG_WARN("mlog is younger than last refresh", KR(ret), K(mview_info), K(i),
                   K(new_mlog_info));
        }
      }
    }
  }
  // update mview last refresh info
  if (OB_SUCC(ret)) {
    WITH_MVIEW_TRANS_INNER_MYSQL_GUARD(trans)
    {
      mview_info.set_last_refresh_scn(refresh_scn_range.end_scn_.get_val_for_inner_table_field());
      mview_info.set_last_refresh_type(ObMVRefreshType::FAST);
      mview_info.set_last_refresh_date(start_time);
      mview_info.set_last_refresh_time((end_time - start_time) / 1000 / 1000);
      char trace_id_buf[OB_MAX_TRACE_ID_BUFFER_SIZE] = {'\0'};
      if (OB_FAIL(mview_info.set_last_refresh_trace_id(ObCurTraceId::get_trace_id_str(trace_id_buf, sizeof(trace_id_buf))))) {
        LOG_WARN("fail to set last refresh trace id", KR(ret));
      } else if (OB_FAIL(ObMViewInfo::update_mview_last_refresh_info(trans, mview_info))) {
        LOG_WARN("fail to update mview last refresh info", KR(ret), K(mview_info));
      }
    }
  }
  return ret;
}

int ObMViewRefresher::set_session_dml_dop_(const uint64_t tenant_id,
                                           const uint64_t data_version,
                                           sql::ObSQLSessionInfo *exec_session_info,
                                           ObMViewTransaction &trans,
                                           const int64_t parallelism,
                                           bool &has_updated_dml_dop,
                                           uint64_t &orig_dml_dop)
{
  int ret = OB_SUCCESS;
  has_updated_dml_dop = false;
  orig_dml_dop = 0;
  const bool is_oracle_mode = refresh_ctx_->is_oracle_mode_;

  if (OB_ISNULL(exec_session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec session info is null", KR(ret));
  } else if (data_version >= DATA_VERSION_4_3_5_1) {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(exec_session_info->get_force_parallel_dml_dop(orig_dml_dop))) {
      LOG_WARN("fail to get force parallel dml dop", KR(ret));
    } else if (is_oracle_mode &&
               OB_FAIL(sql.assign_fmt("SET \"_force_parallel_dml_dop\" = %lu", parallelism))) {
      LOG_WARN("fail to assign sql", KR(ret), K(parallelism));
    } else if (!is_oracle_mode &&
               OB_FAIL(sql.assign_fmt("SET _force_parallel_dml_dop = %lu", parallelism))) {
      LOG_WARN("fail to assign sql", KR(ret), K(parallelism));
    } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to set force parallel dml dop", KR(ret), K(sql));
    } else {
      has_updated_dml_dop = true;
    }
  }

  return ret;
}

int ObMViewRefresher::restore_session_dml_dop_(const uint64_t tenant_id,
                                               const uint64_t data_version,
                                               const bool has_updated_dml_dop,
                                               const uint64_t orig_dml_dop,
                                               ObMViewTransaction &trans)
{
  int ret = OB_SUCCESS;
  const bool is_oracle_mode = refresh_ctx_->is_oracle_mode_;

  if (has_updated_dml_dop && data_version >= DATA_VERSION_4_3_5_1) {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (is_oracle_mode &&
        OB_FAIL(sql.assign_fmt("SET \"_force_parallel_dml_dop\" = %lu", orig_dml_dop))) {
      LOG_WARN("fail to assign sql", KR(ret), K(orig_dml_dop));
    } else if (!is_oracle_mode &&
               OB_FAIL(sql.assign_fmt("SET _force_parallel_dml_dop = %lu", orig_dml_dop))) {
      LOG_WARN("fail to assign sql", KR(ret), K(orig_dml_dop));
    } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to set force parallel dml dop", KR(ret), K(sql));
    }
  }

  return ret;
}

int ObMViewRefresher::calc_mv_refresh_parallelism(int64_t explict_parallelism,
                                                  sql::ObSQLSessionInfo *session_info,
                                                  int64_t &final_parallelism)
{
  int ret = OB_SUCCESS;
  int64_t session_parallelism = 0;
  final_parallelism = 1;

  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session info is null", KR(ret));
  } else if (explict_parallelism < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parallelism", KR(ret), K(explict_parallelism));
  } else if (explict_parallelism != 0) {
    final_parallelism = explict_parallelism;
  } else if (OB_FAIL(session_info->get_mview_refresh_dop(session_parallelism))) {
    LOG_WARN("fail to get materialized view parallelism", KR(ret));
  } else if (session_parallelism < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session parallelism is invalid", KR(ret), K(session_parallelism));
  } else if (session_parallelism != 0) {
    final_parallelism = session_parallelism;
  } else {
    final_parallelism = 1;
  }

  LOG_INFO("calc mv refresh parallelism", KR(ret), K(explict_parallelism), K(session_parallelism),
           K(final_parallelism), K(session_info->get_sessid()));

  return ret;
}

} // namespace storage
} // namespace oceanbase
