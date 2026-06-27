/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/mview/ob_mview_refresh.h"
#include "share/ob_cluster_version.h"
#include "share/schema/ob_mview_refresh_stats_params.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "sql/ob_sql_utils.h"
#include "lib/hash/ob_hashmap.h"
#include "sql/resolver/mv/ob_mv_provider.h"
#include "sql/resolver/mv/ob_mv_compat_control.h"
#include "storage/mview/ob_mview_refresh_helper.h"
#include "storage/mview/ob_mview_refresh_stats_utils.h"
#include "storage/mview/ob_mview_mds.h"
#include "rootserver/mview/ob_mview_utils.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "sql/optimizer/ob_dynamic_sampling.h"
#include "sql/resolver/mv/ob_mv_dep_utils.h"
#include "storage/mview/ob_mlog_purge.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace observer;
using namespace share;
using namespace rootserver;
using namespace share::schema;
using namespace sql;

/**
 * ObMViewRefresher
 */

ObMViewRefresher::ObMViewRefresher(ObExecContext &ctx,
                                   const ObMViewRefreshParam &param)
  : ctx_(ctx),
    param_(param),
    trans_(),
    collection_level_(ObMVRefreshStatsCollectionLevel::MAX),
    complete_refresh_ratio_threshold_(0.0),
    enable_adaptive_refresh_step_(false),
    refresh_parallel_(0),
    mview_info_(),
    mlog_infos_(),
    dependency_infos_(),
    base_table_scn_range_(),
    mview_refresh_scn_range_()
{}

ObMViewRefresher::~ObMViewRefresher() {}

ERRSIM_POINT_DEF(ERRSIM_MVIEW_REFRESH)
int ObMViewRefresher::refresh()
{
  int ret = OB_SUCCESS;
  ObMVRefreshType refresh_type = ObMVRefreshType::MAX;
  ObArenaAllocator allocator("MViewRefresh");
  allocator.set_tenant_id(param_.tenant_id_);
  ObSEArray<ObString, 4> fast_refresh_sqls;
  const ObDatabaseSchema *database_schema = NULL;
  const ObTableSchema *mv_schema = NULL;
  const int64_t start_time = ObTimeUtil::current_time();
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(param_.tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(param_.tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(param_.tenant_id_, param_.mview_id_, mv_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(param_.tenant_id_), K(param_.mview_id_));
  } else if (OB_ISNULL(mv_schema) || OB_UNLIKELY(!mv_schema->is_materialized_view())) {
    ret = OB_ERR_MVIEW_NOT_EXIST;
    LOG_WARN("mview not exist", KR(ret), K(param_.tenant_id_), K(param_.mview_id_));
  } else if (OB_FAIL(schema_guard.get_database_schema(param_.tenant_id_, mv_schema->get_database_id(), database_schema))) {
    LOG_WARN("fail to get database schema", KR(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema is nullptr", KR(ret));
  } else if (OB_FAIL(trans_.start(ctx_.get_my_session(),
                                 ctx_.get_sql_proxy(),
                                 database_schema->get_database_id(),
                                 database_schema->get_database_name_str(),
                                 &mv_schema->get_local_session_var()))) {
    LOG_WARN("fail to start trans", KR(ret), K(database_schema->get_database_id()),
                                  K(database_schema->get_database_name_str()));
  } else if (OB_FAIL(lock_mview_for_refresh(trans_))) {
    LOG_WARN("fail to lock mview for refresh", KR(ret));
  } else if (OB_FAIL(prepare_for_refresh(allocator, refresh_type, fast_refresh_sqls))) {
    LOG_WARN("fail to prepare for refresh", KR(ret));
  } else if (OB_FAIL(ObMViewRefreshStatsUtils::write_mv_start(ctx_,
                                                              trans_,
                                                              param_,
                                                              collection_level_,
                                                              refresh_type,
                                                              start_time,
                                                              fast_refresh_sqls.count()))) {
    LOG_WARN("fail to write_mv_start", KR(ret));
  } else if (OB_UNLIKELY(ObMVRefreshType::FAST != refresh_type && ObMVRefreshType::COMPLETE != refresh_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected refresh type", KR(ret), K(refresh_type));
  } else if (ObMVRefreshType::FAST == refresh_type && OB_FAIL(fast_refresh(fast_refresh_sqls))) {
    LOG_WARN("fail to fast refresh", KR(ret));
  } else if (ObMVRefreshType::COMPLETE == refresh_type && OB_FAIL(complete_refresh())) {
    LOG_WARN("fail to complete refresh", KR(ret));
  }

  if (OB_ERR_TASK_SKIPPED == ret) {
    ret = OB_SUCCESS;
    LOG_INFO("skip this refresh", K(ret), K(param_));
  }
  LOG_INFO("mview refresh finish", KR(ret), K(param_));

  const int64_t end_time = ObTimeUtil::current_time();
  const int64_t elapsed_time = end_time - start_time;
  int tmp_ret = OB_SUCCESS;
  // write_mv_end must be called before trans_.end() because it queries final_num_rows
  // via trans.read(); after trans_.end() the connection is closed.
  if (OB_TMP_FAIL(ObMViewRefreshStatsUtils::write_mv_end(ctx_,
                                                         trans_,
                                                         param_,
                                                         collection_level_,
                                                         end_time,
                                                         elapsed_time,
                                                         ret,
                                                         mview_refresh_scn_range_))) {
    LOG_WARN("fail to write_mv_end", KR(tmp_ret));
  }

  if (trans_.is_started()) {
    if (OB_SUCCESS != (tmp_ret = trans_.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }

  const int64_t start_purge_time = ObTimeUtil::current_time();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(purge_mlog(mlog_infos_, dependency_infos_))) {
    LOG_WARN("fail to purge mlog", KR(ret));
  }
  const int64_t log_purge_time = (ObTimeUtil::current_time() - start_purge_time) / 1000 / 1000;

#ifdef ERRSIM
  if (OB_SUCC(ret) && OB_FAIL(ERRSIM_MVIEW_REFRESH)) {
    LOG_WARN("errsim mview refresh", K(ret));
  }
#endif
  return ret;
}

int ObMViewRefresher::calc_mv_refresh_parallelism(const int64_t refresh_param_dop,
                                                  const int64_t mview_info_dop,
                                                  int64_t &refresh_parallelism)
{
  int ret = OB_SUCCESS;
  refresh_parallelism = 0;
  uint64_t session_var_refresh_dop = 0;
  if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret));
  } else if (refresh_param_dop > 0) {
    refresh_parallelism = refresh_param_dop;
  } else if (mview_info_dop > 0) {
    refresh_parallelism = mview_info_dop;
  } else if (OB_FAIL(ctx_.get_my_session()->get_mview_refresh_dop(session_var_refresh_dop))) {
    LOG_WARN("fail to get mview refresh dop from session variable", K(ret));
  } else if (session_var_refresh_dop > 0) {
    refresh_parallelism = session_var_refresh_dop;
  } else {
    refresh_parallelism = 1;  // defualt parallelism is 1
  }
  LOG_TRACE("calc mv refresh parallelism", K(refresh_param_dop), K(mview_info_dop), K(session_var_refresh_dop), K(refresh_parallelism));
  return ret;
}

int ObMViewRefresher::calc_create_mv_refresh_parallelism(ObSchemaGetterGuard &schema_guard,
                                                         const uint64_t tenant_id,
                                                         const int64_t ddl_parallel_hint,
                                                         const int64_t refresh_dop,
                                                         int64_t &refresh_parallelism)
{
  int ret = OB_SUCCESS;
  refresh_parallelism = 0;
  uint64_t session_var_refresh_dop = 0;
  const ObSysVarSchema *mview_refresh_dop_schema = nullptr;
  if (ddl_parallel_hint > 0) {
    refresh_parallelism = ddl_parallel_hint;
  } else if (refresh_dop > 0) {
    refresh_parallelism = refresh_dop;
  } else if (OB_FAIL(schema_guard.get_tenant_system_variable(tenant_id,
                                                             share::SYS_VAR_MVIEW_REFRESH_DOP,
                                                             mview_refresh_dop_schema))) {
    LOG_WARN("fail to get tenant system variable mview_refresh_dop", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(mview_refresh_dop_schema)) {
    refresh_parallelism = 1;
  } else {
    ObArenaAllocator allocator("MViewRefresh");
    ObObj value_obj;
    if (OB_FAIL(mview_refresh_dop_schema->get_value(&allocator, NULL, value_obj))) {
      LOG_WARN("fail to get value of mview_refresh_dop", KR(ret));
    } else {
      session_var_refresh_dop = value_obj.get_uint64();
      if (session_var_refresh_dop > 0) {
        refresh_parallelism = session_var_refresh_dop;
      } else {
        refresh_parallelism = 1;
      }
    }
  }
  LOG_TRACE("calc create mv refresh parallelism", K(ddl_parallel_hint), K(refresh_dop),
            K(session_var_refresh_dop), K(refresh_parallelism));
  return ret;
}

// purge mlog, to removed in future
int ObMViewRefresher::purge_mlog(const ObIArray<ObMLogInfo> &mlog_infos,
                                 const ObIArray<ObDependencyInfo> &dependency_infos)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < mlog_infos.count(); ++i) {
    const ObMLogInfo &mlog_info = mlog_infos.at(i);
    const ObDependencyInfo &dep = dependency_infos.at(i);
    if (!mlog_info.is_valid()) {
      // table no mlog, ignore
    } else if (ObMLogPurgeMode::DEFERRED == mlog_info.get_purge_mode()) {
      // ignore
    } else if (ObMLogPurgeMode::IMMEDIATE_ASYNC == mlog_info.get_purge_mode()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("mlog purge immediate async not supported", KR(ret), K(mlog_info));
    } else if (ObMLogPurgeMode::IMMEDIATE_SYNC == mlog_info.get_purge_mode()) {
      ObMLogPurgeParam purge_param;
      ObMLogPurger purger;
      purge_param.tenant_id_ = param_.tenant_id_;
      purge_param.master_table_id_ = dep.get_ref_obj_id();
      purge_param.purge_log_parallel_ = refresh_parallel_;
      if (OB_TMP_FAIL(purger.init(ctx_, purge_param, true /*called_in_refresh*/))) {
        LOG_WARN("fail to init mlog purger", KR(tmp_ret), K(purge_param));
      } else if (OB_TMP_FAIL(purger.purge())) {
        LOG_WARN("fail to do purge", KR(tmp_ret), K(purge_param));
      }
    }
  }
  return ret;
}

int ObMViewRefresher::lock_mview_for_refresh(ObMViewTransaction &trans) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = param_.tenant_id_;
  const uint64_t mview_id = param_.mview_id_;
  int64_t retries = 0;
  while (OB_SUCC(ret) && OB_SUCC(ctx_.check_status())) {
    if (OB_FAIL(ObMViewRefreshHelper::lock_mview(trans, param_.tenant_id_,
                                                 param_.mview_id_,
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
  DEBUG_SYNC(AFTER_LOCK_MVIEW_IN_REFRESH);
  return ret;
}

int ObMViewRefresher::prepare_for_refresh(ObIAllocator &allocator,
                                          ObMVRefreshType &refresh_type,
                                          ObIArray<ObString> &fast_refresh_sqls)
{
  int ret = OB_SUCCESS;
  refresh_type = ObMVRefreshType::MAX;
  fast_refresh_sqls.reuse();
  ObSEArray<uint64_t, 8> tables_need_mlog;
  ObSEArray<uint64_t, 4> tables_without_delete;
  ObSEArray<uint64_t, 4> tables_without_insert;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *mview_table_schema = NULL;
  ObMVProvider mv_provider(param_.mview_id_);
  bool can_fast_refresh = false;
  bool reached_complete_refresh_threshold = false;
  complete_refresh_ratio_threshold_ = (!param_.is_consistent_refresh_ && tenant_config.is_valid() && ObMVRefreshMethod::MAX == param_.refresh_method_)
                                      ? static_cast<double>(tenant_config->_mv_adaptive_complete_refresh_threshold.get()) / 100.0
                                      : 0.0;
  enable_adaptive_refresh_step_ = tenant_config.is_valid() && tenant_config->_enable_mv_adaptive_refresh_step;
  ObMVRefreshMethod refresh_method = param_.refresh_method_;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(param_.tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(param_.tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(param_.tenant_id_, param_.mview_id_, mview_table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(param_.tenant_id_), K(param_.mview_id_));
  } else if (OB_ISNULL(mview_table_schema)) {
    ret = OB_ERR_MVIEW_NOT_EXIST;
    LOG_WARN("mview not exist", KR(ret), K(param_.tenant_id_), K(param_.mview_id_));
  } else if (OB_FAIL(fetch_mview_info(trans_, mview_info_))) {
    LOG_WARN("fail to fetch mview info and dep infos", KR(ret));
  } else if (enable_adaptive_refresh_step_ &&
             OB_FAIL(ObMVCompatControl::check_feature_enable(mview_info_.get_compat_version(),
                                                             ObMVCompatFeatureType::ADAPTIVE_REFRESH_STEP,
                                                             enable_adaptive_refresh_step_))) {
    LOG_WARN("fail to check mv compat feature", KR(ret));
  } else if (ObMVRefreshMode::NEVER == mview_info_.get_refresh_mode()) {
    ret = OB_ERR_MVIEW_NEVER_REFRESH;
    LOG_WARN("mview never refresh", KR(ret), K(mview_info_));
  } else if (OB_FAIL(get_and_check_refresh_scn(trans_,
                                               mview_refresh_scn_range_,
                                               base_table_scn_range_))) {
    LOG_WARN("fail to get and check refresh scn", KR(ret));
  } else if (OB_FAIL(calc_mv_refresh_parallelism(param_.parallel_,
                                                 mview_info_.get_refresh_dop(),
                                                 refresh_parallel_))) {
    LOG_WARN("fail to calc mv refresh parallelism", KR(ret));
  } else if (OB_FAIL(fetch_mview_refresh_stats_collection_level(trans_,
                                                                param_.tenant_id_,
                                                                param_.mview_id_,
                                                                collection_level_))) {
    LOG_WARN("fail to fetch mview refresh stats collection level", KR(ret));
  } else if (OB_FAIL(mv_provider.get_mlog_mv_refresh_infos(ctx_.get_my_session(),
                                                           &schema_guard,
                                                           mview_info_.get_refresh_method(),
                                                           dependency_infos_,
                                                           tables_need_mlog,
                                                           can_fast_refresh))) {
    LOG_WARN("fail to get mlog mv refresh infos", KR(ret), K(param_.tenant_id_));
  } else if (OB_FAIL(fetch_based_infos(trans_,
                                       schema_guard,
                                       tables_need_mlog,
                                       mlog_infos_))) {
    LOG_WARN("fail to fetch based infos", KR(ret));
  } else if (OB_FAIL(collect_and_check_detail_table_changes(trans_, schema_guard,
                                                            tables_need_mlog,
                                                            tables_without_delete,
                                                            tables_without_insert,
                                                            reached_complete_refresh_threshold))) {
    LOG_WARN("fail to collect and check detail table changes", KR(ret));
  } else if (OB_FALSE_IT(refresh_method = ObMVRefreshMethod::MAX == refresh_method ? mview_info_.get_refresh_method() : refresh_method)) {
  } else if (ObMVRefreshMethod::COMPLETE == refresh_method
             || (!can_fast_refresh && ObMVRefreshMethod::FORCE == refresh_method)
             || reached_complete_refresh_threshold) {
    refresh_type = ObMVRefreshType::COMPLETE;
    LOG_INFO("using complete refresh", K(refresh_method), K(can_fast_refresh), K(reached_complete_refresh_threshold));
  } else if (OB_UNLIKELY(!can_fast_refresh && ObMVRefreshMethod::FAST == refresh_method)) {
    ret = OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH;
    LOG_WARN("mv can not fast refresh", KR(ret));
    LOG_USER_ERROR(OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH, mview_table_schema->get_table_name(),
                   mv_provider.get_error_str().ptr());
  } else {
    refresh_type = ObMVRefreshType::FAST;
    ObMVPrinterRefreshInfo refresh_info(base_table_scn_range_,
                                        mview_refresh_scn_range_,
                                        tables_without_delete,
                                        tables_without_insert,
                                        mview_info_.get_compat_version());
    if (OB_FAIL(mv_provider.print_mv_operators(ctx_.get_my_session(),
                                               &refresh_info,
                                               allocator,
                                               fast_refresh_sqls))) {
      LOG_WARN("fail to print mv operators", KR(ret));
    }
  }
  return ret;
}

int ObMViewRefresher::fetch_mview_info(ObMViewTransaction &trans, ObMViewInfo &mview_info) const
{
  int ret = OB_SUCCESS;
  WITH_MVIEW_TRANS_INNER_MYSQL_GUARD(trans)
  {
    if (OB_FAIL(ObMViewInfo::fetch_mview_info(trans,
                                              param_.tenant_id_,
                                              param_.mview_id_,
                                              mview_info))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to fetch mview info", KR(ret), K(param_));
      } else {
        ret = OB_ERR_MVIEW_NOT_EXIST;
        LOG_WARN("mview may dropped", KR(ret), K(param_));
      }
    } else if (!mview_info.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid mview info", K(ret), K(mview_info));
    }
  }
  return ret;
}

int ObMViewRefresher::fetch_mview_refresh_stats_collection_level(ObMViewTransaction &trans,
                                                                 const uint64_t tenant_id,
                                                                 const uint64_t mview_id,
                                                                 ObMVRefreshStatsCollectionLevel &collection_level) const
{
  int ret = OB_SUCCESS;
  collection_level = ObMVRefreshStatsCollectionLevel::MAX;
  WITH_MVIEW_TRANS_INNER_MYSQL_GUARD(trans)
  {
    ObMViewRefreshStatsParams stats_params;
    if (OB_FAIL(ObMViewRefreshStatsParams::fetch_mview_refresh_stats_params(trans,
                                                                            tenant_id,
                                                                            mview_id,
                                                                            stats_params,
                                                                            true /* with_sys_defaults */))) {
      LOG_WARN("fail to fetch mview refresh stats params", KR(ret), K(tenant_id), K(mview_id));
    } else {
      collection_level = stats_params.get_collection_level();
    }
  }
  return ret;
}

bool ObMViewRefresher::reached_detail_complete_refresh_threshold(const ObMViewDetailTableChangeStats &data) const
{
  const int64_t num_mlog_rows = data.num_rows_ins_ + data.num_rows_upd_ + data.num_rows_del_;
  const int64_t num_base_table_rows = data.num_rows_ - data.num_rows_ins_ + data.num_rows_del_;
  return complete_refresh_ratio_threshold_ > 0 && num_base_table_rows != 0 && num_mlog_rows > 500
      && static_cast<double>(num_mlog_rows) / num_base_table_rows > complete_refresh_ratio_threshold_;
}

// compute mview/base scn ranges and short-circuit if this mview already reached target
int ObMViewRefresher::get_and_check_refresh_scn(ObMViewTransaction &trans,
                                                ObScnRange &mview_refresh_scn_range,
                                                ObScnRange &base_table_scn_range) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = param_.tenant_id_;
  const uint64_t mview_id = param_.mview_id_;
  const ObMViewInfo &mview_info = mview_info_;
  const SCN &target_data_sync_scn = param_.target_data_sync_scn_;
  SCN current_scn;
  uint64_t data_version = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_INVALID_SCN_VAL == mview_info.get_last_refresh_scn() ||
             OB_INVALID_SCN_VAL == mview_info.get_data_sync_scn()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("last refresh scn is invalid", KR(ret), K(mview_info));
  } else if (OB_FAIL(mview_refresh_scn_range.start_scn_.convert_for_inner_table_field(mview_info.get_last_refresh_scn()))) {
    LOG_WARN("fail to convert for inner table field", KR(ret), K(mview_info));
  } else if (OB_FAIL(ObMViewRefreshHelper::get_current_scn(current_scn))) {
    LOG_WARN("fail to get current scn", KR(ret));
  } else if ((data_version >= MOCK_DATA_VERSION_4_3_5_3 && data_version < DATA_VERSION_4_4_0_0)
             || (data_version >= MOCK_DATA_VERSION_4_4_2_0 && data_version < DATA_VERSION_4_5_0_0)
             || (data_version >= DATA_VERSION_4_6_1_0)) {
    // valid data sync scn means need sync refresh
    bool satisfy = false;
    if (OB_FAIL(ObMViewInfo::check_satisfy_target_data_sync_scn(mview_info,
                                                                target_data_sync_scn.get_val_for_inner_table_field(),
                                                                satisfy))) {
      LOG_WARN("fail to satisfy target data sync scn", K(ret));
    } else if (satisfy) {
      ret = OB_ERR_TASK_SKIPPED;
      LOG_INFO("curr mview satisfied this target scn, skip refresh task", K(ret), K(mview_info), K(target_data_sync_scn));
    } else if (OB_FAIL(base_table_scn_range.start_scn_.convert_for_inner_table_field(mview_info.get_data_sync_scn()))) {
      LOG_WARN("fail to convert for inner table field", KR(ret), K(mview_info));
    } else {
      mview_refresh_scn_range.end_scn_ = current_scn;
      base_table_scn_range.end_scn_ = target_data_sync_scn;
    }
    LOG_INFO("calc mview scn range", K(mview_refresh_scn_range), K(base_table_scn_range));
  } else {
    // data version < 4.3.5.3 not need target scn
    mview_refresh_scn_range.end_scn_ = current_scn;
    base_table_scn_range.start_scn_ = mview_refresh_scn_range.start_scn_;
    base_table_scn_range.end_scn_ = mview_refresh_scn_range.end_scn_;
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(!mview_refresh_scn_range.is_valid()
                                  || !base_table_scn_range.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("calc wrong scn range", K(ret), K(mview_refresh_scn_range), K(base_table_scn_range));
  }
  return ret;
}

int ObMViewRefresher::collect_and_check_detail_table_changes(ObMViewTransaction &trans,
                                                            ObSchemaGetterGuard &schema_guard,
                                                            const ObIArray<uint64_t> &tables_need_mlog,
                                                            ObIArray<uint64_t> &tables_without_delete,
                                                            ObIArray<uint64_t> &tables_without_insert,
                                                            bool &reached_complete_refresh_threshold) const
{
  int ret = OB_SUCCESS;
  reached_complete_refresh_threshold = false;
  ObWarningBufferIgnoreScope ignore_warning_guard;
  const bool enable_adaptive_refresh = complete_refresh_ratio_threshold_ > 0;
  if (dependency_infos_.empty()) {
    /* do nothing */
  } else if (collection_level_ >= ObMVRefreshStatsCollectionLevel::TYPICAL
             || enable_adaptive_refresh || enable_adaptive_refresh_step_) {
    common::hash::ObHashMap<uint64_t, uint64_t> ct_to_mv;
    sql::ObSQLSessionInfo *exec_session = trans.get_session_info();
    if (OB_NOT_NULL(exec_session)) {
      // set mlog expected output rows for optimizer
      exec_session->reset_mlog_expected_rows();
    }
    if (OB_FAIL(ct_to_mv.create(dependency_infos_.count(), "CtToMv"))) {
      LOG_WARN("fail to create ct_to_mv map", KR(ret));
    }
    for (int64_t vi = 0; OB_SUCC(ret) && vi < dependency_infos_.count(); ++vi) {
      const ObDependencyInfo &view_dep = dependency_infos_.at(vi);
      const ObTableSchema *view_schema = nullptr;
      if (view_dep.get_ref_obj_type() != ObObjectType::VIEW) {
      } else if (OB_FAIL(schema_guard.get_table_schema(param_.tenant_id_, view_dep.get_ref_obj_id(), view_schema))) {
        LOG_WARN("fail to get view schema", KR(ret), K(view_dep.get_ref_obj_id()));
      } else if (OB_NOT_NULL(view_schema) && view_schema->is_materialized_view()) {
        if (OB_FAIL(ct_to_mv.set_refactored(view_schema->get_data_table_id(), view_dep.get_ref_obj_id()))) {
          LOG_WARN("fail to set ct_to_mv map",
                   KR(ret),
                   K(view_schema->get_data_table_id()),
                   K(view_dep.get_ref_obj_id()));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dependency_infos_.count(); ++i) {
      const ObDependencyInfo &dep = dependency_infos_.at(i);
      const ObTableSchema *dep_schema = nullptr;
      ObScnRange tmp_scn_range;
      uint64_t detail_table_id = dep.get_ref_obj_id();
      const bool cur_table_need_mlog = ObOptimizerUtil::find_item(tables_need_mlog, dep.get_ref_obj_id());
      if (dep.get_ref_obj_type() != ObObjectType::TABLE) {

      } else if (OB_FAIL(schema_guard.get_table_schema(param_.tenant_id_, dep.get_ref_obj_id(), dep_schema))) {
        LOG_WARN("fail to get table schema", KR(ret), K(param_.tenant_id_));
      } else if (OB_ISNULL(dep_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", KR(ret), K(dep));
      } else if (dep_schema->mv_container_table()) {
        uint64_t mv_id = OB_INVALID_ID;
        if (OB_FAIL(ct_to_mv.get_refactored(dep.get_ref_obj_id(), mv_id))) {
          if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
            LOG_WARN("fail to get mv id from ct_to_mv map", KR(ret), K(dep.get_ref_obj_id()));
          }
          ret = OB_SUCCESS;
        } else {
          detail_table_id = mv_id;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_NOT_NULL(dep_schema)) {
        ObMViewDetailTableChangeStats detail_change(detail_table_id);
        if (OB_FALSE_IT(tmp_scn_range = dep_schema->mv_container_table() ? mview_refresh_scn_range_ : base_table_scn_range_)) {
        } else if ((collection_level_ >= ObMVRefreshStatsCollectionLevel::TYPICAL || enable_adaptive_refresh)
                   && OB_FAIL(ObMViewRefreshHelper::get_table_row_num(trans,
                                                                      param_.tenant_id_,
                                                                      dep.get_ref_obj_id(),
                                                                      tmp_scn_range.end_scn_,
                                                                      refresh_parallel_,
                                                                      detail_change.num_rows_))) {
          LOG_WARN("fail to get based table row num", KR(ret), K(dep));
        } else if (cur_table_need_mlog &&
                   OB_FAIL(ObMViewRefreshHelper::get_mlog_dml_row_num(trans,
                                                                      param_.tenant_id_,
                                                                      dep_schema->get_mlog_tid(),
                                                                      tmp_scn_range,
                                                                      refresh_parallel_,
                                                                      detail_change.num_rows_ins_,
                                                                      detail_change.num_rows_upd_,
                                                                      detail_change.num_rows_del_))) {
          LOG_WARN("fail to get mlog dml row num", KR(ret), K(dep_schema->get_mlog_tid()));
        } else if (collection_level_ >= ObMVRefreshStatsCollectionLevel::TYPICAL
                   && OB_FAIL(ObMViewRefreshStatsUtils::write_detail_table_change(ctx_, param_, detail_change))) {
          LOG_WARN("fail to push change stats", KR(ret), K(dep.get_ref_obj_id()));
        } else {
          reached_complete_refresh_threshold |= reached_detail_complete_refresh_threshold(detail_change);
          if (cur_table_need_mlog && OB_NOT_NULL(exec_session)) {
            const uint64_t mlog_id = dep_schema->get_mlog_tid();
            int64_t expected_rows = detail_change.num_rows_ins_
                                    + detail_change.num_rows_upd_ * 2
                                    + detail_change.num_rows_del_;
            expected_rows = (expected_rows > 0) ? expected_rows : 1;
            if (OB_INVALID_ID != mlog_id
                && OB_FAIL(exec_session->set_mlog_expected_rows(mlog_id, expected_rows))) {
              LOG_WARN("fail to set mlog expected rows", KR(ret), K(mlog_id), K(expected_rows));
            }
          }
          if (OB_SUCC(ret) && cur_table_need_mlog && enable_adaptive_refresh_step_) {
            if (detail_change.num_rows_del_ + detail_change.num_rows_upd_ == 0 &&
                OB_FAIL(tables_without_delete.push_back(dep.get_ref_obj_id()))) {
              LOG_WARN("fail to push back table without delete", KR(ret), K(dep.get_ref_obj_id()));
            } else if (detail_change.num_rows_ins_ + detail_change.num_rows_upd_ == 0 &&
                       OB_FAIL(tables_without_insert.push_back(dep.get_ref_obj_id()))) {
              LOG_WARN("fail to push back table without insert", KR(ret), K(dep.get_ref_obj_id()));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMViewRefresher::fetch_based_infos(ObMViewTransaction &trans,
                                        ObSchemaGetterGuard &schema_guard,
                                        const ObIArray<uint64_t> &tables_need_mlog,
                                        ObIArray<ObMLogInfo> &mlog_infos) const
{
  int ret = OB_SUCCESS;
  mlog_infos.reset();
  const uint64_t tenant_id = param_.tenant_id_;
  const ObIArray<ObDependencyInfo> &dependency_infos = dependency_infos_;
  WITH_MVIEW_TRANS_INNER_MYSQL_GUARD(trans)
  {
    ARRAY_FOREACH(dependency_infos, i) {
      const ObDependencyInfo &dep = dependency_infos.at(i);
      const ObTableSchema *based_table_schema = nullptr;
      const ObObjectType ref_obj_type = dep.get_ref_obj_type();
      uint64_t mlog_table_id = OB_INVALID_ID;
      const ObTableSchema *mlog_table_schema = nullptr;
      ObMLogInfo mlog_info;
      if (ObObjectType::FUNCTION == ref_obj_type || ObObjectType::TYPE == ref_obj_type) {
        // do nothing
      } else if (ObObjectType::TABLE != ref_obj_type && ObObjectType::VIEW != ref_obj_type) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid obj type", KR(ret), K(dep));
      } else if (!ObOptimizerUtil::find_item(tables_need_mlog, dep.get_ref_obj_id())) {
        // do nothing
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, dep.get_ref_obj_id(), based_table_schema))) {
        LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(dep));
      } else if (OB_ISNULL(based_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("based table not exist", KR(ret), K(tenant_id), K(dep));
      } else if (OB_UNLIKELY(OB_INVALID_ID == (mlog_table_id = based_table_schema->get_mlog_tid()))) {
        // do nothing
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, mlog_table_id, mlog_table_schema))) {
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
      } else if (OB_FAIL(ObMLogInfo::fetch_mlog_info(trans, tenant_id, mlog_table_id, mlog_info))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          LOG_WARN("fail to fetch mlog info", KR(ret), K(mlog_table_id));
        } else {
          ret = OB_SUCCESS;  // mlog may dropped
        }
      } else if (OB_FAIL(mlog_infos.push_back(mlog_info))) {
        LOG_WARN("fail to push back mlog info", KR(ret));
      }
    }
  }
  return ret;
}

int ObMViewRefresher::complete_refresh()
{
  int ret = OB_SUCCESS;
  LOG_INFO("mview complete refresh begin", K(param_));
  const uint64_t tenant_id = param_.tenant_id_;
  const uint64_t mview_id = param_.mview_id_;

  ObSQLSessionInfo *session_info = nullptr;
  CK(OB_NOT_NULL(session_info = ctx_.get_my_session()));
  obrpc::ObMViewCompleteRefreshArg arg;
  obrpc::ObMViewCompleteRefreshRes res;
  // prepare mview complete refresh arg
  if (OB_SUCC(ret)) {
    arg.tenant_id_ = tenant_id;
    arg.table_id_ = mview_id;
    arg.consumer_group_id_ = THIS_WORKER.get_group_id();
    arg.session_id_ = session_info->get_sessid_for_table();
    arg.parallelism_ = refresh_parallel_;
    arg.sql_mode_ = session_info->get_sql_mode();
    arg.target_data_sync_scn_ = param_.target_data_sync_scn_;
    arg.tz_info_ = session_info->get_tz_info_wrap().get_tz_info_offset();
    arg.nls_formats_[ObNLSFormatEnum::NLS_DATE] = session_info->get_local_nls_date_format();
    arg.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP] =
      session_info->get_local_nls_timestamp_format();
    arg.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP_TZ] =
      session_info->get_local_nls_timestamp_tz_format();
    arg.exec_tenant_id_ = tenant_id;
    uint64_t data_version = 0;
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *mview_table_schema = NULL;
    ObString select_sql;
    if (OB_FAIL(arg.last_refresh_scn_.convert_for_inner_table_field(mview_info_.get_last_refresh_scn()))) {
      LOG_WARN("fail to convert for inner table field", KR(ret), K(mview_info_));
    } else if (OB_FAIL(arg.tz_info_wrap_.deep_copy(session_info->get_tz_info_wrap()))) {
      LOG_WARN("failed to deep copy tz_info_wrap", KR(ret));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(collect_based_schema_object_infos(tenant_id,
                                                        data_version,
                                                        schema_guard,
                                                        dependency_infos_,
                                                        arg.based_schema_object_infos_,
                                                        arg.direct_dep_cnt_))) {
      LOG_WARN("fail to collect based schema object infos", KR(ret), K(tenant_id), K_(dependency_infos));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, mview_id, mview_table_schema))) {
      LOG_WARN("fail to get mview table schema", KR(ret), K(tenant_id), K(mview_id));
    } else if (OB_ISNULL(mview_table_schema)) {
      ret = OB_ERR_MVIEW_NOT_EXIST;
      LOG_WARN("mview not exist", KR(ret), K(tenant_id), K(mview_id));
    } else if (OB_FAIL(sql::ObMVProvider::get_complete_refresh_mview_str(*mview_table_schema,
                                                                         schema_guard,
                                                                         &mview_refresh_scn_range_.end_scn_,
                                                                         &base_table_scn_range_.end_scn_,
                                                                         arg.allocator_,
                                                                         select_sql))) {
      LOG_WARN("fail to generate mview select sql", KR(ret));
    } else {
      arg.select_sql_ = select_sql;
    }
  }
  // do mview complete refresh rpc
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
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObTimeoutCtx timeout_ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, OB_MAX_USER_SPECIFIED_TIMEOUT))) {
      LOG_WARN("fail to set default timeout ctx", KR(ret));
    } else if (OB_FAIL(ObDDLExecutorUtil::wait_ddl_finish(tenant_id,
                                                          res.task_id_,
                                                          false,
                                                          session_info,
                                                          GCTX.rs_rpc_proxy_))) {
      LOG_WARN("fail to wait mview complete refresh finish", KR(ret), K(arg));
    } else {
      LOG_INFO("mview complete refresh success", K(arg), K(res));
    }
  }
  return ret;
}

int ObMViewRefresher::fast_refresh(const ObIArray<ObString> &refresh_sqls)
{
  int ret = OB_SUCCESS;
  LOG_INFO("mview fast refresh begin", K(param_));
  const bool need_do_refresh = !refresh_sqls.empty();
  // do fast refresh when refresh SQLs are not empty
  const int64_t start_time = ObTimeUtil::current_time();
  if (!need_do_refresh) {
    // do nothing
  } else if (OB_FAIL(ObMViewRefreshStatsUtils::write_stmt_batch(ctx_, trans_.get_session_info(), param_, refresh_sqls))) {
    LOG_WARN("fail to write_stmt_batch", KR(ret));
  } else if (OB_FAIL(do_fast_refresh(refresh_sqls))) {
    LOG_WARN("failed to do fast refresh", KR(ret));
  }
  const int64_t end_time = ObTimeUtil::current_time();
  // update mview last refresh info
  WITH_MVIEW_TRANS_INNER_MYSQL_GUARD(trans_)
  {
    if (OB_FAIL(mview_info_.set_last_refresh_info(trans_,
                                                  param_.tenant_id_,
                                                  mview_refresh_scn_range_.end_scn_.get_val_for_inner_table_field(),
                                                  param_.target_data_sync_scn_.get_val_for_inner_table_field(),
                                                  ObMVRefreshType::FAST,
                                                  start_time,
                                                  end_time))) {
      LOG_WARN("failed to set last refresh info", KR(ret), K(mview_info_));
    } else if (OB_FAIL(ObMViewInfo::record_mview_info(trans_, mview_info_))) {
      LOG_WARN("failed to record mview info into __all_mview table", KR(ret), K(mview_info_));
    }
  }
  return ret;
}

int ObMViewRefresher::do_fast_refresh(const ObIArray<ObString> &refresh_sqls)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = param_.tenant_id_;
  const uint64_t mview_id = param_.mview_id_;
  const int64_t parallelism = refresh_parallel_;
  const ObIArray<ObMLogInfo> &mlog_infos = mlog_infos_;
  const ObScnRange &mview_refresh_scn_range = mview_refresh_scn_range_;
  const SCN &target_data_sync_scn = param_.target_data_sync_scn_;
  ObSQLSessionInfo *exec_session_info = nullptr;
  int64_t affected_rows = 0;
  if (OB_ISNULL(exec_session_info = trans_.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans_ is not valid", KR(ret));
  }
  // register mview mds
  if (OB_SUCC(ret)) {
    ObMViewOpArg mds_arg;
    mds_arg.table_id_ = mview_id;
    mds_arg.parallel_ = parallelism;
    mds_arg.session_id_ = trans_.get_session_info()->get_server_sid();
    mds_arg.start_ts_ = ObTimeUtil::current_time();
    mds_arg.mview_op_type_ = MVIEW_OP_TYPE::FAST_REFRESH;
    mds_arg.read_snapshot_ = mview_refresh_scn_range.end_scn_.get_val_for_tx();
    mds_arg.target_data_sync_scn_ = target_data_sync_scn;
    mds_arg.refresh_id_ = param_.refresh_id_;
    if (OB_FAIL(ObMViewMdsOpHelper::register_mview_mds(tenant_id, mds_arg, trans_))) {
      LOG_WARN("register mview mds failed", KR(ret), K(tenant_id), K(mds_arg));
    }
  }

  // erase mlog's dynamic sampling KV cache
  if (OB_SUCC(ret)) {
    ObOptDSStat::Key key;
    key.tenant_id_ = tenant_id;
    key.partition_hash_ = 0;
    key.ds_level_ = ObDynamicSamplingLevel::BASIC_DYNAMIC_SAMPLING;
    key.sample_block_ = OB_DS_BASIC_SAMPLE_MICRO_CNT;
    key.expression_hash_ = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < mlog_infos.count(); ++i) {
      key.table_id_ = mlog_infos.at(i).get_mlog_id();
      if (OB_FAIL(ObOptStatManager::get_instance().erase_ds_stat(key))) {
        LOG_WARN("failed to erase mlog ds stat cache before fast refresh", KR(ret), K(key), K(mlog_infos.at(i)));
      }
    }
  }

  RefreshEnvState env_state;
  if (OB_SUCC(ret) &&
      OB_FAIL(setup_refresh_env_(exec_session_info, trans_.is_inner_session(), parallelism, env_state))) {
    LOG_WARN("failed to setup refresh env", KR(ret));
  }
  const int64_t last_refresh_date = mview_info_.get_last_refresh_date();
  const int64_t refresh_exec_start_time = ObTimeUtil::current_time();
  // TODO: 这里应该用一个准确的刷新周期时间，而不是临时算出来的值
  const int64_t scheduling_period = (last_refresh_date > 0) ? (refresh_exec_start_time - last_refresh_date) : 0;
  const int64_t SLOW_SQL_THRESHOLD_US = 60L * 1000L * 1000L; // 1 min

  DEBUG_SYNC(BEFORE_MV_EXECUTE_REFRESH_SQL);

  // exec sqls
  for (int64_t i = 0; OB_SUCC(ret) && OB_SUCC(ctx_.check_status()) && i < refresh_sqls.count(); ++i) {
    const ObString &fast_refresh_sql = refresh_sqls.at(i);
    const int64_t exec_start_time = ObTimeUtil::current_time();
    if (OB_FAIL(ObMViewRefreshStatsUtils::write_stmt_before_step(ctx_, param_, i + 1, exec_start_time))) {
      LOG_WARN("fail to write stmt before step", KR(ret), K(i));
    } else if (OB_FAIL(trans_.write(tenant_id, fast_refresh_sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute write", KR(ret), K(fast_refresh_sql));
    }

    const int64_t exec_end_time = ObTimeUtil::current_time();
    const int64_t execution_time = exec_end_time - exec_start_time;

    ObMViewStmtPlanCaptureInfo capture_info;
    capture_info.should_capture_ = (ObMVRefreshStatsCollectionLevel::ADVANCED == collection_level_)
                                   || (OB_SUCCESS != ret)
                                   || (scheduling_period > 0 && execution_time > SLOW_SQL_THRESHOLD_US
                                       && execution_time > scheduling_period);
    // Always capture server identity for per-step observability.
    GCTX.self_addr().ip_to_string(capture_info.svr_ip_buf_, sizeof(capture_info.svr_ip_buf_));
    capture_info.svr_port_ = GCTX.self_addr().get_port();
    if (capture_info.should_capture_) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(sql::ObSQLUtils::gen_sql_id_from_sql_string(*exec_session_info,
                                                                  fast_refresh_sql,
                                                                  capture_info.sql_id_buf_,
                                                                  sizeof(capture_info.sql_id_buf_)))) {
        LOG_WARN("fail to calc sql_id for plan capture, ignore", KR(tmp_ret));
      }
      ObCurTraceId::get_trace_id_str(capture_info.trace_id_buf_, sizeof(capture_info.trace_id_buf_));
      capture_info.plan_id_ = exec_session_info->get_last_plan_id();
      capture_info.plan_hash_ = exec_session_info->get_current_plan_hash();
    }
    int tmp_ret = OB_SUCCESS;
    LOG_INFO("mview_refresh", K(tenant_id), K(mview_id), K(parallelism), K(i), K(fast_refresh_sql), "time", execution_time);
    if (OB_TMP_FAIL(ObMViewRefreshStatsUtils::write_stmt_after_step(ctx_, param_, i + 1, execution_time, ret, capture_info, parallelism))) {
      LOG_WARN("fail to write stmt after step", KR(tmp_ret), K(i));
    }
  }

  DEBUG_SYNC(BEFORE_MV_FINISH_RUNNING_JOB);

  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(teardown_refresh_env_(exec_session_info, env_state))) {
    LOG_WARN("failed to teardown refresh env", KR(tmp_ret));
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }

  return ret;
}

int ObMViewRefresher::setup_refresh_env_(ObSQLSessionInfo *session,
                                         bool is_inner_session,
                                         int64_t parallelism,
                                         RefreshEnvState &env_state)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret));
  } else if (OB_FAIL(session->get_force_parallel_dml_dop(env_state.orig_dml_dop_))) {
    LOG_WARN("fail to get force parallel dml dop", KR(ret));
  } else {
    ObObj val;
    val.set_uint64(parallelism);
    if (OB_FAIL(session->update_sys_variable(share::SYS_VAR__FORCE_PARALLEL_DML_DOP, val))) {
      LOG_WARN("fail to set force parallel dml dop", KR(ret), K(parallelism));
    } else {
      env_state.has_updated_dml_dop_ = true;
    }
  }
  // Always enable plan monitor so __ALL_VIRTUAL_SQL_PLAN_MONITOR data is available.
  if (OB_SUCC(ret) && !session->get_enable_sql_plan_monitor()) {
    ObObj val;
    val.set_int(1);
    if (OB_FAIL(session->update_sys_variable(share::SYS_VAR_ENABLE_SQL_PLAN_MONITOR, val))) {
      LOG_WARN("fail to enable sql plan monitor", KR(ret));
    } else {
      env_state.has_enabled_plan_monitor_ = true;
    }
  }
  if (OB_SUCC(ret) && is_inner_session) {
    session->set_mysql_cmd(COM_QUERY);
  }
  return ret;
}

int ObMViewRefresher::teardown_refresh_env_(ObSQLSessionInfo *session,
                                            const RefreshEnvState &env_state)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret));
  } else if (env_state.has_enabled_plan_monitor_) {
    ObObj val;
    val.set_int(0);
    if (OB_FAIL(session->update_sys_variable(share::SYS_VAR_ENABLE_SQL_PLAN_MONITOR, val))) {
      LOG_WARN("fail to restore sql plan monitor", KR(ret));
    }
  }
  if (env_state.has_updated_dml_dop_ && OB_NOT_NULL(session)) {
    ObObj val;
    val.set_uint64(env_state.orig_dml_dop_);
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(session->update_sys_variable(share::SYS_VAR__FORCE_PARALLEL_DML_DOP, val))) {
      LOG_WARN("fail to restore force parallel dml dop", KR(tmp_ret), K(env_state.orig_dml_dop_));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

int ObMViewRefresher::collect_based_schema_object_infos(const uint64_t tenant_id,
                                                        const uint64_t data_version,
                                                        ObSchemaGetterGuard &schema_guard,
                                                        const ObIArray<ObDependencyInfo> &dependency_infos,
                                                        ObIArray<ObBasedSchemaObjectInfo> &based_schema_object_infos,
                                                        uint64_t &direct_dep_cnt)
{
  int ret = OB_SUCCESS;
  direct_dep_cnt = 0;
  based_schema_object_infos.reuse();
  ARRAY_FOREACH(dependency_infos, i) {
    const ObDependencyInfo &dep = dependency_infos.at(i);
    const ObObjectType ref_obj_type = dep.get_ref_obj_type();
    const ObSchema *schema_obj = nullptr;
    ObBasedSchemaObjectInfo based_info;
    based_info.schema_id_ = dep.get_ref_obj_id();
    based_info.schema_tenant_id_ = tenant_id;
    if (OB_FAIL(ObMViewUtils::get_schema_object_from_dependency(
                based_info.schema_tenant_id_, schema_guard, based_info.schema_id_, ref_obj_type,
                schema_obj, based_info.schema_version_, based_info.schema_type_))) {
      LOG_WARN("fail to get schema object from dependency", KR(ret), K(dep));
    } else if (OB_FAIL(based_schema_object_infos.push_back(based_info))) {
      LOG_WARN("fail to push back base info", KR(ret));
    } else if (OB_INVALID_ID != dep.get_dep_obj_id()) {
      /* do nothing */
    } else if (OB_UNLIKELY(based_schema_object_infos.count() != ++direct_dep_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      //  In function ObDependencyInfo::collect_dep_infos_for_view,
      //  directly dependency base objects placed at the front of the array.
      LOG_WARN("unexpected ordering in based_schema_object_infos", K(ret), K(direct_dep_cnt), K(dependency_infos));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
