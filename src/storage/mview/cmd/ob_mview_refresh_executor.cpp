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

#include "storage/mview/cmd/ob_mview_refresh_executor.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/mview/cmd/ob_mview_executor_util.h"
#include "storage/mview/ob_mlog_purge.h"
#include "storage/mview/ob_mview_refresh.h"
#include "storage/mview/ob_mview_mds.h"
#include "storage/mview/ob_mview_refresh_helper.h"

namespace oceanbase
{
namespace storage
{
using namespace share;
using namespace share::schema;
using namespace sql;

void ObMViewRefreshArg::operator()(const ObMViewRefreshArg &other)
{
  list_ = other.list_;
  method_ = other.method_;
  rollback_seg_ = other.rollback_seg_;
  push_deferred_rpc_ = other.push_deferred_rpc_;
  refresh_after_errors_ = other.refresh_after_errors_;
  purge_option_ = other.purge_option_;
  parallelism_ = other.parallelism_;
  heap_size_ = other.heap_size_;
  atomic_refresh_ = other.atomic_refresh_;
  nested_ = other.nested_;
  out_of_place_ = other.out_of_place_;
  skip_ext_data_ = other.skip_ext_data_;
  refresh_parallel_ = other.refresh_parallel_;
  nested_consistent_refresh_ = other.nested_consistent_refresh_;
}

ObMViewRefreshExecutor::ObMViewRefreshExecutor()
  : ctx_(nullptr), arg_(nullptr), session_info_(nullptr), schema_checker_(), tenant_id_(OB_INVALID_TENANT_ID) 
{
}

ObMViewRefreshExecutor::~ObMViewRefreshExecutor() {}

int ObMViewRefreshExecutor::execute(ObExecContext &ctx, const ObMViewRefreshArg &arg)
{
  int ret = OB_SUCCESS;
  ctx_ = &ctx;
  arg_ = &arg;
  CK(OB_NOT_NULL(session_info_ = ctx.get_my_session()));
  CK(OB_NOT_NULL(ctx.get_sql_ctx()->schema_guard_));
  OV(OB_LIKELY(arg.is_valid()), OB_INVALID_ARGUMENT, arg);
  OZ(schema_checker_.init(*ctx.get_sql_ctx()->schema_guard_, session_info_->get_server_sid()));
  OX(tenant_id_ = session_info_->get_effective_tenant_id());
  OZ(ObMViewExecutorUtil::check_min_data_version(
    tenant_id_, DATA_VERSION_4_3_0_0, "tenant's data version is below 4.3.0.0, refresh mview is"));
  OZ(resolve_arg(arg));
  OZ(mview_infos_.prepare_allocate(mview_ids_.count()));
  if (OB_SUCC(ret)) {
    if (mview_ids_.empty()) {
      // do nothing
    } else if (mview_ids_.count() > 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported refresh multiple mviews", KR(ret), K(arg));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "refresh multiple materialized views");
    } else if (!arg_->nested_ && OB_FAIL(do_refresh())) {
      LOG_WARN("fail to do refresh", KR(ret));
    } else if (arg_->nested_ && OB_FAIL(do_nested_refresh_())) {
      LOG_WARN("fail to do nested refresh", KR(ret));
    }
  }

  return ret;
}

int ObMViewRefreshExecutor::resolve_arg(const ObMViewRefreshArg &arg)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret), KP(session_info_));
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", KR(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation_connection", KR(ret));
  }
  // resolve list
  if (OB_SUCC(ret)) {
    ObArray<ObString> mview_names;
    if (OB_FAIL(ObMViewExecutorUtil::split_table_list(arg.list_, mview_names))) {
      LOG_WARN("fail to split table list", KR(ret), K(arg.list_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < mview_names.count(); ++i) {
      const ObString &mview_name = mview_names.at(i);
      ObString database_name, table_name;
      bool has_synonym = false;
      ObString new_db_name, new_tbl_name;
      const ObTableSchema *table_schema = nullptr;
      if (OB_FAIL(ObMViewExecutorUtil::resolve_table_name(cs_type, case_mode, lib::is_oracle_mode(),
                                                          mview_name, database_name, table_name))) {
        LOG_WARN("fail to resolve table name", KR(ret), K(cs_type), K(case_mode), K(mview_name));
        LOG_USER_ERROR(OB_WRONG_TABLE_NAME, static_cast<int>(mview_name.length()),
                       mview_name.ptr());
      } else if (database_name.empty() &&
                 FALSE_IT(database_name = session_info_->get_database_name())) {
      } else if (OB_UNLIKELY(database_name.empty())) {
        ret = OB_ERR_NO_DB_SELECTED;
        LOG_WARN("No database selected", KR(ret));
      } else if (OB_FAIL(schema_checker_.get_table_schema_with_synonym(
                   tenant_id_, database_name, table_name, false /*is_index_table*/, has_synonym,
                   new_db_name, new_tbl_name, table_schema))) {
        LOG_WARN("fail to get table schema with synonym", KR(ret), K(database_name), K(table_name));
      } else if (OB_ISNULL(table_schema) || OB_UNLIKELY(!table_schema->is_materialized_view())) {
        ret = OB_ERR_MVIEW_NOT_EXIST;
        LOG_WARN("mview not exist", KR(ret), K(database_name), K(table_name), KPC(table_schema));
      }
      // check duplicate
      for (int64_t j = 0; OB_SUCC(ret) && j < mview_ids_.count(); ++j) {
        if (table_schema->get_table_id() == mview_ids_.at(j)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("duplicate mview is invalid", KR(ret), K(mview_name), K(j));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(mview_ids_.push_back(table_schema->get_table_id()))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
  }
  // resolve method
  if (OB_SUCC(ret)) {
    ObMVRefreshMethod refresh_method = ObMVRefreshMethod::MAX;
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.method_.length(); ++i) {
      const char c = arg.method_.ptr()[i];
      if (OB_FAIL(ObMViewExecutorUtil::to_refresh_method(c, refresh_method))) {
        LOG_WARN("fail to refresh method", KR(ret));
      } else if (OB_FAIL(refresh_methods_.push_back(refresh_method))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }
  return ret;
}

int ObMViewRefreshExecutor::do_refresh()
{
  int ret = OB_SUCCESS;
  int64_t refresh_id = OB_INVALID_ID;
  ObMViewRefreshStatsCollector stats_collector;
  if (OB_ISNULL(ctx_) || OB_ISNULL(arg_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", KP(ctx_), KP(arg_), KP(session_info_));
  } else if (OB_FAIL(ObMViewExecutorUtil::generate_refresh_id(tenant_id_, refresh_id))) {
    LOG_WARN("fail to generate refresh id", KR(ret));
  } else if (OB_FAIL(
               stats_collector.init(*ctx_, *arg_, tenant_id_, refresh_id, mview_ids_.count()))) {
    LOG_WARN("fail to init stats collector", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mview_ids_.count(); ++i) {
    const uint64_t &mview_id = mview_ids_.at(i);
    const ObMVRefreshMethod refresh_method =
      refresh_methods_.count() > i ? refresh_methods_.at(i) : ObMVRefreshMethod::MAX;

    int64_t start_time = ObTimeUtil::current_time();
    int64_t end_time = OB_INVALID_TIMESTAMP;
    int64_t log_purge_time = -1;

    ObMViewRefreshCtx refresh_ctx;
    ObMViewRefreshStatsCollection *refresh_stats_collection = nullptr;
    refresh_ctx.allocator_.set_tenant_id(tenant_id_);
    refresh_ctx.tenant_id_ = tenant_id_;
    refresh_ctx.mview_id_ = mview_id;
    const ObTableSchema *mv_schema = nullptr;
    if (OB_FAIL(stats_collector.alloc_collection(mview_id, refresh_stats_collection))) {
      LOG_WARN("fail to alloc collection", KR(ret), K(mview_id));
    } else if (OB_FAIL(ctx_->get_sql_ctx()->schema_guard_->get_table_schema(tenant_id_, mview_id, mv_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(mview_id));
    }
    // 1. refresh mview
    if (OB_SUCC(ret)) {
      ObMViewRefreshParam refresh_param;
      refresh_param.tenant_id_ = tenant_id_;
      refresh_param.mview_id_ = mview_id;
      refresh_param.refresh_method_ = refresh_method;
      refresh_param.parallelism_ = arg_->refresh_parallel_;
      while (OB_SUCC(ret) && OB_SUCC(ctx_->check_status())) {
        const ObDatabaseSchema *database_schema = nullptr;
        ObMViewTransaction trans;
        ObMViewRefresher refresher;
        const int64_t subtask_start_time = ObTimeUtil::current_time();
        const sql::ObLocalSessionVar *mv_solidified_session_var = &mv_schema->get_local_session_var();
        if (OB_FAIL(get_and_check_mview_database_schema(ctx_->get_sql_ctx()->schema_guard_,
                                                        mview_id,
                                                        database_schema))) {
          LOG_WARN("failed to get and check mview database schema", KR(ret));
        } else if (OB_FAIL(trans.start(ctx_->get_my_session(),
                                       ctx_->get_sql_proxy(),
                                       database_schema->get_database_id(),
                                       database_schema->get_database_name_str(),
                                       mv_solidified_session_var))) {
          LOG_WARN("fail to start trans", KR(ret), K(database_schema->get_database_id()),
              K(database_schema->get_database_name_str()));
        } else if (FALSE_IT(refresh_ctx.trans_ = &trans)) {
        } else if (OB_FAIL(refresher.init(*ctx_, refresh_ctx, refresh_param, refresh_stats_collection))) {
          LOG_WARN("fail to init refresher", KR(ret), K(refresh_param));
        } else if (OB_FAIL(refresher.refresh())) {
          LOG_WARN("fail to do refresh", KR(ret), K(refresh_param));
        }
        if (trans.is_started()) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
            LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
            ret = COVER_SUCC(tmp_ret);
          }
        }
        refresh_ctx.trans_ = nullptr;
        if (OB_FAIL(ret)) {
          // if failed,retry_id will inc, when success,
          // the last success record with new retry_id
          if (OB_NOT_NULL(refresh_stats_collection)) {
            int tmp_ret = OB_SUCCESS;
            const int64_t fail_time = ObTimeUtil::current_time();
            ObMViewRefreshStats stat = refresh_stats_collection->refresh_stats_;
            stat.set_start_time(subtask_start_time);
            stat.set_end_time(fail_time);
            stat.set_elapsed_time((fail_time - subtask_start_time) / 1000 / 1000);
            stat.set_result(ret);
            stat.set_refresh_type(refresh_ctx.refresh_type_);
            if (OB_TMP_FAIL(ObMViewRefreshStats::insert_refresh_stats(stat))) {
              LOG_WARN("fail to insert refresh stats in trans", K(ret), K(tmp_ret), K(stat));
            }
          }
          if (ObMViewExecutorUtil::is_mview_refresh_retry_ret_code(ret)) {
            ret = OB_SUCCESS;
            refresh_ctx.reuse();
            if (OB_FAIL(refresh_stats_collection->clear_for_retry())) {
              LOG_WARN("fail to clear for retry", KR(ret));
            } else {
              ob_usleep(1LL * 1000 * 1000);
            }
          }
        } else {
          mview_infos_.at(i) = refresh_ctx.mview_info_;
          break; // refresh succ
        }
      }
    }
    // 2. purge mlog
    if (OB_SUCC(ret)) {
      const int64_t start_purge_time = ObTimeUtil::current_time();
      int tmp_ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < refresh_ctx.mlog_infos_.count(); ++i) {
        const ObMLogInfo &mlog_info = refresh_ctx.mlog_infos_.at(i);
        const ObDependencyInfo &dep = refresh_ctx.dependency_infos_.at(i);
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
          purge_param.tenant_id_ = tenant_id_;
          purge_param.master_table_id_ = dep.get_ref_obj_id();
          purge_param.purge_log_parallel_ = arg_->refresh_parallel_; // reuse refresh_parallel_ in purge_log
          if (OB_TMP_FAIL(purger.init(*ctx_, purge_param))) {
            LOG_WARN("fail to init mlog purger", KR(tmp_ret), K(purge_param));
          } else if (OB_TMP_FAIL(purger.purge())) { // mlog may dropped, ignore
            LOG_WARN("fail to do purge", KR(tmp_ret), K(purge_param));
          }
        }
      }
      const int64_t end_purge_time = ObTimeUtil::current_time();
      log_purge_time = (end_purge_time - start_purge_time) / 1000 / 1000;
    }
    // 3. collect refresh stats
    if (OB_SUCC(ret)) {
      end_time = ObTimeUtil::current_time();
      refresh_stats_collection->set_start_time(start_time);
      refresh_stats_collection->set_end_time(end_time);
      refresh_stats_collection->set_elapsed_time((end_time - start_time) / 1000 / 1000);
      refresh_stats_collection->set_log_purge_time(log_purge_time);
      refresh_stats_collection->set_result(OB_SUCCESS);
      refresh_stats_collection->set_refresh_parallelism(refresh_ctx.refresh_parallelism_);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(stats_collector.commit())) {
      LOG_WARN("fail to commit stats", KR(ret));
    }
  }
  return ret;
}

int ObMViewRefreshExecutor::get_and_check_mview_database_schema(
    ObSchemaGetterGuard *schema_guard,
    const uint64_t mview_id,
    const ObDatabaseSchema *&database_schema)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  if (OB_ISNULL(schema_guard)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema guard cannot be null", KR(ret), KP(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(tenant_id_, mview_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id_), K(mview_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is nullptr", KR(ret), K(tenant_id_), K(mview_id));
  } else if (OB_FAIL(schema_guard->get_database_schema(
      tenant_id_, table_schema->get_database_id(), database_schema))) {
    LOG_WARN("fail to get database schema", KR(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema is nullptr", KR(ret));
  }
  return ret;
}

// step 1: get topo order mviews
// step 2: register mview list mds into sys ls memory and check
// step 3: get target scn, register mds and recheck
// step 4: scheduler nested refresh
int ObMViewRefreshExecutor::do_nested_refresh_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t bucket_num = 16;
  ObMemAttr attr(tenant_id_, "MViewRefresh");
  using namespace rootserver;
  const uint64_t target_mview_id = mview_ids_.at(0);
  ObSEArray<uint64_t, 4> nested_mview_ids;
  MViewDeps target_mview_deps;
  MViewDeps mview_reverse_deps;
  int64_t refresh_id = OB_INVALID_ID;
  share::SCN target_data_sync_scn;
  ObMySQLTransaction trans;
  int64_t start_ts = ObTimeUtility::fast_current_time();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  bool nested_consistent_refresh = false;
  ObMViewMaintenanceService *mview_maintenance_service =
                            MTL(ObMViewMaintenanceService*);
  if (OB_ISNULL(ctx_) || OB_ISNULL(arg_) ||
      OB_ISNULL(mview_maintenance_service) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", KP(ctx_), KP(arg_),
             KP(mview_maintenance_service), KP(session_info_));
  } else {
    nested_consistent_refresh = arg_->nested_consistent_refresh_;
  }
  // for test
  // if (nested_consistent_refresh) {
  //   ret = OB_NOT_SUPPORTED;
  //   LOG_WARN("sync refresh not supported now", K(ret));
  //   LOG_USER_ERROR(OB_NOT_SUPPORTED, "nested sync refresh");
  // }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(mview_maintenance_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mview maintenance service is nullptr", K(ret));
  } else if (OB_FAIL(target_mview_deps.create(bucket_num, attr))) {
    LOG_WARN("fail to create mview deps", K(ret));
  } else if (OB_FAIL(mview_reverse_deps.create(bucket_num, attr))) {
    LOG_WARN("fail to create mview reverse deps", K(ret));
  } else if (OB_FAIL(mview_maintenance_service->
                     get_target_nested_mview_deps(
                     target_mview_id, target_mview_deps))) {
    LOG_WARN("fail to get target nested mview deps", K(ret));
  } else if (OB_FAIL(mview_maintenance_service->
                     gen_target_nested_mview_topo_order(
                     target_mview_deps, mview_reverse_deps, nested_mview_ids))) {
    LOG_WARN("fail to gen target nested mview topo order", K(ret));
  } else if (nested_mview_ids.count() == 1) {
    nested_consistent_refresh = false;
    LOG_INFO("not nested mview, no need sync refresh", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (nested_consistent_refresh) {
    // nested too many mviews not support
    if (OB_FAIL(trans.start(ctx_->get_sql_proxy(), tenant_id_))) {
      LOG_WARN("fail to start trans", K(ret));
    } else if (OB_FAIL(ObMViewExecutorUtil::generate_refresh_id(
                       tenant_id_, refresh_id))) {
      LOG_WARN("fail to generate refresh id", K(ret));
    } else if (OB_FAIL(register_nested_mview_mds_and_check(target_mview_id,
                       refresh_id, nested_mview_ids, target_data_sync_scn, trans))) {
      LOG_WARN("fail to register and check mds", K(ret));
    }
    DEBUG_SYNC(BEFORE_NESTED_MV_GET_SCN);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObMViewRefreshHelper::get_current_scn(target_data_sync_scn))) {
      LOG_WARN("fail to get target data sync scn", K(ret));
    } else if (OB_FAIL(register_nested_mview_mds_and_check(target_mview_id,
                       refresh_id, nested_mview_ids, target_data_sync_scn, trans))) {
      LOG_WARN("fail to register and check mds", K(ret));
    } else if (OB_FAIL(scheduler_nested_mviews_sync_refresh_(target_mview_id, refresh_id,
                       target_data_sync_scn, nested_mview_ids, target_mview_deps,
                       mview_reverse_deps, trans))) {
      LOG_WARN("fail to scheduler nested mview refresh", K(ret));
    }
    if (trans.is_started()) {
      if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to end trans", K(ret), K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  } else if (!nested_consistent_refresh) {
    if (OB_FAIL(scheduler_nested_mviews_refresh_(nested_mview_ids))) {
      LOG_WARN("fail to scheduler nested mview refresh", K(ret), KPC(arg_), K(nested_mview_ids));
    }
  }
  int64_t end_ts = ObTimeUtility::fast_current_time();
  LOG_INFO("do nested refresh", K(ret), K(nested_consistent_refresh), K(target_mview_id), K(end_ts - start_ts));
  return ret;
}

int ObMViewRefreshExecutor::sync_check_nested_mview_mds(
                            const uint64_t mview_id,
                            const uint64_t refresh_id,
                            const share::SCN &target_data_sync_scn)
{
  int ret = OB_SUCCESS;
  obrpc::ObCheckNestedMViewMdsArg arg;
  arg.tenant_id_ = tenant_id_;
  arg.mview_id_ = mview_id;
  arg.refresh_id_ = refresh_id;
  arg.target_data_sync_scn_ = target_data_sync_scn;
  obrpc::ObCheckNestedMViewMdsRes res;
  int64_t start_ts = ObTimeUtility::fast_current_time();
  const int64_t timeout_ts = 30 * 1000 * 1000; // 30s
  if (mview_id == OB_INVALID_ID || refresh_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(mview_id), K(refresh_id));
  } else {
    do {
      if (OB_FAIL(ObMViewRefreshHelper::sync_post_nested_mview_rpc(arg, res))) {
        LOG_WARN("fail to post nested mview rpc", K(ret));
      } else {
        ret = res.ret_;
      }
      if (OB_NOT_MASTER == ret || OB_EAGAIN == ret) {
        int64_t cur_ts = ObTimeUtility::fast_current_time();
        if (cur_ts - start_ts > timeout_ts) {
          ret = OB_TIMEOUT;
          LOG_WARN("post nested mview cost too long time", K(ret));
          break;
        }
        ob_usleep(200 * 1000); // sleep 100ms
      }
    } while (OB_NOT_MASTER == ret ||
            OB_EAGAIN == ret);
  }
  return ret;
}

int ObMViewRefreshExecutor::register_nested_mview_mds_(
                            const uint64_t mview_id,
                            const uint64_t refresh_id,
                            const ObIArray<uint64_t> &nest_mview_ids,
                            const share::SCN &target_data_sync_scn,
                            ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObMViewOpArg mds_arg;
  if (OB_ISNULL(ctx_) || OB_ISNULL(arg_) ||
      OB_ISNULL(session_info_) ||
      mview_id == OB_INVALID_ID || refresh_id == OB_INVALID_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", KP(ctx_), KP(arg_), K(mview_id), K(refresh_id),
             KP(session_info_));
  } else {
    mds_arg.table_id_ = mview_id;
    mds_arg.mview_op_type_ = MVIEW_OP_TYPE::NESTED_SYNC_REFRESH;
    mds_arg.read_snapshot_ = ObTimeUtility::current_time_ns();
    mds_arg.parallel_ = arg_->refresh_parallel_;
    mds_arg.session_id_ = session_info_->get_server_sid();
    mds_arg.start_ts_ = ObTimeUtility::fast_current_time();
    // nested mivew refresh arg
    mds_arg.refresh_id_ = refresh_id;
    mds_arg.target_data_sync_scn_ = target_data_sync_scn;
  }
  if (OB_FAIL(ret)) {
  } else if (!trans.is_started() ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trans is not started", K(ret));
  } else if (OB_FAIL(mds_arg.nested_mview_lists_.assign(nest_mview_ids))) {
    LOG_WARN("fail to assign nested mview ids", K(ret));
  } else if (OB_FAIL(ObMViewMdsOpHelper::
                     register_mview_mds(tenant_id_, mds_arg, trans))) {
    LOG_WARN("fail to register mview mds", K(ret), K(mds_arg));
  }
  return ret;
}

// register nested mview mds and check sys ls
int ObMViewRefreshExecutor::register_nested_mview_mds_and_check(
                            const uint64_t mview_id,
                            const uint64_t refresh_id,
                            const ObIArray<uint64_t> &nest_mview_ids,
                            const share::SCN &target_data_sync_scn,
                            ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (mview_id == OB_INVALID_ID || refresh_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(mview_id), K(refresh_id));
  } else if (OB_FAIL(register_nested_mview_mds_(mview_id, refresh_id,
              nest_mview_ids, target_data_sync_scn, trans))) {
    LOG_WARN("fail to register nested mview mds", K(ret),
             K(mview_id), K(target_data_sync_scn));
  } else if (OB_FAIL(sync_check_nested_mview_mds(
                     mview_id, refresh_id, target_data_sync_scn))) {
    LOG_WARN("fail to sync check nested mview mds", K(ret),
             K(mview_id), K(target_data_sync_scn));
  }

  return ret;
}

// for each topo order to scheduler mview refresh
// step 1: get min target scn and check need scheduler this target scn
// step 2: do refresh mview
// step 3: check data_sync_scn satisfy after refresh
int ObMViewRefreshExecutor::scheduler_nested_mviews_sync_refresh_(
                            const uint64_t target_mview_id,
                            const uint64_t refresh_id,
                            const share::SCN &target_data_sync_scn,
                            const ObIArray<uint64_t> &nested_mview_ids,
                            rootserver::MViewDeps &target_mview_deps,
                            rootserver::MViewDeps &mview_reverse_deps,
                            ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  using namespace share;
  using namespace schema;
  bool satisfy = false;
  share::SCN current_scn; // invalid scn
  ObSEArray<uint64_t, 2> dep_mview_ids;
  hash::ObHashSet<uint64_t> mv_sets;
  if (OB_ISNULL(ctx_) || OB_ISNULL(arg_) ||
      target_mview_id == OB_INVALID_ID || refresh_id == OB_INVALID_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", KP(ctx_), KP(arg_), K(target_mview_id), K(refresh_id));
  } else if (OB_FAIL(mv_sets.create(10))) {
    LOG_WARN("fail to create mv sets", K(ret));
  } else {
    ARRAY_FOREACH(nested_mview_ids, idx) {
      if (OB_FAIL(mv_sets.set_refactored(nested_mview_ids.at(idx)))) {
        LOG_WARN("fail to set mview id to sets", K(ret), K(idx));
      }
    }
    if (OB_SUCC(ret)) {
      ARRAY_FOREACH(nested_mview_ids, idx) {
        DEBUG_SYNC(BEFORE_NESTED_MV_REFRESH);
        const uint64_t mview_id = nested_mview_ids.at(idx);
        // check mds exist in sys ls leader
        if (OB_FAIL(ctx_->check_status())) {
          LOG_INFO("fail to check status", K(ret));
        } else if (OB_FAIL(target_mview_deps.get_refactored(mview_id, dep_mview_ids))) {
          LOG_WARN("fail to get dep mviews", K(ret));
        } else {
          bool need_retry = false;
          ObMViewRefreshArg refresh_arg = *arg_;
          refresh_arg.nested_ = false;
          refresh_arg.nested_consistent_refresh_ = false;
          do {
            ObMViewInfo mview_info;
            refresh_arg.list_.reset();
            const ObTableSchema *table_schema = nullptr;
            ObSqlString table_name;
            ObMViewRefreshExecutor refresh_executor;
            share::SCN min_target_data_sync_scn;
            if (OB_FAIL(ObMViewRefreshHelper::sync_get_min_target_data_sync_scn(tenant_id_,
                        mview_id, min_target_data_sync_scn))) {
              LOG_WARN("fail to get min target data sync scn", K(ret));
            } else if (!min_target_data_sync_scn.is_valid() ||
                       min_target_data_sync_scn > target_data_sync_scn) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected min target scn", K(ret), K(mview_id), K(min_target_data_sync_scn),
                       K(target_data_sync_scn));
            } else if (min_target_data_sync_scn != target_data_sync_scn) {
              need_retry = true;
              LOG_INFO("curr mview need satisfy min target scn, need wait", K(mview_id),
                       K(min_target_data_sync_scn), K(target_data_sync_scn));
            } else if (OB_FAIL(schema_checker_.get_table_schema(
                               tenant_id_, mview_id, table_schema))) {
              LOG_WARN("fail to get table schema", K(ret));
            } else if (OB_ISNULL(table_schema)) {
              LOG_INFO("mview not exist, skip refresh it, may try complete refresh", K(ret), K(mview_id), KP(table_schema));
            } else if (OB_FAIL(generate_database_table_name_(table_schema, table_name))) {
              LOG_INFO("fail to generate database table name", K(ret), K(table_name));
            } else if (OB_FALSE_IT(refresh_arg.list_ = table_name.ptr())) {
            } else if (OB_FAIL(refresh_executor.execute(*ctx_, refresh_arg))) {
              LOG_WARN("fail to do nested refresh", K(ret));
            } else if (OB_FALSE_IT(mview_info = refresh_executor.get_first_mview_info())) {
            } else if (!mview_info.is_valid()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("mview info is invalid", K(ret), K(mview_info));
            } else if (OB_FAIL(ObMViewInfo::check_satisfy_target_data_sync_scn(mview_info,
                               target_data_sync_scn.get_val_for_gts(), satisfy))) {
              need_retry = false;
              LOG_WARN("fail to check satisfy target data sync scn",
                       K(ret), K(mview_info), K(target_data_sync_scn));
            } else if (!satisfy) {
              // a mview dep only on base table and blocked by other nested refresh
              LOG_INFO("mview can not satisfy target scn after refresh, need retry", K(mview_info),
                       K(ret), K(mview_id), K(target_data_sync_scn), K(dep_mview_ids));
              need_retry = true;
            } else if (satisfy) {
              need_retry = false;
              // optimise to avoid block other sync refresh
              int tmp_ret = OB_SUCCESS;
              if (OB_TMP_FAIL(check_register_new_mview_list_(target_mview_id, mview_id, refresh_id, 
                              target_data_sync_scn, target_mview_deps, mview_reverse_deps, trans, mv_sets))) {
                LOG_INFO("fail to register new mview list", K(tmp_ret), K(mview_id), K(mview_info));
              }
            }
            if (need_retry) {
              ob_usleep(1 * 1000 * 1000); // 1s
            }
            LOG_INFO("do nested refresh mview", K(ret), K(refresh_arg), K(mview_info), K(idx), K(need_retry));
          } while (need_retry && OB_SUCC(ret) && OB_SUCC(ctx_->check_status()));
        }
      }
    }
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(mv_sets.destroy())) {
    ret = COVER_SUCC(tmp_ret);
    LOG_WARN("fail to destory hash set", K(ret), K(tmp_ret));
  }
  return ret;
}

int ObMViewRefreshExecutor::scheduler_nested_mviews_refresh_(
                            const ObIArray<uint64_t> &nested_mview_ids)
{
  int ret = OB_SUCCESS;
  ObMViewRefreshArg refresh_arg = *arg_;
  refresh_arg.nested_ = false;
  refresh_arg.nested_consistent_refresh_ = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(arg_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", KP(ctx_), KP(arg_));
  } else {
    ARRAY_FOREACH(nested_mview_ids, idx) {
      ObMViewRefreshExecutor refresh_executor;
      refresh_arg.list_.reset();
      const ObTableSchema *table_schema = nullptr;
      const uint64_t mview_id = nested_mview_ids.at(idx);
      ObSqlString table_name;
      if (OB_SUCC(ctx_->check_status())) {
        if (OB_FAIL(schema_checker_.get_table_schema(
                    tenant_id_, mview_id, table_schema))) {
          LOG_WARN("fail to get table schema", K(ret));
        } else if (OB_ISNULL(table_schema)) {
          LOG_INFO("mview not exist, skip refresh it, may try complete refresh", K(ret));
        } else if (OB_FAIL(generate_database_table_name_(table_schema, table_name))) {
          LOG_INFO("fail to generate database table name", K(ret), K(table_name));
        } else if (OB_FALSE_IT(refresh_arg.list_ = table_name.ptr())) {
        } else if (OB_FAIL(refresh_executor.execute(*ctx_, refresh_arg))) {
          LOG_WARN("fail to do nested refresh", K(ret));
        }
        LOG_INFO("do non sync nested refresh", K(ret), K(refresh_arg), K(table_name),
                 K(table_schema->get_table_name_str()), K(idx));
      }
    }
  }
  return ret;
}

int ObMViewRefreshExecutor::check_register_new_mview_list_(
                            const uint64_t target_mview_id,
                            const uint64_t mview_id,
                            const uint64_t refresh_id,
                            const share::SCN &target_data_sync_scn,
                            const rootserver::MViewDeps &target_mview_deps,
                            const rootserver::MViewDeps &mview_reverse_deps, 
                            ObMySQLTransaction &trans,
                            hash::ObHashSet<uint64_t> &mv_sets)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 2> dep_mview_ids;
  ObSEArray<uint64_t, 2> reverse_dep_mview_ids;
  ObSEArray<uint64_t, 2> mv_list;
  bool need_register_new_list = false;
  if (mview_id == OB_INVALID_ID || refresh_id == OB_INVALID_ID ||
      target_mview_id == OB_INVALID_ID || !target_data_sync_scn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(mview_id), K(refresh_id));
  } else if (OB_FAIL(target_mview_deps.get_refactored(mview_id, dep_mview_ids))) {
    LOG_WARN("fail to get dep mviews", K(ret));
  } else if (dep_mview_ids.count() > 0) {
    ARRAY_FOREACH(dep_mview_ids, idx) {
      const uint64_t mview_id = dep_mview_ids.at(idx);
      if (OB_HASH_EXIST == mv_sets.exist_refactored(mview_id)) {
        if (OB_FAIL(mview_reverse_deps.get_refactored(mview_id, reverse_dep_mview_ids))) {
          LOG_WARN("fail to get reverse dep mviews", K(ret));
        } else if (reverse_dep_mview_ids.count() > 0) {
          bool satisfy = false;
          if (OB_FAIL(ObMViewRefreshHelper::check_dep_mviews_satisfy_target_scn(
                      tenant_id_, target_data_sync_scn, share::SCN(), reverse_dep_mview_ids,
                      trans, satisfy))) {
            LOG_WARN("fail to check dep mviews satisfy target scn", K(ret));
          } else if (satisfy) {
            if (OB_FAIL(mv_sets.erase_refactored(mview_id))) {
              LOG_WARN("fail to erase mview id", K(ret));
            } else {
              // remove from mv set
              need_register_new_list = true;
              LOG_INFO("mview can be remvoed from mds list", K(target_mview_id), K(mview_id),
                       K(dep_mview_ids), K(reverse_dep_mview_ids));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && need_register_new_list) {
      for(hash::ObHashSet<uint64_t>::iterator it = mv_sets.begin();
          OB_SUCC(ret) && it != mv_sets.end(); it++) {
        if (OB_FAIL(mv_list.push_back(it->first))) {
          LOG_WARN("fail to push back mview id", K(ret));
        }
      }
      if (OB_SUCC(ret) &&
          (OB_FAIL(register_nested_mview_mds_(target_mview_id,
                   refresh_id, mv_list, target_data_sync_scn, trans)))) {
        LOG_WARN("fail to register new mview list", K(ret));
      }
    }
  }
  return ret;
}

int ObMViewRefreshExecutor::generate_database_table_name_(
                            const ObTableSchema *table_schema,
                            ObSqlString &table_name)
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema *database_schema = nullptr;
  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_INFO("table schema is null", K(ret), KP(table_schema));
  } else if (OB_FAIL(schema_checker_.get_database_schema(tenant_id_,
                     table_schema->get_database_id(), database_schema))) {
    LOG_WARN("fail to get database schema", KR(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("database not exist", K(ret));
  } else if (OB_FAIL(table_name.assign_fmt("%s.%s",
                     database_schema->get_database_name_str().ptr(),
                     table_schema->get_table_name_str().ptr()))) {
    LOG_WARN("fail to apped fmt table name", K(ret));
  }
  return ret;
}

int ObMViewRefreshExecutor::set_session_vars_(
                            const uint64_t mview_id,
                            ObMViewTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_id_; 
  const ObTableSchema *mv_schema = nullptr;
  sql::ObSessionSysVar *collation_connection_var = nullptr;
  sql::ObSessionSysVar *compatibility_version_var = nullptr;
  if (OB_FAIL(schema_checker_.get_table_schema(tenant_id, mview_id, mv_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(mview_id));
  } else if (OB_ISNULL(mv_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv schema is null", K(ret), KP(mv_schema));
  } else if (OB_ISNULL(trans.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", KP(trans.get_session_info()));
  } else {
    const sql::ObLocalSessionVar &session_vars = mv_schema->get_local_session_var();
    if (OB_FAIL(session_vars.get_local_var(ObSysVarClassType::SYS_VAR_COLLATION_CONNECTION,
                                           collation_connection_var))) {
      LOG_WARN("fail to get local session var", K(ret));
    } else if (OB_ISNULL(collation_connection_var)) {
      LOG_INFO("no collation connection var, skip", K(ret), KP(collation_connection_var));
    } else if (OB_FAIL(trans.get_session_info()->update_sys_variable(
                       collation_connection_var->type_, collation_connection_var->val_))) {
      LOG_WARN("fail to update sys var", K(ret));
    } 
    
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(session_vars.get_local_var(ObSysVarClassType::SYS_VAR_OB_COMPATIBILITY_VERSION,
                       compatibility_version_var))) {
      LOG_WARN("fail to get local session var", K(ret));
    } else if (OB_ISNULL(compatibility_version_var)) {
      LOG_INFO("no compatibility version var, skip", K(ret), KP(compatibility_version_var));
    } else if (OB_FAIL(trans.get_session_info()->update_sys_variable(
                       compatibility_version_var->type_, compatibility_version_var->val_))) {
      LOG_WARN("fail to update sys var", K(ret));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
