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
#include "storage/mview/ob_mview_transaction.h"

namespace oceanbase
{
namespace storage
{
using namespace share;
using namespace share::schema;
using namespace sql;

ObMViewRefreshExecutor::ObMViewRefreshExecutor()
  : ctx_(nullptr), arg_(nullptr), session_info_(nullptr), tenant_id_(OB_INVALID_TENANT_ID)
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
  OZ(schema_checker_.init(*ctx.get_sql_ctx()->schema_guard_, session_info_->get_sessid()));
  OX(tenant_id_ = session_info_->get_effective_tenant_id());
  OZ(ObMViewExecutorUtil::check_min_data_version(
    tenant_id_, DATA_VERSION_4_3_0_0, "tenant's data version is below 4.3.0.0, refresh mview is"));
  OZ(resolve_arg(arg));

  if (OB_SUCC(ret)) {
    if (mview_ids_.empty()) {
      // do nothing
    } else if (mview_ids_.count() > 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported refresh multiple mviews", KR(ret), K(arg));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "refresh multiple materialized views");
    } else if (OB_FAIL(do_refresh())) {
      LOG_WARN("fail to do refresh", KR(ret));
    }
  }

  return ret;
}

int ObMViewRefreshExecutor::resolve_arg(const ObMViewRefreshArg &arg)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
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
  if (OB_FAIL(ObMViewExecutorUtil::generate_refresh_id(tenant_id_, refresh_id))) {
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
    if (OB_FAIL(stats_collector.alloc_collection(mview_id, refresh_stats_collection))) {
      LOG_WARN("fail to alloc collection", KR(ret), K(mview_id));
    }
    // 1. refresh mview
    if (OB_SUCC(ret)) {
      ObMViewRefreshParam refresh_param;
      refresh_param.tenant_id_ = tenant_id_;
      refresh_param.mview_id_ = mview_id;
      refresh_param.refresh_method_ = refresh_method;
      refresh_param.parallelism_ = arg_->refresh_parallel_;
      while (OB_SUCC(ret) && OB_SUCC(ctx_->check_status())) {
        ObMViewTransaction trans;
        ObMViewRefresher refresher;
        if (OB_FAIL(trans.start(ctx_->get_my_session(), ctx_->get_sql_proxy()))) {
          LOG_WARN("fail to start trans", KR(ret));
        } else if (FALSE_IT(refresh_ctx.trans_ = &trans)) {
        } else if (OB_FAIL(
                     refresher.init(*ctx_, refresh_ctx, refresh_param, refresh_stats_collection))) {
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
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(stats_collector.commit())) {
      LOG_WARN("fail to commit stats", KR(ret));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
