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

#include "storage/mview/ob_mview_refresh_stats_collect.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "observer/ob_inner_sql_connection.h"
#include "share/schema/ob_schema_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/mview/cmd/ob_mview_refresh_executor.h"
#include "storage/mview/ob_mview_refresh_ctx.h"
#include "storage/mview/ob_mview_refresh_helper.h"
#include "storage/mview/ob_mview_transaction.h"

namespace oceanbase
{
namespace storage
{
using namespace share;
using namespace share::schema;
using namespace sql;
using namespace observer;

/**
 * ObMViewRefreshStatsCollection
 */

ObMViewRefreshStatsCollection::ObMViewRefreshStatsCollection()
  : ctx_(nullptr),
    tenant_id_(OB_INVALID_TENANT_ID),
    refresh_id_(OB_INVALID_ID),
    mview_id_(OB_INVALID_ID),
    retry_id_(OB_INVALID_ID),
    collection_level_(ObMVRefreshStatsCollectionLevel::MAX),
    is_inited_(false)
{
}

ObMViewRefreshStatsCollection::~ObMViewRefreshStatsCollection() {}

int ObMViewRefreshStatsCollection::init(ObExecContext &ctx, const uint64_t tenant_id,
                                        const int64_t refresh_id, const uint64_t mview_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMViewRefreshStatsCollection init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == ctx.get_my_session() || nullptr == ctx.get_sql_proxy() ||
                         OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == refresh_id ||
                         OB_INVALID_ID == mview_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(ctx), K(tenant_id), K(refresh_id), K(mview_id));
  } else {
    ctx_ = &ctx;
    tenant_id_ = tenant_id;
    refresh_id_ = refresh_id;
    mview_id_ = mview_id;
    retry_id_ = 0;
    refresh_stats_.set_tenant_id(tenant_id_);
    refresh_stats_.set_refresh_id(refresh_id_);
    refresh_stats_.set_mview_id(mview_id_);
    refresh_stats_.set_retry_id(retry_id_);
    is_inited_ = true;
  }
  return ret;
}

int ObMViewRefreshStatsCollection::clear_for_retry()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewRefreshStatsCollection not init", KR(ret), KP(this));
  } else {
    ++retry_id_;
    collection_level_ = ObMVRefreshStatsCollectionLevel::MAX;
    refresh_stats_.set_retry_id(retry_id_);
    refresh_stats_.set_refresh_type(ObMVRefreshType::MAX);
    refresh_stats_.set_num_steps(0);
    refresh_stats_.set_initial_num_rows(0);
    refresh_stats_.set_final_num_rows(0);
    change_stats_array_.reset();
    stmt_stats_array_.reset();
  }
  return ret;
}

int ObMViewRefreshStatsCollection::collect_before_refresh(ObMViewRefreshCtx &refresh_ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewRefreshStatsCollection not init", KR(ret), KP(this));
  } else if (OB_ISNULL(refresh_ctx.trans_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null trans", KR(ret));
  } else {
    ObMViewTransaction &trans = *refresh_ctx.trans_;
    int64_t num_rows = 0;
    collection_level_ = refresh_ctx.refresh_stats_params_.get_collection_level();
    refresh_stats_.set_refresh_type(refresh_ctx.refresh_type_);
    refresh_stats_.set_num_steps(refresh_ctx.refresh_sqls_.count());
    if (ObMVRefreshStatsCollectionLevel::ADVANCED == collection_level_) {
      if (OB_TMP_FAIL(ObMViewRefreshHelper::get_table_row_num(trans, tenant_id_, mview_id_,
                                                              SCN::invalid_scn(), num_rows))) {
        LOG_WARN("fail to get mview row num before refresh", KR(tmp_ret), K(mview_id_));
      }
    }
    refresh_stats_.set_initial_num_rows(num_rows);
  }
  return ret;
}

int ObMViewRefreshStatsCollection::collect_after_refresh(ObMViewRefreshCtx &refresh_ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewRefreshStatsCollection not init", KR(ret), KP(this));
  } else if (OB_ISNULL(refresh_ctx.trans_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null trans", KR(ret));
  } else {
    ObMViewTransaction &trans = *refresh_ctx.trans_;
    const ObIArray<ObDependencyInfo> &dependency_infos = refresh_ctx.dependency_infos_;
    const ObIArray<ObMLogInfo> &mlog_infos = refresh_ctx.mlog_infos_;
    int64_t num_rows = 0;
    if (ObMVRefreshStatsCollectionLevel::ADVANCED == collection_level_ &&
        OB_TMP_FAIL(ObMViewRefreshHelper::get_table_row_num(trans, tenant_id_, mview_id_,
                                                            SCN::invalid_scn(), num_rows))) {
      LOG_WARN("fail to get mview row num after refresh", KR(tmp_ret), K(mview_id_));
    }
    refresh_stats_.set_final_num_rows(num_rows);
    if (collection_level_ >= ObMVRefreshStatsCollectionLevel::TYPICAL) {
      for (int64_t i = 0; OB_SUCC(ret) && i < dependency_infos.count(); ++i) {
        const ObDependencyInfo &dep = dependency_infos.at(i);
        const ObMLogInfo &mlog_info = mlog_infos.at(i);
        int64_t num_rows_ins = 0;
        int64_t num_rows_upd = 0;
        int64_t num_rows_del = 0;
        int64_t num_rows = 0;
        if (mlog_info.is_valid() &&
            OB_TMP_FAIL(ObMViewRefreshHelper::get_mlog_dml_row_num(
              trans, tenant_id_, mlog_info.get_mlog_id(), refresh_ctx.refresh_scn_range_,
              num_rows_ins, num_rows_upd, num_rows_del))) {
          LOG_WARN("fail to get mlog dml row num", KR(tmp_ret), K(mlog_info));
        }
        if (ObMVRefreshStatsCollectionLevel::ADVANCED == collection_level_ &&
            OB_TMP_FAIL(ObMViewRefreshHelper::get_table_row_num(
              trans, tenant_id_, dep.get_ref_obj_id(), refresh_ctx.refresh_scn_range_.end_scn_,
              num_rows))) {
          LOG_WARN("fail to get based table row num", KR(tmp_ret), K(dep));
        }
        if (OB_SUCC(ret)) {
          ObMViewRefreshChangeStats change_stats;
          change_stats.set_tenant_id(tenant_id_);
          change_stats.set_refresh_id(refresh_id_);
          change_stats.set_mview_id(mview_id_);
          change_stats.set_retry_id(retry_id_);
          change_stats.set_detail_table_id(dep.get_ref_obj_id());
          change_stats.set_num_rows_ins(num_rows_ins);
          change_stats.set_num_rows_upd(num_rows_upd);
          change_stats.set_num_rows_del(num_rows_del);
          change_stats.set_num_rows(num_rows);
          if (OB_FAIL(change_stats_array_.push_back(change_stats))) {
            LOG_WARN("fail to push back change stats", KR(ret), K(change_stats));
          }
        }
      }
    }
  }
  return ret;
}

int ObMViewRefreshStatsCollection::collect_stmt_stats(ObMViewRefreshCtx &refresh_ctx,
                                                      const ObString &stmt,
                                                      const int64_t execution_time)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewRefreshStatsCollection not init", KR(ret), KP(this));
  } else if (OB_ISNULL(refresh_ctx.trans_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null trans", KR(ret));
  } else if (ObMVRefreshStatsCollectionLevel::ADVANCED == collection_level_) {
    ObInnerSQLConnection *conn = nullptr;
    ObSQLSessionInfo *session_info = nullptr;
    char sql_id[OB_MAX_SQL_ID_LENGTH + 1];
    if (OB_ISNULL(conn =
                    static_cast<ObInnerSQLConnection *>(refresh_ctx.trans_->get_connection()))) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("connection can not be NULL", KR(ret));
    } else {
      session_info = &conn->get_session();
      session_info->get_cur_sql_id(sql_id, OB_MAX_SQL_ID_LENGTH + 1);
    }
    if (OB_SUCC(ret)) {
      ObMViewRefreshStmtStats stmt_stats;
      stmt_stats.set_tenant_id(tenant_id_);
      stmt_stats.set_refresh_id(refresh_id_);
      stmt_stats.set_mview_id(mview_id_);
      stmt_stats.set_retry_id(retry_id_);
      stmt_stats.set_step(stmt_stats_array_.count() + 1);
      stmt_stats.set_execution_time(execution_time);
      stmt_stats.set_result(OB_SUCCESS);
      if (OB_FAIL(stmt_stats.set_sql_id(ObString(OB_MAX_SQL_ID_LENGTH, sql_id)))) {
        LOG_WARN("fail to set sql id", KR(ret));
      } else if (OB_FAIL(stmt_stats.set_stmt(stmt))) {
        LOG_WARN("fail to set stmt", KR(ret));
      } else if (OB_FAIL(stmt_stats_array_.push_back(stmt_stats))) {
        LOG_WARN("fail to push back", K(stmt_stats));
      }
    }
  }
  return ret;
}

int ObMViewRefreshStatsCollection::commit(ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewRefreshStatsCollection not init", KR(ret), KP(this));
  } else {
    // refresh_stats
    if (OB_FAIL(ObMViewRefreshStats::insert_refresh_stats(sql_client, refresh_stats_))) {
      LOG_WARN("fail to insert refresh stats", KR(ret), K(refresh_stats_));
    }
    // change_stats
    for (int64_t i = 0; OB_SUCC(ret) && i < change_stats_array_.count(); ++i) {
      const ObMViewRefreshChangeStats &change_stats = change_stats_array_.at(i);
      if (OB_FAIL(ObMViewRefreshChangeStats::insert_change_stats(sql_client, change_stats))) {
        LOG_WARN("fail to insert change stats", KR(ret), K(change_stats));
      }
    }
    // stmt_stats
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt_stats_array_.count(); ++i) {
      const ObMViewRefreshStmtStats &stmt_stats = stmt_stats_array_.at(i);
      if (OB_FAIL(ObMViewRefreshStmtStats::insert_stmt_stats(sql_client, stmt_stats))) {
        LOG_WARN("fail to insert stmt stats", KR(ret), K(stmt_stats));
      }
    }
  }
  return ret;
}

/**
 * ObMViewRefreshStatsCollector
 */

ObMViewRefreshStatsCollector::ObMViewRefreshStatsCollector()
  : allocator_("MVRefStatsColl"),
    ctx_(nullptr),
    tenant_id_(OB_INVALID_TENANT_ID),
    refresh_id_(OB_INVALID_ID),
    is_inited_(false)
{
}

ObMViewRefreshStatsCollector::~ObMViewRefreshStatsCollector()
{
  FOREACH(iter, mv_ref_stats_map_)
  {
    ObMViewRefreshStatsCollection *collection = iter->second;
    collection->~ObMViewRefreshStatsCollection();
    allocator_.free(collection);
  }
  mv_ref_stats_map_.reuse();
}

int ObMViewRefreshStatsCollector::init(ObExecContext &ctx, const ObMViewRefreshArg &refresh_arg,
                                       const uint64_t tenant_id, const int64_t refresh_id,
                                       const int64_t mv_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMViewRefreshStatsCollector init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == ctx.get_my_session() || nullptr == ctx.get_sql_proxy() ||
                         !refresh_arg.is_valid() || OB_INVALID_TENANT_ID == tenant_id ||
                         OB_INVALID_ID == refresh_id || mv_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(ctx), K(refresh_arg), K(tenant_id), K(refresh_id),
             K(mv_cnt));
  } else {
    allocator_.set_tenant_id(tenant_id);
    ctx_ = &ctx;
    tenant_id_ = tenant_id;
    refresh_id_ = refresh_id;
    run_stats_.set_tenant_id(tenant_id_);
    run_stats_.set_refresh_id(refresh_id);
    run_stats_.set_run_user_id(ctx.get_my_session()->get_priv_user_id());
    run_stats_.set_num_mvs_total(mv_cnt);
    run_stats_.set_num_mvs_current(0);
    run_stats_.set_push_deferred_rpc(refresh_arg.push_deferred_rpc_);
    run_stats_.set_refresh_after_errors(refresh_arg.refresh_after_errors_);
    run_stats_.set_purge_option(refresh_arg.purge_option_);
    run_stats_.set_parallelism(refresh_arg.parallelism_);
    run_stats_.set_heap_size(refresh_arg.heap_size_);
    run_stats_.set_atomic_refresh(refresh_arg.atomic_refresh_);
    run_stats_.set_nested(refresh_arg.nested_);
    run_stats_.set_out_of_place(refresh_arg.out_of_place_);
    run_stats_.set_number_of_failures(0);
    run_stats_.set_start_time(ObTimeUtil::current_time());
    run_stats_.set_elapsed_time(0);
    run_stats_.set_log_purge_time(0);
    run_stats_.set_complete_stats_avaliable(true);
    if (OB_FAIL(run_stats_.set_mviews(refresh_arg.list_))) {
      LOG_WARN("fail to set mviews", KR(ret), K(refresh_arg.list_));
    } else if (OB_FAIL(run_stats_.set_method(refresh_arg.method_))) {
      LOG_WARN("fail to set method", KR(ret), K(refresh_arg.method_));
    } else if (OB_FAIL(run_stats_.set_rollback_seg(refresh_arg.rollback_seg_))) {
      LOG_WARN("fail to set rollback seg", KR(ret), K(refresh_arg.rollback_seg_));
    } else if (OB_FAIL(run_stats_.set_trace_id(ObCurTraceId::get_trace_id_str()))) {
      LOG_WARN("fail to set trace id", KR(ret));
    } else if (OB_FAIL(
                 mv_ref_stats_map_.create(1024, "MVRefStatsMap", "MVRefStatsMap", tenant_id))) {
      LOG_WARN("fail to create hashmap", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObMViewRefreshStatsCollector::alloc_collection(const uint64_t mview_id,
                                                   ObMViewRefreshStatsCollection *&stats_collection)
{
  int ret = OB_SUCCESS;
  stats_collection = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewRefreshStatsCollector not init", KR(ret), KP(this));
  } else {
    if (OB_UNLIKELY(nullptr != mv_ref_stats_map_.get(mview_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected alloc collection duplicate", KR(ret), K(mview_id));
    } else {
      ObMViewRefreshStatsCollection *new_stats_collection = nullptr;
      if (OB_ISNULL(new_stats_collection = OB_NEWx(ObMViewRefreshStatsCollection, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObMViewRefreshStatsCollection", KR(ret));
      } else if (OB_FAIL(new_stats_collection->init(*ctx_, tenant_id_, refresh_id_, mview_id))) {
        LOG_WARN("fail to init stats collection", KR(ret));
      } else if (OB_FAIL(mv_ref_stats_map_.set_refactored(mview_id, new_stats_collection))) {
        LOG_WARN("fail to set refactored", KR(ret));
      } else {
        stats_collection = new_stats_collection;
      }
      if (OB_FAIL(ret)) {
        if (nullptr != new_stats_collection) {
          new_stats_collection->~ObMViewRefreshStatsCollection();
          allocator_.free(new_stats_collection);
          new_stats_collection = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObMViewRefreshStatsCollector::commit()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMViewRefreshStatsCollector not init", KR(ret), KP(this));
  } else {
    run_stats_.end_time_ = ObTimeUtil::current_time();
    run_stats_.elapsed_time_ = (run_stats_.end_time_ - run_stats_.start_time_) / 1000 / 1000;

    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(ctx_->get_sql_proxy(), tenant_id_))) {
      LOG_WARN("fail to start trans", KR(ret));
    }
    FOREACH_X(iter, mv_ref_stats_map_, OB_SUCC(ret))
    {
      ObMViewRefreshStatsCollection *collection = iter->second;
      run_stats_.log_purge_time_ += collection->refresh_stats_.log_purge_time_;
      collection->refresh_stats_.result_ != OB_SUCCESS ? run_stats_.number_of_failures_++ : NULL;
      if (ObMVRefreshStatsCollectionLevel::NONE != collection->collection_level_) {
        if (OB_FAIL(collection->commit(trans))) {
          LOG_WARN("fail to commit collection", KR(ret), KPC(collection));
        } else {
          run_stats_.num_mvs_current_++;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObMViewRefreshRunStats::insert_run_stats(trans, run_stats_))) {
        LOG_WARN("fail to insert run stats", KR(ret), K(run_stats_));
      }
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
