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
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/mv/ob_mv_dep_utils.h"
#include "storage/mview/cmd/ob_mview_executor_util.h"
#include "storage/mview/ob_mview_refresh.h"
#include "storage/mview/ob_mview_mds.h"
#include "storage/mview/ob_mview_refresh_helper.h"
#include "share/scn.h"
#include "share/schema/ob_mview_refresh_stats_params.h"
#include "share/schema/ob_mview_info.h"
#include "share/ob_rpc_struct.h"

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
  heap_size_ = other.heap_size_;
  atomic_refresh_ = other.atomic_refresh_;
  nested_ = other.nested_;
  out_of_place_ = other.out_of_place_;
  skip_ext_data_ = other.skip_ext_data_;
  refresh_parallel_ = other.refresh_parallel_;
}

ObMViewRefreshExecutor::ObMViewRefreshExecutor()
  : ctx_(nullptr), arg_(nullptr), session_info_(nullptr),
    tenant_id_(OB_INVALID_TENANT_ID), refresh_id_(OB_INVALID_ID),
    target_data_sync_scn_(),
    refresh_method_(share::schema::ObMVRefreshMethod::MAX)
{
}

ObMViewRefreshExecutor::~ObMViewRefreshExecutor() {}

int ObMViewRefreshExecutor::execute(ObExecContext &ctx, const ObMViewRefreshArg &arg)
{
  int ret = OB_SUCCESS;
  uint64_t mview_id = OB_INVALID_ID;
  ctx_ = &ctx;
  arg_ = &arg;
  session_info_ = ctx.get_my_session();
  refresh_method_ = ObMVRefreshMethod::MAX;
  const int64_t start_time = ObTimeUtil::current_time();
  common::ObISQLClient *sql_proxy = NULL;
  ObSEArray<uint64_t, 4> nested_mview_ids;
  if (OB_ISNULL(session_info_) || OB_ISNULL(ctx.get_sql_ctx()->schema_guard_) || OB_UNLIKELY(!arg.is_valid())
      || OB_ISNULL(sql_proxy = ctx_->get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ctx or arg", K(ret), K(session_info_), K(arg), K(sql_proxy));
  } else if (OB_FALSE_IT(tenant_id_ = session_info_->get_effective_tenant_id())) {
  } else if (OB_FAIL(ObMViewExecutorUtil::resolve_mview_list_and_method(ctx.get_sql_ctx()->schema_guard_, session_info_, arg.list_, arg.method_, mview_id, refresh_method_))) {
    LOG_WARN("fail to resolve mview list and method", KR(ret));
  } else if (OB_INVALID_ID == mview_id) {
    // empty list: do nothing, return success
  } else if (OB_FAIL(ObMViewExecutorUtil::generate_refresh_id(tenant_id_, refresh_id_))) {
    LOG_WARN("fail to generate refresh id", KR(ret));
  } else if (OB_FAIL(ObMViewRefreshHelper::get_current_scn(target_data_sync_scn_))) {
    LOG_WARN("fail to get current scn", KR(ret));
  } else if (OB_FAIL(write_run_start(arg, mview_id, start_time))) {
    LOG_WARN("fail to write_run_start", KR(ret));
  } else if (arg_->nested_
             && OB_FAIL(sql::ObMVDepUtils::get_mview_ids_in_topo_refresh_order(*sql_proxy,
                                                                               tenant_id_,
                                                                               mview_id,
                                                                               nested_mview_ids))) {
    LOG_WARN("fail to get target nested mview refresh info", K(ret), K(mview_id));
  } else if (1 >= nested_mview_ids.count()
             && OB_FAIL(do_refresh(ctx, tenant_id_, refresh_id_, mview_id, refresh_method_, arg.refresh_parallel_, false))) {
    LOG_WARN("fail to do refresh", KR(ret));
  } else if (1 < nested_mview_ids.count() && OB_FAIL(do_nested_refresh(mview_id, nested_mview_ids))) {
    LOG_WARN("fail to do nested refresh", KR(ret));
  }
  if (OB_INVALID_ID != mview_id) {
    const int64_t end_time = ObTimeUtil::current_time();
    const int64_t log_purge_time = 0;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObMViewRefreshStatsUtils::write_run_end(ctx.get_sql_proxy(),
                                                            tenant_id_,
                                                            refresh_id_,
                                                            end_time,
                                                            log_purge_time,
                                                            ret,
                                                            common::ObString(),
                                                            0 /*num_failures, old engine path*/))) {
      LOG_WARN("fail to write_run_end", KR(tmp_ret));
    }
  }
  return ret;
}

int ObMViewRefreshExecutor::write_run_start(const ObMViewRefreshArg &refresh_arg,
                                            const uint64_t mview_id,
                                            const int64_t start_time)
{
  int ret = OB_SUCCESS;
  ObMViewRefreshRunStatsParam run_params;
  run_params.tenant_id_       = tenant_id_;
  run_params.mview_id_        = mview_id;
  run_params.refresh_id_      = refresh_id_;
  run_params.start_time_      = start_time;
  run_params.data_target_scn_ = target_data_sync_scn_.get_val_for_gts();
  run_params.method_          = refresh_arg.method_;
  run_params.parallelism_     = refresh_arg.refresh_parallel_;
  run_params.nested_          = refresh_arg.nested_;
  if (OB_ISNULL(ctx_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("executor state not initialized", KR(ret), KP(ctx_), KP(session_info_));
  } else if (OB_FALSE_IT(run_params.run_user_id_ = session_info_->get_priv_user_id())) {
  } else if (OB_FAIL(ObMViewRefreshStatsUtils::write_run_start(ctx_->get_sql_proxy(), run_params))) {
    LOG_WARN("failed to write_run_start", KR(ret), K(run_params));
  }
  return ret;
}

int ObMViewRefreshExecutor::do_refresh(ObExecContext &ctx,
                                       const uint64_t tenant_id,
                                       const uint64_t refresh_id,
                                       const uint64_t mview_id,
                                       const ObMVRefreshMethod refresh_method_arg,
                                       const int64_t refresh_parallel,
                                       const bool nested_consistent_refresh)
{
  int ret = OB_SUCCESS;
  ObMViewRefreshParam refresh_param(tenant_id, mview_id, refresh_id, refresh_method_arg, refresh_parallel);
  refresh_param.retry_id_ = 0;
  refresh_param.is_consistent_refresh_ = nested_consistent_refresh;
  refresh_param.target_data_sync_scn_ = target_data_sync_scn_;
  bool finish = false;
  while (!finish && OB_SUCC(ret) && OB_SUCC(ctx.check_status())) {
    ObMViewRefresher refresher(ctx, refresh_param);
    if (OB_SUCC(refresher.refresh())) {
      finish = true;
    } else if (ObMViewExecutorUtil::is_mview_refresh_retry_ret_code(ret)) {
      LOG_INFO("get do refresh retry ret code", KR(ret), K(refresh_param));
      ret = OB_SUCCESS;
      ++refresh_param.retry_id_;
      ob_usleep(1LL * 1000 * 1000);
    } else {
      LOG_WARN("fail to do refresh", KR(ret), K(refresh_param));
    }
  }
  return ret;
}

// step 1: register mview list mds into sys ls memory and sync check on SYS LS
// step 2: schedule nested refresh in topo order under the registered target scn
int ObMViewRefreshExecutor::do_nested_refresh(const uint64_t target_mview_id,
                                              const ObIArray<uint64_t> &nested_mview_ids)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const SCN &target_data_sync_scn = target_data_sync_scn_;
  ObMySQLTransaction trans;
  int64_t start_ts = ObTimeUtility::fast_current_time();
  common::ObISQLClient *sql_proxy = nullptr;
  if (OB_ISNULL(ctx_) || OB_ISNULL(arg_) || OB_ISNULL(session_info_)
      || OB_ISNULL(sql_proxy = ctx_->get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ctx_), K(arg_), K(session_info_), K(sql_proxy));
  } else if (OB_FAIL(trans.start(sql_proxy, tenant_id_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else if (OB_FAIL(register_nested_mview_mds_and_check(target_mview_id,
                      refresh_id_, nested_mview_ids, target_data_sync_scn, trans))) {
    LOG_WARN("fail to register and check mds", K(ret));
  } else if (OB_FALSE_IT(DEBUG_SYNC(BEFORE_NESTED_MV_GET_SCN))) {
  } else if (OB_FAIL(scheduler_nested_mviews_sync_refresh(target_mview_id, refresh_id_,
                      target_data_sync_scn, nested_mview_ids, trans))) {
    LOG_WARN("fail to scheduler nested mview refresh", K(ret));
  }
  if (trans.is_started()) {
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end trans", K(ret), K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  int64_t end_ts = ObTimeUtility::fast_current_time();
  LOG_INFO("do nested refresh", K(ret), K(target_mview_id), K(nested_mview_ids), K(end_ts - start_ts));
  return ret;
}

int ObMViewRefreshExecutor::sync_check_nested_mview_mds(
                            const uint64_t mview_id,
                            const uint64_t refresh_id,
                            const SCN &target_data_sync_scn)
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
        usleep(200 * 1000); // sleep 100ms
      }
    } while (OB_NOT_MASTER == ret ||
            OB_EAGAIN == ret);
  }
  return ret;
}

int ObMViewRefreshExecutor::register_nested_mview_mds(
                            const uint64_t mview_id,
                            const uint64_t refresh_id,
                            const ObIArray<uint64_t> &nest_mview_ids,
                            const SCN &target_data_sync_scn,
                            common::ObMySQLTransaction &trans)
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
                            const SCN &target_data_sync_scn,
                            common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (mview_id == OB_INVALID_ID || refresh_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(mview_id), K(refresh_id));
  } else if (OB_FAIL(register_nested_mview_mds(mview_id, refresh_id, nest_mview_ids,
                                               target_data_sync_scn, trans))) {
    LOG_WARN("fail to register nested mview mds", K(ret),
             K(mview_id), K(target_data_sync_scn));
  } else if (OB_FAIL(sync_check_nested_mview_mds(
                     mview_id, refresh_id, target_data_sync_scn))) {
    LOG_WARN("fail to sync check nested mview mds", K(ret),
             K(mview_id), K(target_data_sync_scn));
  }

  return ret;
}

// schedule each mview in topo order:
// step 1: read min target scn from MDS and skip if a newer target is in flight
// step 2: do refresh mview against the registered target scn
// step 3: drop MDS records of already-refreshed mviews to unblock concurrent refresh
int ObMViewRefreshExecutor::scheduler_nested_mviews_sync_refresh(
                            const uint64_t target_mview_id,
                            const uint64_t refresh_id,
                            const SCN &target_data_sync_scn,
                            const ObIArray<uint64_t> &nested_mview_ids,
                            ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  SCN current_scn; // invalid scn
  ObSEArray<uint64_t, 4> mds_locked_mview_ids;
  uint64_t mds_locked_mview_cnt = nested_mview_ids.count();
  ObISQLClient *sql_client = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(arg_) || OB_ISNULL(sql_client = ctx_->get_sql_proxy())
      || OB_UNLIKELY(target_mview_id == OB_INVALID_ID || refresh_id == OB_INVALID_ID)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret), K(ctx_), K(arg_), K(sql_client), K(target_mview_id), K(refresh_id));
  } else {
    ARRAY_FOREACH(nested_mview_ids, idx) {
      DEBUG_SYNC(BEFORE_NESTED_MV_REFRESH);
      const uint64_t mview_id = nested_mview_ids.at(idx);
      bool need_retry = false;
      do {
        SCN min_target_data_sync_scn;
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
        } else if (OB_FAIL(do_refresh(*ctx_, tenant_id_, refresh_id, mview_id, refresh_method_, arg_->refresh_parallel_, true))) {
          LOG_WARN("fail to do refresh", KR(ret));
        } else {
          need_retry = false;
          // optimise to avoid block other sync refresh
          int tmp_ret = OB_SUCCESS;
          mds_locked_mview_ids.reuse();
          const int64_t unrefreshed_mv_count = nested_mview_ids.count() - idx - 1;
          if (OB_TMP_FAIL(ObMVDepUtils::get_mds_locked_mview_ids(*sql_client,
             tenant_id_, target_mview_id, unrefreshed_mv_count, mds_locked_mview_ids))) {
             LOG_WARN("fail to get remaining nested mview dep info", K(ret));
          } else if (mds_locked_mview_ids.count() == mds_locked_mview_cnt) {
            // do nothing
          } else if (OB_TMP_FAIL(register_nested_mview_mds(target_mview_id,
                        refresh_id, mds_locked_mview_ids, target_data_sync_scn, trans))) {
            LOG_WARN("fail to register mds locked mview list", K(ret));
          } else {
            mds_locked_mview_cnt = mds_locked_mview_ids.count();
          }
        }
        if (need_retry) {
          usleep(1 * 1000 * 1000); // 1s
        }
        LOG_INFO("do nested refresh mview", K(ret), KPC(arg_), K(mview_id), K(idx), K(need_retry));
      } while (need_retry && OB_SUCC(ret) && OB_SUCC(ctx_->check_status()));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
