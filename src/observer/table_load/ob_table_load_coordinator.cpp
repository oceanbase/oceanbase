// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/ob_server.h"
#include "observer/table_load/ob_table_load_coordinator_ctx.h"
#include "observer/table_load/ob_table_load_coordinator_trans.h"
#include "observer/table_load/ob_table_load_redef_table.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "observer/table_load/ob_table_load_store.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task.h"
#include "observer/table_load/ob_table_load_task_scheduler.h"
#include "observer/table_load/ob_table_load_trans_bucket_writer.h"
#include "observer/table_load/ob_table_load_utils.h"
#include "share/ob_share_util.h"
#include "share/table/ob_table_load_rpc_struct.h"
#include "share/stat/ob_incremental_stat_estimator.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace table;
using namespace share;
using namespace sql;

#define TABLE_LOAD_RPC_CALL(name, addr, request, ...)                             \
  if (OB_SUCC(ret)) {                                                             \
    ObTimeoutCtx ctx;                                                             \
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, DEFAULT_TIMEOUT_US))) { \
      LOG_WARN("fail to set default timeout ctx", KR(ret));                       \
    } else if (OB_FAIL(ObServer::get_instance()                                   \
                         .get_table_rpc_proxy()                                   \
                         .to(addr)                                                \
                         .timeout(ctx.get_timeout())                              \
                         .by(MTL_ID())                                            \
                         .name(request, ##__VA_ARGS__))) {                        \
      LOG_WARN("fail to rpc call " #name, KR(ret), K(addr), K(request));          \
    }                                                                             \
  }

ObTableLoadCoordinator::ObTableLoadCoordinator(ObTableLoadTableCtx *ctx)
  : ctx_(ctx),
    param_(ctx->param_),
    coordinator_ctx_(ctx->coordinator_ctx_),
    is_inited_(false)
{
}

bool ObTableLoadCoordinator::is_ctx_inited(ObTableLoadTableCtx *ctx)
{
  bool ret = false;
  if (OB_NOT_NULL(ctx) && OB_LIKELY(ctx->is_valid()) && nullptr != ctx->coordinator_ctx_ &&
      OB_LIKELY(ctx->coordinator_ctx_->is_valid())) {
    ret = true;
  }
  return ret;
}

int ObTableLoadCoordinator::init_ctx(ObTableLoadTableCtx *ctx, const ObIArray<int64_t> &idx_array,
                                     uint64_t user_id, ObTableLoadExecCtx *exec_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid agrs", KR(ret));
  } else if (OB_FAIL(ctx->init_coordinator_ctx(idx_array, user_id, exec_ctx))) {
    LOG_WARN("fail to init coordinator ctx", KR(ret));
  }
  return ret;
}

void ObTableLoadCoordinator::abort_ctx(ObTableLoadTableCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ctx || !ctx->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(ctx));
  } else if (OB_UNLIKELY(nullptr == ctx->coordinator_ctx_ || !ctx->coordinator_ctx_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid coordinator ctx", KR(ret), KP(ctx->coordinator_ctx_));
  } else {
    LOG_INFO("coordinator abort");
    // 1. mark status abort, speed up background task exit
    if (OB_FAIL(ctx->coordinator_ctx_->set_status_abort())) {
      LOG_WARN("fail to set coordinator status abort", KR(ret));
    }
    // 2. mark all active trans abort
    if (OB_FAIL(abort_active_trans(ctx))) {
      LOG_WARN("fail to abort active trans", KR(ret));
    }
    // 3. abort peers ctx
    if (OB_FAIL(abort_peers_ctx(ctx))) {
      LOG_WARN("fail to abort peers ctx", KR(ret));
    }
    // 4. abort redef table, release table lock
    if (OB_FAIL(abort_redef_table(ctx))) {
      LOG_WARN("fail to abort redef table", KR(ret));
    }
  }
}

int ObTableLoadCoordinator::abort_active_trans(ObTableLoadTableCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObTableLoadTransId> trans_id_array;
  if (OB_FAIL(ctx->coordinator_ctx_->get_active_trans_ids(trans_id_array))) {
    LOG_WARN("fail to get active trans ids", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < trans_id_array.count(); ++i) {
    const ObTableLoadTransId &trans_id = trans_id_array.at(i);
    ObTableLoadCoordinatorTrans *trans = nullptr;
    if (OB_FAIL(ctx->coordinator_ctx_->get_trans(trans_id, trans))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get trans", KR(ret), K(trans_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(trans->set_trans_status_abort())) {
      LOG_WARN("fail to set trans status abort", KR(ret));
    }
    if (OB_NOT_NULL(trans)) {
      ctx->coordinator_ctx_->put_trans(trans);
      trans = nullptr;
    }
  }
  return ret;
}

int ObTableLoadCoordinator::abort_peers_ctx(ObTableLoadTableCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadArray<ObAddr> all_addr_array;
  if (OB_FAIL(ctx->coordinator_ctx_->partition_location_.get_all_leader(all_addr_array))) {
    LOG_WARN("fail to get all addr", KR(ret));
  } else {
    LOG_INFO("route_abort_peer_request begin", K(all_addr_array.count()));
    ObTableLoadAbortPeerRequest request;
    ObTableLoadAbortPeerResult result;
    request.credential_ = ctx->coordinator_ctx_->credential_;
    request.table_id_ = ctx->param_.table_id_;
    request.task_id_ = ctx->ddl_param_.task_id_;
    for (int64_t i = 0; i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ObTableLoadStore::abort_ctx(ctx);
      } else { // 远端, 发送rpc
        const int64_t origin_timeout_ts = THIS_WORKER.get_timeout_ts();
        THIS_WORKER.set_timeout_ts(INT64_MAX); // use default timeout value, avoid timeout now
        TABLE_LOAD_RPC_CALL(load_abort_peer, addr, request, result);
        THIS_WORKER.set_timeout_ts(origin_timeout_ts);
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinator::abort_redef_table(ObTableLoadTableCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObTableLoadRedefTableAbortArg arg;
  arg.tenant_id_ = ctx->param_.tenant_id_;
  arg.task_id_ = ctx->ddl_param_.task_id_;
  if (OB_FAIL(
        ObTableLoadRedefTable::abort(arg, *ctx->coordinator_ctx_->exec_ctx_->get_session_info()))) {
    LOG_WARN("fail to abort redef table", KR(ret), K(arg));
  }
  return ret;
}

int ObTableLoadCoordinator::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadCoordinator init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!ctx_->is_valid()) || OB_ISNULL(coordinator_ctx_) ||
             OB_UNLIKELY(!coordinator_ctx_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC_(ctx));
  } else if (THIS_WORKER.is_timeout_ts_valid() && OB_UNLIKELY(THIS_WORKER.is_timeout())) {
    ret = OB_TIMEOUT;
    LOG_WARN("worker timeouted", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

/**
 * begin
 */

int ObTableLoadCoordinator::pre_begin_peers()
{
  int ret = OB_SUCCESS;
  ObTableLoadArray<ObTableLoadPartitionLocation::LeaderInfo> all_leader_info_array;
  ObTableLoadArray<ObTableLoadPartitionLocation::LeaderInfo> target_all_leader_info_array;
  if (OB_FAIL(coordinator_ctx_->partition_location_.get_all_leader_info(all_leader_info_array))) {
    LOG_WARN("fail to get all leader info", KR(ret));
  } else if (OB_FAIL(coordinator_ctx_->target_partition_location_.get_all_leader_info(target_all_leader_info_array))) {
    LOG_WARN("fail to get all leader info", KR(ret));
  } else if (all_leader_info_array.count() != target_all_leader_info_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "origin table leader count must be qual to target table leader count",
        K(all_leader_info_array.count()),
        K(target_all_leader_info_array.count()), KR(ret));
  } else {
    LOG_INFO("route_pre_begin_peer_request begin", K(all_leader_info_array.count()));
    ObTableLoadPreBeginPeerRequest request;
    ObTableLoadPreBeginPeerResult result;
    request.credential_ = coordinator_ctx_->credential_;
    request.table_id_ = param_.table_id_;
    request.config_.session_count_ = param_.session_count_;
    request.config_.max_error_row_count_ = param_.max_error_row_count_;
    request.config_.batch_size_ = param_.batch_size_;
    request.config_.flag_.is_need_sort_ = param_.need_sort_;
    request.column_count_ = param_.column_count_;
    request.dup_action_ = param_.dup_action_;
    request.px_mode_ = param_.px_mode_;
    request.online_opt_stat_gather_ = param_.online_opt_stat_gather_;
    request.dest_table_id_ = ctx_->ddl_param_.dest_table_id_;
    request.task_id_ = ctx_->ddl_param_.task_id_;
    request.schema_version_ = ctx_->ddl_param_.schema_version_;
    request.snapshot_version_ = ctx_->ddl_param_.snapshot_version_;
    request.data_version_ = ctx_->ddl_param_.data_version_;
    request.session_info_ = ctx_->session_info_;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_leader_info_array.count(); ++i) {
      const ObTableLoadPartitionLocation::LeaderInfo &leader_info = all_leader_info_array.at(i);
      const ObTableLoadPartitionLocation::LeaderInfo &target_leader_info = target_all_leader_info_array.at(i);
      //目前源表和目标表的分区信息连同每个分区的地址都完全一样
      const ObAddr &addr = leader_info.addr_;
      if (OB_UNLIKELY(leader_info.addr_ != target_leader_info.addr_)) {
        LOG_INFO("addr must be same", K(leader_info.addr_), K(target_leader_info.addr_));
      }
      request.partition_id_array_ = leader_info.partition_id_array_;
      request.target_partition_id_array_ = target_leader_info.partition_id_array_;
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        if (OB_FAIL(ObTableLoadStore::init_ctx(ctx_, request.partition_id_array_,
                                                request.target_partition_id_array_))) {
          LOG_WARN("fail to store init ctx", KR(ret));
        } else {
          ObTableLoadStore store(ctx_);
          if (OB_FAIL(store.init())) {
            LOG_WARN("fail to init store", KR(ret));
          } else if (OB_FAIL(store.pre_begin())) {
            LOG_WARN("fail to store pre begin", KR(ret));
          }
        }
      } else { // 对端, 发送rpc
        TABLE_LOAD_RPC_CALL(load_pre_begin_peer, addr, request, result);
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinator::confirm_begin_peers()
{
  int ret = OB_SUCCESS;
  ObTableLoadArray<ObAddr> all_addr_array;
  if (OB_FAIL(coordinator_ctx_->partition_location_.get_all_leader(all_addr_array))) {
    LOG_WARN("fail to get all addr", KR(ret));
  } else {
    LOG_INFO("route_confirm_begin_peer_request begin", K(all_addr_array.count()));
    ObTableLoadConfirmBeginPeerRequest request;
    ObTableLoadConfirmBeginPeerResult result;
    request.credential_ = coordinator_ctx_->credential_;
    request.table_id_ = param_.table_id_;
    request.task_id_ = ctx_->ddl_param_.task_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ObTableLoadStore store(ctx_);
        if (OB_FAIL(store.init())) {
          LOG_WARN("fail to init store", KR(ret));
        } else if (OB_FAIL(store.confirm_begin())) {
          LOG_WARN("fail to store confirm begin", KR(ret));
        }
      } else { // 对端, 发送rpc
        TABLE_LOAD_RPC_CALL(load_confirm_begin_peer, addr, request, result);
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinator::begin()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else {
    LOG_INFO("coordinator begin");
    ObMutexGuard guard(coordinator_ctx_->get_op_lock());
    if (OB_FAIL(coordinator_ctx_->check_status(ObTableLoadStatusType::INITED))) {
      LOG_WARN("fail to check status", KR(ret));
    } else if (OB_FAIL(pre_begin_peers())) {
      LOG_WARN("fail to pre begin peers", KR(ret));
    } else if (OB_FAIL(confirm_begin_peers())) {
      LOG_WARN("fail to confirm begin peers", KR(ret));
    } else if (OB_FAIL(coordinator_ctx_->set_status_loading())) {
      LOG_WARN("fail to set coordinator status loading", KR(ret));
    }
  }
  return ret;
}

/**
 * finish
 */

int ObTableLoadCoordinator::pre_merge_peers()
{
  int ret = OB_SUCCESS;
  ObTableLoadArray<ObAddr> all_addr_array;
  if (OB_FAIL(coordinator_ctx_->partition_location_.get_all_leader(all_addr_array))) {
    LOG_WARN("fail to get all addr", KR(ret));
  } else {
    LOG_INFO("route_pre_merge_peer_request begin", K(all_addr_array.count()));
    ObArenaAllocator allocator("TLD_Coord");
    ObTableLoadPreMergePeerRequest request;
    ObTableLoadPreMergePeerResult result;
    allocator.set_tenant_id(MTL_ID());
    request.credential_ = coordinator_ctx_->credential_;
    request.table_id_ = param_.table_id_;
    request.task_id_ = ctx_->ddl_param_.task_id_;
    if (!ctx_->param_.px_mode_) {
      if (OB_FAIL(coordinator_ctx_->get_committed_trans_ids(request.committed_trans_id_array_,
                                                            allocator))) {
        LOG_WARN("fail to get committed trans ids", KR(ret));
      } else {
        std::sort(request.committed_trans_id_array_.begin(), request.committed_trans_id_array_.end());
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ObTableLoadStore store(ctx_);
        if (OB_FAIL(store.init())) {
          LOG_WARN("fail to init store", KR(ret));
        } else if (OB_FAIL(store.pre_merge(request.committed_trans_id_array_))) {
          LOG_WARN("fail to store pre merge", KR(ret));
        }
      } else { // 远端, 发送rpc
        TABLE_LOAD_RPC_CALL(load_pre_merge_peer, addr, request, result);
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinator::start_merge_peers()
{
  int ret = OB_SUCCESS;
  ObTableLoadArray<ObAddr> all_addr_array;
  if (OB_FAIL(coordinator_ctx_->partition_location_.get_all_leader(all_addr_array))) {
    LOG_WARN("fail to get all addr", KR(ret));
  } else {
    LOG_INFO("route_start_merge_peer_request begin", K(all_addr_array.count()));
    ObTableLoadStartMergePeerRequest request;
    ObTableLoadStartMergePeerResult result;
    request.credential_ = coordinator_ctx_->credential_;
    request.table_id_ = param_.table_id_;
    request.task_id_ = ctx_->ddl_param_.task_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ObTableLoadStore store(ctx_);
        if (OB_FAIL(store.init())) {
          LOG_WARN("fail to init store", KR(ret));
        } else if (OB_FAIL(store.start_merge())) {
          LOG_WARN("fail to store start merge", KR(ret));
        }
      } else { // 远端, 发送rpc
        TABLE_LOAD_RPC_CALL(load_start_merge_peer, addr, request, result);
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinator::finish()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else {
    LOG_INFO("coordinator finish");
    ObMutexGuard guard(coordinator_ctx_->get_op_lock());
    bool active_trans_exist = false;
    bool committed_trans_eixst = false;
    // 1. 冻结状态, 防止后续继续创建trans
    if (OB_FAIL(coordinator_ctx_->set_status_frozen())) {
      LOG_WARN("fail to set coordinator status frozen", KR(ret));
    }
    // 2. 检查当前是否还有trans没有结束
    else if (OB_FAIL(coordinator_ctx_->check_exist_trans(active_trans_exist))) {
      LOG_WARN("fail to check exist trans", KR(ret));
    } else if (OB_UNLIKELY(active_trans_exist)) {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("trans already exist", KR(ret));
    } else if (!ctx_->param_.px_mode_) {
      // 3. 检查是否有数据
      if (OB_FAIL(coordinator_ctx_->check_exist_committed_trans(committed_trans_eixst))) {
        LOG_WARN("fail to check exist committed trans", KR(ret));
      } else if (OB_UNLIKELY(!committed_trans_eixst)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("segment is null", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // 4. 触发数据节点发起合并
      if (OB_FAIL(pre_merge_peers())) {
        LOG_WARN("fail to pre merge peers", KR(ret));
      } else if (OB_FAIL(start_merge_peers())) {
        LOG_WARN("fail to start merge peers", KR(ret));
      }
      // 5. 设置当前状态为合并中
      else if (OB_FAIL(coordinator_ctx_->set_status_merging())) {
        LOG_WARN("fail to set coordinator status merging", KR(ret));
      }
      // 6. 添加定时任务检查合并结果
      else if (OB_FAIL(add_check_merge_result_task())) {
        LOG_WARN("fail to add check merge result task", KR(ret));
      }
    }
  }
  return ret;
}

/**
 * check merge result
 */

int ObTableLoadCoordinator::check_peers_merge_result(bool &is_finish)
{
  int ret = OB_SUCCESS;
  ObTableLoadArray<ObAddr> all_addr_array;
  if (OB_FAIL(coordinator_ctx_->partition_location_.get_all_leader(all_addr_array))) {
    LOG_WARN("fail to get all addr", KR(ret));
  } else {
    LOG_INFO("route_get_status_peer_request begin", K(all_addr_array.count()));
    ObTableLoadGetStatusPeerRequest request;
    ObTableLoadGetStatusPeerResult result;
    request.credential_ = coordinator_ctx_->credential_;
    request.table_id_ = param_.table_id_;
    request.task_id_ = ctx_->ddl_param_.task_id_;
    is_finish = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ObTableLoadStore store(ctx_);
        if (OB_FAIL(store.init())) {
          LOG_WARN("fail to init store", KR(ret));
        } else if (OB_FAIL(store.get_status(result.status_, result.error_code_))) {
          LOG_WARN("fail to store get status", KR(ret));
        }
      } else { // 远端, 发送rpc
        TABLE_LOAD_RPC_CALL(load_get_status_peer, addr, request, result);
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(ObTableLoadStatusType::ERROR == result.status_)) {
          ret = result.error_code_;
          LOG_WARN("store has error", KR(ret), K(addr), K(result.status_));
        } else if (OB_UNLIKELY(ObTableLoadStatusType::MERGING != result.status_ &&
                               ObTableLoadStatusType::MERGED != result.status_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected peer status", KR(ret), K(addr), K(result.status_));
        } else if (ObTableLoadStatusType::MERGED != result.status_) {
          is_finish = false;
        }
      }
    }
  }
  return ret;
}

class ObTableLoadCoordinator::CheckMergeResultTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  CheckMergeResultTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx)
    : ObITableLoadTaskProcessor(task), ctx_(ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~CheckMergeResultTaskProcessor()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    int ret = OB_SUCCESS;
    bool is_merge_finish = false;
    ObTableLoadCoordinator coordinator(ctx_);
    if (OB_FAIL(coordinator.init())) {
      LOG_WARN("fail to init coordinator", KR(ret));
    }
    while (OB_SUCC(ret)) {
      // 确认状态
      if (OB_FAIL(ctx_->coordinator_ctx_->check_status(ObTableLoadStatusType::MERGING))) {
        LOG_WARN("fail to check coordinator status merging", KR(ret));
      }
      // 查询合并状态
      else if (OB_FAIL(coordinator.check_peers_merge_result(is_merge_finish))) {
        LOG_WARN("fail to check peers merge result", KR(ret));
      } else if (!is_merge_finish) {
        usleep(WAIT_INTERVAL_US);  // 等待1s后重试
      } else {
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ctx_->coordinator_ctx_->set_status_merged())) {
        LOG_WARN("fail to set coordinator status merged", KR(ret));
      }
    }
    return ret;
  }
private:
  ObTableLoadTableCtx * const ctx_;
};

class ObTableLoadCoordinator::CheckMergeResultTaskCallback : public ObITableLoadTaskCallback
{
public:
  CheckMergeResultTaskCallback(ObTableLoadTableCtx *ctx)
    : ctx_(ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~CheckMergeResultTaskCallback()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ret_code)) {
      ctx_->coordinator_ctx_->set_status_error(ret);
    }
    ctx_->free_task(task);
  }
private:
  ObTableLoadTableCtx * const ctx_;
};

int ObTableLoadCoordinator::add_check_merge_result_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else {
    ObTableLoadTask *task = nullptr;
    // 1. 分配task
    if (OB_FAIL(ctx_->alloc_task(task))) {
      LOG_WARN("fail to alloc task", KR(ret));
    }
    // 2. 设置processor
    else if (OB_FAIL(task->set_processor<CheckMergeResultTaskProcessor>(ctx_))) {
      LOG_WARN("fail to set check merge result task processor", KR(ret));
    }
    // 3. 设置callback
    else if (OB_FAIL(task->set_callback<CheckMergeResultTaskCallback>(ctx_))) {
      LOG_WARN("fail to set check merge result task callback", KR(ret));
    }
    // 4. 把task放入调度器
    else if (OB_FAIL(coordinator_ctx_->task_scheduler_->add_task(0, task))) {
      LOG_WARN("fail to add task", KR(ret), KPC(task));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != task) {
        ctx_->free_task(task);
      }
    }
  }
  return ret;
}

/**
 * commit
 */

int ObTableLoadCoordinator::commit_peers(ObTableLoadSqlStatistics &sql_statistics)
{
  int ret = OB_SUCCESS;
  ObTableLoadArray<ObAddr> all_addr_array;
  if (OB_FAIL(coordinator_ctx_->partition_location_.get_all_leader(all_addr_array))) {
    LOG_WARN("fail to get all addr", KR(ret));
  } else {
    LOG_INFO("route_commit_peer_request begin", K(all_addr_array.count()));
    ObTableLoadCommitPeerRequest request;
    ObTableLoadCommitPeerResult result;
    request.credential_ = coordinator_ctx_->credential_;
    request.table_id_ = param_.table_id_;
    request.task_id_ = ctx_->ddl_param_.task_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ObTableLoadStore store(ctx_);
        if (OB_FAIL(store.init())) {
          LOG_WARN("fail to init store", KR(ret));
        } else if (OB_FAIL(store.commit(result.result_info_, result.sql_statistics_))) {
          LOG_WARN("fail to commit store", KR(ret));
        }
      } else { // 远端, 发送rpc
        TABLE_LOAD_RPC_CALL(load_commit_peer, addr, request, result);
      }
      if (OB_SUCC(ret)) {
        ATOMIC_AAF(&coordinator_ctx_->result_info_.rows_affected_, result.result_info_.rows_affected_);
        ATOMIC_AAF(&coordinator_ctx_->result_info_.deleted_, result.result_info_.deleted_);
        ATOMIC_AAF(&coordinator_ctx_->result_info_.skipped_, result.result_info_.skipped_);
        ATOMIC_AAF(&coordinator_ctx_->result_info_.warnings_, result.result_info_.warnings_);
        if (OB_FAIL(sql_statistics.add(result.sql_statistics_))) {
          LOG_WARN("fail to add result sql stats", KR(ret), K(addr), K(result));
        }
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinator::commit_redef_table()
{
  int ret = OB_SUCCESS;
  ObTableLoadRedefTableFinishArg arg;
  arg.tenant_id_ = param_.tenant_id_;
  arg.table_id_ = param_.table_id_;
  arg.dest_table_id_ = ctx_->ddl_param_.dest_table_id_;
  arg.task_id_ = ctx_->ddl_param_.task_id_;
  arg.schema_version_ = ctx_->ddl_param_.schema_version_;
  if (OB_FAIL(
        ObTableLoadRedefTable::finish(arg, *coordinator_ctx_->exec_ctx_->get_session_info()))) {
    LOG_WARN("fail to finish redef table", KR(ret), K(arg));
  }
  return ret;
}

// commit() = px_commit_data() + px_commit_ddl()
// used in non px_mode
int ObTableLoadCoordinator::commit(ObTableLoadResultInfo &result_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else {
    LOG_INFO("coordinator commit");
    ObMutexGuard guard(coordinator_ctx_->get_op_lock());
    ObTableLoadSqlStatistics sql_statistics;
    if (OB_FAIL(coordinator_ctx_->check_status(ObTableLoadStatusType::MERGED))) {
      LOG_WARN("fail to check coordinator status", KR(ret));
    } else if (OB_FAIL(commit_peers(sql_statistics))) {
      LOG_WARN("fail to commit peers", KR(ret));
    } else if (param_.online_opt_stat_gather_ &&
               OB_FAIL(
                 drive_sql_stat(coordinator_ctx_->exec_ctx_->get_exec_ctx(), sql_statistics))) {
      LOG_WARN("fail to drive sql stat", KR(ret));
    } else if (OB_FAIL(commit_redef_table())) {
      LOG_WARN("fail to commit redef table", KR(ret));
    } else if (OB_FAIL(coordinator_ctx_->set_status_commit())) {
      LOG_WARN("fail to set coordinator status commit", KR(ret));
    } else {
      result_info = coordinator_ctx_->result_info_;
    }
  }
  return ret;
}

// used in insert /*+ append */ into select clause
// commit data loaded
int ObTableLoadCoordinator::px_commit_data()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else {
    LOG_INFO("coordinator px_commit_data");
    ObMutexGuard guard(coordinator_ctx_->get_op_lock());
    ObTableLoadSqlStatistics sql_statistics;
    if (OB_FAIL(coordinator_ctx_->check_status(ObTableLoadStatusType::MERGED))) {
      LOG_WARN("fail to check coordinator status", KR(ret));
    } else if (OB_FAIL(commit_peers(sql_statistics))) {
      LOG_WARN("fail to commit peers", KR(ret));
    } else if (param_.online_opt_stat_gather_ &&
               OB_FAIL(
                 drive_sql_stat(coordinator_ctx_->exec_ctx_->get_exec_ctx(), sql_statistics))) {
      LOG_WARN("fail to drive sql stat", KR(ret));
    }
  }
  return ret;
}

// commit ddl procedure
int ObTableLoadCoordinator::px_commit_ddl()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else {
    LOG_INFO("coordinator px_commit_ddl");
    ObMutexGuard guard(coordinator_ctx_->get_op_lock());
    if (OB_FAIL(coordinator_ctx_->check_status(ObTableLoadStatusType::MERGED))) {
      LOG_WARN("fail to check coordinator status", KR(ret));
    } else if (OB_FAIL(commit_redef_table())) {
      LOG_WARN("fail to commit redef table", KR(ret));
    } else if (OB_FAIL(coordinator_ctx_->set_status_commit())) {
      LOG_WARN("fail to set coordinator status commit", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadCoordinator::drive_sql_stat(ObExecContext *ctx,
                                           ObTableLoadSqlStatistics &sql_statistics)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const uint64_t table_id = ctx_->ddl_param_.dest_table_id_;
  ObSchemaGetterGuard schema_guard;
  ObSchemaGetterGuard *tmp_schema_guard = nullptr;
  const ObTableSchema *table_schema = nullptr;
  if (OB_UNLIKELY(nullptr == ctx || sql_statistics.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(ctx), K(sql_statistics));
  } else if (OB_FAIL(ObTableLoadSchema::get_table_schema(tenant_id, table_id, schema_guard,
                                                         table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else {
    tmp_schema_guard = ctx->get_virtual_table_ctx().schema_guard_;
    ctx->get_sql_ctx()->schema_guard_ = &schema_guard;
    ctx->get_das_ctx().set_sql_ctx(ctx->get_sql_ctx());
  }
  ObSEArray<ObOptColumnStat *, 64> part_column_stats;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql_statistics.get_col_stat_array(part_column_stats))) {
    LOG_WARN("failed to get column stat array");
  } else if (OB_FAIL(ObIncrementalStatEstimator::derive_global_stat_by_direct_load(
               *ctx, sql_statistics.table_stat_array_, part_column_stats))) {
    LOG_WARN("fail to drive global stat by direct load", KR(ret));
  }
  ctx->get_sql_ctx()->schema_guard_ = tmp_schema_guard;
  ctx->get_das_ctx().set_sql_ctx(ctx->get_sql_ctx());
  return ret;
}

/**
 * get status
 */

int ObTableLoadCoordinator::get_status(ObTableLoadStatusType &status, int &error_code)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else {
    LOG_INFO("coordinator get status");
    coordinator_ctx_->get_status(status, error_code);
  }
  return ret;
}

/**
 * start trans
 */

int ObTableLoadCoordinator::pre_start_trans_peers(ObTableLoadCoordinatorTrans *trans)
{
  int ret = OB_SUCCESS;
  ObTableLoadArray<ObAddr> all_addr_array;
  if (OB_FAIL(coordinator_ctx_->partition_location_.get_all_leader(all_addr_array))) {
    LOG_WARN("fail to get all addr", KR(ret));
  } else {
    LOG_INFO("route_pre_start_trans_peer_request begin", K(all_addr_array.count()));
    const ObTableLoadTransId &trans_id = trans->get_trans_id();
    ObTableLoadPreStartTransPeerRequest request;
    ObTableLoadPreStartTransPeerResult result;
    request.credential_ = coordinator_ctx_->credential_;
    request.table_id_ = param_.table_id_;
    request.task_id_ = ctx_->ddl_param_.task_id_;
    request.trans_id_ = trans_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ObTableLoadStore store(ctx_);
        if (OB_FAIL(store.init())) {
          LOG_WARN("fail to init store", KR(ret));
        } else if (OB_FAIL(store.pre_start_trans(trans_id))) {
          LOG_WARN("fail to store pre start trans", KR(ret), K(trans_id));
        }
      } else { // 远端, 发送rpc
        TABLE_LOAD_RPC_CALL(load_pre_start_trans_peer, addr, request, result);
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinator::confirm_start_trans_peers(ObTableLoadCoordinatorTrans *trans)
{
  int ret = OB_SUCCESS;
  ObTableLoadArray<ObAddr> all_addr_array;
  if (OB_FAIL(coordinator_ctx_->partition_location_.get_all_leader(all_addr_array))) {
    LOG_WARN("fail to get all addr", KR(ret));
  } else {
    LOG_INFO("route_confirm_start_trans_peer_request begin", K(all_addr_array.count()));
    const ObTableLoadTransId &trans_id = trans->get_trans_id();
    ObTableLoadConfirmStartTransPeerRequest request;
    ObTableLoadConfirmStartTransPeerResult result;
    request.credential_ = coordinator_ctx_->credential_;
    request.table_id_ = param_.table_id_;
    request.task_id_ = ctx_->ddl_param_.task_id_;
    request.trans_id_ = trans_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ObTableLoadStore store(ctx_);
        if (OB_FAIL(store.init())) {
          LOG_WARN("fail to init store", KR(ret));
        } else if (OB_FAIL(store.confirm_start_trans(trans_id))) {
          LOG_WARN("fail to store confirm start trans", KR(ret), K(trans_id));
        }
      } else { // 远端, 发送rpc
        TABLE_LOAD_RPC_CALL(load_confirm_start_trans_peer, addr, request, result);
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinator::start_trans(const ObTableLoadSegmentID &segment_id,
                                        ObTableLoadTransId &trans_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else {
    LOG_INFO("coordinator start trans", K(segment_id));
    ObTableLoadTransCtx *trans_ctx = nullptr;
    while (OB_SUCC(ret) && nullptr == trans_ctx) {
      if (OB_FAIL(coordinator_ctx_->get_segment_trans_ctx(segment_id, trans_ctx))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          LOG_WARN("fail to get segment trans ctx", KR(ret), K(segment_id));
        } else {
          ObTableLoadCoordinatorTrans *trans = nullptr;
          if (OB_FAIL(coordinator_ctx_->start_trans(segment_id, trans))) {
            if (OB_UNLIKELY(OB_ENTRY_EXIST != ret)) {
              LOG_WARN("fail to create trans", KR(ret));
            } else {
              ret = OB_SUCCESS;
            }
          }
          // 2. 同步到对端
          else if (OB_FAIL(pre_start_trans_peers(trans))) {
            LOG_WARN("fail to pre start trans peers", KR(ret));
          } else if (OB_FAIL(confirm_start_trans_peers(trans))) {
            LOG_WARN("fail to confirm start trans peers", KR(ret));
          }
          // 3. 状态设置为running
          else if (OB_FAIL(trans->set_trans_status_running())) {
            LOG_WARN("fail to set trans status running", KR(ret));
          } else {
            trans_ctx = trans->get_trans_ctx();
          }
          if (OB_NOT_NULL(trans)) {
            coordinator_ctx_->put_trans(trans);
            trans = nullptr;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      trans_id = trans_ctx->trans_id_;
    }
  }
  return ret;
}

/**
 * finish trans
 */

int ObTableLoadCoordinator::pre_finish_trans_peers(ObTableLoadCoordinatorTrans *trans)
{
  int ret = OB_SUCCESS;
  ObTableLoadArray<ObAddr> all_addr_array;
  if (OB_FAIL(coordinator_ctx_->partition_location_.get_all_leader(all_addr_array))) {
    LOG_WARN("fail to get all addr", KR(ret));
  } else {
    LOG_INFO("route_pre_finish_trans_peer_request begin", K(all_addr_array.count()));
    const ObTableLoadTransId &trans_id = trans->get_trans_id();
    ObTableLoadPreFinishTransPeerRequest request;
    ObTableLoadPreFinishTransPeerResult result;
    request.credential_ = coordinator_ctx_->credential_;
    request.table_id_ = param_.table_id_;
    request.task_id_ = ctx_->ddl_param_.task_id_;
    request.trans_id_ = trans_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ObTableLoadStore store(ctx_);
        if (OB_FAIL(store.init())) {
          LOG_WARN("fail to init store", KR(ret));
        } else if (OB_FAIL(store.pre_finish_trans(trans_id))) {
          LOG_WARN("fail to store pre finish trans", KR(ret), K(trans_id));
        }
      } else { // 远端, 发送rpc
        TABLE_LOAD_RPC_CALL(load_pre_finish_trans_peer, addr, request, result);
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinator::confirm_finish_trans_peers(ObTableLoadCoordinatorTrans *trans)
{
  int ret = OB_SUCCESS;
  ObTableLoadArray<ObAddr> all_addr_array;
  if (OB_FAIL(coordinator_ctx_->partition_location_.get_all_leader(all_addr_array))) {
    LOG_WARN("fail to get all addr", KR(ret));
  } else {
    LOG_INFO("route_pre_finish_trans_peer_request begin", K(all_addr_array.count()));
    const ObTableLoadTransId &trans_id = trans->get_trans_id();
    ObTableLoadConfirmFinishTransPeerRequest request;
    ObTableLoadConfirmFinishTransPeerResult result;
    request.credential_ = coordinator_ctx_->credential_;
    request.table_id_ = param_.table_id_;
    request.task_id_ = ctx_->ddl_param_.task_id_;
    request.trans_id_ = trans_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ObTableLoadStore store(ctx_);
        if (OB_FAIL(store.init())) {
          LOG_WARN("fail to init store", KR(ret));
        } else if (OB_FAIL(store.confirm_finish_trans(trans_id))) {
          LOG_WARN("fail to store confirm finish trans", KR(ret), K(trans_id));
        }
      } else { // 远端, 发送rpc
        TABLE_LOAD_RPC_CALL(load_confirm_finish_trans_peer, addr, request, result);
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinator::finish_trans(const ObTableLoadTransId &trans_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else {
    LOG_INFO("coordinator finish trans", K(trans_id));
    ObTableLoadCoordinatorTrans *trans = nullptr;
    if (OB_FAIL(coordinator_ctx_->get_trans(trans_id, trans))) {
      LOG_WARN("fail to get trans", KR(ret), K(trans_id));
    } else if (OB_FAIL(trans->set_trans_status_frozen())) {
      LOG_WARN("fail to freeze trans", KR(ret));
    } else if (OB_FAIL(flush(trans))) {
      LOG_WARN("fail to flush", KR(ret));
    }
    if (OB_NOT_NULL(trans)) {
      coordinator_ctx_->put_trans(trans);
      trans = nullptr;
    }
  }
  return ret;
}

/**
 * check peers trans commit
 */

int ObTableLoadCoordinator::check_peers_trans_commit(ObTableLoadCoordinatorTrans *trans, bool &is_commit)
{
  int ret = OB_SUCCESS;
  ObTableLoadArray<ObAddr> all_addr_array;
  if (OB_FAIL(coordinator_ctx_->partition_location_.get_all_leader(all_addr_array))) {
    LOG_WARN("fail to get all addr", KR(ret));
  } else {
    LOG_INFO("route_check_peers_trans_commit begin", K(all_addr_array.count()));
    ObTableLoadGetTransStatusPeerRequest request;
    ObTableLoadGetTransStatusPeerResult result;
    request.credential_ = coordinator_ctx_->credential_;
    request.table_id_ = param_.table_id_;
    request.task_id_ = ctx_->ddl_param_.task_id_;
    request.trans_id_ = trans->get_trans_id();
    is_commit = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ObTableLoadStore store(ctx_);
        if (OB_FAIL(store.init())) {
          LOG_WARN("fail to init store", KR(ret));
        } else if (OB_FAIL(store.get_trans_status(request.trans_id_, result.trans_status_,
                                                  result.error_code_))) {
          LOG_WARN("fail to store get trans status", KR(ret));
        }
      } else { // 远端, 发送rpc
        TABLE_LOAD_RPC_CALL(load_get_trans_status_peer, addr, request, result);
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(ObTableLoadTransStatusType::ERROR == result.trans_status_)) {
          ret = result.error_code_;
          LOG_WARN("trans has error", KR(ret), K(addr));
        } else if (OB_UNLIKELY(ObTableLoadTransStatusType::FROZEN != result.trans_status_ &&
                               ObTableLoadTransStatusType::COMMIT != result.trans_status_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected peer trans status", KR(ret), K(addr), K(result.trans_status_));
        } else if (ObTableLoadTransStatusType::COMMIT != result.trans_status_) {
          is_commit = false;
        }
      }
    }
  }
  return ret;
}

class ObTableLoadCoordinator::CheckPeersTransCommitTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  CheckPeersTransCommitTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx,
                                     ObTableLoadCoordinatorTrans *trans)
    : ObITableLoadTaskProcessor(task), ctx_(ctx), trans_(trans)
  {
    ctx_->inc_ref_count();
    trans_->inc_ref_count();
  }
  virtual ~CheckPeersTransCommitTaskProcessor()
  {
    ctx_->coordinator_ctx_->put_trans(trans_);
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    int ret = OB_SUCCESS;
    bool is_peers_commit = false;
    ObTableLoadCoordinator coordinator(ctx_);
    if (OB_FAIL(coordinator.init())) {
      LOG_WARN("fail to init coordinator", KR(ret));
    }
    while (OB_SUCC(ret)) {
      // 确认trans状态为frozen
      if (OB_FAIL(trans_->check_trans_status(ObTableLoadTransStatusType::FROZEN))) {
        LOG_WARN("fail to check trans status frozen", KR(ret));
      }
      // 向对端发送pre finish
      else if (OB_FAIL(coordinator.check_peers_trans_commit(trans_, is_peers_commit))) {
        LOG_WARN("fail to check peers trans commit", KR(ret));
      } else if (!is_peers_commit) {
        usleep(WAIT_INTERVAL_US);  // 等待1s后重试
      } else {
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(coordinator.confirm_finish_trans_peers(trans_))) {
        LOG_WARN("fail to confirm finish trans peers", KR(ret));
      } else if (OB_FAIL(coordinator.commit_trans(trans_))) {
        LOG_WARN("fail to coordinator commit trans", KR(ret));
      }
    }
    return ret;
  }
private:
  ObTableLoadTableCtx * const ctx_;
  ObTableLoadCoordinatorTrans * const trans_;
};

class ObTableLoadCoordinator::CheckPeersTransCommitTaskCallback : public ObITableLoadTaskCallback
{
public:
  CheckPeersTransCommitTaskCallback(ObTableLoadTableCtx *ctx, ObTableLoadCoordinatorTrans *trans)
    : ctx_(ctx), trans_(trans)
  {
    ctx_->inc_ref_count();
    trans_->inc_ref_count();
  }
  virtual ~CheckPeersTransCommitTaskCallback()
  {
    ctx_->coordinator_ctx_->put_trans(trans_);
    ObTableLoadService::put_ctx(ctx_);
  }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ret_code)) {
      trans_->set_trans_status_error(ret);
    }
    ctx_->free_task(task);
  }
private:
  ObTableLoadTableCtx * const ctx_;
  ObTableLoadCoordinatorTrans * const trans_;
};

int ObTableLoadCoordinator::add_check_peers_trans_commit_task(ObTableLoadCoordinatorTrans *trans)
{
  int ret = OB_SUCCESS;
  ObTableLoadTask *task = nullptr;
  // 1. 分配task
  if (OB_FAIL(ctx_->alloc_task(task))) {
    LOG_WARN("fail to alloc task", KR(ret));
  }
  // 2. 设置processor
  else if (OB_FAIL(task->set_processor<CheckPeersTransCommitTaskProcessor>(ctx_, trans))) {
    LOG_WARN("fail to set check peer trans commit task processor", KR(ret));
  }
  // 3. 设置callback
  else if (OB_FAIL(task->set_callback<CheckPeersTransCommitTaskCallback>(ctx_, trans))) {
    LOG_WARN("fail to set check peer trans commit task callback", KR(ret));
  }
  // 4. 把task放入调度器
  else if (OB_FAIL(coordinator_ctx_->task_scheduler_->add_task(0, task))) {
    LOG_WARN("fail to add task", KR(ret), KPC(task));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != task) {
      ctx_->free_task(task);
    }
  }
  return ret;
}

int ObTableLoadCoordinator::finish_trans_peers(ObTableLoadCoordinatorTrans *trans)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else {
    LOG_INFO("coordinator finish trans peers");
    if (OB_FAIL(pre_finish_trans_peers(trans))) {
      LOG_WARN("fail to pre finish trans peers", KR(ret));
    } else if (OB_FAIL(add_check_peers_trans_commit_task(trans))) {
      LOG_WARN("fail to add check peers trans commit task", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadCoordinator::commit_trans(ObTableLoadCoordinatorTrans *trans)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else {
    LOG_INFO("coordinator commit trans");
    if (OB_FAIL(trans->set_trans_status_commit())) {
      LOG_WARN("fail to set trans status commit", KR(ret));
    } else if (OB_FAIL(coordinator_ctx_->commit_trans(trans))) {
      LOG_WARN("fail to commit trans", KR(ret));
    }
  }
  return ret;
}

/**
 * abandon trans
 */

int ObTableLoadCoordinator::abandon_trans_peers(ObTableLoadCoordinatorTrans *trans)
{
  int ret = OB_SUCCESS;
  ObTableLoadArray<ObAddr> all_addr_array;
  if (OB_FAIL(coordinator_ctx_->partition_location_.get_all_leader(all_addr_array))) {
    LOG_WARN("fail to get all addr", KR(ret));
  } else {
    LOG_INFO("route_abandon_trans_peer_request begin", K(all_addr_array.count()));
    const ObTableLoadTransId &trans_id = trans->get_trans_id();
    ObTableLoadAbandonTransPeerRequest request;
    ObTableLoadAbandonTransPeerResult result;
    request.credential_ = coordinator_ctx_->credential_;
    request.table_id_ = param_.table_id_;
    request.task_id_ = ctx_->ddl_param_.task_id_;
    request.trans_id_ = trans_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) {  // 本机
        ObTableLoadStore store(ctx_);
        if (OB_FAIL(store.init())) {
          LOG_WARN("fail to init store", KR(ret));
        } else if (OB_FAIL(store.abandon_trans(trans_id))) {
          LOG_WARN("fail to store abandon trans", KR(ret), K(trans_id));
        }
      } else {  // 远端, 发送rpc
        TABLE_LOAD_RPC_CALL(load_abandon_trans_peer, addr, request, result);
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinator::abandon_trans(const ObTableLoadTransId &trans_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else {
    LOG_INFO("coordinator abandon trans");
    ObTableLoadCoordinatorTrans *trans = nullptr;
    if (OB_FAIL(coordinator_ctx_->get_trans(trans_id, trans))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get trans", KR(ret), K(trans_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(trans->set_trans_status_abort())) {
      LOG_WARN("fail to set trans status abort", KR(ret));
    } else if (OB_FAIL(abandon_trans_peers(trans))) {
      LOG_WARN("fail to abandon trans peers", KR(ret));
    } else if (OB_FAIL(coordinator_ctx_->abort_trans(trans))) {
      LOG_WARN("fail to abort trans", KR(ret));
    }
    if (OB_NOT_NULL(trans)) {
      coordinator_ctx_->put_trans(trans);
      trans = nullptr;
    }
  }
  return ret;
}

/**
 * get trans status
 */

int ObTableLoadCoordinator::get_trans_status(const ObTableLoadTransId &trans_id,
                                             ObTableLoadTransStatusType &trans_status,
                                             int &error_code)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else {
    LOG_INFO("coordinator get trans status");
    ObTableLoadTransCtx *trans_ctx = nullptr;
    if (OB_FAIL(coordinator_ctx_->get_trans_ctx(trans_id, trans_ctx))) {
      LOG_WARN("fail to get trans ctx", KR(ret), K(trans_id));
    } else {
      trans_ctx->get_trans_status(trans_status, error_code);
    }
  }
  return ret;
}

/**
 * write
 */

class ObTableLoadCoordinator::WriteTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  WriteTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx, ObTableLoadCoordinatorTrans *trans,
                     ObTableLoadTransBucketWriter *bucket_writer, int32_t session_id)
    : ObITableLoadTaskProcessor(task),
      ctx_(ctx),
      trans_(trans),
      bucket_writer_(bucket_writer),
      session_id_(session_id)
  {
    ctx_->inc_ref_count();
    trans_->inc_ref_count();
    bucket_writer_->inc_ref_count();
  }
  virtual ~WriteTaskProcessor()
  {
    trans_->put_bucket_writer(bucket_writer_);
    ctx_->coordinator_ctx_->put_trans(trans_);
    ObTableLoadService::put_ctx(ctx_);
  }
  int set_objs(const ObTableLoadObjRowArray &obj_rows, const ObIArray<int64_t> &idx_array)
  {
    int ret = OB_SUCCESS;

    for (int64_t i = 0; OB_SUCC(ret) && (i < obj_rows.count()); ++i) {
      const ObTableLoadObjRow &src_obj_row = obj_rows.at(i);
      ObTableLoadObjRow out_obj_row;

      if (OB_FAIL(out_obj_row.init(src_obj_row.count_, src_obj_row.allocator_handle_))) {
        LOG_WARN("failed to init out_obj_row", KR(ret), K(src_obj_row.count_));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && (j < src_obj_row.count_); ++j) {
          out_obj_row.cells_[j] = src_obj_row.cells_[idx_array.at(j)];
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(obj_rows_.push_back(out_obj_row))) {
          LOG_WARN("failed to add row to obj_rows_", KR(ret), K(out_obj_row));
        }
      }
    }

    return ret;
  }
  int process() override
  {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(coordinator_write_time_us);
    int ret = OB_SUCCESS;
    if (OB_SUCC(trans_->check_trans_status(ObTableLoadTransStatusType::RUNNING)) ||
        OB_SUCC(trans_->check_trans_status(ObTableLoadTransStatusType::FROZEN))) {
      if (OB_FAIL(bucket_writer_->write(session_id_, obj_rows_))) {
        LOG_WARN("fail to write bucket pool", KR(ret));
      }
    }
    return ret;
  }
private:
  ObTableLoadTableCtx * const ctx_;
  ObTableLoadCoordinatorTrans * const trans_;
  ObTableLoadTransBucketWriter * const bucket_writer_;
  const int32_t session_id_;
  ObTableLoadObjRowArray obj_rows_;
};

class ObTableLoadCoordinator::WriteTaskCallback : public ObITableLoadTaskCallback
{
public:
  WriteTaskCallback(ObTableLoadTableCtx *ctx, ObTableLoadCoordinatorTrans *trans,
                    ObTableLoadTransBucketWriter *bucket_writer)
    : ctx_(ctx), trans_(trans), bucket_writer_(bucket_writer)
  {
    ctx_->inc_ref_count();
    trans_->inc_ref_count();
    bucket_writer_->inc_ref_count();
  }
  virtual ~WriteTaskCallback()
  {
    trans_->put_bucket_writer(bucket_writer_);
    ctx_->coordinator_ctx_->put_trans(trans_);
    ObTableLoadService::put_ctx(ctx_);
  }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ret_code)) {
      trans_->set_trans_status_error(ret);
    }
    ctx_->free_task(task);
  }
private:
  ObTableLoadTableCtx * const ctx_;
  ObTableLoadCoordinatorTrans * const trans_;
  ObTableLoadTransBucketWriter * const bucket_writer_; // 为了保证接收完本次写入结果之后再让bucket_writer的引用归零
};

int ObTableLoadCoordinator::write(const ObTableLoadTransId &trans_id, int32_t session_id,
                                  uint64_t sequence_no, const ObTableLoadObjRowArray &obj_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else {
    LOG_DEBUG("coordinator write");
    ObTableLoadCoordinatorTrans *trans = nullptr;
    ObTableLoadTransBucketWriter *bucket_writer = nullptr;
    ObTableLoadMutexGuard guard;
    if (OB_FAIL(coordinator_ctx_->get_trans(trans_id, trans))) {
      LOG_WARN("fail to get trans", KR(ret));
    } else if (session_id == 0 && FALSE_IT(session_id = trans->get_default_session_id())) {
    }
    // 取出bucket_writer
    else if (OB_FAIL(trans->get_bucket_writer_for_write(bucket_writer))) {
      LOG_WARN("fail to get bucket writer", KR(ret));
    } else if (OB_FAIL(bucket_writer->advance_sequence_no(session_id, sequence_no, guard))) {
      if (OB_UNLIKELY(OB_ENTRY_EXIST != ret)) {
        LOG_WARN("fail to advance sequence no", KR(ret), K(session_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      ObTableLoadTask *task = nullptr;
      WriteTaskProcessor *processor = nullptr;
      // 1. 分配task
      if (OB_FAIL(ctx_->alloc_task(task))) {
        LOG_WARN("fail to alloc task", KR(ret));
      }
      // 2. 设置processor
      else if (OB_FAIL(task->set_processor<WriteTaskProcessor>(ctx_, trans, bucket_writer, session_id))) {
        LOG_WARN("fail to set write task processor", KR(ret));
      } else if (OB_ISNULL(processor = dynamic_cast<WriteTaskProcessor *>(task->get_processor()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null processor", KR(ret));
      } else if (OB_FAIL(processor->set_objs(obj_rows, coordinator_ctx_->idx_array_))) {
        LOG_WARN("fail to set objs", KR(ret), K(coordinator_ctx_->idx_array_));
      }
      // 3. 设置callback
      else if (OB_FAIL(task->set_callback<WriteTaskCallback>(ctx_, trans, bucket_writer))) {
        LOG_WARN("fail to set write task callback", KR(ret));
      }
      // 4. 把task放入调度器
      else if (OB_FAIL(coordinator_ctx_->task_scheduler_->add_task(session_id - 1, task))) {
        LOG_WARN("fail to add task", KR(ret), K(session_id), KPC(task));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != task) {
          ctx_->free_task(task);
        }
      }
    }
    if (OB_NOT_NULL(trans)) {
      if (OB_NOT_NULL(bucket_writer)) {
        trans->put_bucket_writer(bucket_writer);
        bucket_writer = nullptr;
      }
      coordinator_ctx_->put_trans(trans);
      trans = nullptr;
    }
  }
  return ret;
}

/**
 * flush
 */

class ObTableLoadCoordinator::FlushTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  FlushTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx, ObTableLoadCoordinatorTrans *trans,
                     ObTableLoadTransBucketWriter *bucket_writer, int32_t session_id)
    : ObITableLoadTaskProcessor(task),
      ctx_(ctx),
      trans_(trans),
      bucket_writer_(bucket_writer),
      session_id_(session_id)
  {
    ctx_->inc_ref_count();
    trans_->inc_ref_count();
    bucket_writer_->inc_ref_count();
  }
  virtual ~FlushTaskProcessor()
  {
    trans_->put_bucket_writer(bucket_writer_);
    ctx_->coordinator_ctx_->put_trans(trans_);
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(coordinator_flush_time_us);
    int ret = OB_SUCCESS;
    if (OB_SUCC(trans_->check_trans_status(ObTableLoadTransStatusType::FROZEN))) {
      if (OB_FAIL(bucket_writer_->flush(session_id_))) {
        LOG_WARN("fail to flush bucket", KR(ret));
      }
    }
    return ret;
  }
private:
  ObTableLoadTableCtx * const ctx_;
  ObTableLoadCoordinatorTrans * const trans_;
  ObTableLoadTransBucketWriter * const bucket_writer_;
  const int32_t session_id_;
};

class ObTableLoadCoordinator::FlushTaskCallback : public ObITableLoadTaskCallback
{
public:
  FlushTaskCallback(ObTableLoadTableCtx *ctx, ObTableLoadCoordinatorTrans *trans,
                    ObTableLoadTransBucketWriter *bucket_writer)
    : ctx_(ctx), trans_(trans), bucket_writer_(bucket_writer)
  {
    ctx_->inc_ref_count();
    trans_->inc_ref_count();
    bucket_writer_->inc_ref_count();
  }
  virtual ~FlushTaskCallback()
  {
    trans_->put_bucket_writer(bucket_writer_);
    ctx_->coordinator_ctx_->put_trans(trans_);
    ObTableLoadService::put_ctx(ctx_);
  }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ret_code)) {
      trans_->set_trans_status_error(ret);
    }
    ctx_->free_task(task);
    OB_TABLE_LOAD_STATISTICS_PRINT_AND_RESET();
  }
private:
  ObTableLoadTableCtx * const ctx_;
  ObTableLoadCoordinatorTrans * const trans_;
  ObTableLoadTransBucketWriter * const bucket_writer_; // 为了保证接收完本次写入结果之后再让bucket_writer的引用归零
};

int ObTableLoadCoordinator::flush(ObTableLoadCoordinatorTrans *trans)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else {
    LOG_DEBUG("coordinator flush");
    ObTableLoadTransBucketWriter *bucket_writer = nullptr;
    // 取出bucket_writer
    if (OB_FAIL(trans->get_bucket_writer_for_flush(bucket_writer))) {
      LOG_WARN("fail to get bucket writer", KR(ret));
    } else {
      for (int32_t session_id = 1; OB_SUCC(ret) && session_id <= param_.session_count_; ++session_id) {
        ObTableLoadTask *task = nullptr;
        // 1. 分配task
        if (OB_FAIL(ctx_->alloc_task(task))) {
          LOG_WARN("fail to alloc task", KR(ret));
        }
        // 2. 设置processor
        else if (OB_FAIL(task->set_processor<FlushTaskProcessor>(ctx_, trans, bucket_writer,
                                                                 session_id))) {
          LOG_WARN("fail to set flush task processor", KR(ret));
        }
        // 3. 设置callback
        else if (OB_FAIL(task->set_callback<FlushTaskCallback>(ctx_, trans, bucket_writer))) {
          LOG_WARN("fail to set flush task callback", KR(ret));
        }
        // 4. 把task放入调度器
        else if (OB_FAIL(coordinator_ctx_->task_scheduler_->add_task(session_id - 1, task))) {
          LOG_WARN("fail to add task", KR(ret), K(session_id), KPC(task));
        }
        if (OB_FAIL(ret)) {
          if (nullptr != task) {
            ctx_->free_task(task);
          }
        }
      }
    }
    if (OB_NOT_NULL(bucket_writer)) {
      trans->put_bucket_writer(bucket_writer);
      bucket_writer = nullptr;
    }
  }
  return ret;
}

int ObTableLoadCoordinator::write_peer_leader(const ObTableLoadTransId &trans_id,
                                              int32_t session_id, uint64_t sequence_no,
                                              const ObTableLoadTabletObjRowArray &tablet_obj_rows,
                                              const ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else if (tablet_obj_rows.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret));
  } else {
    LOG_DEBUG("coordinator write peer leader", K(addr));
    if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
      ObTableLoadStore store(ctx_);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.write(trans_id, session_id, sequence_no, tablet_obj_rows))) {
        LOG_WARN("fail to store write", KR(ret), K(trans_id));
      }
    } else { // 远端, 发送rpc
      common::ObArenaAllocator allocator("TLD_Coord");
      allocator.set_tenant_id(MTL_ID());
      int64_t pos = 0;
      int64_t buf_len = tablet_obj_rows.get_serialize_size();
      char *buf = static_cast<char *>(allocator.alloc(buf_len));
      if (OB_ISNULL(buf)) {
        LOG_WARN("failed to allocate memory", KR(ret), K(buf_len));
      } else if (OB_FAIL(tablet_obj_rows.serialize(buf, buf_len, pos))) {
        LOG_WARN("failed to serialize obj row array", KR(ret), KP(buf), K(buf_len), K(pos));
      } else {
        ObTableLoadPeerRequest request;
        ObTableLoadPeerResult result;
        request.credential_ = coordinator_ctx_->credential_;
        request.table_id_ = param_.table_id_;
        request.task_id_ = ctx_->ddl_param_.task_id_;
        request.trans_id_ = trans_id;
        request.session_id_ = session_id;
        request.sequence_no_ = sequence_no;
        request.payload_.assign(buf, buf_len);
        TABLE_LOAD_RPC_CALL(load_peer, addr, request, result);
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
