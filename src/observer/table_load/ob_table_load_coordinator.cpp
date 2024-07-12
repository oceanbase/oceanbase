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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/ob_server.h"
#include "observer/table_load/control/ob_table_load_control_rpc_proxy.h"
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
#include "share/stat/ob_incremental_stat_estimator.h"
#include "share/stat/ob_dbms_stats_executor.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "observer/table_load/ob_table_load_index_long_wait.h"
#include "observer/omt/ob_tenant.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace table;
using namespace share;
using namespace sql;
using namespace storage;
using namespace transaction;
using namespace lib;
using namespace omt;

#define TABLE_LOAD_CONTROL_RPC_CALL(name, addr, arg, ...)                         \
  ({                                                                              \
    ObTableLoadControlRpcProxy proxy(*GCTX.srv_rpc_proxy_);                       \
    ObTimeoutCtx ctx;                                                             \
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, DEFAULT_TIMEOUT_US))) { \
      LOG_WARN("fail to set default timeout ctx", KR(ret));                       \
    } else if (OB_FAIL(proxy.to(addr)                                             \
                         .timeout(ctx.get_timeout())                              \
                         .by(MTL_ID())                                            \
                         .name(arg, ##__VA_ARGS__))) {                            \
      LOG_WARN("fail to rpc call " #name, KR(ret), K(addr), K(arg));              \
    }                                                                             \
  })

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

int ObTableLoadCoordinator::init_ctx(ObTableLoadTableCtx *ctx,
                                     const ObIArray<uint64_t> &column_ids,
                                     ObTableLoadExecCtx *exec_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid agrs", KR(ret));
  } else if (OB_FAIL(ctx->init_coordinator_ctx(column_ids, exec_ctx))) {
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
    int tmp_ret = OB_SUCCESS;
    // 1. mark status abort, speed up background task exit
    if (OB_SUCCESS != (tmp_ret = ctx->coordinator_ctx_->set_status_abort())) {
      LOG_WARN("fail to set coordinator status abort", KR(tmp_ret));
    }
    // 2. disable heart beat
    ctx->coordinator_ctx_->set_enable_heart_beat(false);
    // 3. mark all active trans abort
    if (OB_SUCCESS != (tmp_ret = abort_active_trans(ctx))) {
      LOG_WARN("fail to abort active trans", KR(tmp_ret));
    }
    // 4. abort peers ctx
    if (OB_SUCCESS != (tmp_ret = abort_peers_ctx(ctx))) {
      LOG_WARN("fail to abort peers ctx", KR(tmp_ret));
    }
  }
}

int ObTableLoadCoordinator::abort_active_trans(ObTableLoadTableCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObTableLoadTransId> trans_id_array;
  trans_id_array.set_tenant_id(MTL_ID());
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
    static const int64_t max_retry_times = 100; // ensure store ctx detect heart beat timeout and abort
    ObArray<ObAddr> addr_array1, addr_array2;
    ObIArray<ObAddr> *curr_round = &addr_array1, *next_round = &addr_array2;
    int64_t running_cnt = 0;
    int64_t fail_cnt = 0;
    int64_t round = 0;
    int64_t tries = 0;
    ObDirectLoadControlAbortArg arg;
    ObDirectLoadControlAbortRes res;
    addr_array1.set_tenant_id(MTL_ID());
    addr_array2.set_tenant_id(MTL_ID());
    arg.table_id_ = ctx->param_.table_id_;
    arg.task_id_ = ctx->ddl_param_.task_id_;
    for (int64_t i = 0; i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (OB_FAIL(curr_round->push_back(addr))) {
        LOG_WARN("fail to push back", KR(ret), K(addr));
      }
    }
    ObTableLoadIndexLongWait wait_obj(10 * 1000, WAIT_INTERVAL_US);
    while (!curr_round->empty() && tries < max_retry_times) {
      ret = OB_SUCCESS;
      ++round;
      running_cnt = 0;
      fail_cnt = 0;
      for (int64_t i = 0; i < curr_round->count(); ++i) {
        const ObAddr &addr = curr_round->at(i);
        if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
          ObTableLoadStore::abort_ctx(ctx, res.is_stopped_);
          ret = OB_SUCCESS;
        } else { // 远端, 发送rpc
          // use default timeout value, avoid timeout immediately
          const int64_t origin_timeout_ts = THIS_WORKER.get_timeout_ts();
          THIS_WORKER.set_timeout_ts(ObTimeUtil::current_time() + DEFAULT_TIMEOUT_US);
          TABLE_LOAD_CONTROL_RPC_CALL(abort, addr, arg, res);
          THIS_WORKER.set_timeout_ts(origin_timeout_ts);
        }
        if (OB_SUCC(ret) && res.is_stopped_) {
          // peer is stopped
        } else {
          if (OB_FAIL(ret)) {
            ++fail_cnt;
            ret = OB_SUCCESS;
          } else {
            ++running_cnt;
          }
          if (OB_FAIL(next_round->push_back(addr))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
      }
      if (running_cnt > 0 || fail_cnt > 0) {
        if (running_cnt > 0) {
          // peer still running, keep waiting
          tries = 0;
        } else {
          ++tries;
        }
        if (round % 10 == 0) {
          FLOG_WARN("retry too many times", K(round), K(running_cnt), K(fail_cnt), K(tries), KPC(next_round));
        }
        ob_usleep(WAIT_INTERVAL_US);
      }
      std::swap(curr_round, next_round);
      next_round->reuse();
      wait_obj.wait();
    }
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
int ObTableLoadCoordinator::gen_apply_arg(ObDirectLoadResourceApplyArg &apply_arg)
{
  int ret = OB_SUCCESS;
  const int64_t MIN_THREAD_COUNT = 2;
  ObTenant *tenant = nullptr;
  int64_t tenant_id = MTL_ID();
  uint64_t cluster_version = ctx_->ddl_param_.cluster_version_;
  if (OB_FAIL(GCTX.omt_->get_tenant(tenant_id, tenant))) {
    LOG_INFO("fail to get tenant", KR(ret), K(tenant_id));
  } else if (cluster_version < CLUSTER_VERSION_4_2_2_0 ||
             (cluster_version >= CLUSTER_VERSION_4_3_0_0 && cluster_version < CLUSTER_VERSION_4_3_1_0)) {
    // not support resource manage
    if (OB_FAIL(coordinator_ctx_->init_partition_location())) {
      LOG_WARN("fail to init partition location", KR(ret));
    } else {
      ctx_->param_.session_count_ = MAX(MIN(ctx_->param_.parallel_, (int64_t)tenant->unit_max_cpu() * 2), MIN_THREAD_COUNT);
      ctx_->param_.write_session_count_ = ctx_->param_.session_count_;
    }
  } else {
    apply_arg.tenant_id_ = tenant_id;
    apply_arg.task_key_ = ObTableLoadUniqueKey(ctx_->param_.table_id_, ctx_->ddl_param_.task_id_);
    int64_t retry_count = 0;
    common::ObAddr coordinator_addr = ObServer::get_instance().get_self();
    while (OB_SUCC(ret)) {
      apply_arg.apply_array_.reset();
      int64_t memory_limit = 0;
      table::ObTableLoadArray<ObTableLoadPartitionLocation::LeaderInfo> all_leader_info_array;
      if (THIS_WORKER.is_timeout()) {
        ret = OB_TIMEOUT;
        LOG_WARN("gen_apply_arg wait too long", KR(ret));
      } else if (OB_FAIL(coordinator_ctx_->check_status(ObTableLoadStatusType::INITED))) {
        LOG_WARN("fail to check status", KR(ret));
      } else if (OB_FAIL(coordinator_ctx_->exec_ctx_->check_status())) {
        LOG_WARN("fail to check status", KR(ret));
      } else if (OB_FAIL(coordinator_ctx_->init_partition_location())) {
        LOG_WARN("fail to init partition location", KR(ret));
      } else if (OB_FAIL(coordinator_ctx_->partition_location_.get_all_leader_info(all_leader_info_array))) {
        LOG_WARN("fail to get all leader info", KR(ret));
      } else if (OB_FAIL(ObTableLoadService::get_memory_limit(memory_limit))) {
        LOG_WARN("fail to get memory_limit", K(ret));
      } else {
        bool include_cur_addr = false;
        bool need_sort = ctx_->param_.need_sort_;
        int64_t total_partitions = 0;
        ObArray<int64_t> partitions;
        int64_t store_server_count = all_leader_info_array.count();
        int64_t coordinator_session_count = 0;
        int64_t min_session_count = MAX(ctx_->param_.parallel_, 2);
        int64_t max_session_count = (int64_t)tenant->unit_max_cpu() * 2;
        int64_t total_session_count = MIN(ctx_->param_.parallel_, max_session_count * store_server_count);
        int64_t remain_session_count = total_session_count;
        partitions.set_tenant_id(MTL_ID());
        for (int64_t i = 0; i < store_server_count; i++) {
          total_partitions += all_leader_info_array[i].partition_id_array_.count();
          if (coordinator_addr == all_leader_info_array[i].addr_) {
            include_cur_addr = true;
          }
        }
        // is_heap_table==true，we prioritize non multiple modes that do not require sorting
        //     FAST_HEAP_TABLE: macroblock_buffer * partition_count * parallel
	      // is_heap_table==false，we prioritize non multiple modes that do not require sorting
	      // 		 GENERAL_TABLE_COMPACT：max(sstable_buffer * partition_count * parallel，macroblock_buffer * parallel)
	      // param_.need_sort_==true，we apply for the minimum required memory(MIN_SORT_MEMORY_PER_TASK)
        //     MULTIPLE_HEAP_TABLE_COMPACT
	      // 		 MEM_COMPACT
        for (int64_t i = 0; OB_SUCC(ret) && i < store_server_count; i++) {
          ObDirectLoadResourceUnit unit;
          unit.addr_ = all_leader_info_array[i].addr_;
          if (OB_FAIL(partitions.push_back(all_leader_info_array[i].partition_id_array_.count()))) {
            LOG_WARN("fail to push back", KR(ret));
          } else {
            unit.thread_count_ = MAX(MIN(max_session_count, total_session_count * partitions[i] / total_partitions), MIN_THREAD_COUNT);
            if (OB_FAIL(apply_arg.apply_array_.push_back(unit))) {
              LOG_WARN("fail to push back", KR(ret));
            } else {
              remain_session_count -= unit.thread_count_;
            }
          }
        }
        if (OB_SUCC(ret) && !include_cur_addr) {
          ObDirectLoadResourceUnit unit;
          unit.addr_ = coordinator_addr;
          unit.thread_count_ = MAX(total_session_count / store_server_count, MIN_THREAD_COUNT);
          unit.memory_size_ = 0;
          coordinator_session_count = unit.thread_count_;
          min_session_count = MIN(min_session_count, unit.thread_count_);
          if (OB_FAIL(apply_arg.apply_array_.push_back(unit))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
        if (OB_SUCC(ret)) {
          while (remain_session_count > 0) {
            for (int64_t i = 0; remain_session_count > 0 && i < store_server_count; i++) {
              ObDirectLoadResourceUnit &unit = apply_arg.apply_array_[i];
              if (unit.thread_count_ < max_session_count) {
                unit.thread_count_++;
                remain_session_count--;
              }
            }
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < store_server_count; i++) {
          ObDirectLoadResourceUnit &unit = apply_arg.apply_array_[i];
          if (all_leader_info_array[i].addr_ == coordinator_addr) {
            coordinator_session_count = unit.thread_count_;
          }
          min_session_count = MIN(min_session_count, unit.thread_count_);
          int64_t min_unsort_memory = MACROBLOCK_BUFFER_SIZE * partitions[i] * unit.thread_count_;
          if (ctx_->schema_.is_heap_table_) {
            if (min_unsort_memory <= memory_limit) {
              need_sort = false;
              unit.memory_size_ = min_unsort_memory;
            } else {
              need_sort = true;
              unit.memory_size_ = MIN(ObTableLoadAssignedMemoryManager::MIN_SORT_MEMORY_PER_TASK, memory_limit);
            }
          } else {
            if (need_sort) {
              unit.memory_size_ = MIN(ObTableLoadAssignedMemoryManager::MIN_SORT_MEMORY_PER_TASK, memory_limit);
            } else {
              unit.memory_size_ = MIN(min_unsort_memory, memory_limit);
            }
          }
        }

        if (OB_SUCC(ret)) {
          ObDirectLoadResourceOpRes apply_res;
          if (OB_FAIL(ObTableLoadResourceService::apply_resource(apply_arg, apply_res))) {
            if (retry_count % 100 == 0) {
              LOG_WARN("fail to apply resource", KR(ret), K(apply_res.error_code_), K(retry_count));
            }
            if (ret == OB_EAGAIN) {
              retry_count++;
              ret = OB_SUCCESS;
              usleep(RESOURCE_OP_WAIT_INTERVAL_US);
            }
          } else {
            ctx_->param_.need_sort_ = need_sort;
            ctx_->param_.session_count_ = coordinator_session_count;
            ctx_->param_.write_session_count_ =
                (include_cur_addr ? MIN(min_session_count, (coordinator_session_count + 1) / 2) : min_session_count);
            ctx_->param_.exe_mode_ = (ctx_->schema_.is_heap_table_ ?
                (need_sort ? ObTableLoadExeMode::MULTIPLE_HEAP_TABLE_COMPACT : ObTableLoadExeMode::FAST_HEAP_TABLE) :
                (need_sort ? ObTableLoadExeMode::MEM_COMPACT : ObTableLoadExeMode::GENERAL_TABLE_COMPACT));
            ctx_->job_stat_->parallel_ = coordinator_session_count;
            if (OB_FAIL(ObTableLoadService::add_assigned_task(apply_arg))) {
              LOG_WARN("fail to add_assigned_task", KR(ret));
            } else {
              ctx_->set_assigned_resource();
              LOG_INFO("Coordinator::gen_apply_arg", K(retry_count), K(param_.exe_mode_), K(partitions), K(coordinator_addr), K(apply_arg));
              break;
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObTableLoadCoordinator::pre_begin_peers(ObDirectLoadResourceApplyArg &apply_arg)
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
    ObDirectLoadControlPreBeginArg arg;
    arg.table_id_ = param_.table_id_;
    arg.config_.max_error_row_count_ = param_.max_error_row_count_;
    arg.config_.batch_size_ = param_.batch_size_;
    arg.config_.is_need_sort_ = param_.need_sort_;
    arg.column_count_ = param_.column_count_;
    arg.dup_action_ = param_.dup_action_;
    arg.px_mode_ = param_.px_mode_;
    arg.online_opt_stat_gather_ = param_.online_opt_stat_gather_;
    arg.ddl_param_ = ctx_->ddl_param_;
    arg.session_info_ = ctx_->session_info_;
    arg.write_session_count_ = param_.write_session_count_;
    arg.exe_mode_ = ctx_->param_.exe_mode_;
    arg.method_ = param_.method_;
    arg.insert_mode_ = param_.insert_mode_;
    arg.load_mode_ = param_.load_mode_;
    arg.compressor_type_ = param_.compressor_type_;
    arg.online_sample_percent_ = param_.online_sample_percent_;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_leader_info_array.count(); ++i) {
      const ObTableLoadPartitionLocation::LeaderInfo &leader_info = all_leader_info_array.at(i);
      const ObTableLoadPartitionLocation::LeaderInfo &target_leader_info =
        target_all_leader_info_array.at(i);
      //目前源表和目标表的分区信息连同每个分区的地址都完全一样
      const ObAddr &addr = leader_info.addr_;
      if (OB_UNLIKELY(leader_info.addr_ != target_leader_info.addr_)) {
        LOG_INFO("addr must be same", K(leader_info.addr_), K(target_leader_info.addr_));
      }
      if (arg.exe_mode_ == ObTableLoadExeMode::MAX_TYPE) {
        arg.config_.parallel_ = ctx_->param_.session_count_;
      } else {
        arg.config_.parallel_ = apply_arg.apply_array_[i].thread_count_;
        arg.avail_memory_ = apply_arg.apply_array_[i].memory_size_;
      }
      arg.partition_id_array_ = leader_info.partition_id_array_;
      arg.target_partition_id_array_ = target_leader_info.partition_id_array_;
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ctx_->param_.session_count_ = arg.config_.parallel_;
        ctx_->param_.avail_memory_ = arg.avail_memory_;
        if (OB_FAIL(ObTableLoadService::assign_memory(ctx_->param_.need_sort_, arg.avail_memory_))) {
          LOG_WARN("fail to assign_memory", KR(ret));
        } else {
          ctx_->set_assigned_memory();
          if (OB_FAIL(ObTableLoadStore::init_ctx(ctx_, arg.partition_id_array_, arg.target_partition_id_array_))) {
            LOG_WARN("fail to store init ctx", KR(ret));
          } else {
            ObTableLoadStore store(ctx_);
            if (OB_FAIL(store.init())) {
              LOG_WARN("fail to init store", KR(ret));
            } else if (OB_FAIL(store.pre_begin())) {
              LOG_WARN("fail to store pre begin", KR(ret));
            }
          }
        }
      } else { // 对端, 发送rpc
        TABLE_LOAD_CONTROL_RPC_CALL(pre_begin, addr, arg);
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
    ObDirectLoadControlConfirmBeginArg arg;
    arg.table_id_ = param_.table_id_;
    arg.task_id_ = ctx_->ddl_param_.task_id_;
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
        TABLE_LOAD_CONTROL_RPC_CALL(confirm_begin, addr, arg);
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
    ObDirectLoadResourceApplyArg apply_arg;
    LOG_INFO("coordinator begin");
    ObMutexGuard guard(coordinator_ctx_->get_op_lock());
    if (OB_FAIL(coordinator_ctx_->check_status(ObTableLoadStatusType::INITED))) {
      LOG_WARN("fail to check status", KR(ret));
    } else if (OB_FAIL(gen_apply_arg(apply_arg)))  {
      LOG_WARN("fail to gen_apply_arg", KR(ret));
    } else if (OB_FAIL(coordinator_ctx_->init_complete())) {
      LOG_WARN("fail to coordinator_ctx_ init complete", KR(ret));
    } else if (OB_FAIL(pre_begin_peers(apply_arg))) {
      LOG_WARN("fail to pre begin peers", KR(ret));
    } else if (OB_FAIL(confirm_begin_peers())) {
      LOG_WARN("fail to confirm begin peers", KR(ret));
    } else {
      coordinator_ctx_->set_enable_heart_beat(true);
      if (OB_FAIL(add_check_begin_result_task())) {
        LOG_WARN("fail to add check begin result task", KR(ret));
      }
    }
  }
  return ret;
}

/**
 * check begin result
 */

int ObTableLoadCoordinator::check_peers_begin_result(bool &is_finish)
{
  int ret = OB_SUCCESS;
  ObTableLoadArray<ObAddr> all_addr_array;
  if (OB_FAIL(coordinator_ctx_->partition_location_.get_all_leader(all_addr_array))) {
    LOG_WARN("fail to get all addr", KR(ret));
  } else {
    LOG_INFO("check_peers_begin_result begin", K(all_addr_array.count()));
    ObDirectLoadControlGetStatusArg arg;
    ObDirectLoadControlGetStatusRes res;
    arg.table_id_ = param_.table_id_;
    arg.task_id_ = ctx_->ddl_param_.task_id_;
    is_finish = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ObTableLoadStore store(ctx_);
        if (OB_FAIL(store.init())) {
          LOG_WARN("fail to init store", KR(ret));
        } else if (OB_FAIL(store.get_status(res.status_, res.error_code_))) {
          LOG_WARN("fail to store get status", KR(ret));
        }
      } else { // 远端, 发送rpc
        TABLE_LOAD_CONTROL_RPC_CALL(get_status, addr, arg, res);
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(ObTableLoadStatusType::ERROR == res.status_)) {
          ret = res.error_code_;
          LOG_WARN("store has error", KR(ret), K(addr), K(res.status_));
        } else if (OB_UNLIKELY(ObTableLoadStatusType::ABORT == res.status_)) {
          ret = OB_SUCCESS != res.error_code_ ? res.error_code_ : OB_CANCELED;
          LOG_WARN("store has abort", KR(ret), K(addr), K(res.status_));
        } else if (OB_UNLIKELY(ObTableLoadStatusType::INITED != res.status_ &&
                               ObTableLoadStatusType::LOADING != res.status_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected peer status", KR(ret), K(addr), K(res.status_));
        } else if (ObTableLoadStatusType::LOADING != res.status_) {
          is_finish = false;
        }
      }
    }
  }
  return ret;
}

class ObTableLoadCoordinator::CheckBeginResultTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  CheckBeginResultTaskProcessor(ObTableLoadTask &task, ObTableLoadTableCtx *ctx)
    : ObITableLoadTaskProcessor(task), ctx_(ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~CheckBeginResultTaskProcessor()
  {
    ObTableLoadService::put_ctx(ctx_);
  }
  int process() override
  {
    int ret = OB_SUCCESS;
    bool is_finish = false;
    ObTableLoadCoordinator coordinator(ctx_);
    if (OB_FAIL(coordinator.init())) {
      LOG_WARN("fail to init coordinator", KR(ret));
    }
    ObTableLoadIndexLongWait wait_obj(10 * 1000, WAIT_INTERVAL_US);
    while (OB_SUCC(ret)) {
      // 确认状态
      if (OB_FAIL(ctx_->coordinator_ctx_->check_status(ObTableLoadStatusType::INITED))) {
        LOG_WARN("fail to check coordinator status inited", KR(ret));
      }
      // 查询合并状态
      else if (OB_FAIL(coordinator.check_peers_begin_result(is_finish))) {
        LOG_WARN("fail to check peers begin result", KR(ret));
      } else if (!is_finish) {
        wait_obj.wait();
      } else {
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ctx_->coordinator_ctx_->set_status_loading())) {
        LOG_WARN("fail to set coordinator status loading", KR(ret));
      }
    }
    return ret;
  }
private:
  ObTableLoadTableCtx * const ctx_;
};

class ObTableLoadCoordinator::CheckBeginResultTaskCallback : public ObITableLoadTaskCallback
{
public:
  CheckBeginResultTaskCallback(ObTableLoadTableCtx *ctx)
    : ctx_(ctx)
  {
    ctx_->inc_ref_count();
  }
  virtual ~CheckBeginResultTaskCallback()
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

int ObTableLoadCoordinator::add_check_begin_result_task()
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
    else if (OB_FAIL(task->set_processor<CheckBeginResultTaskProcessor>(ctx_))) {
      LOG_WARN("fail to set check begin result task processor", KR(ret));
    }
    // 3. 设置callback
    else if (OB_FAIL(task->set_callback<CheckBeginResultTaskCallback>(ctx_))) {
      LOG_WARN("fail to set check begin result task callback", KR(ret));
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
    ObDirectLoadControlPreMergeArg arg;
    allocator.set_tenant_id(MTL_ID());
    arg.table_id_ = param_.table_id_;
    arg.task_id_ = ctx_->ddl_param_.task_id_;
    if (!ctx_->param_.px_mode_) {
      if (OB_FAIL(coordinator_ctx_->get_committed_trans_ids(arg.committed_trans_id_array_,
                                                            allocator))) {
        LOG_WARN("fail to get committed trans ids", KR(ret));
      } else {
        lib::ob_sort(arg.committed_trans_id_array_.begin(), arg.committed_trans_id_array_.end());
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ObTableLoadStore store(ctx_);
        if (OB_FAIL(store.init())) {
          LOG_WARN("fail to init store", KR(ret));
        } else if (OB_FAIL(store.pre_merge(arg.committed_trans_id_array_))) {
          LOG_WARN("fail to store pre merge", KR(ret));
        }
      } else { // 远端, 发送rpc
        TABLE_LOAD_CONTROL_RPC_CALL(pre_merge, addr, arg);
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
    ObDirectLoadControlStartMergeArg arg;
    arg.table_id_ = param_.table_id_;
    arg.task_id_ = ctx_->ddl_param_.task_id_;
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
        TABLE_LOAD_CONTROL_RPC_CALL(start_merge, addr, arg);
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
    ObDirectLoadControlGetStatusArg arg;
    ObDirectLoadControlGetStatusRes res;
    arg.table_id_ = param_.table_id_;
    arg.task_id_ = ctx_->ddl_param_.task_id_;
    is_finish = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ObTableLoadStore store(ctx_);
        if (OB_FAIL(store.init())) {
          LOG_WARN("fail to init store", KR(ret));
        } else if (OB_FAIL(store.get_status(res.status_, res.error_code_))) {
          LOG_WARN("fail to store get status", KR(ret));
        }
      } else { // 远端, 发送rpc
        TABLE_LOAD_CONTROL_RPC_CALL(get_status, addr, arg, res);
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(ObTableLoadStatusType::ERROR == res.status_)) {
          ret = res.error_code_;
          LOG_WARN("store has error", KR(ret), K(addr), K(res.status_));
        } else if (OB_UNLIKELY(ObTableLoadStatusType::ABORT == res.status_)) {
          ret = OB_SUCCESS != res.error_code_ ? res.error_code_ : OB_CANCELED;
          LOG_WARN("store has abort", KR(ret), K(addr), K(res.status_));
        } else if (OB_UNLIKELY(ObTableLoadStatusType::MERGING != res.status_ &&
                               ObTableLoadStatusType::MERGED != res.status_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected peer status", KR(ret), K(addr), K(res.status_));
        } else if (ObTableLoadStatusType::MERGED != res.status_) {
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
    ObTableLoadIndexLongWait wait_obj(10 * 1000, WAIT_INTERVAL_US);
    while (OB_SUCC(ret)) {
      // 确认状态
      if (OB_FAIL(ctx_->coordinator_ctx_->check_status(ObTableLoadStatusType::MERGING))) {
        LOG_WARN("fail to check coordinator status merging", KR(ret));
      }
      // 查询合并状态
      else if (OB_FAIL(coordinator.check_peers_merge_result(is_merge_finish))) {
        LOG_WARN("fail to check peers merge result", KR(ret));
      } else if (!is_merge_finish) {
        wait_obj.wait();
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

int ObTableLoadCoordinator::commit_peers(ObTableLoadSqlStatistics &sql_statistics,
                                         ObTableLoadDmlStat &dml_stats)
{
  int ret = OB_SUCCESS;
  ObTransService *txs = nullptr;
  ObTableLoadArray<ObAddr> all_addr_array;
  if (OB_ISNULL(MTL(ObTransService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("trans service is null", KR(ret));
  } else if (OB_FAIL(coordinator_ctx_->partition_location_.get_all_leader(all_addr_array))) {
    LOG_WARN("fail to get all addr", KR(ret));
  } else {
    LOG_INFO("route_commit_peer_request begin", K(all_addr_array.count()));
    ObDirectLoadControlCommitArg arg;
    ObDirectLoadControlCommitRes res;
    arg.table_id_ = param_.table_id_;
    arg.task_id_ = ctx_->ddl_param_.task_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ObTableLoadStore store(ctx_);
        if (OB_FAIL(store.init())) {
          LOG_WARN("fail to init store", KR(ret));
        } else if (OB_FAIL(store.commit(res.result_info_,
                                        res.sql_statistics_,
                                        res.dml_stats_,
                                        res.trans_result_))) {
          LOG_WARN("fail to commit store", KR(ret));
        }
      } else { // 远端, 发送rpc
        TABLE_LOAD_CONTROL_RPC_CALL(commit, addr, arg, res);
      }
      if (OB_SUCC(ret)) {
        ATOMIC_AAF(&coordinator_ctx_->result_info_.rows_affected_, res.result_info_.rows_affected_);
        ATOMIC_AAF(&coordinator_ctx_->result_info_.deleted_, res.result_info_.deleted_);
        ATOMIC_AAF(&coordinator_ctx_->result_info_.skipped_, res.result_info_.skipped_);
        ATOMIC_AAF(&coordinator_ctx_->result_info_.warnings_, res.result_info_.warnings_);
        if (OB_FAIL(sql_statistics.merge(res.sql_statistics_))) {
          LOG_WARN("fail to add result sql stats", KR(ret), K(addr), K(res));
        } else if (OB_FAIL(dml_stats.merge(res.dml_stats_))) {
          LOG_WARN("fail to add result dml stats", KR(ret), K(addr), K(res));
        } else if (ObDirectLoadMethod::is_incremental(param_.method_) &&
                   txs->add_tx_exec_result(*ctx_->session_info_->get_tx_desc(), res.trans_result_)) {
          LOG_WARN("fail to add tx exec result", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinator::build_table_stat_param(ObTableStatParam &param, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const uint64_t table_id = ctx_->ddl_param_.dest_table_id_;
  param.tenant_id_ = tenant_id;
  param.table_id_ = table_id;
  param.part_level_ = ctx_->schema_.part_level_;
  param.allocator_ = &allocator;
  param.global_stat_param_.need_modify_ = true;
  param.part_stat_param_.need_modify_ = false;
  param.subpart_stat_param_.need_modify_ = false;
  if (!ctx_->schema_.is_partitioned_table_) {
    param.global_part_id_ = table_id;
    param.global_tablet_id_ = table_id;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->schema_.column_descs_.count(); ++i) {
    const ObColDesc &col_desc = ctx_->schema_.column_descs_.at(i);
    ObColumnStatParam col_param;
    col_param.column_id_ = col_desc.col_id_;
    col_param.cs_type_ = col_desc.col_type_.get_collation_type();
    if (OB_HIDDEN_PK_INCREMENT_COLUMN_ID == col_param.column_id_) {
      // skip hidden pk
    } else if (OB_FAIL(param.column_params_.push_back(col_param))) {
      LOG_WARN("fail to push back column param", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadCoordinator::write_sql_stat(ObTableLoadSqlStatistics &sql_statistics,
                                           ObTableLoadDmlStat &dml_stats)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const uint64_t table_id = ctx_->ddl_param_.dest_table_id_;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(ObTableLoadSchema::get_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (ObDirectLoadMethod::is_full(ctx_->param_.method_)) { // full direct load
    ObArray<ObOptColumnStat *> global_column_stats;
    ObArray<ObOptTableStat *> global_table_stats;
    global_column_stats.set_tenant_id(MTL_ID());
    global_table_stats.set_tenant_id(MTL_ID());
    if (OB_UNLIKELY(sql_statistics.is_empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", KR(ret), K(sql_statistics));
    } else if (OB_FAIL(sql_statistics.get_table_stat_array(global_table_stats))) {
      LOG_WARN("fail to get table stat array", KR(ret));
    } else if (OB_FAIL(sql_statistics.get_col_stat_array(global_column_stats))) {
      LOG_WARN("fail to get column stat array", KR(ret));
    } else if (OB_UNLIKELY(global_table_stats.empty() || global_column_stats.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty sql stats", KR(ret), K(global_table_stats), K(global_column_stats));
    } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_2_0 &&
               OB_FAIL(ObDbmsStatsUtils::scale_col_stats(tenant_id,
                                                         global_table_stats,
                                                         global_column_stats))) {
      LOG_WARN("failed to scale col stats", KR(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::split_batch_write(&schema_guard,
                                                           ctx_->session_info_,
                                                           GCTX.sql_proxy_,
                                                           global_table_stats,
                                                           global_column_stats))) {
      LOG_WARN("fail to split batch write", KR(ret), K(sql_statistics), K(global_table_stats), K(global_column_stats));
    }
  } else { // inc direct load
    ObExecContext *exec_ctx = nullptr;
    ObArenaAllocator allocator("TLD_Temp");
    ObTableStatParam param;
    TabStatIndMap inc_table_stats;
    ColStatIndMap inc_column_stats;
    allocator.set_tenant_id(MTL_ID());
    if (OB_UNLIKELY(sql_statistics.is_empty() || dml_stats.is_empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", KR(ret), K(sql_statistics), K(dml_stats));
    } else if (OB_ISNULL(exec_ctx = coordinator_ctx_->exec_ctx_->exec_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected exec ctx is null", KR(ret));
    } else if (OB_FAIL(inc_table_stats.create(1,
                                              "TLD_TabStatBkt",
                                              "TLD_TabStatNode",
                                              tenant_id))) {
      LOG_WARN("fail to create table stats map", KR(ret));
    } else if (OB_FAIL(inc_column_stats.create(ctx_->param_.column_count_,
                                               "TLD_ColStatBkt",
                                               "TLD_ColStatNode",
                                               tenant_id))) {
      LOG_WARN("fail to create column stats map", KR(ret));
    } else if (OB_FAIL(sql_statistics.get_table_stats(inc_table_stats))) {
      LOG_WARN("fail to get table stat array", KR(ret));
    } else if (OB_FAIL(sql_statistics.get_col_stats(inc_column_stats))) {
      LOG_WARN("fail to get column stat array", KR(ret));
    } else if (OB_FAIL(build_table_stat_param(param, allocator))) {
      LOG_WARN("fail to build table stat param", KR(ret));
    } else if (OB_FAIL(ObDbmsStatsExecutor::update_online_stat(*exec_ctx,
                                                               param,
                                                               &schema_guard,
                                                               inc_table_stats,
                                                               inc_column_stats,
                                                               &dml_stats.dml_stat_array_))) {
      LOG_WARN("fail to update online stat", KR(ret));
    }
  }
  return ret;
}

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
    ObTableLoadDmlStat dml_stats;
    if (OB_FAIL(coordinator_ctx_->check_status(ObTableLoadStatusType::MERGED))) {
      LOG_WARN("fail to check coordinator status", KR(ret));
    } else if (OB_FAIL(commit_peers(sql_statistics, dml_stats))) {
      LOG_WARN("fail to commit peers", KR(ret));
    } else if (FALSE_IT(coordinator_ctx_->set_enable_heart_beat(false))) {
    } else if (param_.online_opt_stat_gather_ &&
               OB_FAIL(write_sql_stat(sql_statistics, dml_stats))) {
      LOG_WARN("fail to write sql stat", KR(ret));
    } else if (OB_FAIL(coordinator_ctx_->set_status_commit())) {
      LOG_WARN("fail to set coordinator status commit", KR(ret));
    } else {
      result_info = coordinator_ctx_->result_info_;
    }
  }
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
 * heart_beat
 */

int ObTableLoadCoordinator::heart_beat_peer()
{
  int ret = OB_SUCCESS;
  ObTableLoadArray<ObAddr> all_addr_array;
  if (OB_FAIL(coordinator_ctx_->partition_location_.get_all_leader(all_addr_array))) {
    LOG_WARN("fail to get all addr", KR(ret));
  } else {
    LOG_DEBUG("route_heart_beat_peer_request begin", K(all_addr_array.count()));
    ObDirectLoadControlHeartBeatArg arg;
    arg.table_id_ = param_.table_id_;
    arg.task_id_ = ctx_->ddl_param_.task_id_;
    for (int64_t i = 0; i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ObTableLoadStore store(ctx_);
        if (OB_FAIL(store.init())) {
          LOG_WARN("fail to init store", KR(ret));
        } else if (OB_FAIL(store.heart_beat())) {
          LOG_WARN("fail to heart beat store", KR(ret));
        }
      } else { // 远端, 发送rpc
        const int64_t origin_timeout_ts = THIS_WORKER.get_timeout_ts();
        THIS_WORKER.set_timeout_ts(ObTimeUtil::current_time() + HEART_BEAT_RPC_TIMEOUT_US);
        TABLE_LOAD_CONTROL_RPC_CALL(heart_beat, addr, arg);
        THIS_WORKER.set_timeout_ts(origin_timeout_ts);
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinator::heart_beat()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else {
    // 心跳是为了让数据节点感知控制节点存活, 控制节点不依赖心跳感知数据节点状态, 忽略失败
    heart_beat_peer();
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
    ObDirectLoadControlPreStartTransArg arg;
    arg.table_id_ = param_.table_id_;
    arg.task_id_ = ctx_->ddl_param_.task_id_;
    arg.trans_id_ = trans_id;
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
        TABLE_LOAD_CONTROL_RPC_CALL(pre_start_trans, addr, arg);
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
    ObDirectLoadControlConfirmStartTransArg arg;
    arg.table_id_ = param_.table_id_;
    arg.task_id_ = ctx_->ddl_param_.task_id_;
    arg.trans_id_ = trans_id;
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
        TABLE_LOAD_CONTROL_RPC_CALL(confirm_start_trans, addr, arg);
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
    ObDirectLoadControlPreFinishTransArg arg;
    arg.table_id_ = param_.table_id_;
    arg.task_id_ = ctx_->ddl_param_.task_id_;
    arg.trans_id_ = trans_id;
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
        TABLE_LOAD_CONTROL_RPC_CALL(pre_finish_trans, addr, arg);
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
    ObDirectLoadControlConfirmFinishTransArg arg;
    arg.table_id_ = param_.table_id_;
    arg.task_id_ = ctx_->ddl_param_.task_id_;
    arg.trans_id_ = trans_id;
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
        TABLE_LOAD_CONTROL_RPC_CALL(confirm_finish_trans, addr, arg);
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
    ObDirectLoadControlGetTransStatusArg arg;
    ObDirectLoadControlGetTransStatusRes res;
    arg.table_id_ = param_.table_id_;
    arg.task_id_ = ctx_->ddl_param_.task_id_;
    arg.trans_id_ = trans->get_trans_id();
    is_commit = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_addr_array.count(); ++i) {
      const ObAddr &addr = all_addr_array.at(i);
      if (ObTableLoadUtils::is_local_addr(addr)) { // 本机
        ObTableLoadStore store(ctx_);
        if (OB_FAIL(store.init())) {
          LOG_WARN("fail to init store", KR(ret));
        } else if (OB_FAIL(store.get_trans_status(arg.trans_id_, res.trans_status_,
                                                  res.error_code_))) {
          LOG_WARN("fail to store get trans status", KR(ret));
        }
      } else { // 远端, 发送rpc
        TABLE_LOAD_CONTROL_RPC_CALL(get_trans_status, addr, arg, res);
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(ObTableLoadTransStatusType::ERROR == res.trans_status_)) {
          ret = res.error_code_;
          LOG_WARN("trans has error", KR(ret), K(addr));
        } else if (OB_UNLIKELY(ObTableLoadTransStatusType::FROZEN != res.trans_status_ &&
                               ObTableLoadTransStatusType::COMMIT != res.trans_status_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected peer trans status", KR(ret), K(addr), K(res.trans_status_));
        } else if (ObTableLoadTransStatusType::COMMIT != res.trans_status_) {
          is_commit = false;
        }
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinator::check_trans_commit(ObTableLoadCoordinatorTrans *trans)
{
  int ret = OB_SUCCESS;
  bool is_peers_commit = false;
  while (OB_SUCC(ret)) {
    // 确认trans状态为frozen
    if (OB_FAIL(trans->check_trans_status(ObTableLoadTransStatusType::FROZEN))) {
      LOG_WARN("fail to check trans status frozen", KR(ret));
    }
    // 向对端发送pre finish
    else if (OB_FAIL(check_peers_trans_commit(trans, is_peers_commit))) {
      LOG_WARN("fail to check peers trans commit", KR(ret));
    } else if (!is_peers_commit) {
      usleep(WAIT_INTERVAL_US);  // 等待1s后重试
    } else {
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(confirm_finish_trans_peers(trans))) {
      LOG_WARN("fail to confirm finish trans peers", KR(ret));
    } else if (OB_FAIL(commit_trans(trans))) {
      LOG_WARN("fail to coordinator commit trans", KR(ret));
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
    } else if (OB_FAIL(check_trans_commit(trans))) {
      LOG_WARN("fail to check trans commit", KR(ret));
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
    ObDirectLoadControlAbandonTransArg arg;
    arg.table_id_ = param_.table_id_;
    arg.task_id_ = ctx_->ddl_param_.task_id_;
    arg.trans_id_ = trans_id;
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
        TABLE_LOAD_CONTROL_RPC_CALL(abandon_trans, addr, arg);
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
      if (OB_FAIL(src_obj_row.project(idx_array, out_obj_row))) {
        LOG_WARN("failed to projecte out_obj_row", KR(ret), K(src_obj_row.count_));
      } else if (OB_FAIL(obj_rows_.push_back(out_obj_row))) {
        LOG_WARN("failed to add row to obj_rows_", KR(ret), K(out_obj_row));
      }
    }
    return ret;
  }
  int process() override
  {
    OB_TABLE_LOAD_STATISTICS_TIME_COST(INFO, coordinator_write_time_us);
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
  } else if (OB_FAIL(coordinator_ctx_->check_status(ObTableLoadStatusType::LOADING))) {
    LOG_WARN("fail to check coordinator status", KR(ret));
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

int ObTableLoadCoordinator::write(const ObTableLoadTransId &trans_id,
                                  const ObTableLoadObjRowArray &obj_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinator not init", KR(ret), KP(this));
  } else if (OB_FAIL(coordinator_ctx_->check_status(ObTableLoadStatusType::LOADING))) {
    LOG_WARN("fail to check coordinator status", KR(ret));
  } else {
    LOG_DEBUG("coordinator write");
    ObTableLoadCoordinatorTrans *trans = nullptr;
    ObTableLoadTransBucketWriter *bucket_writer = nullptr;
    int32_t session_id = 0;
    if (OB_FAIL(coordinator_ctx_->get_trans(trans_id, trans))) {
      LOG_WARN("fail to get trans", KR(ret));
    } else if (FALSE_IT(session_id = trans->get_default_session_id())) {
    }
    // 取出bucket_writer
    else if (OB_FAIL(trans->get_bucket_writer_for_write(bucket_writer))) {
      LOG_WARN("fail to get bucket writer", KR(ret));
    } else {
      ObTableLoadTask *task = nullptr;
      WriteTaskProcessor *processor = nullptr;
      // 1. 分配task
      if (OB_FAIL(ctx_->alloc_task(task))) {
        LOG_WARN("fail to alloc task", KR(ret));
      }
      // 2. 设置processor
      else if (OB_FAIL(
                 task->set_processor<WriteTaskProcessor>(ctx_, trans, bucket_writer, session_id))) {
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
    OB_TABLE_LOAD_STATISTICS_TIME_COST(INFO, coordinator_flush_time_us);
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
    } else if (OB_FAIL(trans->set_trans_status_frozen())) {
      LOG_WARN("fail to freeze trans", KR(ret));
    } else {
      for (int32_t session_id = 1; OB_SUCC(ret) && session_id <= param_.write_session_count_; ++session_id) {
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
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", KR(ret), K(buf_len));
      } else if (OB_FAIL(tablet_obj_rows.serialize(buf, buf_len, pos))) {
        LOG_WARN("failed to serialize obj row array", KR(ret), KP(buf), K(buf_len), K(pos));
      } else {
        ObDirectLoadControlInsertTransArg arg;
        arg.table_id_ = param_.table_id_;
        arg.task_id_ = ctx_->ddl_param_.task_id_;
        arg.trans_id_ = trans_id;
        arg.session_id_ = session_id;
        arg.sequence_no_ = sequence_no;
        arg.payload_.assign(buf, buf_len);
        TABLE_LOAD_CONTROL_RPC_CALL(insert_trans, addr, arg);
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
