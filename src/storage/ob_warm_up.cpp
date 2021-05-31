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

#include "ob_warm_up.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/utility/utility.h"
#include "share/ob_define.h"
#include "observer/ob_server_struct.h"
#include "share/config/ob_server_config.h"
#include "share/ob_alive_server_tracer.h"
#include "clog/ob_partition_log_service.h"
#include "lib/stat/ob_diagnose_info.h"
#include "ob_partition_service.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;
namespace storage {
/**
 * ------------------------------------------------------------ObWarmUpCtx---------------------------------------------------------
 */
ObWarmUpCtx::ObWarmUpCtx()
    : allocator_(ObModIds::OB_WARM_UP_REQUEST), request_list_(allocator_), tenant_id_(0), ref_cnt_(0)
{}

ObWarmUpCtx::~ObWarmUpCtx()
{}

void ObWarmUpCtx::record_exist_check(const common::ObPartitionKey& pkey, const int64_t table_id,
    const common::ObStoreRowkey& rowkey, const common::ObIArray<share::schema::ObColDesc>& column_ids)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObWarmUpExistRequest* request = NULL;
  if (allocator_.used() > MAX_WARM_UP_REQUESTS_SIZE) {
    // do nothing
  } else if (OB_UNLIKELY(NULL == (buf = allocator_.alloc(sizeof(ObWarmUpExistRequest))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
  } else {
    request = new (buf) ObWarmUpExistRequest(allocator_);
    if (OB_FAIL(request->assign(pkey, table_id, rowkey, column_ids))) {
      STORAGE_LOG(WARN, "Fail to record get request, ", K(ret));
    } else if (OB_FAIL(request_list_.push_back(request))) {
      STORAGE_LOG(WARN, "Fail to push request to list, ", K(ret));
    }
  }
}

void ObWarmUpCtx::record_get(
    const ObTableAccessParam& param, const ObTableAccessContext& ctx, const ObExtStoreRowkey& rowkey)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObWarmUpGetRequest* request = NULL;
  if (allocator_.used() > MAX_WARM_UP_REQUESTS_SIZE) {
    // do nothing
  } else if (OB_UNLIKELY(NULL == (buf = allocator_.alloc(sizeof(ObWarmUpGetRequest))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
  } else {
    request = new (buf) ObWarmUpGetRequest(allocator_);
    if (OB_FAIL(request->assign(param, ctx, rowkey))) {
      STORAGE_LOG(WARN, "Fail to record get request, ", K(ret));
    } else if (OB_FAIL(request_list_.push_back(request))) {
      STORAGE_LOG(WARN, "Fail to push request to list, ", K(ret));
    }
  }
}

void ObWarmUpCtx::record_multi_get(
    const ObTableAccessParam& param, const ObTableAccessContext& ctx, const common::ObIArray<ObExtStoreRowkey>& rowkeys)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObWarmUpMultiGetRequest* request = NULL;
  if (allocator_.used() > MAX_WARM_UP_REQUESTS_SIZE) {
    // do nothing
  } else if (OB_UNLIKELY(NULL == (buf = allocator_.alloc(sizeof(ObWarmUpMultiGetRequest))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
  } else {
    request = new (buf) ObWarmUpMultiGetRequest(allocator_);
    if (OB_FAIL(request->assign(param, ctx, rowkeys))) {
      STORAGE_LOG(WARN, "Fail to record multi get request, ", K(ret));
    } else if (OB_FAIL(request_list_.push_back(request))) {
      STORAGE_LOG(WARN, "Fail to push request to list, ", K(ret));
    }
  }
}

void ObWarmUpCtx::record_scan(
    const ObTableAccessParam& param, const ObTableAccessContext& ctx, const ObExtStoreRange& range)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObWarmUpScanRequest* request = NULL;
  if (allocator_.used() > MAX_WARM_UP_REQUESTS_SIZE) {
    // do nothing
  } else if (OB_UNLIKELY(NULL == (buf = allocator_.alloc(sizeof(ObWarmUpScanRequest))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
  } else {
    request = new (buf) ObWarmUpScanRequest(allocator_);
    if (OB_FAIL(request->assign(param, ctx, range))) {
      STORAGE_LOG(WARN, "Fail to record scan request, ", K(ret));
    } else if (OB_FAIL(request_list_.push_back(request))) {
      STORAGE_LOG(WARN, "Fail to push request to list, ", K(ret));
    }
  }
}

void ObWarmUpCtx::record_multi_scan(
    const ObTableAccessParam& param, const ObTableAccessContext& ctx, const common::ObIArray<ObExtStoreRange>& ranges)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObWarmUpMultiScanRequest* request = NULL;
  if (allocator_.used() > MAX_WARM_UP_REQUESTS_SIZE) {
    // do nothing
  } else if (OB_UNLIKELY(NULL == (buf = allocator_.alloc(sizeof(ObWarmUpMultiScanRequest))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
  } else {
    request = new (buf) ObWarmUpMultiScanRequest(allocator_);
    if (OB_FAIL(request->assign(param, ctx, ranges))) {
      STORAGE_LOG(WARN, "Fail to record multi scan request, ", K(ret));
    } else if (OB_FAIL(request_list_.push_back(request))) {
      STORAGE_LOG(WARN, "Fail to push request to list, ", K(ret));
    }
  }
}

void ObWarmUpCtx::reuse()
{
  request_list_.reset();
  allocator_.reuse();
}

/**
 * -------------------------------------------------------------ObSendWarmUpTask---------------------------------------------------
 */
ObSendWarmUpTask::ObSendWarmUpTask()
    : IObDedupTask(T_WARM_UP_TASK), warm_service_(NULL), warm_ctx_(NULL), task_create_time_(0)
{}

ObSendWarmUpTask::~ObSendWarmUpTask()
{
  if (NULL != warm_ctx_ && 0 == warm_ctx_->dec_ref()) {
    op_free(warm_ctx_);
    warm_ctx_ = NULL;
  }
}

void ObSendWarmUpTask::assign(ObWarmUpService& warm_service, ObWarmUpCtx& warm_ctx, int64_t task_create_time)
{
  warm_service_ = &warm_service;
  warm_ctx_ = &warm_ctx;
  task_create_time_ = task_create_time;
  warm_ctx_->inc_ref();
}

int64_t ObSendWarmUpTask::get_deep_copy_size() const
{
  return sizeof(ObSendWarmUpTask);
}

IObDedupTask* ObSendWarmUpTask::deep_copy(char* buffer, const int64_t buf_size) const
{
  ObSendWarmUpTask* task = NULL;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == warm_service_) || OB_UNLIKELY(NULL == warm_ctx_)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "Invalid send warm up task, ", K(ret));
  } else if (OB_UNLIKELY(NULL == buffer) || OB_UNLIKELY(buf_size < static_cast<int64_t>(sizeof(ObSendWarmUpTask)))) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "buffer not enough", K(ret), KP(buffer), K(buf_size));
  } else {
    task = new (buffer) ObSendWarmUpTask();
    task->assign(*warm_service_, *warm_ctx_, task_create_time_);
  }
  return task;
}

int ObSendWarmUpTask::process()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL == warm_service_) || OB_UNLIKELY(NULL == warm_ctx_)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "Invalid warm up task, ", K(ret));
  } else if (task_create_time_ + TASK_EXPIRE_TIME <= ObTimeUtility::current_time()) {
    // do nothing
    EVENT_INC(WARM_UP_REQUEST_OUT_DROP_COUNT);
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {  // print drop log every 1s
      STORAGE_LOG(INFO, "drop expired warm request when create time is too old", K(task_create_time_));
    }
  } else if (OB_FAIL(warm_service_->send_warm_up_request(*warm_ctx_))) {
    STORAGE_LOG(WARN, "Fail to do warm up, ", K(ret));
  }

  return ret;
}

/**
 * -------------------------------------------------------------ObWarmUpService---------------------------------------------------------
 */
ObWarmUpService::ObWarmUpService()
    : is_inited_(false), rpc_(NULL), server_tracer_(NULL), rand_(), bandwidth_throttle_(NULL)
{}

ObWarmUpService::~ObWarmUpService()
{}

void ObWarmUpService::stop()
{
  if (is_inited_) {
    STORAGE_LOG(INFO, "warm up service is stop");
    send_task_queue_.destroy();
    is_inited_ = false;
  }
}

int ObWarmUpService::init(ObPartitionServiceRpc& pts_rpc, share::ObAliveServerTracer& server_tracer,
    common::ObInOutBandwidthThrottle& bandwidth_throttle)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_FAIL(send_task_queue_.init(
                 MAX_SEND_TASK_THREAD_CNT, "WarmupTask", SEND_TASK_QUEUE_SIZE, SEND_TASK_MAP_SIZE))) {
    STORAGE_LOG(WARN, "failed to init send task queue", K(ret));
  } else {
    send_task_queue_.set_label(ObModIds::OB_WARM_UP_SERVICE);
    rpc_ = &pts_rpc;
    server_tracer_ = &server_tracer;
    bandwidth_throttle_ = &bandwidth_throttle;
    is_inited_ = true;
  }

  return ret;
}

/*
 * yansuli: disable warm-up service for now. To avoid compatibility problem,
 * warm-up service can only be enabled *after* ObExtStoreRowkey and ObExtStoreRange
 * are used instead of ObExtRowkey and ObExtRange.
 */
int ObWarmUpService::check_need_warm_up(bool& is_need)
{
  is_need = false;
  return OB_SUCCESS;
}

/*
int ObWarmUpService::check_need_warm_up(bool &is_need)
{
  int ret = OB_SUCCESS;
  const int64_t warm_up_start_time = *(GCTX.warm_up_start_time_);
  const int64_t warm_up_duration = 0;// forbid GCONF.merger_warm_up_duration_time;
  const int64_t warm_up_max_duration = 0;// forbid GCONF.merger_warm_up_duration_time +
GCONF.merger_switch_leader_duration_time; const int64_t now = ObTimeUtility::current_time(); const int64_t cost_time =
now - warm_up_start_time;

  is_need = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (warm_up_start_time <= 0 || now > warm_up_start_time + warm_up_max_duration) {
    // do nothing
  } else {
    const int64_t task_count = send_task_queue_.task_count();
    if (task_count >= SEND_TASK_QUEUE_SIZE - SEND_TASK_QUEUE_RESERVE_COUNT) {
      EVENT_INC(WARM_UP_REQUEST_IGNORE_COUNT);
      if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
        STORAGE_LOG(INFO, "send task queue is full, ignore record new task", K(task_count));
      }
    } else if (cost_time >= warm_up_duration / 2) {

      is_need = true;
    } else if (rand_.get(0, warm_up_duration / 2) <= cost_time) {

      is_need = true;
    }
  }
  return ret;
}
*/

int ObWarmUpService::get_members(const ObPartitionKey& pkey, ObMemberList& members)
{
  int ret = OB_SUCCESS;
  common::ObRole role;
  int64_t last_active_leader_ts = 0;
  const int64_t warm_up_start_time = *GCTX.warm_up_start_time_;
  ObIPartitionGroupGuard guard;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(pkey, guard))) {
    if (OB_PARTITION_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "failed to get partition", K(ret), K(pkey), KT(pkey.get_table_id()));
    } else {
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {  // print log every 1s
        STORAGE_LOG(WARN, "failed to get partition", K(ret), K(pkey), KT(pkey.get_table_id()));
      }
      // ignore not exist partition
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(ret), K(pkey));
  } else if (OB_ISNULL(guard.get_partition_group()->get_log_service())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "log service must not null", K(ret), K(pkey));
  } else if (OB_FAIL(guard.get_partition_group()->get_log_service()->get_role_and_last_leader_active_time(
                 role, last_active_leader_ts))) {
    STORAGE_LOG(WARN, "failed to get role and last leader active time", K(ret), K(pkey));
  } else if (!is_strong_leader(role) || last_active_leader_ts > warm_up_start_time) {
    // no need to warm up if leader is taken over after warm up start
  } else if (OB_FAIL(guard.get_partition_group()->get_curr_member_list(members))) {
    STORAGE_LOG(WARN, "failed to get cur member list", K(ret));
  }

  return ret;
}

int ObWarmUpService::register_warm_up_ctx(transaction::ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  ObWarmUpCtx* warm_up_ctx = NULL;
  bool is_need = true;
  const uint64_t tenant_id = trans_desc.get_tenant_id();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(check_need_warm_up(is_need))) {
    STORAGE_LOG(WARN, "failed to check need warm up", K(ret));
  } else if (!is_need) {
    // do nothing
  } else if (NULL == (warm_up_ctx = op_alloc(ObWarmUpCtx))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc warm up ctx", K(ret));
  } else {
    warm_up_ctx->set_tenant_id(tenant_id);
    trans_desc.set_warm_up_ctx(warm_up_ctx);
  }
  return ret;
}

int ObWarmUpService::deregister_warm_up_ctx(transaction::ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  ObWarmUpCtx* warm_up_ctx = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (NULL == (warm_up_ctx = trans_desc.get_warm_up_ctx())) {
    // do nothing
  } else {
    const int64_t task_count = send_task_queue_.task_count();
    ObSendWarmUpTask task;
    task.assign(*this, *warm_up_ctx, ObTimeUtility::current_time());
    if (task_count >= SEND_TASK_QUEUE_SIZE - SEND_TASK_QUEUE_RESERVE_COUNT) {
      EVENT_INC(WARM_UP_REQUEST_IGNORE_COUNT);
      if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
        STORAGE_LOG(INFO, "send task queue is full, ignore send new task", K(task_count));
      }
    } else if (OB_FAIL(send_task_queue_.add_task(task))) {
      STORAGE_LOG(WARN, "Fail to add warm up task, ", K(ret));
    }
    trans_desc.set_warm_up_ctx(NULL);
  }
  return ret;
}

int ObWarmUpService::send_warm_up_request(const ObWarmUpCtx& warm_up_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    STORAGE_LOG(WARN, "The ObWarmUpService has not been inited, ", K(ret));
  } else {
    ObArenaAllocator allocator(ObModIds::OB_WARM_UP_SERVICE);
    obrpc::ObWarmUpRequestArg* arg = NULL;
    const ObWarmUpRequestList& request_list = warm_up_ctx.get_requests();
    const ObIWarmUpRequest* request = NULL;
    MemberMap member_map;
    ServerMap server_map;
    ObMemberList* members = NULL;
    ObMember member;
    void* tmp = NULL;

    if (OB_FAIL(member_map.create(1000, ObModIds::OB_WARM_UP_SERVICE, ObModIds::OB_WARM_UP_SERVICE))) {
      STORAGE_LOG(WARN, "Fail to create member map, ", K(ret));
    } else if (OB_FAIL(server_map.create(1000, ObModIds::OB_WARM_UP_SERVICE, ObModIds::OB_WARM_UP_SERVICE))) {
      STORAGE_LOG(WARN, "Fail to create server map, ", K(ret));
    }

    for (ObWarmUpRequestList::const_iterator iter = request_list.begin(); OB_SUCC(ret) && iter != request_list.end();
         ++iter) {
      if (NULL != (request = *iter)) {
        if (OB_FAIL(member_map.get_refactored(request->get_pkey(), members))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            if (OB_UNLIKELY(NULL == (tmp = allocator.alloc(sizeof(ObMemberList))))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
            } else {
              members = new (tmp) ObMemberList();
              if (OB_FAIL(get_members(request->get_pkey(), *members))) {
                STORAGE_LOG(WARN, "Fail to get members, ", K(ret));
              } else if (OB_FAIL(member_map.set_refactored(request->get_pkey(), members))) {
                STORAGE_LOG(WARN, "Fail to set members to map, ", K(ret));
              }
            }
          } else {
            STORAGE_LOG(WARN, "Fail to get members, ", K(request->get_pkey()), K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          for (int64_t i = 0; OB_SUCC(ret) && i < members->get_member_number(); ++i) {
            if (OB_FAIL(members->get_member_by_index(i, member))) {
              STORAGE_LOG(WARN, "Fail to get ith member, ", K(i), K(ret));
            } else if (rpc_->get_self() != member.get_server()) {
              //  && (REPLICA_TYPE_FULL == member.get_replica_type() || REPLICA_TYPE_READONLY ==
              //  member.get_replica_type())) {

              if (OB_FAIL(server_map.get_refactored(member.get_server(), arg))) {
                if (OB_HASH_NOT_EXIST == ret) {
                  ret = OB_SUCCESS;
                  if (OB_UNLIKELY(NULL == (tmp = allocator.alloc(sizeof(obrpc::ObWarmUpRequestArg))))) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
                  } else {
                    arg = new (tmp) obrpc::ObWarmUpRequestArg();
                    if (OB_FAIL(server_map.set_refactored(member.get_server(), arg))) {
                      STORAGE_LOG(WARN, "Fail to set server, ", K(ret));
                    }
                  }
                } else {
                  STORAGE_LOG(WARN, "Fail to get servers, ", K(member), K(ret));
                }
              }

              if (OB_SUCC(ret)) {
                if (OB_FAIL(arg->wrapper_.add_request(*request))) {
                  STORAGE_LOG(WARN, "Fail to add request, ", K(ret));
                }
              }
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      for (ServerMap::iterator iter = server_map.begin(); OB_SUCC(ret) && iter != server_map.end(); ++iter) {
        if (NULL != (arg = iter->second)) {
          const int64_t rpc_size = arg->get_serialize_size();
          const int64_t last_active_time = ObTimeUtility::current_time();
          const int64_t max_idle_time = INT64_MAX;
          if (OB_FAIL(bandwidth_throttle_->limit_out_and_sleep(rpc_size, last_active_time, max_idle_time))) {
            STORAGE_LOG(WARN, "failed to limit out bandwidth", K(ret));
          } else {
            if (OB_FAIL(rpc_->post_warm_up_request(iter->first, warm_up_ctx.get_tenant_id(), *arg))) {
              STORAGE_LOG(WARN,
                  "failed to post warm up request",
                  K(ret),
                  "slave",
                  iter->first,
                  "tenant_id",
                  warm_up_ctx.get_tenant_id());
            }
            EVENT_INC(WARM_UP_REQUEST_SEND_COUNT);
            EVENT_ADD(WARM_UP_REQUEST_SEND_SIZE, rpc_size);
          }
        }
      }
    }

    for (ServerMap::iterator iter = server_map.begin(); iter != server_map.end(); ++iter) {
      if (NULL != (arg = iter->second)) {
        arg->~ObWarmUpRequestArg();
      }
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
