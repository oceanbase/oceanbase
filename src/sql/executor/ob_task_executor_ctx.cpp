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

#define USING_LOG_PREFIX SQL_EXE

#include "lib/net/ob_addr.h"
#include "lib/hash/ob_hashmap.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/executor/ob_job_control.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/ob_phy_table_location.h"
#include "share/ob_common_rpc_proxy.h"
#include "sql/executor/ob_executor_rpc_impl.h"
#include "observer/ob_server.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace sql
{

int ObTaskExecutorCtx::CalcVirtualPartitionIdParams::init(uint64_t ref_table_id)
{
  int ret = common::OB_SUCCESS;
  if (true == inited_) {
    ret = common::OB_INIT_TWICE;
    LOG_ERROR("init twice", K(ret), K(inited_), K(ref_table_id));
  } else {
    inited_ = true;
    ref_table_id_ = ref_table_id;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTaskExecutorCtx,
                    table_locations_,
                    retry_times_,
                    min_cluster_version_,
                    expected_worker_cnt_,
                    admited_worker_cnt_,
                    query_tenant_begin_schema_version_,
                    query_sys_begin_schema_version_,
                    minimal_worker_cnt_);

ObTaskExecutorCtx::ObTaskExecutorCtx(ObExecContext &exec_context)
    : task_resp_handler_(NULL),
      virtual_part_servers_(exec_context.get_allocator()),
      exec_ctx_(&exec_context),
      expected_worker_cnt_(0),
      minimal_worker_cnt_(0),
      admited_worker_cnt_(0),
      retry_times_(0),
      min_cluster_version_(ObExecutorRpcCtx::INVALID_CLUSTER_VERSION),
      sys_job_id_(-1),
      rs_rpc_proxy_(nullptr),
      query_tenant_begin_schema_version_(-1),
      query_sys_begin_schema_version_(-1),
      schema_service_(GCTX.schema_service_)
{
}

ObTaskExecutorCtx::~ObTaskExecutorCtx()
{
  if (NULL != task_resp_handler_) {
    task_resp_handler_->~RemoteExecuteStreamHandle();
    task_resp_handler_ = NULL;
  }
  if (rs_rpc_proxy_ != nullptr) {
    rs_rpc_proxy_->~ObCommonRpcProxy();
    rs_rpc_proxy_ = nullptr;
  }
}

int ObTaskExecutorCtx::get_addr_by_virtual_partition_id(int64_t partition_id, ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (partition_id < 0 || partition_id >= virtual_part_servers_.size()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("addr not exist", K(ret), K(partition_id), K(virtual_part_servers_.size()));
  } else {
    ObList<ObAddr, ObIAllocator>::iterator it = virtual_part_servers_.begin();
    int64_t idx = 0;
    for (; idx != partition_id; ++it) {
      ++idx;
    }
    addr = *it;
  }
  return ret;
}

int ObTaskExecutorCtx::set_table_locations(const ObTablePartitionInfoArray &table_partition_infos)
{
  int ret = OB_SUCCESS;
  //table_locations_在这里必须先reset，确保table partition infos没有被重复添加
  table_locations_.reset();
  ObPhyTableLocation phy_table_loc;
  int64_t N = table_partition_infos.count();
  if (OB_FAIL(table_locations_.reserve(N))) {
    LOG_WARN("fail reserve locations", K(ret), K(N));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    phy_table_loc.reset();
    ObTablePartitionInfo *partition_info = table_partition_infos.at(i);
    if (OB_ISNULL(partition_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error. table partition info is null", K(ret), K(i));
    } else if (partition_info->get_table_location().use_das()) {
      //do nothing,DAS的location由自己维护和计算
    } else if (OB_FAIL(phy_table_loc.assign_from_phy_table_loc_info(partition_info->get_phy_tbl_location_info()))) {
      LOG_WARN("fail to assign_from_phy_table_loc_info", K(ret), K(i), K(partition_info->get_phy_tbl_location_info()), K(N));
    } else if (OB_FAIL(table_locations_.push_back(phy_table_loc))) {
      LOG_WARN("fail to push back into table locations", K(ret), K(i), K(phy_table_loc), K(N));
    }
  }
  return ret;
}

int ObTaskExecutorCtx::append_table_location(const ObCandiTableLoc &phy_location_info)
{
  int ret = OB_SUCCESS;
  ObPhyTableLocation phy_table_loc;
  if (OB_FAIL(phy_table_loc.assign_from_phy_table_loc_info(phy_location_info))) {
    LOG_WARN("assign from physical table location info failed", K(ret));
  } else if (OB_FAIL(table_locations_.push_back(phy_table_loc))) {
    LOG_WARN("store table location failed", K(ret));
  }
  return ret;
}

//
//
//  Utility
//
int ObTaskExecutorCtxUtil::get_stream_handler(
    ObExecContext &ctx,
    RemoteExecuteStreamHandle *&handler)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *executor_ctx = NULL;
  if (OB_ISNULL(executor_ctx = ctx.get_task_executor_ctx())) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail get executor ctx");
  } else if (OB_ISNULL(handler = executor_ctx->get_stream_handler())) {
    ret = OB_NOT_INIT;
    LOG_WARN("stream handler is not inited", K(ret));
  }
  return ret;
}
int ObTaskExecutorCtxUtil::get_task_executor_rpc(
    ObExecContext &ctx,
    ObExecutorRpcImpl *&rpc)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *executor_ctx = NULL;
  if (OB_ISNULL(executor_ctx = ctx.get_task_executor_ctx())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTaskExecutorCtx is null", K(ret));
  } else if (OB_ISNULL(rpc = executor_ctx->get_task_executor_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("rpc is null", K(ret));
  }
  return ret;
}

obrpc::ObCommonRpcProxy *ObTaskExecutorCtx::get_common_rpc()
{
  int ret = OB_SUCCESS;
  obrpc::ObCommonRpcProxy *ret_pointer = NULL;
  if (OB_FAIL(get_common_rpc(ret_pointer))) {
    LOG_WARN("get common rpc problem ", K(ret));
  }
  return ret_pointer;
}

int ObTaskExecutorCtx::get_common_rpc(obrpc::ObCommonRpcProxy *&common_rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null pointer", K_(exec_ctx), K(ret));
  } else if (OB_ISNULL(exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx is null", K(ret));
  } else {
    const int64_t timeout = exec_ctx_->get_physical_plan_ctx()->get_timeout_timestamp() -
        ObTimeUtility::current_time();
    if (rs_rpc_proxy_ == nullptr) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = exec_ctx_->get_allocator().alloc(sizeof(ObCommonRpcProxy)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate rpc proxy memory failed", K(ret));
      } else {
        rs_rpc_proxy_ = new(buf) ObCommonRpcProxy();
        *rs_rpc_proxy_ = *GCTX.rs_rpc_proxy_;
      }
    }
    if (OB_SUCC(ret)) {
      if (timeout <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("execute task timeout", K(timeout), K(ret));
      } else {
        rs_rpc_proxy_->set_timeout(timeout);
        common_rpc_proxy = rs_rpc_proxy_;
      }
    }
  }
  return ret;
}

int ObTaskExecutorCtx::reset_and_init_stream_handler()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exec_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("unexpected error. exec ctx is not inited", K(ret));
  } else {
    if (NULL != task_resp_handler_) {
      // 有可能是transaction_set_violation_and_retry引起的执行器层面的重试，
      // ObTaskExecutorCtx析构之前多次调用本函数，
      // 所以这里要先析构掉之前的内存
      task_resp_handler_->~RemoteExecuteStreamHandle();
      task_resp_handler_ = NULL;
    }
    RemoteExecuteStreamHandle *buffer = NULL;
    if (OB_ISNULL(buffer = static_cast<RemoteExecuteStreamHandle*>(exec_ctx_->get_allocator().//is this allocator ok ?
        alloc(sizeof(RemoteExecuteStreamHandle))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for RemoteExecuteStreamHandle", K(ret));
    } else {
      task_resp_handler_ = new (buffer) RemoteExecuteStreamHandle("RemoteExecStream", MTL_ID());
    }
  }
  return ret;
}

int ObTaskExecutorCtxUtil::nonblock_renew(
    ObExecContext *exec_ctx,
    const ObTabletID &tablet_id,
    const int64_t expire_renew_time,
    const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  UNUSED(cluster_id);
  UNUSED(expire_renew_time);
  if (NULL == GCTX.location_service_) {
    ret = OB_NOT_INIT;
    LOG_WARN("loc_cache is NULL", K(ret));
  } else if (OB_FAIL(GCTX.location_service_->nonblock_renew(GET_MY_SESSION(*exec_ctx)->get_effective_tenant_id(),
        tablet_id))) {
    LOG_WARN("nonblock_renew failed", K(tablet_id), K(ret));
  }
  return ret;
}

int ObTaskExecutorCtx::nonblock_renew_with_limiter(
                                const ObTabletID &tablet_id,
                                const int64_t expire_renew_time,
                                bool &is_limited)
{
  int ret = OB_SUCCESS;
  UNUSED(expire_renew_time);
  is_limited = false;
  if (NULL == GCTX.location_service_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tmp_loc_cache is NULL", K(ret));
  } else if (OB_FAIL(GCTX.location_service_->nonblock_renew(GET_MY_SESSION(*exec_ctx_)->get_effective_tenant_id(),
        tablet_id))) {
    LOG_WARN("nonblock_renew failed", K(tablet_id), K(ret));
  }
  return ret;
}

void ObTaskExecutorCtx::set_self_addr(const common::ObAddr &self_addr)
{
  UNUSED(self_addr);
}
const common::ObAddr ObTaskExecutorCtx::get_self_addr() const
{
  return MYADDR;
}
}/* ns sql*/
}/* ns oceanbase */
