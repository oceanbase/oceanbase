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
#include "common/ob_partition_key.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/executor/ob_job_control.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/ob_phy_table_location.h"
#include "sql/ob_sql_partition_location_cache.h"
#include "share/ob_common_rpc_proxy.h"
#include "storage/ob_partition_service.h"
#include "sql/executor/ob_executor_rpc_impl.h"
#include "observer/ob_server.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace sql {

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

OB_SERIALIZE_MEMBER(ObTaskExecutorCtx, table_locations_, retry_times_, min_cluster_version_, expected_worker_cnt_,
    allocated_worker_cnt_);

ObTaskExecutorCtx::ObTaskExecutorCtx(ObExecContext& exec_context)
    : task_resp_handler_(NULL),
      execute_result_(),
      virtual_part_servers_(exec_context.get_allocator()),
      calc_params_(),
      last_failed_partitions_(),
      exec_ctx_(&exec_context),
      partition_infos_(exec_context.get_allocator()),
      need_renew_location_cache_(false),
      need_renew_partition_keys_(exec_context.get_allocator()),
      expected_worker_cnt_(0),
      allocated_worker_cnt_(0),
      table_locations_(),
      retry_times_(0),
      min_cluster_version_(ObExecutorRpcCtx::INVALID_CLUSTER_VERSION),
      sys_job_id_(-1),
      partition_location_cache_(NULL),
      task_executor_rpc_(NULL),
      rs_rpc_proxy_(),
      srv_rpc_proxy_(NULL),
      query_tenant_begin_schema_version_(-1),
      query_sys_begin_schema_version_(-1),
      schema_service_(NULL),
      partition_service_(NULL),
      vt_partition_service_(NULL)
{}

ObTaskExecutorCtx::~ObTaskExecutorCtx()
{
  if (NULL != partition_location_cache_) {
    (static_cast<ObSqlPartitionLocationCache*>(partition_location_cache_))
        ->set_task_exec_ctx(NULL);  // FIXME:static_cast is not safe
  }
  if (NULL != task_resp_handler_) {
    task_resp_handler_->~RemoteExecuteStreamHandle();
    task_resp_handler_ = NULL;
  }
}

int ObTaskExecutorCtx::is_valid_addr(uint64_t ref_table_id, const ObAddr& addr, bool& is_valid) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObPartitionLocation, 16> locs;
  bool is_cache_hit = false;
  const int64_t expire_renew_time = 0;
  ObSqlPartitionLocationCache::LocationDistributedMode loc_dist_mode =
      ObSqlPartitionLocationCache::LOC_DIST_MODE_INVALID;

  is_valid = false;
  if (OB_ISNULL(partition_location_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("partition_location_cache_ is NULL", K(ret), K(addr), K(ref_table_id));
  } else if (OB_UNLIKELY(ObIPartitionLocationCache::PART_LOC_CACHE_TYPE_SQL != partition_location_cache_->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("partition_location_cache_ is not sql type",
        K(ret),
        K(addr),
        K(ref_table_id),
        K(partition_location_cache_->get_type()));
  } else if (FALSE_IT(loc_dist_mode = (static_cast<ObSqlPartitionLocationCache*>(partition_location_cache_))
                                          ->get_location_distributed_mode(ref_table_id))) {
  } else if (OB_UNLIKELY(ObSqlPartitionLocationCache::LOC_DIST_MODE_INVALID == loc_dist_mode)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("location distributed mode is invalid", K(ret), K(addr), K(ref_table_id));
  } else if (ObSqlPartitionLocationCache::LOC_DIST_MODE_ONLY_LOCAL == loc_dist_mode ||
             (ObSqlPartitionLocationCache::LOC_DIST_MODE_DISTRIBUTED == loc_dist_mode)) {
    if (addr == MYADDR) {
      is_valid = true;
    }
  }
  if (OB_SUCC(ret) && ObSqlPartitionLocationCache::LOC_DIST_MODE_ONLY_LOCAL != loc_dist_mode && !is_valid) {
    if (OB_FAIL(partition_location_cache_->get(ref_table_id, locs, expire_renew_time, is_cache_hit))) {
      LOG_WARN("fail to get locations from partition location cache", K(ret), K(ref_table_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && false == is_valid && i < locs.count(); ++i) {
        const ObIArray<ObReplicaLocation>& rep_locs = locs.at(i).get_replica_locations();
        if (1 != rep_locs.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR(
              "virtual table must have only one replica location", K(ret), K(i), K(rep_locs.count()), K(rep_locs));
        } else if (rep_locs.at(0).server_ == addr) {
          is_valid = true;
        }
      }
    }
  }
  return ret;
}

int ObTaskExecutorCtx::calc_virtual_partition_id(uint64_t ref_table_id, const ObAddr& addr, int64_t& partition_id)
{
  int ret = OB_SUCCESS;
  bool addr_found = false;
  int64_t idx = 0;
  FOREACH_X(it, virtual_part_servers_, OB_SUCC(ret) && !addr_found)
  {
    if (*it == addr) {
      partition_id = idx;
      addr_found = true;
    }
    ++idx;
  }
  if (!addr_found) {
    bool addr_is_valid = false;
    if (OB_FAIL(is_valid_addr(ref_table_id, addr, addr_is_valid))) {
      LOG_WARN("fail to check if it is valid addr", K(ret), K(addr));
    } else {
      if (false == addr_is_valid) {
        partition_id = OB_INVALID_PARTITION_ID;
        LOG_WARN("invalid partition id", K(ref_table_id), K(addr), K(partition_id));
      } else {
        if (OB_FAIL(virtual_part_servers_.push_back(addr))) {
          LOG_WARN("fail to push back partition server", K(ret), K(addr));
        } else {
          partition_id = virtual_part_servers_.size() - 1;
        }
      }
    }
  }
  return ret;
}

int ObTaskExecutorCtx::get_addr_by_virtual_partition_id(int64_t partition_id, ObAddr& addr)
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

int ObTaskExecutorCtx::set_table_locations(const ObTablePartitionInfoArray& table_partition_infos)
{
  int ret = OB_SUCCESS;
  table_locations_.reset();
  ObPhyTableLocation phy_table_loc;
  int64_t N = table_partition_infos.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    phy_table_loc.reset();
    ObTablePartitionInfo* partition_info = table_partition_infos.at(i);
    if (OB_ISNULL(partition_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error. table partition info is null", K(ret), K(i));
    } else if (OB_FAIL(phy_table_loc.assign_from_phy_table_loc_info(partition_info->get_phy_tbl_location_info()))) {
      LOG_WARN("fail to assign_from_phy_table_loc_info", K(ret), K(i), K(partition_info->get_phy_tbl_location_info()));
    } else if (OB_FAIL(table_locations_.push_back(phy_table_loc))) {
      LOG_WARN("fail to push back into table locations", K(ret), K(i), K(phy_table_loc));
    }
  }
  return ret;
}

int ObTaskExecutorCtx::append_table_location(const ObPhyTableLocationInfo& phy_location_info)
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

int ObTaskExecutorCtx::set_table_locations(const ObPhyTableLocationIArray& phy_table_locations)
{
  int ret = OB_SUCCESS;
  table_locations_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < phy_table_locations.count(); ++i) {
    if (OB_FAIL(table_locations_.push_back(phy_table_locations.at(i)))) {
      LOG_WARN("fail to push back into table locations", K(ret), K(i));
    }
  }
  return ret;
}

int ObTaskExecutorCtx::add_need_renew_partition_keys_distinctly(const ObPartitionKey& part_key)
{
  int ret = OB_SUCCESS;
  bool has_found = false;
  if (OB_UNLIKELY(!part_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition key is invalid", K(ret), K(part_key));
  }
  FOREACH_X(it, need_renew_partition_keys_, OB_SUCC(ret) && !has_found)
  {
    if (part_key == *it) {
      has_found = true;
    }
  }
  if (OB_SUCC(ret) && !has_found) {
    if (OB_FAIL(need_renew_partition_keys_.push_back(part_key))) {
      LOG_WARN("fail to push back partition key", K(ret), K(part_key));
    } else {
      LOG_DEBUG("add dated partition location key", K(part_key));
    }
  }
  return ret;
}

ObPhyTableLocationIArray& ObTaskExecutorCtx::get_table_locations()
{
  return table_locations_;
}

const ObTablePartitionInfoArray& ObTaskExecutorCtx::get_partition_infos() const
{
  return partition_infos_;
}

int ObTaskExecutorCtx::merge_last_failed_partitions()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec_ctx_ is NULL", K(ret));
  } else {
    ObSchedulerThreadCtx& sche_thread_ctx = exec_ctx_->get_scheduler_thread_ctx();
    const ObIArray<ObTaskInfo::ObRangeLocation*>& failed_partitions = sche_thread_ctx.get_last_failed_partitions();
    for (int64_t i = 0; OB_SUCC(ret) && i < failed_partitions.count(); ++i) {
      if (OB_FAIL(last_failed_partitions_.push_back(failed_partitions.at(i)))) {
        LOG_WARN("fail to push back failed partition", K(i), K(ret));
      }
    }
  }
  return ret;
}

//
//
//  Utility
//
int ObTaskExecutorCtxUtil::get_stream_handler(ObExecContext& ctx, RemoteExecuteStreamHandle*& handler)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = NULL;
  if (OB_ISNULL(executor_ctx = ctx.get_task_executor_ctx())) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail get executor ctx");
  } else if (OB_ISNULL(handler = executor_ctx->get_stream_handler())) {
    ret = OB_NOT_INIT;
    LOG_WARN("stream handler is not inited", K(ret));
  }
  return ret;
}
int ObTaskExecutorCtxUtil::get_task_executor_rpc(ObExecContext& ctx, ObExecutorRpcImpl*& rpc)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = NULL;
  if (OB_ISNULL(executor_ctx = ctx.get_task_executor_ctx())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTaskExecutorCtx is null", K(ret));
  } else if (OB_ISNULL(rpc = executor_ctx->get_task_executor_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("rpc is null", K(ret));
  }
  return ret;
}

int ObTaskExecutorCtxUtil::get_part_runner_server(
    ObExecContext& ctx, uint64_t table_id, uint64_t index_tid, int64_t part_id, ObAddr& runner_server)
{
  int ret = OB_SUCCESS;
  const ObPhyTableLocation* table_location = NULL;
  const ObPartitionReplicaLocation* part_replica = NULL;
  if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(ctx, table_id, index_tid, table_location))) {
    LOG_WARN("get physical table location failed", K(ret));
  } else if (OB_ISNULL(table_location)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy table location is null", K(table_id), K(ret));
  } else if (OB_ISNULL(part_replica = table_location->get_part_replic_by_part_id(part_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy part replica is null", K(part_id));
  } else {
    runner_server = part_replica->get_replica_location().server_;
  }
  return ret;
}

int ObTaskExecutorCtxUtil::get_full_table_phy_table_location(ObExecContext& ctx, uint64_t table_location_key,
    uint64_t ref_table_id, bool is_weak, const ObPhyTableLocation*& table_location)
{
  int ret = OB_SUCCESS;
  ObPhyTableLocationInfo phy_location_info;
  ObTaskExecutorCtx& executor_ctx = ctx.get_task_exec_ctx();
  ObIPartitionLocationCache* location_cache = executor_ctx.get_partition_location_cache();
  share::schema::ObMultiVersionSchemaService* schema_service = executor_ctx.schema_service_;
  share::schema::ObSchemaGetterGuard schema_guard;
  ObSEArray<int64_t, 16> part_ids;
  const ObTableSchema* table_schema = NULL;
  const uint64_t tenant_id = extract_tenant_id(ref_table_id);
  // ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  ObPhyTableLocation* loc = nullptr;
  table_location = NULL;

  if (OB_ISNULL(location_cache) || OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location cache or schema_service is null", KP(location_cache), KP(schema_service), K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("get table schema failed");
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
  } else {
    bool check_dropped_schema = false;
    schema::ObTablePartitionKeyIter keys(*table_schema, check_dropped_schema);
    while (OB_SUCC(ret)) {
      int64_t part_id = 0;
      if (OB_FAIL(keys.next_partition_id_v2(part_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("partition id iterate failed", K(ret));
        }
      } else if (OB_FAIL(part_ids.push_back(part_id))) {
        LOG_WARN("array push back failed", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    // bypass
  } else if (NULL == (loc = static_cast<ObPhyTableLocation*>(ctx.get_allocator().alloc(sizeof(ObPhyTableLocation))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (NULL == (loc = new (loc) ObPhyTableLocation())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail new object", K(ret));
  } else if (OB_FAIL(ObTableLocation::get_phy_table_location_info(
                 ctx, table_location_key, ref_table_id, is_weak, part_ids, *location_cache, phy_location_info))) {
    LOG_WARN("get phy table location info failed", K(ret));
  } else if (OB_FAIL(loc->add_partition_locations(phy_location_info))) {
    LOG_WARN("add partition locations failed", K(ret), K(phy_location_info));
  } else {
    table_location = loc;
  }
  return ret;
}

int ObTaskExecutorCtxUtil::get_phy_table_location(
    ObExecContext& ctx, uint64_t table_location_key, uint64_t ref_table_id, const ObPhyTableLocation*& table_location)
{
  int ret = OB_SUCCESS;
  table_location = NULL;
  if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
          ctx.get_task_exec_ctx(), table_location_key, ref_table_id, table_location))) {
    LOG_WARN("fail to get phy table location", K(ret), K(table_location_key), K(ref_table_id));
  }
  return ret;
}

int ObTaskExecutorCtxUtil::get_phy_table_location_for_update(
    ObExecContext& ctx, uint64_t table_location_key, uint64_t ref_table_id, ObPhyTableLocation*& table_location)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = NULL;
  table_location = NULL;
  if (OB_ISNULL(executor_ctx = ctx.get_task_executor_ctx())) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to get executor ctx");
  } else if (OB_FAIL(
                 get_phy_table_location_for_update(*executor_ctx, table_location_key, ref_table_id, table_location))) {
    LOG_WARN("fail to get phy table location", K(ret), K(table_location_key), K(ref_table_id));
  }
  return ret;
}

int ObTaskExecutorCtxUtil::get_phy_table_location(ObTaskExecutorCtx& executor_ctx, uint64_t table_location_key,
    uint64_t ref_table_id, const ObPhyTableLocation*& table_location)
{
  return get_phy_table_location_for_update(
      executor_ctx, table_location_key, ref_table_id, const_cast<ObPhyTableLocation*&>(table_location));
}

int ObTaskExecutorCtxUtil::get_phy_table_location_for_update(ObTaskExecutorCtx& executor_ctx,
    uint64_t table_location_key, uint64_t ref_table_id, ObPhyTableLocation*& table_location)
{
  int ret = OB_SUCCESS;
  bool found = false;
  table_location = NULL;
  for (int64_t i = 0; !found && i < executor_ctx.get_table_locations().count(); ++i) {
    found = (table_location_key == executor_ctx.get_table_locations().at(i).get_table_location_key() &&
             (ref_table_id == executor_ctx.get_table_locations().at(i).get_ref_table_id()));
    if (found) {
      table_location = &(executor_ctx.get_table_locations().at(i));
    }
  }
  if (!found) {
    LOG_WARN("failed to find table location",
        K(table_location_key),
        K(ref_table_id),
        "all_locations",
        executor_ctx.get_table_locations());
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

ObPhyTableLocation* ObTaskExecutorCtxUtil::get_phy_table_location_for_update(
    ObTaskExecutorCtx& executor_ctx, uint64_t table_location_key, uint64_t ref_table_id)
{
  ObPhyTableLocation* table_loc = NULL;
  for (int64_t i = 0; NULL == table_loc && i < executor_ctx.get_table_locations().count(); ++i) {
    if (table_location_key == executor_ctx.get_table_locations().at(i).get_table_location_key() &&
        (ref_table_id == executor_ctx.get_table_locations().at(i).get_ref_table_id())) {
      table_loc = &(executor_ctx.get_table_locations().at(i));
    }
  }
  return table_loc;
}

int ObTaskExecutorCtxUtil::extract_server_participants(
    ObExecContext& ctx, const ObAddr& svr, ObPartitionIArray& participants)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = NULL;
  if (OB_ISNULL(executor_ctx = ctx.get_task_executor_ctx())) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to get executor ctx", K(ret));
  } else {
    const ObPhyTableLocationIArray& tb_locs = executor_ctx->get_table_locations();
    int64_t tbloc_cnt = tb_locs.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < tbloc_cnt; ++i) {
      const ObPartitionReplicaLocationIArray& ploc_arr = tb_locs.at(i).get_partition_location_list();
      int64_t ploc_cnt = ploc_arr.count();
      bool is_vtable = false;
      for (int j = 0; OB_SUCC(ret) && j < ploc_cnt && false == is_vtable; ++j) {
        const share::ObPartitionReplicaLocation& ploc = ploc_arr.at(j);
        if (true == is_virtual_table(ploc.get_table_id())) {
          is_vtable = true;
        } else {
          const share::ObReplicaLocation& rep_loc = ploc.get_replica_location();
          if (!rep_loc.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("replica location is invalid", K(ret), K(rep_loc));
          } else if (svr == rep_loc.server_) {
            ObPartitionKey key;
            if (OB_FAIL(key.init(ploc.get_table_id(), ploc.get_partition_id(), ploc.get_partition_cnt()))) {
              LOG_WARN("fail init key", K(key));
            } else if (OB_FAIL(ObSqlTransControl::append_participant_to_array_distinctly(participants, key))) {
              LOG_WARN("fail to append participant to array distinctly", K(ret), K(key), K(participants));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTaskExecutorCtxUtil::get_participants(ObExecContext& ctx, const ObTask& task, ObPartitionLeaderArray& pla)
{
  int ret = OB_SUCCESS;
  pla.reset();
  ObPartitionLeaderArray participants;
  const ObPartitionIArray& pkeys = task.get_partition_keys();
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx);
  ObPartitionType type;
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("my_session is NULL", K(ret), K(my_session));
  } else if (pkeys.count() > 0) {
    ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
    const ObIArray<ObPhyTableLocation>& table_locations = task_exec_ctx.get_table_locations();
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      const ObPartitionKey& pkey = pkeys.at(i);
      type = ObPhyTableLocation::get_partition_type(pkey, table_locations, my_session->get_is_in_retry_for_dup_tbl());
      if (is_virtual_table(pkey.get_table_id())) {
      } else if (OB_FAIL(ObSqlTransControl::append_participant_to_array_distinctly(pla, pkey, type, ctx.get_addr()))) {
        LOG_WARN("fail to add participant", K(ret), K(pkey), K(ctx.get_addr()));
      }
    }
  } else {
    if (OB_FAIL(ObSqlTransControl::get_participants(ctx, pla))) {
      LOG_WARN("fail to get participants", K(ret));
    }
  }
  return ret;
}

int ObTaskExecutorCtxUtil::translate_pid_to_ldx(ObTaskExecutorCtx& ctx, int64_t partition_id,
    int64_t table_location_key, int64_t ref_table_id, int64_t& location_idx)
{
  int ret = OB_SUCCESS;
  location_idx = -1;
  const ObPhyTableLocation* table_location = NULL;
  if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(ctx, table_location_key, ref_table_id, table_location))) {
    LOG_WARN("failed to get physical table location", K(table_location_key), K(ref_table_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_location->get_partition_location_list().count(); ++i) {
      if (table_location->get_partition_location_list().at(i).get_partition_id() == partition_id) {
        location_idx = i;
      }
    }
    if (OB_SUCC(ret) && location_idx == -1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the partition_idx do not find in phy table location",
          K(ret),
          K(partition_id),
          K(table_location->get_partition_location_list()));
    }
  }
  return ret;
}

obrpc::ObCommonRpcProxy* ObTaskExecutorCtx::get_common_rpc()
{
  int ret = OB_SUCCESS;
  obrpc::ObCommonRpcProxy* ret_pointer = NULL;
  if (OB_FAIL(get_common_rpc(ret_pointer))) {
    LOG_WARN("get common rpc problem ", K(ret));
  }
  return ret_pointer;
}

int ObTaskExecutorCtx::get_common_rpc(obrpc::ObCommonRpcProxy*& common_rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null pointer", K_(exec_ctx), K(ret));
  } else if (OB_ISNULL(exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx is null", K(ret));
  } else {
    const int64_t timeout = exec_ctx_->get_physical_plan_ctx()->get_timeout_timestamp() - ObTimeUtility::current_time();
    if (timeout <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("execute task timeout", K(timeout), K(ret));
    } else {
      rs_rpc_proxy_.set_timeout(timeout);
      common_rpc_proxy = &rs_rpc_proxy_;
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
      task_resp_handler_->~RemoteExecuteStreamHandle();
      task_resp_handler_ = NULL;
    }
    RemoteExecuteStreamHandle* buffer = NULL;
    if (OB_ISNULL(
            buffer = static_cast<RemoteExecuteStreamHandle*>(exec_ctx_->get_allocator().  // is this allocator ok ?
                                                             alloc(sizeof(RemoteExecuteStreamHandle))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for RemoteExecuteStreamHandle", K(ret));
    } else {
      task_resp_handler_ = new (buffer) RemoteExecuteStreamHandle(ObModIds::OB_SQL_EXECUTOR);
    }
  }
  return ret;
}

int64_t ObTaskExecutorCtx::get_related_part_cnt() const
{
  int64_t total_cnt = 0;
  for (int64_t i = 0; i < table_locations_.count(); i++) {
    total_cnt += table_locations_.at(i).get_partition_cnt();
  }

  return total_cnt;
}

int ObTaskExecutorCtxUtil::try_nonblock_refresh_location_cache(
    ObTaskExecutorCtx* task_executor_ctx, ObPartitionIArray& partition_keys, const int64_t expire_renew_time /* = 0 */)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task_executor_ctx) || partition_keys.empty()) {
    // skip
  } else {
    ObIPartitionLocationCache* cache = task_executor_ctx->get_partition_location_cache();
    if (OB_ISNULL(cache)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("cache is NULL", K(ret));
    } else {
      ARRAY_FOREACH(partition_keys, idx)
      {
        bool is_limited = false;
        if (OB_FAIL(cache->nonblock_renew_with_limiter(partition_keys.at(idx), expire_renew_time, is_limited))) {
          LOG_WARN("LOCATION: fail to renew", K(ret), K(partition_keys.at(idx)), K(expire_renew_time), K(is_limited));
        } else {
          LOG_INFO("LOCATION: noblock renew with limiter", K(partition_keys.at(idx)));
        }
      }
    }
  }
  return ret;
}

int ObTaskExecutorCtxUtil::refresh_location_cache(ObTaskExecutorCtx& task_exec_ctx, bool is_nonblock)
{
  int ret = OB_SUCCESS;
  NG_TRACE_TIMES(1, get_location_cache_begin);
  ObIPartitionLocationCache* cache = NULL;
  bool is_cache_hit = false;
  if (OB_ISNULL(cache = task_exec_ctx.get_partition_location_cache())) {
    ret = OB_NOT_INIT;
    LOG_WARN("loc cache not inited", K(ret));
  } else {
    ObPartitionLocation dummy_loc;
    const ObIArray<ObTaskInfo::ObRangeLocation*>& last_failed_parts = task_exec_ctx.get_last_failed_partitions();
    if (last_failed_parts.count() > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < last_failed_parts.count(); ++i) {
        if (OB_ISNULL(last_failed_parts.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error. last failed partinfo is null", K(ret), K(i));
        } else {
          const ObTaskInfo::ObRangeLocation& loc = *(last_failed_parts.at(i));
          for (int64_t j = 0; OB_SUCC(ret) && j < loc.part_locs_.count(); ++j) {
            const ObTaskInfo::ObPartLoc& task_part_loc = loc.part_locs_.at(j);
            if (is_nonblock) {
              if (OB_FAIL(cache->nonblock_renew(task_part_loc.partition_key_, 0))) {
                LOG_WARN("LOCATION: fail to nonblock renew location cache", K(ret), K(task_part_loc), K(loc));
              } else {
#if !defined(NDEBUG)
                LOG_INFO("LOCATION: nonblock renew success", "partition", task_part_loc.partition_key_);
#endif
              }
            } else {
              dummy_loc.reset();
              if (OB_FAIL(
                      cache->get(task_part_loc.partition_key_, dummy_loc, task_part_loc.renew_time_, is_cache_hit))) {
                LOG_WARN("LOCATION: refresh cache failed", K(ret), K(task_part_loc));
              } else {
#if !defined(NDEBUG)
                LOG_INFO("LOCATION: refresh table cache succ", K(task_part_loc), K(dummy_loc));
#endif
              }
            }
          }  // end inner for
        }    // end else
      }      // end outer for
    } else {
      ObPartitionKey part_key;
      const ObPhyTableLocationIArray& tables = task_exec_ctx.get_table_locations();
      for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
        const ObPhyTableLocation& table_loc = tables.at(i);
        const ObPartitionReplicaLocationIArray& part_loc_list = table_loc.get_partition_location_list();
        for (int64_t j = 0; OB_SUCC(ret) && j < part_loc_list.count(); ++j) {
          const ObPartitionReplicaLocation& part_loc = part_loc_list.at(j);
          part_key.reset();
          if (OB_FAIL(part_loc.get_partition_key(part_key))) {
            LOG_WARN("fail to get partition key", K(ret), K(part_loc), K(part_key));
          } else {
            if (is_nonblock) {
              const int64_t expire_renew_time = 0;
              if (OB_FAIL(cache->nonblock_renew(part_key, expire_renew_time))) {
                LOG_WARN("LOCATION: fail to nonblock renew location cache", K(ret), K(part_key), K(expire_renew_time));
              } else {
#if !defined(NDEBUG)
                LOG_INFO("LOCATION: nonblock renew success", K(part_key), K(expire_renew_time));
#endif
              }
            } else {
              dummy_loc.reset();
              const int64_t expire_renew_time = INT64_MAX;
              if (OB_FAIL(cache->get(part_key, dummy_loc, expire_renew_time, is_cache_hit))) {
                LOG_WARN("LOCATION: refresh cache failed", K(ret), K(part_key), K(expire_renew_time));
              } else {
#if !defined(NDEBUG)
                LOG_INFO("LOCATION: refresh table cache succ", K(part_key), K(dummy_loc));
#endif
              }
            }
          }
        }
      }
    }
  }
  NG_TRACE_TIMES(1, get_location_cache_end);
  return ret;
}

void ObTaskExecutorCtx::set_self_addr(const common::ObAddr& self_addr)
{
  UNUSED(self_addr);
}
const common::ObAddr ObTaskExecutorCtx::get_self_addr() const
{
  return MYADDR;
}

int ObTaskExecutorCtx::append_table_locations_no_dup(const ObTablePartitionInfoArray& table_partition_infos)
{
  int ret = OB_SUCCESS;
  ObPhyTableLocation phy_table_loc;
  int64_t N = table_partition_infos.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    phy_table_loc.reset();
    bool find = false;
    ObTablePartitionInfo* partition_info = table_partition_infos.at(i);
    if (OB_ISNULL(partition_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error. table partition info is null", K(ret), K(i));
    } else if (OB_FAIL(phy_table_loc.assign_from_phy_table_loc_info(partition_info->get_phy_tbl_location_info()))) {
      LOG_WARN("fail to assign_from_phy_table_loc_info", K(ret), K(i), K(partition_info->get_phy_tbl_location_info()));
    }

    for (int64_t j = 0; OB_SUCC(ret) && !find && j < table_locations_.count(); ++j) {
      const ObPhyTableLocation& cur_location = table_locations_.at(j);
      if (phy_table_loc == cur_location) {
        if (OB_UNLIKELY(phy_table_loc.get_partition_cnt() != 1) || OB_UNLIKELY(cur_location.get_partition_cnt() != 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected partition count",
              K(ret),
              K(phy_table_loc.get_partition_cnt()),
              K(cur_location.get_partition_cnt()));
        } else if (phy_table_loc.get_partition_location_list().at(0) ==
                   cur_location.get_partition_location_list().at(0)) {
          find = true;
        }
      }
    }
    if (OB_SUCC(ret) && !find) {
      if (OB_FAIL(table_locations_.push_back(phy_table_loc))) {
        LOG_WARN("failed to push back phy table location", K(ret));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
