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

#ifndef OCEANBASE_SQL_TASK_EXECUTOR_CTX_
#define OCEANBASE_SQL_TASK_EXECUTOR_CTX_

#include "share/partition_table/ob_partition_location_cache.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_common_rpc_proxy.h"
#include "sql/executor/ob_executor_rpc_impl.h"
#include "sql/executor/ob_execute_result.h"
#include "sql/ob_phy_table_location.h"
#include "sql/optimizer/ob_table_partition_info.h"
#include "sql/ob_sql_context.h"
#include "share/ob_worker.h"
#include "lib/list/ob_list.h"
namespace oceanbase {
namespace common {
class ObAddr;
class ObIDataAccessService;
}  // namespace common

namespace obrpc {
class ObSrvRpcProxy;
class ObCommonRpcProxy;
}  // namespace obrpc

namespace sql {

typedef common::ObIArray<ObPhyTableLocation> ObPhyTableLocationIArray;
typedef common::ObIArray<ObPhyTableLocationInfo> ObPhyTableLocationInfoIArray;
typedef common::ObSEArray<ObPhyTableLocation, 2> ObPhyTableLocationFixedArray;

class ObJobControl;
class ObExecContext;
class ObTaskExecutorCtx {
  OB_UNIS_VERSION(1);

public:
  class CalcVirtualPartitionIdParams {
  public:
    CalcVirtualPartitionIdParams() : inited_(false), ref_table_id_(common::OB_INVALID_ID)
    {}
    ~CalcVirtualPartitionIdParams()
    {}

    int init(uint64_t ref_table_id);
    inline void reset()
    {
      inited_ = false;
      ref_table_id_ = common::OB_INVALID_ID;
    }
    inline bool is_inited() const
    {
      return inited_;
    }
    inline uint64_t get_ref_table_id() const
    {
      return ref_table_id_;
    }

    TO_STRING_KV(K_(inited), K_(ref_table_id));

  private:
    bool inited_;
    uint64_t ref_table_id_;
  };

  explicit ObTaskExecutorCtx(ObExecContext& exec_context);
  virtual ~ObTaskExecutorCtx();

  int calc_virtual_partition_id(uint64_t ref_table_id, const common::ObAddr& addr, int64_t& partition_id);
  int get_addr_by_virtual_partition_id(int64_t partition_id, common::ObAddr& addr);
  int set_table_locations(const ObTablePartitionInfoArray& table_partition_infos);
  int append_table_location(const ObPhyTableLocationInfo& phy_location_info);
  int set_table_locations(const ObPhyTableLocationIArray& phy_table_locations);
  int init_table_location(int64_t count)
  {
    return table_locations_.reserve(count);
  }
  inline void set_need_renew_location_cache(bool need_renew_location_cache)
  {
    need_renew_location_cache_ = need_renew_location_cache;
  }
  inline bool is_need_renew_location_cache() const
  {
    return need_renew_location_cache_;
  }
  inline const common::ObList<common::ObPartitionKey, common::ObIAllocator>& get_need_renew_partition_keys() const
  {
    return need_renew_partition_keys_;
  }
  int add_need_renew_partition_keys_distinctly(const common::ObPartitionKey& part_key);
  ObPhyTableLocationIArray& get_table_locations();
  const ObTablePartitionInfoArray& get_partition_infos() const;
  int64_t get_related_part_cnt() const;
  inline RemoteExecuteStreamHandle* get_stream_handler()
  {
    return task_resp_handler_;
  }
  int reset_and_init_stream_handler();
  inline void set_partition_location_cache(share::ObIPartitionLocationCache* cache)
  {
    partition_location_cache_ = cache;
  }
  inline share::ObIPartitionLocationCache* get_partition_location_cache()
  {
    return partition_location_cache_;
  }
  /**
   * @brief: job control helps task getting child/parent task info
   */
  inline void set_task_executor_rpc(ObExecutorRpcImpl& exec_proxy)
  {
    task_executor_rpc_ = &exec_proxy;
  }
  ObExecutorRpcImpl* get_task_executor_rpc()
  {
    return task_executor_rpc_;
  }
  inline void set_rs_rpc(obrpc::ObCommonRpcProxy& rs_proxy)
  {
    rs_rpc_proxy_ = rs_proxy;
  }
  // int get_common_rpc(obrpc::ObCommonRpcProxy *&)
  obrpc::ObCommonRpcProxy* get_common_rpc();

  int get_common_rpc(obrpc::ObCommonRpcProxy*& common_rpc_proxy);
  inline ObExecuteResult& get_execute_result()
  {
    return execute_result_;
  }
  inline void set_partition_service(storage::ObPartitionService* partition_service)
  {
    partition_service_ = partition_service;
  }
  inline void set_vt_partition_service(common::ObIDataAccessService* vt_partition_service)
  {
    vt_partition_service_ = vt_partition_service;
  }
  inline storage::ObPartitionService* get_partition_service()
  {
    return partition_service_;
  }
  inline common::ObIDataAccessService* get_vt_partition_service()
  {
    return vt_partition_service_;
  }
  inline obrpc::ObSrvRpcProxy* get_srv_rpc()
  {
    return srv_rpc_proxy_;
  }
  inline void set_srv_rpc(obrpc::ObSrvRpcProxy& srv_proxy)
  {
    srv_rpc_proxy_ = &srv_proxy;
  }
  inline void set_query_tenant_begin_schema_version(const int64_t schema_version)
  {
    query_tenant_begin_schema_version_ = schema_version;
  }
  void set_self_addr(const common::ObAddr& self_addr);
  const common::ObAddr get_self_addr() const;
  inline int64_t get_query_tenant_begin_schema_version() const
  {
    return query_tenant_begin_schema_version_;
  }
  inline void set_query_sys_begin_schema_version(const int64_t schema_version)
  {
    query_sys_begin_schema_version_ = schema_version;
  }
  inline int64_t get_query_sys_begin_schema_version() const
  {
    return query_sys_begin_schema_version_;
  }
  // init_calc_virtual_part_id_params and reset_calc_virtual_part_id_params need used in pair.
  inline int init_calc_virtual_part_id_params(uint64_t ref_table_id)
  {
    return calc_params_.init(ref_table_id);
  }
  inline void reset_calc_virtual_part_id_params()
  {
    calc_params_.reset();
  }
  inline const CalcVirtualPartitionIdParams& get_calc_virtual_part_id_params() const
  {
    return calc_params_;
  }
  int merge_last_failed_partitions();
  inline const common::ObIArray<ObTaskInfo::ObRangeLocation*>& get_last_failed_partitions() const
  {
    return last_failed_partitions_;
  }
  inline void set_retry_times(int64_t retry_times)
  {
    retry_times_ = retry_times;
  }
  inline int64_t get_retry_times() const
  {
    return retry_times_;
  }
  // for upgrade compatibility.
  inline bool min_cluster_version_is_valid() const
  {
    return ObExecutorRpcCtx::INVALID_CLUSTER_VERSION != min_cluster_version_;
  }
  inline void set_min_cluster_version(uint64_t min_cluster_version)
  {
    min_cluster_version_ = min_cluster_version;
  }
  inline uint64_t get_min_cluster_version() const
  {
    return min_cluster_version_;
  }

  void set_sys_job_id(const int64_t id)
  {
    sys_job_id_ = id;
  }
  int64_t get_sys_job_id() const
  {
    return sys_job_id_;
  }

  ObExecContext* get_exec_context() const
  {
    return exec_ctx_;
  }

  void set_expected_worker_cnt(int64_t cnt)
  {
    expected_worker_cnt_ = cnt;
  }
  int64_t get_expected_worker_cnt() const
  {
    return expected_worker_cnt_;
  }
  void set_allocated_worker_cnt(int64_t cnt)
  {
    allocated_worker_cnt_ = cnt;
  }  // alias
  int64_t get_allocated_worker_cnt() const
  {
    return allocated_worker_cnt_;
  }  // alias
  int append_table_locations_no_dup(const ObTablePartitionInfoArray& table_partition_infos);

private:
  int is_valid_addr(uint64_t ref_table_id, const common::ObAddr& addr, bool& is_valid) const;

private:
  // BEGIN local variables
  RemoteExecuteStreamHandle* task_resp_handler_;
  ObExecuteResult execute_result_;
  common::ObList<common::ObAddr, common::ObIAllocator> virtual_part_servers_;
  CalcVirtualPartitionIdParams calc_params_;
  common::ObSEArray<ObTaskInfo::ObRangeLocation*, 1> last_failed_partitions_;
  ObExecContext* exec_ctx_;
  common::ObFixedArray<ObTablePartitionInfo*, common::ObIAllocator> partition_infos_;
  bool need_renew_location_cache_;
  common::ObList<common::ObPartitionKey, common::ObIAllocator> need_renew_partition_keys_;
  int64_t expected_worker_cnt_;
  int64_t allocated_worker_cnt_;
  // END local variables

  // BEGIN variables need serialization
  ObPhyTableLocationFixedArray table_locations_;
  int64_t retry_times_;
  uint64_t min_cluster_version_;
  //  END variables need serialization

  int64_t sys_job_id_;

public:
  // BEGIN global singleton
  share::ObIPartitionLocationCache* partition_location_cache_;
  ObExecutorRpcImpl* task_executor_rpc_;
  obrpc::ObCommonRpcProxy rs_rpc_proxy_;
  obrpc::ObSrvRpcProxy* srv_rpc_proxy_;
  int64_t query_tenant_begin_schema_version_;
  int64_t query_sys_begin_schema_version_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  storage::ObPartitionService* partition_service_;
  common::ObIDataAccessService* vt_partition_service_;
  // END global singleton

  DISALLOW_COPY_AND_ASSIGN(ObTaskExecutorCtx);
};

class ObExecutorRpcImpl;
class ObTaskExecutorCtxUtil {
public:
  static int get_stream_handler(ObExecContext& ctx, RemoteExecuteStreamHandle*& handler);
  static int get_task_executor_rpc(ObExecContext& ctx, ObExecutorRpcImpl*& rpc);
  static int get_phy_table_location(ObExecContext& ctx, uint64_t table_location_key, uint64_t ref_table_id,
      const ObPhyTableLocation*& table_location);
  static int get_phy_table_location_for_update(
      ObExecContext& ctx, uint64_t table_location_key, uint64_t ref_table_id, ObPhyTableLocation*& table_location);
  static int get_phy_table_location(ObTaskExecutorCtx& ctx, uint64_t table_location_key, uint64_t ref_table_id,
      const ObPhyTableLocation*& table_location);
  static int get_phy_table_location_for_update(
      ObTaskExecutorCtx& ctx, uint64_t table_location_key, uint64_t ref_table_id, ObPhyTableLocation*& table_location);
  static ObPhyTableLocation* get_phy_table_location_for_update(
      ObTaskExecutorCtx& ctx, uint64_t table_location_key, uint64_t ref_table_id);
  static int get_full_table_phy_table_location(ObExecContext& ctx, uint64_t table_location_key, uint64_t ref_table_id,
      bool is_weak, const ObPhyTableLocation*& table_location);

  static int extract_server_participants(
      ObExecContext& ctx, const common::ObAddr& svr, common::ObPartitionIArray& participants);
  static int filter_invalid_locs(share::ObIPartitionLocationCache& part_loc_cache,
      const ObPhyTableLocation& in_table_loc, ObPhyTableLocation& out_table_loc);
  static int get_part_runner_server(
      ObExecContext& ctx, uint64_t table_id, uint64_t index_tid, int64_t part_id, common::ObAddr& runner_server);
  static int refresh_location_cache(ObTaskExecutorCtx& task_exec_ctx, bool is_nonblock);
  static int get_participants(ObExecContext& ctx, const ObTask& task, common::ObPartitionLeaderArray& pla);
  template <typename DEST_TYPE, typename SRC_TYPE>
  static int merge_task_result_meta(DEST_TYPE& dest, const SRC_TYPE& task_meta);
  static int translate_pid_to_ldx(ObTaskExecutorCtx& ctx, int64_t partition_id, int64_t table_location_key,
      int64_t ref_table_id, int64_t& location_idx);
  static int try_nonblock_refresh_location_cache(
      ObTaskExecutorCtx* task_execute_ctx, ObPartitionIArray& partition_keys, const int64_t expire_renew_time = 0);
}; /* class ObTaskExecutorCtxUtil */

template <typename DEST_TYPE, typename SRC_TYPE>
int ObTaskExecutorCtxUtil::merge_task_result_meta(DEST_TYPE& dest, const SRC_TYPE& task_meta)
{
  int ret = common::OB_SUCCESS;
  dest.set_affected_rows(dest.get_affected_rows() + task_meta.get_affected_rows());
  dest.set_found_rows(dest.get_found_rows() + task_meta.get_found_rows());
  dest.set_row_matched_count(dest.get_row_matched_count() + task_meta.get_row_matched_count());
  dest.set_row_duplicated_count(dest.get_row_duplicated_count() + task_meta.get_row_duplicated_count());
  dest.set_last_insert_id_session(task_meta.get_last_insert_id_session());
  dest.set_last_insert_id_changed(task_meta.get_last_insert_id_changed());
  if (!task_meta.is_result_accurate()) {
    dest.set_is_result_accurate(task_meta.is_result_accurate());
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_TASK_EXECUTOR_CTX_ */
//// end of header file
