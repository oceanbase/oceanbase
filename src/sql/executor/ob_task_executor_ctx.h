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

#include "share/ob_autoincrement_service.h"
#include "share/ob_common_rpc_proxy.h"
#include "sql/executor/ob_executor_rpc_impl.h"
#include "sql/executor/ob_execute_result.h"
#include "sql/ob_phy_table_location.h"
#include "sql/optimizer/ob_table_partition_info.h"
#include "sql/ob_sql_context.h"
#include "lib/worker.h"
#include "lib/list/ob_list.h"
#include "sql/das/ob_das_ref.h"
namespace oceanbase
{
namespace common
{
class ObAddr;
class ObITabletScan;
}

namespace obrpc
{
class ObSrvRpcProxy;
class ObCommonRpcProxy;
}

namespace sql
{

typedef common::ObIArray<ObPhyTableLocation> ObPhyTableLocationIArray;
typedef common::ObIArray<ObCandiTableLoc> ObCandiTableLocIArray;
typedef common::ObSEArray<ObPhyTableLocation, 2> ObPhyTableLocationFixedArray;

class ObJobControl;
class ObExecContext;
class ObTaskExecutorCtx
{
  OB_UNIS_VERSION(1);
public:
  class CalcVirtualPartitionIdParams
  {
  public:
    CalcVirtualPartitionIdParams() : inited_(false), ref_table_id_(common::OB_INVALID_ID) {}
    ~CalcVirtualPartitionIdParams() {}

    int init(uint64_t ref_table_id);
    inline void reset() { inited_ = false; ref_table_id_ = common::OB_INVALID_ID; }
    inline bool is_inited() const { return inited_; }
    inline uint64_t get_ref_table_id() const { return ref_table_id_; }

    TO_STRING_KV(K_(inited), K_(ref_table_id));
  private:
    bool inited_;
    uint64_t ref_table_id_;
  };

  explicit ObTaskExecutorCtx(ObExecContext &exec_context);
  virtual ~ObTaskExecutorCtx();

  int get_addr_by_virtual_partition_id(int64_t partition_id, common::ObAddr &addr);
  int set_table_locations(const ObTablePartitionInfoArray &table_partition_infos);
  int append_table_location(const ObCandiTableLoc &phy_location_info);

  const ObTablePartitionInfoArray &get_partition_infos() const;
  inline RemoteExecuteStreamHandle* get_stream_handler()
  {
    return task_resp_handler_;
  }
  int reset_and_init_stream_handler();
  ObExecutorRpcImpl *get_task_executor_rpc()
  {
    return GCTX.executor_rpc_;
  }
  // @nijia.nj FIXme: this func should be replaced by
  // int get_common_rpc(obrpc::ObCommonRpcProxy *&)
  obrpc::ObCommonRpcProxy *get_common_rpc();

  int get_common_rpc(obrpc::ObCommonRpcProxy *&common_rpc_proxy);
  inline ObExecuteResult &get_execute_result()
  {
    return execute_result_;
  }
  inline common::ObITabletScan *get_vt_partition_service()
  {
    return GCTX.vt_par_ser_;
  }
  inline obrpc::ObSrvRpcProxy *get_srv_rpc()
  {
    return GCTX.srv_rpc_proxy_;
  }
  inline void set_query_tenant_begin_schema_version(const int64_t schema_version)
  {
    query_tenant_begin_schema_version_ = schema_version;
  }
  void set_self_addr(const common::ObAddr &self_addr);
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
  // init_calc_virtual_part_id_params和reset_calc_virtual_part_id_params最好成对使用，
  // 否则calc_virtual_partition_id函数容易出错；
  // 涉及到addr_to_part_id函数的calc函数运行的时候或者calc_virtual_partition_id函数运行的时候
  // 才需要用到init_calc_virtual_part_id_params和reset_calc_virtual_part_id_params
  inline int init_calc_virtual_part_id_params(uint64_t ref_table_id)
  {
    return calc_params_.init(ref_table_id);
  }
  inline void reset_calc_virtual_part_id_params()
  {
    calc_params_.reset();
  }
  inline const CalcVirtualPartitionIdParams &get_calc_virtual_part_id_params() const
  {
    return calc_params_;
  }
  inline void set_retry_times(int64_t retry_times)
  {
    retry_times_ = retry_times;
  }
  inline int64_t get_retry_times() const
  {
    return retry_times_;
  }
  //FIXME qianfu 兼容性代码，1.4.0之后去掉这个函数
  // 等于INVALID_CLUSTER_VERSION说明是从远端的旧observer上序列化过来的
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

  void set_sys_job_id(const int64_t id) { sys_job_id_ = id; }
  int64_t get_sys_job_id() const { return sys_job_id_; }

  ObExecContext *get_exec_context() const { return exec_ctx_; }

  void set_expected_worker_cnt(int64_t cnt) { expected_worker_cnt_ = cnt; }
  int64_t get_expected_worker_cnt() const { return expected_worker_cnt_; }
  void set_minimal_worker_cnt(int64_t cnt) { minimal_worker_cnt_ = cnt; }
  int64_t get_minimal_worker_cnt() const { return minimal_worker_cnt_; }
  void set_admited_worker_cnt(int64_t cnt) { admited_worker_cnt_ = cnt; } // alias
  int64_t get_admited_worker_cnt() const { return admited_worker_cnt_; } // alias
  // try to trigger a location update task and clear location in cache,
  // if it is limited by the limiter and not be done, is_limited will be set to true
  int nonblock_renew_with_limiter(const ObTabletID &tablet_id,
                                  const int64_t expire_renew_time,
                                  bool &is_limited);

private:
  // BEGIN 本地局部变量
  //
  // RPC提供的流式处理接口, LocalReceiveOp从这个接口读取远端数据
  // 之所以将这个变量放在ObTaskExecutorCtx中的原因是：task_resp_handler_
  // 必须在Scheduler中初始化，在LocalReceiveOp中使用，所以才利用
  // ObTaskExecutorCtx传递变量
  RemoteExecuteStreamHandle *task_resp_handler_;
  // 用于封装executor最顶层Job的Op Tree，对外吐数据
  ObExecuteResult execute_result_;
  // 用于记录虚拟表的partition_id和机器的(ip, port)的对应关系，
  // 该数组的下标即为该server对应的partition_id
  common::ObList<common::ObAddr, common::ObIAllocator> virtual_part_servers_;
  // 用于计算虚拟表的partition id的时候临时传递的参数，计算完最好reset掉该成员变量
  CalcVirtualPartitionIdParams calc_params_;
  //
  ObExecContext *exec_ctx_;
  // PX 记录执行预期整个 Query 需要的线程数，以及实际分配的线程数
  int64_t expected_worker_cnt_; // query expected worker count computed by optimizer
  int64_t minimal_worker_cnt_;  // minimal worker count to support execute this query
  int64_t admited_worker_cnt_; // query final used worker count admitted by admission
  // END 本地局部变量

  // BEGIN 需要序列化的变量
  // 这个信息序列化到所有参与了查询的机器上.
  // NOTE:中间计算的机器需要判断一下，不作为Participant
  ObPhyTableLocationFixedArray table_locations_;
  // 重试的次数
  int64_t retry_times_;
  //
  // 全局的observer最小版本号
  uint64_t min_cluster_version_;
  //
  //  END 需要序列化的变量

  int64_t sys_job_id_;
public:

  // BEGIN 全局单例变量
  //
  obrpc::ObCommonRpcProxy *rs_rpc_proxy_;
  int64_t query_tenant_begin_schema_version_; // Query开始时获取全局最新的 tenant schema version
  int64_t query_sys_begin_schema_version_; // Query开始时获取全局最新的 sys schema version
  share::schema::ObMultiVersionSchemaService *schema_service_;
  //
  // END 全局单例变量


  DISALLOW_COPY_AND_ASSIGN(ObTaskExecutorCtx);
  TO_STRING_KV(K(table_locations_), K(retry_times_), K(min_cluster_version_), K(expected_worker_cnt_),
      K(admited_worker_cnt_), K(query_tenant_begin_schema_version_), K(query_sys_begin_schema_version_),
      K(minimal_worker_cnt_));
};

class ObExecutorRpcImpl;
class ObTaskExecutorCtxUtil
{
public:
  // trigger a location update task and clear location in cache
  static int nonblock_renew(
                            ObExecContext *exec_ctx,
                            const ObTabletID &tablet_id,
                            const int64_t last_renew_time,
                            const int64_t cluster_id = common::OB_INVALID_ID);
  static int get_stream_handler(ObExecContext &ctx, RemoteExecuteStreamHandle *&handler);
  static int get_task_executor_rpc(ObExecContext &ctx, ObExecutorRpcImpl *&rpc);

  template<typename DEST_TYPE, typename SRC_TYPE>
  static int merge_task_result_meta(DEST_TYPE &dest, const SRC_TYPE &task_meta);
}; /* class ObTaskExecutorCtxUtil */

template<typename DEST_TYPE, typename SRC_TYPE>
int ObTaskExecutorCtxUtil::merge_task_result_meta(DEST_TYPE &dest, const SRC_TYPE &task_meta)
{
  int ret  = common::OB_SUCCESS;
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
}
}
#endif /* OCEANBASE_SQL_TASK_EXECUTOR_CTX_ */
//// end of header file
