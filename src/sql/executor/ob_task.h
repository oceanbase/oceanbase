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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_TASK_
#define OCEANBASE_SQL_EXECUTOR_OB_TASK_

#include "share/ob_define.h"
#include "sql/executor/ob_job.h"
#include "lib/ob_name_id_def.h"
#include "share/rpc/ob_batch_proxy.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObPhysicalPlan;
class ObJob;
class ObTaskInfo;
class ObOpSpec;
/*
 * 将一个任务序列化到远端执行，需要指明如下内容：
 * 1. Operator Tree
 * 2. Operator Input Array
 * 3. Query ID, Job ID, Task ID
 */
class ObTask
{
  OB_UNIS_VERSION_V(1);
public:
  ObTask();
  virtual ~ObTask();
  void set_ob_task_id(const ObTaskID &ob_task_id) { ob_task_id_ = ob_task_id; }
  const ObTaskID &get_ob_task_id() const { return ob_task_id_; }
  void set_location_idx(int64_t location_idx) { location_idx_ = location_idx; }
  int64_t get_location_idx() const { return location_idx_; }
  void set_runner_server(const common::ObAddr &svr) { runner_svr_ = svr; }
  const common::ObAddr &get_runner_server() const { return runner_svr_; }
  void set_ctrl_server(const common::ObAddr &svr) { ctrl_svr_ = svr; }
  const common::ObAddr &get_ctrl_server() const { return ctrl_svr_; }
  void set_job(ObJob &job) { job_ = &job; }
  void set_serialize_param(ObExecContext *exec_ctx,
                           ObOpSpec *op_root,
                           const ObPhysicalPlan *ser_phy_plan);
  void set_deserialize_param(ObExecContext &exec_ctx, ObPhysicalPlan &des_phy_plan);
  ObOpSpec *get_root_spec() const { return root_spec_; }
  int load_code();
  const ObPhysicalPlan& get_des_phy_plan() const { return *des_phy_plan_; }
  const ObPhysicalPlan& get_ser_phy_plan() const { return *ser_phy_plan_; }
  ObExecContext *get_exec_context() const { return exec_ctx_; }
  inline void set_exec_context(ObExecContext *exec_ctx) { exec_ctx_ = exec_ctx; }
  inline void set_ser_phy_plan(const ObPhysicalPlan *ser_phy_plan) { ser_phy_plan_ = ser_phy_plan; }


  int add_range(const common::ObNewRange &range)
  {
    return ranges_.push_back(range);
  }

  void set_max_sql_no(int64_t max_sql_no) { max_sql_no_ = max_sql_no; }
  const common::ObIArray<ObNewRange> &get_ranges() const { return ranges_; }
  int assign_ranges(const ObIArray<ObNewRange> &ranges);
  TO_STRING_KV(N_OB_TASK_ID, ob_task_id_,
               K_(runner_svr),
               K_(ctrl_svr),
               K_(ranges),
               K_(location_idx));
  DECLARE_TO_YSON_KV;
protected:
  //TODO：晓楚
  //ObTask发到远端后，远端需要如下信息：
  //1. task location info
  //2. 事务控制
  //3. 扫描范围
  //4. ?
  ObExecContext *exec_ctx_;
  const ObPhysicalPlan *ser_phy_plan_;
  ObPhysicalPlan *des_phy_plan_;
  ObOpSpec *root_spec_;
  ObJob *job_;
  // ObTask要发往的目标机器
  common::ObAddr runner_svr_;
  // 主控机地址，用于远端执行后汇报状态给主控机
  common::ObAddr ctrl_svr_;
  // 本Task的寻址信息
  ObTaskID ob_task_id_;
  int64_t location_idx_;
  // 本Task涉及到的扫描范围，默认涉及的一张表（一个或者多个partition）
  common::ObSEArray<ObNewRange, 32> ranges_;
  int64_t max_sql_no_;
  //DISALLOW_COPY_AND_ASSIGN(ObTask);
};

inline void ObTask::set_serialize_param(ObExecContext *exec_ctx,
                                        ObOpSpec *root_spec,
                                        const ObPhysicalPlan *ser_phy_plan)
{
  exec_ctx_ = exec_ctx;
  root_spec_ = root_spec;
  ser_phy_plan_ = ser_phy_plan;
}

inline void ObTask::set_deserialize_param(ObExecContext &exec_ctx, ObPhysicalPlan &des_phy_plan)
{
  exec_ctx_ = &exec_ctx;
  des_phy_plan_ = &des_phy_plan;
}

class ObMiniTask : public ObTask
{
  OB_UNIS_VERSION_V(1);
public:
  ObMiniTask()
    : ObTask(),
      extend_root_spec_(NULL)
  {}
  ~ObMiniTask() {}

  inline void set_extend_root_spec(ObOpSpec *spec) { extend_root_spec_ = spec; }
  inline const ObOpSpec *get_extend_root_spec() const { return extend_root_spec_; }

  TO_STRING_KV(K_(ob_task_id),
               K_(runner_svr),
               K_(ctrl_svr),
               K_(ranges),
               K_(location_idx),
               KP_(extend_root_spec));
private:
  //对于mini task,允许在执行主计划的时候，附带执行一个扩展计划，
  //这个主要是用于当冲突检查的时候，获取unique index的冲突行主键的时候，
  //可以将主键和local unique index的冲突rowkey的其它列信息也一并带回来，优化RPC次数
  ObOpSpec *extend_root_spec_;
};

class ObPingSqlTask
{
  OB_UNIS_VERSION_V(1);
public:
  ObPingSqlTask();
  virtual ~ObPingSqlTask();
  TO_STRING_KV(K(trans_id_),
               K(sql_no_),
               K(task_id_),
               K(exec_svr_),
               K(cur_status_));
public:
  transaction::ObTransID trans_id_;
  uint64_t sql_no_;
  ObTaskID task_id_;
  common::ObAddr exec_svr_;
  int64_t cur_status_;
};

class ObPingSqlTaskResult
{
  OB_UNIS_VERSION_V(1);
public:
  ObPingSqlTaskResult();
  virtual ~ObPingSqlTaskResult();
  TO_STRING_KV(K(err_code_),
               K(ret_status_));
public:
  int err_code_;
  int64_t ret_status_;
};

class ObDesExecContext;
class ObRemoteTask : public obrpc::ObIFill
{
  OB_UNIS_VERSION(1);
public:
  ObRemoteTask() :
    tenant_schema_version_(-1),
    sys_schema_version_(-1),
    runner_svr_(),
    ctrl_svr_(),
    task_id_(),
    remote_sql_info_(nullptr),
    session_info_(NULL),
    exec_ctx_(NULL),
    inner_alloc_("RemoteTask"),
    dependency_tables_(&inner_alloc_),
    snapshot_()
  {
  }
  ~ObRemoteTask() = default;

  void set_query_schema_version(int64_t tenant_schema_version, int64_t sys_schema_version)
  {
    tenant_schema_version_ = tenant_schema_version;
    sys_schema_version_ = sys_schema_version;
  }
  int64_t get_tenant_schema_version() const { return tenant_schema_version_; }
  int64_t get_sys_schema_version() const { return sys_schema_version_; }
  void set_session(ObSQLSessionInfo *session_info) { session_info_ = session_info; }
  void set_runner_svr(const common::ObAddr &runner_svr) { runner_svr_ = runner_svr; }
  const common::ObAddr &get_runner_svr() const { return runner_svr_; }
  void set_ctrl_server(const common::ObAddr &ctrl_svr) { ctrl_svr_ = ctrl_svr; }
  const common::ObAddr &get_ctrl_server() const { return ctrl_svr_; }
  void set_task_id(const ObTaskID &task_id) { task_id_ = task_id; }
  const ObTaskID &get_task_id() const { return task_id_; }
  void set_remote_sql_info(ObRemoteSqlInfo *remote_sql_info) { remote_sql_info_ = remote_sql_info; }
  const ObRemoteSqlInfo *get_remote_sql_info() const { return remote_sql_info_; }
  void set_exec_ctx(ObDesExecContext *exec_ctx) { exec_ctx_ = exec_ctx; }
  int assign_dependency_tables(const DependenyTableStore &dependency_tables);
  const DependenyTableStore &get_dependency_tables() const {
    return dependency_tables_;
  }
  void set_snapshot(const transaction::ObTxReadSnapshot &snapshot) { snapshot_ = snapshot; }
  const transaction::ObTxReadSnapshot &get_snapshot() const { return snapshot_; }
  int fill_buffer(char* buf, int64_t size, int64_t &filled_size) const
  {
    filled_size = 0;
    return serialize(buf, size, filled_size);
  }
  int64_t get_req_size() const { return get_serialize_size(); }
  TO_STRING_KV(K_(tenant_schema_version),
               K_(sys_schema_version),
               K_(runner_svr),
               K_(ctrl_svr),
               K_(task_id),
               KPC_(remote_sql_info),
               K_(snapshot));
  DECLARE_TO_YSON_KV;
private:
  int64_t tenant_schema_version_;
  int64_t sys_schema_version_;
  // ObTask要发往的目标机器
  common::ObAddr runner_svr_;
  // 主控机地址，用于远端执行后汇报状态给主控机
  common::ObAddr ctrl_svr_;
  // 本Task的寻址信息
  ObTaskID task_id_;
  //远程执行对应的模板化SQL文本
  ObRemoteSqlInfo *remote_sql_info_;
  //执行涉及到的session info，主要是session中的事务状态信息
  ObSQLSessionInfo *session_info_;
  ObDesExecContext *exec_ctx_;
  common::ModulePageAllocator inner_alloc_;
  DependenyTableStore dependency_tables_;
  transaction::ObTxReadSnapshot snapshot_;
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_TASK_ */
//// end of header file
