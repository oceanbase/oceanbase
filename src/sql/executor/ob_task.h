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
#include "common/ob_partition_key.h"
#include "share/rpc/ob_batch_proxy.h"

namespace oceanbase {
namespace sql {
class ObExecContext;
class ObPhysicalPlan;
class ObPhyOperator;
class ObJob;
class ObTaskInfo;
class ObOpSpec;
/*
 * To serialize a task to remote execution, you need to specify the following:
 * 1. Operator Tree
 * 2. Operator Input Array
 * 3. Query ID, Job ID, Task ID
 */
class ObTask {
  OB_UNIS_VERSION_V(1);

public:
  ObTask();
  virtual ~ObTask();
  void set_ob_task_id(const ObTaskID& ob_task_id)
  {
    ob_task_id_ = ob_task_id;
  }
  const ObTaskID& get_ob_task_id() const
  {
    return ob_task_id_;
  }
  void set_location_idx(int64_t location_idx)
  {
    location_idx_ = location_idx;
  }
  int64_t get_location_idx() const
  {
    return location_idx_;
  }
  void set_runner_server(const common::ObAddr& svr)
  {
    runner_svr_ = svr;
  }
  const common::ObAddr& get_runner_server() const
  {
    return runner_svr_;
  }
  void set_ctrl_server(const common::ObAddr& svr)
  {
    ctrl_svr_ = svr;
  }
  const common::ObAddr& get_ctrl_server() const
  {
    return ctrl_svr_;
  }
  void set_job(ObJob& job)
  {
    job_ = &job;
  }
  void set_serialize_param(ObExecContext& exec_ctx, ObPhyOperator& op_root, const ObPhysicalPlan& ser_phy_plan);
  void set_serialize_param(ObExecContext* exec_ctx, ObOpSpec* op_root, const ObPhysicalPlan* ser_phy_plan);
  void set_deserialize_param(ObExecContext& exec_ctx, ObPhysicalPlan& des_phy_plan);
  ObPhyOperator* get_root_op() const
  {
    return op_root_;
  }
  ObOpSpec* get_root_spec() const
  {
    return root_spec_;
  }
  const ObPhysicalPlan& get_des_phy_plan() const
  {
    return *des_phy_plan_;
  }
  const ObPhysicalPlan& get_ser_phy_plan() const
  {
    return *ser_phy_plan_;
  }
  ObExecContext* get_exec_context() const
  {
    return exec_ctx_;
  }
  inline void set_exec_context(ObExecContext* exec_ctx)
  {
    exec_ctx_ = exec_ctx;
  }
  inline void set_root_op(ObPhyOperator* root_op)
  {
    op_root_ = root_op;
  }
  inline void set_ser_phy_plan(const ObPhysicalPlan* ser_phy_plan)
  {
    ser_phy_plan_ = ser_phy_plan;
  }
  int add_partition_key(const common::ObPartitionKey& pkey)
  {
    return partition_keys_.push_back(pkey);
  }

  int add_range(const common::ObNewRange& range)
  {
    return ranges_.push_back(range);
  }

  void set_max_sql_no(int64_t max_sql_no)
  {
    max_sql_no_ = max_sql_no;
  }
  // const common::ObPartitionIArray &get_partition_keys() const { return partition_keys_; }
  const common::ObIArray<ObNewRange>& get_ranges() const
  {
    return ranges_;
  }
  int assign_ranges(const ObIArray<ObNewRange>& ranges);
  const common::ObPartitionArray& get_partition_keys() const
  {
    return partition_keys_;
  }
  static int build_cte_op_pair(ObPhyOperator* root, common::ObIArray<ObPhyOperator*>& cte_pumps);
  TO_STRING_KV(
      N_OB_TASK_ID, ob_task_id_, K_(runner_svr), K_(ctrl_svr), K_(partition_keys), K_(ranges), K_(location_idx));
  DECLARE_TO_YSON_KV;

protected:
  int serialize_tree(char* buf, int64_t buf_len, int64_t& pos, const ObPhyOperator& root) const;
  int deserialize_tree(const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan, ObPhyOperator*& root);
  int64_t get_tree_serialize_size(const ObPhyOperator& root) const;

protected:
  ObExecContext* exec_ctx_;
  const ObPhysicalPlan* ser_phy_plan_;
  ObPhysicalPlan* des_phy_plan_;
  ObPhyOperator* op_root_;
  ObOpSpec* root_spec_;
  ObJob* job_;
  // the target machine to which ObTask will be sent
  common::ObAddr runner_svr_;
  // main control machine address, used to report status to the main control machine after remote execution
  common::ObAddr ctrl_svr_;
  ObTaskID ob_task_id_;
  int64_t location_idx_;
  common::ObPartitionArray partition_keys_;
  common::ObSEArray<ObNewRange, 32> ranges_;
  int64_t max_sql_no_;
  // DISALLOW_COPY_AND_ASSIGN(ObTask);
};

inline void ObTask::set_serialize_param(
    ObExecContext& exec_ctx, ObPhyOperator& op_root, const ObPhysicalPlan& ser_phy_plan)
{
  exec_ctx_ = &exec_ctx;
  op_root_ = &op_root;
  root_spec_ = NULL;
  ser_phy_plan_ = &ser_phy_plan;
}

inline void ObTask::set_serialize_param(
    ObExecContext* exec_ctx, ObOpSpec* root_spec, const ObPhysicalPlan* ser_phy_plan)
{
  exec_ctx_ = exec_ctx;
  op_root_ = NULL;
  root_spec_ = root_spec;
  ser_phy_plan_ = ser_phy_plan;
}

inline void ObTask::set_deserialize_param(ObExecContext& exec_ctx, ObPhysicalPlan& des_phy_plan)
{
  exec_ctx_ = &exec_ctx;
  des_phy_plan_ = &des_phy_plan;
}

class ObMiniTask : public ObTask {
  OB_UNIS_VERSION_V(1);

public:
  ObMiniTask() : ObTask(), extend_root_(NULL), extend_root_spec_(NULL)
  {}
  ~ObMiniTask()
  {}

  inline void set_extend_root(ObPhyOperator* extend_root)
  {
    extend_root_ = extend_root;
  }
  inline const ObPhyOperator* get_extend_root() const
  {
    return extend_root_;
  }

  inline void set_extend_root_spec(ObOpSpec* spec)
  {
    extend_root_spec_ = spec;
  }
  inline const ObOpSpec* get_extend_root_spec() const
  {
    return extend_root_spec_;
  }

  TO_STRING_KV(K_(ob_task_id), K_(runner_svr), K_(ctrl_svr), K_(partition_keys), K_(ranges), K_(location_idx),
      KPC_(extend_root), KP_(extend_root_spec));

private:
  // For mini tasks, it is allowed to execute an extended plan when the main plan is executed,
  // this is mainly used to obtain the primary key of the conflict row of the unique index when
  // the conflict is checked. you can also bring back other column information of the conflicting
  // rowkey between the primary key and the local unique index to optimize the number of RPC
  ObPhyOperator* extend_root_;
  ObOpSpec* extend_root_spec_;
};

class ObPingSqlTask {
  OB_UNIS_VERSION_V(1);

public:
  ObPingSqlTask();
  virtual ~ObPingSqlTask();
  TO_STRING_KV(K(trans_id_), K(sql_no_), K(task_id_), K(exec_svr_), K(part_keys_), K(cur_status_));

public:
  transaction::ObTransID trans_id_;
  uint64_t sql_no_;
  ObTaskID task_id_;
  common::ObAddr exec_svr_;
  common::ObPartitionArray part_keys_;
  int64_t cur_status_;
};

class ObPingSqlTaskResult {
  OB_UNIS_VERSION_V(1);

public:
  ObPingSqlTaskResult();
  virtual ~ObPingSqlTaskResult();
  TO_STRING_KV(K(err_code_), K(ret_status_));

public:
  int err_code_;
  int64_t ret_status_;
};

class ObDesExecContext;
class ObRemoteTask : public obrpc::ObIFill {
  OB_UNIS_VERSION(1);

public:
  ObRemoteTask()
      : tenant_schema_version_(-1),
        sys_schema_version_(-1),
        runner_svr_(),
        ctrl_svr_(),
        task_id_(),
        remote_sql_info_(nullptr),
        session_info_(NULL),
        exec_ctx_(NULL),
        inner_alloc_("RemoteTask"),
        dependency_tables_(&inner_alloc_)
  {}
  ~ObRemoteTask() = default;

  void set_query_schema_version(int64_t tenant_schema_version, int64_t sys_schema_version)
  {
    tenant_schema_version_ = tenant_schema_version;
    sys_schema_version_ = sys_schema_version;
  }
  int64_t get_tenant_schema_version() const
  {
    return tenant_schema_version_;
  }
  int64_t get_sys_schema_version() const
  {
    return sys_schema_version_;
  }
  void set_session(ObSQLSessionInfo* session_info)
  {
    session_info_ = session_info;
  }
  void set_runner_svr(const common::ObAddr& runner_svr)
  {
    runner_svr_ = runner_svr;
  }
  const common::ObAddr& get_runner_svr() const
  {
    return runner_svr_;
  }
  void set_ctrl_server(const common::ObAddr& ctrl_svr)
  {
    ctrl_svr_ = ctrl_svr;
  }
  const common::ObAddr& get_ctrl_server() const
  {
    return ctrl_svr_;
  }
  void set_task_id(const ObTaskID& task_id)
  {
    task_id_ = task_id;
  }
  const ObTaskID& get_task_id() const
  {
    return task_id_;
  }
  void set_remote_sql_info(ObRemoteSqlInfo* remote_sql_info)
  {
    remote_sql_info_ = remote_sql_info;
  }
  const ObRemoteSqlInfo* get_remote_sql_info() const
  {
    return remote_sql_info_;
  }
  void set_exec_ctx(ObDesExecContext* exec_ctx)
  {
    exec_ctx_ = exec_ctx;
  }
  int assign_dependency_tables(const DependenyTableStore& dependency_tables);
  const DependenyTableStore& get_dependency_tables() const
  {
    return dependency_tables_;
  }
  int fill_buffer(char* buf, int64_t size, int64_t& filled_size) const
  {
    filled_size = 0;
    return serialize(buf, size, filled_size);
  }
  int64_t get_req_size() const
  {
    return get_serialize_size();
  }
  TO_STRING_KV(K_(tenant_schema_version), K_(sys_schema_version), K_(runner_svr), K_(ctrl_svr), K_(task_id),
      KPC_(remote_sql_info));
  DECLARE_TO_YSON_KV;

private:
  int64_t tenant_schema_version_;
  int64_t sys_schema_version_;
  common::ObAddr runner_svr_;
  common::ObAddr ctrl_svr_;
  ObTaskID task_id_;
  ObRemoteSqlInfo* remote_sql_info_;
  ObSQLSessionInfo* session_info_;
  ObDesExecContext* exec_ctx_;
  common::ModulePageAllocator inner_alloc_;
  DependenyTableStore dependency_tables_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_OB_TASK_ */
//// end of header file
