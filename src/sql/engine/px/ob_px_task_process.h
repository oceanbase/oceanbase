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

#ifndef __OB_SQL_ENGINE_PX_TASK_PROCESS_H__
#define __OB_SQL_ENGINE_PX_TASK_PROCESS_H__

#include "share/schema/ob_schema_getter_guard.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/ob_granule_iterator.h"
#include "observer/virtual_table/ob_virtual_table_iterator_factory.h"
#include "sql/ob_sql_partition_location_cache.h"

namespace oceanbase {
namespace sql {
class ObPxTaskProcess {
private:
  class OpPreparation : public ObPxOperatorVisitor::ApplyFunc {
  public:
    OpPreparation()
        : task_id_(common::OB_INVALID_ID),
          sqc_id_(common::OB_INVALID_ID),
          dfo_id_(common::OB_INVALID_ID),
          pw_gi_(nullptr),
          pw_gi_spec_(nullptr),
          on_set_tscs_(false),
          tsc_ops_(),
          dml_op_(NULL),
          dml_spec_(NULL)
    {}
    ~OpPreparation() = default;
    virtual int apply(ObExecContext& ctx, ObPhyOperator& op);
    virtual int reset(ObPhyOperator& op);
    virtual int apply(ObExecContext& ctx, ObOpSpec& op);
    virtual int reset(ObOpSpec& op);
    void set_task_id(int64_t task_id)
    {
      task_id_ = task_id;
    }
    void set_sqc_id(int64_t sqc_id)
    {
      sqc_id_ = sqc_id;
    }
    void set_dfo_id(int64_t dfo_id)
    {
      dfo_id_ = dfo_id;
    }
    void set_pwj_gi(ObGranuleIterator* gi)
    {
      pw_gi_ = gi;
      on_set_tscs_ = true;
    }
    void set_pwj_gi_spec(ObGranuleIteratorSpec* gi)
    {
      pw_gi_spec_ = gi;
      on_set_tscs_ = true;
    }
    bool to_set_tsc()
    {
      return on_set_tscs_;
    }
    void set_exec_ctx(ObExecContext* ctx)
    {
      ctx_ = ctx;
    }

  private:
    int64_t task_id_;
    int64_t sqc_id_;
    int64_t dfo_id_;
    ObGranuleIterator* pw_gi_;
    ObGranuleIteratorSpec* pw_gi_spec_;
    bool on_set_tscs_;
    common::ObSEArray<const ObTableScan*, 32> tsc_ops_;
    common::ObSEArray<const ObTableScanSpec*, 32> tsc_op_specs_;
    ObTableModify* dml_op_;
    ObTableModifySpec* dml_spec_;
    ObExecContext* ctx_;
  };
  class OpPreCloseProcessor : public ObPxOperatorVisitor::ApplyFunc {
  public:
    OpPreCloseProcessor(ObPxTask& task) : task_(task)
    {}
    ~OpPreCloseProcessor() = default;

    virtual int apply(ObExecContext& ctx, ObPhyOperator& op);
    virtual int reset(ObPhyOperator& op);
    virtual int apply(ObExecContext& ctx, ObOpSpec& op);
    virtual int reset(ObOpSpec& op);

  private:
    ObPxTask& task_;
  };

public:
  ObPxTaskProcess(const observer::ObGlobalContext& gctx, ObPxRpcInitTaskArgs& arg);
  virtual ~ObPxTaskProcess();
  int process();
  void run();
  // for corotine RunFuncT
  void operator()(void);

  void set_is_oracle_mode(bool oracle_mode)
  {
    is_oracle_mode_ = oracle_mode;
  }
  bool is_oracle_mode() const
  {
    return is_oracle_mode_;
  }
  ObPxSqcHandler* get_sqc_handler()
  {
    return arg_.sqc_handler_;
  }

public:
  // for sql audit to monitor worker exec time
  void set_enqueue_timestamp(int64_t v)
  {
    enqueue_timestamp_ = v;
  }
  int64_t get_enqueue_timestamp() const
  {
    return enqueue_timestamp_;
  }
  int64_t get_process_timestamp() const
  {
    return process_timestamp_;
  }
  int64_t get_exec_start_timestamp() const
  {
    return exec_start_timestamp_;
  }
  int64_t get_exec_end_timestamp() const
  {
    return exec_end_timestamp_;
  }
  // simulated interface for thread pool (RPC)
  int64_t get_send_timestamp() const
  {
    return get_enqueue_timestamp();
  }
  int64_t get_receive_timestamp() const
  {
    return get_enqueue_timestamp();
  }
  int64_t get_run_timestamp() const
  {
    return get_process_timestamp();
  }
  int64_t get_single_process_timestamp() const
  {
    return get_process_timestamp();
  }
  uint64_t get_qc_id() const
  {
    return arg_.task_.get_qc_id();
  }
  int64_t get_sqc_id() const
  {
    return arg_.task_.get_sqc_id();
  }
  int64_t get_worker_id() const
  {
    return arg_.task_.get_task_id();
  }
  int64_t get_dfo_id() const
  {
    return arg_.task_.get_dfo_id();
  }
  ObPxInterruptID get_interrupt_id()
  {
    return arg_.task_.get_interrupt_id();
  }
  uint64_t get_session_id() const;
  uint64_t get_tenant_id() const;

  int execute(ObPhyOperator& root);
  int execute(ObOpSpec& root);

private:
  /* functions */
  int do_process();
  int check_inner_stat();
  virtual void record_exec_timestamp(bool is_first, ObExecTimestamp& exec_timestamp)
  {
    ObExecStatUtils::record_exec_timestamp(*this, is_first, exec_timestamp);
  }
  /* remember to call this function at the end of process() */
  void release();
  /* variables */
  const observer::ObGlobalContext& gctx_;
  ObPxRpcInitTaskArgs& arg_;
  sql::ObSqlCtx sql_ctx_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  observer::ObVirtualTableIteratorFactory vt_iter_factory_;

  /* timestamps for sql audit */
  int64_t enqueue_timestamp_;
  int64_t process_timestamp_;
  int64_t exec_start_timestamp_;
  int64_t exec_end_timestamp_;

  /* record oracle mode */
  bool is_oracle_mode_;
  /* partition cache for global index lookup*/
  ObSqlPartitionLocationCache partition_location_cache_;
  DISALLOW_COPY_AND_ASSIGN(ObPxTaskProcess);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_ENGINE_PX_TASK_PROCESS_H__ */
//// end of header file
