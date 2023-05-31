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
#include "sql/engine/px/ob_granule_iterator_op.h"
#include "observer/virtual_table/ob_virtual_table_iterator_factory.h"

namespace oceanbase
{
namespace sql
{
class ObPxTaskProcess
{
private:
  class OpPreparation : public ObPxOperatorVisitor::ApplyFunc
  {
  public:
    OpPreparation() :
    task_id_(common::OB_INVALID_ID),
    sqc_id_(common::OB_INVALID_ID),
    dfo_id_(common::OB_INVALID_ID),
    pw_gi_spec_(nullptr),
    on_set_tscs_(false),
    dml_spec_(NULL),
    task_(NULL)
    {}
    ~OpPreparation() = default;
    virtual int apply(ObExecContext &ctx, ObOpSpec &op);
    virtual int reset(ObOpSpec &op);
    void set_task_id(int64_t task_id) { task_id_ = task_id; }
    void set_sqc_id(int64_t sqc_id) { sqc_id_ = sqc_id; }
    void set_dfo_id(int64_t dfo_id) { dfo_id_ = dfo_id; }
    void set_pwj_gi_spec(ObGranuleIteratorSpec *gi) { pw_gi_spec_ = gi; on_set_tscs_ = true; }
    bool to_set_tsc() { return on_set_tscs_; }
    void set_exec_ctx(ObExecContext *ctx) { ctx_ = ctx; }
    void set_px_task(ObPxTask *task) {task_ = task; }
  private:
    int64_t task_id_;
    int64_t sqc_id_;
    int64_t dfo_id_;
    ObGranuleIteratorSpec *pw_gi_spec_;
    // TODO: remove on_set_tscs_, tsc_op_specs_ and dml_spec_ on 4.2
    bool on_set_tscs_;
    common::ObSEArray<const ObTableScanSpec *, 32> tsc_op_specs_;
    ObTableModifySpec *dml_spec_;
    ObExecContext *ctx_;
    ObPxTask *task_;
  };

  class OpPostparation : public ObPxOperatorVisitor::ApplyFunc
  {
  public:
    OpPostparation(int ret): ret_(ret) {}

    virtual int apply(ObExecContext &ctx, ObOpSpec &op);
    virtual int reset(ObOpSpec &op);

  private:
    int ret_;
  };
public:
  ObPxTaskProcess(const observer::ObGlobalContext &gctx, ObPxRpcInitTaskArgs &arg);
  virtual ~ObPxTaskProcess();
  int process();
  void run();
  // for corotine RunFuncT
  void operator()(void);

  void set_is_oracle_mode(bool oracle_mode) { is_oracle_mode_ = oracle_mode; }
  bool is_oracle_mode() const { return is_oracle_mode_; }
  ObPxSqcHandler *get_sqc_handler() { return arg_.sqc_handler_; }
public:
  // 以下一组时间，是为了便于 sql audit 查看 worker 执行时间消耗在哪里
  void set_enqueue_timestamp(int64_t v) { enqueue_timestamp_ = v; }
  int64_t get_enqueue_timestamp() const { return enqueue_timestamp_; }
  int64_t get_process_timestamp() const { return process_timestamp_; }
  int64_t get_exec_start_timestamp() const { return exec_start_timestamp_; }
  int64_t get_exec_end_timestamp() const { return exec_end_timestamp_; }
  // 为了接口需要，需要实现以下四个方法，因为使用的是线程池，不是 RPC，故而模拟之
  int64_t get_send_timestamp() const { return get_enqueue_timestamp(); }
  int64_t get_receive_timestamp() const { return get_enqueue_timestamp(); }
  int64_t get_run_timestamp() const { return get_process_timestamp(); }
  int64_t get_single_process_timestamp() const { return get_process_timestamp(); }
  uint64_t get_qc_id() const {return arg_.task_.get_qc_id();}
  int64_t get_sqc_id() const {return arg_.task_.get_sqc_id();}
  int64_t get_worker_id() const {return arg_.task_.get_task_id();}
  int64_t get_dfo_id() const {return arg_.task_.get_dfo_id();}
  ObPxInterruptID get_interrupt_id()
      { return arg_.task_.get_interrupt_id(); }
  uint64_t get_session_id() const;
  uint64_t get_tenant_id() const;

  int execute(ObOpSpec &root);

private:
  /* functions */
  int do_process();
  int check_inner_stat();
  /* remember to call this function at the end of process() */
  virtual void record_exec_timestamp(bool is_first, ObExecTimestamp &exec_timestamp)
  { ObExecStatUtils::record_exec_timestamp(*this, is_first, exec_timestamp); }
  int record_tx_desc();
  int record_exec_feedback_info();
  int record_user_error_msg(int retcode);
  void release();
  /* variables */
  const observer::ObGlobalContext &gctx_;
  ObPxRpcInitTaskArgs &arg_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  observer::ObVirtualTableIteratorFactory vt_iter_factory_;

  /* timestamps for sql audit */
  int64_t enqueue_timestamp_;
  int64_t process_timestamp_;
  int64_t exec_start_timestamp_;
  int64_t exec_end_timestamp_;

  /* record oracle mode */
  bool is_oracle_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObPxTaskProcess);
};
}
}
#endif /* __OB_SQL_ENGINE_PX_TASK_PROCESS_H__ */
//// end of header file
