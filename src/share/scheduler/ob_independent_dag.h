/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_SHARE_SCHEDULER_OB_INDEPENDENT_DAG_H_
#define SRC_SHARE_SCHEDULER_OB_INDEPENDENT_DAG_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace share
{

class ObIndependentDag : public ObIDag
{
public:
  friend class ObPrintIndependentDag;
  explicit ObIndependentDag(const ObDagType::ObDagTypeEnum type);
  virtual ~ObIndependentDag(); // DO NOT ACQUIRE LOCK OF DAG_SCHEDULER !!!!
public:
  virtual bool operator == (const ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override { return compat_mode_; }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return false; }
public:
  virtual int basic_init(ObIAllocator &allocator) final;
  virtual int add_task(ObITask &task) final;
  virtual int batch_add_task(const ObIArray<ObITask *> &task_array);
public:
  int init(const ObIDagInitParam *param, const ObDagId *dag_id = nullptr, const bool inherit_trace_id = false);
  int process();
  void dump_dag_status(const char *log_info = "Print the status of independent dag");
  void post_signal();
  OB_INLINE void set_compat_mode(lib::Worker::CompatMode mode) { compat_mode_ = mode; }
  VIRTUAL_TO_STRING_KV(KP(this), K_(id), "type", get_dag_type_str(type_),
      K_(is_inited), K_(is_stop), K_(dag_status), K_(start_time), K_(dag_ret),
      "ready_task_count", task_list_.get_size(), "waiting_task_count", waiting_task_list_.get_size(),
      K_(running_task_cnt), K_(compat_mode));
protected:
  virtual int64_t inner_get_total_task_list_count() const override final { return task_list_.get_size() + waiting_task_list_.get_size(); }
  virtual bool inner_add_task_into_list(ObITask *task) override final;
  virtual int inner_remove_task(ObITask &task) override final;
  virtual void clear_task_list() override final;
  virtual void reset() override final;
  virtual int check_cycle() override final;
private:
  int update_ready_task_list();
  int try_get_next_ready_task(ObITask *&task);
  int schedule_one(ObITask *&task);
  void wait_signal();
  int execute_task(ObITask &task);
  int process_task(ObITask *&task, int &task_ret);
  int deal_with_finish_task(ObITask *&task, const int error_code, bool &task_is_suspended);
  int deal_with_suspended_task(ObITask &task, const int error_code, bool &task_is_suspended);
private:
  static const int64_t DAG_COND_TIMEOUT_MS = 50L /*50ms*/;
  static const int64_t DAG_SCHEDULE_ONE_PRINT_INTERVAL = 60 * 1000L * 1000L /*60s*/;
  static const int64_t READY_TASK_BATCH_MOVE_SIZE = 50L;
private:
  lib::Worker::CompatMode compat_mode_;
  common::ObThreadCond cond_;
  TaskList waiting_task_list_; // for independent dag, task added into waiting task list firstly
};

class ObDagExecutor
{
public:
  ObDagExecutor();
  ~ObDagExecutor();
  template <typename T>
  int init(
      ObIAllocator *allocator,
      const ObIDagInitParam *param,
      const ObDagId *dag_id);
  int run();
  OB_INLINE bool is_inited() const { return is_inited_; }
  TO_STRING_KV(K_(is_inited), K_(dag_status), K_(ref_cnt), KPC_(dag));
private:
  int acquire_();
  void release_();
private:
  bool is_inited_;
  ObIAllocator *allocator_;
  lib::ObMutex lock_;
  ObIndependentDag *dag_;
  ObIDag::ObDagStatus dag_status_;
  int64_t ref_cnt_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDagExecutor);
};

template <typename T>
int ObDagExecutor::init(
    ObIAllocator *allocator,
    const ObIDagInitParam *param,
    const ObDagId *dag_id)
{
  int ret = OB_SUCCESS;
  T *new_dag = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "dag executor is inited", K(ret));
  } else if (OB_ISNULL(allocator) || OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid args", K(ret), KP(allocator), KP(param));
  } else if (OB_FAIL(ObTenantDagScheduler::alloc_dag(*allocator, false/*is_ha_dag*/, new_dag))) {
    COMMON_LOG(WARN, "failed to alloc dag", K(ret));
  } else if (FALSE_IT(dag_ = static_cast<T *>(new_dag))) {
  } else if (OB_FAIL(dag_->init(param, dag_id))) {
    COMMON_LOG(WARN, "failed to init dag", K(ret));
  } else {
    allocator_ = allocator;
    dag_status_ = ObIDag::DAG_STATUS_NODE_RUNNING;
    is_inited_ = true;
    COMMON_LOG(INFO, "dag executor init success", K(ret), KPC_(dag));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(dag_)) {
    dag_->~ObIndependentDag();
    allocator->free(dag_);
    dag_ = nullptr;
  }
  return ret;
}

/* attention! It maybe unsafe to use this class without holding ObIndependentDag::lock_ */
class ObPrintIndependentDag
{
public:
  ObPrintIndependentDag(ObIndependentDag &dag) : dag_(dag) {}
  virtual ~ObPrintIndependentDag() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  static constexpr int64_t MAX_PRINT_TASK_CNT = 10;
private:
  void task_to_string(
       ObITask *task,
       const char* task_status,
       char *buf,
       const int64_t buf_len,
       int64_t &pos) const;
  void print_task_list(
      common::ObDList<ObITask> &task_list,
      const char* task_list_name,
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      bool &is_print) const;
private:
  ObIndependentDag &dag_;
};

} // namespace share
} // namespace oceanbase

#endif /* SRC_SHARE_SCHEDULER_OB_INDEPENDENT_DAG_H_ */
