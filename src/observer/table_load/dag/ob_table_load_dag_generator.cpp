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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/dag/ob_table_load_dag_generator.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/xml/ob_multi_mode_interface.h"
#include "observer/table_load/dag/ob_table_load_dag.h"
#include "observer/table_load/dag/ob_table_load_dag_exec_ctx.h"
#include "observer/table_load/dag/ob_table_load_dag_task.h"
#include "observer/table_load/plan/ob_table_load_op.h"
#include "observer/table_load/plan/ob_table_load_plan.h"
#include "observer/table_load/plan/ob_table_load_table_op.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace observer
{
using namespace share;

class ObTableLoadArrayQueue
{
public:
  ObTableLoadArrayQueue() : capacity_(0), front_(0), rear_(0), count_(0), is_inited_(false)
  {
    array_.set_tenant_id(MTL_ID());
  }

  ~ObTableLoadArrayQueue() { array_.reset(); }

  int init(const uint64_t capacity)
  {
    int ret = OB_SUCCESS;
    if (IS_INIT) {
      ret = OB_INIT_TWICE;
      SERVER_LOG(WARN, "ObTableLoadArrayQueue init twice", KR(ret));
    } else {
      capacity_ = capacity;
      front_ = 0;
      rear_ = -1;
      count_ = 0;
      if (OB_FAIL(array_.prepare_allocate(capacity))) {
        SERVER_LOG(WARN, "fail to prepare allocate", KR(ret));
      } else {
        is_inited_ = true;
      }
    }
    return ret;
  }

  bool is_full() { return (count_ == capacity_); }

  bool is_empty() { return (count_ == 0); }

  int push(const int64_t item)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      SERVER_LOG(WARN, "ObTableLoadArrayQueue not init", KR(ret));
    } else if (is_full()) {
      ret = OB_SIZE_OVERFLOW;
      SERVER_LOG(WARN, "queue is full", KR(ret));
    } else {
      rear_ = (rear_ + 1) % capacity_;
      array_.at(rear_) = item;
      count_++;
    }
    return ret;
  }

  int pop()
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      SERVER_LOG(WARN, "ObTableLoadArrayQueue not init", KR(ret));
    } else if (is_empty()) {
      ret = OB_ENTRY_NOT_EXIST;
      SERVER_LOG(WARN, "queue is empty", KR(ret));
    } else {
      front_ = (front_ + 1) % capacity_;
      count_--;
    }
    return ret;
  }

  int peek(int64_t &item)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      SERVER_LOG(WARN, "ObTableLoadArrayQueue not init", KR(ret));
    } else if (is_empty()) {
      ret = OB_ENTRY_NOT_EXIST;
      SERVER_LOG(WARN, "queue is empty", KR(ret));
    } else {
      item = array_.at(front_);
    }
    return ret;
  }

private:
  uint64_t capacity_;
  int64_t front_;
  int64_t rear_;
  uint64_t count_;
  ObArray<int64_t> array_;
  bool is_inited_;
};

class ObTableLoadDagOpQueue
{
private:
  struct hash_fun
  {
    int operator()(const ObTableLoadTableOp *plan_op, uint64_t &res) const
    {
      res = reinterpret_cast<uint64_t>(plan_op);
      return OB_SUCCESS;
    }
  };
  struct equal
  {
    bool operator()(const ObTableLoadTableOp *a, const ObTableLoadTableOp *b) { return (a == b); }
  };
  typedef hash::ObHashMap<ObTableLoadTableOp *, int64_t, common::hash::NoPthreadDefendMode,
                          hash_fun, equal>
    TableOpMap;

public:
  ObTableLoadDagOpQueue() : is_inited_(false)
  {
    table_op_set_.set_tenant_id(MTL_ID());
    adj_.set_tenant_id(MTL_ID());
    in_degree_.set_tenant_id(MTL_ID());
  }
  virtual ~ObTableLoadDagOpQueue()
  {
    table_op_set_.reset();
    adj_.reset();
    in_degree_.reset();
  }
  int init(ObTableLoadTableOp *root_op);
  int top(ObTableLoadTableOp *&table_op);
  int pop();
  bool empty() { return ready_table_op_indices_.is_empty(); }

private:
  int init_table_op_set(ObTableLoadTableOp *root_op);
  int init_adjacency();
  int init_ready_table_op_index_queue();

private:
  ObArray<ObTableLoadTableOp *> table_op_set_;
  ObArray<ObArray<int64_t>> adj_;
  ObArray<int64_t> in_degree_;
  ObTableLoadArrayQueue ready_table_op_indices_;
  bool is_inited_;
};

int ObTableLoadDagOpQueue::init(ObTableLoadTableOp *root_op)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_table_op_set(root_op))) {
    LOG_WARN("fail to init table op set", KR(ret));
  } else if (OB_FAIL(init_adjacency())) {
    LOG_WARN("fail to init adjacency", KR(ret));
  } else if (OB_FAIL(init_ready_table_op_index_queue())) {
    LOG_WARN("fail to init ready table op index queue", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadDagOpQueue::init_table_op_set(ObTableLoadTableOp *root_op)
{
  int ret = OB_SUCCESS;
  ObFixedQueue<ObTableLoadTableOp> table_op_queue;
  TableOpMap table_op_map;
  if (OB_FAIL(table_op_queue.init(100))) {
    LOG_WARN("fail to init table op queue", KR(ret));
  } else if (OB_FAIL(table_op_map.create(100, "TLD_TableOpMap", "TLD_TableOpMap", MTL_ID()))) {
    LOG_WARN("fail to create table op map", KR(ret));
  } else if (OB_FAIL(table_op_queue.push(root_op))) {
    LOG_WARN("fail to push", KR(ret));
  } else if (OB_FAIL(table_op_map.set_refactored(root_op, 0))) {
    LOG_WARN("fail to set refactored", KR(ret));
  } else {
    while (OB_SUCC(ret) && table_op_queue.get_total() > 0) {
      ObTableLoadTableOp *table_op = nullptr;
      if (OB_FAIL(table_op_queue.pop(table_op))) {
        LOG_WARN("fail to pop", KR(ret));
      } else if (OB_FAIL(table_op_set_.push_back(table_op))) {
        LOG_WARN("fail to push back", KR(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < table_op->get_dependees().count(); i++) {
          ObTableLoadTableOp *depend_op = table_op->get_dependees().at(i);
          if (nullptr != table_op_map.get(depend_op)) {
          } else if (OB_FAIL(table_op_queue.push(depend_op))) {
            LOG_WARN("fail to push", KR(ret));
          } else if (OB_FAIL(table_op_map.set_refactored(depend_op, 0))) {
            LOG_WARN("fail to set refactored", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadDagOpQueue::init_adjacency()
{
  int ret = OB_SUCCESS;
  TableOpMap table_op_map;
  int64_t table_op_count = table_op_set_.count();
  if (OB_FAIL(table_op_map.create(100, "TLD_TableOpMap", "TLD_TableOpMap", MTL_ID()))) {
    LOG_WARN("fail to create table op map", KR(ret));
  } else if (OB_FAIL(adj_.prepare_allocate(table_op_count))) {
    LOG_WARN("fail to prepare allocate", KR(ret));
  } else if (OB_FAIL(in_degree_.prepare_allocate(table_op_count, 0))) {
    LOG_WARN("fail to prepare allocate", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_op_set_.count(); i++) {
    ObTableLoadTableOp *table_op = table_op_set_.at(i);
    if (OB_FAIL(table_op_map.set_refactored(table_op, i))) {
      LOG_WARN("fail to set_refactored", KR(ret));
    }
  }
  for (int i = 0; OB_SUCC(ret) && i < table_op_set_.count(); i++) {
    ObTableLoadTableOp *table_op = table_op_set_.at(i);
    for (int j = 0; OB_SUCC(ret) && j < table_op->get_dependees().count(); j++) {
      ObTableLoadTableOp *dependency_table_op = table_op->get_dependees().at(j);
      int64_t child_index = 0;
      if (OB_FAIL(table_op_map.get_refactored(dependency_table_op, child_index))) {
        LOG_WARN("fail to get refactored", KR(ret));
      } else if (OB_FAIL(adj_.at(i).push_back(child_index))) {
        LOG_WARN("fail to push back", KR(ret));
      } else {
        in_degree_.at(child_index)++;
      }
    }
  }
  return ret;
}

int ObTableLoadDagOpQueue::init_ready_table_op_index_queue()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ready_table_op_indices_.init(table_op_set_.count()))) {
    LOG_WARN("fail to init ready table op index queue", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_op_set_.count(); i++) {
    if (0 == in_degree_.at(i)) {
      if (OB_FAIL(ready_table_op_indices_.push(i))) {
        LOG_WARN("fail to push", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadDagOpQueue::top(ObTableLoadTableOp *&table_op)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagOpQueue not init", KR(ret));
  } else if (ready_table_op_indices_.is_empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("queue is empty", KR(ret));
  } else {
    int64_t front_index = 0;
    if (OB_FAIL(ready_table_op_indices_.peek(front_index))) {
      LOG_WARN("fail to peek", KR(ret));
    } else {
      table_op = table_op_set_.at(front_index);
    }
  }
  return ret;
}

int ObTableLoadDagOpQueue::pop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDagOpQueue not init", KR(ret));
  } else if (ready_table_op_indices_.is_empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("queue is empty", KR(ret));
  } else {
    int64_t front_index = 0;
    if (OB_FAIL(ready_table_op_indices_.peek(front_index))) {
      LOG_WARN("fail to peek", KR(ret));
    } else if (OB_FAIL(ready_table_op_indices_.pop())) {
      LOG_WARN("fail to pop", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < adj_.at(front_index).count(); i++) {
        int64_t dependee_index = adj_.at(front_index).at(i);
        in_degree_.at(dependee_index)--;
        if (0 == in_degree_.at(dependee_index)) {
          if (OB_FAIL(ready_table_op_indices_.push(dependee_index))) {
            LOG_WARN("fail to push", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadDagGenerator::generate(ObTableLoadDagExecCtx &dag_exec_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == dag_exec_ctx.plan_ || nullptr == dag_exec_ctx.dag_ ||
                  !dag_exec_ctx.plan_->is_generated())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(dag_exec_ctx));
  } else {
    ObArray<ObTableLoadTableOp *> table_ops;
    ObArray<ObTableLoadOp *> executable_ops;
    ObArray<ObITask *> dag_tasks;
    if (OB_FAIL(generate_table_op_topological_order(dag_exec_ctx.plan_->get_first_table_op(),
                                                    table_ops))) {
      LOG_WARN("fail to generate topological order", KR(ret), K(dag_exec_ctx));
    } else if (OB_FAIL(table_op_list_to_executable_op_list(table_ops, executable_ops))) {
      LOG_WARN("fail to plan op list to op list", KR(ret), K(dag_exec_ctx));
    }
    // add finish op
    else if (OB_FAIL(executable_ops.push_back(dag_exec_ctx.plan_->get_finish_op()))) {
      LOG_WARN("fail to push back", KR(ret));
    } else if (OB_FAIL(executable_op_list_to_dag_task_list(executable_ops, dag_exec_ctx.dag_,
                                                           dag_tasks))) {
      LOG_WARN("fail to executable op list to dag task list", KR(ret), K(dag_exec_ctx));
    } else {
      // add task to dag
      for (int i = 0; OB_SUCC(ret) && i < dag_tasks.count(); ++i) {
        ObITask *task = dag_tasks.at(i);
        if (OB_FAIL(dag_exec_ctx.dag_->add_task(*task))) {
          LOG_WARN("fail to add task", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableLoadDagGenerator::generate_table_op_topological_order(
  ObTableLoadTableOp *root_op, ObIArray<ObTableLoadTableOp *> &table_ops)
{
  int ret = OB_SUCCESS;
  ObTableLoadDagOpQueue queue;
  ObTableLoadTableOp *table_op;
  if (OB_FAIL(queue.init(root_op))) {
    LOG_WARN("fail to init queue", KR(ret));
  }
  while (OB_SUCC(ret) && !queue.empty()) {
    if (OB_FAIL(queue.top(table_op))) {
      LOG_WARN("fail to top", KR(ret));
    } else if (OB_FAIL(table_ops.push_back(table_op))) {
      LOG_WARN("fail to push back", KR(ret));
    } else if (OB_FAIL(queue.pop())) {
      LOG_WARN("fail to pop", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadDagGenerator::table_op_list_to_executable_op_list(
  const ObIArray<ObTableLoadTableOp *> &table_ops, ObIArray<ObTableLoadOp *> &executable_ops)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_allocator("TLD_OP");
  tmp_allocator.set_tenant_id(MTL_ID());
  ObStack<ObTableLoadOp *> op_stack(&tmp_allocator);
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ops.count(); i++) {
    if (OB_FAIL(op_stack.push(table_ops.at(i)))) {
      LOG_WARN("fail to push back plan op", KR(ret));
    }
    while (OB_SUCC(ret) && !op_stack.empty()) {
      ObTableLoadOp *op = op_stack.top();
      op_stack.pop();
      // leaf node
      if (ObTableLoadOpType::is_executable_op(op->get_op_type())) {
        if (OB_FAIL(executable_ops.push_back(op))) {
          LOG_WARN("fail to push back op", KR(ret));
        }
      } else {
        for (int64_t j = op->get_childs().count() - 1; OB_SUCC(ret) && j >= 0; j--) {
          if (OB_FAIL(op_stack.push(op->get_childs().at(j)))) {
            LOG_WARN("fail to push back plan op", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadDagGenerator::executable_op_list_to_dag_task_list(
  const ObIArray<ObTableLoadOp *> &executable_ops, ObTableLoadDag *dag,
  ObIArray<ObITask *> &dag_tasks)
{
  int ret = OB_SUCCESS;
  ObITask *dag_task = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < executable_ops.count(); i++) {
    if (OB_FAIL(ObTableLoadDagOpTaskBase::create_op_task(dag, executable_ops.at(i), dag_task))) {
      LOG_WARN("fail to create dag task", KR(ret));
    } else if (OB_FAIL(dag_tasks.push_back(dag_task))) {
      LOG_WARN("fail to push back dag task", KR(ret));
    }
  }
  // build dependence on dag task
  for (int i = 1; OB_SUCC(ret) && i < dag_tasks.count(); ++i) {
    ObITask *prev_task = dag_tasks.at(i - 1);
    ObITask *cur_task = dag_tasks.at(i);
    if (OB_FAIL(prev_task->add_child(*cur_task))) {
      LOG_WARN("fail to add child", KR(ret));
    }
  }
  return ret;
}
} // namespace observer
} // namespace oceanbase
