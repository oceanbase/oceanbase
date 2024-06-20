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

#ifndef OCEANBASE_ENGINE_PX_EXCHANGE_OB_RECEIVE_OP_H_
#define OCEANBASE_ENGINE_PX_EXCHANGE_OB_RECEIVE_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/executor/ob_task_location.h"
#include "sql/executor/ob_slice_id.h"
#include "share/ob_scanner.h"
#include "lib/container/ob_array_serialization.h"

namespace oceanbase
{
namespace sql
{

#define IS_RECEIVE(type) \
(((type) == PHY_FIFO_RECEIVE) || \
 ((type) == PHY_FIFO_RECEIVE_V2) || \
 ((type) == PHY_PX_FIFO_RECEIVE) || \
 ((type) == PHY_PX_MERGE_SORT_RECEIVE) || \
 ((type) == PHY_VEC_PX_MERGE_SORT_RECEIVE) || \
 ((type) == PHY_PX_FIFO_COORD) || \
 ((type) == PHY_PX_ORDERED_COORD) || \
 ((type) == PHY_PX_MERGE_SORT_COORD) || \
 ((type) == PHY_VEC_PX_MERGE_SORT_COORD) || \
 ((type) == PHY_TASK_ORDER_RECEIVE) || \
 ((type) == PHY_MERGE_SORT_RECEIVE) || \
 ((type) == PHY_DIRECT_RECEIVE))

#define IS_TABLE_INSERT(type) \
(((type) == PHY_INSERT) || \
 ((type) == PHY_REPLACE) || \
 ((type) == PHY_INSERT_ON_DUP) || \
 ((type) == PHY_INSERT_RETURNING))



class ObReceiveOpInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObReceiveOpInput(ObExecContext &ctx, const ObOpSpec &spec);
  virtual ~ObReceiveOpInput();
  virtual void reset() override;
  // Setup
  virtual int init(ObTaskInfo &task_info);
  // Use
  inline uint64_t get_pull_slice_id() { return pull_slice_id_; }
  inline int64_t get_child_job_id() { return child_job_id_; }
  inline uint64_t get_child_op_id() { return child_op_id_; };
  int get_result_location(const int64_t child_job_id,
                          const int64_t child_task_id,
                          common::ObAddr &svr);
protected:
  uint64_t pull_slice_id_;
  int64_t child_job_id_;
  uint64_t child_op_id_;
  common::ObSArray<ObTaskLocation> task_locs_;
};

class ObReceiveSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObReceiveSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  bool is_receive() const override { return true; }

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec,
                       K_(partition_order_specified),
                       K_(need_set_affected_row),
                       K_(is_merge_sort));

  // 是否要按指定的顺序拉取partition的数据
  bool partition_order_specified_;
  // 是否需要设置plan ctx 中的affected_row元信息
  bool need_set_affected_row_;
  bool is_merge_sort_;
};

class ObReceiveOp : public ObOperator
{
public:
  ObReceiveOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObReceiveOp() {}

  virtual int inner_open() override { return ObOperator::inner_open(); }
  virtual void destroy() override { ObOperator::destroy(); }
  virtual int inner_close() override { return ObOperator::inner_close(); }

  int switch_iterator()
  {
    //exchange operator not support switch iterator, return OB_ITER_END directly
    return common::OB_ITER_END;
  }

  virtual int inner_drain_exch() override
  {
    // Drain exchange is used in parallelism execution,
    // do nothing for old fashion distributed execution.
    return common::OB_SUCCESS;
  }
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_PX_EXCHANGE_OB_RECEIVE_OP_H_
