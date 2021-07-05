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

#ifndef __OB_SQLL_EXECUTOR_RECEIVE_OPERATOR__
#define __OB_SQLL_EXECUTOR_RECEIVE_OPERATOR__

#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/executor/ob_task_location.h"
#include "sql/executor/ob_slice_id.h"
#include "share/ob_scanner.h"
#include "lib/container/ob_array_serialization.h"

#define IS_RECEIVE(type)                                                                                               \
  (((type) == PHY_FIFO_RECEIVE) || ((type) == PHY_FIFO_RECEIVE_V2) || ((type) == PHY_PX_FIFO_RECEIVE) ||               \
      ((type) == PHY_PX_MERGE_SORT_RECEIVE) || ((type) == PHY_PX_FIFO_COORD) || ((type) == PHY_PX_MERGE_SORT_COORD) || \
      ((type) == PHY_TASK_ORDER_RECEIVE) || ((type) == PHY_MERGE_SORT_RECEIVE) || ((type) == PHY_DIRECT_RECEIVE))

#define IS_ASYNC_RECEIVE(type) \
  (((type) == PHY_FIFO_RECEIVE_V2) || ((type) == PHY_TASK_ORDER_RECEIVE) || ((type) == PHY_MERGE_SORT_RECEIVE))

#define IS_TABLE_INSERT(type)                                                            \
  (((type) == PHY_INSERT) || ((type) == PHY_REPLACE) || ((type) == PHY_INSERT_ON_DUP) || \
      ((type) == PHY_INSERT_RETURNING))

namespace oceanbase {
namespace sql {
class ObReceiveInput : public ObIPhyOperatorInput {
  friend class ObReceive;
  OB_UNIS_VERSION_V(1);

public:
  ObReceiveInput();
  virtual ~ObReceiveInput();
  virtual void reset() override;
  // Setup
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op);
  // Use
  inline uint64_t get_pull_slice_id() const
  {
    return pull_slice_id_;
  }
  inline int64_t get_child_job_id() const
  {
    return child_job_id_;
  }
  inline uint64_t get_child_op_id() const
  {
    return child_op_id_;
  };
  int get_result_location(const int64_t child_job_id, const int64_t child_task_id, common::ObAddr& svr) const;

protected:
  uint64_t pull_slice_id_;
  int64_t child_job_id_;
  uint64_t child_op_id_;
  common::ObSArray<ObTaskLocation> task_locs_;
};

class ObReceive : public ObSingleChildPhyOperator {
  OB_UNIS_VERSION_V(1);

protected:
  class ObReceiveCtx : public ObPhyOperatorCtx {
  public:
    explicit ObReceiveCtx(ObExecContext& ctx);
    virtual ~ObReceiveCtx();
    virtual void destroy()
    {
      ObPhyOperatorCtx::destroy_base();
    }
  };

public:
  explicit ObReceive(common::ObIAllocator& alloc);
  virtual ~ObReceive();
  virtual int switch_iterator(ObExecContext& ctx) const override;
  void set_partition_order_specified(bool order_specified)
  {
    partition_order_specified_ = order_specified;
  }
  virtual bool is_receive_op() const override
  {
    return true;
  }
  void set_is_merge_sort(bool is_merge_sort)
  {
    is_merge_sort_ = is_merge_sort;
  }

  bool is_merge_sort() const
  {
    return is_merge_sort_;
  }

  void set_need_set_affected_row(bool b)
  {
    need_set_affected_row_ = b;
  }

  bool get_need_set_affected_row() const
  {
    return need_set_affected_row_;
  }
  virtual OperatorOpenOrder get_operator_open_order(ObExecContext& ctx) const
  {
    UNUSED(ctx);
    return OPEN_SELF_ONLY;
  }

  virtual int drain_exch(ObExecContext& ctx) const override
  {
    // Drain exchange is used in parallelism execution,
    // do nothing for old fashion distributed execution.
    UNUSED(ctx);
    return common::OB_SUCCESS;
  }

protected:
  bool partition_order_specified_;
  bool need_set_affected_row_;
  bool is_merge_sort_;

private:
  // disallow copy
  ObReceive(const ObReceive& other);
  ObReceive& operator=(const ObReceive& ohter);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQLL_EXECUTOR_RECEIVE_OPERATOR__ */
//// end of header file
