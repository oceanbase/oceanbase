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

#ifndef OCEANBASE_EXECUTOR_OB_DETERMINATE_TASK_TRANSMIT_H_
#define OCEANBASE_EXECUTOR_OB_DETERMINATE_TASK_TRANSMIT_H_

#include "sql/executor/ob_distributed_transmit.h"
#include "sql/executor/ob_task_info.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase {
namespace sql {
class ObTableLocation;

// Tasks are determined before execute, we sore those tasks in this operator.
class ObDeterminateTaskTransmit : public ObDistributedTransmit {
  OB_UNIS_VERSION_V(1);

public:
  class ITaskRouting {
  public:
    enum Policy {
      DATA_REPLICA_PICKER,
      INDEX_REPLICA_PICKER,
      SAME_WITH_CHILD,
    };

    ITaskRouting()
    {}
    virtual ~ITaskRouting()
    {}

    virtual int route(
        Policy policy, const ObTaskInfo& task, const common::ObIArray<common::ObAddr>& previous, ObAddr& server) = 0;
  };

  struct TaskIndex {
    OB_UNIS_VERSION_V(1);

  public:
    int32_t loc_idx_;
    int32_t part_loc_idx_;

    TaskIndex() : loc_idx_(0), part_loc_idx_(0)
    {}
    TO_STRING_KV(K(loc_idx_), K(part_loc_idx_));
  };

  struct IdRange {
    OB_UNIS_VERSION_V(1);

  public:
    int32_t begin_;
    int32_t end_;

    IdRange() : begin_(0), end_(0)
    {}
    TO_STRING_KV(K(begin_), K(end_));
  };

  struct ResultRange {
    OB_UNIS_VERSION_V(1);

  public:
    IdRange task_range_;
    IdRange slice_range_;

    TO_STRING_KV(K(task_range_), K(slice_range_));
  };

  // compare ObNewRange::start_ and ObNewRow
  struct RangeStartCompare;

private:
  class ObDeterminateTaskTransmitCtx : public ObDistributedTransmitCtx {
  public:
    explicit ObDeterminateTaskTransmitCtx(ObExecContext& ctx)
        : ObDistributedTransmitCtx(ctx), close_child_manually_(false)
    {}
    virtual ~ObDeterminateTaskTransmitCtx()
    {}

    virtual void destroy() override
    {
      ObDistributedTransmitCtx::destroy();
    }

  public:
    bool close_child_manually_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObDeterminateTaskTransmitCtx);
  };

  typedef common::hash::ObHashMap<int64_t, int64_t, common::hash::NoPthreadDefendMode> Id2IdxMap;
  typedef common::hash::ObHashSet<ObTaskID> TaskIDSet;

public:
  explicit ObDeterminateTaskTransmit(common::ObIAllocator& alloc);
  virtual ~ObDeterminateTaskTransmit()
  {}

  virtual int init_op_ctx(ObExecContext& exec_ctx) const override;
  virtual int inner_open(ObExecContext& exec_ctx) const override;
  virtual int inner_close(ObExecContext& ctx) const override;
  virtual OperatorOpenOrder get_operator_open_order(ObExecContext& ctx) const override;
  typedef common::ObFixedArray<ObTaskInfo::ObRangeLocation, common::ObIAllocator> RangeLocations;
  typedef common::ObFixedArray<TaskIndex, common::ObIAllocator> Tasks;
  typedef common::ObFixedArray<common::ObFixedArray<common::ObNewRange, common::ObIAllocator>, common::ObIAllocator>
      ShuffleRanges;
  typedef common::ObFixedArray<int64_t, common::ObIAllocator> StartSliceIds;
  typedef common::ObFixedArray<ResultRange, common::ObIAllocator> ResultMapping;

  void set_result_reusable(bool reusable)
  {
    result_reusable_ = reusable;
  }
  RangeLocations& get_range_locations()
  {
    return range_locations_;
  }
  Tasks& get_tasks()
  {
    return tasks_;
  }
  ShuffleRanges& get_shuffle_ranges()
  {
    return shuffle_ranges_;
  }
  StartSliceIds& get_start_slice_ids()
  {
    return start_slice_ids_;
  }
  ResultMapping& get_result_mapping()
  {
    return result_mapping_;
  }

  void set_shuffle_by_part()
  {
    shuffle_by_part_ = true;
  }
  void set_shuffle_by_range()
  {
    shuffle_by_range_ = true;
  }

  ITaskRouting* get_task_routing() const
  {
    return task_routing_;
  }
  void set_task_routing(ITaskRouting* routing)
  {
    task_routing_ = routing;
  }

  ITaskRouting::Policy get_task_route_policy() const
  {
    return task_route_policy_;
  }
  void set_task_routing_policy(ITaskRouting::Policy policy)
  {
    task_route_policy_ = policy;
  }

  void set_background(const bool v)
  {
    background_ = v;
  }
  bool is_background() const
  {
    return background_;
  }

private:
  int alloc_result_array(
      ObExecContext& exec_ctx, ObIntermResultManager& mgr, const int64_t cnt, ObIntermResult**& results) const;
  int free_result_array(ObIntermResultManager& mgr, const int64_t cnt, ObIntermResult**& results) const;
  // delete all result, return the first error.
  int delete_all_results(ObIntermResultManager& mgr, const ObTaskID& task_id, const int64_t cnt) const;
  int shuffle_row(ObExecContext& exec_ctx, ObSqlSchemaGuard& schema_guard, ObTableLocation& table_location,
      Id2IdxMap& partition_id2idx_map, const common::ObNewRow& row, int64_t& slice_idx) const;

private:
  static common::ObLatch task_set_init_lock_;
  static TaskIDSet executing_task_set_instance_;
  static TaskIDSet* executing_tasks();

private:
  bool result_reusable_;
  RangeLocations range_locations_;
  Tasks tasks_;

  // shuffle info
  // 1. shuffle by table partition
  // 2. shuffle by range
  bool shuffle_by_part_;
  bool shuffle_by_range_;
  // ranges must be: left open right close `(left, right]`,
  // and cover the whole range (min, max), and has no overlay
  ShuffleRanges shuffle_ranges_;
  StartSliceIds start_slice_ids_;

  // describe how the result are mapped to the receiver
  ResultMapping result_mapping_;

  ITaskRouting::Policy task_route_policy_;
  ITaskRouting* task_routing_;

  // run task in background threads
  bool background_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_EXECUTOR_OB_DETERMINATE_TASK_TRANSMIT_H_
