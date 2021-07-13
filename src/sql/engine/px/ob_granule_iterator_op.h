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

#ifndef OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_GRANULE_ITERATOR_OP_H_
#define OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_GRANULE_ITERATOR_OP_H_

#include "sql/engine/ob_operator.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/engine/px/ob_granule_pump.h"

namespace oceanbase {
namespace sql {

class ObGIOpInput : public ObOpInput {
  OB_UNIS_VERSION_V(1);

public:
  ObGIOpInput(ObExecContext& ctx, const ObOpSpec& spec);
  virtual ~ObGIOpInput()
  {}
  virtual void reset() override
  { /*@TODO fix reset member*/
  }
  virtual int init(ObTaskInfo& task_info) override;
  virtual void set_deserialize_allocator(common::ObIAllocator* allocator) override;
  inline int64_t get_parallelism()
  {
    return parallelism_;
  }
  int assign_ranges(const common::ObIArray<common::ObNewRange>& ranges);
  int assign_pkeys(const common::ObIArray<common::ObPartitionKey>& pkeys);
  void set_granule_pump(ObGranulePump* pump)
  {
    pump_ = pump;
  }
  void set_parallelism(int64_t parallelism)
  {
    parallelism_ = parallelism;
  }
  void set_worker_id(int64_t worker_id)
  {
    worker_id_ = worker_id;
  }
  int64_t get_worker_id()
  {
    return worker_id_;
  }

private:
  int deep_copy_range(ObIAllocator* allocator, const ObNewRange& src, ObNewRange& dst);

public:
  // the dop, the QC deside the dop before our task send to SQC server
  // but the dop may be change as the worker server don't has enough process.
  int64_t parallelism_;
  // In AFFINITIZE mode, GI should know current GI's task id and pull corresponding partition sub-task
  int64_t worker_id_;

  // Need serialize
  common::ObSEArray<common::ObNewRange, 16> ranges_;
  // use partition key/partition idx to tag partition
  common::ObSEArray<common::ObPartitionKey, 16> pkeys_;
  ObGranulePump* pump_;

private:
  common::ObIAllocator* deserialize_allocator_;
};

class ObGranuleIteratorSpec : public ObOpSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObGranuleIteratorSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type);
  ~ObGranuleIteratorSpec()
  {}

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(ref_table_id), K_(tablet_size), K_(affinitize), K_(access_all));

  void set_related_id(uint64_t ref_id)
  {
    ref_table_id_ = ref_id;
  }
  void set_tablet_size(int64_t tablet_size)
  {
    tablet_size_ = tablet_size;
  }
  int64_t get_tablet_size()
  {
    return tablet_size_;
  }

  bool partition_filter() const
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_USE_PARTITION_FILTER);
  }
  bool pwj_gi() const
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_PARTITION_WISE);
  }
  bool affinitize() const
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_AFFINITIZE);
  }
  bool access_all() const
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_ACCESS_ALL);
  }
  bool with_param_down() const
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_NLJ_PARAM_DOWN);
  }
  bool asc_partition_order() const
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_ASC_PARTITION_ORDER);
  }
  bool desc_partition_order() const
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_DESC_PARTITION_ORDER);
  }
  bool force_partition_granule() const
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_FORCE_PARTITION_GRANULE);
  }
  bool enable_partition_pruning() const
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_ENABLE_PARTITION_PRUNING);
  }

  void set_gi_flags(uint64_t flags)
  {
    gi_attri_flag_ = flags;
    affinitize_ = affinitize();
    partition_wise_join_ = pwj_gi();
    access_all_ = access_all();
    nlj_with_param_down_ = with_param_down();
  }
  uint64_t get_gi_flags()
  {
    return gi_attri_flag_;
  }
  inline bool partition_wise() const
  {
    return partition_wise_join_ && !affinitize_;
  }

public:
  uint64_t ref_table_id_;
  int64_t tablet_size_;
  // indicate whether has binds between threads and tasks
  bool affinitize_;
  // whether is partition wise join/union/subplan filter
  bool partition_wise_join_;
  // whether sqc threads scaned contains all PARTITION
  bool access_all_;
  bool nlj_with_param_down_;
  common::ObFixedArray<const ObTableScanSpec*, ObIAllocator> pw_op_tscs_;
  uint64_t gi_attri_flag_;
  ObTableModifySpec* dml_op_;
};

class ObGranuleIteratorOp : public ObOperator {
private:
  enum ObGranuleIteratorState {
    GI_UNINITIALIZED,
    GI_PREPARED,
    GI_TABLE_SCAN,
    GI_GET_NEXT_GRANULE_TASK,
    GI_END,
  };

public:
  ObGranuleIteratorOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
  ~ObGranuleIteratorOp()
  {}

  virtual int inner_open() override;
  virtual int rescan() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override;
  virtual int inner_close() override;

  void reset();
  void reuse();
  int set_tscs(common::ObIArray<const ObTableScanSpec*>& tscs);
  int set_dml_op(const ObTableModifySpec* dml_op);

  virtual OperatorOpenOrder get_operator_open_order() const override
  {
    return OPEN_SELF_FIRST;
  }

private:
  int parameters_init();
  int try_fetch_task(ObGranuleTaskInfo& info);
  int fetch_full_pw_tasks(ObIArray<ObGranuleTaskInfo>& infos, const ObIArray<int64_t>& op_ids);
  int try_fetch_tasks(ObIArray<ObGranuleTaskInfo>& infos, const ObIArray<const ObTableScanSpec*>& tscs);
  int try_get_next_row();
  int do_get_next_granule_task(bool prepare = false);
  int prepare_table_scan();
  bool is_not_init()
  {
    return state_ == GI_UNINITIALIZED;
  }
  int get_gi_task_consumer_node(ObOperator* cur, ObOperator*& child) const;
  int try_pruning_partition(const ObGITaskSet& taskset, ObGITaskSet::Pos& pos, bool& partition_pruned);

private:
  int64_t parallelism_;
  int64_t worker_id_;
  uint64_t tsc_op_id_;
  common::ObSEArray<common::ObNewRange, 16> ranges_;
  common::ObSEArray<common::ObPartitionKey, 16> pkeys_;
  ObGranulePump* pump_;
  ObGranuleIteratorState state_;

  bool all_task_fetched_;
  bool is_rescan_;
  const ObGITaskSet* rescan_taskset_ = NULL;
  common::ObSEArray<ObGITaskSet::Pos, OB_MIN_PARALLEL_TASK_COUNT * 2> rescan_tasks_;
  int64_t rescan_task_idx_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_GRANULE_ITERATOR_OP_H_
