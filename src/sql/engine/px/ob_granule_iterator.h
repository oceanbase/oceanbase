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

#ifndef OB_GRANULE_ITERATOR_H_
#define OB_GRANULE_ITERATOR_H_
#include "sql/engine/ob_phy_operator.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/ob_no_children_phy_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/engine/px/ob_granule_pump.h"

namespace oceanbase {
namespace sql {
class ObGranulePump;
class ObGranuleIterator;
class ObGranuleIteratorCtx;
// The only way is set parameters via Input before the physical operator start running.
class ObGIInput : public ObIPhyOperatorInput {
  friend class ObGranuleIterator;
  friend class ObGranuleIteratorCtx;
  OB_UNIS_VERSION_V(1);

public:
  ObGIInput()
      : parallelism_(-1),
        worker_id_(common::OB_INVALID_INDEX),
        ranges_(),
        pkeys_(),
        pump_(nullptr),
        deserialize_allocator_(nullptr){};
  virtual ~ObGIInput(){};
  virtual void reset() override
  { /*@TODO fix reset member by*/
  }
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op) override;
  virtual ObPhyOperatorType get_phy_op_type() const override;
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
  int64_t get_worker_id() const
  {
    return worker_id_;
  }

private:
  int deep_copy_range(ObIAllocator* allocator, const ObNewRange& src, ObNewRange& dst);

private:
  // the dop, the QC deside the dop before our task send to SQC server
  // but the dop may be change as the worker server don't has enough process.
  int64_t parallelism_;
  int64_t worker_id_;

  // Need serialize
  common::ObSEArray<common::ObNewRange, 16> ranges_;
  common::ObSEArray<common::ObPartitionKey, 16> pkeys_;
  ObGranulePump* pump_;

private:
  common::ObIAllocator* deserialize_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObGIInput);
};

class ObGranuleIterator : public ObSingleChildPhyOperator {
  OB_UNIS_VERSION_V(1);

private:
  enum ObGranuleIteratorState {
    GI_UNINITIALIZED,
    GI_PREPARED,
    GI_TABLE_SCAN,
    GI_GET_NEXT_GRANULE_TASK,
    GI_END,
  };

public:
  class ObGranuleIteratorCtx : public ObPhyOperatorCtx {
  public:
    ObGranuleIteratorCtx(ObExecContext& exec_ctx)
        : ObPhyOperatorCtx(exec_ctx),
          parallelism_(-1),
          tablet_size_(common::OB_DEFAULT_TABLET_SIZE),
          worker_id_(-1),
          tsc_op_id_(OB_INVALID_ID),
          ranges_(),
          pkeys_(),
          pump_(nullptr),
          state_(GI_UNINITIALIZED),
          all_task_fetched_(false),
          is_rescan_(false),
          rescan_taskset_(nullptr),
          rescan_task_idx_(0)
    {}
    virtual ~ObGranuleIteratorCtx()
    {
      destroy();
    }
    virtual void destroy();
    bool is_not_init()
    {
      return state_ == GI_UNINITIALIZED;
    }
    // void set_inited() { state_ = GI_GET_NEXT_GRANULE_TASK; }
    int parameters_init(const ObGIInput* input);
    int64_t parallelism_;
    int64_t tablet_size_;
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

public:
  explicit ObGranuleIterator(common::ObIAllocator& alloc);
  virtual ~ObGranuleIterator();
  virtual void reset() override;
  virtual void reuse() override;
  // basic infomation
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const override;
  virtual int rescan(ObExecContext& ctx) const override;
  virtual int init_op_ctx(ObExecContext& ctx) const override;
  virtual int inner_open(ObExecContext& ctx) const override;
  virtual int inner_close(ObExecContext& ctx) const override;
  virtual int inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const;
  virtual int inner_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const override;
  virtual int create_operator_input(ObExecContext& ctx) const;
  virtual OperatorOpenOrder get_operator_open_order(ObExecContext& ctx) const override
  {
    UNUSED(ctx);
    return OPEN_SELF_FIRST;
  }
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObGranuleIterator);

public:
  int set_tscs(common::ObIArray<const ObTableScan*>& tscs);
  int set_dml_op(ObTableModify* dml_op);
  void set_related_id(uint64_t ref_id)
  {
    ref_table_id_ = ref_id;
  }
  void set_tablet_size(int64_t tablet_size)
  {
    tablet_size_ = tablet_size;
  }
  int64_t get_tablet_size() const
  {
    return tablet_size_;
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

private:
  int try_fetch_task(ObExecContext& ctx, ObGranuleTaskInfo& info) const;
  int fetch_full_pw_tasks(
      ObExecContext& ctx, ObIArray<ObGranuleTaskInfo>& infos, const ObIArray<int64_t>& op_ids) const;
  int try_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  int do_get_next_granule_task(ObGranuleIteratorCtx& gi_ctx, ObExecContext& ctx, bool prepare = false) const;
  int prepare_table_scan(ObExecContext& ctx) const;
  inline bool partition_wise() const
  {
    return partition_wise_join_ && !affinitize_;
  }
  int get_gi_task_consumer_node(const ObPhyOperator* cur, ObPhyOperator*& child) const;

private:
  uint64_t ref_table_id_;
  int64_t tablet_size_;
  // work and task affinitize
  bool affinitize_;
  bool partition_wise_join_;
  // scan all partitions of current SQC for every work
  bool access_all_;
  // hash param push down nlj
  bool nlj_with_param_down_;
  common::ObFixedArray<const ObTableScan*, ObIAllocator> pw_op_tscs_;
  uint64_t gi_attri_flag_;
  ObTableModify* dml_op_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif
