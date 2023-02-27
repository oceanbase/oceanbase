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

namespace oceanbase
{
namespace sql
{

class ObPartitionIdHashFunc
{
public:
  uint64_t operator()(const int64_t partition_id, const uint64_t hash)
  {
       // TODO
    uint64_t hash_val = hash;
    hash_val = partition_id;
    return hash_val;
  }
};

class ObGIOpInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObGIOpInput(ObExecContext &ctx, const ObOpSpec &spec);
  virtual ~ObGIOpInput() {}
  virtual void reset() override
  {
    table_location_keys_.reset();
  }
  virtual int init(ObTaskInfo &task_info) override;
  virtual void set_deserialize_allocator(common::ObIAllocator *allocator) override;
  inline int64_t get_parallelism() { return parallelism_; }
  void set_granule_pump(ObGranulePump *pump) { pump_ = pump; }
  void set_parallelism(int64_t parallelism) { parallelism_ = parallelism; }
  void set_worker_id(int64_t worker_id) { worker_id_ = worker_id; }
  int64_t get_worker_id() { return worker_id_; }
  int64_t get_px_sequence_id() { return px_sequence_id_; }
  void set_px_sequence_id(int64_t id) { px_sequence_id_ = id; }
  int add_table_location_keys(common::ObIArray<const ObTableScanSpec*> &tscs);
private:
  int deep_copy_range(ObIAllocator *allocator, const ObNewRange &src, ObNewRange &dst);
public:
  // the dop, the QC deside the dop before our task send to SQC server
  // but the dop may be change as the worker server don't has enough process.
  int64_t parallelism_;
  // 在 affinitize 模式下 GI 需要知道当前 GI 所属的 task id，以拉取对应的 partition 任务
  int64_t worker_id_;

  ObGranulePump *pump_;
  //for partition pruning
  common::ObSEArray<uint64_t, 2> table_location_keys_;
  int64_t px_sequence_id_;
private:
  common::ObIAllocator *deserialize_allocator_;
};

class ObGranuleIteratorSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObGranuleIteratorSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
  ~ObGranuleIteratorSpec() {}

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec,
                       K_(ref_table_id), K_(tablet_size), K_(affinitize), K_(access_all));

  void set_related_id(uint64_t ref_id) { ref_table_id_ = ref_id; }
  void set_tablet_size(int64_t tablet_size) { tablet_size_ = tablet_size; }
  int64_t get_tablet_size() { return tablet_size_; }

  bool partition_filter() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_USE_PARTITION_FILTER); }
  bool pwj_gi() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_PARTITION_WISE); }
  bool affinitize() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_AFFINITIZE); }
  bool access_all() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_ACCESS_ALL); }
  bool with_param_down() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_NLJ_PARAM_DOWN); }
  bool asc_order() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_ASC_ORDER); }
  bool desc_order() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_DESC_ORDER); }
  bool force_partition_granule() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_FORCE_PARTITION_GRANULE); }
  bool enable_partition_pruning() const { return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_ENABLE_PARTITION_PRUNING); }

  void set_gi_flags(uint64_t flags) {
    gi_attri_flag_ = flags;
    affinitize_ = affinitize();
    partition_wise_join_ = pwj_gi();
    access_all_ = access_all();
    nlj_with_param_down_ = with_param_down();
  }
  uint64_t get_gi_flags() { return gi_attri_flag_; }
  inline bool partition_wise() const { return partition_wise_join_ && !affinitize_; }
public:
  uint64_t ref_table_id_;
  int64_t tablet_size_;
  // affinitize用于表示线程和任务是否有进行绑定。
  bool affinitize_;
  // 是否是partition wise join/union/subplan filter，注意，如果是hybrid的pwj这个
  // flag也会被设置上的。
  bool partition_wise_join_;
  // 是否是每个线程扫描sqc包含的所有的partition。
  bool access_all_;
  // 是否是含有条件下降的nlj。
  bool nlj_with_param_down_;
  common::ObFixedArray<const ObTableScanSpec*, ObIAllocator> pw_op_tscs_;
  // 目前GI的所有属性都设置进了flag里面，使用的时候也尽量使用flag，而不是上面的
  // 几个单独的变量。目前来说因为兼容性的原因，无法删除上面的几个变量了，新加的
  // GI的属性都通过这个flag来进行判断。
  uint64_t gi_attri_flag_;
  // FULL PARTITION WISE情况下，GI可以分配在INSERT/REPLACE算子上，GI会控制insert/REPLACE表的任务划分
  ObTableModifySpec *dml_op_;
  // for partition join filter
  ObPxBFStaticInfo bf_info_;
  ObHashFunc hash_func_;
  ObExpr *tablet_id_expr_;
  // end for partition join filter
};

class ObGranuleIteratorOp : public ObOperator
{
private:
  enum ObGranuleIteratorState {
    GI_UNINITIALIZED,
    GI_PREPARED,
    GI_TABLE_SCAN,
    GI_GET_NEXT_GRANULE_TASK,
    GI_END,
  };
public:
  ObGranuleIteratorOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  ~ObGranuleIteratorOp() {}

  virtual int inner_open() override;
  virtual int rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;
  virtual int inner_close() override;

  void reset();
  void reuse();
  int set_tscs(common::ObIArray<const ObTableScanSpec*> &tscs);
  int set_dml_op(const ObTableModifySpec *dml_op);

  virtual OperatorOpenOrder get_operator_open_order() const override
  { return OPEN_SELF_FIRST; }
  int get_next_granule_task(bool prepare = false);
private:
  int parameters_init();
  // 非full partition wise获得task的方式
  // TODO: jiangting.lk 重构下函数名字
  int try_fetch_task(ObGranuleTaskInfo &info);
  /**
   * @brief
   * full partition wise的模式下，通过op ids获得对应的task infos
   * IN ctx
   * IN op_ids 消费GI task的phy op对应的op ids
   * OUT infos 每一个phy op对应的GI tasks
   */
  int fetch_full_pw_tasks(ObIArray<ObGranuleTaskInfo> &infos, const ObIArray<int64_t> &op_ids);
  int try_fetch_tasks(ObIArray<ObGranuleTaskInfo> &infos, const ObIArray<const ObTableScanSpec *> &tscs);
  int try_get_rows(const int64_t max_row_cnt);
  int do_get_next_granule_task(bool &partition_pruning);
  int prepare_table_scan();
  bool is_not_init() { return state_ == GI_UNINITIALIZED; }
  // 获得消费GI task的node：
  // 目前仅仅支持TSC或者Join
  int get_gi_task_consumer_node(ObOperator *cur, ObOperator *&child) const;
  int try_pruning_partition(
      const ObGITaskSet &taskset,
      int64_t &pos,
      bool &partition_pruned);
  int do_join_filter_partition_pruning(const ObGranuleTaskInfo &gi_task_info, bool &partition_pruning);
  int do_partition_pruning(const ObGranuleTaskInfo &gi_task_info,
      bool &partition_pruning);
  int do_partition_pruning(const common::ObIArray<ObGranuleTaskInfo> &gi_task_infos,
      bool &partition_pruning);
  int fetch_rescan_pw_task_infos(const common::ObIArray<int64_t> &op_ids,
      GIPrepareTaskMap *gi_prepare_map,
      common::ObIArray<ObGranuleTaskInfo> &gi_task_infos);
  int fetch_normal_pw_task_infos(const common::ObIArray<int64_t> &op_ids,
      GIPrepareTaskMap *gi_prepare_map,
      common::ObIArray<ObGranuleTaskInfo> &gi_task_infos);
  int try_build_tablet2part_id_map(ObDASTabletLoc *loc);
private:
  typedef common::hash::ObHashMap<int64_t, int64_t,
      common::hash::NoPthreadDefendMode> ObPxTablet2PartIdMap;
private:
  int64_t parallelism_;
  int64_t worker_id_;
  uint64_t tsc_op_id_;
  ObGranulePump *pump_;
  int64_t pump_version_;
  ObGranuleIteratorState state_;

  bool all_task_fetched_;
  bool is_rescan_;
  const ObGITaskSet *rescan_taskset_ = NULL;
  common::ObSEArray<int64_t, OB_MIN_PARALLEL_TASK_COUNT * 2> rescan_tasks_;
  int64_t rescan_task_idx_;
  // full pwj场景下, 在执行过程中缓存住了自己的任务队列.
  // 供GI rescan使用
  common::ObSEArray<ObGranuleTaskInfo, 2> pwj_rescan_task_infos_;
  // for px batch rescan and dynamic partition pruning
  common::ObSEArray<uint64_t, 2> table_location_keys_;
  common::ObSEArray<int64_t, 16> pruning_partition_ids_;
   //for partition pruning
  int64_t filter_count_; // filtered part count when part pruning activated
  int64_t total_count_; // total partition count or block count processed, rescan included
  ObPXBloomFilterHashWrapper bf_key_;
  ObPxBloomFilter *bloom_filter_ptr_;
  ObPxTablet2PartIdMap tablet2part_id_map_;
  ObOperator *real_child_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_GRANULE_ITERATOR_OP_H_
