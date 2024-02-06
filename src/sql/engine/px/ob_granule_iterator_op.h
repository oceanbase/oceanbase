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
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_share_info.h"
namespace oceanbase
{
namespace sql
{
class ObP2PDatahubMsgBase;
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
  int64_t get_rf_max_wait_time() { return rf_max_wait_time_; }
  void set_rf_max_wait_time(int64_t rf_max_wait_time) { rf_max_wait_time_ = rf_max_wait_time; }
private:
  int deep_copy_range(ObIAllocator *allocator, const ObNewRange &src, ObNewRange &dst);
public:
  // the dop, the QC decide the dop before our task send to SQC server
  // but the dop may be change as the worker server don't has enough process.
  int64_t parallelism_;
  // 在 affinitize 模式下 GI 需要知道当前 GI 所属的 task id，以拉取对应的 partition 任务
  int64_t worker_id_;

  ObGranulePump *pump_;
  //for partition pruning
  common::ObSEArray<uint64_t, 2> table_location_keys_;
  int64_t px_sequence_id_;
  int64_t rf_max_wait_time_;
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
                       K_(index_table_id), K_(tablet_size), K_(affinitize), K_(access_all));

  void set_related_id(uint64_t index_id) { index_table_id_ = index_id; }
  void set_tablet_size(int64_t tablet_size) { tablet_size_ = tablet_size; }
  int64_t get_tablet_size() { return tablet_size_; }



  void set_gi_flags(uint64_t flags) {
    gi_attri_flag_ = flags;
    affinitize_ = ObGranuleUtil::affinitize(gi_attri_flag_);
    partition_wise_join_ = ObGranuleUtil::pwj_gi(gi_attri_flag_);
    access_all_ = ObGranuleUtil::access_all(gi_attri_flag_);
    nlj_with_param_down_ = ObGranuleUtil::with_param_down(gi_attri_flag_);
  }
  uint64_t get_gi_flags() { return gi_attri_flag_; }
  // pw_op_tscs_和pw_dml_tsc_ids_作用是相同的，前者在调度sqc时生成，后者是4.1新增的cg时生成的，为了兼容性，
  // 目前同时保留了这两个结构，4.2上可以直接删除pw_op_tscs_和所有引用到的地方
  inline bool full_partition_wise() const { return partition_wise_join_ && (!affinitize_ || pw_op_tscs_.count() > 1 || pw_dml_tsc_ids_.count() > 1); }
public:
  uint64_t index_table_id_;
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
  // for compatibility now, to be removed on 4.2
  common::ObFixedArray<const ObTableScanSpec*, ObIAllocator> pw_op_tscs_;
  common::ObFixedArray<int64_t, ObIAllocator> pw_dml_tsc_ids_;
  // 目前GI的所有属性都设置进了flag里面，使用的时候也尽量使用flag，而不是上面的
  // 几个单独的变量。目前来说因为兼容性的原因，无法删除上面的几个变量了，新加的
  // GI的属性都通过这个flag来进行判断。
  uint64_t gi_attri_flag_;
  // FULL PARTITION WISE情况下，GI可以分配在INSERT/REPLACE算子上，GI会控制insert/REPLACE表的任务划分
  ObTableModifySpec *dml_op_;
  // for partition join filter
  ObPxBFStaticInfo bf_info_;
  ObHashFunc hash_func_;
  //TODO: shanting. remove this expr in 4.3
  ObExpr *tablet_id_expr_;
  // end for partition join filter
  int64_t repart_pruning_tsc_idx_;
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
  // ---for nlj pkey
  int try_pruning_repart_partition(
      const ObGITaskSet &taskset,
      int64_t &pos,
      bool &partition_pruned);
  bool repart_partition_pruned(const ObGranuleTaskInfo &info) {
    return info.tablet_loc_->tablet_id_.id() != ctx_.get_gi_pruning_info().get_part_id();
  }
  // ---end
  // for nlj/sbf param down, dynamic partition pruning
  int do_dynamic_partition_pruning(const ObGranuleTaskInfo &gi_task_info,
      bool &partition_pruning);
  int do_dynamic_partition_pruning(const common::ObIArray<ObGranuleTaskInfo> &gi_task_infos,
      bool &partition_pruning);
  // ---end---

  int fetch_rescan_pw_task_infos(const common::ObIArray<int64_t> &op_ids,
      GIPrepareTaskMap *gi_prepare_map,
      common::ObIArray<ObGranuleTaskInfo> &gi_task_infos);
  int fetch_normal_pw_task_infos(const common::ObIArray<int64_t> &op_ids,
      GIPrepareTaskMap *gi_prepare_map,
      common::ObIArray<ObGranuleTaskInfo> &gi_task_infos);

  //---for partition run time filter
  bool enable_parallel_runtime_filter_pruning();
  bool enable_single_runtime_filter_pruning();
  int do_single_runtime_filter_pruning(const ObGranuleTaskInfo &gi_task_info, bool &partition_pruning);
  int do_parallel_runtime_filter_pruning();
  int wait_runtime_ready(bool &partition_pruning);
  int do_join_filter_partition_pruning(int64_t tablet_id, bool &partition_pruning);
  int try_build_tablet2part_id_map();
  //---end----
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
  ObP2PDatahubMsgBase *rf_msg_;
  ObP2PDhKey rf_key_;
  int64_t rf_start_wait_time_;
  ObPxTablet2PartIdMap tablet2part_id_map_;
  ObOperator *real_child_;
  bool is_parallel_runtime_filtered_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_GRANULE_ITERATOR_OP_H_
