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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_FUSION_PARALLEL_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_FUSION_PARALLEL_H_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/lock/ob_thread_cond.h"
#include "observer/ob_srv_task.h"
#include "rpc/frame/ob_req_processor.h"
#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/search/ob_das_bitmap_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "share/diagnosis/ob_runtime_profile.h"

namespace oceanbase
{
namespace sql
{

class ObExpr;
class ObDesExecContext;
struct ObDASFusionCtDef;
struct ObDASFusionRtDef;
struct ObDASBaseRtDef;
class ObExecContext;
class ObEvalCtx;
class ObFastBitmap;
class ObDASFusionParallelCtx;
class ObDASFusionParallelCoordinator;

struct ObDASFusionMaterializedRow
{
  ObDASFusionMaterializedRow()
    : rowkey_(0),
      score_(0.0)
  {}
  ObDASFusionMaterializedRow(const uint64_t rowkey, const double score)
    : rowkey_(rowkey),
      score_(score)
  {}

  uint64_t rowkey_;
  double score_;
  TO_STRING_KV(K_(rowkey), K_(score));
};

struct ObSharedBitmapSlot
{
  ObSharedBitmapSlot()
    : bitmap_alloc_(common::ObMemAttr(MTL_ID(), "DASSharedBmp")),
      bitmap_(nullptr),
      bitmap_occurrence_idx_(-1),
      is_built_(false),
      build_ret_(common::OB_SUCCESS)
  {}

  void release();

  common::ObArenaAllocator bitmap_alloc_;
  ObFastBitmap *bitmap_;
  int64_t bitmap_occurrence_idx_;
  bool is_built_;
  int build_ret_;

  TO_STRING_KV(KP_(bitmap), K_(bitmap_occurrence_idx),
               K_(is_built), K_(build_ret));
};

struct ObBitmapPhaseBarrier
{
  ObBitmapPhaseBarrier()
    : total_cnt_(0),
      built_cnt_(0),
      first_err_(common::OB_SUCCESS),
      is_inited_(false)
  {}

  int init(int64_t total_cnt);
  void on_bitmap_built(int err_code);
  int wait_all_built(int64_t timeout_ts);

  int64_t total_cnt_;
  int64_t built_cnt_;       // Always accessed under cond_'s mutex.
  int first_err_;           // Accessed via ATOMIC_BCAS/ATOMIC_LOAD which handle memory ordering.
  common::ObThreadCond cond_;
  bool is_inited_;
};

struct ObDASFusionChildRuntime
{
  ObDASFusionChildRuntime()
    : path_idx_(-1),
      child_iter_(nullptr),
      owns_child_iter_(false),
      child_exec_ctx_(nullptr),
      child_eval_ctx_(nullptr),
      child_search_ctx_(nullptr),
      rowkey_expr_(nullptr),
      score_expr_(nullptr),
      max_batch_size_(1),
      child_allocator_(common::ObMemAttr(MTL_ID(), "DASFusEval")),
      submitted_(false),
      finished_(false),
      err_code_(common::OB_SUCCESS),
      use_rescan_(false),
      rows_(),
      src_exec_ctx_(nullptr),
      src_eval_ctx_(nullptr),
      src_search_ctx_(nullptr),
      fusion_ctdef_(nullptr),
      child_rtdef_root_(nullptr),
      is_range_parallel_(false),
      docid_range_lo_(),
      docid_range_hi_(),
      range_top_k_limit_(-1),
      cloned_rtdef_root_(nullptr),
      profile_(nullptr),
      parallel_ctx_(nullptr),
      assigned_bitmap_slots_(),
      child_bitmap_ops_(),
      preser_exec_ctx_buf_(nullptr),
      preser_exec_ctx_size_(0)
  {}

  /// Lightweight init: only store pointers, no deep copy.
  int init(const int64_t path_idx,
           const ObDASFusionCtDef &fusion_ctdef,
           ObExecContext &src_exec_ctx,
           ObEvalCtx &src_eval_ctx,
           ObDASSearchCtx &src_search_ctx,
           ObDASBaseRtDef *child_rtdef_root);

  /// Deep copy contexts, rtdef tree, create iter, and reserve result buffer.
  /// Called on worker thread.
  int prepare_parallel_resources();
  /// Release worker-thread resources (iter, contexts, allocator) that must be
  /// destroyed on the thread where they were created (e.g. MemoryContext).
  /// Preserves rows_ so the main thread can still read results.
  void release_parallel_resources();
  void reset_result()
  {
    submitted_ = false;
    finished_ = false;
    err_code_ = common::OB_SUCCESS;
    rows_.reset();
  }
  int swizzle_child_rtdef(ObExecContext &child_exec_ctx,
                          ObEvalCtx &child_eval_ctx,
                          ObDASBaseRtDef &child_rtdef);
  int create_fusion_child_exec_ctx(ObExecContext &src_exec_ctx,
                                   common::ObIAllocator &child_alloc,
                                   ObDesExecContext *&child_exec_ctx);
  int create_fusion_child_eval_ctx(ObExecContext &child_exec_ctx,
                                   ObEvalCtx &src_eval_ctx,
                                   common::ObIAllocator &child_alloc,
                                   ObEvalCtx *&child_eval_ctx);
  int create_fusion_child_search_ctx(common::ObIAllocator &alloc,
                                     ObEvalCtx &child_eval_ctx,
                                     ObDASSearchCtx *&child_search_ctx);
  int create_parallel_iter();
  int materialize_batch_result(const int64_t batch_size);
  int drain_child_iter(ObDASFusionParallelCoordinator &coordinator);

  void destroy_rtdef_tree(ObDASBaseRtDef *rtdef);
  int deep_copy_rtdef_tree(common::ObIAllocator &alloc,
                           ObDASBaseRtDef *src,
                           ObDASBaseRtDef *&dst);

  int64_t path_idx_;
  ObDASIter *child_iter_;
  bool owns_child_iter_;  // true if child_iter_ was created by this runtime (via create_parallel_iter)
  ObDesExecContext *child_exec_ctx_;
  ObEvalCtx *child_eval_ctx_;
  ObDASSearchCtx *child_search_ctx_;
  ObExpr *rowkey_expr_;
  ObExpr *score_expr_;
  int64_t max_batch_size_;
  common::ObArenaAllocator child_allocator_;
  bool submitted_;
  bool finished_;
  int err_code_;
  bool use_rescan_;
  common::ObSEArray<ObDASFusionMaterializedRow, 256> rows_;

  // Source references for deferred deep copy (set during init, used in prepare_parallel_context)
  ObExecContext *src_exec_ctx_;
  ObEvalCtx *src_eval_ctx_;
  ObDASSearchCtx *src_search_ctx_;
  const ObDASFusionCtDef *fusion_ctdef_;
  ObDASBaseRtDef *child_rtdef_root_;

  // Range-parallel fields
  bool is_range_parallel_;
  common::ObObj docid_range_lo_;
  common::ObObj docid_range_hi_;
  int64_t range_top_k_limit_;
  ObDASBaseRtDef *cloned_rtdef_root_;
  // Per-task profile allocated on a shared thread-safe arena (ObSafeArenaAllocator)
  // and adopted into the parent fusion profile via adopt_child().
  // Created on main thread before task submission; worker thread writes metrics into it.
  // Lifetime is tied to ObExecContext::profile_arena_ (destroyed with ObExecContext).
  common::ObOpProfile<common::ObMetric> *profile_;

  // Back-pointer to parallel context for shared bitmap coordination
  ObDASFusionParallelCtx *parallel_ctx_;
  // Bitmap slots assigned to this runtime for building (round-robin in create_child_runtime).
  common::ObSEArray<ObSharedBitmapSlot *, 2> assigned_bitmap_slots_;
  // Bitmap ops collected from this runtime's deep-copied search op tree (DFS order).
  // Index i corresponds to shared_bitmap_slots_[i].bitmap_occurrence_idx_ == i.
  common::ObSEArray<ObDASBitmapOp *, 4> child_bitmap_ops_;

  // Pre-serialized exec ctx buffer (set by main thread before task submission,
  // read-only by worker thread). Points into the main alloc (fusion_memctx_).
  const char *preser_exec_ctx_buf_;
  int64_t preser_exec_ctx_size_;

  // Shared bitmap phase methods (called on worker thread)
  int build_assigned_bitmaps(int64_t timeout_ts);
  int wait_all_bitmaps_ready(int64_t timeout_ts);
  int inject_shared_bitmaps();
  void compensate_bitmap_slots(int err_code);

  TO_STRING_KV(K_(path_idx),
               KP_(child_iter),
               KP_(child_exec_ctx),
               KP_(child_eval_ctx),
               KP_(child_search_ctx),
               KP_(rowkey_expr),
               KP_(score_expr),
               K_(max_batch_size),
               K_(submitted),
               K_(finished),
               K_(err_code),
               K_(use_rescan),
               K_(is_range_parallel),
               K_(docid_range_lo),
               K_(docid_range_hi),
               K_(rows));
};

class ObDASFusionParallelCtx
{
public:
  ObDASFusionParallelCtx() : exec_ctx_ser_buf_(nullptr), exec_ctx_ser_size_(0) {}
  ~ObDASFusionParallelCtx() = default;

  int init(common::ObIAllocator &alloc,
           const ObDASFusionCtDef &fusion_ctdef,
           ObDASFusionRtDef &fusion_rtdef,
           ObDASSearchCtx &search_ctx,
           ObExecContext &exec_ctx,
           ObEvalCtx &eval_ctx,
           ObDASIter **children,
           const int64_t children_cnt);
  void release();
  int64_t get_runtime_count() const { return child_runtimes_.count(); }
  ObDASFusionChildRuntime *at(const int64_t idx) const
  {
    return (idx >= 0 && idx < child_runtimes_.count()) ? child_runtimes_.at(idx) : nullptr;
  }
  static bool should_enable_parallel(const ObDASFusionCtDef &fusion_ctdef,
                                     const ObDASSearchCtx &search_ctx);
  bool has_shared_bitmaps() const { return shared_bitmap_slots_.count() > 0; }
  int64_t get_shared_bitmap_count() const { return shared_bitmap_slots_.count(); }
  ObSharedBitmapSlot *get_shared_bitmap_slot(int64_t idx)
  {
    return (idx >= 0 && idx < shared_bitmap_slots_.count()) ? shared_bitmap_slots_.at(idx) : nullptr;
  }
  ObBitmapPhaseBarrier &get_bitmap_barrier() { return bitmap_barrier_; }

private:
  int discover_shared_bitmaps(common::ObIAllocator &alloc,
                              ObDASIter *search_child_iter, int64_t actual_dop);

  int alloc_and_init_runtime(
      common::ObIAllocator &alloc,
      const int64_t path_idx,
      const ObDASFusionCtDef &fusion_ctdef,
      ObDASBaseRtDef *child_rtdef,
      ObExecContext &exec_ctx,
      ObEvalCtx &eval_ctx,
      ObDASSearchCtx &search_ctx,
      ObDASFusionChildRuntime *&runtime);

  int create_child_runtime(
      common::ObIAllocator &alloc,
      const int64_t path_idx,
      const int64_t range_dop,
      const ObDASFusionCtDef &fusion_ctdef,
      ObDASBaseRtDef *child_rtdef,
      ObDASSearchCtx &search_ctx,
      ObExecContext &exec_ctx,
      ObEvalCtx &eval_ctx,
      ObDASIter *child_iter);

  static int compute_docid_split_points(
      ObDASSearchCtx &search_ctx,
      common::ObIAllocator &alloc,
      const int64_t range_dop,
      common::ObIArray<common::ObObj> &split_lo,
      common::ObIArray<common::ObObj> &split_hi);

  common::ObSEArray<ObDASFusionChildRuntime *, 4> child_runtimes_;
  common::ObSEArray<ObSharedBitmapSlot *, 4> shared_bitmap_slots_;
  ObBitmapPhaseBarrier bitmap_barrier_;
  // exec_ctx pre-serialized in main thread; worker threads deserialize their own copy.
  // This avoids the data race from ObExecContext::serialize/get_serialize_size
  // modifying session package-info state when called from multiple worker threads.
  char *exec_ctx_ser_buf_;
  int64_t exec_ctx_ser_size_;
};

class ObDASFusionParallelCoordinator
{
public:
  ObDASFusionParallelCoordinator();
  ~ObDASFusionParallelCoordinator() = default;

  int init(const int64_t total_cnt);
  void reset();
  int wait_all_complete(const int64_t timeout_ts);
  void on_child_finish(const int err_code);
  void set_first_error(const int err_code);
  int get_first_error() const { return ATOMIC_LOAD(&first_err_code_); }
  bool is_inited() const { return is_inited_; }

private:
  int64_t total_cnt_;
  int64_t finished_cnt_;    // Always accessed under cond_'s mutex.
  int first_err_code_;      // Lock-free access via ATOMIC_BCAS/ATOMIC_LOAD which handle memory ordering.
  common::ObThreadCond cond_;
  bool is_inited_;
};

class ObDASFusionChildTaskHandler : public rpc::frame::ObReqProcessor
{
public:
  ObDASFusionChildTaskHandler()
    : task_(nullptr)
  {}
  ~ObDASFusionChildTaskHandler() = default;

  int init(observer::ObSrvTask *task);
  void reset() { task_ = nullptr; }

protected:
  int run() override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDASFusionChildTaskHandler);
  observer::ObSrvTask *task_;
};

class ObDASFusionChildTask : public observer::ObSrvTask
{
public:
  ObDASFusionChildTask();
  ~ObDASFusionChildTask() = default;

  int init(ObDASFusionChildRuntime *runtime,
           ObDASFusionParallelCoordinator *coordinator,
           const int64_t timeout_ts,
           const int32_t group_id);
  void reset();

  rpc::frame::ObReqProcessor &get_processor() override { return handler_; }
  const common::ObCurTraceId::TraceId &get_trace_id() const { return trace_id_; }
  int64_t get_timeout_ts() const { return timeout_ts_; }
  ObDASFusionChildRuntime *get_runtime() const { return runtime_; }
  ObDASFusionParallelCoordinator *get_coordinator() const { return coordinator_; }

private:
  ObDASFusionChildRuntime *runtime_;
  ObDASFusionParallelCoordinator *coordinator_;
  common::ObCurTraceId::TraceId trace_id_;
  int64_t timeout_ts_;
  ObDASFusionChildTaskHandler handler_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_ITER_OB_DAS_FUSION_PARALLEL_H_
