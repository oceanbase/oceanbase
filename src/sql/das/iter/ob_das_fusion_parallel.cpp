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

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/iter/ob_das_fusion_parallel.h"
#include "lib/ob_errno.h"
#include "lib/time/ob_time_utility.h"
#include "sql/das/iter/ob_das_iter_utils.h"
#include "sql/das/iter/ob_das_search_driver_iter.h"
#include "sql/das/iter/ob_das_vec_index_driver_iter.h"
#include "sql/das/search/ob_das_search_define.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/ob_das_factory.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_des_exec_context.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_basic_session_info.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"
#include "sql/das/search/ob_das_bitmap_op.h"
#include "sql/das/search/ob_das_bmm_op.h"
#include "storage/tx_storage/ob_access_service.h"

namespace oceanbase
{
namespace sql
{
// ---------------------------------------------------------------------------
// Shared bitmap helpers
// ---------------------------------------------------------------------------
static int collect_bitmap_ops_dfs(ObIDASSearchOp *op, common::ObIArray<ObDASBitmapOp *> &bitmap_ops)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is null", KR(ret));
  } else {
    if (op->get_op_type() == DAS_SEARCH_OP_BITMAP) {
      if (OB_FAIL(bitmap_ops.push_back(static_cast<ObDASBitmapOp *>(op)))) {
        LOG_WARN("failed to push bitmap op", KR(ret));
      }
    }
    ObIDASSearchOp **children = op->get_children();
    for (int64_t i = 0; OB_SUCC(ret) && i < op->get_children_cnt(); ++i) {
      if (OB_FAIL(collect_bitmap_ops_dfs(children[i], bitmap_ops))) {
        LOG_WARN("failed to collect bitmap ops in child", KR(ret), K(i));
      }
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// ObDASFusionChildRuntime
// ---------------------------------------------------------------------------
int ObDASFusionChildRuntime::init(const int64_t path_idx,
                                  const ObDASFusionCtDef &fusion_ctdef,
                                  ObExecContext &src_exec_ctx,
                                  ObEvalCtx &src_eval_ctx,
                                  ObDASSearchCtx &src_search_ctx,
                                  ObDASBaseRtDef *child_rtdef_root)
{
  int ret = OB_SUCCESS;
  child_exec_ctx_ = nullptr;
  child_eval_ctx_ = nullptr;
  child_search_ctx_ = nullptr;
  child_iter_ = nullptr;
  if (OB_UNLIKELY(fusion_ctdef.rowid_exprs_.count() < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowid_exprs_ is empty in fusion ctdef", KR(ret), K(path_idx));
  } else if (OB_UNLIKELY(path_idx < 0 || path_idx >= fusion_ctdef.get_score_exprs().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("score expr index out of range", KR(ret), K(path_idx),
        K(fusion_ctdef.get_score_exprs().count()));
  } else if (OB_ISNULL(rowkey_expr_ = fusion_ctdef.rowid_exprs_.at(0))
      || OB_ISNULL(score_expr_ = fusion_ctdef.get_score_exprs().at(path_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion child rowkey or score expr is null", KR(ret), K(path_idx),
        KP(rowkey_expr_), KP(score_expr_));
  } else {
    path_idx_ = path_idx;
    max_batch_size_ = src_eval_ctx.is_vectorized() ? src_eval_ctx.max_batch_size_ : 1;
    use_rescan_ = false;
    // Store source references for deferred deep copy in worker thread
    src_exec_ctx_ = &src_exec_ctx;
    src_eval_ctx_ = &src_eval_ctx;
    src_search_ctx_ = &src_search_ctx;
    fusion_ctdef_ = &fusion_ctdef;
    child_rtdef_root_ = child_rtdef_root;
  }
  return ret;
}

int ObDASFusionChildRuntime::prepare_parallel_resources()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_exec_ctx_) || OB_ISNULL(src_eval_ctx_) || OB_ISNULL(src_search_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source context pointers are null", KR(ret), K(path_idx_),
        KP(src_exec_ctx_), KP(src_eval_ctx_), KP(src_search_ctx_));
  } else if (OB_FAIL(create_fusion_child_exec_ctx(*src_exec_ctx_, child_allocator_, child_exec_ctx_))) {
    LOG_WARN("failed to create fusion child exec ctx", KR(ret), K(path_idx_));
  } else if (OB_ISNULL(child_exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion child exec ctx is null", KR(ret), K(path_idx_));
  } else if (OB_FAIL(create_fusion_child_eval_ctx(*child_exec_ctx_, *src_eval_ctx_, child_allocator_, child_eval_ctx_))) {
    LOG_WARN("failed to create fusion child eval ctx", KR(ret), K(path_idx_));
  } else if (OB_ISNULL(child_eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion child eval ctx is null", KR(ret), K(path_idx_));
  } else if (OB_FAIL(create_fusion_child_search_ctx(child_allocator_, *child_eval_ctx_, child_search_ctx_))) {
    LOG_WARN("failed to create fusion child search ctx", KR(ret), K(path_idx_));
  } else if (OB_ISNULL(child_search_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion child search ctx is null", KR(ret), K(path_idx_));
  } else if (is_range_parallel_ && FALSE_IT(child_search_ctx_->set_docid_range(docid_range_lo_, docid_range_hi_))) {
    // Set docid range BEFORE create_parallel_iter so that init_scan_range (called
    // inside do_init) sees has_docid_range=true and constrains the storage scan range.
  } else if (OB_FAIL(create_parallel_iter())) {
    LOG_WARN("failed to create parallel iter", KR(ret), K(path_idx_));
  } else if (OB_ISNULL(parallel_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parallel ctx is null", KR(ret), K(path_idx_));
  } else if (!parallel_ctx_->has_shared_bitmaps() || !is_range_parallel_) {
    // no shared bitmaps or not a range-parallel path, skip bitmap op collection
  } else if (OB_UNLIKELY(child_iter_->get_type() != ObDASIterType::DAS_ITER_SEARCH_DRIVER)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child iter type for bitmap op collection",
             KR(ret), K(path_idx_), K(child_iter_->get_type()));
  } else {
    ObDASSearchDriverIter *search_iter = static_cast<ObDASSearchDriverIter *>(child_iter_);
    ObIDASSearchOp *root_op = nullptr;
    if (OB_ISNULL(search_iter) || OB_ISNULL(root_op = search_iter->get_root_op())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("search iter or root op is null for bitmap op collection", KR(ret), K(path_idx_));
    } else if (OB_FAIL(collect_bitmap_ops_dfs(root_op, child_bitmap_ops_))) {
      LOG_WARN("failed to collect child bitmap ops", KR(ret), K(path_idx_));
    } else if (OB_UNLIKELY(child_bitmap_ops_.count() != parallel_ctx_->get_shared_bitmap_count())) {
      // Sanity check: shared bitmap injection relies on bitmap_occurrence_idx_ being
      // identical between the main-thread DFS (discover_shared_bitmaps) and the worker
      // DFS here. Any mismatch in op-tree shape after deep_copy_rtdef would silently
      // bind bitmap data to the wrong op and produce incorrect results.
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("worker bitmap op count mismatches main thread", KR(ret), K(path_idx_),
                "worker_count", child_bitmap_ops_.count(),
                "main_count", parallel_ctx_->get_shared_bitmap_count());
    }
  }
  return ret;
}

void ObDASFusionChildRuntime::release_parallel_resources()
{
  // Only release child_iter_ if this runtime owns it (created via create_parallel_iter).
  // When prepare_parallel_resources fails before creating a new iter, child_iter_ still
  // points to the shared original iter owned by FusionIter::children_[]; releasing it
  // would cause use-after-free for other runtimes and the main thread.
  if (owns_child_iter_ && OB_NOT_NULL(child_iter_)) {
    child_iter_->release();
    child_iter_ = nullptr;
  } else {
    child_iter_ = nullptr;
  }
  if (OB_NOT_NULL(child_search_ctx_)) {
    child_search_ctx_->reset();
    child_search_ctx_->~ObDASSearchCtx();
    child_search_ctx_ = nullptr;
  }
  if (OB_NOT_NULL(child_eval_ctx_)) {
    child_eval_ctx_->~ObEvalCtx();
    child_eval_ctx_ = nullptr;
  }
  if (OB_NOT_NULL(child_exec_ctx_)) {
    child_exec_ctx_->~ObDesExecContext();
    child_exec_ctx_ = nullptr;
  }
  if (OB_NOT_NULL(cloned_rtdef_root_)) {
    destroy_rtdef_tree(cloned_rtdef_root_);
    cloned_rtdef_root_ = nullptr;
  }
  child_bitmap_ops_.reset();
  assigned_bitmap_slots_.reset();
  child_allocator_.reset();
}

int ObDASFusionChildRuntime::create_fusion_child_exec_ctx(ObExecContext &src_exec_ctx,
                                                          common::ObIAllocator &child_alloc,
                                                          ObDesExecContext *&child_exec_ctx)
{
  int ret = OB_SUCCESS;
  child_exec_ctx = nullptr;
  void *ctx_buf = nullptr;
  int64_t des_pos = 0;
  // Use the pre-serialized buffer produced by the main thread in
  // ObDASFusionParallelCtx::init(), rather than calling src_exec_ctx.serialize()
  // here on the worker thread.  ObExecContext::serialize/get_serialize_size both
  // invoke my_session_->add/reset_all_package_changed_info() which mutate shared
  // session state — calling them concurrently from multiple worker threads causes
  // a data race
  if (OB_ISNULL(preser_exec_ctx_buf_) || OB_UNLIKELY(preser_exec_ctx_size_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pre-serialized exec ctx buffer is invalid", KR(ret),
             KP(preser_exec_ctx_buf_), K(preser_exec_ctx_size_));
  } else if (OB_ISNULL(src_exec_ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src exec ctx physical plan ctx is null", KR(ret));
  } else if (OB_ISNULL(ctx_buf = child_alloc.alloc(sizeof(ObDesExecContext)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc ObDesExecContext for fusion child", KR(ret));
  } else if (FALSE_IT(child_exec_ctx = new (ctx_buf) ObDesExecContext(child_alloc, nullptr))) {
  } else if (OB_FAIL(child_exec_ctx->deserialize(preser_exec_ctx_buf_, preser_exec_ctx_size_, des_pos))) {
    LOG_WARN("failed to deserialize fusion child exec ctx from pre-serialized buffer",
             KR(ret), K(preser_exec_ctx_size_));
  } else {
    // phy_plan pointer is not serialized, restore it from src to keep frame info accessible
    child_exec_ctx->reference_my_plan(src_exec_ctx.get_physical_plan_ctx()->get_phy_plan());
  }
  return ret;
}

// Allocate expression frames without relying on zero_init_pos_ / zero_init_size_.
// This mirrors the approach used by ObPxTreeSerializer::deserialize_frame_info:
// allocate frame_size_ bytes and MEMSET the entire region to zero.
// alloc_frame's ALLOC_FRAME_MEM macro uses zero_init_pos_/zero_init_size_ for
// partial zero-init, but generate_partial_expr_frame may shrink frame_size_
// without adjusting those fields, causing a buffer overflow.
static int alloc_frames_safe(common::ObIAllocator &alloc,
                             const ObExprFrameInfo &fi,
                             const common::ObIArray<char *> &param_frame_ptrs,
                             uint64_t &frame_cnt,
                             char **&frames)
{
  int ret = common::OB_SUCCESS;
  frame_cnt = fi.const_frame_ptrs_.count()
              + fi.param_frame_.count()
              + fi.dynamic_frame_.count()
              + fi.datum_frame_.count();
  if (0 == frame_cnt) {
    // nothing to allocate
  } else if (OB_ISNULL(frames = static_cast<char **>(alloc.alloc(frame_cnt * sizeof(char *))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc frames array", KR(ret), K(frame_cnt));
  } else {
    MEMSET(frames, 0, frame_cnt * sizeof(char *));
    int64_t frame_idx = 0;
    // const frames: share the plan's const frame memory (read-only)
    for (int64_t i = 0; i < fi.const_frame_ptrs_.count(); ++i) {
      frames[frame_idx++] = fi.const_frame_ptrs_.at(i);
    }
    // param frames: allocate a private copy per worker.
    // Sharing the plan ctx's param frame pointers directly causes data races when multiple
    // workers concurrently evaluate expressions whose results land in a param frame slot
    // (e.g. rowkey_expr_). Each worker must have its own writable copy.
    // IMPORTANT: iterate over ALL fi.param_frame_ entries, not just param_frame_ptrs.count().
    // If param_frame_ptrs has fewer entries (e.g. after exec ctx deserialization), allocate
    // zeroed frames for the missing ones. Failing to do so leaves NULL slots in frames_[]
    // and shifts dynamic/datum frame indices, causing SIGSEGV when expressions access their
    // frames via frame_idx_.
    for (int64_t i = 0; OB_SUCC(ret) && i < fi.param_frame_.count(); ++i) {
      const ObFrameInfo &f = fi.param_frame_.at(i);
      char *buf = nullptr;
      if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(f.frame_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc param frame copy for fusion child", KR(ret), K(i), K(f.frame_size_));
      } else if (i < param_frame_ptrs.count()) {
        MEMCPY(buf, param_frame_ptrs.at(i), f.frame_size_);
        frames[frame_idx++] = buf;
      } else {
        // No source param frame available (e.g. exec ctx deserialization may not populate
        // all param frames); zero the frame. Expressions will re-evaluate into it.
        MEMSET(buf, 0, f.frame_size_);
        frames[frame_idx++] = buf;
      }
    }
    // dynamic frames: allocate frame_size_ and zero the entire region
    for (int64_t i = 0; OB_SUCC(ret) && i < fi.dynamic_frame_.count(); ++i) {
      const ObFrameInfo &f = fi.dynamic_frame_.at(i);
      char *buf = static_cast<char *>(alloc.alloc(f.frame_size_));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc dynamic frame", KR(ret), K(i), K(f.frame_size_));
      } else {
        MEMSET(buf, 0, f.frame_size_);
        frames[frame_idx++] = buf;
      }
    }
    // datum frames: allocate frame_size_ and zero the entire region
    for (int64_t i = 0; OB_SUCC(ret) && i < fi.datum_frame_.count(); ++i) {
      const ObFrameInfo &f = fi.datum_frame_.at(i);
      char *buf = static_cast<char *>(alloc.alloc(f.frame_size_));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc datum frame", KR(ret), K(i), K(f.frame_size_));
      } else {
        MEMSET(buf, 0, f.frame_size_);
        frames[frame_idx++] = buf;
      }
    }
  }
  return ret;
}

int ObDASFusionChildRuntime::create_fusion_child_eval_ctx(ObExecContext &child_exec_ctx,
                                                          ObEvalCtx &src_eval_ctx,
                                                          common::ObIAllocator &child_alloc,
                                                          ObEvalCtx *&child_eval_ctx)
{
  int ret = OB_SUCCESS;
  void *ctx_buf = nullptr;
  child_eval_ctx = nullptr;
  const ObPhysicalPlanCtx *plan_ctx = nullptr;
  const ObPhysicalPlan *phy_plan = nullptr;
  char **frames = nullptr;
  uint64_t frame_cnt = 0;
  if (OB_ISNULL(plan_ctx = child_exec_ctx.get_physical_plan_ctx()) || OB_ISNULL(phy_plan = plan_ctx->get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx or phy plan is null", KR(ret), KP(plan_ctx), KP(phy_plan));
  } else {
    const ObExprFrameInfo &fi = phy_plan->get_expr_frame_info();
    // Allocate frames using the same approach as deserialize_frame_info
    // (ob_px_util.cpp) instead of alloc_frame.  alloc_frame uses zero_init_pos_
    // / zero_init_size_ for partial zero-initialization, but when the plan goes
    // through generate_partial_expr_frame the frame_size_ may be shrunk without
    // adjusting those fields, causing a buffer overflow.  deserialize_frame_info
    // simply MEMSETs the entire frame_size_ region, which is safe regardless.
    if (OB_FAIL(alloc_frames_safe(child_alloc, fi, plan_ctx->get_param_frame_ptrs(),
                                  frame_cnt, frames))) {
      LOG_WARN("failed to alloc fusion child expr frames", KR(ret), K(frame_cnt));
    }
  }
  if (OB_FAIL(ret)) {
    // already logged above
  } else if (FALSE_IT(child_exec_ctx.set_frame_cnt(frame_cnt))) {
  } else if (FALSE_IT(child_exec_ctx.set_frames(frames))) {
  } else if (OB_ISNULL(ctx_buf = child_alloc.alloc(sizeof(ObEvalCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc fusion child eval ctx", KR(ret));
  } else if (FALSE_IT(child_eval_ctx = new (ctx_buf) ObEvalCtx(child_exec_ctx, &child_alloc))) {
  } else {
    child_eval_ctx->max_batch_size_ = src_eval_ctx.max_batch_size_;
    child_eval_ctx->reuse(src_eval_ctx.get_batch_size());
  }
  return ret;
}

int ObDASFusionChildRuntime::create_fusion_child_search_ctx(common::ObIAllocator &alloc,
                                                            ObEvalCtx &child_eval_ctx,
                                                            ObDASSearchCtx *&child_search_ctx)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  child_search_ctx = nullptr;
  if (OB_ISNULL(src_search_ctx_->rowid_exprs_) || OB_ISNULL(src_search_ctx_->output_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("search ctx is not ready for fusion clone", KR(ret), KP(src_search_ctx_->rowid_exprs_),
             KP(src_search_ctx_->output_));
  } else if (OB_ISNULL(buf = alloc.alloc(sizeof(ObDASSearchCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc fusion child search ctx", KR(ret));
  } else if (FALSE_IT(child_search_ctx = new (buf) ObDASSearchCtx(alloc, src_search_ctx_->root_das_task_))) {
  } else if (OB_FAIL(child_search_ctx->init(*src_search_ctx_->rowid_exprs_, *src_search_ctx_->output_,
                                            src_search_ctx_->use_dynamic_pruning_))) {
    LOG_WARN("failed to init fusion child search ctx", KR(ret));
    child_search_ctx->~ObDASSearchCtx();
    child_search_ctx = nullptr;
  } else {
    child_search_ctx->eval_ctx_ = &child_eval_ctx;
    child_search_ctx->rowid_type_ = src_search_ctx_->rowid_type_;
    child_search_ctx->table_row_count_ = src_search_ctx_->table_row_count_;
    child_search_ctx->docid_range_lo_ = src_search_ctx_->docid_range_lo_;
    child_search_ctx->docid_range_hi_ = src_search_ctx_->docid_range_hi_;
    child_search_ctx->has_docid_range_ = src_search_ctx_->has_docid_range_;
  }
  return ret;
}

int ObDASFusionChildRuntime::create_parallel_iter()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_exec_ctx_) || OB_ISNULL(child_eval_ctx_) || OB_ISNULL(child_search_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child context not prepared", KR(ret), K(path_idx_), KP(child_exec_ctx_), KP(child_eval_ctx_), KP(child_search_ctx_));
  } else if (OB_ISNULL(fusion_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion ctdef is null", KR(ret), K(path_idx_));
  } else if (fusion_ctdef_->is_search_index(path_idx_)) {
    if (OB_ISNULL(child_rtdef_root_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rtdef root is null", KR(ret), K(path_idx_));
    } else if (OB_FAIL(deep_copy_rtdef_tree(child_allocator_, child_rtdef_root_, cloned_rtdef_root_))) {
      LOG_WARN("failed to deep copy rtdef tree", KR(ret), K(path_idx_));
    } else if (OB_ISNULL(cloned_rtdef_root_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cloned rtdef root is null", KR(ret), K(path_idx_));
    } else if (OB_FAIL(swizzle_child_rtdef(*child_exec_ctx_, *child_eval_ctx_, *cloned_rtdef_root_))) {
      LOG_WARN("failed to prepare cloned rtdef tree", KR(ret), K(path_idx_));
    } else {
      ObIDASSearchRtDef *search_rtdef = static_cast<ObIDASSearchRtDef *>(cloned_rtdef_root_);
      ObDASSearchDriverIter *search_iter = nullptr;
      common::ObLimitParam top_k_limit_param;
      top_k_limit_param.limit_ = range_top_k_limit_;
      if (OB_FAIL(ObDASIterUtils::create_search_driver_iter(
              child_allocator_, search_rtdef, child_search_ctx_,
              top_k_limit_param, search_iter, score_expr_))) {
        LOG_WARN("failed to create search driver iter",
            KR(ret), K(path_idx_), K(docid_range_lo_), K(docid_range_hi_));
      } else if (OB_ISNULL(search_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("search iter is null after create", KR(ret), K(path_idx_));
      } else {
        child_iter_ = search_iter;
        owns_child_iter_ = true;

        LOG_TRACE("[FUSION_TRACE] created parallel search driver iter",
            K(path_idx_), K(is_range_parallel_), K(docid_range_lo_), K(docid_range_hi_), KP(search_iter));
      }
    }
  } else if (fusion_ctdef_->is_vector_index(path_idx_)) {
    ObDASRelatedTabletID saved_tablet_ids(child_allocator_);
    ObDASVecIndexDriverIter *orig_vec_driver = nullptr;
    if (OB_ISNULL(child_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child iter is null for vec index", KR(ret), K(path_idx_));
    } else if (FALSE_IT(orig_vec_driver = static_cast<ObDASVecIndexDriverIter *>(child_iter_))) {
    } else if (FALSE_IT(orig_vec_driver->fill_related_tablet_ids(saved_tablet_ids))) {
    } else if (OB_ISNULL(child_rtdef_root_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rtdef root is null", KR(ret), K(path_idx_));
    } else if (OB_FAIL(deep_copy_rtdef_tree(child_allocator_, child_rtdef_root_, cloned_rtdef_root_))) {
      LOG_WARN("failed to deep copy rtdef tree", KR(ret), K(path_idx_));
    } else if (OB_ISNULL(cloned_rtdef_root_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cloned rtdef root is null", KR(ret), K(path_idx_));
    } else if (OB_FAIL(swizzle_child_rtdef(*child_exec_ctx_, *child_eval_ctx_, *cloned_rtdef_root_))) {
      LOG_WARN("failed to prepare cloned rtdef tree", KR(ret), K(path_idx_));
    } else {
      const ObDASVecIndexDriverCtDef *vec_ctdef =
          static_cast<const ObDASVecIndexDriverCtDef *>(fusion_ctdef_->children_[path_idx_]);
      ObDASVecIndexDriverRtDef *vec_rtdef =
          static_cast<ObDASVecIndexDriverRtDef *>(cloned_rtdef_root_);
      ObDASIter *vec_iter = nullptr;
      if (OB_ISNULL(vec_ctdef) || OB_ISNULL(vec_rtdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("vec ctdef or rtdef is null", KR(ret), K(path_idx_), KP(vec_ctdef), KP(vec_rtdef));
      } else if (OB_FAIL(ObDASIterUtils::create_vec_search_iter(
              child_allocator_, child_search_ctx_,
              child_search_ctx_->ls_id_, child_search_ctx_->tx_desc_, child_search_ctx_->snapshot_,
              score_expr_, vec_ctdef, vec_rtdef, vec_iter))) {
        LOG_WARN("failed to create vec search iter", KR(ret), K(path_idx_));
      } else if (OB_ISNULL(vec_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("vec iter is null after create", KR(ret), K(path_idx_));
      } else {
        // Propagate tablet IDs from original iter to the new one
        ObDASVecIndexDriverIter *new_vec_driver = static_cast<ObDASVecIndexDriverIter *>(vec_iter);
        new_vec_driver->set_related_tablet_ids(saved_tablet_ids);
        ObDASVecIndexScanIter *new_vec_scan = new_vec_driver->get_vec_index_scan_iter();
        if (OB_NOT_NULL(new_vec_scan)) {
          new_vec_scan->set_related_tablet_ids(saved_tablet_ids);
        }
        child_iter_ = vec_iter;
        owns_child_iter_ = true;

        LOG_TRACE("[FUSION_TRACE] created parallel vec index driver iter",
            K(path_idx_), KP(vec_iter));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown fusion child type", KR(ret), K(path_idx_), KP(fusion_ctdef_));
  }
  return ret;
}

int ObDASFusionChildRuntime::deep_copy_rtdef_tree(common::ObIAllocator &alloc,
                                                  ObDASBaseRtDef *src,
                                                  ObDASBaseRtDef *&dst)
{
  int ret = OB_SUCCESS;
  dst = nullptr;
  if (OB_ISNULL(src)) {
    // null subtree, nothing to copy
  } else if (OB_FAIL(ObDASTaskFactory::create_das_rtdef(src->op_type_, alloc, dst))) {
    LOG_WARN("failed to create rtdef node for deep copy", KR(ret), K(src->op_type_));
  } else if (OB_ISNULL(dst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("created rtdef is null", KR(ret), K(src->op_type_));
  } else {
    int64_t ser_size = src->get_serialize_size();
    if (ser_size > 0) {
      char *ser_buf = nullptr;
      int64_t pos = 0;
      if (OB_ISNULL(ser_buf = static_cast<char *>(alloc.alloc(ser_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc rtdef serialize buf", KR(ret), K(ser_size), K(src->op_type_));
      } else if (OB_FAIL(src->serialize(ser_buf, ser_size, pos))) {
        LOG_WARN("failed to serialize rtdef for deep copy", KR(ret), K(src->op_type_));
      } else {
        pos = 0;
        if (OB_FAIL(dst->deserialize(ser_buf, ser_size, pos))) {
          LOG_WARN("failed to deserialize rtdef for deep copy", KR(ret), K(src->op_type_));
        }
      }
    }
    if (OB_SUCC(ret)) {
      dst->ctdef_ = src->ctdef_;
      dst->eval_ctx_ = src->eval_ctx_;
      dst->table_loc_ = src->table_loc_;
      dst->op_type_ = src->op_type_;
    }
    if (OB_SUCC(ret) && src->children_cnt_ > 0 && OB_NOT_NULL(src->children_)) {
      void *buf = alloc.alloc(sizeof(ObDASBaseRtDef *) * src->children_cnt_);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc rtdef children array", KR(ret), K(src->children_cnt_));
      } else {
        dst->children_ = static_cast<ObDASBaseRtDef **>(buf);
        dst->children_cnt_ = src->children_cnt_;
        MEMSET(dst->children_, 0, sizeof(ObDASBaseRtDef *) * src->children_cnt_);
        for (int64_t i = 0; OB_SUCC(ret) && i < src->children_cnt_; ++i) {
          if (OB_FAIL(SMART_CALL(deep_copy_rtdef_tree(alloc, src->children_[i], dst->children_[i])))) {
            LOG_WARN("failed to deep copy rtdef child", KR(ret), K(i), K(src->op_type_));
          }
        }
      }
    }
  }
  return ret;
}

void ObDASFusionChildRuntime::destroy_rtdef_tree(ObDASBaseRtDef *rtdef)
{
  if (OB_NOT_NULL(rtdef)) {
    for (uint32_t i = 0; i < rtdef->children_cnt_; ++i) {
      destroy_rtdef_tree(rtdef->children_[i]);
    }
    rtdef->~ObDASBaseRtDef();
  }
}

int ObDASFusionChildRuntime::swizzle_child_rtdef(ObExecContext &child_exec_ctx,
                                                 ObEvalCtx &child_eval_ctx,
                                                 ObDASBaseRtDef &child_rtdef)
{
  int ret = OB_SUCCESS;
  child_rtdef.eval_ctx_ = &child_eval_ctx;
  if (DAS_OP_TABLE_SCAN == child_rtdef.op_type_ || DAS_OP_TABLE_BATCH_SCAN == child_rtdef.op_type_) {
    ObDASScanRtDef &scan_rtdef = static_cast<ObDASScanRtDef &>(child_rtdef);
    const ObDASScanCtDef *scan_ctdef = static_cast<const ObDASScanCtDef *>(scan_rtdef.ctdef_);
    scan_rtdef.p_pd_expr_op_ = nullptr;
    scan_rtdef.p_row2exprs_projector_ = nullptr;
    scan_rtdef.stmt_allocator_.set_alloc(&child_exec_ctx.get_allocator());
    scan_rtdef.scan_allocator_.set_alloc(&child_exec_ctx.get_allocator());
    if (OB_ISNULL(scan_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scan ctdef is null", KR(ret), KP(scan_ctdef));
    } else if (OB_FAIL(scan_rtdef.init_pd_op(child_exec_ctx, *scan_ctdef))) {
      LOG_WARN("failed to init scan pd op for fusion child", KR(ret), K(scan_rtdef.op_type_));
    }
  } else if (DAS_OP_SCALAR_SCAN_QUERY == child_rtdef.op_type_) {
    ObDASScalarScanRtDef &scalar_rtdef = static_cast<ObDASScalarScanRtDef &>(child_rtdef);
    const ObDASScalarScanCtDef *scalar_ctdef = static_cast<const ObDASScalarScanCtDef *>(scalar_rtdef.ctdef_);
    scalar_rtdef.p_pd_expr_op_ = nullptr;
    scalar_rtdef.p_row2exprs_projector_ = nullptr;
    scalar_rtdef.stmt_allocator_.set_alloc(&child_exec_ctx.get_allocator());
    scalar_rtdef.scan_allocator_.set_alloc(&child_exec_ctx.get_allocator());
    if (OB_ISNULL(scalar_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scalar scan ctdef is null", KR(ret), KP(scalar_ctdef));
    } else if (OB_FAIL(scalar_rtdef.init_pd_op(child_exec_ctx, *scalar_ctdef))) {
      LOG_WARN("failed to init scalar scan pd op for fusion child", KR(ret), K(scalar_rtdef.op_type_));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_rtdef.children_cnt_; ++i) {
    if (OB_ISNULL(child_rtdef.children_) || OB_ISNULL(child_rtdef.children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rtdef child is null", KR(ret), K(i), KP(child_rtdef.children_));
    } else if (OB_FAIL(SMART_CALL(swizzle_child_rtdef(child_exec_ctx, child_eval_ctx, *child_rtdef.children_[i])))) {
      LOG_WARN("failed to prepare fusion child rtdef", KR(ret), K(i), K(child_rtdef.op_type_));
    }
  }
  return ret;
}

int ObDASFusionChildRuntime::build_assigned_bitmaps(int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  if (assigned_bitmap_slots_.count() == 0) {
    // nothing to build
  } else {
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < assigned_bitmap_slots_.count(); ++i) {
      ObSharedBitmapSlot *slot = assigned_bitmap_slots_.at(i);
      ObDASBitmapOp *bitmap_op = nullptr;
      if (timeout_ts != INT64_MAX && common::ObTimeUtility::current_time() > timeout_ts) {
        ret = OB_TIMEOUT;
        LOG_WARN("build_assigned_bitmaps timeout", KR(ret), K(timeout_ts), K(i),
                 K(assigned_bitmap_slots_.count()));
      } else if (OB_ISNULL(slot)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("slot is null, thread may hang", KR(ret), K(i));
      } else {
        if (slot->bitmap_occurrence_idx_ < 0 || slot->bitmap_occurrence_idx_ >= child_bitmap_ops_.count() ||
            OB_ISNULL(bitmap_op = child_bitmap_ops_.at(slot->bitmap_occurrence_idx_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("bitmap op not found by occurrence idx", KR(ret), K(slot->bitmap_occurrence_idx_));
        } else if (OB_FAIL(bitmap_op->build_shared_bitmap(slot->bitmap_alloc_, slot->bitmap_))) {
          LOG_WARN("failed to build shared bitmap", KR(ret), K(i), K(slot->bitmap_occurrence_idx_));
        }
        ATOMIC_STORE(&slot->build_ret_, ret);
        ATOMIC_STORE(&slot->is_built_, true);
        parallel_ctx_->get_bitmap_barrier().on_bitmap_built(ret);
        LOG_TRACE("[FUSION_TRACE] shared bitmap built", KR(ret), K(slot->bitmap_occurrence_idx_),
                  K(slot->bitmap_ != nullptr ? slot->bitmap_->cardinality() : 0));

      }
    }
    // On early failure, signal remaining assigned slots to avoid barrier deadlock.
    for (; i < assigned_bitmap_slots_.count(); ++i) {
      ObSharedBitmapSlot *slot = assigned_bitmap_slots_.at(i);
      if (OB_ISNULL(slot)) {
        // ignore ret
        LOG_ERROR("slot is null, thread may hang", KR(ret), K(i));
      } else {
        ATOMIC_STORE(&slot->build_ret_, ret);
        ATOMIC_STORE(&slot->is_built_, true);
        parallel_ctx_->get_bitmap_barrier().on_bitmap_built(ret);
        LOG_WARN("[FUSION_TRACE] shared bitmap skipped due to earlier failure", KR(ret), K(slot->bitmap_occurrence_idx_), K(i));
      }
    }
  }
  return ret;
}

int ObDASFusionChildRuntime::wait_all_bitmaps_ready(int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parallel_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parallel ctx is null", KR(ret));
  } else if (!parallel_ctx_->has_shared_bitmaps()) {
    // nothing to wait
  } else if (OB_FAIL(parallel_ctx_->get_bitmap_barrier().wait_all_built(timeout_ts))) {
    LOG_WARN("failed to wait shared bitmaps", KR(ret));
  }
  return ret;
}

int ObDASFusionChildRuntime::inject_shared_bitmaps()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parallel_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parallel ctx is null", KR(ret));
  } else if (!parallel_ctx_->has_shared_bitmaps()) {
    // nothing to inject
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parallel_ctx_->get_shared_bitmap_count(); ++i) {
      ObSharedBitmapSlot *slot = parallel_ctx_->get_shared_bitmap_slot(i);
      ObDASBitmapOp *bitmap_op = nullptr;
      if (OB_ISNULL(slot)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shared bitmap slot is null after barrier", KR(ret), K(i));
      } else if (OB_SUCCESS != ATOMIC_LOAD(&slot->build_ret_)) {
        ret = ATOMIC_LOAD(&slot->build_ret_);
        LOG_WARN("shared bitmap build failed", KR(ret), K(i), K(slot->bitmap_occurrence_idx_));
      } else if (OB_ISNULL(slot->bitmap_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shared bitmap is null despite successful build", KR(ret), K(i));
      } else if (slot->bitmap_occurrence_idx_ < 0
                 || slot->bitmap_occurrence_idx_ >= child_bitmap_ops_.count()
                 || OB_ISNULL(bitmap_op = child_bitmap_ops_.at(slot->bitmap_occurrence_idx_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bitmap op not found for injection", KR(ret), K(slot->bitmap_occurrence_idx_));
      } else if (OB_FAIL(bitmap_op->set_external_bitmap(slot->bitmap_))) {
        LOG_WARN("failed to inject shared bitmap", KR(ret), K(slot->bitmap_occurrence_idx_));
      } else {
        LOG_TRACE("[FUSION_TRACE] shared bitmap injected", K(slot->bitmap_occurrence_idx_), K(slot->bitmap_->cardinality()));
      }
    }
  }
  return ret;
}

// Signal any bitmap slots assigned to this runtime that were not yet built
// (e.g. because prepare_parallel_resources failed before reaching the bitmap
// phase). Without this, other workers would hang in wait_all_bitmaps_ready().
void ObDASFusionChildRuntime::compensate_bitmap_slots(int err_code)
{
  if (is_range_parallel_ && OB_NOT_NULL(parallel_ctx_) && parallel_ctx_->has_shared_bitmaps()) {
    for (int64_t i = 0; i < assigned_bitmap_slots_.count(); ++i) {
      ObSharedBitmapSlot *slot = assigned_bitmap_slots_.at(i);
      if (OB_NOT_NULL(slot) && !ATOMIC_LOAD(&slot->is_built_)) {
        ATOMIC_STORE(&slot->build_ret_, err_code);
        ATOMIC_STORE(&slot->is_built_, true);
        parallel_ctx_->get_bitmap_barrier().on_bitmap_built(err_code);
      }
    }
  }
}

int ObDASFusionChildRuntime::drain_child_iter(ObDASFusionParallelCoordinator &coordinator)
{
  int ret = OB_SUCCESS;
  ObDASIter *child_iter = child_iter_;
  if (OB_ISNULL(child_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child iter is null", KR(ret), K(path_idx_));
  } else {
    LOG_TRACE("[FUSION_TRACE] fusion child drain begin",
        KR(ret), K(path_idx_), KP(child_iter), KP(child_eval_ctx_),
        KP(child_search_ctx_), KP(rowkey_expr_), KP(score_expr_),
        K(max_batch_size_), K(use_rescan_));
    bool is_drained = false;
    while (OB_SUCC(ret) && !is_drained) {
      if (OB_SUCCESS != coordinator.get_first_error()) {
        ret = OB_CANCELED;
        LOG_WARN("fusion child cancelled by first error", KR(ret), K(path_idx_), K(coordinator.get_first_error()));
      } else if (THIS_WORKER.is_timeout()) {
        ret = OB_TIMEOUT;
        LOG_WARN("fusion child timeout", KR(ret), K(path_idx_));
      } else {
        child_iter->clear_evaluated_flag();
        int64_t read_count = 0;
        if (OB_FAIL(child_iter->get_next_rows(read_count, max_batch_size_))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next batch from fusion child", KR(ret), K(path_idx_));
          } else if (read_count > 0) {
            ret = OB_SUCCESS;
            if (OB_FAIL(materialize_batch_result(read_count))) {
              LOG_WARN("failed to materialize last fusion child batch",
                  KR(ret), K(path_idx_), K(read_count));
            } else {
              is_drained = true;
            }
          } else {
            ret = OB_SUCCESS;
            is_drained = true;
          }
        } else if (read_count > 0 && OB_FAIL(materialize_batch_result(read_count))) {
          LOG_WARN("failed to materialize fusion child batch",
              KR(ret), K(path_idx_), K(read_count));
        }
      }
    }
  }
  return ret;
}

int ObDASFusionChildRuntime::materialize_batch_result(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObEvalCtx *eval_ctx = child_eval_ctx_;
  ObExpr *rowkey_expr = rowkey_expr_;
  ObExpr *score_expr = score_expr_;
  if (OB_ISNULL(eval_ctx) || OB_ISNULL(rowkey_expr) || OB_ISNULL(score_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr when materializing fusion child batch",
        KR(ret), KP(eval_ctx), KP(rowkey_expr), KP(score_expr), K(path_idx_));
  } else if (batch_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected batch size when materializing fusion child batch", KR(ret), K(path_idx_), K(batch_size));
  } else if (score_expr->enable_rich_format() && is_valid_format(score_expr->get_format(*eval_ctx))) {
    ObIVector *rowkey_vec = rowkey_expr->get_vector(*eval_ctx);
    ObIVector *score_vec = score_expr->get_vector(*eval_ctx);
    if (OB_ISNULL(rowkey_vec) || OB_ISNULL(score_vec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null vector when materializing fusion child batch",
          KR(ret), KP(rowkey_vec), KP(score_vec), K(path_idx_), K(batch_size));
    } else {
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_size; ++row_idx) {
        if (OB_FAIL(rows_.push_back(
                ObDASFusionMaterializedRow(rowkey_vec->get_uint(row_idx),
                                           score_vec->get_double(row_idx))))) {
          LOG_WARN("failed to append fusion child materialized row",
              KR(ret), K(path_idx_), K(row_idx), K(batch_size));
        }
      }
    }
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_guard(*eval_ctx);
    batch_guard.set_batch_size(batch_size);
    ObDatum *rowkey_datums = rowkey_expr->locate_batch_datums(*eval_ctx);
    ObDatum *score_datums = score_expr->locate_batch_datums(*eval_ctx);
    if (OB_ISNULL(rowkey_datums) || OB_ISNULL(score_datums)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null datum batch when materializing fusion child batch",
          KR(ret), KP(rowkey_datums), KP(score_datums), K(path_idx_), K(batch_size));
    } else {
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_size; ++row_idx) {
        batch_guard.set_batch_idx(row_idx);
        if (OB_FAIL(rows_.push_back(
                ObDASFusionMaterializedRow(rowkey_datums[row_idx].get_uint64(),
                                           score_datums[row_idx].get_double())))) {
          LOG_WARN("failed to append fusion child datum row",
              KR(ret), K(path_idx_), K(row_idx), K(batch_size));
        }
      }
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// ObDASFusionParallelCtx
// ---------------------------------------------------------------------------
int ObDASFusionParallelCtx::init(common::ObIAllocator &alloc,
                                 const ObDASFusionCtDef &fusion_ctdef,
                                 ObDASFusionRtDef &fusion_rtdef,
                                 ObDASSearchCtx &search_ctx,
                                 ObExecContext &exec_ctx,
                                 ObEvalCtx &eval_ctx,
                                 ObDASIter **children,
                                 const int64_t children_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(children_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fusion children count", KR(ret), K(children_cnt));
  } else if (OB_ISNULL(fusion_rtdef.children_) || OB_ISNULL(children)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion rtdef children or iter children is null", KR(ret), KP(fusion_rtdef.children_), KP(children));
  } else {
    // Pre-serialize exec_ctx ONCE in the main thread to avoid a data race.
    // ObExecContext::serialize() and get_serialize_size() both call
    // my_session_->add_changed_package_info() / reset_all_package_changed_info()
    // which modify session state and are NOT thread-safe for concurrent calls
    // from multiple worker threads. After the serial left side of EXCEPT runs,
    // the session has non-trivial package state, making these mutations
    // observable and causing heap corruption when called from 3+ concurrent workers.
    {
      const int64_t ser_size = exec_ctx.get_serialize_size();
      char *ser_buf = nullptr;
      if (OB_UNLIKELY(ser_size <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected exec ctx serialize size", KR(ret), K(ser_size));
      } else if (OB_ISNULL(ser_buf = static_cast<char *>(alloc.alloc(ser_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc exec ctx pre-serialize buf", KR(ret), K(ser_size));
      } else {
        int64_t ser_pos = 0;
        if (OB_FAIL(exec_ctx.serialize(ser_buf, ser_size, ser_pos))) {
          LOG_WARN("failed to pre-serialize exec ctx for parallel fusion", KR(ret), K(ser_size));
        } else {
          exec_ctx_ser_buf_ = ser_buf;
          exec_ctx_ser_size_ = ser_pos;
          LOG_DEBUG("[FUSION_TRACE] pre-serialized exec ctx", K(ser_size), K(ser_pos));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt; ++i) {
      const int64_t child_dop = fusion_ctdef.is_search_index(i) ? fusion_ctdef.query_dop_ : 1;
      if (OB_FAIL(create_child_runtime(alloc, i, child_dop, fusion_ctdef,
                                        fusion_rtdef.children_[i], search_ctx,
                                        exec_ctx, eval_ctx, children[i]))) {
        LOG_WARN("failed to create child runtime", KR(ret), K(i), K(child_dop));
      }
    }
    if (OB_FAIL(ret)) {
      release();
    }
  }
  LOG_INFO("[FUSION_TRACE] init parallel ctx", KR(ret), K(children_cnt));
  return ret;
}

void ObSharedBitmapSlot::release()
{
  if (OB_NOT_NULL(bitmap_)) {
    bitmap_->~ObFastBitmap();
    bitmap_ = nullptr;
  }
  bitmap_alloc_.reset();
  is_built_ = false;
  build_ret_ = common::OB_SUCCESS;
}

void ObDASFusionParallelCtx::release()
{
  for (int64_t i = 0; i < child_runtimes_.count(); ++i) {
    ObDASFusionChildRuntime *runtime = child_runtimes_.at(i);
    if (OB_NOT_NULL(runtime)) {
      runtime->~ObDASFusionChildRuntime();
    }
  }
  child_runtimes_.reset();
  // Release shared bitmap slots (each holds bitmap data via its own allocator).
  for (int64_t i = 0; i < shared_bitmap_slots_.count(); ++i) {
    ObSharedBitmapSlot *slot = shared_bitmap_slots_.at(i);
    if (OB_NOT_NULL(slot)) {
      slot->release();
      slot->~ObSharedBitmapSlot();
    }
  }
  shared_bitmap_slots_.reset();
}

int ObDASFusionParallelCtx::create_child_runtime(
    common::ObIAllocator &alloc,
    const int64_t path_idx,
    const int64_t child_dop,
    const ObDASFusionCtDef &fusion_ctdef,
    ObDASBaseRtDef *child_rtdef,
    ObDASSearchCtx &search_ctx,
    ObExecContext &exec_ctx,
    ObEvalCtx &eval_ctx,
    ObDASIter *child_iter)
{
  int ret = OB_SUCCESS;
  int64_t actual_dop = 1;
  common::ObSEArray<common::ObObj, 8> split_lo;
  common::ObSEArray<common::ObObj, 8> split_hi;
  int64_t range_top_k_limit = -1;
  // Step 1: For search index with dop > 1, try computing split points to determine actual_dop.
  // If split yields <= 1 range (e.g. empty buckets from storage), fall back to normal path.
  if (fusion_ctdef.is_search_index(path_idx) && child_dop > 1) {
    if (OB_ISNULL(child_iter)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("child iter is null for range parallel", KR(ret), K(path_idx));
    } else if (FALSE_IT(range_top_k_limit =
        static_cast<ObDASSearchDriverIter *>(child_iter)->get_top_k_limit())) {
    } else if (OB_FAIL(compute_docid_split_points(search_ctx, alloc, child_dop, split_lo, split_hi))) {
      LOG_WARN("compute_docid_split_points failed", KR(ret), K(child_dop));
    } else if (OB_UNLIKELY(split_lo.count() != split_hi.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("split lo and hi count mismatch", KR(ret), K(split_lo.count()), K(split_hi.count()));
    } else {
      actual_dop = split_lo.count();
      LOG_TRACE("[FUSION_TRACE] range split computed", K(path_idx), K(child_dop), K(actual_dop));
    }
  }
  // Step 2: Based on actual_dop, choose range-parallel or normal path.
  if (OB_FAIL(ret)) {
  } else if (fusion_ctdef.is_search_index(path_idx) && actual_dop > 1) {
    // Range-parallel: create one runtime per sub-range.
    if (OB_FAIL(discover_shared_bitmaps(alloc, child_iter, actual_dop))) {
      LOG_WARN("failed to discover shared bitmaps", KR(ret), K(path_idx));
    }
    for (int64_t r = 0; OB_SUCC(ret) && r < actual_dop; ++r) {
      ObDASFusionChildRuntime *runtime = nullptr;
      if (OB_FAIL(alloc_and_init_runtime(alloc, path_idx, fusion_ctdef, child_rtdef, exec_ctx, eval_ctx, search_ctx,
                                         runtime))) {
        LOG_WARN("failed to alloc and init range parallel runtime", KR(ret), K(path_idx), K(r));
      } else {
        runtime->is_range_parallel_ = true;
        runtime->docid_range_lo_ = split_lo.at(r);
        runtime->docid_range_hi_ = split_hi.at(r);
        runtime->range_top_k_limit_ = range_top_k_limit;
        runtime->child_iter_ = child_iter;
        // Round-robin assign shared bitmap slots to this runtime.
        for (int64_t s = r; OB_SUCC(ret) && s < shared_bitmap_slots_.count(); s += actual_dop) {
          if (OB_FAIL(runtime->assigned_bitmap_slots_.push_back(shared_bitmap_slots_.at(s)))) {
            LOG_WARN("failed to assign bitmap slot to runtime", KR(ret), K(r), K(s));
          }
        }
      }
    }
  } else {
    // Normal: one runtime per child (vector index, or search index with dop<=1 / split fallback).
    ObDASFusionChildRuntime *runtime = nullptr;
    if (OB_FAIL(alloc_and_init_runtime(alloc, path_idx, fusion_ctdef, child_rtdef,
                                        exec_ctx, eval_ctx, search_ctx, runtime))) {
      LOG_WARN("failed to alloc and init fusion child runtime", KR(ret), K(path_idx));
    } else {
      runtime->child_iter_ = child_iter;
    }
  }
  return ret;
}

int ObDASFusionParallelCtx::alloc_and_init_runtime(
    common::ObIAllocator &alloc,
    const int64_t path_idx,
    const ObDASFusionCtDef &fusion_ctdef,
    ObDASBaseRtDef *child_rtdef,
    ObExecContext &exec_ctx,
    ObEvalCtx &eval_ctx,
    ObDASSearchCtx &search_ctx,
    ObDASFusionChildRuntime *&runtime)
{
  int ret = OB_SUCCESS;
  runtime = nullptr;
  void *runtime_buf = nullptr;
  if (OB_ISNULL(child_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child rtdef is null", KR(ret), K(path_idx));
  } else if (OB_ISNULL(runtime_buf = alloc.alloc(sizeof(ObDASFusionChildRuntime)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc fusion child runtime", KR(ret), K(path_idx));
  } else if (FALSE_IT(runtime = new (runtime_buf) ObDASFusionChildRuntime())) {
  } else if (OB_FAIL(runtime->init(path_idx, fusion_ctdef, exec_ctx, eval_ctx, search_ctx,
                                   child_rtdef))) {
    LOG_WARN("failed to init fusion child runtime", KR(ret), K(path_idx));
  } else if (OB_FAIL(child_runtimes_.push_back(runtime))) {
    LOG_WARN("failed to save fusion child runtime", KR(ret), K(path_idx));
  } else {
    runtime->parallel_ctx_ = this;
    // Share the pre-serialized exec ctx buffer (set in init(), read-only in workers)
    runtime->preser_exec_ctx_buf_ = exec_ctx_ser_buf_;
    runtime->preser_exec_ctx_size_ = exec_ctx_ser_size_;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(runtime)) {
    runtime->~ObDASFusionChildRuntime();
    runtime = nullptr;
  }
  LOG_TRACE("[FUSION_TRACE] alloc and init fusion child runtime", KR(ret), K(path_idx));
  return ret;
}

int ObDASFusionParallelCtx::discover_shared_bitmaps(
    common::ObIAllocator &alloc,
    ObDASIter *search_child_iter, int64_t actual_dop)
{
  int ret = OB_SUCCESS;
  if (actual_dop <= 1 || OB_ISNULL(search_child_iter)) {
    // No range parallel or invalid args — nothing to discover.
  } else {
    // Walk the main-thread SearchDriverIter's op tree to find BitmapOps.
    ObDASSearchDriverIter *search_iter =
        static_cast<ObDASSearchDriverIter *>(search_child_iter);
    ObIDASSearchOp *root_op = search_iter->get_root_op();
    if (OB_ISNULL(root_op)) {
      LOG_TRACE("[FUSION_TRACE] discover_shared_bitmaps: root_op is null, skip");
    } else {
      common::ObSEArray<ObDASBitmapOp *, 4> serial_bitmap_ops;
      LOG_TRACE("[FUSION_TRACE] discover_shared_bitmaps: DFS walk start",
               KP(root_op), K(root_op->get_op_type()), K(root_op->get_children_cnt()));
      if (OB_FAIL(collect_bitmap_ops_dfs(root_op, serial_bitmap_ops))) {
        LOG_WARN("failed to discover bitmap ops", KR(ret));
      } else {
        LOG_TRACE("[FUSION_TRACE] discover_shared_bitmaps: DFS walk done",
                 "bitmap_count", serial_bitmap_ops.count());
      }
      if (OB_SUCC(ret) && serial_bitmap_ops.count() > 0) {
        // Create SharedBitmapSlots. Assignment to runtimes is done in the caller's DOP loop.
        for (int64_t i = 0; OB_SUCC(ret) && i < serial_bitmap_ops.count(); ++i) {
          ObSharedBitmapSlot *slot = nullptr;
          void *slot_buf = nullptr;
          if (OB_ISNULL(slot_buf = alloc.alloc(sizeof(ObSharedBitmapSlot)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate shared bitmap slot", KR(ret), K(i));
          } else if (FALSE_IT(slot = new (slot_buf) ObSharedBitmapSlot())) {
          } else {
            slot->bitmap_occurrence_idx_ = i;
            if (OB_FAIL(shared_bitmap_slots_.push_back(slot))) {
              LOG_WARN("failed to push shared bitmap slot", KR(ret), K(i));
              slot->release();
              slot->~ObSharedBitmapSlot();
            }
          }
        }
        if (OB_SUCC(ret) && shared_bitmap_slots_.count() > 0) {
          if (OB_FAIL(bitmap_barrier_.init(shared_bitmap_slots_.count()))) {
            LOG_WARN("failed to init bitmap barrier", KR(ret), K(shared_bitmap_slots_.count()));
          } else {
            LOG_TRACE("[FUSION_TRACE] discovered shared bitmaps",
                     K(shared_bitmap_slots_.count()), K(actual_dop));
          }
        }
      }
    }
  }
  return ret;
}

int ObDASFusionParallelCtx::compute_docid_split_points(
    ObDASSearchCtx &search_ctx,
    common::ObIAllocator &alloc,
    const int64_t range_dop,
    common::ObIArray<common::ObObj> &split_lo,
    common::ObIArray<common::ObObj> &split_hi)
{
  int ret = OB_SUCCESS;
  ObAccessService *access_service = MTL(ObAccessService *);
  const share::ObLSID &ls_id = search_ctx.get_ls_id();
  const common::ObTabletID &tablet_id = search_ctx.get_root_tablet_id();
  const int64_t timeout_us = THIS_WORKER.get_timeout_remain();
  common::ObStoreRange whole_range;
  whole_range.set_whole_range();
  common::ObSEArray<common::ObStoreRange, 1> input_ranges;
  common::ObArrayArray<common::ObStoreRange> split_result;
  if (OB_ISNULL(access_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access service is null", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid()) || OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls_id or tablet_id for range split", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(input_ranges.push_back(whole_range))) {
    LOG_WARN("failed to push whole range", KR(ret));
  } else if (OB_FAIL(access_service->split_multi_ranges(
                 ls_id, tablet_id, timeout_us,
                 input_ranges, range_dop, alloc, split_result))) {
    LOG_WARN("storage split_multi_ranges failed", KR(ret), K(ls_id), K(tablet_id), K(range_dop));
  } else {
    const int64_t actual_cnt = split_result.count();
    LOG_TRACE("[FUSION_TRACE] storage split_multi_ranges returned", K(ls_id), K(tablet_id), K(range_dop), K(actual_cnt));
    for (int64_t i = 0; OB_SUCC(ret) && i < actual_cnt; ++i) {
      const common::ObIArray<common::ObStoreRange> &task_ranges = split_result.at(i);
      if (task_ranges.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty task ranges", KR(ret), K(i));
      } else {
        common::ObObj lo;
        common::ObObj hi;
        const common::ObStoreRange &first = task_ranges.at(0);
        const common::ObStoreRange &last = task_ranges.at(task_ranges.count() - 1);
        // Preserve MIN/MAX from storage range boundaries directly.
        if (first.get_start_key().is_min()) {
          lo.set_min_value();
        } else if (first.get_start_key().get_obj_cnt() > 0
                   && OB_NOT_NULL(first.get_start_key().get_obj_ptr())) {
          lo = first.get_start_key().get_obj_ptr()[0];
          if (OB_UNLIKELY(!lo.is_uint64())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expected uint64 docid in split start key", KR(ret), K(lo), K(i));
          } else if (!first.get_border_flag().inclusive_start()) {
            const uint64_t v = lo.get_uint64();
            if (OB_UNLIKELY(v == UINT64_MAX)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("exclusive start at UINT64_MAX would cause overflow", KR(ret), K(i), K(v));
            } else {
              lo.set_uint64(v + 1);
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (last.get_end_key().is_max()) {
          hi.set_max_value();
        } else if (last.get_end_key().get_obj_cnt() > 0
                   && OB_NOT_NULL(last.get_end_key().get_obj_ptr())) {
          hi = last.get_end_key().get_obj_ptr()[0];
          if (OB_UNLIKELY(!hi.is_uint64())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expected uint64 docid in split end key", KR(ret), K(hi), K(i));
          } else if (!last.get_border_flag().inclusive_end()) {
            // exclusive end: retreat hi by 1
            const uint64_t v = hi.get_uint64();
            hi.set_uint64((v > 0) ? (v - 1) : 0);
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(split_lo.push_back(lo))) {
          LOG_WARN("failed to push split lo", KR(ret), K(i), K(lo));
        } else if (OB_FAIL(split_hi.push_back(hi))) {
          LOG_WARN("failed to push split hi", KR(ret), K(i), K(hi));
        } else {
          LOG_TRACE("[FUSION_TRACE] computed docid split range", K(i), K(task_ranges), K(lo), K(hi));
        }
      }
    }
  }
  return ret;
}

bool ObDASFusionParallelCtx::should_enable_parallel(
    const ObDASFusionCtDef &fusion_ctdef,
    const ObDASSearchCtx &search_ctx)
{
  return fusion_ctdef.enable_parallel_ &&
         (fusion_ctdef.children_cnt_ > 1 || fusion_ctdef.query_dop_ > 1) &&
         fusion_ctdef.is_top_k_query_ &&
         search_ctx.get_rowid_type() == DAS_ROWID_TYPE_UINT64;
}

// ---------------------------------------------------------------------------
// ObBitmapPhaseBarrier
// ---------------------------------------------------------------------------
int ObBitmapPhaseBarrier::init(int64_t total_cnt)
{
  int ret = OB_SUCCESS;
  if (total_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bitmap phase total count", KR(ret), K(total_cnt));
  } else if (!is_inited_
             && OB_FAIL(cond_.init(common::ObWaitEventIds::OB_DAS_FUSION_BITMAP_BARRIER_COND_WAIT))) {
    LOG_WARN("failed to init bitmap phase cond", KR(ret), K(total_cnt));
  } else {
    total_cnt_ = total_cnt;
    built_cnt_ = 0;
    first_err_ = OB_SUCCESS;
    is_inited_ = true;
  }
  return ret;
}

void ObBitmapPhaseBarrier::on_bitmap_built(int err_code)
{
  if (OB_SUCCESS != err_code) {
    (void)ATOMIC_BCAS(&first_err_, OB_SUCCESS, err_code);
  }
  common::ObThreadCondGuard guard(cond_);
  ++built_cnt_;
  if (built_cnt_ >= total_cnt_) {
    cond_.broadcast();
  }
}

int ObBitmapPhaseBarrier::wait_all_built(int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("bitmap phase barrier not inited", KR(ret));
  } else {
    common::ObThreadCondGuard guard(cond_);
    while (OB_SUCC(ret) && built_cnt_ < total_cnt_) {
      const int64_t now = common::ObTimeUtility::current_time();
      const int64_t wait_us = timeout_ts - now;
      if (wait_us <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("bitmap phase wait timeout", KR(ret), K(timeout_ts), K(total_cnt_), K(built_cnt_));
      } else if (OB_FAIL(cond_.wait_us(wait_us))) {
        LOG_WARN("failed to wait bitmap phase", KR(ret), K(total_cnt_), K(built_cnt_));
      }
    }
  }
  return OB_SUCC(ret) ? ATOMIC_LOAD(&first_err_) : ret;
}

// ---------------------------------------------------------------------------
// ObDASFusionParallelCoordinator
// ---------------------------------------------------------------------------
ObDASFusionParallelCoordinator::ObDASFusionParallelCoordinator()
  : total_cnt_(0),
    finished_cnt_(0),
    first_err_code_(OB_SUCCESS),
    cond_(),
    is_inited_(false)
{
}

int ObDASFusionParallelCoordinator::init(const int64_t total_cnt)
{
  int ret = OB_SUCCESS;
  if (total_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fusion parallel child count", KR(ret), K(total_cnt));
  } else if (!is_inited_
             && OB_FAIL(cond_.init(common::ObWaitEventIds::OB_DAS_FUSION_COORDINATOR_COND_WAIT))) {
    LOG_WARN("failed to init fusion parallel cond", KR(ret), K(total_cnt));
  } else {
    total_cnt_ = total_cnt;
    finished_cnt_ = 0;
    first_err_code_ = OB_SUCCESS;
    is_inited_ = true;
  }
  return ret;
}

void ObDASFusionParallelCoordinator::reset()
{
  // Only reset counters and error code. Intentionally keep is_inited_ = true
  // so that the cond_ (which cannot be re-initialized after first init) is
  // reused on the next init() call. init() skips cond_.init() when is_inited_
  // is already true.
  total_cnt_ = 0;
  finished_cnt_ = 0;
  first_err_code_ = OB_SUCCESS;
}

void ObDASFusionParallelCoordinator::set_first_error(const int err_code)
{
  if (OB_SUCCESS != err_code) {
    (void)ATOMIC_BCAS(&first_err_code_, OB_SUCCESS, err_code);
  }
}

void ObDASFusionParallelCoordinator::on_child_finish(const int err_code)
{
  set_first_error(err_code);
  common::ObThreadCondGuard guard(cond_);
  ++finished_cnt_;
  cond_.signal();
}

int ObDASFusionParallelCoordinator::wait_all_complete(const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("fusion parallel coordinator not init", KR(ret));
  } else {
    common::ObThreadCondGuard guard(cond_);
    while (OB_SUCC(ret) && finished_cnt_ < total_cnt_) {
      if (INT64_MAX == timeout_ts) {
        if (OB_FAIL(cond_.wait())) {
          LOG_WARN("failed to wait fusion child tasks indefinitely", KR(ret), K(total_cnt_), K(finished_cnt_));
        }
      } else {
        const int64_t now = common::ObTimeUtility::current_time();
        const int64_t wait_us = timeout_ts - now;
        if (wait_us <= 0) {
          ret = OB_TIMEOUT;
          LOG_WARN("wait fusion child tasks timeout",
              KR(ret), K(timeout_ts), K(now), K(total_cnt_), K(finished_cnt_));
        } else if (OB_FAIL(cond_.wait_us(wait_us))) {
          LOG_WARN("failed to wait fusion child tasks", KR(ret), K(total_cnt_), K(finished_cnt_));
        }
      }
    }
  }
  return OB_SUCC(ret) ? get_first_error() : ret;
}

// ---------------------------------------------------------------------------
// ObDASFusionChildTaskHandler / ObDASFusionChildTask
// ---------------------------------------------------------------------------
int ObDASFusionChildTaskHandler::init(observer::ObSrvTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fusion child task is null", KR(ret), KP(task));
  } else {
    task_ = task;
  }
  return ret;
}

int ObDASFusionChildTaskHandler::run()
{
  int ret = OB_SUCCESS;
  ObDASFusionChildTask *task = static_cast<ObDASFusionChildTask *>(task_);
  ObDASFusionChildRuntime *runtime = nullptr;
  ObDASFusionParallelCoordinator *coordinator = nullptr;
  if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion child task is null", KR(ret), KP(task));
  } else if (OB_ISNULL(runtime = task->get_runtime())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion child runtime is null", KR(ret), KP(runtime));
  } else if (OB_ISNULL(coordinator = task->get_coordinator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion child coordinator is null", KR(ret), KP(coordinator));
  } else if (OB_ISNULL(runtime->parallel_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion child parallel ctx is null", KR(ret), K(runtime->path_idx_));
  } else {
    common::ObCurTraceId::set(task->get_trace_id());
    THIS_WORKER.set_timeout_ts(task->get_timeout_ts());
    common::ObProfileSwitcher profile_guard(runtime->profile_);
    LOG_TRACE("[FUSION_TRACE] fusion child task run begin", KR(ret), K(runtime->path_idx_), KP(runtime),
             KP(runtime->child_iter_), KP(runtime->child_eval_ctx_), KP(runtime->child_search_ctx_),
             KP(runtime->rowkey_expr_), KP(runtime->score_expr_), K(runtime->max_batch_size_), K(runtime->use_rescan_));
    // Step 1: Deep copy contexts, create iter
    if (OB_FAIL(runtime->prepare_parallel_resources())) {
      LOG_WARN("failed to prepare parallel resources", KR(ret), K(runtime->path_idx_));
    } else if (OB_ISNULL(runtime->child_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child iter is null", KR(ret), K(runtime->path_idx_));
    }
    // Step 2: Shared bitmap phase (range-parallel only)
    if (OB_SUCC(ret) && runtime->is_range_parallel_
        && runtime->parallel_ctx_->has_shared_bitmaps()) {
      const int64_t timeout_ts = task->get_timeout_ts();
      if (OB_FAIL(runtime->build_assigned_bitmaps(timeout_ts))) {
        LOG_WARN("failed to build assigned bitmaps", KR(ret), K(runtime->path_idx_));
      } else if (OB_FAIL(runtime->wait_all_bitmaps_ready(timeout_ts))) {
        LOG_WARN("bitmap barrier wait failed", KR(ret), K(runtime->path_idx_));
      } else if (OB_FAIL(runtime->inject_shared_bitmaps())) {
        LOG_WARN("failed to inject shared bitmaps", KR(ret), K(runtime->path_idx_));
      }
    }
    // Step 3: Execute scan
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(runtime->child_iter_->do_table_scan())) {
      LOG_WARN("failed to do table scan on fusion child", KR(ret), K(runtime->path_idx_));
    } else if (OB_FAIL(runtime->drain_child_iter(*coordinator))) {
      LOG_WARN("failed to drain fusion child iter", KR(ret), K(runtime->path_idx_));
    }
    LOG_TRACE("[FUSION_TRACE] fusion child task run end", KR(ret), "path_idx", runtime->path_idx_, KP(runtime),
             KP(runtime->child_iter_), KP(runtime->child_eval_ctx_), KP(runtime->child_search_ctx_),
             K(runtime->rows_.count()), K(runtime->submitted_), K(runtime->finished_));
    runtime->compensate_bitmap_slots(ret);
    ATOMIC_STORE(&runtime->err_code_, ret);
    ATOMIC_STORE(&runtime->finished_, true);
    runtime->release_parallel_resources();
    // on_child_finish must be the last step, otherwise main thread may operate memory concurrently with this worker
    coordinator->on_child_finish(ret);
  }
  // Clear request-scoped thread local (e.g. lock wait placeholder) when running on worker thread.
  oceanbase::obmysql::request_finish_callback();
  return OB_SUCCESS;
}

ObDASFusionChildTask::ObDASFusionChildTask()
  : runtime_(nullptr),
    coordinator_(nullptr),
    trace_id_(),
    timeout_ts_(INT64_MAX),
    handler_()
{
}

int ObDASFusionChildTask::init(ObDASFusionChildRuntime *runtime,
                               ObDASFusionParallelCoordinator *coordinator,
                               const int64_t timeout_ts,
                               const int32_t group_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(runtime) || OB_ISNULL(coordinator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fusion child task init argument", KR(ret), KP(runtime), KP(coordinator));
  } else if (OB_FAIL(handler_.init(this))) {
    LOG_WARN("failed to init fusion child task handler", KR(ret), K(runtime->path_idx_));
  } else {
    common::ObCurTraceId::TraceId *trace_id = common::ObCurTraceId::get_trace_id();
    runtime_ = runtime;
    coordinator_ = coordinator;
    timeout_ts_ = timeout_ts;
    if (OB_NOT_NULL(trace_id)) {
      trace_id_.set(*trace_id);
    }
    set_group_id(group_id);
    set_type(ObRequest::OB_TASK);
  }
  return ret;
}

void ObDASFusionChildTask::reset()
{
  runtime_ = nullptr;
  coordinator_ = nullptr;
  timeout_ts_ = INT64_MAX;
  handler_.reset();
}

} // namespace sql
} // namespace oceanbase
