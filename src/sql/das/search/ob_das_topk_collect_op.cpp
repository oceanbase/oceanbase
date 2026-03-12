/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_DAS

#include "ob_das_topk_collect_op.h"
#include "ob_das_dummy_op.h"

namespace oceanbase
{
namespace sql
{

int ObDASTopKCollectOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(children.push_back(child_))) {
    LOG_WARN("failed to push child op", KR(ret), KPC(child_));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDASTopKCollectCtDef, ObIDASSearchCtDef),
                    limit_);

OB_SERIALIZE_MEMBER((ObDASTopKCollectRtDef, ObIDASSearchRtDef));

int ObDASTopKCollectRtDef::generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  const ObDASTopKCollectCtDef *ctdef = static_cast<const ObDASTopKCollectCtDef *>(ctdef_);
  ObDatum *limit_datum = nullptr;
  ObIDASSearchOp *child_op = nullptr;
  if (OB_UNLIKELY(children_cnt_ != 1 || OB_ISNULL(children_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected children for top k collect op", KR(ret), K(children_cnt_), KP(children_[0]));
  } else if (OB_FAIL(static_cast<ObIDASSearchRtDef *>(children_[0])->generate_op(lead_cost, search_ctx, child_op))) {
    LOG_WARN("failed to generate child op", KR(ret));
  } else if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx_) || OB_ISNULL(ctdef->limit_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", KR(ret), KPC(ctdef), KP_(eval_ctx));
  } else if (OB_FAIL(ctdef->limit_->eval(*eval_ctx_, limit_datum))) {
    LOG_WARN("expr evaluation failed", KR(ret));
  } else if (OB_UNLIKELY(limit_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum", KR(ret), KPC(limit_datum));
  } else if (0 == limit_datum->get_int()) {
    ObDASDummyOp *dummy_op = nullptr;
    ObDASDummyOpParam dummy_op_param;
    if (OB_FAIL(search_ctx.create_op(dummy_op_param, dummy_op))) {
      LOG_WARN("failed to create dummy op", KR(ret));
    } else {
      op = dummy_op;
    }
  } else {
    ObDASTopKCollectOpParam op_param;
    ObDASTopKCollectOp *topk_collect_op = nullptr;
    op_param.limit_ = limit_datum->get_int();
    op_param.child_ = child_op;
    if (OB_FAIL(search_ctx.create_op(op_param, topk_collect_op))) {
      LOG_WARN("failed to create op", KR(ret));
    } else {
      op = topk_collect_op;
    }
  }
  return ret;
}

ObDASTopKCollectOp::ObDASTopKCollectOp(ObDASSearchCtx &search_ctx)
  : ObIDASSearchOp(search_ctx),
    child_(nullptr),
    cmp_(),
    heap_(cmp_, &ctx_allocator()),
    id_cache_(),
    curr_id_(),
    limit_(-1),
    is_loaded_(false),
    is_inited_(false)
{}

int ObDASTopKCollectOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASTopKCollectOpParam &param = static_cast<const ObDASTopKCollectOpParam &>(op_param);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), K(param));
  } else if (FALSE_IT(id_cache_.set_allocator(&ctx_allocator()))) {
  } else if (OB_FAIL(id_cache_.init(param.limit_))) {
    LOG_WARN("failed to init id cache", K(ret));
  } else {
    child_ = param.child_;
    limit_ = param.limit_;
    is_inited_ = true;
  }
  return ret;
}

int ObDASTopKCollectOp::do_open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(child_->open())) {
    LOG_WARN("failed to open child op", K(ret));
  }
  return ret;
}

int ObDASTopKCollectOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(child_->rescan())) {
    LOG_WARN("failed to rescan child op", K(ret));
  }
  while (OB_SUCC(ret) && !heap_.empty()) {
    if (OB_FAIL(heap_.pop())) {
      LOG_WARN("failed to pop from heap", K(ret));
    }
  }
  id_cache_.clear();
  is_loaded_ = false;
  return ret;
}

int ObDASTopKCollectOp::do_close()
{
  int ret = OB_SUCCESS;
  heap_.reset();
  id_cache_.reset();
  is_inited_ = false;
  return ret;
}

int ObDASTopKCollectOp::load_results()
{
  int ret = OB_SUCCESS;
  ObDASRowID rowid;
  ObDatum id_datum;
  double score = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(child_->next_rowid(rowid, score))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row from child iter", K(ret));
      }
    } else if (heap_.count() < limit_) {
      int64_t cache_idx = id_cache_.count();
      if (OB_FAIL(id_cache_.push_back(ObDocIdExt()))) {
        LOG_WARN("failed to append to id cache", K(ret));
      } else if (OB_FAIL(search_ctx_.get_datum_from_rowid(rowid, id_datum))) {
        LOG_WARN("failed to get datum from rowid", K(ret));
      } else if (OB_FAIL(id_cache_[cache_idx].from_datum(id_datum))) {
        LOG_WARN("failed to cache id from datum", K(ret));
      } else if (OB_FAIL(heap_.push(ObDASTopKItem{score, cache_idx}))) {
        LOG_WARN("failed to push item onto heap", K(ret));
      }
    } else if (score > heap_.top().score_) {
      const ObDASTopKItem &top_item = heap_.top();
      int64_t cache_idx = top_item.cache_idx_;
      if (OB_FAIL(search_ctx_.get_datum_from_rowid(rowid, id_datum))) {
        LOG_WARN("failed to get datum from rowid", K(ret));
      } else if (OB_FAIL(id_cache_[cache_idx].from_datum(id_datum))) {
        LOG_WARN("failed to cache id from datum", K(ret));
      } else if (OB_FAIL(heap_.pop())) {
        LOG_WARN("failed to pop from heap", K(ret));
      } else if (OB_FAIL(heap_.push(ObDASTopKItem{score, cache_idx}))) {
        LOG_WARN("failed to push item onto heap", K(ret));
      } else if (OB_FAIL(child_->set_min_competitive_score(heap_.top().score_))) {
        LOG_WARN("failed to set min competitive score", K(ret));
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    is_loaded_ = true;
  }
  return ret;
}

int ObDASTopKCollectOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (!is_loaded_ && OB_FAIL(load_results())) {
    LOG_WARN("failed to load results", K(ret));
  } else if (heap_.empty()) {
    ret = OB_ITER_END;
  } else {
    const ObDatum &curr_id = id_cache_.at(heap_.top().cache_idx_).get_datum();
    score = heap_.top().score_;
    if (OB_FAIL(search_ctx_.write_datum_to_rowid(curr_id, curr_id_, ctx_allocator()))) {
      LOG_WARN("failed to write datum to rowid", K(ret));
    } else if (OB_FAIL(heap_.pop())) {
      LOG_WARN("failed to pop from heap", K(ret));
    } else {
      next_id = curr_id_;
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
