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

#include "ob_das_disjunctive_max_op.h"

namespace oceanbase
{
namespace sql
{

int ObDASDisjunctiveMaxOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(children_ops_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children ops is null", KR(ret));
  } else if (OB_FAIL(children.assign(*children_ops_))) {
    LOG_WARN("failed to assign children ops", KR(ret));
  }
  return ret;
}

ObDASDisjunctiveMaxOp::ObDASDisjunctiveMaxOp(ObDASSearchCtx &search_ctx)
  : ObIDASSearchOp(search_ctx),
    cmp_(),
    heap_(cmp_, &ctx_allocator()),
    map_(),
    id_cache_(),
    curr_id_(),
    limit_(-1),
    is_loaded_(false),
    is_inited_(false)
{}

int ObDASDisjunctiveMaxOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASDisjunctiveMaxOpParam &param = static_cast<const ObDASDisjunctiveMaxOpParam &>(op_param);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret));
  } else {
    limit_ = param.limit_;
    is_inited_ = true;
  }
  return ret;
}

int ObDASDisjunctiveMaxOp::do_open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(map_.create(
      children_cnt_ * limit_,
      common::ObMemAttr(MTL_ID(), "FTTopKMap")))) {
    LOG_WARN("failed to create map", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_FAIL(children_[i]->open())) {
      LOG_WARN("failed to open child op", K(ret));
    }
  }
  return ret;
}

int ObDASDisjunctiveMaxOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_FAIL(children_[i]->rescan())) {
      LOG_WARN("failed to rescan child op", K(ret));
    }
  }
  while (OB_SUCC(ret) && !heap_.empty()) {
    if (OB_FAIL(heap_.pop())) {
      LOG_WARN("failed to pop from heap", K(ret));
    }
  }
  if (FAILEDx(map_.clear())) {
    LOG_WARN("failed to clear map", K(ret));
  }
  id_cache_.reset();
  is_loaded_ = false;
  return ret;
}

int ObDASDisjunctiveMaxOp::do_close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(map_.destroy())) {
    LOG_WARN("failed to destroy map", K(ret));
  }
  heap_.reset();
  id_cache_.reset();
  is_inited_ = false;
  return ret;
}

int ObDASDisjunctiveMaxOp::load_results()
{
  int ret = OB_SUCCESS;
  ObDASRowID rowid;
  double score = 0;
  ObDatum id_datum;
  ObDocIdExt curr_id;
  ObDASTopKItem prev_item;
  if (OB_UNLIKELY(!heap_.empty() || !map_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-empty heap or map", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(children_[i]->next_rowid(rowid, score))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next row of child iter", K(ret));
        }
      } else if (OB_FAIL(search_ctx_.get_datum_from_rowid(rowid, id_datum))) {
        LOG_WARN("failed to get datum from rowid", K(ret));
      } else if (OB_FAIL(curr_id.from_datum(id_datum))) {
        LOG_WARN("failed to get curr id from datum", K(ret));
      } else if (OB_FAIL(map_.get_refactored(curr_id, prev_item))) {
        if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
          LOG_WARN("failed to get prev item from map", K(ret));
        } else {
          const int64_t cache_idx = id_cache_.count();
          if (OB_FAIL(id_cache_.push_back(curr_id))) {
            LOG_WARN("failed to cache curr id", K(ret));
          } else if (OB_FAIL(map_.set_refactored(
              curr_id, ObDASTopKItem(score, cache_idx), 0 /*overwrite*/))) {
            LOG_WARN("failed to set curr item in map", K(ret));
          }
        }
      } else if (score <= prev_item.score_) {
        // discard this result
      } else if (OB_FAIL(map_.set_refactored(
          curr_id, ObDASTopKItem(score, prev_item.cache_idx_), 1 /*overwrite*/))) {
        LOG_WARN("failed to update curr item in map", K(ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  for (ObDASTopKHashMap::const_iterator iter = map_.begin();
      OB_SUCC(ret) && iter != map_.end();
      ++iter) {
    double score = iter->second.score_;
    if (heap_.count() < limit_) {
      if (OB_FAIL(heap_.push(iter->second))) {
        LOG_WARN("failed to push onto heap", K(ret));
      }
    } else if (heap_.top().score_ < score) {
      if (OB_FAIL(heap_.pop())) {
        LOG_WARN("failed to pop from heap", K(ret));
      } else if (OB_FAIL(heap_.push(iter->second))) {
        LOG_WARN("failed to push onto heap", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(heap_.count() > limit_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected heap size", K(ret), K(heap_.count()), K_(limit));
  } else {
    is_loaded_ = true;
  }
  return ret;
}

int ObDASDisjunctiveMaxOp::do_next_rowid(ObDASRowID &next_id, double &score)
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
} // namesapce oceanbase
