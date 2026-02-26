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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_hp_infras_vec_mgr.h"

namespace oceanbase
{
namespace sql
{

//////////////////// start ObHashPartInfrasVecGroup //////////////////
void ObHashPartInfrasVecGroup::reset()
{
  tenant_id_ = UINT64_MAX;
  enable_sql_dumped_ = false;
  est_rows_ = 0;
  width_ = 0;
  unique_ = false;
  ways_ = 1;
  eval_ctx_ = nullptr;
  compressor_type_ = NONE_COMPRESSOR;
  sql_mem_processor_ = nullptr;
  io_event_observer_ = nullptr;
  est_bucket_num_ = 0;
  initial_hp_size_ = 0;
  hp_infras_buffer_idx_ = MAX_HP_INFRAS_CNT;
  foreach_call(hp_infras_list_, &HashPartInfrasVec::destroy);
  foreach_call(hp_infras_free_list_, &HashPartInfrasVec::destroy);
  hp_infras_list_.reset();
  hp_infras_free_list_.reset();
}

int ObHashPartInfrasVecGroup::init(uint64_t tenant_id, bool enable_sql_dumped,
                                   const int64_t est_rows, const int64_t width, const bool unique,
                                   const int64_t ways, ObEvalCtx *eval_ctx,
                                   ObSqlMemMgrProcessor *sql_mem_processor,
                                   ObIOEventObserver *io_event_observer,
                                   common::ObCompressorType compressor_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(eval_ctx) || OB_ISNULL(sql_mem_processor) || OB_ISNULL(io_event_observer)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: is null", K(ret));
  } else {
    tenant_id_ = tenant_id;
    enable_sql_dumped_ = enable_sql_dumped;
    est_rows_ = est_rows;
    width_ = width;
    unique_ = unique;
    ways_ = ways;
    eval_ctx_ = eval_ctx;
    compressor_type_ = compressor_type;
    io_event_observer_ = io_event_observer;
    sql_mem_processor_ = sql_mem_processor;
  }
  return ret;
}

int ObHashPartInfrasVecGroup::init_one_hp_infras(
  HashPartInfrasVec *&hp_infras, const common::ObIArray<ObExpr *> &exprs,
  const common::ObIArray<ObSortFieldCollation> *sort_collations, bool need_rewind)
{
  int ret = OB_SUCCESS;
  int64_t est_bucket_num = 0;
  int64_t batch_size = eval_ctx_->max_batch_size_;
  auto total_mem_used_func = [this]() -> int64_t {
    int64_t total_mem_used = 0;
    // It has been checked whether it is nullptr when it is added, so it will not be checked here
    DLIST_FOREACH_NORET(hp_infras, hp_infras_list_)
    {
      total_mem_used += hp_infras->get_total_mem_used();
    }
    SQL_ENG_LOG(TRACE, "calc total mem used", K(total_mem_used));
    return total_mem_used;
  };
  auto slice_cnt_func = [this]() -> int64_t {
    int64_t slice_cnt = hp_infras_list_.get_size();
    return slice_cnt > initial_hp_size_ ? slice_cnt : initial_hp_size_;
  };
  if (need_rewind && 2 == ways_) {
    ret = OB_NOT_SUPPORTED;
    SQL_ENG_LOG(WARN, "Two-way input does not support rewind", K(ret), K(need_rewind), K(ways_));
  } else if (OB_FAIL(try_get_hp_infras_from_free_list(hp_infras, exprs))) {
    SQL_ENG_LOG(WARN, "failed to try get hash partition infrastructure", K(ret));
  } else if (nullptr == hp_infras || hp_infras->is_destroyed()) {
    if (nullptr == hp_infras && OB_FAIL(alloc_hp_infras(hp_infras))) {
      SQL_ENG_LOG(WARN, "failed to alloc hash partition infrastructure", K(ret));
    } else {
      hp_infras = new (hp_infras) HashPartInfrasVec();
      if (OB_FAIL(hp_infras->init(tenant_id_, enable_sql_dumped_, unique_, true, ways_, batch_size,
                                  exprs, sql_mem_processor_, compressor_type_, need_rewind))) {
        SQL_ENG_LOG(WARN, "failed to init hash partition infrastructure", K(ret));
      } else if (OB_FAIL(hp_infras->set_funcs(sort_collations, eval_ctx_))) {
        SQL_ENG_LOG(WARN, "failed to set funcs", K(ret));
      } else if (FALSE_IT(
                   hp_infras->set_hp_infras_group_func(total_mem_used_func, slice_cnt_func))) {
      } else if (OB_FAIL(init_hash_table(hp_infras, est_rows_ * RATIO, width_))) {
        SQL_ENG_LOG(WARN, "failed to init hash table", K(ret));
      } else if (batch_size > 0 && OB_FAIL(hp_infras->init_my_skip(batch_size))) {
        SQL_ENG_LOG(WARN, "failed to init hp skip", K(ret));
      }
    }
  } else if (OB_FAIL(hp_infras->set_need_rewind(need_rewind))) {
    SQL_ENG_LOG(WARN, "failed to set need rewind", K(ret));
  } else if (OB_FAIL(hp_infras->start_round())) {
    SQL_ENG_LOG(WARN, "failed to start round", K(ret));
  } else {
    hp_infras->set_hp_infras_group_func(total_mem_used_func, slice_cnt_func);
  }
  if (OB_FAIL(ret)) {
  } else if (!hp_infras_list_.add_last(hp_infras)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "failed to add hash partition infrastructure", K(ret));
  } else {
    hp_infras->set_io_event_observer(io_event_observer_);
    SQL_ENG_LOG(TRACE, "trace info", K(hp_infras_list_.get_size()),
                K(hp_infras_free_list_.get_size()));
  }
  return ret;
}

int ObHashPartInfrasVecGroup::init_hash_table(HashPartInfrasVec *&hp_infras, const int64_t est_rows,
                                              const int64_t width)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hp_infras)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "hash partition infrastructure is null", K(ret));
  } else if (FALSE_IT(est_bucket_num_ = hp_infras->est_bucket_count(
                        est_rows, width, ObHashPartInfrasVecMgr::MIN_BUCKET_COUNT,
                        ObHashPartInfrasVecMgr::MAX_BUCKET_COUNT))) {
  } else if (OB_FAIL(hp_infras->start_round())) {
    SQL_ENG_LOG(WARN, "failed to start round", K(ret));
  } else if (OB_FAIL(hp_infras->init_hash_table(est_bucket_num_,
                                                ObHashPartInfrasVecMgr::MIN_BUCKET_COUNT,
                                                ObHashPartInfrasVecMgr::MAX_BUCKET_COUNT))) {
    SQL_ENG_LOG(WARN, "failed to init hash table", K(ret));
  } else {
    SQL_ENG_LOG(TRACE, "init_hash_table", K(est_bucket_num_), K(est_rows), K(width));
  }
  return ret;
}

int ObHashPartInfrasVecGroup::try_get_hp_infras_from_free_list(
  HashPartInfrasVec *&hp_infras, const common::ObIArray<ObExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  hp_infras = nullptr;
  if (!hp_infras_free_list_.is_empty()) {
    DLIST_FOREACH_NORET(cur, hp_infras_free_list_) {
      if (cur->is_destroyed() || cur->is_equal_hash_infras(exprs)) {
        hp_infras = hp_infras_free_list_.remove(cur);
        break;
      }
    }
  }
  return ret;
}

int ObHashPartInfrasVecGroup::free_one_hp_infras(HashPartInfrasVec *&hp_infras)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hp_infras)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: hp infras is null", K(ret));
  } else if (FALSE_IT(hp_infras_list_.remove(hp_infras))) {
  } else {
    if (!hp_infras->is_inited()) {
    } else if (hp_infras->get_bucket_num() > est_bucket_num_ * RATIO) {
      hp_infras->destroy();
    } else {
      hp_infras->reuse();
    }
    if (!hp_infras_free_list_.add_last(hp_infras)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "failed to add hp infras to free list", K(ret));
    }
  }
  return ret;
}

int ObHashPartInfrasVecGroup::alloc_hp_infras(HashPartInfrasVec *&hp_infras)
{
  int ret = OB_SUCCESS;
  hp_infras = nullptr;
  if (hp_infras_buffer_idx_ < MAX_HP_INFRAS_CNT) {
    hp_infras = &hp_infras_buffer_[hp_infras_buffer_idx_++];
  } else if (OB_ISNULL(hp_infras_buffer_ = reinterpret_cast<HashPartInfrasVec *>(
                         allocator_.alloc(MAX_HP_INFRAS_CNT * sizeof(HashPartInfrasVec))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "failed to alloc mem for hp infras buffer", K(ret));
  } else {
    hp_infras_buffer_idx_ = 0;
    hp_infras = &hp_infras_buffer_[hp_infras_buffer_idx_++];
  }
  return ret;
}

//////////////////// end ObHashPartInfrasVecGroup /////////////////////

//////////////////// start ObHashPartInfrasVecMgr /////////////////////

int ObHashPartInfrasVecMgr::init(uint64_t tenant_id, bool enable_sql_dumped, const int64_t est_rows,
                                 const int64_t width, const bool unique, const int64_t ways,
                                 ObEvalCtx *eval_ctx, ObSqlMemMgrProcessor *sql_mem_processor,
                                 ObIOEventObserver *io_event_observer,
                                 common::ObCompressorType compressor_type)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    SQL_ENG_LOG(WARN, "init twice", K(ret));
  } else if (OB_FAIL(hp_infras_group_.init(tenant_id, enable_sql_dumped, est_rows, width, unique,
                                           ways, eval_ctx, sql_mem_processor, io_event_observer,
                                           compressor_type))) {
    SQL_ENG_LOG(WARN, "failed to init hash infras group", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObHashPartInfrasVecMgr::free_one_hp_infras(HashPartInfrasVec *&hp_infras)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hp_infras_group_.free_one_hp_infras(hp_infras))) {
    SQL_ENG_LOG(WARN, "failed to free one hp infras", K(ret));
  }
  return ret;
}

int ObHashPartInfrasVecMgr::init_one_hp_infras(const bool need_rewind,
                                               const ObSortCollations *sort_collations,
                                               const common::ObIArray<ObExpr *> &exprs,
                                               HashPartInfrasVec *&hp_infras)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(sort_collations)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: func is null", K(ret));
  } else if (OB_FAIL(hp_infras_group_.init_one_hp_infras(hp_infras, exprs, sort_collations,
                                                         need_rewind))) {
    SQL_ENG_LOG(WARN, "failed to create one hash partition infrastructure", K(ret));
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////////

}  // namespace sql
}  // namespace oceanbase