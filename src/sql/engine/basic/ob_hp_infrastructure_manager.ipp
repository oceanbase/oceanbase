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

namespace oceanbase
{
namespace sql
{

//////////////////// start ObHashPartInfrastructureGroup //////////////////
template<typename HashCol, typename HashRowStore>
void ObHashPartInfrastructureGroup<HashCol, HashRowStore>::reset()
{
  est_bucket_num_ = 0;
  initial_hp_size_ = 0;
  hp_infras_buffer_idx_ = MAX_HP_INFRAS_CNT;
  foreach_call(hp_infras_list_, &HashPartInfras::destroy);
  foreach_call(hp_infras_free_list_, &HashPartInfras::destroy);
  hp_infras_list_.reset();
  hp_infras_free_list_.reset();
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructureGroup<HashCol, HashRowStore>::init_one_hp_infras(
  HashPartInfras *&hp_infras, uint64_t tenant_id, bool enable_sql_dumped, bool unique,
  int64_t ways, int64_t batch_size, int64_t est_rows, int64_t width,
  ObSqlMemMgrProcessor *sql_mem_processor, bool need_rewind)
{
  int ret = OB_SUCCESS;
  int64_t est_bucket_num = 0;
  HashPartInfras *tmp_hp_infras = nullptr;
  auto total_mem_used_func = [this] () -> int64_t {
    int64_t total_mem_used = 0;
    // It has been checked whether it is nullptr when it is added, so it will not be checked here
    DLIST_FOREACH_NORET(hp_infras, hp_infras_list_) {
      total_mem_used += hp_infras->get_total_mem_used();
    }
    SQL_ENG_LOG(TRACE, "calc total mem used", K(total_mem_used));
    return total_mem_used;
  };
  auto slice_cnt_func = [this] () -> int64_t {
    int64_t slice_cnt = hp_infras_list_.get_size();
    return slice_cnt > initial_hp_size_ ? slice_cnt : initial_hp_size_;
  };
  if (OB_FAIL(try_get_hp_infras_from_free_list(hp_infras))) {
    SQL_ENG_LOG(WARN, "failed to try get hash partition infrastructure", K(ret));
  } else if (nullptr == hp_infras || hp_infras->is_destroyed()) {
    if (nullptr == hp_infras && OB_FAIL(alloc_hp_infras(hp_infras))) {
      SQL_ENG_LOG(WARN, "failed to alloc hash partition infrastructure", K(ret));
    } else {
      hp_infras = new(hp_infras) HashPartInfras();
      hp_infras->set_hp_infras_group_func(total_mem_used_func, slice_cnt_func);
      if (OB_FAIL(hp_infras->init(tenant_id, enable_sql_dumped, unique, true,
        ways, sql_mem_processor, need_rewind))) {
        SQL_ENG_LOG(WARN, "failed to init hash partition infrastructure", K(ret));
      } else if (OB_FAIL(init_hash_table(hp_infras, est_rows, width))) {
        SQL_ENG_LOG(WARN, "failed to init hash table", K(ret));
      } else if (batch_size > 0) {
        if (OB_FAIL(hp_infras->init_my_skip(batch_size))) {
          SQL_ENG_LOG(WARN, "failed to init hp skip", K(ret));
        } else if (OB_FAIL(hp_infras->init_items(batch_size))) {
          SQL_ENG_LOG(WARN, "failed to init items", K(ret));
        } else if (OB_FAIL(hp_infras->init_distinct_map(batch_size))) {
          SQL_ENG_LOG(WARN, "failed to init distinct map", K(ret));
        }
      }
    }
  } else if (OB_FAIL(hp_infras->set_need_rewind(need_rewind))) {
    SQL_ENG_LOG(WARN, "failed to set need rewind", K(ret));
  } else {
    hp_infras->set_hp_infras_group_func(total_mem_used_func, slice_cnt_func);
  }
  if (OB_FAIL(ret)) {
  } else if (!hp_infras_list_.add_last(hp_infras)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "failed to add hash partition infrastructure", K(ret));
  } else {
    SQL_ENG_LOG(TRACE, "trace info", K(hp_infras_list_.get_size()), K(hp_infras_free_list_.get_size()));
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructureGroup<HashCol, HashRowStore>::init_hash_table(
  HashPartInfras *&hp_infras, const int64_t est_rows, const int64_t width)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hp_infras)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "hash partition infrastructure is null", K(ret));
  } else if (FALSE_IT(est_bucket_num_ = hp_infras->est_bucket_count(est_rows, width,
      HashPartInfrasMgr::MIN_BUCKET_COUNT, HashPartInfrasMgr::MAX_BUCKET_COUNT))) {
  } else if (OB_FAIL(hp_infras->start_round())) {
    SQL_ENG_LOG(WARN, "failed to start round", K(ret));
  } else if (OB_FAIL(hp_infras->init_hash_table(est_bucket_num_,
      HashPartInfrasMgr::MIN_BUCKET_COUNT, HashPartInfrasMgr::MAX_BUCKET_COUNT))) {
    SQL_ENG_LOG(WARN, "failed to init hash table", K(ret));
  } else {
    SQL_ENG_LOG(TRACE, "init_hash_table", K(est_bucket_num_), K(est_rows), K(width));
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructureGroup<HashCol, HashRowStore>::try_get_hp_infras_from_free_list(HashPartInfras *&hp_infras)
{
  int ret = OB_SUCCESS;
  hp_infras = nullptr;
  if (!hp_infras_free_list_.is_empty()) {
    hp_infras = hp_infras_free_list_.remove_first();
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructureGroup<HashCol, HashRowStore>::free_one_hp_infras(HashPartInfras *&hp_infras)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hp_infras)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: hp infras is null", K(ret));
  } else if (FALSE_IT(hp_infras_list_.remove(hp_infras))) {
  } else {
    if (hp_infras->get_bucket_num() > est_bucket_num_ * RATIO) {
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

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructureGroup<HashCol, HashRowStore>::alloc_hp_infras(
  HashPartInfras *&hp_infras)
{
  int ret = OB_SUCCESS;
  hp_infras = nullptr;
  if (hp_infras_buffer_idx_ < MAX_HP_INFRAS_CNT) {
    hp_infras = &hp_infras_buffer_[hp_infras_buffer_idx_++];
  } else if (OB_ISNULL(hp_infras_buffer_
        = reinterpret_cast<HashPartInfras *>
          (allocator_.alloc(MAX_HP_INFRAS_CNT * sizeof(HashPartInfras))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "failed to alloc mem for hp infras buffer", K(ret));
  } else {
    hp_infras_buffer_idx_ = 0;
    hp_infras = &hp_infras_buffer_[hp_infras_buffer_idx_++];
  }
  return ret;
}

//////////////////// end ObHashPartInfrastructureGroup /////////////////////

//////////////////// start ObHashPartInfrastructureMgr //////////////////
template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructureMgr<HashCol, HashRowStore>::init(
  uint64_t tenant_id, bool enable_sql_dumped, const int64_t est_rows, const int64_t width,
  const bool unique, const int64_t ways, ObEvalCtx *eval_ctx,
  ObSqlMemMgrProcessor *sql_mem_processor, ObIOEventObserver *io_event_observer)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    SQL_ENG_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(eval_ctx) || OB_ISNULL(sql_mem_processor) || OB_ISNULL(io_event_observer)) {
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
    io_event_observer_ = io_event_observer;
    sql_mem_processor_ = sql_mem_processor;
    inited_ = true;
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructureMgr<HashCol, HashRowStore>::free_one_hp_infras(HashPartInfras *&hp_infras)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hp_infras_group_.free_one_hp_infras(hp_infras))) {
    SQL_ENG_LOG(WARN, "failed to free one hp infras", K(ret));
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructureMgr<HashCol, HashRowStore>::init_one_hp_infras(
  const bool need_rewind, const ObSortCollations *sort_collations,
  const ObSortFuncs *sort_cmp_funcs, const ObHashFuncs *hash_funcs, HashPartInfras *&hp_infras)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not init", K(ret));
  } else if (need_rewind && 2 == ways_) {
    ret = OB_NOT_SUPPORTED;
    SQL_ENG_LOG(WARN, "Two-way input does not support rewind", K(ret), K(need_rewind), K(ways_));
  } else if (OB_ISNULL(sort_collations) || OB_ISNULL(sort_cmp_funcs) || OB_ISNULL(hash_funcs)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: func is null", K(ret));
  } else if (OB_FAIL(hp_infras_group_.init_one_hp_infras(hp_infras, tenant_id_, enable_sql_dumped_,
    unique_, ways_, eval_ctx_->max_batch_size_, est_rows_, width_, sql_mem_processor_, need_rewind))) {
    SQL_ENG_LOG(WARN, "failed to create one hash partition infrastructure", K(ret));
  } else if (FALSE_IT(hp_infras->set_io_event_observer(io_event_observer_))) {
  } else if (OB_FAIL(hp_infras->set_funcs(hash_funcs, sort_collations, sort_cmp_funcs, eval_ctx_))) {
    SQL_ENG_LOG(WARN, "failed to set funcs", K(ret));
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////////

using HashPartInfrasMgr = ObHashPartInfrastructureMgr<ObHashPartCols, ObHashPartStoredRow>;

}  // namespace sql
}  // namespace oceanbase