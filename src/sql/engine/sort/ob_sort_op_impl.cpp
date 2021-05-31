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

#include "ob_sort_op_impl.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"

namespace oceanbase {
using namespace common;
namespace sql {

/************************************* start ObSortOpImpl *********************************/
ObSortOpImpl::Compare::Compare() : ret_(OB_SUCCESS), sort_collations_(nullptr), sort_cmp_funs_(nullptr)
{}

int ObSortOpImpl::Compare::init(
    const ObIArray<ObSortFieldCollation>* sort_collations, const ObIArray<ObSortCmpFunc>* sort_cmp_funs)
{
  int ret = OB_SUCCESS;
  bool is_static_cmp = false;
  if (nullptr == sort_collations || nullptr == sort_cmp_funs) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sort_collations), KP(sort_cmp_funs));
  } else if (sort_cmp_funs->count() != sort_cmp_funs->count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column count miss match", K(ret), K(sort_cmp_funs->count()), K(sort_cmp_funs->count()));
  } else {
    sort_collations_ = sort_collations;
    sort_cmp_funs_ = sort_cmp_funs;
  }
  return ret;
}

bool ObSortOpImpl::Compare::operator()(const ObChunkDatumStore::StoredRow* l, const ObChunkDatumStore::StoredRow* r)
{
  bool less = false;
  int& ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    // already fail
  } else if (!is_inited() || OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = !is_inited() ? OB_NOT_INIT : OB_INVALID_ARGUMENT;
    LOG_WARN("not init or invalid argument", K(ret), KP(l), KP(r));
  } else {
    const ObDatum* lcells = l->cells();
    const ObDatum* rcells = r->cells();
    int cmp = 0;
    for (int64_t i = 0; 0 == cmp && i < sort_cmp_funs_->count(); i++) {
      const int64_t idx = sort_collations_->at(i).field_idx_;
      cmp = sort_cmp_funs_->at(i).cmp_func_(lcells[idx], rcells[idx]);
      if (cmp < 0) {
        less = sort_collations_->at(i).is_ascending_;
      } else if (cmp > 0) {
        less = !sort_collations_->at(i).is_ascending_;
      }
    }
  }
  return less;
}

bool ObSortOpImpl::Compare::operator()(
    const common::ObIArray<ObExpr*>* l, const ObChunkDatumStore::StoredRow* r, ObEvalCtx& eval_ctx)
{
  bool less = false;
  int& ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    // already fail
  } else if (!is_inited() || OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = !is_inited() ? OB_NOT_INIT : OB_INVALID_ARGUMENT;
    LOG_WARN("not init or invalid argument", K(ret), KP(l), KP(r));
  } else {
    const ObDatum* rcells = r->cells();
    ObDatum* other_datum = nullptr;
    int cmp = 0;
    for (int64_t i = 0; 0 == cmp && i < sort_cmp_funs_->count() && OB_SUCC(ret); i++) {
      const int64_t idx = sort_collations_->at(i).field_idx_;
      if (OB_FAIL(l->at(idx)->eval(eval_ctx, other_datum))) {
        LOG_WARN("failed to eval expr", K(ret));
      } else {
        cmp = sort_cmp_funs_->at(i).cmp_func_(*other_datum, rcells[idx]);
        if (cmp < 0) {
          less = sort_collations_->at(i).is_ascending_;
        } else if (cmp > 0) {
          less = !sort_collations_->at(i).is_ascending_;
        }
      }
    }
  }
  return less;
}

// compare function for external merge sort
bool ObSortOpImpl::Compare::operator()(const ObSortOpChunk* l, const ObSortOpChunk* r)
{
  bool less = false;
  int& ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    // already fail
  } else if (OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(l), KP(r));
  } else {
    // Return the reverse order since the heap top is the maximum element.
    // NOTE: can not return !(*this)(l->row_, r->row_)
    //       because we should always return false if l == r.
    less = (*this)(r->row_, l->row_);
  }
  return less;
}

bool ObSortOpImpl::Compare::operator()(ObChunkDatumStore::StoredRow** l, ObChunkDatumStore::StoredRow** r)
{
  bool less = false;
  int& ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    // already fail
  } else if (OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(l), KP(r));
  } else {
    // Return the reverse order since the heap top is the maximum element.
    // NOTE: can not return !(*this)(l->row_, r->row_)
    //       because we should always return false if l == r.
    less = (*this)(*r, *l);
  }
  return less;
}

ObSortOpImpl::ObSortOpImpl()
    : inited_(false),
      local_merge_sort_(false),
      need_rewind_(false),
      got_first_row_(false),
      sorted_(false),
      mem_context_(NULL),
      mem_entify_guard_(mem_context_),
      tenant_id_(OB_INVALID_ID),
      sort_collations_(nullptr),
      sort_cmp_funs_(nullptr),
      eval_ctx_(nullptr),
      inmem_row_size_(0),
      mem_check_interval_mask_(1),
      row_idx_(0),
      heap_iter_begin_(false),
      imms_heap_(NULL),
      ems_heap_(NULL),
      next_stored_row_func_(&ObSortOpImpl::array_next_stored_row),
      input_rows_(OB_INVALID_ID),
      input_width_(OB_INVALID_ID),
      profile_(ObSqlWorkAreaType::SORT_WORK_AREA),
      sql_mem_processor_(profile_),
      op_type_(PHY_INVALID),
      op_id_(UINT64_MAX),
      exec_ctx_(nullptr)
{}

ObSortOpImpl::~ObSortOpImpl()
{
  reset();
}

// Set the note in ObPrefixSortImpl::init(): %sort_columns may be zero, to compatible with
// the wrong generated prefix sort.
int ObSortOpImpl::init(const uint64_t tenant_id, const ObIArray<ObSortFieldCollation>* sort_collations,
    const ObIArray<ObSortCmpFunc>* sort_cmp_funs, ObEvalCtx* eval_ctx, const bool in_local_order /* = false */,
    const bool need_rewind /* = false */)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_ISNULL(sort_collations) || OB_ISNULL(sort_cmp_funs) || OB_ISNULL(eval_ctx) ||
             sort_collations->count() != sort_cmp_funs->count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "invalid argument: argument is null", K(ret), K(tenant_id), K(sort_collations), K(sort_cmp_funs), K(eval_ctx));
  } else {
    local_merge_sort_ = in_local_order;
    need_rewind_ = need_rewind;
    tenant_id_ = tenant_id;
    sort_collations_ = sort_collations;
    sort_cmp_funs_ = sort_cmp_funs;
    eval_ctx_ = eval_ctx;
    lib::ContextParam param;
    param.set_mem_attr(tenant_id, ObModIds::OB_SQL_SORT_ROW, ObCtxIds::WORK_AREA)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (NULL == mem_context_ && OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("create entity failed", K(ret));
    } else if (NULL == mem_context_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null memory entity returned", K(ret));
    } else if (OB_FAIL(datum_store_.init(INT64_MAX /* mem limit, big enough to hold all rows in memory */,
                   tenant_id_,
                   ObCtxIds::WORK_AREA,
                   ObModIds::OB_SQL_SORT_ROW,
                   false /*+ disable dump */))) {
      LOG_WARN("init row store failed", K(ret));
    } else {
      rows_.set_block_allocator(ModulePageAllocator(mem_context_->get_malloc_allocator(), "SortOpRows"));
      datum_store_.set_dir_id(sql_mem_processor_.get_dir_id());
      datum_store_.set_allocator(mem_context_->get_malloc_allocator());
      inited_ = true;
    }
  }

  return ret;
}

void ObSortOpImpl::reuse()
{
  sorted_ = false;
  iter_.reset();
  rows_.reuse();
  datum_store_.reset();
  inmem_row_size_ = 0;
  mem_check_interval_mask_ = 1;
  row_idx_ = 0;
  next_stored_row_func_ = &ObSortOpImpl::array_next_stored_row;
  while (!sort_chunks_.is_empty()) {
    ObSortOpChunk* chunk = sort_chunks_.remove_first();
    chunk->~ObSortOpChunk();
    if (NULL != mem_context_) {
      mem_context_->get_malloc_allocator().free(chunk);
    }
  }
  if (NULL != imms_heap_) {
    imms_heap_->reset();
  }
  heap_iter_begin_ = false;
  if (NULL != ems_heap_) {
    ems_heap_->reset();
  }
}

void ObSortOpImpl::unregister_profile()
{
  sql_mem_processor_.unregister_profile();
}

void ObSortOpImpl::reset()
{
  sql_mem_processor_.unregister_profile();
  iter_.reset();
  reuse();
  rows_.reset();
  datum_store_.reset();
  local_merge_sort_ = false;
  need_rewind_ = false;
  sorted_ = false;
  got_first_row_ = false;
  comp_.reset();
  if (NULL != mem_context_) {
    if (NULL != imms_heap_) {
      imms_heap_->~IMMSHeap();
      mem_context_->get_malloc_allocator().free(imms_heap_);
      imms_heap_ = NULL;
    }
    if (NULL != ems_heap_) {
      ems_heap_->~EMSHeap();
      mem_context_->get_malloc_allocator().free(ems_heap_);
      ems_heap_ = NULL;
    }
    // can not destroy mem_entify here, the memory may hold by %iter_ or %datum_store_
  }
  inited_ = false;
}

template <typename Input>
int ObSortOpImpl::build_chunk(const int64_t level, Input& input)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore* datum_store = NULL;
  const ObChunkDatumStore::StoredRow* src_store_row = NULL;
  ObChunkDatumStore::StoredRow* dst_store_row = NULL;
  ObSortOpChunk* chunk = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(chunk = OB_NEWx(ObSortOpChunk, (&mem_context_->get_malloc_allocator()), level))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(chunk->datum_store_.init(1 /*+ mem limit, small limit for dump immediately */,
                 tenant_id_,
                 ObCtxIds::WORK_AREA,
                 ObModIds::OB_SQL_SORT_ROW,
                 true /*+ enable dump */))) {
    LOG_WARN("init row store failed", K(ret));
  } else {
    chunk->datum_store_.set_dir_id(sql_mem_processor_.get_dir_id());
    chunk->datum_store_.set_allocator(mem_context_->get_malloc_allocator());
    chunk->datum_store_.set_callback(&sql_mem_processor_);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(input(datum_store, src_store_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get input row failed", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
        break;
      } else if (OB_FAIL(chunk->datum_store_.add_row(*src_store_row, &dst_store_row))) {
        LOG_WARN("copy row to row store failed");
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(chunk->datum_store_.dump(false, true))) {
      LOG_WARN("failed to dump row store", K(ret));
    } else if (OB_FAIL(chunk->datum_store_.finish_add_row(true /*+ need dump */))) {
      LOG_WARN("finish add row failed", K(ret));
    } else {
      LOG_TRACE("dump sort file",
          "level",
          level,
          "rows",
          chunk->datum_store_.get_row_cnt(),
          "file_size",
          chunk->datum_store_.get_file_size(),
          "memory_hold",
          chunk->datum_store_.get_mem_hold(),
          "mem_used",
          mem_context_->used());
    }
  }

  if (OB_SUCC(ret)) {
    // In increase sort, chunk->level_ may less than the last of sort chunks.
    // insert the chunk to the upper bound the level.
    ObSortOpChunk* pos = sort_chunks_.get_last();
    for (; pos != sort_chunks_.get_header() && pos->level_ > level; pos = pos->get_prev()) {}
    pos = pos->get_next();
    if (!sort_chunks_.add_before(pos, chunk)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add link node to list failed", K(ret));
    }
  }
  if (OB_SUCCESS != ret && NULL != chunk) {
    chunk->~ObSortOpChunk();
    mem_context_->get_malloc_allocator().free(chunk);
    chunk = NULL;
  }

  return ret;
}

int ObSortOpImpl::preprocess_dump(bool& dumped)
{
  int ret = OB_SUCCESS;
  dumped = false;
  if (OB_FAIL(sql_mem_processor_.get_max_available_mem_size(&mem_context_->get_malloc_allocator()))) {
    LOG_WARN("failed to get max available memory size", K(ret));
  } else if (OB_FAIL(sql_mem_processor_.update_used_mem_size(mem_context_->used()))) {
    LOG_WARN("failed to update used memory size", K(ret));
  } else {
    dumped = need_dump();
    if (dumped) {
      if (!sql_mem_processor_.is_auto_mgr()) {
        // If dump is in non-auto management mode, it also needs to be registered to workarea
        if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(
                &mem_context_->get_malloc_allocator(),
                [&](int64_t max_memory_size) {
                  UNUSED(max_memory_size);
                  return need_dump();
                },
                dumped,
                mem_context_->used()))) {
          LOG_WARN("failed to extend memory size", K(ret));
        }
      } else if (profile_.get_cache_size() < profile_.get_global_bound_size()) {
        // in-memory: All data can be cached, that is, the global bound size is relatively large,
        // then continue to see if more memory is available
        if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(
                &mem_context_->get_malloc_allocator(),
                [&](int64_t max_memory_size) {
                  UNUSED(max_memory_size);
                  return need_dump();
                },
                dumped,
                mem_context_->used()))) {
          LOG_WARN("failed to extend memory size", K(ret));
        }
        LOG_TRACE("trace sort need dump",
            K(dumped),
            K(mem_context_->used()),
            K(get_memory_limit()),
            K(profile_.get_cache_size()),
            K(profile_.get_expect_size()));
      } else {
        if (profile_.get_cache_size() <= datum_store_.get_mem_hold() + datum_store_.get_file_size()) {
          if (OB_FAIL(sql_mem_processor_.update_cache_size(
                  &mem_context_->get_malloc_allocator(), profile_.get_cache_size() * EXTEND_MULTIPLE))) {
            LOG_WARN("failed to update cache size", K(ret), K(profile_.get_cache_size()));
          } else {
            dumped = need_dump();
          }
        } else {
        }
      }
    }
    LOG_TRACE("trace sort need dump",
        K(dumped),
        K(mem_context_->used()),
        K(get_memory_limit()),
        K(profile_.get_cache_size()),
        K(profile_.get_expect_size()),
        K(sql_mem_processor_.get_data_size()));
  }
  return ret;
}

template <typename T>
int ObSortOpImpl::add_row(const T& exprs, const ObChunkDatumStore::StoredRow*& store_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!got_first_row_)) {
    if (OB_FAIL(comp_.init(sort_collations_, sort_cmp_funs_))) {
      LOG_WARN("update sort column idx failed", K(ret));
    } else {
      got_first_row_ = true;
      int64_t size = OB_INVALID_ID == input_rows_ ? 0 : input_rows_ * input_width_;
      if (OB_FAIL(sql_mem_processor_.init(
              &mem_context_->get_malloc_allocator(), tenant_id_, size, op_type_, op_id_, exec_ctx_))) {
        LOG_WARN("failed to init sql mem processor", K(ret));
      } else {
        datum_store_.set_dir_id(sql_mem_processor_.get_dir_id());
        datum_store_.set_callback(&sql_mem_processor_);
      }
    }
  }

  if (OB_SUCC(ret) && !rows_.empty()) {
    bool updated = false;
    if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
            &mem_context_->get_malloc_allocator(),
            [&](int64_t cur_cnt) { return rows_.count() > cur_cnt; },
            updated))) {
      LOG_WARN("failed to update max available mem size periodically", K(ret));
    } else if (updated && OB_FAIL(sql_mem_processor_.update_used_mem_size(mem_context_->used()))) {
      LOG_WARN("failed to update used memory size", K(ret));
    } else if (GCONF.is_sql_operator_dump_enabled()) {
      if (rows_.count() >= MAX_ROW_CNT) {
        // Maximum 2G, more than 2G will expand to 4G, 4G application will fail
        if (OB_FAIL(do_dump())) {
          LOG_WARN("dump failed", K(ret));
        }
      } else if (need_dump()) {
        bool dumped = false;
        if (OB_FAIL(preprocess_dump(dumped))) {
          LOG_WARN("failed preprocess dump", K(ret));
        } else if (dumped && OB_FAIL(do_dump())) {
          LOG_WARN("dump failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && sorted_) {
    if (!need_rewind_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not add row after sort if no need rewind", K(ret));
    } else {
      sorted_ = false;
      // add null sentry row
      if (!rows_.empty() && NULL != rows_.at(rows_.count() - 1)) {
        if (OB_FAIL(rows_.push_back(NULL))) {
          LOG_WARN("array push back failed", K(ret));
        }
      }
    }
  }

  // row should be added after dumping to make sure %store_row memory available.
  if (OB_SUCC(ret)) {
    ObChunkDatumStore::StoredRow* sr = NULL;
    if (OB_FAIL(datum_store_.add_row(exprs, eval_ctx_, &sr))) {
      LOG_WARN("add store row failed", K(ret), K(mem_context_->used()), K(get_memory_limit()));
    } else {
      inmem_row_size_ += sr->row_size_;
      store_row = sr;
      if (local_merge_sort_ && rows_.count() > 0 && NULL != rows_.at(rows_.count() - 1)) {
        const bool less = comp_(sr, rows_.at(rows_.count() - 1));
        if (OB_SUCCESS != comp_.ret_) {
          ret = comp_.ret_;
          LOG_WARN("compare failed", K(ret));
        } else if (less) {
          // If new is less than previous row, add NULL to separate different local order rows.
          if (OB_FAIL(rows_.push_back(NULL))) {
            LOG_WARN("array push back failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(rows_.push_back(sr))) {
          LOG_WARN("array push back failed", K(ret), K(rows_.count()));
        }
      }
    }
  }
  return ret;
}

int ObSortOpImpl::add_stored_row(const ObChunkDatumStore::StoredRow& input_row)
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow* store_row = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!got_first_row_)) {
    if (OB_FAIL(comp_.init(sort_collations_, sort_cmp_funs_))) {
      LOG_WARN("update sort column idx failed", K(ret));
    } else {
      got_first_row_ = true;
      int64_t size = OB_INVALID_ID == input_rows_ ? 0 : input_rows_ * input_width_;
      if (OB_FAIL(sql_mem_processor_.init(
              &mem_context_->get_malloc_allocator(), tenant_id_, size, op_type_, op_id_, exec_ctx_))) {
        LOG_WARN("failed to init sql mem processor", K(ret));
      } else {
        datum_store_.set_callback(&sql_mem_processor_);
      }
    }
  }

  if (OB_SUCC(ret) && !rows_.empty()) {
    bool updated = false;
    if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
            &mem_context_->get_malloc_allocator(),
            [&](int64_t cur_cnt) { return rows_.count() > cur_cnt; },
            updated))) {
      LOG_WARN("failed to update max available mem size periodically", K(ret));
    } else if (updated && OB_FAIL(sql_mem_processor_.update_used_mem_size(mem_context_->used()))) {
      LOG_WARN("failed to update used memory size", K(ret));
    } else if (GCONF.is_sql_operator_dump_enabled()) {
      if (rows_.count() >= MAX_ROW_CNT) {
        if (OB_FAIL(do_dump())) {
          LOG_WARN("dump failed", K(ret));
        }
      } else if (need_dump()) {
        bool dumped = false;
        if (OB_FAIL(preprocess_dump(dumped))) {
          LOG_WARN("failed preprocess dump", K(ret));
        } else if (dumped && OB_FAIL(do_dump())) {
          LOG_WARN("dump failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && sorted_) {
    if (!need_rewind_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not add row after sort if no need rewind", K(ret));
    } else {
      sorted_ = false;
      // add null sentry row
      if (!rows_.empty() && NULL != rows_.at(rows_.count() - 1)) {
        if (OB_FAIL(rows_.push_back(NULL))) {
          LOG_WARN("array push back failed", K(ret), K(rows_.count()));
        }
      }
    }
  }

  // row should be added after dumping to make sure %store_row memory available.
  if (OB_SUCC(ret)) {
    ObChunkDatumStore::StoredRow* sr = NULL;
    if (OB_FAIL(datum_store_.add_row(input_row, &sr))) {
      LOG_WARN("add store row failed", K(ret), K(mem_context_->used()), K(get_memory_limit()));
    } else {
      inmem_row_size_ += sr->row_size_;
      store_row = sr;
      if (local_merge_sort_ && rows_.count() > 0 && NULL != rows_.at(rows_.count() - 1)) {
        const bool less = comp_(sr, rows_.at(rows_.count() - 1));
        if (OB_SUCCESS != comp_.ret_) {
          ret = comp_.ret_;
          LOG_WARN("compare failed", K(ret));
        } else if (less) {
          // If new is less than previous row, add NULL to separate different local order rows.
          if (OB_FAIL(rows_.push_back(NULL))) {
            LOG_WARN("array push back failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(rows_.push_back(sr))) {
          LOG_WARN("array push back failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSortOpImpl::do_dump()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (rows_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(sort_inmem_data())) {
    LOG_WARN("sort in-memory data failed", K(ret));
  } else {
    const int64_t level = 0;
    if (!need_imms()) {
      int64_t pos = 0;
      auto input = [&](ObChunkDatumStore*& rs, const ObChunkDatumStore::StoredRow*& row) {
        int ret = OB_SUCCESS;
        if (pos >= rows_.count()) {
          ret = OB_ITER_END;
        } else {
          row = rows_.at(pos);
          rs = &datum_store_;
          pos += 1;
        }
        return ret;
      };
      if (OB_FAIL(build_chunk(level, input))) {
        LOG_WARN("build chunk failed", K(ret));
      }
    } else {
      auto input = [&](ObChunkDatumStore*& rs, const ObChunkDatumStore::StoredRow*& row) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(imms_heap_next(row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get row from memory heap failed", K(ret));
          }
        } else {
          rs = &datum_store_;
        }
        return ret;
      };
      if (OB_FAIL(build_chunk(level, input))) {
        LOG_WARN("build chunk failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      sql_mem_processor_.set_number_pass(level + 1);
      heap_iter_begin_ = false;
      row_idx_ = 0;
      rows_.reset();
      datum_store_.reset();
      inmem_row_size_ = 0;
      mem_check_interval_mask_ = 1;
      sql_mem_processor_.reset();
    }
  }
  return ret;
}

int ObSortOpImpl::build_ems_heap(int64_t& merge_ways)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (sort_chunks_.get_size() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty or one way, merge sort not needed", K(ret));
  } else if (OB_FAIL(sql_mem_processor_.get_max_available_mem_size(&mem_context_->get_malloc_allocator()))) {
    LOG_WARN("failed to get max available memory size", K(ret));
  } else {
    ObSortOpChunk* first = sort_chunks_.get_first();
    if (first->level_ != first->get_next()->level_) {
      LOG_TRACE("only one chunk in current level, move to next level directly", K(first->level_));
      first->level_ += first->get_next()->level_;
    }
    int64_t max_ways = 1;
    ObSortOpChunk* c = first->get_next();
    // get max merge ways in same level
    for (int64_t i = 0;
         first->level_ == c->level_ && i < std::min(sort_chunks_.get_size(), (int32_t)MAX_MERGE_WAYS) - 1;
         i++) {
      max_ways += 1;
      c = c->get_next();
    }
    merge_ways = get_memory_limit() / ObChunkDatumStore::BLOCK_SIZE;
    merge_ways = std::max(2L, merge_ways);
    merge_ways = std::min(merge_ways, max_ways);
    LOG_TRACE("do merge sort", K(first->level_), K(merge_ways), K(sort_chunks_.get_size()));

    if (NULL == ems_heap_) {
      if (OB_ISNULL(
              ems_heap_ = OB_NEWx(
                  EMSHeap, (&mem_context_->get_malloc_allocator()), comp_, &mem_context_->get_malloc_allocator()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      }
    } else {
      ems_heap_->reset();
    }
    if (OB_SUCC(ret)) {
      ObSortOpChunk* chunk = sort_chunks_.get_first();
      for (int64_t i = 0; i < merge_ways && OB_SUCC(ret); i++) {
        chunk->iter_.reset();
        if (OB_FAIL(chunk->iter_.init(&chunk->datum_store_))) {
          LOG_WARN("init iterator failed", K(ret));
        } else if (OB_FAIL(chunk->iter_.get_next_row(chunk->row_)) || NULL == chunk->row_) {
          if (OB_ITER_END == ret || OB_SUCCESS == ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row store is not empty, iterate end is unexpected", K(ret), KP(chunk->row_));
          }
          LOG_WARN("get next row failed", K(ret));
        } else if (OB_FAIL(ems_heap_->push(chunk))) {
          LOG_WARN("heap push failed", K(ret));
        } else {
          chunk = chunk->get_next();
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    heap_iter_begin_ = false;
  }
  return ret;
}

template <typename Heap, typename NextFunc, typename Item>
int ObSortOpImpl::heap_next(Heap& heap, const NextFunc& func, Item& item)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (heap_iter_begin_) {
      if (!heap.empty()) {
        Item it = heap.top();
        bool is_end = false;
        if (OB_FAIL(func(it, is_end))) {
          LOG_WARN("get next item fail");
        } else {
          if (is_end) {
            if (OB_FAIL(heap.pop())) {
              LOG_WARN("heap pop failed", K(ret));
            }
          } else {
            if (OB_FAIL(heap.replace_top(it))) {
              LOG_WARN("heap replace failed", K(ret));
            }
          }
        }
      }
    } else {
      heap_iter_begin_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (heap.empty()) {
      ret = OB_ITER_END;
    } else {
      item = heap.top();
    }
  }
  return ret;
}

int ObSortOpImpl::ems_heap_next(ObSortOpChunk*& chunk)
{
  const auto f = [](ObSortOpChunk*& c, bool& is_end) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(c->iter_.get_next_row(c->row_))) {
      if (OB_ITER_END == ret) {
        is_end = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next row failed", K(ret));
      }
    }
    return ret;
  };
  return heap_next(*ems_heap_, f, chunk);
}

int ObSortOpImpl::imms_heap_next(const ObChunkDatumStore::StoredRow*& store_row)
{
  ObChunkDatumStore::StoredRow** sr = NULL;
  const auto f = [](ObChunkDatumStore::StoredRow**& r, bool& is_end) {
    r += 1;
    is_end = (NULL == *r);
    return OB_SUCCESS;
  };

  int ret = heap_next(*imms_heap_, f, sr);
  if (OB_SUCC(ret)) {
    store_row = *sr;
  }
  return ret;
}

int ObSortOpImpl::sort_inmem_data()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!rows_.empty()) {
    if (local_merge_sort_ || sorted_) {
      // row already in order, do nothing.
    } else {
      int64_t begin = 0;
      if (need_imms()) {
        // is increment sort (rows add after sort()), sort the last add rows
        for (int64_t i = rows_.count() - 1; i >= 0; i--) {
          if (NULL == rows_.at(i)) {
            begin = i + 1;
            break;
          }
        }
      }
      std::sort(&rows_.at(begin), &rows_.at(0) + rows_.count(), CopyableComparer(comp_));
      if (OB_SUCCESS != comp_.ret_) {
        ret = comp_.ret_;
        LOG_WARN("compare failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && need_imms()) {
      if (NULL == imms_heap_) {
        if (OB_ISNULL(
                imms_heap_ = OB_NEWx(
                    IMMSHeap, (&mem_context_->get_malloc_allocator()), comp_, &mem_context_->get_malloc_allocator()))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        }
      } else {
        imms_heap_->reset();
      }
      // add null sentry row first
      if (OB_FAIL(ret)) {
      } else if (NULL != rows_.at(rows_.count() - 1) && OB_FAIL(rows_.push_back(NULL))) {
        LOG_WARN("array push back failed", K(ret));
      } else {
        int64_t merge_ways = rows_.count() - datum_store_.get_row_cnt();
        LOG_TRACE("do local merge sort ways", K(merge_ways), K(rows_.count()), K(datum_store_.get_row_cnt()));
        if (merge_ways > INMEMORY_MERGE_SORT_WARN_WAYS) {
          // only log warning msg
          LOG_WARN("too many merge ways", K(ret), K(merge_ways), K(rows_.count()), K(datum_store_.get_row_cnt()));
        }
        ObChunkDatumStore::StoredRow** prev = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && i < rows_.count(); i++) {
          if (NULL == prev || NULL == *prev) {
            if (OB_FAIL(imms_heap_->push(&rows_.at(i)))) {
              LOG_WARN("heap push back failed", K(ret));
            }
          }
          prev = &rows_.at(i);
        }
        heap_iter_begin_ = false;
      }
    }
  }
  return ret;
}

int ObSortOpImpl::sort()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!rows_.empty()) {
    // in memory sort
    if (sort_chunks_.is_empty()) {
      iter_.reset();
      if (OB_FAIL(sort_inmem_data())) {
        LOG_WARN("sort in-memory data failed", K(ret));
      } else if (OB_FAIL(iter_.init(&datum_store_))) {
        LOG_WARN("init iterator failed", K(ret));
      } else {
        if (!need_imms()) {
          row_idx_ = 0;
          next_stored_row_func_ = &ObSortOpImpl::array_next_stored_row;
        } else {
          next_stored_row_func_ = &ObSortOpImpl::imms_heap_next_stored_row;
        }
      }
    } else if (OB_FAIL(do_dump())) {
      LOG_WARN("dump failed");
    }
  }
  if (OB_SUCC(ret) && !sort_chunks_.is_empty()) {
    // do merge sort
    int64_t ways = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(build_ems_heap(ways))) {
        LOG_WARN("build heap failed", K(ret));
      } else {
        // last merge round,
        if (ways == sort_chunks_.get_size()) {
          break;
        }
        auto input = [&](ObChunkDatumStore*& rs, const ObChunkDatumStore::StoredRow*& row) {
          int ret = OB_SUCCESS;
          ObSortOpChunk* chunk = NULL;
          if (OB_FAIL(ems_heap_next(chunk))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next heap row failed", K(ret));
            }
          } else if (NULL == chunk) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get chunk from heap is NULL", K(ret));
          } else {
            rs = &chunk->datum_store_;
            row = chunk->row_;
          }
          return ret;
        };
        const int64_t level = sort_chunks_.get_first()->level_ + 1;
        if (OB_FAIL(build_chunk(level, input))) {
          LOG_WARN("build chunk failed", K(ret));
        } else {
          sql_mem_processor_.set_number_pass(level + 1);
          for (int64_t i = 0; i < ways; i++) {
            ObSortOpChunk* c = sort_chunks_.remove_first();
            c->~ObSortOpChunk();
            mem_context_->get_malloc_allocator().free(c);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      next_stored_row_func_ = &ObSortOpImpl::ems_heap_next_stored_row;
    }
  }
  return ret;
}

int ObSortOpImpl::array_next_stored_row(const ObChunkDatumStore::StoredRow*& sr)
{
  int ret = OB_SUCCESS;
  if (row_idx_ >= rows_.count()) {
    ret = OB_ITER_END;
  } else {
    sr = rows_.at(row_idx_);
    row_idx_ += 1;
  }
  return ret;
}

int ObSortOpImpl::imms_heap_next_stored_row(const ObChunkDatumStore::StoredRow*& sr)
{
  return imms_heap_next(sr);
}

int ObSortOpImpl::ems_heap_next_stored_row(const ObChunkDatumStore::StoredRow*& sr)
{
  int ret = OB_SUCCESS;
  ObSortOpChunk* chunk = NULL;
  if (OB_FAIL(ems_heap_next(chunk))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next heap row failed", K(ret));
    }
  } else if (NULL == chunk || NULL == chunk->row_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL chunk or store row", K(ret));
  } else {
    sr = chunk->row_;
  }
  return ret;
}
int ObSortOpImpl::rewind()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!need_rewind_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inited with non rewind support", K(ret));
  } else {
    if (&ObSortOpImpl::array_next_stored_row == next_stored_row_func_) {
      row_idx_ = 0;
    } else {
      if (OB_FAIL(sort())) {
        LOG_WARN("sort failed", K(ret));
      }
    }
  }
  return ret;
}
/************************************* end ObSortOpImpl ********************************/

/*********************************** start ObPrefixSortImpl *****************************/
ObPrefixSortImpl::ObPrefixSortImpl()
    : prefix_pos_(0),
      full_sort_collations_(nullptr),
      full_sort_cmp_funs_(nullptr),
      base_sort_collations_(),
      base_sort_cmp_funs_(),
      prev_row_(nullptr),
      next_prefix_row_store_(),
      next_prefix_row_(nullptr),
      child_(nullptr),
      self_op_(nullptr),
      sort_row_count_(nullptr)
{}

void ObPrefixSortImpl::reset()
{
  prefix_pos_ = 0;
  full_sort_collations_ = nullptr;
  base_sort_collations_.reset();
  base_sort_cmp_funs_.reset();
  next_prefix_row_store_.reset();
  next_prefix_row_ = nullptr;
  prev_row_ = nullptr;
  child_ = nullptr;
  self_op_ = nullptr;
  sort_row_count_ = nullptr;
  ObSortOpImpl::reset();
}

int ObPrefixSortImpl::init(const int64_t tenant_id, const int64_t prefix_pos,
    const common::ObIArray<ObExpr*>& all_exprs, const ObIArray<ObSortFieldCollation>* sort_collations,
    const ObIArray<ObSortCmpFunc>* sort_cmp_funs, ObEvalCtx* eval_ctx, ObOperator* child_op, ObOperator* self_op,
    ObExecContext& exec_ctx, int64_t& sort_row_cnt)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_ISNULL(sort_collations) || OB_ISNULL(sort_cmp_funs) ||
             OB_ISNULL(eval_ctx) || OB_ISNULL(child_op) || OB_ISNULL(self_op) || prefix_pos <= 0 ||
             prefix_pos > sort_collations->count() || sort_collations->count() != sort_cmp_funs->count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(prefix_pos));
  } else {
    prefix_pos_ = prefix_pos;
    full_sort_collations_ = sort_collations;
    full_sort_cmp_funs_ = sort_cmp_funs;
    // NOTE: %cnt may be zero, some plan is wrong generated with prefix sort:
    // %prefix_pos == %sort_columns.count(), the sort operator should be eliminated but not.
    //
    // To be compatible with this plan, we keep this behavior.
    const int64_t cnt = sort_collations->count() - prefix_pos;
    base_sort_collations_.init(cnt, const_cast<ObSortFieldCollation*>(&sort_collations->at(0) + prefix_pos), cnt);
    base_sort_cmp_funs_.init(cnt, const_cast<ObSortCmpFunc*>(&sort_cmp_funs->at(0) + prefix_pos), cnt);
    prev_row_ = nullptr;
    next_prefix_row_ = nullptr;
    child_ = child_op;
    self_op_ = self_op;
    exec_ctx_ = &exec_ctx;
    sort_row_count_ = &sort_row_cnt;
    if (OB_FAIL(ObSortOpImpl::init(tenant_id, &base_sort_collations_, &base_sort_cmp_funs_, eval_ctx))) {
      LOG_WARN("sort impl init failed", K(ret));
    } else if (OB_FAIL(next_prefix_row_store_.init(mem_context_->get_malloc_allocator(), all_exprs.count()))) {
      LOG_WARN("failed to init next prefix row store", K(ret));
    } else if (OB_FAIL(fetch_rows(all_exprs))) {
      LOG_WARN("fetch rows failed");
    }
  }
  return ret;
}

int ObPrefixSortImpl::fetch_rows(const common::ObIArray<ObExpr*>& all_exprs)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSortOpImpl::reuse();
    int64_t row_count = 0;
    prev_row_ = NULL;
    if (NULL != next_prefix_row_) {
      // Restore next_prefix_row_ to expressions, to make sure no overwrite of expression's value
      // when get row from child.
      row_count += 1;
      if (OB_FAIL(next_prefix_row_store_.restore(all_exprs, *eval_ctx_))) {
        LOG_WARN("restore expr values failed", K(ret));
      } else if (OB_FAIL(add_row(all_exprs, prev_row_))) {
        LOG_WARN("add row to sort impl failed", K(ret));
      } else if (OB_ISNULL(prev_row_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("add stored row is NULL", K(ret));
      } else {
        next_prefix_row_ = NULL;
        LOG_DEBUG("trace restore row", K(ObToStringExprRow(*eval_ctx_, all_exprs)));
      }
    }
    while (OB_SUCC(ret)) {
      self_op_->clear_evaluated_flag();
      if (OB_FAIL(child_->get_next_row())) {
        if (OB_ITER_END == ret) {
          // Set %next_prefix_row_ to NULL to indicate that all rows are fetched.
          next_prefix_row_ = NULL;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next row failed", K(ret));
        }
        break;
      } else {
        *sort_row_count_ += 1;
        row_count += 1;
        // check the prefix is the same with previous row
        bool same_prefix = true;
        if (NULL != prev_row_) {
          const ObDatum* rcells = prev_row_->cells();
          ObDatum* l_datum = nullptr;
          for (int64_t i = 0; same_prefix && i < prefix_pos_ && OB_SUCC(ret); i++) {
            const int64_t idx = full_sort_collations_->at(i).field_idx_;
            if (OB_FAIL(all_exprs.at(idx)->eval(*eval_ctx_, l_datum))) {
              LOG_WARN("failed to eval expr", K(ret));
            } else {
              same_prefix = (0 == full_sort_cmp_funs_->at(i).cmp_func_(*l_datum, rcells[idx]));
            }
          }
        }
        if (!same_prefix) {
          // row are saved in %next_prefix_row_, will be added in the next call
          if (OB_FAIL(next_prefix_row_store_.shadow_copy(all_exprs, *eval_ctx_))) {
            LOG_WARN("failed to add datum row", K(ret));
          } else {
            next_prefix_row_ = next_prefix_row_store_.get_store_row();
          }
          break;
        }
        if (OB_FAIL(add_row(all_exprs, prev_row_))) {
          LOG_WARN("add row to sort impl failed", K(ret));
        } else if (OB_ISNULL(prev_row_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("add stored row is NULL", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && row_count > 0) {
      if (OB_FAIL(ObSortOpImpl::sort())) {
        LOG_WARN("sort rows failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPrefixSortImpl::get_next_row(const common::ObIArray<ObExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_FAIL(ObSortOpImpl::get_next_row(exprs))) {
      if (OB_ITER_END == ret) {
        if (NULL != next_prefix_row_) {
          if (OB_FAIL(fetch_rows(exprs))) {
            LOG_WARN("fetch rows failed", K(ret));
          } else if (OB_FAIL(ObSortOpImpl::get_next_row(exprs))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("sort impl get next row failed", K(ret));
            }
          }
        }
      } else {
        LOG_WARN("sort impl get next row failed", K(ret));
      }
    }
  }
  return ret;
}
/*********************************** end ObPrefixSortImpl *****************************/

/*********************************** start ObUniqueSortImpl *****************************/
int ObUniqueSortImpl::get_next_row(const common::ObIArray<ObExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow* sr = NULL;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(ObSortOpImpl::get_next_row(exprs, sr))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row failed");
      }
      break;
    } else if (NULL == sr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL store row returned", K(ret));
    } else {
      if (NULL != prev_row_) {
        const ObDatum* lcells = prev_row_->cells();
        const ObDatum* rcells = sr->cells();
        int cmp = 0;
        for (int64_t i = 0; 0 == cmp && i < sort_cmp_funs_->count(); i++) {
          const int64_t idx = sort_collations_->at(i).field_idx_;
          cmp = sort_cmp_funs_->at(i).cmp_func_(lcells[idx], rcells[idx]);
        }
        if (0 == cmp) {
          continue;
        }
      }
      if (OB_FAIL(save_prev_row(*sr))) {
        LOG_WARN("save prev row failed", K(ret));
      }
      break;
    }
  }
  return ret;
}

int ObUniqueSortImpl::get_next_stored_row(const ObChunkDatumStore::StoredRow*& sr)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(ObSortOpImpl::get_next_row(sr))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row failed");
      }
      break;
    } else if (NULL == sr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL store row returned", K(ret));
    } else {
      if (NULL != prev_row_) {
        const ObDatum* lcells = prev_row_->cells();
        const ObDatum* rcells = sr->cells();
        int cmp = 0;
        for (int64_t i = 0; 0 == cmp && i < sort_cmp_funs_->count(); i++) {
          const int64_t idx = sort_collations_->at(i).field_idx_;
          cmp = sort_cmp_funs_->at(i).cmp_func_(lcells[idx], rcells[idx]);
        }
        if (0 == cmp) {
          continue;
        }
      }
      if (OB_FAIL(save_prev_row(*sr))) {
        LOG_WARN("save prev row failed", K(ret));
      }
      break;
    }
  }
  return ret;
}

void ObUniqueSortImpl::free_prev_row()
{
  if (NULL != prev_row_ && NULL != mem_context_) {
    mem_context_->get_malloc_allocator().free(prev_row_);
    prev_row_ = NULL;
    prev_buf_size_ = 0;
  }
}

void ObUniqueSortImpl::reuse()
{
  free_prev_row();
  ObSortOpImpl::reuse();
}

void ObUniqueSortImpl::reset()
{
  free_prev_row();
  ObSortOpImpl::reset();
}

int ObUniqueSortImpl::save_prev_row(const ObChunkDatumStore::StoredRow& sr)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_UNLIKELY(sr.row_size_ > prev_buf_size_)) {
      free_prev_row();
      // allocate more memory to avoid too much memory alloc times.
      const int64_t size = sr.row_size_ * 2;
      prev_row_ = static_cast<ObChunkDatumStore::StoredRow*>(mem_context_->get_malloc_allocator().alloc(size));
      if (NULL == prev_row_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        prev_buf_size_ = size;
        prev_row_ = new (prev_row_) ObChunkDatumStore::StoredRow();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(prev_row_->assign(&sr))) {
        LOG_WARN("store row assign failed", K(ret));
      }
    }
  }
  return ret;
}
/*********************************** end ObUniqueSortImpl *****************************/

/***************************** start ObInMemoryTopnSortImpl ***************************/
ObInMemoryTopnSortImpl::ObInMemoryTopnSortImpl()
    : prefix_pos_(0),
      topn_cnt_(INT64_MAX),
      topn_sort_array_pos_(0),
      is_fetch_with_ties_(false),
      iter_end_(false),
      last_row_(NULL),
      sort_collations_(nullptr),
      sort_cmp_funs_(nullptr),
      eval_ctx_(nullptr),
      cmp_(),
      cur_alloc_(ObModIds::OB_SQL_SORT_ROW, OB_MALLOC_NORMAL_BLOCK_SIZE, OB_SERVER_TENANT_ID, ObCtxIds::WORK_AREA),
      heap_(cmp_, &cur_alloc_)
{}

ObInMemoryTopnSortImpl::~ObInMemoryTopnSortImpl()
{
  reset();
}

void ObInMemoryTopnSortImpl::reset()
{
  last_row_ = NULL;
  topn_sort_array_pos_ = 0;
  topn_cnt_ = 0;
  prefix_pos_ = 0;
  sort_collations_ = nullptr;
  sort_cmp_funs_ = nullptr;
  eval_ctx_ = nullptr;
  heap_.reset();
  cur_alloc_.reset();
  is_fetch_with_ties_ = false;
  iter_end_ = false;
}

void ObInMemoryTopnSortImpl::reuse()
{
  heap_.reset();
  cur_alloc_.reset();
  last_row_ = NULL;
  topn_sort_array_pos_ = 0;
  topn_cnt_ = 0;
  is_fetch_with_ties_ = false;
  iter_end_ = false;
}

int ObInMemoryTopnSortImpl::init(const int64_t tenant_id, const int64_t prefix_pos,
    const ObIArray<ObSortFieldCollation>* sort_collations, const ObIArray<ObSortCmpFunc>* sort_cmp_funs,
    ObEvalCtx* eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sort_collations) || OB_ISNULL(sort_cmp_funs) || OB_ISNULL(eval_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort info is null", K(sort_collations), K(sort_cmp_funs), K(eval_ctx));
  } else if (sort_collations->count() != sort_cmp_funs->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort info is not match", K(sort_collations->count()), K(sort_cmp_funs->count()));
  } else if (cmp_.init(sort_collations, sort_cmp_funs)) {
    LOG_WARN("failed to init compare functions", K(ret));
  } else {
    cur_alloc_.set_tenant_id(tenant_id);
    prefix_pos_ = prefix_pos;
    sort_collations_ = sort_collations;
    sort_cmp_funs_ = sort_cmp_funs;
    eval_ctx_ = eval_ctx;
  }
  return ret;
}

int ObInMemoryTopnSortImpl::check_block_row(
    const common::ObIArray<ObExpr*>& exprs, const SortStoredRow* last_row, bool& is_cur_block)
{
  int ret = OB_SUCCESS;
  is_cur_block = true;
  if (!has_prefix_pos() || NULL == last_row) {
    // do nothing, need add row
    // 1) no prefix sort, need sort all data, belong to same group
    // 2) first row belongs to the "same" group
  } else {
    int cmp = 0;
    const ObDatum* lcells = last_row->cells();
    ObDatum* other_datum = nullptr;
    for (int64_t i = 0; 0 == cmp && OB_SUCC(ret) && i < prefix_pos_; i++) {
      int64_t idx = sort_collations_->at(i).field_idx_;
      if (OB_FAIL(exprs.at(idx)->eval(*eval_ctx_, other_datum))) {
        LOG_WARN("failed to eval expr", K(ret));
      } else {
        cmp = sort_cmp_funs_->at(i).cmp_func_(lcells[idx], *other_datum);
        if ((cmp > 0 && sort_collations_->at(i).is_ascending_) ||
            (cmp < 0 && !sort_collations_->at(i).is_ascending_)) {  // check prefix_sort_columns
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("prefix sort ordering is invalid", K(ret), K(*last_row), K(sort_collations_->at(i)));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (0 == cmp) {  // row is equal last_row with prefix sort keys
      } else {
        is_cur_block = false;
      }
    }
  }
  return ret;
}

int ObInMemoryTopnSortImpl::add_row(const common::ObIArray<ObExpr*>& exprs, bool& need_sort)
{
  int ret = OB_SUCCESS;
  bool is_cur_block_row = true;
  if (topn_cnt_ <= 0) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(check_block_row(exprs, last_row_, is_cur_block_row))) {
    LOG_WARN("check if row is cur block failed", K(ret));
  } else if (!is_cur_block_row && get_row_count() >= topn_cnt_) {
    need_sort = true;                                                    // no need add row
  } else if (!is_fetch_with_ties_ && heap_.count() == get_topn_cnt()) {  // adjust heap
    if (OB_FAIL(adjust_topn_heap(exprs))) {
      LOG_WARN("failed to adjust topn heap", K(ret));
    }
  } else {  // push back array
    SortStoredRow* new_row = NULL;
    char* buf = NULL;
    // optimize for hit-rate: enlarge first Limit-Count row's space,
    // so following rows are more likely to fit in.
    int64_t row_size = 0;
    if (OB_FAIL(ObChunkDatumStore::row_copy_size(exprs, *eval_ctx_, row_size))) {
      LOG_WARN("failed to calc copy size", K(ret));
    } else {
      int64_t buffer_len = STORE_ROW_HEADER_SIZE + 2 * row_size + STORE_ROW_EXTRA_SIZE;
      if (OB_ISNULL(buf = reinterpret_cast<char*>(cur_alloc_.alloc(buffer_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc buf failed", K(ret));
      } else if (OB_ISNULL(new_row = new (buf) SortStoredRow())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to new row", K(ret));
      } else {
        int64_t pos = STORE_ROW_HEADER_SIZE;
        if (OB_FAIL(new_row->copy_datums(
                exprs, *eval_ctx_, buf + pos, buffer_len - STORE_ROW_HEADER_SIZE, row_size, STORE_ROW_EXTRA_SIZE))) {
          LOG_WARN("failed to deep copy row", K(ret), K(buffer_len));
        } else if (OB_FAIL(heap_.push(new_row))) {
          LOG_WARN("failed to push back row", K(ret), K(buffer_len));
        } else {
          // Must be set last, because if store row is not assigned,
          // like cnt_ and row_size are both 0, there is a problem with obtaining extra_info
          new_row->set_max_size(buffer_len);
          last_row_ = new_row;
          // LOG_TRACE("in memory topn sort check add row",
          //   KPC(new_row), K(buffer_len), K(row_size), K(new_row->get_max_size()));
        }
      }
    }
  }
  return ret;
}

int ObInMemoryTopnSortImpl::adjust_topn_heap(const common::ObIArray<ObExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(heap_.top())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error.top of the heap is NULL", K(ret), K(topn_sort_array_pos_), K(heap_.count()));
  } else if (!heap_.empty()) {
    if (cmp_(&exprs, heap_.top(), *eval_ctx_)) {
      SortStoredRow* new_row = NULL;
      SortStoredRow* dt_row = heap_.top();
      char* buf = NULL;
      int64_t row_size = 0;
      int64_t buffer_len = 0;
      if (OB_FAIL(ObChunkDatumStore::row_copy_size(exprs, *eval_ctx_, row_size))) {
        LOG_WARN("failed to calc copy size", K(ret));
      } else {
        // check to see whether this old row's space is adequate for new one
        if (dt_row->get_max_size() >= row_size + STORE_ROW_HEADER_SIZE + STORE_ROW_EXTRA_SIZE) {
          buf = reinterpret_cast<char*>(dt_row);
          new_row = dt_row;
          buffer_len = dt_row->get_max_size();
        } else {
          buffer_len = row_size * 2 + STORE_ROW_HEADER_SIZE + STORE_ROW_EXTRA_SIZE;
          if (OB_ISNULL(buf = reinterpret_cast<char*>(cur_alloc_.alloc(buffer_len)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("alloc buf failed", K(ret));
          } else if (OB_ISNULL(new_row = new (buf) SortStoredRow())) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("failed to new row", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        int64_t pos = STORE_ROW_HEADER_SIZE;
        if (OB_FAIL(new_row->copy_datums(
                exprs, *eval_ctx_, buf + pos, buffer_len - STORE_ROW_HEADER_SIZE, row_size, STORE_ROW_EXTRA_SIZE))) {
          LOG_WARN("failed to deep copy row", K(ret), K(buffer_len), K(row_size));
        } else if (OB_FAIL(heap_.replace_top(new_row))) {
          LOG_WARN("failed to replace top", K(ret));
        } else {
          new_row->set_max_size(buffer_len);
          last_row_ = new_row;
          // LOG_TRACE("in memory topn sort check replace row", KPC(new_row),
          //   K(buffer_len), K(row_size), K(new_row->get_max_size()));
        }
      }
    } else {
      ret = cmp_.ret_;
    }
  }
  return ret;
}

int ObInMemoryTopnSortImpl::sort_rows()
{
  int ret = OB_SUCCESS;
  if (0 == heap_.count()) {
    // do nothing
  } else if (!is_fetch_with_ties_ && get_topn_cnt() < heap_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("topn is less than array_count", K(ret), K(get_topn_cnt()), K(heap_.count()));
  } else {
    LOG_DEBUG("in memory topn sort check topn heap", K_(heap));
    SortStoredRow** first_row = &heap_.top();
    std::sort(first_row, first_row + heap_.count(), ObSortOpImpl::CopyableComparer(cmp_));
  }
  return ret;
}

int ObInMemoryTopnSortImpl::get_next_row(const common::ObIArray<ObExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  if (topn_sort_array_pos_ < 0) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("sort array out of range", K(ret), K_(topn_sort_array_pos));
  } else if (topn_sort_array_pos_ >= heap_.count()) {
    ret = OB_ITER_END;
    SQL_ENG_LOG(DEBUG, "end of the in-memory run");
    if (topn_sort_array_pos_ > 0) {
      // Reset status when iterating end
      //  because we will add rows and sort again after dumped to disk.
      topn_sort_array_pos_ = 0;
      heap_.reset();
      cur_alloc_.reset();
      last_row_ = nullptr;
    }
  } else if (OB_ISNULL(heap_.at(topn_sort_array_pos_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. row is null", K(ret), K(topn_sort_array_pos_), K(heap_.count()));
  } else {
    const SortStoredRow* store_row = heap_.at(topn_sort_array_pos_);
    if (OB_FAIL(convert_row(store_row, exprs))) {
      LOG_WARN("fail to get row", K(ret));
    } else {
      ++topn_sort_array_pos_;
    }
  }
  return ret;
}

int ObInMemoryTopnSortImpl::convert_row(const SortStoredRow* sr, const common::ObIArray<ObExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: store row is null", K(ret));
  } else {
    for (uint32_t i = 0; i < sr->cnt_; ++i) {
      exprs.at(i)->locate_expr_datum(*eval_ctx_) = sr->cells()[i];
      exprs.at(i)->get_eval_info(*eval_ctx_).evaluated_ = true;
    }
  }
  return ret;
}
/***************************** end ObInMemoryTopnSortImpl ****************************/

}  // end namespace sql
}  // end namespace oceanbase
