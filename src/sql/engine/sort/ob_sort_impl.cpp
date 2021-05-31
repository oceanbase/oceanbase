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

#include "ob_sort_impl.h"
#include "ob_base_sort.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObSortImpl::Compare::Compare()
    : ret_(OB_SUCCESS), sort_columns_(NULL), indexes_(NULL), null_poses_(NULL), cmp_funcs_(NULL)
{}

int ObSortImpl::Compare::init(
    ObArenaAllocator& alloc, const ObNewRow& row, const SortColumns* sort_columns, const SortExtraInfos* extra_infos)
{
  int ret = OB_SUCCESS;
  bool is_static_cmp = false;
  if (NULL == sort_columns) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sort_columns), KP(extra_infos));
  } else if (NULL != extra_infos && extra_infos->count() != sort_columns->count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column count miss match", K(ret), K(*sort_columns), K(*extra_infos));
  } else if (NULL != extra_infos) {
    const int64_t prefix_pos = 0;
    if (OB_FAIL(ObBaseSort::enable_typed_sort(*sort_columns, prefix_pos, row, is_static_cmp))) {
      LOG_WARN("check enable typed sort failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t cnt = sort_columns->count();
    if (cnt > 0) {
      if (OB_ISNULL(indexes_ = static_cast<int32_t*>(alloc.alloc(sizeof(*indexes_) * cnt))) ||
          OB_ISNULL(null_poses_ = static_cast<ObCmpNullPos*>(alloc.alloc(sizeof(*null_poses_) * cnt)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (is_static_cmp) {
        cmp_funcs_ = static_cast<obj_cmp_func_nullsafe*>(alloc.alloc(sizeof(*cmp_funcs_) * cnt));
        if (OB_ISNULL(cmp_funcs_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        }
      }
    }

    for (int64_t i = 0; i < cnt && OB_SUCC(ret); i++) {
      // cell index
      int32_t idx = sort_columns->at(i).index_;
      if (row.projector_size_ > 0 && (NULL == row.projector_ || idx >= row.projector_size_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid idx", K(ret), K(idx), K(row));
      } else {
        if (row.projector_size_ > 0) {
          idx = row.projector_[idx];
        }
        indexes_[i] = idx;
      }

      // null position
      if (OB_SUCC(ret)) {
        if (NULL != extra_infos) {
          bool null_first = extra_infos->at(i).is_null_first();
          null_poses_[i] = (null_first ^ sort_columns->at(i).is_ascending()) ? NULL_LAST : NULL_FIRST;
        } else {
          // Use default NULL order of each db mode if no extra_infos
          null_poses_[i] = default_null_pos();
        }
      }
      // compare function
      if (OB_SUCC(ret) && is_static_cmp) {
        obj_cmp_func_nullsafe func = NULL;
        if (!ObObjCmpFuncs::can_cmp_without_cast(
                extra_infos->at(i).obj_type_, extra_infos->at(i).obj_type_, common::CO_CMP, func) ||
            (OB_ISNULL(func))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get cmp function failed", K(ret), K(extra_infos->at(i)), KP(func));
        } else {
          cmp_funcs_[i] = func;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    sort_columns_ = sort_columns;
    cmp_ctx_.cmp_type_ = ObMaxType;
    cmp_ctx_.cmp_cs_type_ = CS_TYPE_INVALID;
    cmp_ctx_.is_null_safe_ = true;
    cmp_ctx_.tz_off_ = INVALID_TZ_OFF;
  }
  return ret;
}

bool ObSortImpl::Compare::operator()(const ObChunkRowStore::StoredRow* l, const ObChunkRowStore::StoredRow* r)
{
  bool less = false;
  int& ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    // already fail
  } else if (!is_inited() || OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = !is_inited() ? OB_NOT_INIT : OB_INVALID_ARGUMENT;
    LOG_WARN("not init or invalid argument", K(ret), KP(sort_columns_), KP(l), KP(r));
  } else {
    const ObObj* lcells = l->cells();
    const ObObj* rcells = r->cells();
    int cmp = 0;
    if (NULL == cmp_funcs_) {
      for (int64_t i = 0; 0 == cmp && i < sort_columns_->count(); i++) {
        cmp_ctx_.null_pos_ = null_poses_[i];
        cmp_ctx_.cmp_cs_type_ = sort_columns_->at(i).cs_type_;
        const int64_t idx = indexes_[i];
        cmp = lcells[idx].compare(rcells[idx], cmp_ctx_);
        if (cmp < 0) {
          less = sort_columns_->at(i).is_ascending();
        } else if (cmp > 0) {
          less = !sort_columns_->at(i).is_ascending();
        }
      }
    } else {
      for (int64_t i = 0; 0 == cmp && i < sort_columns_->count(); i++) {
        const int64_t idx = indexes_[i];
        cmp = cmp_funcs_[i](lcells[idx], rcells[idx], sort_columns_->at(i).cs_type_, null_poses_[i]);
        if (cmp < 0) {
          less = sort_columns_->at(i).is_ascending();
        } else if (cmp > 0) {
          less = !sort_columns_->at(i).is_ascending();
        }
      }
    }
  }
  return less;
}

// compare function for external merge sort
bool ObSortImpl::Compare::operator()(const ObSortChunk* l, const ObSortChunk* r)
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
    // NOTE: can not return !(*this)(l->row_, r->row_), because we should always return false if l == r.
    less = (*this)(r->row_, l->row_);
  }
  return less;
}

bool ObSortImpl::Compare::operator()(ObChunkRowStore::StoredRow** l, ObChunkRowStore::StoredRow** r)
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
    // NOTE: can not return !(*this)(l->row_, r->row_), because we should always return false if l == r.
    less = (*this)(*r, *l);
  }
  return less;
}

ObSortImpl::ObSortImpl()
    : inited_(false),
      local_merge_sort_(false),
      need_rewind_(false),
      got_first_row_(false),
      sorted_(false),
      mem_context_(NULL),
      mem_entify_guard_(mem_context_),
      tenant_id_(OB_INVALID_ID),
      sort_columns_(NULL),
      extra_infos_(NULL),
      inmem_row_size_(0),
      mem_check_interval_mask_(1),
      row_idx_(0),
      heap_iter_begin_(false),
      imms_heap_(NULL),
      ems_heap_(NULL),
      next_row_func_(&ObSortImpl::array_next_row),
      input_rows_(OB_INVALID_ID),
      input_width_(OB_INVALID_ID),
      profile_(ObSqlWorkAreaType::SORT_WORK_AREA),
      sql_mem_processor_(profile_),
      op_type_(PHY_INVALID),
      op_id_(UINT64_MAX),
      exec_ctx_(nullptr)
{}

ObSortImpl::~ObSortImpl()
{
  reset();
}

// Set the note in ObPrefixSort::init(): %sort_columns may be zero, to compatible with
// the wrong generated prefix sort.
int ObSortImpl::init(const uint64_t tenant_id, const SortColumns& sort_columns, const SortExtraInfos* extra_infos,
    const bool in_local_order /* = false */, const bool need_rewind /* = false */)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(sort_columns));
  } else {
    local_merge_sort_ = in_local_order;
    need_rewind_ = need_rewind;
    tenant_id_ = tenant_id;
    sort_columns_ = &sort_columns;
    extra_infos_ = extra_infos;
    lib::ContextParam param;
    param.set_mem_attr(tenant_id, ObModIds::OB_SQL_SORT_ROW, ObCtxIds::WORK_AREA)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (NULL == mem_context_ && OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("create entity failed", K(ret));
    } else if (NULL == mem_context_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null memory entity returned", K(ret));
    } else if (OB_FAIL(row_store_.init(INT64_MAX /* mem limit, big enough to hold all rows in memory */,
                   tenant_id_,
                   ObCtxIds::WORK_AREA,
                   ObModIds::OB_SQL_SORT_ROW,
                   false /*+ disable dump */,
                   ObChunkRowStore::FULL))) {
      LOG_WARN("init row store failed", K(ret));
    } else {
      rows_.set_block_allocator(ModulePageAllocator(mem_context_->get_malloc_allocator(), "SortImplRows"));
      row_store_.set_allocator(mem_context_->get_malloc_allocator());
      inited_ = true;
    }
  }

  return ret;
}

void ObSortImpl::reuse()
{
  sorted_ = false;
  iter_.reset();
  rows_.reuse();
  row_store_.reuse();
  inmem_row_size_ = 0;
  mem_check_interval_mask_ = 1;
  row_idx_ = 0;
  next_row_func_ = &ObSortImpl::array_next_row;
  while (!sort_chunks_.is_empty()) {
    ObSortChunk* chunk = sort_chunks_.remove_first();
    chunk->~ObSortChunk();
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

void ObSortImpl::unregister_profile()
{
  sql_mem_processor_.unregister_profile();
}

void ObSortImpl::reset()
{
  sql_mem_processor_.unregister_profile();
  iter_.reset();
  reuse();
  rows_.reset();
  row_store_.reset();
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
    // can not destroy mem_entify here, the memory may hold by %iter_ or %row_store_
  }
  inited_ = false;
}

template <typename Input>
int ObSortImpl::build_chunk(const int64_t level, Input& input)
{
  int ret = OB_SUCCESS;
  ObChunkRowStore* rs = NULL;
  const ObChunkRowStore::StoredRow* row = NULL;
  ObSortChunk* chunk = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(chunk = OB_NEWx(ObSortChunk, (&mem_context_->get_malloc_allocator()), level))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(chunk->row_store_.init(1 /*+ mem limit, small limit for dump immediately */,
                 tenant_id_,
                 ObCtxIds::WORK_AREA,
                 ObModIds::OB_SQL_SORT_ROW,
                 true /*+ enable dump */,
                 ObChunkRowStore::FULL))) {
    LOG_WARN("init row store failed", K(ret));
  } else {
    chunk->row_store_.set_dir_id(sql_mem_processor_.get_dir_id());
    chunk->row_store_.set_allocator(mem_context_->get_malloc_allocator());
    chunk->row_store_.set_callback(&sql_mem_processor_);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(input(rs, row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get input row failed", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
        break;
      } else if (OB_FAIL(chunk->row_store_.copy_row(row, rs))) {
        LOG_WARN("copy row to row store failed");
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(chunk->row_store_.dump(false, true))) {
      LOG_WARN("failed to dump row store", K(ret));
    } else if (OB_FAIL(chunk->row_store_.finish_add_row(true /*+ need dump */))) {
      LOG_WARN("finish add row failed", K(ret));
    } else {
      LOG_TRACE("dump sort file",
          "level",
          level,
          "rows",
          chunk->row_store_.get_row_cnt(),
          "file_size",
          chunk->row_store_.get_file_size(),
          "memory_hold",
          chunk->row_store_.get_mem_hold(),
          "mem_used",
          mem_context_->used());
    }
  }

  if (OB_SUCC(ret)) {
    // In increase sort, chunk->level_ may less than the last of sort chunks.
    // insert the chunk to the upper bound the level.
    ObSortChunk* pos = sort_chunks_.get_last();
    for (; pos != sort_chunks_.get_header() && pos->level_ > level; pos = pos->get_prev()) {}
    pos = pos->get_next();
    if (!sort_chunks_.add_before(pos, chunk)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add link node to list failed", K(ret));
    }
  }
  if (OB_SUCCESS != ret && NULL != chunk) {
    chunk->~ObSortChunk();
    mem_context_->get_malloc_allocator().free(chunk);
    chunk = NULL;
  }

  return ret;
}

int ObSortImpl::preprocess_dump(bool& dumped)
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
        // one-pass
        if (profile_.get_cache_size() <= row_store_.get_mem_hold() + row_store_.get_file_size()) {
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

int ObSortImpl::add_row(const common::ObNewRow& row, const ObChunkRowStore::StoredRow*& store_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!got_first_row_)) {
    if (OB_FAIL(comp_.init(mem_context_->get_arena_allocator(), row, sort_columns_, extra_infos_))) {
      LOG_WARN("update sort column idx failed", K(ret));
    } else {
      got_first_row_ = true;
      int64_t size = OB_INVALID_ID == input_rows_ ? 0 : input_rows_ * input_width_;
      if (OB_FAIL(sql_mem_processor_.init(
              &mem_context_->get_malloc_allocator(), tenant_id_, size, op_type_, op_id_, exec_ctx_))) {
        LOG_WARN("failed to init sql mem processor", K(ret));
      } else {
        row_store_.set_dir_id(sql_mem_processor_.get_dir_id());
        row_store_.set_callback(&sql_mem_processor_);
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
        // Maximum 2G
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
    ObChunkRowStore::StoredRow* sr = NULL;
    if (OB_FAIL(row_store_.add_row(row, &sr))) {
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

int ObSortImpl::do_dump()
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
      auto input = [&](ObChunkRowStore*& rs, const ObChunkRowStore::StoredRow*& row) {
        int ret = OB_SUCCESS;
        if (pos >= rows_.count()) {
          ret = OB_ITER_END;
        } else {
          row = rows_.at(pos);
          rs = &row_store_;
          pos += 1;
        }
        return ret;
      };
      if (OB_FAIL(build_chunk(level, input))) {
        LOG_WARN("build chunk failed", K(ret));
      }
    } else {
      auto input = [&](ObChunkRowStore*& rs, const ObChunkRowStore::StoredRow*& row) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(imms_heap_next(row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get row from memory heap failed", K(ret));
          }
        } else {
          rs = &row_store_;
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
      row_store_.reuse();
      inmem_row_size_ = 0;
      mem_check_interval_mask_ = 1;
      sql_mem_processor_.reset();
    }
  }
  return ret;
}

int ObSortImpl::build_ems_heap(int64_t& merge_ways)
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
    ObSortChunk* first = sort_chunks_.get_first();
    if (first->level_ != first->get_next()->level_) {
      LOG_TRACE("only one chunk in current level, move to next level directly", K(first->level_));
      first->level_ += first->get_next()->level_;
    }
    int64_t max_ways = 1;
    ObSortChunk* c = first->get_next();
    // get max merge ways in same level
    for (int64_t i = 0;
         first->level_ == c->level_ && i < std::min(sort_chunks_.get_size(), (int32_t)MAX_MERGE_WAYS) - 1;
         i++) {
      max_ways += 1;
      c = c->get_next();
    }
    merge_ways = get_memory_limit() / ObChunkRowStore::BLOCK_SIZE;
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
      ObSortChunk* chunk = sort_chunks_.get_first();
      for (int64_t i = 0; i < merge_ways && OB_SUCC(ret); i++) {
        chunk->iter_.reset();
        if (OB_FAIL(chunk->iter_.init(&chunk->row_store_, ObChunkRowStore::BLOCK_SIZE))) {
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
int ObSortImpl::heap_next(Heap& heap, const NextFunc& func, Item& item)
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

int ObSortImpl::ems_heap_next(ObSortChunk*& chunk)
{
  const auto f = [](ObSortChunk*& c, bool& is_end) {
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

int ObSortImpl::imms_heap_next(const ObChunkRowStore::StoredRow*& store_row)
{
  ObChunkRowStore::StoredRow** sr = NULL;
  const auto f = [](ObChunkRowStore::StoredRow**& r, bool& is_end) {
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

int ObSortImpl::sort_inmem_data()
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
        int64_t merge_ways = rows_.count() - row_store_.get_row_cnt();
        LOG_TRACE("do local merge sort ways", K(merge_ways), K(rows_.count()), K(row_store_.get_row_cnt()));
        if (merge_ways > INMEMORY_MERGE_SORT_WARN_WAYS) {
          // only log warning msg
          LOG_WARN("too many merge ways", K(ret), K(merge_ways), K(rows_.count()), K(row_store_.get_row_cnt()));
        }
        ObChunkRowStore::StoredRow** prev = NULL;
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

int ObSortImpl::sort()
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
      } else if (OB_FAIL(iter_.init(&row_store_, ObChunkRowStore::BLOCK_SIZE))) {
        LOG_WARN("init iterator failed", K(ret));
      } else {
        if (!need_imms()) {
          row_idx_ = 0;
          next_row_func_ = &ObSortImpl::array_next_row;
        } else {
          next_row_func_ = &ObSortImpl::imms_heap_next_row;
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
        auto input = [&](ObChunkRowStore*& rs, const ObChunkRowStore::StoredRow*& row) {
          int ret = OB_SUCCESS;
          ObSortChunk* chunk = NULL;
          if (OB_FAIL(ems_heap_next(chunk))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next heap row failed", K(ret));
            }
          } else if (NULL == chunk) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get chunk from heap is NULL", K(ret));
          } else {
            rs = &chunk->row_store_;
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
            ObSortChunk* c = sort_chunks_.remove_first();
            c->~ObSortChunk();
            mem_context_->get_malloc_allocator().free(c);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      next_row_func_ = &ObSortImpl::ems_heap_next_row;
    }
  }
  return ret;
}

int ObSortImpl::array_next_row(const common::ObNewRow*& row, const ObChunkRowStore::StoredRow*& sr)
{
  int ret = OB_SUCCESS;
  if (row_idx_ >= rows_.count()) {
    ret = OB_ITER_END;
  } else {
    sr = rows_.at(row_idx_);
    row_idx_ += 1;
    if (OB_FAIL(iter_.convert_to_row(sr, *const_cast<ObNewRow**>(&row)))) {
      LOG_WARN("convert to row failed", K(ret));
    }
  }
  return ret;
}

int ObSortImpl::imms_heap_next_row(const common::ObNewRow*& row, const ObChunkRowStore::StoredRow*& sr)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(imms_heap_next(sr))) {
    if (OB_FAIL(iter_.convert_to_row(sr, *const_cast<ObNewRow**>(&row)))) {
      LOG_WARN("convert to row failed", K(ret));
    }
  }
  return ret;
}

int ObSortImpl::ems_heap_next_row(const common::ObNewRow*& row, const ObChunkRowStore::StoredRow*& sr)
{
  int ret = OB_SUCCESS;
  ObSortChunk* chunk = NULL;
  if (OB_FAIL(ems_heap_next(chunk))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next heap row failed", K(ret));
    }
  } else if (NULL == chunk || NULL == chunk->row_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL chunk or store row", K(ret));
  } else if (OB_FAIL(chunk->iter_.convert_to_row(chunk->row_, *const_cast<ObNewRow**>(&row)))) {
    LOG_WARN("convert to row failed", K(ret));
  } else {
    sr = chunk->row_;
  }
  return ret;
}

int ObSortImpl::rewind()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!need_rewind_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inited with non rewind support", K(ret));
  } else {
    if (&ObSortImpl::array_next_row == next_row_func_) {
      row_idx_ = 0;
    } else {
      if (OB_FAIL(sort())) {
        LOG_WARN("sort failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSortImpl::get_sort_columns(common::ObIArray<ObSortColumn>& sort_columns)
{
  int ret = OB_SUCCESS;
  if (!is_inited() || OB_ISNULL(sort_columns_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort_ isn't inited", K(ret));
  } else if (OB_FAIL(append(sort_columns, *sort_columns_))) {
    LOG_WARN("failed to append sort columns", K(ret));
  } else { /*do nothing */
  }
  return ret;
}

int ObUniqueSort::get_next_row(const common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  const ObChunkRowStore::StoredRow* sr = NULL;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(ObSortImpl::get_next_row(row, sr))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row failed");
      }
      break;
    } else if (NULL == sr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL store row returned", K(ret));
    } else {
      if (NULL != prev_row_) {
        int cmp = 0;
        for (int64_t i = 0; 0 == cmp && i < sort_columns_->count(); i++) {
          const int64_t idx = comp_.indexes_[i];
          cmp = sr->cells()[idx].compare(prev_row_->cells()[idx], sort_columns_->at(i).cs_type_);
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

void ObUniqueSort::free_prev_row()
{
  if (NULL != prev_row_ && NULL != mem_context_) {
    mem_context_->get_malloc_allocator().free(prev_row_);
    prev_row_ = NULL;
    prev_buf_size_ = 0;
  }
}

void ObUniqueSort::reuse()
{
  free_prev_row();
  ObSortImpl::reuse();
}

void ObUniqueSort::reset()
{
  free_prev_row();
  ObSortImpl::reset();
}

int ObUniqueSort::save_prev_row(const ObChunkRowStore::StoredRow& sr)
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
      prev_row_ = static_cast<ObChunkRowStore::StoredRow*>(mem_context_->get_malloc_allocator().alloc(size));
      if (NULL == prev_row_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        prev_buf_size_ = size;
        prev_row_ = new (prev_row_) ObChunkRowStore::StoredRow();
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

}  // end namespace sql
}  // end namespace oceanbase
