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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_interm_result.h"
#include "sql/executor/ob_task_event.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "share/config/ob_server_config.h"
#include "sql/executor/ob_interm_result_pool.h"

namespace oceanbase {
namespace sql {
using namespace common;

ObIntermResult::ObIntermResult()
    : cur_scanner_(NULL),
      data_(),
      rows_is_completed_(false),
      expire_time_(-1),
      found_rows_(0),
      last_insert_id_session_(0),
      is_result_accurate_(true),
      matched_rows_(0),
      duplicated_rows_(0),
      fd_(-1),
      dir_id_(-1),
      offset_(0),
      row_reclaim_func_(NULL)
{
  cnt_and_state_.cnt_ = 0;
  cnt_and_state_.state_ = STATE_NORMAL;
  ir_item_pool_ = ObIntermResultItemPool::get_instance();
  if (OB_ISNULL(ir_item_pool_)) {
    LOG_ERROR("unexpected, global interm result item pool is NULL");
  }
  affected_rows_ = 0;
}

ObIntermResult::~ObIntermResult()
{
  reset();
}

void ObIntermResult::reset()
{
  int ret = OB_SUCCESS;
  reclaim_rows();
  rows_is_completed_ = false;
  expire_time_ = -1;
  found_rows_ = 0;
  last_insert_id_session_ = 0;
  is_result_accurate_ = true;
  matched_rows_ = 0;
  duplicated_rows_ = 0;
  ObIIntermResultItem* cur_ir_item = NULL;
  // ir_item_pool_ need not set to NULL
  if (OB_I(t1) OB_ISNULL(ir_item_pool_)) {
    LOG_ERROR("unexpected, interm result item pool is NULL");
  } else {
    while (OB_SUCCESS == data_.pop_back(cur_ir_item)) {
      if (OB_ISNULL(cur_ir_item)) {
        LOG_ERROR("interm result item in data_ is NULL");
      } else {
        cur_ir_item->reset();
        ir_item_pool_->free(cur_ir_item);
      }
    }
  }
  data_.reset();
  if (is_disk_store_opened()) {
    if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.remove(fd_))) {
      LOG_WARN("close disk store file failed", K(ret), K_(fd));
    } else {
      LOG_INFO("close disk store file success", K_(fd));
    }
    fd_ = -1;
    dir_id_ = -1;
  }
  offset_ = 0;
  free_scanner();
  if (OB_FAIL(try_end_recycle())) {
    LOG_DEBUG("fail to end recycle, maybe it has not begin recycling", K(ret));
  }
  affected_rows_ = 0;
}

int ObIntermResult::try_inc_cnt()
{
  int ret = OB_SUCCESS;
  AtomicCntAndState atomic_old = {0};
  AtomicCntAndState atomic_new = {0};
  AtomicCntAndState atomic_cmp = {0};
  bool cas_succ = false;
  while (OB_SUCC(ret) && false == cas_succ) {
    atomic_old.atomic_ = cnt_and_state_.atomic_;
    atomic_cmp.atomic_ = atomic_old.atomic_;
    atomic_new.atomic_ = atomic_old.atomic_;
    if (OB_I(t1)(STATE_NORMAL != atomic_cmp.state_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_DEBUG("the interm result is being recycled, fail to increase it's reference count", K(ret));
    } else {
      atomic_new.cnt_ += 1;
      if (atomic_old.atomic_ == ATOMIC_VCAS(&(cnt_and_state_.atomic_), atomic_cmp.atomic_, atomic_new.atomic_)) {
        cas_succ = true;
      }
    }
  }
  return ret;
}

int ObIntermResult::try_dec_cnt()
{
  int ret = OB_SUCCESS;
  AtomicCntAndState atomic_old = {0};
  AtomicCntAndState atomic_new = {0};
  AtomicCntAndState atomic_cmp = {0};
  bool cas_succ = false;
  while (OB_SUCC(ret) && false == cas_succ) {
    atomic_old.atomic_ = cnt_and_state_.atomic_;
    atomic_cmp.atomic_ = atomic_old.atomic_;
    atomic_new.atomic_ = atomic_old.atomic_;
    if (OB_I(t1)(STATE_NORMAL != atomic_cmp.state_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_DEBUG("the interm result is being recycled, fail to decrease it's reference count", K(ret));
    } else {
      atomic_new.cnt_ -= 1;
      if (atomic_old.atomic_ == ATOMIC_VCAS(&(cnt_and_state_.atomic_), atomic_cmp.atomic_, atomic_new.atomic_)) {
        cas_succ = true;
      }
    }
  }
  return ret;
}

int ObIntermResult::try_begin_recycle()
{
  int ret = OB_SUCCESS;
  AtomicCntAndState atomic_old = {0};
  AtomicCntAndState atomic_new = {0};
  AtomicCntAndState atomic_cmp = {0};
  bool cas_succ = false;
  while (OB_SUCC(ret) && false == cas_succ) {
    atomic_old.atomic_ = cnt_and_state_.atomic_;
    atomic_cmp.atomic_ = atomic_old.atomic_;
    atomic_new.atomic_ = atomic_old.atomic_;
    atomic_new.state_ = STATE_RECYCLE;
    if (OB_I(t1)(STATE_NORMAL != atomic_cmp.state_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_DEBUG("the iterm result is already in recycling, fail to set state to STATE_RECYCLE", K(ret));
    } else if (0 != atomic_cmp.cnt_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_DEBUG("the reference count is not 0, fail to set state to STATE_RECYCLE", K(ret));
    } else if (atomic_old.atomic_ == ATOMIC_VCAS(&(cnt_and_state_.atomic_), atomic_cmp.atomic_, atomic_new.atomic_)) {
      cas_succ = true;
    }
  }
  return ret;
}

int ObIntermResult::try_end_recycle()
{
  int ret = OB_SUCCESS;
  AtomicCntAndState atomic_old = {0};
  AtomicCntAndState atomic_new = {0};
  AtomicCntAndState atomic_cmp = {0};
  bool cas_succ = false;
  while (OB_SUCC(ret) && false == cas_succ) {
    atomic_old.atomic_ = cnt_and_state_.atomic_;
    atomic_cmp.atomic_ = atomic_old.atomic_;
    atomic_new.atomic_ = atomic_old.atomic_;
    atomic_new.state_ = STATE_NORMAL;
    if (OB_I(t1)(STATE_RECYCLE != atomic_cmp.state_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_DEBUG("the iterm result has not begin recycling, fail to set state to STATE_NORMAL", K(ret));
    } else if (OB_I(t2)(0 != atomic_cmp.cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("the reference count is not 0, fail to set state to STATE_NORMAL", K(ret));
    } else if (atomic_old.atomic_ == ATOMIC_VCAS(&(cnt_and_state_.atomic_), atomic_cmp.atomic_, atomic_new.atomic_)) {
      cas_succ = true;
    }
  }
  return ret;
}

int ObIntermResult::choose_store(bool& disk, const uint64_t tenant_id, const bool force_disk_store) const
{
  int ret = OB_SUCCESS;
  disk = false;  // default memory store
  if (force_disk_store || is_disk_store_opened()) {
    disk = true;
  } else if (!GCONF.is_sql_operator_dump_enabled()) {
  } else {
    const int64_t mem_ctx_pct_trigger = 70;
    lib::ObMallocAllocator* instance = lib::ObMallocAllocator::get_instance();
    lib::ObTenantCtxAllocator* allocator = NULL;
    if (NULL == instance) {
      ret = common::OB_ERR_SYS;
      LOG_ERROR("NULL malloc allocator", K(ret));
    } else if (OB_ISNULL(allocator = instance->get_tenant_ctx_allocator(tenant_id, common::ObCtxIds::WORK_AREA))) {
      // no tenant allocator, do nothing
    } else {
      const int64_t limit = allocator->get_limit();
      const int64_t hold = allocator->get_hold();
      if (limit / 100 * mem_ctx_pct_trigger <= hold) {
        disk = true;
      }
      LOG_TRACE("choose store for interm result", K(tenant_id), K(limit), K(hold), K(disk));
    }
  }
  return ret;
}

int ObIntermResult::alloc_ir_item(ObIIntermResultItem*& item, const uint64_t tenant_id, const bool force_disk_store)
{
  int ret = OB_SUCCESS;
  bool use_disk_store = false;
  if (OB_ISNULL(ir_item_pool_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item pool is NULL", K(ret));
  } else if (OB_FAIL(choose_store(use_disk_store, tenant_id, force_disk_store))) {
    LOG_WARN("choose store failed", K(ret), K(tenant_id), K(force_disk_store));
  } else {
    item = NULL;
    if (!use_disk_store) {
      ObIntermResultItem* mem_item = NULL;
      if (OB_FAIL(ir_item_pool_->alloc_mem_item(mem_item, tenant_id))) {
        LOG_WARN("alloc memory interm result item failed", K(ret), K(tenant_id));
      } else {
        item = mem_item;
      }
    } else {
      if (!is_disk_store_opened()) {
        if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.alloc_dir(dir_id_))) {
          LOG_WARN("allocate disk store file directory failed", K(ret));
        } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.open(fd_, dir_id_))) {
          LOG_WARN("open disk store file failed", K(ret));
        } else {
          offset_ = 0;
          LOG_INFO("open disk store file success", K_(fd), K_(dir_id));
        }
      }
      ObDiskIntermResultItem* disk_item = NULL;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ir_item_pool_->alloc_disk_item(disk_item, tenant_id, fd_, dir_id_, offset_))) {
        LOG_WARN("alloc disk interm result item failed", K(ret), K(tenant_id), K_(fd), K_(dir_id));
      } else {
        item = disk_item;
      }
    }
  }
  return ret;
}

int ObIntermResult::save_cur_scanner(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ir_item_pool_) || OB_ISNULL(cur_scanner_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("interm result item pool or scanner is NULL", K(ret));
  } else {
    cur_scanner_->set_is_result_accurate(is_result_accurate_);
    bool in_memory = false;
    ObIIntermResultItem* item = NULL;
    do {
      // try to save on disk since the second scanner or fail to save in memory.
      bool force_disk_store = (!data_.empty() || in_memory) && GCONF.is_sql_operator_dump_enabled();
      in_memory = false;
      if (OB_FAIL(alloc_ir_item(item, tenant_id, force_disk_store))) {
        LOG_WARN("fail to alloc interm result item", K(ret), K(tenant_id));
      } else if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("succeed to alloc interm result item, but item is NULL", K(ret), K(tenant_id));
      } else {
        in_memory = item->in_memory();
        if (OB_FAIL(item->from_scanner(*cur_scanner_))) {
          LOG_WARN("fail to assign interm result item from current scanner", K(ret));
          item->reset();
          ir_item_pool_->free(item);
          item = NULL;
        } else {
          if (!in_memory) {
            offset_ += item->get_data_len();
          }
        }
      }
    } while (GCONF.is_sql_operator_dump_enabled() && in_memory &&
             (OB_ALLOCATE_MEMORY_FAILED == ret || OB_TENANT_OUT_OF_MEM == ret));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(data_.push_back(item))) {
      LOG_WARN("fail to push back interm result item", K(ret), KP(item));
      item->reset();
      ir_item_pool_->free(item);
      item = NULL;
    }
  }
  return ret;
}

int ObIntermResult::add_row(uint64_t tenant_id, const common::ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_I(t2)(STATE_NORMAL != cnt_and_state_.state_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the state of the interm result is not STATE_NORMAL, it is not writable.", K(ret));
  } else if (OB_UNLIKELY(rows_is_completed())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("rows is completed, can not add row", K(ret), K(row));
  } else if (OB_FAIL(check_and_init_cur_scanner())) {
    LOG_WARN("fail to check and init current scanner", K(ret));
  } else if (OB_UNLIKELY(OB_SIZE_OVERFLOW == (ret = cur_scanner_->add_row(row)))) {
    if (OB_FAIL(save_cur_scanner(tenant_id))) {
      LOG_WARN("fail to save current scanner", K(ret), K(row), K(tenant_id));
    } else if (OB_FAIL(reset_and_init_cur_scanner())) {
      LOG_WARN("fail to reset and init current scanner", K(ret));
    } else if (OB_FAIL(OB_I(t6) cur_scanner_->add_row(row))) {
      // give lob row second chance and reset mem size limit
      if (OB_SIZE_OVERFLOW == ret) {
        cur_scanner_->set_mem_size_limit(common::ObScanner::DEFAULT_MAX_SERIALIZE_SIZE);
        if (OB_FAIL(reset_and_init_cur_scanner())) {
          LOG_WARN("fail to reset and init current scanner", K(ret));
        } else if (OB_FAIL(cur_scanner_->add_row(row))) {
          LOG_WARN("fail to add big row to new cur scanner", K(ret), K(row));
        }
      } else {
        LOG_WARN("fail to add row to new cur scanner", K(ret), K(row));
      }
    } else {
      // empty
    }
  } else if (OB_FAIL(ret)) {
    LOG_WARN("fail to add row to cur scanner", K(ret), K(row));
  } else {
    // empty
  }
  return ret;
}

int ObIntermResult::complete_add_rows(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(data_.count() < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("data count less than 0", K(ret), K(data_.count()));
  } else {
    if (NULL != cur_scanner_ && !cur_scanner_->is_empty()) {
      cur_scanner_->set_found_rows(get_found_rows());
      cur_scanner_->set_last_insert_id_session(get_last_insert_id_session());
      cur_scanner_->set_row_matched_count(get_matched_rows());
      cur_scanner_->set_row_duplicated_count(get_duplicated_rows());
      if (!is_result_accurate()) {
        cur_scanner_->set_is_result_accurate(is_result_accurate());
      }
      NG_TRACE_EXT(found_rows, OB_ID(found_rows), get_found_rows());
      NG_TRACE_EXT(last_insert_id, OB_ID(last_insert_id), cur_scanner_->get_last_insert_id_session());
      if (OB_FAIL(save_cur_scanner(tenant_id))) {
        LOG_WARN("fail to save last current scanner", K(ret), K(tenant_id));
      } else {
        // all rows have been added, need not reset.
        cur_scanner_->reset();
      }
    }

    if (OB_SUCC(ret) && is_disk_store_opened()) {
      int64_t timeout_ms = 0;
      if (OB_FAIL(ObDiskIntermResultItem::get_timeout(timeout_ms))) {
        LOG_WARN("get timeout failed", K(ret));
      } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.sync(fd_, timeout_ms))) {
        LOG_WARN("sync interm result disk store file failed", K(ret), K_(fd), K(timeout_ms));
      }
    }
  }

  if (OB_SUCC(ret)) {
    rows_is_completed_ = true;
  }

  // When add row complete, free scanner
  free_scanner();

  return ret;
}

int ObIntermResult::try_fetch_single_scanner(ObTaskSmallResult& small_result) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!rows_is_completed())) {
    LOG_ERROR("rows is not completed", K(ret));
  } else if (OB_UNLIKELY(data_.count() > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("scanner count > 1", K(ret), K(data_.count()));
  } else if (0 == data_.count()) {
    small_result.set_affected_rows(get_affected_rows());
    small_result.set_duplicated_rows(get_duplicated_rows());
    small_result.set_matched_rows(get_matched_rows());
    small_result.set_found_rows(get_found_rows());
    small_result.set_last_insert_id(get_last_insert_id_session());
    small_result.set_has_data(true);
    small_result.set_data_len(0);
  } else {  // 1 == data_.count()
    ObIIntermResultItem* single_ir_item = data_.at(0);
    if (OB_ISNULL(single_ir_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("interm result item ptr is NULL", K(ret));
    } else if (OB_UNLIKELY(single_ir_item->get_data_len() > ObTaskSmallResult::MAX_DATA_BUF_LEN)) {
      // do nothing.
    } else if (OB_FAIL(small_result.assign_from_ir_item(*single_ir_item))) {
      LOG_WARN("fail assign single interm result item to small result", K(ret), K(*single_ir_item));
    }
  }
  return ret;
}

int ObIntermResult::get_all_row_count(int64_t& all_row_count)
{
  int ret = OB_SUCCESS;
  int64_t all_count = 0;
  ObIIntermResultItem* ir_item = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < data_.count(); ++i) {
    if (OB_FAIL(data_.at(i, ir_item))) {
      LOG_WARN("fail to get interm result item from data", K(ret), K(i));
    } else if (OB_ISNULL(ir_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("interm result item is NULL", K(ret), K(i));
    } else {
      all_count += ir_item->get_row_count();
    }
  }
  if (OB_SUCC(ret)) {
    all_row_count = all_count;
  }
  return ret;
}

int ObIntermResult::get_all_data_size(int64_t& size) const
{
  int64_t mem = 0;
  int64_t disk = 0;
  int ret = get_data_size_detail(mem, disk);
  if (OB_FAIL(ret)) {
    LOG_WARN("get data size detail failed", K(ret));
  } else {
    size = mem + disk;
  }
  return ret;
}

int ObIntermResult::get_data_size_detail(int64_t& mem, int64_t& disk) const
{
  int ret = OB_SUCCESS;
  mem = 0;
  disk = 0;
  ObIIntermResultItem* ir_item = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < data_.count(); ++i) {
    if (OB_FAIL(data_.at(i, ir_item))) {
      LOG_WARN("fail to get interm result item from data", K(ret), K(i));
    } else if (OB_ISNULL(ir_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("interm result item is NULL", K(ret), K(i));
    } else {
      if (ir_item->in_memory()) {
        mem += ir_item->get_data_len();
      } else {
        disk += ir_item->get_data_len();
      }
    }
  }
  return ret;
}

int ObIntermResult::alloc_scanner()
{
  int ret = OB_SUCCESS;
  if (NULL == cur_scanner_) {
    ObIntermResultPool* pool = ObIntermResultPool::get_instance();
    if (NULL == pool) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("interm result pool is NULL", K(ret));
    } else if (OB_FAIL(pool->alloc_scanner(cur_scanner_))) {
      LOG_WARN("alloc scanner from interm result pool failed", K(ret));
    }
  }
  return ret;
}

void ObIntermResult::free_scanner()
{
  ObIntermResultPool* pool = ObIntermResultPool::get_instance();
  if (NULL != cur_scanner_ && NULL != pool) {
    pool->free_scanner(cur_scanner_);
    cur_scanner_ = NULL;
  }
}

int ObIntermResult::check_and_init_cur_scanner()
{
  int ret = OB_SUCCESS;
  if (NULL == cur_scanner_) {
    if (OB_FAIL(alloc_scanner())) {
      LOG_WARN("alloc scanner failed", K(ret));
    } else if (OB_ISNULL(cur_scanner_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocated scanner is NULL", K(ret));
    } else if (OB_FAIL(cur_scanner_->init())) {
      LOG_WARN("scanner init failed", K(ret));
    }
  }
  return ret;
}

int ObIntermResult::reset_and_init_cur_scanner()
{
  int ret = OB_SUCCESS;
  if (NULL == cur_scanner_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur scanner is NULL", K(ret));
  } else {
    cur_scanner_->reset();
    if (!cur_scanner_->is_inited() && OB_FAIL(cur_scanner_->init())) {
      LOG_WARN("scanner init failed", K(ret));
    }
  }
  return ret;
}

void ObIntermResult::reclaim_rows()
{
  // reclaim rows are description for associated resources, 128 cells is enough.
  const static int64_t max_row_cells_for_reclaim = 128;
  int ret = OB_SUCCESS;
  if (NULL != row_reclaim_func_) {
    FOREACH_CNT(item, data_)
    {  // continue reclaim when error happen, no need to check ret.
      ObIntermResultItem* mem_item = NULL;
      ObIntermResultItem disk2mem_item;
      if (NULL != *item) {
        if ((*item)->in_memory()) {
          mem_item = static_cast<ObIntermResultItem*>(*item);
        } else if (OB_FAIL(disk2mem_item.from_disk_ir_item(*static_cast<ObDiskIntermResultItem*>(*item)))) {
          LOG_WARN("convert disk interm result item to memory interm item failed", K(ret), "item", *item);
        } else {
          mem_item = &disk2mem_item;
        }
      }
      if (NULL != mem_item) {
        ObScanner scanner;
        const ObRowStore& rs = scanner.get_row_store();
        int64_t pos = 0;
        if (OB_FAIL(scanner.deserialize(mem_item->get_data_buf(), mem_item->get_data_len(), pos))) {
          LOG_WARN("fail to deserialize scanner", K(ret), K(pos));
        } else if (rs.get_col_count() <= 0 || rs.get_col_count() >= max_row_cells_for_reclaim) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("col count is invalid or exceed max cells for reclaim",
              K(ret),
              K(rs.get_col_count()),
              LITERAL_K(max_row_cells_for_reclaim));
        } else {
          ObObj objs[rs.get_col_count()];
          ObNewRow row;
          row.cells_ = objs;
          row.count_ = rs.get_col_count();
          ObRowStore::Iterator it = rs.begin();
          while (OB_SUCC(it.get_next_row(row))) {
            row_reclaim_func_(row);
          }
          if (OB_ITER_END != ret) {
            LOG_WARN("get row from row store iterator failed", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
      }
    }

    // after row reclaimed, set reclaim function to NULL
    row_reclaim_func_ = NULL;
  }
}

int ObIntermResult::get_item(const int64_t index, ObIIntermResultItem*& item)
{
  int ret = OB_SUCCESS;
  if (index < 0 || index >= data_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("index out of range", K(index));
  } else {
    item = data_.at(index);
  }
  return ret;
}

ObIntermResultIterator::ObIntermResultIterator()
    : cur_pos_(0), cur_scanner_(NULL), row_store_it_(), ir_(NULL), ir_info_(), has_inc_cnt_(false)
{}

ObIntermResultIterator::~ObIntermResultIterator()
{
  reset();
}

void ObIntermResultIterator::reset()
{
  int ret = OB_SUCCESS;
  if (NULL != ir_) {
    if (has_inc_cnt_) {
      if (OB_FAIL(ir_->try_dec_cnt())) {
        LOG_ERROR("fail to decrease the reference count", K(ret));
      } else {
        has_inc_cnt_ = false;
      }
    } else {
      LOG_ERROR("has not increase reference count");
    }
  }
  ir_info_.reset();
  ir_ = NULL;
  row_store_it_.reset();
  cur_scanner_ = NULL;
  cur_pos_ = 0;
}

int ObIntermResultIterator::set_interm_result(const ObIntermResultInfo& ir_info, ObIntermResult* ir, bool has_inc_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_I(t1) OB_ISNULL(ir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ir is NULL", K(ret));
  } else if (OB_I(t2)(!ir_info.is_init())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ir info is not init", K(ret), K(ir_info));
  } else {
    reset();
    ir_info_ = ir_info;
    ir_ = ir;
    has_inc_cnt_ = has_inc_cnt;
  }
  return ret;
}

int ObIntermResultIterator::get_interm_result_info(ObIntermResultInfo& ir_info)
{
  int ret = OB_SUCCESS;
  if (OB_I(t1)(!ir_info_.is_init())) {
    ret = OB_NOT_INIT;
    LOG_WARN("interm result info is not init", K(ret), K(ir_info));
  } else {
    ir_info = ir_info_;
  }
  return ret;
}

int64_t ObIntermResultIterator::get_scanner_count()
{
  int64_t count = 0;
  if (OB_I(t1) OB_ISNULL(ir_)) {
    LOG_WARN("The interm result iterator has not been initialized!");
  } else {
    count = ir_->data_.count();
  }
  return count;
}

int ObIntermResultIterator::get_next_scanner(ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  ObIIntermResultItem* item = NULL;
  int64_t pos = 0;
  if (OB_I(t1) OB_ISNULL(ir_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The interm result iterator has not been initialized!", K(ret));
  } else if (OB_I(t2)(ObIntermResult::STATE_NORMAL != ir_->cnt_and_state_.state_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the state of the interm result is not STATE_NORMAL, it is not readable.", K(ret));
  } else if (OB_FAIL(ir_->data_.at(cur_pos_++, item))) {
    if (OB_ARRAY_OUT_OF_RANGE == ret) {
      ret = OB_ITER_END;
    } else {
      LOG_WARN("fail to get interm result item", K(ret), K(cur_pos_));
    }
  } else if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("interm result item in data is NULL", K(ret), K(cur_pos_));
  } else {
    scanner.reset();
    ObIntermResultItem* mem_item = NULL;
    ObIntermResultItem disk2mem_item;
    if (item->in_memory()) {
      mem_item = static_cast<ObIntermResultItem*>(item);
    } else {
      if (OB_FAIL(disk2mem_item.from_disk_ir_item(*static_cast<ObDiskIntermResultItem*>(item)))) {
        LOG_WARN("convert disk interm result item to memory interm item failed", K(ret), "item", *item);
      } else {
        mem_item = &disk2mem_item;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(scanner.deserialize(mem_item->get_data_buf(), mem_item->get_data_len(), pos))) {
      LOG_WARN("fail to deserialize scanner", K(ret), K(cur_pos_), K(pos));
    }
  }
  return ret;
}

int ObIntermResultIterator::get_next_interm_result_item(ObIIntermResultItem*& ir_item)
{
  int ret = OB_SUCCESS;
  if (OB_I(t1) OB_ISNULL(ir_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The interm result iterator has not been initialized!", K(ret));
  } else if (OB_I(t2)(ObIntermResult::STATE_NORMAL != ir_->cnt_and_state_.state_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the state of the interm result is not STATE_NORMAL, it is not readable.", K(ret));
  } else if (OB_FAIL(ir_->data_.at(cur_pos_++, ir_item))) {
    if (OB_ARRAY_OUT_OF_RANGE == ret) {
      ret = OB_ITER_END;
    } else {
      LOG_WARN("fail to get interm result item", K(ret), K(cur_pos_));
    }
  } else if (OB_ISNULL(ir_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("interm result item in data is NULL", K(ret), K(cur_pos_));
  }
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
