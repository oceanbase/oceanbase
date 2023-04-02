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

#define USING_LOG_PREFIX STORAGE
#include "ob_sstable_row_multi_exister.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"

namespace oceanbase {
using namespace blocksstable;
namespace storage {

ObSSTableRowMultiExister::~ObSSTableRowMultiExister()
{
  if (NULL != micro_exister_) {
    micro_exister_->~ObMicroBlockRowExister();
    micro_exister_ = NULL;
  }
}

void ObSSTableRowMultiExister::reset()
{
  ObSSTableRowMultiGetter::reset();
  if (NULL != micro_exister_) {
    micro_exister_->~ObMicroBlockRowExister();
    if (access_ctx_ && access_ctx_->allocator_) {
      access_ctx_->allocator_->free(micro_exister_);
    }
    micro_exister_ = NULL;
  }
}

void ObSSTableRowMultiExister::reuse()
{
  ObSSTableRowMultiGetter::reuse();
  if (NULL != micro_exister_) {
    micro_exister_->~ObMicroBlockRowExister();
    if (access_ctx_ && access_ctx_->allocator_) {
      access_ctx_->allocator_->free(micro_exister_);
    }
    micro_exister_ = NULL;
  }
}

int ObSSTableRowMultiExister::fetch_row(ObSSTableReadHandle &read_handle, const ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(exist_row(read_handle, store_row_))) {
    LOG_WARN("Fail to check exist row, ", K(ret));
  } else {
    store_row = &store_row_;
  }
  return ret;
}

int ObSSTableRowMultiExister::exist_row(ObSSTableReadHandle &read_handle, ObDatumRow &store_row)
{
  int ret = OB_SUCCESS;
  switch (read_handle.row_state_) {
    case ObSSTableRowState::NOT_EXIST:
      store_row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
      break;
    case ObSSTableRowState::IN_ROW_CACHE:
      store_row.row_flag_ = read_handle.row_handle_.row_value_->get_flag();
      break;
    case ObSSTableRowState::IN_BLOCK:
      if (OB_FAIL(exist_block_row(read_handle, store_row))) {
        LOG_WARN("Fail to check row exist in block, ", K(ret));
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid row state", K(ret), K(read_handle.row_state_));
  }
  if (OB_SUCC(ret)) {
    store_row.scan_index_ = read_handle.range_idx_;
    LOG_DEBUG("get exist row", K(read_handle.row_state_), K(*read_handle.rowkey_), KP(this));
  }
  return ret;
}

int ObSSTableRowMultiExister::exist_block_row(ObSSTableReadHandle &read_handle, ObDatumRow &store_row)
{
  int ret = OB_SUCCESS;
  read_handle.micro_handle_ = &prefetcher_.current_micro_handle();
  if (nullptr == micro_exister_) {
    if (nullptr == (micro_exister_ = OB_NEWx(ObMicroBlockRowExister, access_ctx_->allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate micro exister, ", K(ret));
    } else if (OB_FAIL(micro_exister_->init(*iter_param_, *access_ctx_, sstable_))) {
      LOG_WARN("Fail to init micro exister, ", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    bool exist = false;
    bool found = false;
    ObMicroBlockData block_data;
    if (OB_FAIL(read_handle.get_block_data(macro_block_reader_, block_data))) {
      LOG_WARN("Fail to get block data", K(ret), K(read_handle));
    } else if (OB_FAIL(micro_exister_->is_exist(
                *read_handle.rowkey_,
                block_data,
                exist,
                found))) {
      LOG_WARN("Fail to get row", K(ret));
    } else {
      if (!found) {
        store_row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
        if (!access_ctx_->query_flag_.is_index_back() && access_ctx_->query_flag_.is_use_bloomfilter_cache() && !sstable_->is_small_sstable()) {
          (void) OB_STORE_CACHE.get_bf_cache().inc_empty_read(
              MTL_ID(),
              iter_param_->table_id_,
              read_handle.micro_handle_->macro_block_id_,
              read_handle.rowkey_->get_datum_cnt());
          if (read_handle.is_bf_contain_) {
            ++access_ctx_->table_store_stat_.bf_empty_read_cnt_;
          }
        }
        ++access_ctx_->table_store_stat_.exist_row_.empty_read_cnt_;
        EVENT_INC(ObStatEventIds::EXIST_ROW_EMPTY_READ);
      } else {
        if (exist) {
          store_row.row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
        } else {
          store_row.row_flag_.set_flag(ObDmlFlag::DF_DELETE);
        }
        ++access_ctx_->table_store_stat_.exist_row_.effect_read_cnt_;
        EVENT_INC(ObStatEventIds::EXIST_ROW_EFFECT_READ);
      }
    }
  }
  return ret;
}

}
}
