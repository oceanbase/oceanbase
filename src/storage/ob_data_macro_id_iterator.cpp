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

#include "ob_data_macro_id_iterator.h"
#include "ob_partition_service.h"
#include "ob_sstable.h"
#include "ob_pg_partition.h"

namespace oceanbase {
using namespace blocksstable;
namespace storage {

ObDataMacroIdIterator::ObDataMacroIdIterator()
    : is_inited_(false),
      partition_service_(nullptr),
      cur_store_idx_(0),
      cur_sstable_idx_(0),
      cur_macro_idx_(0),
      cur_meta_macro_idx_(0),
      cur_partition_idx_(0),
      partition_iter_(NULL),
      stores_handle_(),
      sstables_(),
      macro_blocks_(NULL),
      normal_data_block_count_(0),
      bf_macro_block_id_()
{}

ObDataMacroIdIterator::~ObDataMacroIdIterator()
{
  if (NULL != partition_iter_ && nullptr != partition_service_) {
    partition_service_->revert_pg_partition_iter(partition_iter_);
  }
}

int ObDataMacroIdIterator::init(ObPartitionService& partition_service, ObIPartitionGroup* pg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDataMacroIdIterator has already been inited", K(ret));
  } else {

    if (nullptr == pg) {
      if (OB_ISNULL(partition_iter_ = partition_service.alloc_pg_partition_iter())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to alloc partition iter, ", K(ret));
      }
    } else {
      ObSinglePGPartitionIterator* single_pg_iter = nullptr;
      if (OB_ISNULL(single_pg_iter = partition_service.alloc_single_pg_partition_iter())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc single partition iterator", K(ret));
      } else if (OB_FAIL(single_pg_iter->init(pg))) {
        STORAGE_LOG(WARN, "fail to init single pg iterator", K(ret));
      } else {
        partition_iter_ = single_pg_iter;
        single_pg_iter = nullptr;
      }

      if (nullptr != single_pg_iter) {
        partition_service.revert_pg_partition_iter(single_pg_iter);
      }
    }

    if (OB_SUCC(ret)) {
      partition_service_ = &partition_service;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDataMacroIdIterator::locate(
    const int64_t last_partition_idx, const int64_t last_sstable_idx, const int64_t last_macro_idx)
{
  int ret = OB_SUCCESS;
  if (last_partition_idx < 0 || last_sstable_idx < 0 || last_macro_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(last_partition_idx), K(last_sstable_idx), K(last_macro_idx));
  }
  ObPGPartition* pg_partition = nullptr;
  while (OB_SUCC(ret) && cur_partition_idx_ < last_partition_idx) {
    if (OB_FAIL(partition_iter_->get_next(pg_partition))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next pg", K(ret));
      }
    } else {
      ++cur_partition_idx_;
    }
  }
  if (OB_SUCC(ret)) {
    cur_sstable_idx_ = last_sstable_idx;
    cur_macro_idx_ = last_macro_idx;
  }
  return ret;
}

int ObDataMacroIdIterator::get_last_idx(int64_t& last_partition_idx, int64_t& last_sstable_idx, int64_t& last_macro_idx)
{
  int ret = OB_SUCCESS;
  last_partition_idx = cur_partition_idx_;
  last_sstable_idx = cur_sstable_idx_;
  last_macro_idx = cur_macro_idx_;
  return ret;
}

int ObDataMacroIdIterator::get_next_macro_id(
    MacroBlockId& block_id, ObMacroBlockCommonHeader::MacroBlockType& block_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDataMacroIdIterator has not been inited", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (NULL != macro_blocks_ && cur_macro_idx_ < macro_blocks_->count()) {
        if (cur_macro_idx_ < normal_data_block_count_) {
          block_type = ObMacroBlockCommonHeader::SSTableData;
        } else {
          block_type = ObMacroBlockCommonHeader::LobData;
        }
        block_id = macro_blocks_->at(cur_macro_idx_++);
        break;
      } else if (bf_macro_block_id_.is_valid()) {
        block_id = bf_macro_block_id_;
        block_type = ObMacroBlockCommonHeader::BloomFilterData;
        bf_macro_block_id_.reset();
        break;
      } else {
        // macro block iter end
        cur_macro_idx_ = 0;
        macro_blocks_ = NULL;
        if (cur_sstable_idx_ < sstables_.count()) {
          macro_blocks_ = &(sstables_.at(cur_sstable_idx_)->get_total_macro_blocks());
          normal_data_block_count_ = sstables_.at(cur_sstable_idx_)->get_macro_block_ids().count();
          bf_macro_block_id_ = sstables_.at(cur_sstable_idx_)->get_bloom_filter_block_id();
          cur_sstable_idx_++;
        } else {
          // sstable iter end
          cur_sstable_idx_ = 0;
          sstables_.reuse();
          // ssstore iter end
          stores_handle_.reset();
          ObPGPartition* pg_partition = nullptr;
          ObIPartitionStorage* storage = nullptr;
          if (OB_FAIL(partition_iter_->get_next(pg_partition))) {
            if (OB_ITER_END != ret) {
              STORAGE_LOG(WARN, "Fail to get next partition, ", K(ret));
            }
          } else if (OB_ISNULL(pg_partition)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "The cur partition is NULL, ", K(ret));
          } else if (OB_ISNULL(storage = pg_partition->get_storage())) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "partition storage is null", K(ret));
          } else if (OB_FAIL(storage->get_all_tables(stores_handle_))) {
            STORAGE_LOG(WARN, "failed to get all tables", K(ret));
          } else if (OB_FAIL(stores_handle_.get_all_sstables(sstables_))) {
            STORAGE_LOG(WARN, "failed to get all sstables", K(ret));
          } else {
            ++cur_partition_idx_;
          }
        }
      }
    }
  }
  return ret;
}

int ObDataMacroIdIterator::get_next_macro_info(blocksstable::ObMacroBlockInfoPair& pair, ObTenantFileKey& file_key)
{
  int ret = OB_SUCCESS;
  blocksstable::ObMacroBlockCommonHeader::MacroBlockType block_type;
  if (OB_FAIL(get_next_macro_id(pair.block_id_, block_type))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to get next macro id", K(ret));
    }
  } else if (OB_FAIL(sstables_.at(cur_sstable_idx_ - 1)->get_meta(pair.block_id_, pair.meta_))) {
    STORAGE_LOG(WARN, "fail to get meta", K(ret));
  } else if (OB_FAIL(sstables_.at(cur_sstable_idx_ - 1)->get_tenant_file_key(file_key))) {
    STORAGE_LOG(WARN, "fail to get tenant file key", K(ret), K_(cur_sstable_idx));
  } else if (OB_UNLIKELY(!file_key.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "file key is invalid", K(ret), K_(cur_sstable_idx));
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
