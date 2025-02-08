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

#include "lib/utility/ob_sort.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/blocksstable/ob_bloom_filter_load_task.h"
#include "storage/blocksstable/ob_macro_block_meta.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"
#include "storage/access/ob_index_tree_prefetcher.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

namespace oceanbase
{

using namespace common;

namespace blocksstable
{

/**
 * -------------------------------------------- ObBloomFilterLoadKey --------------------------------------------
 */

uint64_t ObBloomFilterLoadKey::hash() const
{
  const uint64_t ls_id_hash_value = ls_id_.hash();
  const uint64_t table_key_hash_value = table_key_.hash();
  const uint64_t hash_value = common::murmurhash(&table_key_hash_value, sizeof(uint64_t), ls_id_hash_value);
  return hash_value;
}

int ObBloomFilterLoadKey::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

bool ObBloomFilterLoadKey::operator == (const ObBloomFilterLoadKey &other) const
{
  return ls_id_ == other.ls_id_ && table_key_ == other.table_key_;
}

bool ObBloomFilterLoadKey::is_valid() const
{
  return ls_id_.is_valid() && table_key_.is_valid();
}

/**
 * -------------------------------------------- ObBloomFilterLoadTaskQueue --------------------------------------------
 */

ObBloomFilterLoadTaskQueue::ValuePair::ValuePair(const ValuePair &src)
{
  macro_id_ = src.macro_id_;
  // Shallow copy
  allocator_ = src.allocator_;
  rowkey_ = src.rowkey_;
}

int ObBloomFilterLoadTaskQueue::ValuePair::alloc_rowkey(const ObCommonDatumRowkey &common_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == allocator_ || nullptr != rowkey_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to set rowkey, null allocator", K(ret), KPC(this), KP(allocator_), KP(rowkey_));
  } else if (OB_UNLIKELY(!common_rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to set rowkey, invalid input", K(ret), K(common_rowkey));
  } else if (OB_ISNULL(rowkey_ = OB_NEWx(ObDatumRowkey, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate rowkey for value pair", K(ret), K(common_rowkey));
  } else if (OB_FAIL(common_rowkey.deep_copy(*rowkey_, *allocator_))) {
    LOG_WARN("fail to deep copy common rowket", K(ret), K(common_rowkey));
  }
  return ret;
}

int ObBloomFilterLoadTaskQueue::ValuePair::recycle_rowkey()
{
  int ret = OB_SUCCESS;
  if (nullptr == rowkey_) {
    // do nothing.
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to recycle rowkey, unexpected ptr", K(ret), KPC(this));
  } else {
    if (nullptr != rowkey_->datums_) {
      rowkey_->datums_->~ObStorageDatum();
      allocator_->free(rowkey_->datums_);
      rowkey_->datums_ = nullptr;
    }
    rowkey_->~ObDatumRowkey();
    allocator_->free(rowkey_);
    rowkey_ = nullptr;
  }
  return ret;
}

bool ObBloomFilterLoadTaskQueue::ValuePairCmpFunc::operator () (const ValuePair &lhs, const ValuePair &rhs)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (OB_FAIL(lhs.rowkey_->compare(*rhs.rowkey_, *datum_utils_, cmp_ret))) {
    LOG_WARN("fail to compare rowkey in ValuePairCmpFunc", K(ret), K(lhs), K(rhs), KPC(datum_utils_));
  }
  return cmp_ret < 0;
}

ObBloomFilterLoadTaskQueue::ObBloomFilterLoadTaskQueue()
    : load_map_(), bucket_lock_(), fetch_queue_(), allocator_(), is_inited_(false)
{
}

ObBloomFilterLoadTaskQueue::~ObBloomFilterLoadTaskQueue()
{
  reset();
}

int ObBloomFilterLoadTaskQueue::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail to init bloom filter load task queue", K(ret), K(is_inited_));
  } else if (OB_FAIL(load_map_.create(1024 /* TODO */, "MaBlkLoadMap", "MaBlkLoadMap", MTL_ID()))) {
    LOG_WARN("failed to init load map", K(ret));
  } else if (OB_FAIL(allocator_.init(lib::ObMallocAllocator::get_instance(),
                                     OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                     ObMemAttr(MTL_ID(), "MaBlkBFLoad", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init allocator", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(
                 bucket_lock_.init(1024 /* TODO */, ObLatchIds::MABLK_BF_LOAD_TASKL_LOCK, "BFLoadTask", MTL_ID()))) {
    LOG_WARN("fail to init bucket lock", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObBloomFilterLoadTaskQueue::reset()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(load_map_.destroy())) {
    LOG_ERROR("failed to destroy bloom filter load tg map", K(ret));
  }
  bucket_lock_.destroy();
  allocator_.reset();
  is_inited_ = false;
}

int ObBloomFilterLoadTaskQueue::push_task(const storage::ObITable::TableKey &sstable_key,
                                          const share::ObLSID &ls_id,
                                          const MacroBlockId &macro_id,
                                          const ObCommonDatumRowkey &common_rowkey)
{
  int ret = OB_SUCCESS;
  const ObBloomFilterLoadKey key(ls_id, sstable_key);
  ValuePair value(macro_id, allocator_);

  // Deep copy value pair.
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to push task, not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!key.is_valid() || !macro_id.is_valid() || !common_rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to push task, invalid argument", K(ret), K(key), K(macro_id), K(common_rowkey));
  } else if (OB_FAIL(value.alloc_rowkey(common_rowkey))) {
    LOG_WARN("fail to set value pair", K(ret), K(macro_id), K(common_rowkey));
  }

  if (OB_FAIL(ret)) {
  } else {
    // Try to append macro id.
    ObBucketHashWLockGuard guard(bucket_lock_, key.hash());
    ObArray<ValuePair> * array = nullptr;
    if (OB_FAIL(load_map_.get_refactored(key, array))) { // Set kv pair.
      if (OB_HASH_NOT_EXIST == ret) {
        array = nullptr;
        KeyLink *key_node = nullptr;
        // Allocate memory and push into fetch queue.
        if (OB_ISNULL(array = OB_NEWx(ObArray<ValuePair>, &allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for macro id array", K(ret), K(sizeof(ObArray<MacroBlockId>)));
        } else if (OB_FAIL(array->push_back(value))) {
          LOG_WARN("fail to push back macro id", K(ret), K(key), K(macro_id), K(common_rowkey));
        } else if (OB_ISNULL(key_node = OB_NEWx(KeyLink, &allocator_, key))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for sstable key node", K(ret), K(key), K(macro_id));
        } else if (OB_FAIL(fetch_queue_.push(key_node))) {
          LOG_WARN("fail to push into fetch queue", K(ret), K(key), K(macro_id), K(common_rowkey));
        }
        // Set pair to map.
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(load_map_.set_refactored(key, array))) {
          LOG_WARN("fail to set sstable key and macro id to load map", K(ret), K(key), K(macro_id));
        }
        // Release memory.
        if (OB_FAIL(ret)) {
          if (nullptr == array) {
            array->~ObArray<ValuePair>();
            allocator_.free(array);
          }
          if (nullptr == key_node) {
            key_node->~KeyLink();
            allocator_.free(key_node);
          }
        }
      } else {
        LOG_WARN("fail to atomic append macro id to load map", K(ret), K(key), K(macro_id), K(common_rowkey));
      }
    } else if (OB_FAIL(array->push_back(value))) { // Append to existed kv pair.
      LOG_WARN("fail to push back to value array", K(ret), K(sstable_key), K(value));
    }
  }

  // Release rowkey if error happen.
  if (OB_FAIL(ret)) {
    if (OB_FAIL(value.recycle_rowkey())) {
      LOG_WARN("fail to recycle rowkey", K(ret));
    }
  }

  return ret;
}

int ObBloomFilterLoadTaskQueue::pop_task(ObBloomFilterLoadKey &load_key, ObArray<ValuePair> *&array)
{
  int ret = OB_SUCCESS;
  ObBloomFilterLoadKey key;
  ObLink * key_node = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to pop task, not inited", K(ret), K(is_inited_));
  } else if (OB_FAIL(fetch_queue_.pop(key_node))) {
    if (OB_EAGAIN == ret) {
      // empty queue, do nothing.
    } else {
      LOG_WARN("fail to pop from fetch queue", K(ret));
    }
  } else if (OB_ISNULL(key_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to pop from fetch queue, unexpected null key node", K(ret), KP(key_node));
  } else if (FALSE_IT(key = (static_cast<KeyLink *>(key_node))->key_)) {
  } else if (FALSE_IT(allocator_.free(key_node))) { // Free fetch queue's memory.
  } else {
    ObArray<ValuePair> * value = nullptr;
    ObBucketHashWLockGuard guard(bucket_lock_, key.hash());
    if (OB_FAIL(load_map_.erase_refactored(key, &value))) {
      if (OB_HASH_NOT_EXIST == ret) {
        // already fetched, do nothing.
      } else {
        LOG_WARN("fail to execute get func", K(ret), K(key));
      }
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, null value", K(ret), KP(value), K(key));
    } else {
      load_key = key;
      array = value;
    }
  }
  return ret;
}

int ObBloomFilterLoadTaskQueue::recycle_array(ObArray<ValuePair> *array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to recycle array", K(ret), KP(array));
  } else {
    for (int64_t tail_idx = array->count() - 1; tail_idx >= 0; --tail_idx) {
      int tmp_ret = OB_SUCCESS;
      MacroBlockId tmp_macro_id;
      ValuePair value(tmp_macro_id, allocator_);
      if (OB_TMP_FAIL(array->pop_back(value))) {
        LOG_WARN("fail to pop back from array", K(tmp_ret), K(tail_idx));
      } else if (OB_TMP_FAIL(value.recycle_rowkey())) {
        LOG_WARN("fail to recycle rowkey", K(tmp_ret), K(value));
      }
    }
    array->~ObArray<ValuePair>();
    allocator_.free(array);
  }
  return ret;
}

/**
 * ------------------------------------------ ObMacroBlockBloomFilterLoadTG ------------------------------------------
 */

ObMacroBlockBloomFilterLoadTG::ObMacroBlockBloomFilterLoadTG()
    : tg_id_(-1), idle_cond_(), load_task_queue_(), allocator_(), is_inited_(false)
{
}

ObMacroBlockBloomFilterLoadTG::~ObMacroBlockBloomFilterLoadTG()
{
  destroy();
}

int ObMacroBlockBloomFilterLoadTG::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail to init bloom filter load tg tg", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::MaBlkBFLoader, tg_id_))) {
    LOG_WARN("fail to create bloom filter load tg thread", K(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE(tg_id_, *this))) {
    LOG_WARN("fail to set bloom filter load tg tg runnable", K(ret));
  } else if (OB_FAIL(load_task_queue_.init())) {
    LOG_WARN("failed to init bloom filter load task queue", K(ret));
  } else if (OB_FAIL(idle_cond_.init(ObWaitEventIds::IO_CONTROLLER_COND_WAIT))) {
    LOG_WARN("fail to init bloom filter load tg idle cond", K(ret));
  } else if (OB_FAIL(allocator_.init(lib::ObMallocAllocator::get_instance(),
                                     OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                     ObMemAttr(MTL_ID(), "BFLoadSecMeta", ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init allocator", K(ret), K(MTL_ID()));
  } else {
    is_inited_ = true;
  }
  LOG_INFO("cmdebug, init macro block bloom filter load tg", K(ret), K(MTL_ID()), K(tg_id_));
  return ret;
}

int ObMacroBlockBloomFilterLoadTG::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to start bloom filter load tg", K(ret), K(is_inited_));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start bloom filter load tg tg", K(ret));
  }
  FLOG_INFO("start macro block bloom filter load tg", K(ret), K(MTL_ID()));
  return ret;
}

void ObMacroBlockBloomFilterLoadTG::stop()
{
  TG_STOP(tg_id_);
  LOG_INFO("cmdebug, stop macro block bloom filter load tg", K(MTL_ID()));
}

void ObMacroBlockBloomFilterLoadTG::wait()
{
  TG_WAIT(tg_id_);
  FLOG_INFO("cmdebug, wait macro block bloom filter load tg", K(MTL_ID()));
}

void ObMacroBlockBloomFilterLoadTG::destroy()
{
  if (tg_id_ != -1) {
    TG_DESTROY(tg_id_);
    LOG_INFO("cmdebug, destroy macro block bloom filter load tg", K(tg_id_), K(MTL_ID()));
    tg_id_ = -1;
  }
  load_task_queue_.reset();
  idle_cond_.destroy();
  allocator_.reset();
  is_inited_ = false;
  LOG_INFO("cmdebug, destroy macro block bloom filter load tg", K(MTL_ID()));
}

void ObMacroBlockBloomFilterLoadTG::run1()
{
  lib::set_thread_name("MaBlkBFLoad");

  while (!has_set_stop()) {
    int ret = OB_SUCCESS;
    // Execute multi load task until request queue empty.
    while (!has_set_stop() && OB_SUCC(ret)) {
      ObBloomFilterLoadKey key;
      ObArray<ValuePair> * array = nullptr;
      if (OB_FAIL(load_task_queue_.pop_task(key, array))) {
        if (OB_HASH_NOT_EXIST == ret || OB_EAGAIN == ret) {
          // maybe empty, do nothing.
        } else {
          LOG_WARN("fail to pop task", K(ret));
        }
      } else {
        LOG_INFO("cmdebug, pop task succeed", K(ret), K(key));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(do_multi_load(key, array))) {
        LOG_WARN("fail to do multi load", K(ret), K(key), KPC(array));
      } else if (OB_FAIL(load_task_queue_.recycle_array(array))) {
        LOG_WARN("fail to recycle array after multi load", K(ret), K(key), KPC(array));
      }
    }

    // Wait for next request.
    ObThreadCondGuard guard(idle_cond_);
    idle_cond_.wait(10 * 1000 /* 10s */);
  }
}

int ObMacroBlockBloomFilterLoadTG::load_macro_block_bloom_filter(const ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMacroBlockBloomFilter macro_block_bf;
  ObBloomFilterCacheValue bf_cache_value;
  if (macro_meta.val_.macro_block_bf_size_ == 0) {
    // do nothing.
  } else if (OB_FAIL(macro_block_bf.deserialize(macro_meta.val_.macro_block_bf_buf_,
                                                macro_meta.val_.macro_block_bf_size_,
                                                pos))) {
    LOG_WARN("fail to deserialize macro block bloom filter", K(ret), K(macro_meta));
  } else if (OB_FAIL(bf_cache_value.init(macro_block_bf.get_bloom_filter(), macro_meta.val_.rowkey_count_))) {
    LOG_WARN("fail to init bloom filter cache value", K(ret), K(macro_meta));
  } else if (OB_FAIL(ObStorageCacheSuite::get_instance().get_bf_cache().put_bloom_filter(MTL_ID(),
                                                                                         macro_meta.get_macro_id(),
                                                                                         bf_cache_value,
                                                                                         true /* adaptive */))) {
    LOG_WARN("fail to load macro block bloom filter", K(ret), K(bf_cache_value), K(macro_meta));
  }
  FLOG_INFO("cmdebug, load macro meta", K(ret), K(macro_meta), K(macro_block_bf));
  return ret;
}

int ObMacroBlockBloomFilterLoadTG::do_multi_load(const ObBloomFilterLoadKey &key,
                                                 ObArray<ValuePair> *array)
{
  int ret = OB_SUCCESS;
  const common::ObTabletID tablet_id = key.table_key_.get_tablet_id();
  const ObTabletMapKey tablet_map_key(key.ls_id_, tablet_id);
  const int64_t array_count = array->count();
  ObTabletHandle tablet_handle;
  ObTableHandleV2 sstable_handle;
  ObSSTable * sstable = nullptr;

  if (OB_UNLIKELY(array_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to do multi load, unexpected empty array", K(ret), K(key), K(array_count), KPC(array));
  } else {
    SMART_VAR(ObSSTableSecMetaIterator, sec_meta_iter) {
      // Fetch tablet and sstable.
      if (OB_FAIL(MTL(ObTenantMetaMemMgr *)->get_tablet(WashTabletPriority::WTP_LOW, tablet_map_key, tablet_handle))) {
        if (OB_ENTRY_NOT_EXIST != ret && OB_ITEM_NOT_SETTED != ret) {
          LOG_WARN("fail to get tablet", K(ret), K(key));
        }
      } else if (OB_FAIL(tablet_handle.get_obj()->get_table(key.table_key_, sstable_handle))) {
        LOG_WARN("fail to get table", K(ret), K(key));
      } else if (OB_FAIL(sstable_handle.get_sstable(sstable))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get sstable", K(ret), K(key));
        }
      }

      // Sort macro id by rowkey.
      const ObITableReadInfo *rowkey_read_info;
      ObDatumRange range;
      if (OB_FAIL(ret)) {
      } else if (FALSE_IT(rowkey_read_info = &(tablet_handle.get_obj()->get_rowkey_read_info()))) {
      } else {
        ObBloomFilterLoadTaskQueue::ValuePairCmpFunc func(rowkey_read_info->get_datum_utils());
        lib::ob_sort(array->begin(), array->end(), func);
        range.set_start_key(*(array->at(0).rowkey_));
        range.set_end_key(*(array->at(array_count - 1).rowkey_));
        range.set_left_closed();
        range.set_right_closed();
      }

      // Prepare sec meta iterator.
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sec_meta_iter.open(range,
                                            blocksstable::DATA_BLOCK_META,
                                            *sstable,
                                            tablet_handle.get_obj()->get_rowkey_read_info(),
                                            allocator_))) {
        LOG_WARN("fail to open sec meta iter", K(ret));
      } else {
        // Load macro block bloom filter in while loop.
        int64_t curr_idx = 0;
        int64_t prev_idx = 0;
        while (OB_SUCC(ret) && curr_idx < array_count) {
          const ValuePair &curr_pair = array->at(curr_idx);
          const ValuePair &prev_pair = array->at(prev_idx);
          if (curr_idx > prev_idx && curr_pair.macro_id_ == prev_pair.macro_id_) {
            // Skip duplicate macro block.
            curr_idx++;
          } else {
            // Iterate meta tree to find target macro meta.
            int cmp_ret = 0;
            ObDataMacroBlockMeta macro_meta;
            do {
              cmp_ret = 0;
              macro_meta.reset();
              if (OB_FAIL(sec_meta_iter.get_next(macro_meta))) {
                LOG_WARN("fail to get next", K(ret), K(key), K(curr_pair));
              } else if (OB_UNLIKELY(!macro_meta.is_valid())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("fail to load macro block bloom filter, invalid macro meta",
                         K(ret), K(macro_meta), K(key), K(curr_pair));
              } else if (OB_FAIL(curr_pair.rowkey_->compare(macro_meta.end_key_,
                                                            rowkey_read_info->get_datum_utils(),
                                                            cmp_ret))) {
                LOG_WARN("fail to compare rowkey", K(ret), K(curr_pair), K(macro_meta));
              }
            } while (OB_SUCC(ret) && cmp_ret > 0 /* current pair endkey > macro meta endkey */);

            // Try to load macro block bloom filter.
            if (OB_FAIL(ret)) {
            } else if (OB_UNLIKELY(curr_pair.macro_id_ != macro_meta.get_macro_id())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to find target macro block", K(ret), K(curr_pair), K(macro_meta), K(key));
              ret = OB_SUCCESS; // Continue load other bloom filter.
            } else if (OB_FAIL(load_macro_block_bloom_filter(macro_meta))) {
              LOG_ERROR("fail to load macro block bloom filter", K(ret), K(macro_meta), K(key), K(curr_pair));
              ret = OB_SUCCESS; // Continue load other bloom filter.
            }

            // Update prev macro id.
            prev_idx = curr_idx;
            curr_idx++;
          }
        }
      }

      if (OB_ENTRY_NOT_EXIST == ret || OB_ITEM_NOT_SETTED == ret || OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObMacroBlockBloomFilterLoadTG::add_load_task(const storage::ObITable::TableKey &sstable_key,
                                                 const share::ObLSID &ls_id,
                                                 const MacroBlockId &macro_id,
                                                 const ObCommonDatumRowkey &common_rowkey)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("cmdebug, add_load_task", K(sstable_key), K(ls_id), K(macro_id));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to add bloom filter load tg task, not inited",
             K(ret), K(sstable_key), K(ls_id), K(macro_id), K(common_rowkey));
  } else if (OB_UNLIKELY(!sstable_key.is_valid() || !macro_id.is_valid()
                         || !storage::ObITable::is_sstable(sstable_key.table_type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to add bloom filter load tg task, invalid argument",
             K(ret), K(sstable_key), K(ls_id), K(macro_id), K(common_rowkey));
  } else if (OB_FAIL(load_task_queue_.push_task(sstable_key, ls_id, macro_id, common_rowkey))) {
    LOG_WARN("fail to push back macro id", K(ret), K(sstable_key), K(ls_id), K(macro_id), K(common_rowkey));
  }
  return ret;
}

} /* namespace blocksstable */
} /* namespace oceanbase */
