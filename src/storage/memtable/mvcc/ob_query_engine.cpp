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

#include "storage/memtable/mvcc/ob_query_engine.h"
#include "storage/memtable/ob_memtable_data.h"
#include "common/ob_store_range.h"
#include "storage/blocksstable/ob_row_reader.h"

namespace oceanbase
{
namespace memtable
{
using namespace common;

ObQueryEngine::TableIndex * const ObQueryEngine::PLACE_HOLDER =
  (ObQueryEngine::TableIndex *)0x1;

// modify buf size in ob_keybtree.h together, otherwise there may be memory waste or overflow.
STATIC_ASSERT(sizeof(ObQueryEngine::Iterator<keybtree::BtreeIterator<ObStoreRowkeyWrapper, ObMvccRow *>>) <= 5120, "Iterator size exceeded");
STATIC_ASSERT(sizeof(keybtree::Iterator<ObStoreRowkeyWrapper, ObMvccRow *>) == 376, "Iterator size changed");

int ObQueryEngine::TableIndex::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "init twice", K(this));
  } else if (OB_FAIL(keybtree_.init())) {
    TRANS_LOG(WARN, "keybtree init fail", KR(ret));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void ObQueryEngine::TableIndex::destroy()
{
  is_inited_ = false;
  keybtree_.destroy();
}

void ObQueryEngine::TableIndex::check_cleanout(bool &is_all_cleanout,
                                               bool &is_all_delay_cleanout,
                                               int64_t &count)
{
  int ret = OB_SUCCESS;
  Iterator<keybtree::BtreeIterator<ObStoreRowkeyWrapper, ObMvccRow *>> iter;
  ObStoreRowkeyWrapper scan_start_key_wrapper(&ObStoreRowkey::MIN_STORE_ROWKEY);
  ObStoreRowkeyWrapper scan_end_key_wrapper(&ObStoreRowkey::MAX_STORE_ROWKEY);
  iter.reset();
  const_cast<ObMemtableKey *>(iter.get_key())->encode(nullptr);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
  } else if (OB_FAIL(keybtree_.set_key_range(iter.get_read_handle(),
                                             scan_start_key_wrapper, 1,
                                             scan_end_key_wrapper, 1, INT64_MAX))) {
    TRANS_LOG(ERROR, "set key range to btree scan handle fail", KR(ret));
  } else {
    blocksstable::ObRowReader row_reader;
    blocksstable::ObDatumRow datum_row;
    is_all_cleanout = true;
    is_all_delay_cleanout = true;
    count = 0;
    for (int64_t row_idx = 0; OB_SUCC(ret) && OB_SUCC(iter.next_internal(true)); row_idx++) {
      const ObMemtableKey *key = iter.get_key();
      ObMvccRow *row = iter.get_value();
      for (ObMvccTransNode *node = row->get_list_head(); OB_SUCC(ret) && OB_NOT_NULL(node); node = node->prev_) {
        if (node->is_delayed_cleanout()) {
          is_all_cleanout = false;
        } else {
          is_all_delay_cleanout = false;
        }
        count++;
      }
    }
  }
}

void ObQueryEngine::TableIndex::dump2text(FILE* fd)
{
  int ret = OB_SUCCESS;
  Iterator<BtreeIterator> iter;
  ObStoreRowkeyWrapper scan_start_key_wrapper(&ObStoreRowkey::MIN_STORE_ROWKEY);
  ObStoreRowkeyWrapper scan_end_key_wrapper(&ObStoreRowkey::MAX_STORE_ROWKEY);
  iter.reset();
  const_cast<ObMemtableKey *>(iter.get_key())->encode(nullptr);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
  } else if (OB_FAIL(keybtree_.set_key_range(iter.get_read_handle(),
                                             scan_start_key_wrapper, 1,
                                             scan_end_key_wrapper, 1, INT64_MAX))) {
    TRANS_LOG(ERROR, "set key range to btree scan handle fail", KR(ret));
  } else {
    blocksstable::ObRowReader row_reader;
    blocksstable::ObDatumRow datum_row;
    for (int64_t row_idx = 0; OB_SUCC(ret) && OB_SUCC(iter.next_internal(true)); row_idx++) {
      const ObMemtableKey *key = iter.get_key();
      ObMvccRow *row = iter.get_value();
      fprintf(fd, "row_idx=%ld %s %s purged=%d\n", row_idx, to_cstring(*key), to_cstring(*row), iter.get_iter_flag() & ~STORE_ITER_ROW_PARTIAL);
      for (ObMvccTransNode *node = row->get_list_head(); OB_SUCC(ret) && OB_NOT_NULL(node); node = node->prev_) {
        const ObMemtableDataHeader *mtd = reinterpret_cast<const ObMemtableDataHeader *>(node->buf_);
        fprintf(fd, "\t%s dml=%d size=%ld\n", to_cstring(*node), mtd->dml_flag_, mtd->buf_len_);
        if (OB_FAIL(row_reader.read_row(mtd->buf_, mtd->buf_len_, nullptr, datum_row))) {
          TRANS_LOG(WARN, "Failed to read datum row", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < datum_row.get_column_count(); i++) {
            blocksstable::ObStorageDatum &datum = datum_row.storage_datums_[i];
            fprintf(fd, "\tcidx=%ld val=%s\n", i, to_cstring(datum));
          }
        }
      }
    }
    keybtree_.dump(fd);
    fprintf(fd, "--------------------------\n");
  }
}

int ObQueryEngine::TableIndex::dump_keyhash(FILE *fd) const
{
  int ret = OB_SUCCESS;
  const bool print_bucket_node = true;
  const bool print_row_value = false; // basic info of ObMvccRow
  const bool print_row_value_verbose = false; // data part of ObMvccRow
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", "this", this);
  } else if (OB_ISNULL(fd)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", KP(fd));
  } else {
    keyhash_.dump_hash(fd, print_bucket_node, print_row_value, print_row_value_verbose);
  }
  return ret;
}

int ObQueryEngine::TableIndex::dump_keybtree(FILE* fd)
{
  keybtree_.dump(fd);
  return OB_SUCCESS;
}

int64_t ObQueryEngine::TableIndex::hash_size() const
{
  int64_t arr_size = keyhash_.get_arr_size();
  return arr_size;
}

int64_t ObQueryEngine::TableIndex::hash_alloc_memory() const
{
  int64_t alloc_mem = keyhash_.get_alloc_memory();
  return alloc_mem;
}

int64_t ObQueryEngine::TableIndex::btree_size() const
{
  int64_t obj_cnt = keybtree_.size();
  return obj_cnt;
}

int64_t ObQueryEngine::TableIndex::btree_alloc_memory() const
{
  int64_t alloc_mem = sizeof(KeyBtree);
  return alloc_mem;
}


bool ObQueryEngine::is_partition_memtable_empty(const uint64_t table_id) const
{
  bool b_ret = false;
  int ret = OB_SUCCESS;

  TableIndex *node_ptr = nullptr;
  if (OB_TABLE_NOT_EXIST == (ret = (get_table_index(node_ptr)))) {
    b_ret = true;
  } else {
    b_ret = false;
  }

  return b_ret;
}

int ObQueryEngine::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(tenant_id));
  } else if (OB_UNLIKELY(is_inited_)) {
    TRANS_LOG(WARN, "init twice", K(this));
    ret = OB_INIT_TWICE;
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

void ObQueryEngine::destroy()
{
  if (IS_NOT_INIT) {
    // do nothing
  } else if (OB_NOT_NULL(index_) && NOT_PLACE_HOLDER(index_)) {
    index_->destroy();
    memstore_allocator_.free(index_);
    index_ = nullptr;
    btree_allocator_.reset();
  }
  is_inited_ = false;
}

int ObQueryEngine::set(const ObMemtableKey *key, ObMvccRow *value)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(key) || OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param when query_engine set", KR(ret), KP(key), KP(value));
  } else {
    TableIndex *node_ptr = nullptr;
    if (OB_UNLIKELY(OB_TABLE_NOT_EXIST == (ret = (get_table_index(node_ptr))))) {
      ret = set_table_index(key->get_rowkey()->get_obj_cnt(), node_ptr);
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(node_ptr)) {
      ObStoreRowkeyWrapper key_wrapper(key->get_rowkey());
      if (OB_FAIL(hash_ret = node_ptr->get_keyhash().insert(&key_wrapper, value))) {
        if (OB_ENTRY_EXIST != hash_ret) {
          TRANS_LOG(WARN, "put to keyhash fail", "hash_ret", hash_ret, "key", key);
        }
        ret = hash_ret;
      } else {
        value->set_hash_indexed();
      }
    }
  }
  if (OB_FAIL(ret) && OB_ENTRY_EXIST != ret) {
    TRANS_LOG(WARN, "query engine set fail", KR(ret), K(hash_ret), K(*key), K(*value));
  }
  return ret;
}

int ObQueryEngine::get(const ObMemtableKey *parameter_key, ObMvccRow *&row, ObMemtableKey *returned_key)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", K(this));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(parameter_key)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", KP(parameter_key));
  } else {
    TableIndex *node_ptr = nullptr;
    if (OB_FAIL(get_table_index(node_ptr))) {
      // FIXME fengshuo.fs : to keep compatibility, return old version ret.
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_ISNULL(node_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "node_ptr is nullptr", K(*parameter_key));
    } else {
      const ObStoreRowkeyWrapper parameter_key_wrapper(parameter_key->get_rowkey());
      const ObStoreRowkeyWrapper *copy_inner_key_wrapper = nullptr;
      if (OB_FAIL(node_ptr->get_keyhash().get(&parameter_key_wrapper, row, copy_inner_key_wrapper))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          TRANS_LOG(WARN, "get from keyhash fail", KR(ret), K(*parameter_key));
        }
        row = nullptr;
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "get NULL value from keyhash", KR(ret), K(*parameter_key));
      } else {
        ret = returned_key->encode(copy_inner_key_wrapper->get_rowkey());
      }
    }
  }
  return ret;
}

int ObQueryEngine::ensure(const ObMemtableKey *key, ObMvccRow *value)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(key) || OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "query_engine ensure error, invalid param", KR(ret), KP(key), KP(value));
  } else {
    TableIndex *node_ptr = nullptr;
    if (OB_UNLIKELY(OB_TABLE_NOT_EXIST == (ret = get_table_index(node_ptr)))) {
      ret = set_table_index(key->get_rowkey()->get_obj_cnt(), node_ptr);
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(node_ptr)) {
      if (value->is_btree_indexed()) {
        if (value->is_btree_tag_del()) {
          ObStoreRowkeyWrapper key_wrapper(key->get_rowkey());
          if (OB_FAIL(node_ptr->get_keybtree().re_insert(key_wrapper, value))) {
            TRANS_LOG(WARN, "ensure keybtree fail", KR(ret), K(*key));
          } else {
            value->clear_btree_tag_del();
          }
        }
      } else {
        ObStoreRowkeyWrapper key_wrapper(key->get_rowkey());
        if (OB_FAIL(node_ptr->get_keybtree().insert(key_wrapper, value))) {
          TRANS_LOG(WARN, "ensure keybtree fail", KR(ret), K(*key));
        } else {
          value->set_btree_indexed();
        }
      }
    }
  }
  return ret;
}

int ObQueryEngine::check_and_purge(const ObMemtableKey *key, ObMvccRow *row, int64_t version, bool &purged)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!row->is_del(version)) {
    // do nothing
  } else {
    ObRowLatchGuard guard(row->latch_);
    if (!row->is_del(version)) {
      // do nothing
    } else if (row->is_btree_tag_del()) {
      // do nothing
    } else if (OB_SUCCESS != (tmp_ret = purge(key, version))) {
      TRANS_LOG(WARN, "purge from keybtree fail", K(tmp_ret), K(*key), K(version));
    } else {
      purged = true;
    }
  }
  return ret;
}

int ObQueryEngine::purge(const ObMemtableKey *key, int64_t version)
{
  int ret = OB_SUCCESS;
  ObMvccRow *value = nullptr;
  TableIndex *node_ptr = nullptr;
  ObStoreRowkeyWrapper key_wrapper(key->get_rowkey());
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_table_index(node_ptr))) {
    // do nothing
  } else if (OB_FAIL(node_ptr->get_keybtree().del(key_wrapper, value, version))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      TRANS_LOG(WARN, "purge from keybtree fail", KR(ret), K(*key));
    }
  } else {
    value->set_btree_tag_del();
  }
  return ret;
}

// not supported scan between different table.
int ObQueryEngine::scan(const ObMemtableKey *start_key, const bool start_exclude, const ObMemtableKey *end_key,
                        const bool end_exclude, const int64_t version, ObIQueryEngineIterator *&ret_iter)
{
  int ret = OB_SUCCESS;
  Iterator<BtreeIterator> *iter = nullptr;
  TableIndex *node_ptr = nullptr;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(iter = iter_alloc_.alloc())) {
    TRANS_LOG(WARN, "alloc iter fail");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(get_table_index(node_ptr))) {
    // FIXME fengshuo.fs : to keep compatibility, return old version ret.
    ret = OB_SUCCESS;
  } else {
    ObStoreRowkeyWrapper scan_start_key_wrapper(start_key->get_rowkey());
    ObStoreRowkeyWrapper scan_end_key_wrapper(end_key->get_rowkey());
    iter->reset();
    const_cast<ObMemtableKey *>(iter->get_key())->encode(nullptr);
    if (OB_FAIL(node_ptr->get_keybtree().set_key_range(iter->get_read_handle(),
                                                       scan_start_key_wrapper, start_exclude,
                                                       scan_end_key_wrapper, end_exclude, version))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "set key range to btree scan handle fail", KR(ret));
    }
    TRANS_LOG(DEBUG, "[BTREE_SCAN_PARAM]",
              "start_key", start_key, "start_exclude", start_exclude,
              "end_key", end_key, "end_exclude", end_exclude);
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "query_engine scan fail", KR(ret),
              K(start_key), "start_exclude", STR_BOOL(start_exclude),
              K(end_key), "end_exclude", STR_BOOL(end_exclude),
              KP(iter));
    revert_iter(iter);
  } else {
    ret_iter = iter;
  }
  return ret;
}

void ObQueryEngine::revert_iter(ObIQueryEngineIterator *iter)
{
  iter_alloc_.free((Iterator<BtreeIterator> *)iter);
  iter = NULL;
}

int ObQueryEngine::sample_rows(Iterator<BtreeRawIterator> *iter, const ObMemtableKey *start_key,
                               const int start_exclude, const ObMemtableKey *end_key, const int end_exclude,
                               const transaction::ObTransID &tx_id,
                               int64_t &logical_row_count, int64_t &physical_row_count, double &ratio)
{
  int ret = OB_SUCCESS;
  ObMvccRow *value = nullptr;
  int64_t sample_row_count = 0;
  int64_t gap_size = 0;
  int64_t empty_delete_row_count = 0;
  int64_t delete_row_count = 0;
  logical_row_count = 0;
  physical_row_count = 0;
  ratio = 1.5;
  const bool skip_purge_memtable = false;
  TableIndex *node_ptr = nullptr;
  ObStoreRowkeyWrapper scan_start_key_wrapper(start_key->get_rowkey());
  ObStoreRowkeyWrapper scan_end_key_wrapper(end_key->get_rowkey());
  TRANS_LOG(DEBUG, "estimate row count, key range", K(*start_key), K(*end_key));
  iter->reset();
  if (OB_FAIL(get_table_index(node_ptr))) {
    // FIXME fengshuo.fs : to keep compatibility, return old version ret.
    ret = OB_ITER_END;
  } else if (OB_FAIL(node_ptr->get_keybtree().set_key_range(iter->get_read_handle(),
                                      scan_start_key_wrapper, start_exclude,
                                      scan_end_key_wrapper, end_exclude, 0/*unused version*/))) {
    TRANS_LOG(WARN, "set key range to btree scan handle failed", KR(ret));
  } else {
    // sample
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->next(skip_purge_memtable))) {
        if (OB_ITER_END != ret) {
          TRANS_LOG(WARN, "query engine iter next fail", KR(ret));
        }
      } else if (OB_ISNULL(value = iter->get_value())) {
        TRANS_LOG(ERROR, "unexpected value null pointer");
        ret = OB_ERR_UNEXPECTED;
      } else {
        ++sample_row_count;
        ++physical_row_count;
        if (value->is_btree_tag_del()) {
          ++gap_size;
        } else {
          if (gap_size >= OB_SKIP_RANGE_LIMIT) {
            physical_row_count -= 2 * gap_size;
          }
          gap_size = 0;
        }
        if (blocksstable::ObDmlFlag::DF_NOT_EXIST == value->first_dml_flag_ &&
            blocksstable::ObDmlFlag::DF_NOT_EXIST == value->last_dml_flag_ &&
            nullptr != value->list_head_ &&
            value->list_head_->tx_id_ == tx_id) {
          ++logical_row_count;
        } else if (blocksstable::ObDmlFlag::DF_INSERT == value->first_dml_flag_
            && blocksstable::ObDmlFlag::DF_DELETE != value->last_dml_flag_) {
          // insert new row
          ++logical_row_count;
        } else if (blocksstable::ObDmlFlag::DF_DELETE == value->last_dml_flag_) {
          if (blocksstable::ObDmlFlag::DF_INSERT != value->first_dml_flag_) {
            // delete existent row
            --logical_row_count;
            ++delete_row_count;
          } else {
            ++empty_delete_row_count;
          }
        } else {
          // existent row, not change estimation total row count
        }
        if (sample_row_count >= MAX_SAMPLE_ROW_COUNT) {
          break;
        }
      }
    }
  }
  // ratio of real delete, if all deletes are real delete, we should minus 2*gap_size from physical_row_count
  // since rows from older table can be skipped in this case. if all deletes are empty delete, we simply minus
  // gap_size from physical_row_count since we only skip these rows in memtable.
  const int64_t total_delete_row_count = delete_row_count + empty_delete_row_count;
  if (0 < total_delete_row_count) {
    ratio = 1.0 + static_cast<double>(delete_row_count) / static_cast<double>(total_delete_row_count);
  }
  if (gap_size >= OB_SKIP_RANGE_LIMIT) {
    physical_row_count -= static_cast<int64_t>(static_cast<double>(gap_size) * ratio);
  }
  TRANS_LOG(DEBUG, "memtable after sample", KR(ret), K(sample_row_count), K(logical_row_count),
      K(physical_row_count), K(gap_size), K(ratio));
  return ret;
}

int ObQueryEngine::init_raw_iter_for_estimate(Iterator<BtreeRawIterator>*& iter,
                                              const ObMemtableKey *start_key,
                                              const ObMemtableKey *end_key)
{
  int ret = OB_SUCCESS;
  TableIndex *node_ptr = nullptr;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(iter = raw_iter_alloc_.alloc())) {
    TRANS_LOG(WARN, "alloc raw iter fail");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(get_table_index(node_ptr))) {
    // FIXME fengshuo.fs : to keep compatibility, return old version ret.
    ret = OB_SUCCESS;
  } else {
    ObStoreRowkeyWrapper start_key_wrapper(start_key->get_rowkey());
    ObStoreRowkeyWrapper end_key_wrapper(end_key->get_rowkey());
    iter->reset();
    if (OB_FAIL(node_ptr->get_keybtree().set_key_range(
                    iter->get_read_handle(),
                    start_key_wrapper, 1,
                    end_key_wrapper, 1, 0/*unused version*/))) {
      TRANS_LOG(WARN, "set key range to btree scan handle failed", K(ret));
    }
  }
  return ret;
}

int ObQueryEngine::estimate_size(const ObMemtableKey *start_key,
                  const ObMemtableKey *end_key,
                  int64_t& level,
                  int64_t& branch_count,
                  int64_t& total_bytes,
                  int64_t& total_rows)
{
  int ret = OB_SUCCESS;
  Iterator<BtreeRawIterator> *iter = nullptr;
  branch_count = 0;
  for(level = 0; branch_count < ESTIMATE_CHILD_COUNT_THRESHOLD && OB_SUCC(ret); ) {
    level++;
    if (OB_FAIL(init_raw_iter_for_estimate(iter, start_key, end_key))) {
      TRANS_LOG(WARN, "init raw iter fail", K(ret), K(*start_key), K(*end_key));
    } else if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(iter->get_read_handle().estimate_key_count(level, branch_count, total_rows))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        TRANS_LOG(WARN, "estimate key count fail", K(ret), K(*start_key), K(*end_key));
      }
    }
    if (OB_NOT_NULL(iter)) {
      iter->reset();
      raw_iter_alloc_.free(iter);
      iter = NULL;
    }
  }
  if (OB_SUCC(ret)) {
    int64_t per_row_size = 100;
    total_bytes = total_rows * per_row_size;
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    total_bytes = 0;
    total_rows = 0;
  }
  return ret;
}

int ObQueryEngine::split_range(const ObMemtableKey *start_key,
                               const ObMemtableKey *end_key,
                               int64_t part_count,
                               ObIArray<ObStoreRange> &range_array)
{
  int ret = OB_SUCCESS;
  Iterator<BtreeRawIterator> *iter = nullptr;
  int64_t level = 0;
  int64_t branch_count = 0;
  int64_t total_bytes = 0;
  int64_t total_rows = 0;

  if (part_count < 1 || part_count > MAX_RANGE_SPLIT_COUNT) {
    TRANS_LOG(WARN, "part count should be greater than 1 if you try to split range", K(part_count));
  } else {
    HEAP_VAR(ObStoreRowkeyWrapper[MAX_RANGE_SPLIT_COUNT], key_array) {
      if (OB_FAIL(estimate_size(start_key, end_key, level, branch_count, total_bytes, total_rows)) && OB_ENTRY_NOT_EXIST != ret) {
        TRANS_LOG(WARN, "estimate size fail", K(ret), K(*start_key), K(*end_key));
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        TRANS_LOG(WARN, "range too small, not enough rows ro split", K(ret), K(*start_key), K(*end_key), K(part_count));
      } else if (branch_count < part_count) {
        ret = OB_ENTRY_NOT_EXIST;
        TRANS_LOG(WARN, "branch fan out less than part count", K(branch_count), K(part_count));
      } else if (OB_FAIL(init_raw_iter_for_estimate(iter, start_key, end_key))) {
        TRANS_LOG(WARN, "init raw iter fail", K(ret), K(*start_key), K(*end_key));
      } else if (NULL == iter) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(iter->get_read_handle().split_range(level, branch_count, part_count, key_array))) {
        TRANS_LOG(WARN, "split range fail", K(ret), K(*start_key), K(*end_key), K(part_count), K(level), K(branch_count), K(part_count));
      } else {
        ObStoreRange merge_range;
        for (int64_t i = 0; OB_SUCC(ret) && i < part_count; i++) {
          const ObStoreRowkey *rowkey = nullptr;
          // start key
          if (0 == i) {
            if (OB_ISNULL(rowkey = start_key->get_rowkey())) {
              ret = OB_ERR_UNEXPECTED;
              TRANS_LOG(WARN, "Unexcepted null store rowkey", K(ret), KPC(start_key));
            } else {
              merge_range.set_start_key(*rowkey);
            }
          } else {
            merge_range.set_start_key(*key_array[i - 1].get_rowkey());
          }

          // endkey
          if (OB_FAIL(ret)) {
          } else if (i == part_count - 1) {
            if (OB_ISNULL(rowkey = end_key->get_rowkey())) {
              ret = OB_ERR_UNEXPECTED;
              TRANS_LOG(WARN, "Unexcepted null store rowkey", K(ret), KPC(start_key));
            } else {
              merge_range.set_end_key(*rowkey);
            }
          } else if (OB_ISNULL(rowkey = key_array[i].get_rowkey())) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(WARN, "Unexpected null store rowkey", K(ret), K(i));
          } else {
            merge_range.set_end_key(*rowkey);
          }
          if (OB_SUCC(ret)) {
            merge_range.set_left_open();
            merge_range.set_right_closed();
            if (OB_FAIL(range_array.push_back(merge_range))) {
              TRANS_LOG(WARN, "Failed to push back the merge range to array", K(ret), K(merge_range));
            }
          }
        }
      }
    }

    if (OB_NOT_NULL(iter)) {
      iter->reset();
      raw_iter_alloc_.free(iter);
      iter = NULL;
    }
  }
  return ret;
}


int ObQueryEngine::estimate_row_count(const transaction::ObTransID &tx_id,
                                      const ObMemtableKey *start_key, const int start_exclude,
                                      const ObMemtableKey *end_key, const int end_exclude,
                                      int64_t &logical_row_count, int64_t &physical_row_count)
{
  int ret = OB_SUCCESS;
  Iterator<BtreeRawIterator> *iter = nullptr;
  int64_t remaining_row_count = 0;
  int64_t element_count = 0;
  int64_t phy_row_count1 = 0;
  int64_t phy_row_count2 = 0;
  int64_t log_row_count1 = 0;
  int64_t log_row_count2 = 0;
  double ratio1 = 1.5;
  double ratio2 = 1.5;
  double ratio = 1.5;
  logical_row_count = 0;
  physical_row_count = 0;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(start_key) || OB_ISNULL(end_key)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", KR(ret));
  } else if (OB_ISNULL((iter = raw_iter_alloc_.alloc()))) {
    TRANS_LOG(WARN, "alloc raw iter fail");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(sample_rows(iter, end_key, end_exclude, start_key, start_exclude, tx_id,
      log_row_count1, phy_row_count1, ratio1))) {
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "failed to sample rows reverse", KR(ret), K(*start_key), K(*end_key));
    }
  } else if (OB_FAIL(sample_rows(iter, start_key, start_exclude, end_key, end_exclude, tx_id,
      log_row_count2, phy_row_count2, ratio2))) {
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "failed to sample rows", KR(ret), K(*start_key), K(*end_key));
    }
  }
  logical_row_count = log_row_count1 + log_row_count2;
  // since remaining_row_count actually includes phy_row_count1, so we don't
  // want to add it twice here
  physical_row_count = phy_row_count1 + phy_row_count2;
  ratio = (ratio1 + ratio2) / 2;

  if (OB_SUCC(ret)) {
    // fast caculate the remaining row count
    if (OB_FAIL(iter->get_read_handle().estimate_element_count(remaining_row_count, element_count, ratio))) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "estimate row count fail", KR(ret));
      } else {
        TRANS_LOG(DEBUG, "estimate_element_count result", K(remaining_row_count), K(element_count));
        // logical row count should be calculated with btree element count
        logical_row_count = static_cast<int64_t>(static_cast<double>(logical_row_count)
            * (static_cast<double>(element_count + MAX_SAMPLE_ROW_COUNT)
                / static_cast<double>(MAX_SAMPLE_ROW_COUNT * 2)));
        physical_row_count += remaining_row_count - phy_row_count1;
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_NOT_NULL(iter)) {
    raw_iter_alloc_.free(iter);
    iter = NULL;
  }
  ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  return ret;
}

void ObQueryEngine::check_cleanout(bool &is_all_cleanout,
                                   bool &is_all_delay_cleanout,
                                   int64_t &count)
{
  if (OB_NOT_NULL(index_)) {
    index_->check_cleanout(is_all_cleanout,
                           is_all_delay_cleanout,
                           count);
  }
}

void ObQueryEngine::dump2text(FILE *fd)
{
  TableIndex *index = ATOMIC_LOAD(&index_);
  if (OB_NOT_NULL(index) && NOT_PLACE_HOLDER(index)) {
    index->dump2text(fd);
  }
}

int ObQueryEngine::get_table_index(TableIndex *&return_ptr) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else {
    TableIndex *p = nullptr;
    while (OB_SUCC(ret) && OB_ISNULL(return_ptr)) {
      if (OB_ISNULL(p = ATOMIC_LOAD(&index_))) {
        // if we found a empty slot, table_id must not be in the hash.
        ret = OB_TABLE_NOT_EXIST;
      } else if (OB_UNLIKELY(PLACE_HOLDER == p)) {
        // allocing, spin to wait.
        sched_yield();
      } else {
        // right position
        return_ptr = p;
      }
    }
  }
  return ret;
}

int ObQueryEngine::set_table_index(const int64_t obj_cnt, TableIndex *&return_ptr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(set_table_index_(obj_cnt, return_ptr))) {
    TRANS_LOG(WARN, "set table index failed.", KR(ret));
  } else {
    // set table index succeed.
  }
  return ret;
}

int ObQueryEngine::set_table_index_(const int64_t obj_cnt, TableIndex *&return_ptr)
{
  int ret = OB_SUCCESS;
  return_ptr = nullptr;
  TableIndex *p = nullptr;
  TableIndex *new_node = nullptr;
  while (OB_SUCC(ret) && OB_ISNULL(return_ptr)) {
    if (OB_NOT_NULL(p = ATOMIC_LOAD(&index_))) {
      // cur position has been allocated.
      if (OB_UNLIKELY(PLACE_HOLDER == p)) {
        // allocing, spin to wait.
        sched_yield();
      } else {
        // table_id is equal
        return_ptr = p;
      }
    } else if (OB_LIKELY(ATOMIC_BCAS(&index_, nullptr, PLACE_HOLDER))) {
      // hold the empty slot successfully
      if (OB_NOT_NULL(new_node = reinterpret_cast<TableIndex *>(
                        memstore_allocator_.alloc(sizeof(TableIndex))))
          && OB_NOT_NULL(new (new_node)
                           TableIndex(btree_allocator_, memstore_allocator_, obj_cnt))) {
        if (OB_FAIL(new_node->init())) {
          ret = OB_INIT_FAIL;
          TRANS_LOG(ERROR, "table_index_node init failed", KR(ret), K(new_node));
          new_node->~TableIndex();
          memstore_allocator_.free(new_node);
          new_node = nullptr;
        } else {
          return_ptr = new_node;
        }
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "alloc table_index_node failed", KR(ret)); 
        new_node = nullptr;
      }
      // must unlock
      ATOMIC_STORE(&index_, new_node);
    } else {
      // can not hold that slot, try in next loop
      // other thead may insert equal table_id, so do not inc i.
    }
  }

  return ret;
}

} // namespace memtable
} // namespace oceanbase
