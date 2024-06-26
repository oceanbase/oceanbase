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

// modify buf size in ob_keybtree.h together, otherwise there may be memory waste or overflow.
STATIC_ASSERT(sizeof(ObQueryEngine::Iterator<keybtree::BtreeIterator<ObStoreRowkeyWrapper, ObMvccRow *>>) <= 5120, "Iterator size exceeded");
STATIC_ASSERT(sizeof(keybtree::Iterator<ObStoreRowkeyWrapper, ObMvccRow *>) == 368, "Iterator size changed");

void ObQueryEngine::check_cleanout(bool &is_all_cleanout,
                                   bool &is_all_delay_cleanout,
                                   int64_t &count)
{
  int ret = OB_SUCCESS;
  Iterator<keybtree::BtreeIterator<ObStoreRowkeyWrapper, ObMvccRow *>> iter;
  ObStoreRowkeyWrapper scan_start_key_wrapper(&ObStoreRowkey::MIN_STORE_ROWKEY);
  ObStoreRowkeyWrapper scan_end_key_wrapper(&ObStoreRowkey::MAX_STORE_ROWKEY);
  iter.reset();

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
  } else if (OB_FAIL(keybtree_.set_key_range(iter.get_read_handle(),
                                             scan_start_key_wrapper,
                                             true, /*start_exclusive*/
                                             scan_end_key_wrapper,
                                             true  /*end_exclusive*/))) {
    TRANS_LOG(ERROR, "set key range to btree scan handle fail", KR(ret));
  } else {
    blocksstable::ObRowReader row_reader;
    blocksstable::ObDatumRow datum_row;
    is_all_cleanout = true;
    is_all_delay_cleanout = true;
    count = 0;
    for (int64_t row_idx = 0; OB_SUCC(ret) && OB_SUCC(iter.next_internal()); row_idx++) {
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

void ObQueryEngine::dump2text(FILE* fd)
{
  int ret = OB_SUCCESS;
  Iterator<BtreeIterator> iter;
  ObStoreRowkeyWrapper scan_start_key_wrapper(&ObStoreRowkey::MIN_STORE_ROWKEY);
  ObStoreRowkeyWrapper scan_end_key_wrapper(&ObStoreRowkey::MAX_STORE_ROWKEY);
  iter.reset();

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
  } else if (OB_FAIL(keybtree_.set_key_range(iter.get_read_handle(),
                                             scan_start_key_wrapper,
                                             true, /*start_exclusive*/
                                             scan_end_key_wrapper,
                                             true  /*end_exclusive*/))) {
    TRANS_LOG(ERROR, "set key range to btree scan handle fail", KR(ret));
  } else {
    blocksstable::ObRowReader row_reader;
    blocksstable::ObDatumRow datum_row;
    for (int64_t row_idx = 0; OB_SUCC(ret) && OB_SUCC(iter.next_internal()); row_idx++) {
      const ObMemtableKey *key = iter.get_key();
      ObMvccRow *row = iter.get_value();
      fprintf(fd, "row_idx=%ld %s %s\n", row_idx, to_cstring(*key), to_cstring(*row));
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

int ObQueryEngine::dump_keyhash(FILE *fd) const
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
    keyhash_.dump_hash(fd,
                       print_bucket_node,
                       print_row_value,
                       print_row_value_verbose);
  }

  return ret;
}

int ObQueryEngine::dump_keybtree(FILE* fd)
{
  keybtree_.dump(fd);
  return OB_SUCCESS;
}

int64_t ObQueryEngine::hash_size() const
{
  int64_t arr_size = keyhash_.get_arr_size();
  return arr_size;
}

int64_t ObQueryEngine::hash_alloc_memory() const
{
  int64_t alloc_mem = keyhash_.get_alloc_memory();
  return alloc_mem;
}

int64_t ObQueryEngine::btree_size() const
{
  int64_t obj_cnt = keybtree_.size();
  return obj_cnt;
}

int64_t ObQueryEngine::btree_alloc_memory() const
{
  int64_t alloc_mem = sizeof(KeyBtree) + btree_allocator_.get_allocated();

  return alloc_mem;
}

int ObQueryEngine::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    TRANS_LOG(WARN, "init twice", K(this));
    ret = OB_INIT_TWICE;
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

void ObQueryEngine::destroy()
{
  keybtree_.destroy(true /*is_batch_destroy*/);
  btree_allocator_.reset();
  keyhash_.destroy();
  is_inited_ = false;
}

void ObQueryEngine::pre_batch_destroy_keybtree()
{
  (void)keybtree_.pre_batch_destroy();
}

// The hashmap is thread safe and only one of concurrency inserts will succeed.
// While others will get the OB_ENTRY_EXIST error code.
int ObQueryEngine::set(const ObMemtableKey *key, ObMvccRow *value)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(key) || OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param when query_engine set",
              KR(ret), KP(key), KP(value));
  } else {
    ObStoreRowkeyWrapper key_wrapper(key->get_rowkey());
    if (OB_FAIL(keyhash_.insert(&key_wrapper, value))) {
      if (OB_ENTRY_EXIST != ret) {
        TRANS_LOG(WARN, "put to keyhash fail", K(ret),
                  KPC(key), KPC(value));
      }
    } else {
      value->set_hash_indexed();
    }
  }
  if (OB_FAIL(ret) && OB_ENTRY_EXIST != ret) {
    TRANS_LOG(WARN, "query engine set fail", KR(ret), K(ret), KPC(key), KPC(value));
  }
  return ret;
}

int ObQueryEngine::get(const ObMemtableKey *parameter_key,
                       ObMvccRow *&row,
                       ObMemtableKey *returned_key)
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
    const ObStoreRowkeyWrapper parameter_key_wrapper(parameter_key->get_rowkey());
    const ObStoreRowkeyWrapper *copy_inner_key_wrapper = nullptr;

    if (OB_FAIL(keyhash_.get(&parameter_key_wrapper, row, copy_inner_key_wrapper))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        TRANS_LOG(WARN, "get from keyhash fail", KR(ret), KPC(parameter_key));
      }
      row = nullptr;
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "get NULL value from keyhash", KR(ret), KPC(parameter_key));
    } else if (returned_key) {
      ret = returned_key->encode(copy_inner_key_wrapper->get_rowkey());
    }
  }

  return ret;
}

// The caller need to guarantee the mutual exclusive enforced here, otherwise
// the concurrent modification will violate the rules of the btree(the ERROR
// will be reported if two same key is inserted into keybtree successively)
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
    if (!value->is_btree_indexed()) {
      ObStoreRowkeyWrapper key_wrapper(key->get_rowkey());
      if (OB_FAIL(keybtree_.insert(key_wrapper, value))) {
        TRANS_LOG(WARN, "ensure keybtree fail", KR(ret), K(*key));
        if (OB_ENTRY_EXIST == ret) {
          TRANS_LOG(ERROR, "ensure keybtree fail", KR(ret), K(*key));
        } else {
          TRANS_LOG(WARN, "ensure keybtree fail", KR(ret), K(*key));
        }
      } else {
        value->set_btree_indexed();
      }
    }
  }

  return ret;
}

int ObQueryEngine::scan(const ObMemtableKey *start_key,
                        const bool start_exclude,
                        const ObMemtableKey *end_key,
                        const bool end_exclude,
                        ObIQueryEngineIterator *&ret_iter)
{
  int ret = OB_SUCCESS;
  Iterator<BtreeIterator> *iter = nullptr;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(iter = iter_alloc_.alloc())) {
    TRANS_LOG(WARN, "alloc iter fail");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObStoreRowkeyWrapper scan_start_key_wrapper(start_key->get_rowkey());
    ObStoreRowkeyWrapper scan_end_key_wrapper(end_key->get_rowkey());
    iter->reset();
    if (OB_FAIL(keybtree_.set_key_range(iter->get_read_handle(),
                                        scan_start_key_wrapper, start_exclude,
                                        scan_end_key_wrapper, end_exclude))) {
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

int ObQueryEngine::sample_rows(Iterator<BtreeRawIterator> *iter,
                               const ObMemtableKey *start_key,
                               const int start_exclude,
                               const ObMemtableKey *end_key,
                               const int end_exclude,
                               const transaction::ObTransID &tx_id,
                               int64_t &logical_row_count,
                               int64_t &physical_row_count,
                               double &ratio)
{
  int ret = OB_SUCCESS;
  ObMvccRow *value = nullptr;
  int64_t sample_row_count = 0;
  int64_t empty_delete_row_count = 0;
  int64_t delete_row_count = 0;
  logical_row_count = 0;
  physical_row_count = 0;
  ratio = 1.5;
  ObStoreRowkeyWrapper scan_start_key_wrapper(start_key->get_rowkey());
  ObStoreRowkeyWrapper scan_end_key_wrapper(end_key->get_rowkey());
  TRANS_LOG(DEBUG, "estimate row count, key range", K(*start_key), K(*end_key));
  iter->reset();

  if (OB_FAIL(keybtree_.set_key_range(iter->get_read_handle(),
                                      scan_start_key_wrapper,
                                      start_exclude,
                                      scan_end_key_wrapper,
                                      end_exclude))) {
    TRANS_LOG(WARN, "set key range to btree scan handle failed", KR(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->next())) {
        if (OB_ITER_END != ret) {
          TRANS_LOG(WARN, "query engine iter next fail", KR(ret));
        }
      } else if (OB_ISNULL(value = iter->get_value())) {
        TRANS_LOG(ERROR, "unexpected value null pointer");
        ret = OB_ERR_UNEXPECTED;
      } else {
        ++sample_row_count;
        ++physical_row_count;
        if (blocksstable::ObDmlFlag::DF_NOT_EXIST == value->first_dml_flag_ &&
            blocksstable::ObDmlFlag::DF_NOT_EXIST == value->last_dml_flag_) {
          ObMvccTransNode *iter = value->get_list_head();
          if (nullptr != iter && iter->get_tx_id() == tx_id) {
            // Case1: uncommited row set by myself in the memtable
            ++logical_row_count;
          }
        } else if (blocksstable::ObDmlFlag::DF_INSERT == value->first_dml_flag_
            && blocksstable::ObDmlFlag::DF_DELETE != value->last_dml_flag_) {
          // Case2: insert new row in the memtable
          ++logical_row_count;
        } else if (blocksstable::ObDmlFlag::DF_DELETE == value->last_dml_flag_) {
          if (blocksstable::ObDmlFlag::DF_INSERT != value->first_dml_flag_) {
            // Case3: delete an existent row in the memtable
            --logical_row_count;
            ++delete_row_count;
          } else {
            // Case4: insert and then delete a new row in the memtable
            ++empty_delete_row_count;
          }
        } else {
          // Case5: existent row, not change estimation total row count
        }

        if (sample_row_count >= MAX_SAMPLE_ROW_COUNT) {
          break;
        }
      }
    }
  }

  const int64_t total_delete_row_count = delete_row_count + empty_delete_row_count;
  if (0 < total_delete_row_count) {
    ratio = 1.0 + static_cast<double>(delete_row_count) / static_cast<double>(total_delete_row_count);
  }

  return ret;
}

int ObQueryEngine::init_raw_iter_for_estimate(Iterator<BtreeRawIterator>*& iter,
                                              const ObMemtableKey *start_key,
                                              const ObMemtableKey *end_key)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(iter = raw_iter_alloc_.alloc())) {
    TRANS_LOG(WARN, "alloc raw iter fail");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObStoreRowkeyWrapper start_key_wrapper(start_key->get_rowkey());
    ObStoreRowkeyWrapper end_key_wrapper(end_key->get_rowkey());
    iter->reset();
    if (OB_FAIL(keybtree_.set_key_range(iter->get_read_handle(),
                                        start_key_wrapper,
                                        true, /*start_exclusive*/
                                        end_key_wrapper,
                                        true  /*start_exclusive*/))) {
      TRANS_LOG(WARN, "set key range to btree scan handle failed", K(ret));
    }
  }

  return ret;
}

int ObQueryEngine::estimate_size(const ObMemtableKey *start_key,
                                 const ObMemtableKey *end_key,
                                 int64_t &total_bytes,
                                 int64_t &total_rows)
{
  int ret = OB_SUCCESS;
  int64_t unused_top_level = 0;
  int64_t unused_btree_node_count = 0;
  if (OB_FAIL(inner_loop_find_level_(
          start_key, end_key, ESTIMATE_CHILD_COUNT_THRESHOLD, unused_top_level, unused_btree_node_count, total_rows)))
  {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      total_bytes = 0;
      total_rows = 0;
    } else {
      TRANS_LOG(WARN, "esitmate size failed", KR(ret), KPC(start_key), KPC(end_key));
    }
  } else {
    int64_t per_row_size = 100;
    total_bytes = total_rows * per_row_size;
  }
  return ret;
}

int ObQueryEngine::split_range(const ObMemtableKey *start_key,
                               const ObMemtableKey *end_key,
                               int64_t range_count,
                               ObIArray<ObStoreRange> &range_array)
{
  int ret = OB_SUCCESS;
  Iterator<BtreeRawIterator> *iter = nullptr;
  int64_t top_level = 0;
  int64_t btree_node_count = 0;

  if (range_count < 1 || range_count > MAX_RANGE_SPLIT_COUNT) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "range count should be greater than 1 if you try to split range", KR(ret), K(range_count));
  } else {
    bool need_retry = false;
    do {
      need_retry = false;
      // Here we can not use ESTIMATE_CHILD_COUNT_THRESHOLD to init SEArray due to the stack size limit
      ObSEArray<ObStoreRowkeyWrapper, ESTIMATE_CHILD_COUNT_THRESHOLD / 2> key_array;
      if (OB_FAIL(find_split_range_level_(start_key, end_key, range_count, top_level, btree_node_count)) &&
          OB_ENTRY_NOT_EXIST != ret) {
        TRANS_LOG(WARN, "estimate size fail", K(ret), K(*start_key), K(*end_key));
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        TRANS_LOG(
            WARN, "range too small, not enough rows ro split", K(ret), K(*start_key), K(*end_key), K(range_count));
      } else if (btree_node_count < range_count) {
        ret = OB_ENTRY_NOT_EXIST;
        TRANS_LOG(WARN, "branch fan out less than range count", K(btree_node_count), K(range_count));
      } else if (OB_FAIL(init_raw_iter_for_estimate(iter, start_key, end_key))) {
        TRANS_LOG(WARN, "init raw iter fail", K(ret), K(*start_key), K(*end_key));
      } else if (NULL == iter) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(iter->get_read_handle().split_range(top_level, btree_node_count, range_count, key_array))) {
        TRANS_LOG(WARN,
                  "split range fail",
                  K(ret),
                  K(*start_key),
                  K(*end_key),
                  K(range_count),
                  K(top_level),
                  K(btree_node_count),
                  K(range_count));
        if (OB_EAGAIN == ret) {
          need_retry = true;
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(convert_keys_to_store_ranges_(start_key, end_key, range_count, key_array, range_array))) {
        TRANS_LOG(WARN, "convert keys to store ranges failed", KR(ret), K(range_count), K(key_array));
      } else {
        // split range succeed
      }

      if (OB_NOT_NULL(iter)) {
        iter->reset();
        raw_iter_alloc_.free(iter);
        iter = NULL;
      }
    } while (OB_SUCC(ret) && need_retry);
  }
  return ret;
}

int ObQueryEngine::find_split_range_level_(const ObMemtableKey *start_key,
                                           const ObMemtableKey *end_key,
                                           const int64_t range_count,
                                           int64_t &top_level,
                                           int64_t &btree_node_count)
{
  int64_t unused_total_rows = 0;
  const int64_t node_threshold = MAX(ESTIMATE_CHILD_COUNT_THRESHOLD, range_count);
  return inner_loop_find_level_(start_key, end_key, node_threshold, top_level, btree_node_count, unused_total_rows);
}

int ObQueryEngine::inner_loop_find_level_(const ObMemtableKey *start_key,
                                           const ObMemtableKey *end_key,
                                           const int64_t level_node_threshold,
                                           int64_t &top_level,
                                           int64_t &btree_node_count,
                                           int64_t &total_rows)
{
  int ret = OB_SUCCESS;
  Iterator<BtreeRawIterator> *iter = nullptr;

  // directly loop on the third level because in theory, the second level of BTree can have a maximum of 15 * 15 = 225
  // nodes, which does not meet the THRESHOLD requirement.
  btree_node_count = 0;
  top_level = 2;
  while (OB_SUCC(ret) && btree_node_count < level_node_threshold) {
    top_level++;
    btree_node_count = 0;
    if (OB_FAIL(init_raw_iter_for_estimate(iter, start_key, end_key))) {
      TRANS_LOG(WARN, "init raw iter fail", K(ret), K(*start_key), K(*end_key));
    } else if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(iter->get_read_handle().estimate_key_count(top_level, btree_node_count, total_rows))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        TRANS_LOG(WARN, "find split range level failed", K(ret), K(*start_key), K(*end_key));
      }
    }
    if (OB_NOT_NULL(iter)) {
      iter->reset();
      raw_iter_alloc_.free(iter);
      iter = NULL;
    }
  }

  if (OB_ENTRY_NOT_EXIST != ret) {
    STORAGE_LOG(INFO, "finish find split level", KR(ret), K(top_level), K(btree_node_count), K(total_rows));
  }

  return ret;
}

int ObQueryEngine::convert_keys_to_store_ranges_(const ObMemtableKey *start_key,
                                                 const ObMemtableKey *end_key,
                                                 const int64_t range_count,
                                                 const common::ObIArray<ObStoreRowkeyWrapper> &key_array,
                                                 ObIArray<ObStoreRange> &range_array)
{
  int ret = OB_SUCCESS;

  ObStoreRange merge_range;
  for (int64_t i = 0; OB_SUCC(ret) && i < range_count; i++) {
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
      merge_range.set_start_key(*key_array.at(i - 1).get_rowkey());
    }

    // endkey
    if (OB_FAIL(ret)) {
    } else if (i == range_count - 1) {
      if (OB_ISNULL(rowkey = end_key->get_rowkey())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "Unexcepted null store rowkey", K(ret), KPC(start_key));
      } else {
        merge_range.set_end_key(*rowkey);
      }
    } else if (OB_ISNULL(rowkey = key_array.at(i).get_rowkey())) {
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
  return ret;
}

int ObQueryEngine::estimate_row_count(const transaction::ObTransID &tx_id,
                                      const ObMemtableKey *start_key, const int start_exclude,
                                      const ObMemtableKey *end_key, const int end_exclude,
                                      int64_t &logical_row_count,
                                      int64_t &physical_row_count)
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
    if (OB_FAIL(iter->get_read_handle().estimate_element_count(remaining_row_count, element_count))) {
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

} // namespace memtable
} // namespace oceanbase
