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

#include "common/cell/ob_cell_reader.h"
#include "common/cell/ob_cell_writer.h"
#include "storage/memtable/ob_memtable_data.h"

namespace oceanbase {
namespace memtable {
using namespace common;

ObQueryEngine::TableIndexNode* const ObQueryEngine::TableIndex::PLACE_HOLDER = (ObQueryEngine::TableIndexNode*)0x1;

int ObQueryEngine::TableIndexNode::init()
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

void ObQueryEngine::TableIndexNode::destroy()
{
  is_inited_ = false;
  keybtree_.destroy();
}

void ObQueryEngine::TableIndexNode::dump2text(FILE* fd)
{
  int ret = OB_SUCCESS;
  Iterator<keybtree::TScanHandle> iter;
  ObStoreRowkeyWrapper scan_start_key_wrapper(&ObStoreRowkey::MIN_STORE_ROWKEY);
  ObStoreRowkeyWrapper scan_end_key_wrapper(&ObStoreRowkey::MAX_STORE_ROWKEY);
  iter.reset();
  const_cast<ObMemtableKey*>(iter.get_key())->encode(table_id_, nullptr);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
  } else if (OB_ISNULL(fd)) {
    TRANS_LOG(ERROR, "invalid argument");
  } else if (OB_FAIL(keybtree_.set_key_range(
                 iter.get_read_handle(), scan_start_key_wrapper, 1, scan_end_key_wrapper, 1, INT64_MAX))) {
    TRANS_LOG(ERROR, "set key range to btree scan handle fail", KR(ret));
  } else {
    fprintf(fd, "table_id=%lu\n", table_id_);
    for (int64_t row_idx = 0; OB_SUCC(ret) && OB_SUCC(iter.next_internal(true)); row_idx++) {
      const ObMemtableKey* key = iter.get_key();
      ObMvccRow* row = iter.get_value();
      if (OB_ISNULL(key) || OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "iter NULL value", K(key), K(row));
      } else {
        fprintf(fd,
            "row_idx=%ld %s %s purged=%d\n",
            row_idx,
            to_cstring(*key),
            to_cstring(*row),
            iter.get_iter_flag() & ~STORE_ITER_ROW_PARTIAL);
        for (ObMvccTransNode* node = row->get_list_head(); OB_SUCC(ret) && OB_NOT_NULL(node); node = node->prev_) {
          const ObMemtableDataHeader* mtd = reinterpret_cast<const ObMemtableDataHeader*>(node->buf_);
          ObCellReader cci;
          if (OB_ISNULL(mtd)) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "iter NULL trans_node");
          } else if (OB_FAIL(cci.init(mtd->buf_, mtd->buf_len_, SPARSE))) {
            TRANS_LOG(WARN, "cci.init fail", KR(ret));
          } else {
            fprintf(fd, "\t%s dml=%d size=%ld\n", to_cstring(*node), mtd->dml_type_, mtd->buf_len_);
          }
          while (OB_SUCC(ret) && OB_SUCC(cci.next_cell())) {
            uint64_t column_id = OB_INVALID_ID;
            const ObObj* value = NULL;
            if (OB_FAIL(cci.get_cell(column_id, value))) {
              TRANS_LOG(WARN, "get_cell fail", KR(ret));
            } else if (NULL == value) {
              ret = OB_ERR_UNEXPECTED;
              TRANS_LOG(WARN, "get_cell fail, value=NULL", KR(ret));
            } else if (ObExtendType == value->get_type() && ObActionFlag::OP_END_FLAG == value->get_ext()) {
              ret = OB_ITER_END;
            } else {
              fprintf(fd, "\tcid=%ld val=%s\n", column_id, to_cstring(*value));
            }
          }
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
        }
      }
    }
    keybtree_.dump(fd);
    fprintf(fd, "--------------------------\n");
  }
}

int ObQueryEngine::TableIndex::init(const TableIndex* old_index)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "table_index init twice", KR(ret), K(this));
  } else {
    if (OB_ISNULL(old_index)) {
      capacity_ = INIT_TABLE_INDEX_COUNT;
    } else {
      capacity_ = (old_index->capacity_) << 1;
    }
    if (OB_ISNULL(base_ = (TableIndexNode**)memstore_allocator_.alloc(capacity_ * sizeof(TableIndexNode*)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc table_index failed", KR(ret), K(capacity_));
    } else {
      for (uint64_t i = 0; i < capacity_; ++i) {
        ATOMIC_STORE(base_ + i, nullptr);
      }
      if (OB_NOT_NULL(old_index)) {
        // move old_index
        uint64_t i = 0;
        while (i < old_index->capacity_) {
          TableIndexNode* node = nullptr;
          node = ATOMIC_LOAD(old_index->base_ + i);
          if (OB_NOT_NULL(node) && OB_LIKELY(PLACE_HOLDER != node)) {
            uint64_t j = node->get_table_id() % capacity_;
            while (OB_NOT_NULL(base_[j])) {
              j = (j + 1) % capacity_;
            }
            ATOMIC_STORE(base_ + j, node);
            ++i;
          } else {
            TRANS_LOG(ERROR, "invalid table_index_node", K(i), K(old_index->capacity_), K(node));
            ++i;
          }
        }
      }
      is_inited_ = true;
    }
  }
  return ret;
}

void ObQueryEngine::TableIndex::destroy()
{
  TableIndexNode* node = nullptr;
  if (OB_NOT_NULL(base_)) {
    for (uint64_t i = 0; i < capacity_; ++i) {
      node = ATOMIC_LOAD(base_ + i);
      if (OB_NOT_NULL(node)) {
        node->destroy();
        memstore_allocator_.free(node);
        ATOMIC_STORE(base_ + i, nullptr);
      }
    }
    memstore_allocator_.free(base_);
  }
  is_inited_ = false;
}

int64_t ObQueryEngine::TableIndex::get_occupied_size() const
{
  int64_t table_cnt = 0;
  for (uint64_t i = 0; i < capacity_; ++i) {
    if (OB_NOT_NULL(ATOMIC_LOAD(base_ + i))) {
      ++table_cnt;
    }
  }
  return capacity_ * sizeof(TableIndexNode*) + table_cnt * sizeof(TableIndexNode) + hash_alloc_memory() +
         btree_alloc_memory();
}

int ObQueryEngine::TableIndex::dump_keyhash(FILE* fd) const
{
  int ret = OB_SUCCESS;
  const bool print_bucket_node = true;
  const bool print_row_value = false;          // basic info of ObMvccRow
  const bool print_row_value_verbose = false;  // data part of ObMvccRow
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", "this", this);
  } else if (OB_ISNULL(fd)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", KP(fd));
  } else {
    for (uint64_t i = 0; i < capacity_; ++i) {
      TableIndexNode* node = nullptr;
      node = ATOMIC_LOAD(base_ + i);
      if (OB_NOT_NULL(node)) {
        fprintf(fd, "table_id=%lu\n", node->get_table_id());
        node->get_keyhash().dump_hash(fd, print_bucket_node, print_row_value, print_row_value_verbose);
      }
    }
  }
  return ret;
}

// arr_size is approximately equal to count of data node
int64_t ObQueryEngine::TableIndex::hash_size() const
{
  int64_t arr_size = 0;
  for (uint64_t i = 0; i < capacity_; ++i) {
    TableIndexNode* node = nullptr;
    node = ATOMIC_LOAD(base_ + i);
    if (OB_NOT_NULL(node)) {
      arr_size += node->get_keyhash().get_arr_size();
    }
  }
  return arr_size;
}

int64_t ObQueryEngine::TableIndex::hash_alloc_memory() const
{
  int64_t alloc_mem = 0;
  for (uint64_t i = 0; i < capacity_; ++i) {
    TableIndexNode* node = nullptr;
    node = ATOMIC_LOAD(base_ + i);
    if (OB_NOT_NULL(node)) {
      alloc_mem += node->get_keyhash().get_alloc_memory();
    }
  }
  return alloc_mem;
}

int ObQueryEngine::TableIndex::dump_keybtree(FILE* fd)
{
  for (uint64_t i = 0; i < capacity_; ++i) {
    TableIndexNode* node = nullptr;
    node = ATOMIC_LOAD(base_ + i);
    if (OB_NOT_NULL(node)) {
      fprintf(fd, "table_id=%lu\n", node->get_table_id());
      node->get_keybtree().dump(fd);
    }
  }
  return OB_SUCCESS;
}

int64_t ObQueryEngine::TableIndex::btree_size() const
{
  int64_t obj_cnt = 0;
  for (uint64_t i = 0; i < capacity_; ++i) {
    TableIndexNode* node = nullptr;
    node = ATOMIC_LOAD(base_ + i);
    if (OB_NOT_NULL(node)) {
      obj_cnt += node->get_keybtree().size();
    }
  }
  return obj_cnt;
}

int64_t ObQueryEngine::TableIndex::btree_alloc_memory() const
{
  int64_t alloc_mem = 0;
  for (uint64_t i = 0; i < capacity_; ++i) {
    TableIndexNode* node = nullptr;
    node = ATOMIC_LOAD(base_ + i);
    if (OB_NOT_NULL(node)) {
      alloc_mem += sizeof(keybtree::ObKeyBtree);
    }
  }
  return alloc_mem + btree_allocator_.get_allocated();
}

void ObQueryEngine::TableIndex::dump2text(FILE* fd)
{
  for (uint64_t i = 0; i < capacity_; ++i) {
    TableIndexNode* node = nullptr;
    node = ATOMIC_LOAD(base_ + i);
    if (OB_NOT_NULL(node)) {
      node->dump2text(fd);
    }
  }
}

int ObQueryEngine::TableIndex::get(const uint64_t table_id, TableIndexNode*& return_ptr)
{
  int ret = OB_SUCCESS;
  uint64_t i = table_id % capacity_;
  return_ptr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", KP(this));
  } else {
    TableIndexNode* p = nullptr;
    while (OB_SUCC(ret) && OB_ISNULL(return_ptr)) {
      if (OB_NOT_NULL(p = ATOMIC_LOAD(base_ + i))) {
        if (OB_UNLIKELY(PLACE_HOLDER == p)) {
          // allocing, spin to wait.
          sched_yield();
        } else if (OB_LIKELY(p->get_table_id() == table_id)) {
          // right position
          return_ptr = p;
        } else if (OB_UNLIKELY((i + 1) % capacity_ == table_id % capacity_)) {
          // current position not empty but table_id not equal, and all position has traversed.
          ret = OB_TABLE_NOT_EXIST;
        } else {
          // current position not empty but table_id not equal, try the next one.
          i = (i + 1) % capacity_;
        }
      } else {
        ret = OB_TABLE_NOT_EXIST;
      }
    }
  }
  return ret;
}

int ObQueryEngine::TableIndex::set(const uint64_t table_id, const int64_t obj_cnt, TableIndexNode*& return_ptr)
{
  int ret = OB_SUCCESS;
  uint64_t i = table_id % capacity_;
  return_ptr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    TableIndexNode* p = nullptr;
    TableIndexNode* new_node = nullptr;
    while (OB_SUCC(ret) && OB_ISNULL(return_ptr)) {
      if (OB_NOT_NULL(p = ATOMIC_LOAD(base_ + i))) {
        // cur position has been allocated.
        if (OB_UNLIKELY(PLACE_HOLDER == p)) {
          // allocing, spin to wait.
          sched_yield();
        } else if (OB_LIKELY(p->get_table_id() == table_id)) {
          // table_id is equal
          return_ptr = p;
        } else if (OB_UNLIKELY((i + 1) % capacity_ == table_id % capacity_)) {
          // table_id is not equal and all position has traversed.
          ret = OB_ARRAY_OUT_OF_RANGE;
        } else {
          // table_id is not equal and try the next one.
          i = (i + 1) % capacity_;
        }
      } else if (OB_LIKELY(ATOMIC_BCAS(base_ + i, nullptr, PLACE_HOLDER))) {
        // hold the empty slot successfully
        if (OB_NOT_NULL(
                new_node = reinterpret_cast<TableIndexNode*>(memstore_allocator_.alloc(sizeof(TableIndexNode)))) &&
            OB_NOT_NULL(new (new_node) TableIndexNode(btree_allocator_, memstore_allocator_, table_id, obj_cnt))) {
          if (OB_FAIL(new_node->init())) {
            ret = OB_INIT_FAIL;
            TRANS_LOG(ERROR, "table_index_node init failed", KR(ret), K(table_id), K(new_node));
            new_node->~TableIndexNode();
            memstore_allocator_.free(new_node);
            new_node = nullptr;
          } else {
            return_ptr = new_node;
          }
        } else {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TRANS_LOG(WARN, "alloc table_index_node failed", KR(ret), K(table_id));
          new_node = nullptr;
        }
        // must unlock
        ATOMIC_STORE(base_ + i, new_node);
      } else {
        // can not hold that slot, try in next loop
        // other thead may insert equal table_id, so do not inc i.
      }
    }
  }
  return ret;
}

int ObQueryEngine::TableIndex::get_active_table_ids(common::ObIArray<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    for (uint64_t i = 0; i < capacity_ && OB_SUCC(ret); ++i) {
      TableIndexNode* p = nullptr;
      p = ATOMIC_LOAD(base_ + i);
      if (OB_NOT_NULL(p)) {
        if (OB_UNLIKELY(PLACE_HOLDER == p)) {
          TRANS_LOG(ERROR, "insert during get_active_table_ids");
          ret = OB_ERR_UNEXPECTED;
        } else {
          table_ids.push_back(p->get_table_id());
        }
      }
    }
  }
  return ret;
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
  if (IS_INIT) {
    if (OB_NOT_NULL(index_)) {
      index_->destroy();
      memstore_allocator_.free(index_);
      index_ = nullptr;
    }
    is_inited_ = false;
  }
}

int ObQueryEngine::set(const ObMemtableKey* key, ObMvccRow* value)
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
    TableIndexNode* node_ptr = nullptr;
    if (OB_UNLIKELY(OB_TABLE_NOT_EXIST == (ret = (get_table_index_node(key->get_table_id(), node_ptr))))) {
      ret = set_table_index_node(key->get_table_id(), key->get_rowkey()->get_obj_cnt(), node_ptr);
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

int ObQueryEngine::get(const ObMemtableKey* parameter_key, ObMvccRow*& row, ObMemtableKey* returned_key)
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
    TableIndexNode* node_ptr = nullptr;
    if (OB_FAIL(get_table_index_node(parameter_key->get_table_id(), node_ptr))) {
      // FIXME  : to keep compatibility, return old version ret.
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_ISNULL(node_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "node_ptr is nullptr", K(*parameter_key));
    } else {
      const ObStoreRowkeyWrapper parameter_key_wrapper(parameter_key->get_rowkey());
      const ObStoreRowkeyWrapper* copy_inner_key_wrapper = nullptr;
      if (OB_FAIL(node_ptr->get_keyhash().get(&parameter_key_wrapper, row, copy_inner_key_wrapper))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          TRANS_LOG(WARN, "get from keyhash fail", KR(ret), K(*parameter_key));
        }
        row = nullptr;
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "get NULL value from keyhash", KR(ret), K(*parameter_key));
      } else {
        ret = returned_key->encode(parameter_key->get_table_id(), copy_inner_key_wrapper->get_rowkey());
      }
    }
  }
  return ret;
}

int ObQueryEngine::ensure(const ObMemtableKey* key, ObMvccRow* value)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(key) || OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "query_engine ensure error, invalid param", KR(ret), KP(key), KP(value));
  } else {
    TableIndexNode* node_ptr = nullptr;
    if (OB_UNLIKELY(OB_TABLE_NOT_EXIST == (ret = get_table_index_node(key->get_table_id(), node_ptr)))) {
      ret = set_table_index_node(key->get_table_id(), key->get_rowkey()->get_obj_cnt(), node_ptr);
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

int ObQueryEngine::skip_gap(
    const ObMemtableKey* start, const ObStoreRowkey*& end, int64_t version, bool is_reverse, int64_t& size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    TableIndexNode* node_ptr = nullptr;
    if (OB_FAIL(get_table_index_node(start->get_table_id(), node_ptr))) {
      // do nothing
    } else if (OB_ISNULL(node_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "node_ptr is nullptr", K(*start));
    } else {
      ObStoreRowkeyWrapper start_btk(start->get_rowkey());
      ObStoreRowkeyWrapper end_btk;
      if (OB_SUCC(node_ptr->get_keybtree().skip_gap(start_btk, end_btk, version, is_reverse, size))) {
        end = end_btk.get_rowkey();
      }
    }
  }
  return ret;
}

int ObQueryEngine::check_and_purge(const ObMemtableKey* key, ObMvccRow* row, int64_t version, bool& purged)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(key) || OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
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

int ObQueryEngine::get_sdr(
    const ObMemtableKey* key, ObMemtableKey* start_key, ObMemtableKey* end_key, int64_t& max_version)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(key)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    TableIndexNode* node_ptr = nullptr;
    if (OB_FAIL(get_table_index_node(key->get_table_id(), node_ptr))) {
      // FIXME  : to keep compatibility, return old version ret.
      ret = OB_NEED_WAIT;
    } else if (OB_ISNULL(node_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "node_ptr is nullptr", K(*key), K(*start_key), K(*end_key));
    } else {
      ObStoreRowkeyWrapper btk(key->get_rowkey());
      ObStoreRowkeyWrapper start_btk;
      ObStoreRowkeyWrapper end_btk;
      if (OB_SUCC(node_ptr->get_keybtree().get_sdr(btk, start_btk, end_btk, max_version))) {
        start_key->encode(key->get_table_id(), start_btk.get_rowkey());
        end_key->encode(key->get_table_id(), end_btk.get_rowkey());
      }
    }
  }
  return ret;
}

int ObQueryEngine::purge(const ObMemtableKey* key, int64_t version)
{
  int ret = OB_SUCCESS;
  ObMvccRow* value = nullptr;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(key)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "purge error, invalid param", K(key));
  } else {
    TableIndexNode* node_ptr = nullptr;
    if (OB_FAIL(get_table_index_node(key->get_table_id(), node_ptr))) {
      // do nothing
    } else if (OB_ISNULL(node_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "node_ptr is nullptr", K(*key));
    } else {
      ObStoreRowkeyWrapper key_wrapper(key->get_rowkey());
      if (OB_FAIL(node_ptr->get_keybtree().del(key_wrapper, value, version))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          TRANS_LOG(WARN, "purge from keybtree fail", KR(ret), K(*key));
        }
      } else {
        value->set_btree_tag_del();
      }
    }
  }
  return ret;
}

// not supported scan between different table.
int ObQueryEngine::scan(const ObMemtableKey* start_key, const int start_exclude, const ObMemtableKey* end_key,
    const int end_exclude, const int64_t version, ObIQueryEngineIterator*& ret_iter)
{
  int ret = OB_SUCCESS;
  Iterator<keybtree::TScanHandle>* iter = nullptr;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(start_key) || OB_ISNULL(end_key)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", KR(ret));
  } else if (OB_ISNULL(iter = iter_alloc_.alloc())) {
    TRANS_LOG(WARN, "alloc iter fail");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    TableIndexNode* node_ptr = nullptr;
    if (OB_FAIL(get_table_index_node(start_key->get_table_id(), node_ptr))) {
      // FIXME  : to keep compatibility, return old version ret.
      ret = OB_SUCCESS;
    } else if (OB_ISNULL(node_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "node_ptr is nullptr", K(*start_key), K(*end_key));
    } else {
      ObStoreRowkeyWrapper scan_start_key_wrapper(start_key->get_rowkey());
      ObStoreRowkeyWrapper scan_end_key_wrapper(end_key->get_rowkey());
      iter->reset();
      const_cast<ObMemtableKey*>(iter->get_key())->encode(start_key->get_table_id(), nullptr);
      if (OB_FAIL(node_ptr->get_keybtree().set_key_range(iter->get_read_handle(),
              scan_start_key_wrapper,
              start_exclude,
              scan_end_key_wrapper,
              end_exclude,
              version))) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "set key range to btree scan handle fail", KR(ret));
      }
      TRANS_LOG(DEBUG,
          "[BTREE_SCAN_PARAM]",
          "start_key",
          start_key,
          "start_exclude",
          start_exclude,
          "end_key",
          end_key,
          "end_exclude",
          end_exclude);
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN,
        "query_engine scan fail",
        KR(ret),
        K(start_key),
        "start_exclude",
        STR_BOOL(start_exclude),
        K(end_key),
        "end_exclude",
        STR_BOOL(end_exclude),
        KP(iter));
    revert_iter(iter);
  } else {
    ret_iter = iter;
  }
  return ret;
}

void ObQueryEngine::revert_iter(ObIQueryEngineIterator* iter)
{
  iter_alloc_.free((Iterator<keybtree::TScanHandle>*)iter);
  iter = NULL;
}

int ObQueryEngine::sample_rows(Iterator<keybtree::TScanRawHandle>* iter, const ObMemtableKey* start_key,
    const int start_exclude, const ObMemtableKey* end_key, const int end_exclude, int64_t& logical_row_count,
    int64_t& physical_row_count, double& ratio)
{
  int ret = OB_SUCCESS;
  ObMvccRow* value = nullptr;
  int64_t sample_row_count = 0;
  int64_t gap_size = 0;
  int64_t empty_delete_row_count = 0;
  int64_t delete_row_count = 0;
  logical_row_count = 0;
  physical_row_count = 0;
  ratio = 1.5;
  const bool skip_purge_memtable = false;
  TableIndexNode* node_ptr = nullptr;
  TRANS_LOG(DEBUG, "estimate row count, key range", K(*start_key), K(*end_key));
  iter->reset();
  if (OB_FAIL(get_table_index_node(start_key->get_table_id(), node_ptr))) {
    // FIXME  : to keep compatibility, return old version ret.
    ret = OB_ITER_END;
  } else if (OB_ISNULL(node_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "node_ptr is nullptr", K(*start_key), K(*end_key));
  } else {
    ObStoreRowkeyWrapper scan_start_key_wrapper(start_key->get_rowkey());
    ObStoreRowkeyWrapper scan_end_key_wrapper(end_key->get_rowkey());
    if (OB_FAIL(node_ptr->get_keybtree().set_key_range(iter->get_read_handle(),
            scan_start_key_wrapper,
            start_exclude,
            scan_end_key_wrapper,
            end_exclude,
            0 /*unused version*/))) {
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
          if (storage::T_DML_INSERT == value->first_dml_ && storage::T_DML_DELETE != value->last_dml_) {
            // insert new row
            ++logical_row_count;
          } else if (storage::T_DML_DELETE == value->last_dml_) {
            if (storage::T_DML_INSERT != value->first_dml_) {
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
  TRANS_LOG(DEBUG,
      "memtable after sample",
      KR(ret),
      K(sample_row_count),
      K(logical_row_count),
      K(physical_row_count),
      K(gap_size),
      K(ratio));
  return ret;
}

int ObQueryEngine::prefix_exist(const ObMemtableKey* prefix_key, bool& may_exist)
{
  int ret = OB_SUCCESS;
  Iterator<keybtree::TScanRawHandle>* iter = nullptr;
  may_exist = true;
  const ObMemtableKey* iter_key = nullptr;
  ObStoreRowkeyWrapper scan_start_key_wrapper(prefix_key->get_rowkey());
  ObStoreRowkeyWrapper scan_end_key_wrapper(&ObStoreRowkey::MAX_STORE_ROWKEY);
  const bool skip_purge_memtable = false;
  TableIndexNode* node_ptr = nullptr;
  int cmp = 0;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(prefix_key)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param to prefix exist", KR(ret));
  } else if (OB_ISNULL(iter = raw_iter_alloc_.alloc())) {
    TRANS_LOG(WARN, "alloc raw iter fail");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    const ObMemtableKey* iter_key = nullptr;
    ObStoreRowkeyWrapper scan_start_key_wrapper(prefix_key->get_rowkey());
    ObStoreRowkeyWrapper scan_end_key_wrapper(&ObStoreRowkey::MAX_STORE_ROWKEY);
    const bool skip_purge_memtable = false;
    TableIndexNode* node_ptr = nullptr;
    if (OB_FAIL(get_table_index_node(prefix_key->get_table_id(), node_ptr))) {
      // FIXME  : to keep compatibility, return old version ret.
      ret = OB_SUCCESS;
      may_exist = false;
    } else if (OB_ISNULL(node_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "node_ptr is nullptr", K(*prefix_key));
    } else if (OB_FAIL(node_ptr->get_keybtree().set_key_range(iter->get_read_handle(),
                   scan_start_key_wrapper,
                   1,
                   scan_end_key_wrapper,
                   1,
                   0 /*unused version*/))) {
      TRANS_LOG(WARN, "set key range to btree scan handle failed", KR(ret));
    } else if (OB_FAIL(iter->next(skip_purge_memtable))) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "query engine iter next fail", KR(ret));
      } else {
        ret = OB_SUCCESS;
        may_exist = false;
      }
    } else if (OB_ISNULL(iter_key = iter->get_key())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "Unexpected null iter memtable key", KR(ret));
    } else if (OB_FAIL(prefix_key->get_rowkey()->compare_prefix(*(iter_key->get_rowkey()), cmp))) {
      TRANS_LOG(ERROR, "failed to compare", KR(ret), K(prefix_key->get_rowkey()), K(*(iter_key->get_rowkey())));
    } else if (cmp < 0) {
      may_exist = false;
    } else {
      // do nothing
    }
  }
  if (OB_NOT_NULL(iter)) {
    raw_iter_alloc_.free(iter);
    iter = NULL;
  }
  return ret;
}

int ObQueryEngine::init_raw_iter_for_estimate(
    Iterator<keybtree::TScanRawHandle>*& iter, const ObMemtableKey* start_key, const ObMemtableKey* end_key)
{
  int ret = OB_SUCCESS;
  TableIndexNode* node_ptr = nullptr;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(start_key) || OB_ISNULL(end_key)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", K(ret));
  } else if (OB_ISNULL(iter = raw_iter_alloc_.alloc())) {
    TRANS_LOG(WARN, "alloc raw iter fail");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(get_table_index_node(start_key->get_table_id(), node_ptr))) {
    // FIXME  : to keep compatibility, return old version ret.
    ret = OB_SUCCESS;
  } else if (OB_ISNULL(node_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "node_ptr is nullptr", K(*start_key), K(*end_key));
  } else {
    ObStoreRowkeyWrapper start_key_wrapper(start_key->get_rowkey());
    ObStoreRowkeyWrapper end_key_wrapper(end_key->get_rowkey());
    iter->reset();
    if (OB_FAIL(node_ptr->get_keybtree().set_key_range(
            iter->get_read_handle(), start_key_wrapper, 1, end_key_wrapper, 1, 0 /*unused version*/))) {
      TRANS_LOG(WARN, "set key range to btree scan handle failed", K(ret));
    }
  }
  return ret;
}

int ObQueryEngine::estimate_size(const ObMemtableKey* start_key, const ObMemtableKey* end_key, int64_t& level,
    int64_t& branch_count, int64_t& total_bytes, int64_t& total_rows)
{
  int ret = OB_SUCCESS;
  Iterator<keybtree::TScanRawHandle>* iter = nullptr;
  branch_count = 0;
  for (level = 0; branch_count < ESTIMATE_CHILD_COUNT_THRESHOLD && OB_SUCC(ret);) {
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
    int64_t per_row_size = 0;
    if (OB_FAIL(iter->get_read_handle().estimate_row_size(per_row_size))) {
      TRANS_LOG(WARN, "estimate row size fail", K(ret));
    } else {
      total_bytes = total_rows * per_row_size;
    }
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    total_bytes = 0;
    total_rows = 0;
  }
  return ret;
}

int ObQueryEngine::split_range(const ObMemtableKey* start_key, const ObMemtableKey* end_key, int64_t part_count,
    ObIArray<ObStoreRange>& range_array)
{
  int ret = OB_SUCCESS;
  Iterator<keybtree::TScanRawHandle>* iter = nullptr;
  int64_t level = 0;
  int64_t branch_count = 0;
  int64_t total_bytes = 0;
  int64_t total_rows = 0;
  ObStoreRowkeyWrapper key_array[MAX_RANGE_SPLIT_COUNT];
  if (part_count < 1 || part_count > MAX_RANGE_SPLIT_COUNT) {
    TRANS_LOG(WARN, "part count should be greater than 1 if you try to split range", K(part_count));
  } else if (OB_FAIL(estimate_size(start_key, end_key, level, branch_count, total_bytes, total_rows)) &&
             OB_ENTRY_NOT_EXIST != ret) {
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
    TRANS_LOG(WARN,
        "split range fail",
        K(ret),
        K(*start_key),
        K(*end_key),
        K(part_count),
        K(level),
        K(branch_count),
        K(part_count));
  } else {
    ObStoreRange merge_range;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_count; i++) {
      ObStoreRowkey* rowkey = nullptr;
      if (0 == i) {
        merge_range.set_start_key(ObStoreRowkey::MIN_STORE_ROWKEY);
      } else {
        merge_range.set_start_key(*key_array[i - 1].get_rowkey());
      }
      if (i == part_count - 1) {
        merge_range.set_end_key(ObStoreRowkey::MAX_STORE_ROWKEY);
      } else if (OB_ISNULL(rowkey = key_array[i].get_rowkey())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "Unexpected null store rowkey", K(ret), K(i));
      } else {
        merge_range.set_end_key(*rowkey);
      }
      merge_range.set_left_open();
      merge_range.set_right_closed();
      if (OB_FAIL(range_array.push_back(merge_range))) {
        TRANS_LOG(WARN, "Failed to push back the merge range to array", K(ret), K(merge_range));
      }
    }
  }

  if (OB_NOT_NULL(iter)) {
    iter->reset();
    raw_iter_alloc_.free(iter);
    iter = NULL;
  }
  return ret;
}

int ObQueryEngine::estimate_row_count(const ObMemtableKey* start_key, const int start_exclude,
    const ObMemtableKey* end_key, const int end_exclude, int64_t& logical_row_count, int64_t& physical_row_count)
{
  int ret = OB_SUCCESS;
  Iterator<keybtree::TScanRawHandle>* iter = nullptr;
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
  } else if (OB_FAIL(sample_rows(
                 iter, end_key, end_exclude, start_key, start_exclude, log_row_count1, phy_row_count1, ratio1))) {
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "failed to sample rows reverse", KR(ret), K(*start_key), K(*end_key));
    }
  } else if (OB_FAIL(sample_rows(
                 iter, start_key, start_exclude, end_key, end_exclude, log_row_count2, phy_row_count2, ratio2))) {
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
        logical_row_count = static_cast<int64_t>(
            static_cast<double>(logical_row_count) * (static_cast<double>(element_count + MAX_SAMPLE_ROW_COUNT) /
                                                         static_cast<double>(MAX_SAMPLE_ROW_COUNT * 2)));
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

void ObQueryEngine::dump2text(FILE* fd, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  TableIndexNode* node_ptr = nullptr;
  if (OB_FAIL(get_table_index_node(table_id, node_ptr))) {
    // do nothing
  } else if (OB_ISNULL(node_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "node_ptr is nullptr", K(table_id));
  } else {
    node_ptr->dump2text(fd);
  }
}

void ObQueryEngine::dump2text(FILE* fd)
{
  if (OB_NOT_NULL(index_)) {
    index_->dump2text(fd);
  }
}

int ObQueryEngine::get_table_index_node(const uint64_t table_id, TableIndexNode*& return_ptr)
{
  TableIndex* index = nullptr;
  int ret = OB_SUCCESS;
  index = ATOMIC_LOAD(&index_);
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(index)) {
    ret = OB_TABLE_NOT_EXIST;
  } else {
    ret = index->get(table_id, return_ptr);
  }
  return ret;
}

int ObQueryEngine::set_table_index_node(const uint64_t table_id, const int64_t obj_cnt, TableIndexNode*& return_ptr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", "this", this);
    ret = OB_NOT_INIT;
  } else {
    TableIndex* index = nullptr;
    do {
      index = ATOMIC_LOAD(&index_);
      if (OB_ISNULL(index)) {
        ret = OB_ARRAY_OUT_OF_RANGE;
      } else {
        ret = index->set(table_id, obj_cnt, return_ptr);
      }

      if (OB_UNLIKELY(OB_ARRAY_OUT_OF_RANGE == ret)) {
        if (OB_SUCC(expand_index(index))) {
          ret = OB_EAGAIN;
        }
      }
    } while (OB_EAGAIN == ret);
  }
  return ret;
}

int ObQueryEngine::expand_index(TableIndex* old_index)
{
  int ret = OB_SUCCESS;
  const int64_t start_timestamp = ObTimeUtility::current_time();
  // try expand and lock it to avoid double expanding
  if (!ATOMIC_BCAS(&is_expanding_, false, true)) {
    // the thread whick acquire lock failed will spin.
    while (ATOMIC_LOAD(&is_expanding_)) {
      sched_yield();
    }
  } else if (ATOMIC_LOAD(&index_) == old_index) {
    // if not equal, this old_index has been expanded by other thread, jump it.
    TableIndex* index = nullptr;
    if (OB_ISNULL(index = (TableIndex*)memstore_allocator_.alloc(sizeof(TableIndex))) ||
        OB_ISNULL(new (index) TableIndex(btree_allocator_, memstore_allocator_))) {
      TRANS_LOG(ERROR, "alloc table_index error", K(this));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(index->init(index_))) {
      TRANS_LOG(ERROR, "init table_index error", KR(ret), K(index));
    } else {
      // copy on write
      ATOMIC_STORE(&index_, index);
    }
    ATOMIC_STORE(&is_expanding_, false);
    const int64_t cost_time = ObTimeUtility::current_time() - start_timestamp;
    const uint64_t capacity = OB_ISNULL(index) ? INIT_TABLE_INDEX_COUNT : index->get_capacity();
    if (OB_SUCC(ret)) {
      TRANS_LOG(INFO, "table_index expand success", KR(ret), K(this), K(capacity), K(cost_time));
    } else {
      TRANS_LOG(ERROR, "table_index expand failed", KR(ret), K(this), K(capacity), K(cost_time));
    }
  } else {
    ATOMIC_STORE(&is_expanding_, false);
  }
  return ret;
}

int ObQueryEngine::get_active_table_ids(common::ObIArray<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;

  TableIndex* index = ATOMIC_LOAD(&index_);
  if (OB_ISNULL(index)) {
    // do nothing, return empty array
  } else {
    ret = index->get_active_table_ids(table_ids);
  }

  return ret;
}

}  // namespace memtable
}  // namespace oceanbase
