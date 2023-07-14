/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_TABLE_STORE_ITERATOR_
#define OCEANBASE_STORAGE_OB_TABLET_TABLE_STORE_ITERATOR_

#include "storage/meta_mem/ob_storage_meta_cache.h"
#include "share/cache/ob_kv_storecache.h"

namespace oceanbase
{
namespace storage
{

class ObITable;
class ObTableHandleV2;
class ObSSTableArray;
class ObMemtableArray;

class ObTableStoreIterator
{
// TODO: currently, we will load all related tables into memory on initializetion of iterator,
// maybe we should init with sstable address and prefetch sstable on iteratring for more smooth memory usage
public:
  class TablePtr final
  {
  public:
    TablePtr() : table_(nullptr), hdl_idx_(-1) {}
    ~TablePtr() = default;
    bool is_valid() const { return nullptr != table_; }
    TO_STRING_KV(KPC_(table), K_(hdl_idx));
  public:
    ObITable *table_;
    int64_t hdl_idx_;
  };
public:
  static const int64_t DEFAULT_TABLE_HANDLE_CNT = 1;
  static const int64_t DEFAULT_TABLE_CNT = 16;
  typedef common::ObSEArray<ObStorageMetaHandle, DEFAULT_TABLE_HANDLE_CNT> SSTableHandleArray;
  typedef common::ObSEArray<TablePtr, DEFAULT_TABLE_CNT> TableArray;
  ObTableStoreIterator(const bool is_reverse = false, const bool need_load_sstable = true);
  ObTableStoreIterator(const ObTableStoreIterator& other) { *this = other; } ;
  void operator=(const ObTableStoreIterator& other);
  virtual ~ObTableStoreIterator();

  OB_INLINE bool is_valid() const { return table_ptr_array_.count() > 0; }
  OB_INLINE bool is_valid_with_handle() const
  {
    return table_store_handle_.is_valid();
  }
  int64_t count() const { return table_ptr_array_.count(); }
  void reset();
  void resume();

  int set_handle(const ObStorageMetaHandle &table_store_handle);

  ObITable *get_last_memtable();
  int get_next(ObITable *&table);
  int get_next(ObTableHandleV2 &table_handle);
  int get_boundary_table(const bool is_last, ObITable *&table);
  int set_retire_check();

  int add_table(ObITable *table);
  int add_tables(
      const ObSSTableArray &sstable_array,
      const int64_t start_pos = 0,
      const int64_t count = 1);
  inline bool check_store_expire() const
  {
    return (NULL == memstore_retired_) ? false : ATOMIC_LOAD(memstore_retired_);
  }
  TO_STRING_KV(K_(table_ptr_array), K_(sstable_handle_array), K_(pos), K_(step), K_(memstore_retired),
      K_(need_load_sstable), K_(table_store_handle), KPC_(transfer_src_table_store_handle));
private:
  int inner_move_idx_to_next();
  int add_tables(const ObMemtableArray &memtable_array, const int64_t start_pos = 0);
  int get_ith_table(const int64_t pos, ObITable *&table);
private:
  friend class ObTablet; // TODO: remove this friend class when possible
  friend class ObTabletTableStore;
  bool need_load_sstable_;
  ObStorageMetaHandle table_store_handle_;
  SSTableHandleArray sstable_handle_array_;
  TableArray table_ptr_array_;
  int64_t pos_;
  int64_t step_;
  bool * memstore_retired_;
  ObStorageMetaHandle *transfer_src_table_store_handle_;
};


} // namespace storage
} // namespace oceanbase

#endif
