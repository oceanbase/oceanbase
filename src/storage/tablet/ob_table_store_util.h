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
#ifndef OCEANBASE_STORAGE_OB_TABLE_STORE_UTIL_H_
#define OCEANBASE_STORAGE_OB_TABLE_STORE_UTIL_H_

#include "lib/container/ob_se_array.h"
#include "storage/ob_i_table.h"

namespace oceanbase
{
namespace share
{
class SCN;
}
namespace blocksstable
{
class ObSSTable;
}
namespace storage
{
class ObTabletTableStore;
class ObTabletTablesSet;
class ObTenantMetaMemMgr;
class ObITableArray;
class ObSSTableWrapper;

class ObSSTableArray
{
public:
  friend class ObTabletTableStore;
  ObSSTableArray() : cnt_(0), sstable_array_(nullptr), serialize_table_type_(false), is_inited_(false) {}
  virtual ~ObSSTableArray() { reset(); }

  void reset();
  int init(
      ObArenaAllocator &allocator,
      const ObIArray<ObITable *> &tables,
      const int64_t start_pos = 0);
  int init(ObArenaAllocator &allocator, const blocksstable::ObSSTable *sstable);
  int init(
      ObArenaAllocator &allocator,
      const ObIArray<ObITable *> &tables,
      const ObIArray<ObMetaDiskAddr> &addrs,
      const int64_t start_pos,
      const int64_t count);
  int init(
      ObArenaAllocator &allocator,
      const ObSSTableArray &other);
  // TODO use Arena allocator after creating SSTable by Arena Allocator
  // Attention ! should only be called by COSSTable
  int init_empty_array_for_cg(common::ObArenaAllocator &allocator, const int64_t count);
  int add_tables_for_cg(common::ObArenaAllocator &allocator, const ObIArray<ObITable *> &tables);
  int add_tables_for_cg_without_deep_copy(const ObIArray<ObITable *> &tables);

  int64_t get_deep_copy_size() const;
  int deep_copy(char *dst_buf, const int64_t buf_size, int64_t &pos, ObSSTableArray &dst_array) const;

  int64_t get_serialize_size() const;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      ObArenaAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos,
      const bool is_compat_deserialize = false);
  blocksstable::ObSSTable *operator[](const int64_t pos) const;
  blocksstable::ObSSTable *at(const int64_t pos) const;
  ObITable *get_boundary_table(const bool is_last) const;
  int get_all_table_wrappers(ObIArray<ObSSTableWrapper> &tables, const bool need_unpack = false) const;
  int get_table(const ObITable::TableKey &table_key, ObSSTableWrapper &wrapper) const;
  int inc_macro_ref(bool &is_success) const;
  void dec_macro_ref() const;

  OB_INLINE bool is_valid() const
  {
    return 0 == cnt_ || (is_inited_ && cnt_ > 0 && nullptr != sstable_array_);
  }
  OB_INLINE int64_t count() const { return cnt_; }
  OB_INLINE bool empty() const { return 0 == cnt_; }
  TO_STRING_KV(K_(cnt), KP_(sstable_array), K_(serialize_table_type), K_(is_inited));
private:
  int get_all_tables(ObIArray<ObITable *> &tables) const;
  // construct major_tables with old sstable array and input tables_array, but filter twin sstable of new_co_major
  int replace_twin_majors_and_build_new(
      const ObIArray<ObITable *> &tables_array,
      ObIArray<ObITable *> &major_tables) const;
  int inc_meta_ref_cnt(bool &inc_success) const;
  int inc_data_ref_cnt(bool &inc_success) const;
  void dec_meta_ref_cnt() const;
  void dec_data_ref_cnt() const;
  int inner_init(
      ObArenaAllocator &allocator,
      const ObIArray<ObITable *> &tables,
      const int64_t start_pos,
      const int64_t end_pos);
  int inner_deserialize_tables(
      ObArenaAllocator &allocator,
      const bool serialize_table_type,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  template <class T = blocksstable::ObSSTable>
  int deserialize_table(
      ObArenaAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos,
      blocksstable::ObSSTable *&sstable);
private:
  int64_t cnt_;
  blocksstable::ObSSTable **sstable_array_;
  bool serialize_table_type_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTableArray);
};

class ObMemtableArray
{
public:
  ObMemtableArray() : memtable_array_(), count_(0) {}
  OB_INLINE ObIMemtable *operator[](const int64_t pos) const
  {
    OB_ASSERT(pos < count_ && pos >= 0);
    return memtable_array_[pos];
  }
  OB_INLINE void reset() { new (this) ObMemtableArray(); }
  OB_INLINE int64_t count() const { return count_; }
  OB_INLINE bool empty() const { return 0 == count_; }
  OB_INLINE bool is_valid() const { return !empty(); }

  int build(common::ObIArray<ObITable *> &table_array, const int64_t start_pos = 0);
  int rebuild(const common::ObIArray<ObITable *> &table_array);
  int rebuild(
      const share::SCN &clog_checkpoint_scn,
      const common::ObIArray<ObITable *> &table_array);
  int find(const ObITable::TableKey &table_key, ObITable *&table) const;
  int find(const share::SCN &start_scn, const int64_t base_version, ObITable *&table, int64_t &mem_pos) const;
  int assign(ObMemtableArray &dst_array) const;
  TO_STRING_KV(K_(count));
private:
  bool exist_memtable_with_end_scn(const ObITable *table, const share::SCN &end_scn);
  ObIMemtable *memtable_array_[MAX_MEMSTORE_CNT];
  int64_t count_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMemtableArray);
};

class ObDDLKV;
class ObDDLKVArray final
{
public:
  static const int64_t DDL_KV_ARRAY_SIZE = 64;
public:
  ObDDLKVArray() : is_inited_(false), ddl_kvs_(nullptr), count_(0) {}
  ~ObDDLKVArray() { reset(); }

  OB_INLINE ObDDLKV *operator[](const int64_t pos) const
  {
    ObDDLKV *ddl_kv = nullptr;
    if (OB_UNLIKELY(!is_valid() || pos < 0 || pos >= count_)) {
      ddl_kv = nullptr;
    } else {
      ddl_kv = ddl_kvs_[pos];
    }
    return ddl_kv;
  }
  OB_INLINE void reset()
  {
    is_inited_ = false;
    ddl_kvs_   = nullptr;
    count_     = 0;
  }
  OB_INLINE int64_t count() const { return count_; }
  OB_INLINE bool empty() const { return 0 == count_; }
  OB_INLINE bool is_valid() const { return 1 == count_ || (is_inited_ && count_ > 1 && nullptr != ddl_kvs_); }
  OB_INLINE int64_t get_deep_copy_size() const { return count_ * sizeof(ObDDLKV *); }
  int init(ObArenaAllocator &allocator, common::ObIArray<ObDDLKV *> &ddl_kvs);
  int deep_copy(char *buf, const int64_t buf_size, int64_t &pos, ObDDLKVArray &dst) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  bool is_inited_;
  ObDDLKV **ddl_kvs_;
  int64_t count_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDDLKVArray);
};

struct ObTableStoreUtil
{
  struct ObITableLogTsRangeCompare {
    explicit ObITableLogTsRangeCompare(int &sort_ret)
      : result_code_(sort_ret) {}
    bool operator()(const ObITable *ltable, const ObITable *rtable) const;

    int &result_code_;
  };

  struct ObITableSnapshotVersionCompare {
    explicit ObITableSnapshotVersionCompare(int &sort_ret)
      : result_code_(sort_ret) {}
    bool operator()(const ObITable *ltable, const ObITable *rtable) const;

    int &result_code_;
  };

  struct ObITableEndScnCompare {
    explicit ObITableEndScnCompare(int &sort_ret)
      : result_code_(sort_ret) {}
    bool operator()(const ObITable *ltable, const ObITable *rtable) const;

    int &result_code_;
  };

  struct ObTableHandleV2LogTsRangeCompare {
    explicit ObTableHandleV2LogTsRangeCompare(int &sort_ret)
      : result_code_(sort_ret) {}
    bool operator()(const ObTableHandleV2 &lhandle, const ObTableHandleV2 &rhandle) const;

    int &result_code_;
  };

  struct ObTableHandleV2LogTsRangeReverseCompare {
    explicit ObTableHandleV2LogTsRangeReverseCompare(int &sort_ret)
      : result_code_(sort_ret) {}
    bool operator()(const ObTableHandleV2 &lhandle, const ObTableHandleV2 &rhandle) const;

    int &result_code_;
  };

  struct ObTableHandleV2SnapshotVersionCompare {
    explicit ObTableHandleV2SnapshotVersionCompare(int &sort_ret)
      : result_code_(sort_ret) {}
    bool operator()(const ObTableHandleV2 &lhandle, const ObTableHandleV2 &rhandle) const;

    int &result_code_;
  };

  static int compare_table_by_scn_range(const ObITable *ltable, const ObITable *rtable, const bool is_ascend, bool &bret);
  static int compare_table_by_snapshot_version(const ObITable *ltable, const ObITable *rtable, bool &bret);
  static int compare_table_by_end_scn(const ObITable *ltable, const ObITable *rtable, bool &bret);

  static int sort_minor_tables(ObArray<ObITable *> &tables);
  static int reverse_sort_minor_table_handles(ObArray<ObTableHandleV2> &table_handles);
  static int sort_major_tables(ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> &tables);
  static int sort_column_store_tables(ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> &tables);

  static bool check_include_by_scn_range(const ObITable &ltable, const ObITable &rtable);
  static bool check_intersect_by_scn_range(const ObITable &ltable, const ObITable &rtable);

  static int check_has_backup_macro_block(const ObITable *table, bool &has_backup_macro);
};

} // storage
} // oceanbase


#endif /* OCEANBASE_STORAGE_OB_TABLE_STORE_UTIL_H_ */
