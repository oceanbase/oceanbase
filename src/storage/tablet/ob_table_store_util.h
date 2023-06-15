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

namespace storage
{
class ObTabletTableStore;
class ObTabletTablesSet;
class ObTenantMetaMemMgr;

// TODO maybe need 2-d array to store ObSSTableArray, ObMemtableArray, ObExtendArray
class ObITableArray
{
public:
  ObITableArray();
  virtual ~ObITableArray();

  int init(common::ObIAllocator &allocator, const int64_t count);
  int copy(common::ObIAllocator &allocator, const ObITableArray &other, const bool allow_empty_table = false);
  int init_and_copy(
    common::ObIAllocator &allocator,
    const common::ObIArray<ObITable *> &tables,
    const int64_t start_pos = 0);
  bool empty() const { return count_ <= 0; }
  bool is_valid() const { return is_inited_ && count_ > 0; }
  int64_t count() const { return count_; }
  virtual void destroy();

  int assign(const int64_t pos, ObITable * const table);
  void reset_table(const int64_t pos);

  virtual int get_all_tables(common::ObIArray<ObITable *> &tables) const;
  virtual int get_all_tables(storage::ObTablesHandleArray &tables) const;
  virtual ObITable *get_boundary_table(const bool last) const;
  int get_table(const ObITable::TableKey &table_key, ObTableHandleV2 &handle) const;
  OB_INLINE ObITable *get_table(const int64_t pos) const { return (pos < count_ && pos >= 0) ? array_[pos] : nullptr; }
  OB_INLINE ObITable *operator[](const int64_t pos) const { return (pos < count_ && pos >= 0) ? array_[pos] : nullptr; }
  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(count), KP_(array));

public:
  storage::ObTenantMetaMemMgr* meta_mem_mgr_;
  ObITable **array_;
  common::ObIAllocator *allocator_;
  int64_t count_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObITableArray);
};

class ObSSTableArray: public ObITableArray
{
public:
  virtual int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  virtual int64_t get_serialize_size() const;
  virtual int deserialize(
    common::ObIAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos);
  int get_min_schema_version(int64_t &min_schema_version) const;
};

class ObExtendTableArray: public ObITableArray
{
public:
  virtual int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  virtual int64_t get_serialize_size() const;
  virtual int deserialize(
    common::ObIAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos);

private:
  virtual int get_all_tables(common::ObIArray<ObITable *> &sstables) const override { UNUSED(sstables); return OB_NOT_SUPPORTED; }
  virtual ObITable *get_boundary_table(const bool last) const { UNUSED(last); return nullptr; }
};

class ObMemtableArray
{
public:
  ObMemtableArray();
  virtual ~ObMemtableArray();
  void destroy();
  OB_INLINE int64_t count() const { return (nullptr == array_) ? 0 : array_->count(); }
  OB_INLINE bool empty() const { return 0 == count(); }
  OB_INLINE ObITable *get_table(const int64_t idx) const { return (idx < count() && idx >= 0) ? array_->at(idx) : nullptr; }
  OB_INLINE ObITable *operator[](const int64_t idx) { return get_table(idx); }
  OB_INLINE ObITable *operator[](const int64_t idx) const { return get_table(idx); }

  int init(common::ObIAllocator *allocator);
  int init(common::ObIAllocator *allocator, const ObMemtableArray &other);
  int build(common::ObIArray<ObTableHandleV2> &handle_array, const int64_t start_pos = 0);
  int rebuild(common::ObIArray<ObTableHandleV2> &handle_array);
  int rebuild(
      const share::SCN &clog_checkpoint_scn,
      common::ObIArray<ObTableHandleV2> &handle_array);
  int prepare_allocate();
  int find(const ObITable::TableKey &table_key, ObTableHandleV2 &handle) const;
  int find(const share::SCN &start_scn, const int64_t base_version, ObITable *&table, int64_t &mem_pos) const;
  TO_STRING_KV(K_(is_inited), KPC_(array));
private:
  int add_table(ObITable * const table);
  void reset_table(const int64_t pos);

public:
  static const int64_t DEFAULT_TABLE_CNT = 16;
  storage::ObTenantMetaMemMgr* meta_mem_mgr_;
  common::ObIAllocator *allocator_;
  common::ObSEArray<ObITable *, DEFAULT_TABLE_CNT, common::ObIAllocator&> *array_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMemtableArray);
};

class ObTableStoreIterator
{
public:
  friend class ObPrintTableStoreIterator;
  static const int64_t DEFAULT_TABLET_CNT = 16;
  typedef common::ObSEArray<ObITable *, DEFAULT_TABLET_CNT> TableArray;

  explicit ObTableStoreIterator(const bool reverse_iter = false);
  virtual ~ObTableStoreIterator();
  void reset();
  void resume();
  bool is_valid() const { return array_.count() > 0; }
  int64_t count() const { return array_.count(); }

  int copy(const ObTableStoreIterator &other);
  int add_tables(ObMemtableArray &array, const int64_t start_pos = 0);
  int add_tables(ObITable **start, const int64_t count = 1);
  int add_table(ObITable *input_table);
  int get_next(ObITable *&table);

  ObITable *get_boundary_table(const bool is_last);

  int set_retire_check();
  bool check_store_expire() const { return (NULL == memstore_retired_) ? false : ATOMIC_LOAD(memstore_retired_); }
  TO_STRING_KV(K_(array), K_(pos), K_(memstore_retired), K_(step));

private:
  TableArray array_;
  int64_t pos_;
  int64_t step_;
  bool * memstore_retired_;
};


class ObPrintTableStoreIterator
{
public:
  ObPrintTableStoreIterator(const ObTableStoreIterator &iter);
  virtual ~ObPrintTableStoreIterator();
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  void table_to_string(
      ObITable *table,
      char *buf,
      const int64_t buf_len,
      int64_t &pos) const;
private:
  const ObTableStoreIterator::TableArray &array_;
  int64_t pos_;
  int64_t step_;
  bool *memstore_retired_;
};

#define PRINT_TS_ITER(x) (ObPrintTableStoreIterator(x))


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

  struct ObTableHandleV2LogTsRangeCompare {
    explicit ObTableHandleV2LogTsRangeCompare(int &sort_ret)
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

  static int compare_table_by_scn_range(const ObITable *ltable, const ObITable *rtable, bool &bret);
  static int compare_table_by_snapshot_version(const ObITable *ltable, const ObITable *rtable, bool &bret);

  static int sort_minor_tables(ObArray<ObITable *> &tables);
  static int sort_major_tables(ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> &tables);

  static bool check_include_by_scn_range(const ObITable &ltable, const ObITable &rtable);
  static bool check_intersect_by_scn_range(const ObITable &ltable, const ObITable &rtable);
};

} // storage
} // oceanbase


#endif /* OCEANBASE_STORAGE_OB_TABLE_STORE_UTIL_H_ */
