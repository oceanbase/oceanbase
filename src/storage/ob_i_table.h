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

#ifndef SRC_STORAGE_OB_I_TABLE_H_
#define SRC_STORAGE_OB_I_TABLE_H_

#include "lib/container/ob_iarray.h"
#include "ob_i_store.h"
#include "lib/lock/ob_drw_lock.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_se_array_iterator.h"
#include "common/ob_range.h"

namespace oceanbase {

namespace memtable {
class ObMemtable;
}

namespace storage {
class ObSSTable;
class ObOldSSTable;
class ObITable;
class ObTableCompater;

class ObITable {
  OB_UNIS_VERSION(1);

public:
  enum TableType {
    MEMTABLE = 0,
    MAJOR_SSTABLE = 1,
    MINOR_SSTABLE = 2,  // obsoleted type after 2.2
    TRANS_SSTABLE = 3,  // new table type from 3.1
    MULTI_VERSION_MINOR_SSTABLE = 4,
    COMPLEMENT_MINOR_SSTABLE = 5,            // new table type from 3.1
    MULTI_VERSION_SPARSE_MINOR_SSTABLE = 6,  // reserved table type
    MINI_MINOR_SSTABLE = 7,
    RESERVED_MINOR_SSTABLE = 8,
    MAX_TABLE_TYPE
  };

  OB_INLINE static bool is_table_type_valid(const TableType& type);

  struct TableKey {
    OB_UNIS_VERSION(1);

  public:
    TableKey();
    TableKey(const ObITable::TableType& table_type, const common::ObPartitionKey& pkey, const uint64_t table_id,
        const common::ObVersionRange& trans_version_range, const common::ObVersion& version,
        const common::ObLogTsRange& log_ts_range);
    OB_INLINE bool is_valid(const bool skip_version_range = true, const bool skip_log_ts_range = false) const;
    OB_INLINE bool operator==(const TableKey& table_key) const;
    OB_INLINE bool operator!=(const TableKey& table_key) const;
    OB_INLINE bool is_memtable() const
    {
      return ObITable::is_memtable(table_type_);
    }
    OB_INLINE bool is_minor_sstable() const
    {
      return ObITable::is_minor_sstable(table_type_);
    }
    OB_INLINE bool is_normal_minor_sstable() const
    {
      return ObITable::is_normal_minor_sstable(table_type_);
    }
    OB_INLINE bool is_mini_minor_sstable() const
    {
      return ObITable::is_mini_minor_sstable(table_type_);
    }
    OB_INLINE bool is_major_sstable() const
    {
      return ObITable::is_major_sstable(table_type_);
    }
    OB_INLINE bool is_trans_sstable() const
    {
      return ObITable::is_trans_sstable(table_type_);
    }
    OB_INLINE bool is_multi_version_table() const
    {
      return ObITable::is_multi_version_table(table_type_);
    }
    OB_INLINE bool is_new_table_from_3x() const
    {
      return ObITable::is_new_table_from_3x(table_type_);
    }
    OB_INLINE bool is_complement_minor_sstable() const
    {
      return ObITable::is_complement_minor_sstable(table_type_);
    }
    OB_INLINE uint64_t get_tenant_id() const
    {
      return common::extract_tenant_id(table_id_);
    }
    OB_INLINE bool is_table_with_log_ts_range() const
    {
      return ObITable::is_table_with_log_ts_range(table_type_);
    }
    OB_INLINE const common::ObPartitionKey& get_partition_key() const
    {
      return pkey_;
    }
    OB_INLINE int64_t get_start_log_ts() const
    {
      return log_ts_range_.start_log_ts_;
    }
    OB_INLINE int64_t get_end_log_ts() const
    {
      return log_ts_range_.end_log_ts_;
    }
    OB_INLINE int64_t get_max_log_ts() const
    {
      return log_ts_range_.max_log_ts_;
    }
    OB_INLINE int64_t get_base_version() const
    {
      return trans_version_range_.base_version_;
    }
    OB_INLINE int64_t get_snapshot_version() const
    {
      return trans_version_range_.snapshot_version_;
    }
    bool is_table_log_ts_comparable() const;
    uint64_t hash() const;
    void reset();
    TO_STRING_KV(K_(table_type), K_(pkey), K_(table_id), K_(trans_version_range), K_(log_ts_range), K_(version));

    ObITable::TableType table_type_;
    common::ObPartitionKey pkey_;
    uint64_t table_id_;
    common::ObVersionRange trans_version_range_;
    common::ObVersion version_;  // only used for major merge
    common::ObLogTsRange log_ts_range_;
  };

  ObITable();
  virtual ~ObITable()
  {}

  int init(const TableKey& table_key, const bool skip_version_range = true, const bool skip_log_ts_range = false);
  OB_INLINE const TableKey& get_key() const
  {
    return key_;
  }
  void set_log_ts_range(common::ObLogTsRange log_ts_range)
  {
    key_.log_ts_range_ = log_ts_range;
  }
  void set_table_type(ObITable::TableType table_type)
  {
    key_.table_type_ = table_type;
  }
  void set_base_version(int64_t version)
  {
    key_.trans_version_range_.base_version_ = version;
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return key_.pkey_;
  }

  virtual void destroy() = 0;
  virtual int exist(const ObStoreCtx& ctx, const uint64_t table_id, const common::ObStoreRowkey& rowkey,
      const common::ObIArray<share::schema::ObColDesc>& column_ids, bool& is_exist, bool& has_found);

  virtual int prefix_exist(ObRowsInfo& rows_info, bool& may_exist);

  virtual int exist(ObRowsInfo& rowsInfo, bool& is_exist, bool& has_found);

  virtual int scan(const ObTableIterParam& param, ObTableAccessContext& context,
      const common::ObExtStoreRange& key_range, ObStoreRowIterator*& row_iter) = 0;

  virtual int get(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      const common::ObExtStoreRowkey& rowkey, ObStoreRowIterator*& row_iter) = 0;

  virtual int multi_get(const ObTableIterParam& param, ObTableAccessContext& context,
      const common::ObIArray<common::ObExtStoreRowkey>& rowkeys, ObStoreRowIterator*& row_iter) = 0;

  virtual int multi_scan(const ObTableIterParam& param, ObTableAccessContext& context,
      const common::ObIArray<common::ObExtStoreRange>& ranges, ObStoreRowIterator*& row_iter) = 0;

  virtual int estimate_get_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObIArray<common::ObExtStoreRowkey>& rowkeys, ObPartitionEst& part_est) = 0;

  virtual int estimate_scan_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObExtStoreRange& key_range, ObPartitionEst& part_est) = 0;
  virtual int estimate_multi_scan_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObIArray<common::ObExtStoreRange>& ranges, ObPartitionEst& part_est) = 0;

  virtual int set(const ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_size,
      const common::ObIArray<share::schema::ObColDesc>& columns, ObStoreRowIterator& row_iter) = 0;

  virtual OB_INLINE const common::ObVersion& get_version() const
  {
    return key_.version_;
  }
  virtual OB_INLINE int64_t get_snapshot_version() const;
  virtual OB_INLINE int64_t get_base_version() const;
  virtual OB_INLINE int64_t get_multi_version_start() const;
  virtual OB_INLINE int64_t get_start_log_ts() const;
  virtual OB_INLINE int64_t get_end_log_ts() const;
  virtual OB_INLINE int64_t get_max_log_ts() const;
  virtual OB_INLINE common::ObLogTsRange& get_log_ts_range()
  {
    return key_.log_ts_range_;
  }
  virtual OB_INLINE bool contain(const int64_t snapshot_version) const;
  virtual OB_INLINE const common::ObVersionRange& get_version_range() const
  {
    return key_.trans_version_range_;
  }
  virtual OB_INLINE bool is_trans_state_deterministic()
  {
    return get_upper_trans_version() < INT64_MAX;
  }
  virtual int64_t get_upper_trans_version() const
  {
    return get_snapshot_version();
  }
  virtual int64_t get_max_merged_trans_version() const
  {
    return get_snapshot_version();
  }
  virtual int get_frozen_schema_version(int64_t& schema_version) const = 0;
  // for check ref count
  // inline void inc_ref() { ATOMIC_INC(&ref_cnt_); STORAGE_LOG(ERROR, "yongle_test inc_ref", KP(this), K_(ref_cnt)); }
  // inline int64_t dec_ref() { STORAGE_LOG(ERROR, "yongle_test dec_ref", KP(this), "ref_cnt", ref_cnt_ - 1); return
  // ATOMIC_SAF(&ref_cnt_, 1 /* just sub 1 */); }
  //
  //  inline int64_t get_ref() const { STORAGE_LOG(ERROR, "yongle_test get_ref", KP(this), "ref_cnt", ref_cnt_);  return
  //  ATOMIC_LOAD(&ref_cnt_); }
  inline void inc_ref()
  {
    ATOMIC_INC(&ref_cnt_);
  }
  inline int64_t dec_ref()
  {
    return ATOMIC_SAF(&ref_cnt_, 1 /* just sub 1 */);
  }
  inline int64_t get_ref() const
  {
    return ATOMIC_LOAD(&ref_cnt_);
  }

  // TODO  so many table type judgement
  virtual bool is_sstable() const
  {
    return is_sstable(key_.table_type_);
  }
  virtual bool is_major_sstable() const
  {
    return is_major_sstable(key_.table_type_);
  }
  virtual bool is_old_minor_sstable() const
  {
    return is_old_minor_sstable(key_.table_type_);
  }
  virtual bool is_minor_sstable() const
  {
    return is_minor_sstable(key_.table_type_);
  }
  virtual bool is_mini_minor_sstable() const
  {
    return is_mini_minor_sstable(key_.table_type_);
  }
  virtual bool is_multi_version_minor_sstable() const
  {
    return is_multi_version_minor_sstable(key_.table_type_);
  }
  virtual bool is_multi_version_table() const
  {
    return is_multi_version_table(key_.table_type_);
  }
  virtual bool is_memtable() const
  {
    return is_memtable(key_.table_type_);
  }
  virtual bool is_frozen_memtable() const
  {
    return false;
  }
  virtual bool is_active_memtable() const
  {
    return false;
  }
  virtual bool is_trans_sstable() const
  {
    return is_trans_sstable(key_.table_type_);
  }
  OB_INLINE bool is_table_with_log_ts_range() const
  {
    return is_table_with_log_ts_range(key_.table_type_);
  }
  OB_INLINE bool is_complement_minor_sstable() const
  {
    return is_complement_minor_sstable(key_.table_type_);
  }
  OB_INLINE bool is_new_table_from_3x() const
  {
    return is_new_table_from_3x(key_.table_type_);
  }
  virtual OB_INLINE int64_t get_timestamp() const
  {
    return 0;
  }
  virtual bool is_multi_version_sparse_minor_sstable() const
  {
    return is_multi_version_sparse_minor_sstable(key_.table_type_);
  }

  DECLARE_TO_STRING;

  static bool is_sstable(const TableType table_type)
  {
    return ObITable::MEMTABLE < table_type && ObITable::MAX_TABLE_TYPE > table_type;
  }
  static bool is_major_sstable(const TableType table_type)
  {
    return ObITable::TableType::MAJOR_SSTABLE == table_type || is_trans_sstable(table_type);
  }

  static bool is_old_minor_sstable(const TableType table_type)
  {
    return ObITable::TableType::MINOR_SSTABLE == table_type;
  }
  static bool is_normal_minor_sstable(const TableType table_type)
  {
    return ObITable::TableType::MINOR_SSTABLE == table_type ||
           ObITable::TableType::MULTI_VERSION_MINOR_SSTABLE == table_type ||
           ObITable::TableType::MULTI_VERSION_SPARSE_MINOR_SSTABLE == table_type;
  }
  static bool is_minor_sstable(const TableType table_type)
  {
    return ObITable::TableType::MINOR_SSTABLE == table_type ||
           ObITable::TableType::MULTI_VERSION_MINOR_SSTABLE == table_type ||
           ObITable::TableType::MULTI_VERSION_SPARSE_MINOR_SSTABLE == table_type ||
           ObITable::TableType::MINI_MINOR_SSTABLE == table_type ||
           ObITable::TableType::COMPLEMENT_MINOR_SSTABLE == table_type;
  }
  static bool is_multi_version_minor_sstable(const TableType table_type)
  {
    return ObITable::TableType::MULTI_VERSION_MINOR_SSTABLE == table_type ||
           ObITable::TableType::MULTI_VERSION_SPARSE_MINOR_SSTABLE == table_type ||
           ObITable::TableType::MINI_MINOR_SSTABLE == table_type ||
           ObITable::TableType::COMPLEMENT_MINOR_SSTABLE == table_type;
  }

  static bool is_multi_version_table(const TableType table_type)
  {
    return ObITable::TableType::MEMTABLE == table_type || is_multi_version_minor_sstable(table_type);
  }

  static bool is_multi_version_sparse_minor_sstable(const TableType table_type)
  {
    return ObITable::TableType::MULTI_VERSION_SPARSE_MINOR_SSTABLE == table_type;
  }
  static bool is_mini_minor_sstable(const TableType table_type)
  {
    return ObITable::TableType::MINI_MINOR_SSTABLE == table_type;
  }
  static bool is_memtable(const TableType table_type)
  {
    return ObITable::TableType::MEMTABLE == table_type;
  }
  static bool is_trans_sstable(const TableType table_type)
  {
    return ObITable::TableType::TRANS_SSTABLE == table_type;
  }
  static bool is_table_with_log_ts_range(const TableType table_type)
  {
    return is_multi_version_table(table_type);
  }
  static bool is_complement_minor_sstable(const TableType table_type)
  {
    return table_type == ObITable::COMPLEMENT_MINOR_SSTABLE;
  }
  static bool is_new_table_from_3x(const TableType table_type)
  {
    return ObITable::TableType::TRANS_SSTABLE == table_type ||
           ObITable::TableType::COMPLEMENT_MINOR_SSTABLE == table_type ||
           ObITable::TableType::MULTI_VERSION_SPARSE_MINOR_SSTABLE == table_type;
  }

  OB_INLINE static const char* get_table_type_name(const TableType& table_type)
  {
    return is_table_type_valid(table_type) ? table_type_name_[table_type] : nullptr;
  }

protected:
  TableKey key_;
  int64_t ref_cnt_;

private:
  static const char* table_type_name_[TableType::MAX_TABLE_TYPE];
  DISALLOW_COPY_AND_ASSIGN(ObITable);
};

class ObTableHandle final {
public:
  ObTableHandle();
  ~ObTableHandle();
  storage::ObITable* get_table()
  {
    return table_;
  }
  const storage::ObITable* get_table() const
  {
    return table_;
  }
  int get_sstable(ObSSTable*& sstable);
  int get_sstable(const ObSSTable*& sstable) const;
  int get_old_sstable(ObOldSSTable*& sstable);
  int get_memtable(memtable::ObMemtable*& memtable);
  int get_memtable(const memtable::ObMemtable*& memtable) const;
  int set_table(ObITable* table);
  int assign(const ObTableHandle& other);
  bool is_valid() const;
  void reset();
  int get_sstable_schema_version(int64_t& schema_version) const;
  TO_STRING_KV(KP(table_), K(table_));

private:
  storage::ObITable* table_;
  DISALLOW_COPY_AND_ASSIGN(ObTableHandle);
};

class ObTablesHandle final {
public:
  typedef common::ObSEArray<storage::ObITable*, common::DEFAULT_STORE_CNT_IN_STORAGE> TableArray;
  ObTablesHandle();
  ~ObTablesHandle();
  OB_INLINE common::ObIArray<storage::ObITable*>& get_tables()
  {
    return tables_;
  }
  OB_INLINE const common::ObIArray<storage::ObITable*>& get_tables() const
  {
    return tables_;
  }
  OB_INLINE bool empty() const
  {
    return tables_.empty();
  }
  OB_INLINE int64_t get_count() const
  {
    return tables_.count();
  }
  OB_INLINE ObITable* get_last_table() const
  {
    return tables_.empty() ? NULL : tables_[tables_.count() - 1];
  }
  OB_INLINE ObITable* get_table(const int64_t idx) const
  {
    return tables_.at(idx);
  }

  int add_table(ObITable* table);
  int add_table(ObTableHandle& handle);
  int add_tables(const common::ObIArray<ObITable*>& tables);
  int add_tables(const ObTablesHandle& handle);
  int assign(const ObTablesHandle& other);
  int get_first_sstable(ObSSTable*& sstable) const;
  int get_last_major_sstable(ObSSTable*& sstable) const;
  int get_first_memtable(memtable::ObMemtable*& memtable);
  int get_last_memtable(memtable::ObMemtable*& memtable);
  int get_all_sstables(common::ObIArray<ObSSTable*>& sstables);
  int get_all_minor_sstables(common::ObIArray<ObSSTable*>& sstables);
  int get_all_memtables(common::ObIArray<memtable::ObMemtable*>& memtables);
  void reset();
  int reserve(const int64_t count);
  void set_retire_check();
  bool check_store_expire() const;
  bool has_split_source_table(const common::ObPartitionKey& pkey) const;
  TableArray::iterator table_begin()
  {
    return tables_.begin();
  }
  TableArray::iterator table_end()
  {
    return tables_.end();
  }
  int check_continues(const common::ObLogTsRange* log_ts_range);
  DECLARE_TO_STRING;

private:
  TableArray tables_;
  int64_t protection_cnt_;
  const bool* memstore_retired_;
  DISALLOW_COPY_AND_ASSIGN(ObTablesHandle);
};

OB_INLINE bool ObTablesHandle::check_store_expire() const
{
  if (NULL == memstore_retired_) {
    return false;
  }

  return ATOMIC_LOAD(memstore_retired_);
}

class ObTableProtector {
public:
  static void hold(ObITable& table);
  static void release(ObITable& table);
};

OB_INLINE bool ObITable::is_table_type_valid(const TableType& type)
{
  return type >= ObITable::MEMTABLE && type < ObITable::MAX_TABLE_TYPE;
}

OB_INLINE bool ObITable::TableKey::is_valid(const bool skip_version_range, const bool skip_log_ts_range) const
{
  bool valid = true;

  if (!ObITable::is_table_type_valid(table_type_)) {
    valid = false;
  } else if (!pkey_.is_valid()) {
    valid = false;
  } else if (common::OB_INVALID_ID == table_id_) {
    valid = false;
  } else if (!skip_version_range && !trans_version_range_.is_valid()) {
    valid = false;
  } else if (!version_.is_valid() && version_ != 0) {
    valid = false;
  } else if (ObITable::is_table_with_log_ts_range(table_type_) && !skip_log_ts_range && !log_ts_range_.is_valid()) {
    valid = false;
  }

  return valid;
}

OB_INLINE int64_t ObITable::get_snapshot_version() const
{
  return key_.trans_version_range_.snapshot_version_;
}

OB_INLINE int64_t ObITable::get_base_version() const
{
  return key_.trans_version_range_.base_version_;
}

OB_INLINE int64_t ObITable::get_multi_version_start() const
{
  return key_.trans_version_range_.multi_version_start_;
}

OB_INLINE int64_t ObITable::get_start_log_ts() const
{
  return key_.log_ts_range_.start_log_ts_;
}

OB_INLINE int64_t ObITable::get_end_log_ts() const
{
  return key_.log_ts_range_.end_log_ts_;
}

OB_INLINE int64_t ObITable::get_max_log_ts() const
{
  return key_.log_ts_range_.max_log_ts_;
}

OB_INLINE bool ObITable::contain(const int64_t snapshot_version) const
{
  return key_.trans_version_range_.contain(snapshot_version);
}

bool ObITable::TableKey::operator==(const TableKey& table_key) const
{
  bool bret = table_type_ == table_key.table_type_ && pkey_ == table_key.pkey_ && table_id_ == table_key.table_id_ &&
              trans_version_range_ == table_key.trans_version_range_ && version_ == table_key.version_;
  if (is_table_log_ts_comparable()) {
    bret = bret && (log_ts_range_ == table_key.log_ts_range_);
  }
  return bret;
}

bool ObITable::TableKey::operator!=(const TableKey& table_key) const
{
  return !(this->operator==(table_key));
}

}  // namespace storage
}  // namespace oceanbase

#endif /* SRC_STORAGE_OB_I_TABLE_H_ */
