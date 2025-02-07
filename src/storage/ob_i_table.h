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

#include "common/ob_range.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_drw_lock.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/ob_i_store.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/meta_mem/ob_tenant_meta_obj_pool.h"
#include "storage/meta_mem/ob_storage_meta_cache.h"
#include "share/ob_table_range.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_table_flag.h"

namespace oceanbase
{

namespace memtable
{
class ObMemtable;
}

namespace transaction
{
namespace tablelock
{
class ObLockMemtable;
}
}

namespace blocksstable
{
struct ObDatumRowkey;
struct ObDatumRange;
class ObSSTable;
}

namespace share
{
struct ObScnRange;
}

namespace storage
{
class ObIMemtable;
class ObITabletMemtable;
class ObTxDataMemtable;
class ObTxCtxMemtable;
class ObLSMemberMemtable;
class ObDDLKV;
class ObTenantMetaMemMgr;
struct ObTableIterParam;
struct ObTableAccessParam;
struct ObTableAccessContext;
struct ObRowsInfo;
class ObStoreRowIterator;
struct ObStoreCtx;

class ObITable
{
  OB_UNIS_VERSION_V(1);
public:

  // Attention! keep update with table_types in GV$OB_SSTABLES
  enum TableType : unsigned char
  {
    // < memtable start from here
    DATA_MEMTABLE = 0,
    TX_DATA_MEMTABLE = 1,
    TX_CTX_MEMTABLE = 2,
    LOCK_MEMTABLE = 3,
    DIRECT_LOAD_MEMTABLE = 4,
    MAX_MEMTABLE_TYPE,
    // < add new memtable here

    // < sstable start from here
    MAJOR_SSTABLE = 10, //Reuse type, also represents row store column group in column store
    MINOR_SSTABLE = 11,
    MINI_SSTABLE = 12,
    META_MAJOR_SSTABLE = 13,
    DDL_DUMP_SSTABLE = 14,
    REMOTE_LOGICAL_MINOR_SSTABLE = 15,
    DDL_MEM_SSTABLE = 16,
    COLUMN_ORIENTED_SSTABLE = 17,
    NORMAL_COLUMN_GROUP_SSTABLE = 18,
    ROWKEY_COLUMN_GROUP_SSTABLE = 19,
    COLUMN_ORIENTED_META_SSTABLE = 20,
    DDL_MERGE_CO_SSTABLE = 21, // used for column store ddl, for base sstable
    DDL_MERGE_CG_SSTABLE = 22, // used for column store ddl, for normal cg sstable, rowkey cg not supported
    DDL_MEM_CO_SSTABLE = 23,
    DDL_MEM_CG_SSTABLE = 24,
    DDL_MEM_MINI_SSTABLE = 25,
    MDS_MINI_SSTABLE = 26,
    MDS_MINOR_SSTABLE = 27,
    MICRO_MINI_SSTABLE = 28,
    // < add new sstable before here, See is_sstable()

    MAX_TABLE_TYPE
  };

  OB_INLINE static bool is_table_type_valid(const TableType &type);

  struct TableKey
  {
    OB_UNIS_VERSION(1);
  public:
    TableKey();

    uint64_t hash() const;
    int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
    void reset();
    OB_INLINE bool is_valid() const;
    OB_INLINE bool operator ==(const TableKey &table_key) const;
    OB_INLINE bool operator !=(const TableKey &table_key) const;

    // table type interfaces
    OB_INLINE bool is_memtable() const { return ObITable::is_memtable(table_type_); }
    OB_INLINE bool is_resident_memtable() const { return ObITable::is_resident_memtable(table_type_); }
    OB_INLINE bool is_data_memtable() const { return ObITable::is_data_memtable(table_type_); }
    OB_INLINE bool is_tx_data_memtable() const { return ObITable::is_tx_data_memtable(table_type_); }
    OB_INLINE bool is_tx_ctx_memtable() const { return ObITable::is_tx_ctx_memtable(table_type_); }
    OB_INLINE bool is_lock_memtable() const { return ObITable::is_lock_memtable(table_type_); }
    OB_INLINE bool is_minor_sstable() const { return ObITable::is_minor_sstable(table_type_); }
    OB_INLINE bool is_mini_sstable() const { return ObITable::is_mini_sstable(table_type_); }
    OB_INLINE bool is_major_sstable() const { return ObITable::is_major_sstable(table_type_) || ObITable::is_meta_major_sstable(table_type_); }
    OB_INLINE bool is_major_or_ddl_merge_sstable() const { return is_major_sstable() || ObITable::is_ddl_merge_sstable(table_type_); }
    OB_INLINE bool is_ddl_merge_sstable() const { return ObITable::is_ddl_merge_sstable(table_type_); }
    OB_INLINE bool is_ddl_mem_co_cg_sstable() const { return ObITable::is_ddl_mem_co_cg_sstable(table_type_); }
    OB_INLINE bool is_meta_major_sstable() const { return ObITable::is_meta_major_sstable(table_type_); }
    OB_INLINE bool is_multi_version_table() const { return ObITable::is_multi_version_table(table_type_); }
    OB_INLINE bool is_mds_sstable() const { return ObITable::is_mds_sstable(table_type_); }
    OB_INLINE bool is_mds_mini_sstable() const { return ObITable::is_mds_mini_sstable(table_type_); }
    OB_INLINE bool is_mds_minor_sstable() const { return ObITable::is_mds_minor_sstable(table_type_); }
    OB_INLINE bool is_ddl_sstable() const { return ObITable::is_ddl_sstable(table_type_); }
    OB_INLINE bool is_ddl_dump_sstable() const { return ObITable::is_ddl_dump_sstable(table_type_); }
    OB_INLINE bool is_ddl_mem_sstable() const { return ObITable::is_ddl_mem_sstable(table_type_); }
    OB_INLINE bool is_table_with_scn_range() const { return ObITable::is_table_with_scn_range(table_type_); }
    OB_INLINE bool is_remote_logical_minor_sstable() const { return ObITable::is_remote_logical_minor_sstable(table_type_); }
    OB_INLINE bool is_co_sstable() const { return ObITable::is_co_sstable(table_type_); }
    OB_INLINE bool is_rowkey_cg_sstable() const { return ObITable::is_rowkey_cg_sstable(table_type_); }
    OB_INLINE bool is_normal_cg_sstable() const { return ObITable::is_normal_cg_sstable(table_type_); }
    OB_INLINE bool is_cg_sstable() const { return ObITable::is_cg_sstable(table_type_); }
    OB_INLINE bool is_column_store_sstable() const { return is_co_sstable() || is_cg_sstable(); }
    OB_INLINE bool is_row_store_major_sstable() const { return ObITable::is_row_store_major_sstable(table_type_); }
    OB_INLINE bool is_column_store_major_sstable() const { return ObITable::is_column_store_major_sstable(table_type_); }
    OB_INLINE bool is_true_major_sstable() const { return is_row_store_major_sstable() || is_column_store_major_sstable(); }

    OB_INLINE const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
    share::SCN get_start_scn() const { return scn_range_.start_scn_.atomic_get(); }
    share::SCN get_end_scn() const { return scn_range_.end_scn_.atomic_get(); }
    OB_INLINE int64_t get_snapshot_version() const
    {
      return version_range_.snapshot_version_;
    }
    OB_INLINE uint16_t get_column_group_id() const { return column_group_idx_; }
    OB_INLINE TableKey& operator=(const TableKey &key)
    {
      table_type_ = key.table_type_;
      column_group_idx_ = key.column_group_idx_;
      tablet_id_ = key.tablet_id_;
      scn_range_ = key.scn_range_;
      return *this;
    }

    TO_STRING_KV(K_(tablet_id), K_(column_group_idx), "table_type", get_table_type_name(table_type_),
        K_(scn_range));

  public:
    common::ObTabletID tablet_id_;
    union {
      common::ObNewVersionRange version_range_;
      share::ObScnRange scn_range_;
    };
    uint16_t column_group_idx_;
    ObITable::TableType table_type_;
  };

  ObITable();
  virtual ~ObITable() {}

  int init(const TableKey &table_key);
  void reset();
  virtual int safe_to_destroy(bool &is_safe);
  OB_INLINE const TableKey &get_key() const { return key_; }
  void set_scn_range(share::ObScnRange scn_range) { key_.scn_range_ = scn_range; }
  void set_table_type(ObITable::TableType table_type) { key_.table_type_ = table_type; }
  void set_snapshot_version(int64_t version) { key_.version_range_.snapshot_version_ = version; }
  ObITable::TableType get_table_type() { return key_.table_type_; }

  virtual int exist(
      ObStoreCtx &ctx,
      const uint64_t table_id,
      const storage::ObITableReadInfo &read_info,
      const blocksstable::ObDatumRowkey &rowkey,
      bool &is_exist,
      bool &has_found);
  virtual int exist(
      const ObTableIterParam &param,
	  ObTableAccessContext &context,
	  const blocksstable::ObDatumRowkey &rowkey,
	  bool &is_exist,
	  bool &has_found);

  virtual int exist(
      ObRowsInfo &rowsInfo,
      bool &is_exist,
      bool &has_found);

  virtual int scan(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const blocksstable::ObDatumRange &key_range,
      ObStoreRowIterator *&row_iter) = 0;

  virtual int get(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObDatumRowkey &rowkey,
      ObStoreRowIterator *&row_iter) = 0;

  virtual int multi_get(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const common::ObIArray<blocksstable::ObDatumRowkey> &rowkeys,
      ObStoreRowIterator *&row_iter) = 0;

  virtual int multi_scan(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const common::ObIArray<blocksstable::ObDatumRange> &ranges,
      ObStoreRowIterator *&row_iter) = 0;

  virtual OB_INLINE share::SCN get_start_scn() const;
  virtual OB_INLINE share::SCN get_end_scn() const;
  virtual OB_INLINE share::ObScnRange &get_scn_range() { return key_.scn_range_; }
  virtual OB_INLINE bool is_trans_state_deterministic() { return get_upper_trans_version() < INT64_MAX; }
  virtual int64_t get_snapshot_version() const { return key_.get_snapshot_version(); }
  virtual int64_t get_upper_trans_version() const { return get_snapshot_version(); }
  virtual int64_t get_max_merged_trans_version() const { return get_snapshot_version(); }
  virtual int get_frozen_schema_version(int64_t &schema_version) const = 0;
  OB_INLINE uint16_t get_column_group_id() const { return key_.get_column_group_id(); }
  OB_INLINE common::ObNewVersionRange &get_version_range() { return key_.version_range_; }
  virtual void inc_ref();
  virtual int64_t dec_ref();
  virtual int64_t get_ref() const { return ATOMIC_LOAD(&ref_cnt_); }

  // TODO @hanhui so many table type judgement
  virtual bool is_sstable() const { return is_sstable(key_.table_type_); }
  virtual bool is_co_sstable() const { return is_co_sstable(key_.table_type_); }
  virtual bool is_rowkey_cg_sstable() const { return is_rowkey_cg_sstable(key_.table_type_); }
  virtual bool is_normal_cg_sstable() const { return is_normal_cg_sstable(key_.table_type_); }
  virtual bool is_cg_sstable() const { return is_cg_sstable(key_.table_type_); }
  virtual bool is_column_store_sstable() const { return is_co_sstable() || is_cg_sstable(); }
  virtual bool is_meta_major_sstable() const { return is_meta_major_sstable(key_.table_type_); }
  virtual bool is_major_sstable() const { return is_major_sstable(key_.table_type_) || is_meta_major_sstable(key_.table_type_); }
  virtual bool is_major_or_ddl_merge_sstable() const { return is_major_sstable() || is_ddl_merge_sstable(key_.table_type_); }
  virtual bool is_minor_sstable() const { return is_minor_sstable(key_.table_type_); }
  virtual bool is_mini_sstable() const { return is_mini_sstable(key_.table_type_); }
  virtual bool is_mds_minor_sstable() const { return is_mds_minor_sstable(key_.table_type_); }
  virtual bool is_mds_mini_sstable() const { return is_mds_mini_sstable(key_.table_type_); }
  virtual bool is_multi_version_minor_sstable() const { return is_multi_version_minor_sstable(key_.table_type_); }
  virtual bool is_multi_version_table() const { return is_multi_version_table(key_.table_type_); }
  virtual bool is_memtable() const { return is_memtable(key_.table_type_); }
  virtual bool is_resident_memtable() const { return is_resident_memtable(key_.table_type_); }
  virtual bool is_tablet_memtable() const { return is_tablet_memtable(key_.table_type_); }
  virtual bool is_direct_load_memtable() const { return is_direct_load_memtable(key_.table_type_); }
  virtual bool is_data_memtable() const { return is_data_memtable(key_.table_type_); }
  virtual bool is_tx_data_memtable() const { return is_tx_data_memtable(key_.table_type_); }
  virtual bool is_tx_ctx_memtable() const { return is_tx_ctx_memtable(key_.table_type_); }
  virtual bool is_lock_memtable() const { return is_lock_memtable(key_.table_type_); }
  virtual bool is_frozen_memtable() { return false; }
  virtual bool is_active_memtable() { return false; }
  OB_INLINE bool is_table_with_scn_range() const { return is_table_with_scn_range(key_.table_type_); }
  virtual OB_INLINE int64_t get_timestamp() const { return 0; }
  virtual bool is_mds_sstable() const { return is_mds_sstable(key_.table_type_); }
  virtual bool is_ddl_sstable() const { return is_ddl_sstable(key_.table_type_); }
  virtual bool is_ddl_dump_sstable() const { return is_ddl_dump_sstable(key_.table_type_); }
  virtual bool is_ddl_mem_sstable() const { return is_ddl_mem_sstable(key_.table_type_); }
  virtual bool is_ddl_merge_sstable() const { return is_ddl_merge_sstable(key_.table_type_); }
  virtual bool is_ddl_mem_co_cg_sstable() const { return is_ddl_mem_co_cg_sstable(key_.table_type_); }
  virtual bool is_remote_logical_minor_sstable() const { return is_remote_logical_minor_sstable(key_.table_type_); }
  virtual bool is_empty() const = 0;
  virtual bool no_data_to_read() const { return is_empty(); }
  virtual bool is_ddl_merge_empty_sstable() const { return is_empty() && is_ddl_merge_sstable(); }
  DECLARE_VIRTUAL_TO_STRING;

  static bool is_sstable(const TableType table_type)
  {
    return ObITable::MAJOR_SSTABLE <= table_type && table_type < ObITable::MAX_TABLE_TYPE;
  }
  static bool is_major_sstable(const TableType table_type)
  {
    return ObITable::TableType::MAJOR_SSTABLE == table_type
      || ObITable::TableType::COLUMN_ORIENTED_SSTABLE == table_type
      || ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE == table_type
      || ObITable::TableType::ROWKEY_COLUMN_GROUP_SSTABLE == table_type;
  }
  static bool is_minor_sstable(const TableType table_type)
  {
    return ObITable::TableType::MINOR_SSTABLE == table_type
      || ObITable::TableType::MINI_SSTABLE == table_type
      || ObITable::TableType::REMOTE_LOGICAL_MINOR_SSTABLE == table_type;
  }
  static bool is_multi_version_minor_sstable(const TableType table_type)
  {
    return ObITable::TableType::MINOR_SSTABLE == table_type
        || ObITable::TableType::MINI_SSTABLE == table_type
        || ObITable::TableType::DDL_MEM_MINI_SSTABLE == table_type
        || ObITable::TableType::MDS_MINOR_SSTABLE == table_type
        || ObITable::TableType::MDS_MINI_SSTABLE == table_type
        || ObITable::TableType::REMOTE_LOGICAL_MINOR_SSTABLE == table_type;
  }

  static bool is_multi_version_table(const TableType table_type)
  {
    // Note (yanyuan.cxf) why we need TX_CTX_MEMTABLE and TX_DATA_MEMTABLE
    return ObITable::TableType::DATA_MEMTABLE == table_type
        || ObITable::TableType::TX_CTX_MEMTABLE == table_type
        || ObITable::TableType::TX_DATA_MEMTABLE == table_type
        || ObITable::TableType::LOCK_MEMTABLE == table_type
        || is_multi_version_minor_sstable(table_type);
  }

  static bool is_mini_sstable(const TableType table_type)
  {
    return ObITable::TableType::MINI_SSTABLE == table_type
        || ObITable::TableType::MDS_MINI_SSTABLE == table_type;
  }

  /*
   *column store sstables
   */
  static bool is_co_sstable(const TableType table_type)
  {
    return ObITable::TableType::COLUMN_ORIENTED_SSTABLE == table_type
        || ObITable::TableType::COLUMN_ORIENTED_META_SSTABLE == table_type
      || ObITable::TableType::DDL_MERGE_CO_SSTABLE == table_type;
  }
  static bool is_normal_cg_sstable(const TableType table_type)
  {
    return ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE == table_type
      || ObITable::TableType::DDL_MERGE_CG_SSTABLE == table_type
      || ObITable::TableType::DDL_MEM_CG_SSTABLE == table_type;
  }
  static bool is_rowkey_cg_sstable(const TableType table_type)
  {
    return ObITable::TableType::ROWKEY_COLUMN_GROUP_SSTABLE == table_type;
  }
  static bool is_cg_sstable(const TableType table_type)
  {
    return is_normal_cg_sstable(table_type)
        || is_rowkey_cg_sstable(table_type);
  }
  static bool is_column_store_sstable(const TableType table_type)
  {
    return is_co_sstable(table_type)
        || is_cg_sstable(table_type)
        || ObITable::TableType::DDL_MEM_CO_SSTABLE == table_type;
  }

  static bool is_remote_logical_minor_sstable(const TableType table_type)
  {
    return ObITable::TableType::REMOTE_LOGICAL_MINOR_SSTABLE == table_type;
  }

  static bool is_memtable(const TableType table_type)
  {
    return ObITable::TableType::DATA_MEMTABLE <= table_type &&
           ObITable::TableType::MAX_MEMTABLE_TYPE > table_type;
  }
  static bool is_resident_memtable(const TableType table_type)
  {
    return (ObITable::TableType::TX_CTX_MEMTABLE == table_type
        || ObITable::TableType::LOCK_MEMTABLE == table_type);
  }
  static bool is_tablet_memtable(const TableType table_type)
  {
    return (ObITable::TableType::DATA_MEMTABLE == table_type ||
            ObITable::TableType::DIRECT_LOAD_MEMTABLE == table_type);
  }
  static bool is_direct_load_memtable(const TableType table_type)
  {
    return ObITable::TableType::DIRECT_LOAD_MEMTABLE == table_type;
  }
  static bool is_data_memtable(const TableType table_type)
  {
    return ObITable::TableType::DATA_MEMTABLE == table_type;
  }

  static bool is_tx_data_memtable(const TableType table_type)
  {
    return ObITable::TableType::TX_DATA_MEMTABLE == table_type;
  }

  static bool is_tx_ctx_memtable(const TableType table_type)
  {
    return ObITable::TableType::TX_CTX_MEMTABLE == table_type;
  }

  static bool is_lock_memtable(const TableType table_type)
  {
    return ObITable::TableType::LOCK_MEMTABLE == table_type;
  }

  static bool is_meta_major_sstable(const TableType table_type)
  {
    return ObITable::TableType::META_MAJOR_SSTABLE == table_type
        || ObITable::TableType::COLUMN_ORIENTED_META_SSTABLE == table_type;
  }
  static bool is_ddl_sstable(const TableType table_type)
  {
    return ObITable::TableType::DDL_DUMP_SSTABLE == table_type
      || ObITable::TableType::DDL_MERGE_CO_SSTABLE == table_type
      || ObITable::TableType::DDL_MERGE_CG_SSTABLE == table_type
      || ObITable::TableType::DDL_MEM_SSTABLE == table_type
      || ObITable::TableType::DDL_MEM_CO_SSTABLE == table_type
      || ObITable::TableType::DDL_MEM_CG_SSTABLE == table_type;
  }
  static bool is_ddl_dump_sstable(const TableType table_type)
  {
    return ObITable::TableType::DDL_DUMP_SSTABLE == table_type
      || ObITable::TableType::DDL_MERGE_CO_SSTABLE == table_type
      || ObITable::TableType::DDL_MERGE_CG_SSTABLE == table_type;
  }
  static bool is_ddl_mem_sstable(const TableType table_type)
  {
    return ObITable::TableType::DDL_MEM_SSTABLE == table_type
      || ObITable::TableType::DDL_MEM_CO_SSTABLE == table_type
      || ObITable::TableType::DDL_MEM_CG_SSTABLE == table_type
      || ObITable::TableType::DDL_MEM_MINI_SSTABLE == table_type;
  }
  static bool is_ddl_merge_sstable(const TableType table_type)
  {
    return ObITable::TableType::DDL_MERGE_CO_SSTABLE == table_type
      || ObITable::TableType::DDL_MERGE_CG_SSTABLE == table_type;
  }
  static bool is_ddl_mem_co_cg_sstable(const TableType table_type)
  {
    return ObITable::TableType::DDL_MEM_CG_SSTABLE == table_type
      || ObITable::TableType::DDL_MEM_CO_SSTABLE == table_type;
  }
  static bool is_mds_mini_sstable(const TableType table_type)
  {
    return ObITable::TableType::MDS_MINI_SSTABLE == table_type;
  }
  static bool is_mds_minor_sstable(const TableType table_type)
  {
    return ObITable::TableType::MDS_MINOR_SSTABLE == table_type;
  }
  static bool is_mds_sstable(const TableType table_type)
  {
    return is_mds_mini_sstable(table_type) || is_mds_minor_sstable(table_type);
  }
  static bool is_row_store_major_sstable(const TableType table_type)
  {
    return ObITable::TableType::MAJOR_SSTABLE == table_type;
  }
  static bool is_column_store_major_sstable(const TableType table_type)
  {
    return ObITable::TableType::COLUMN_ORIENTED_SSTABLE == table_type;
  }
  static bool is_table_with_scn_range(const TableType table_type)
  {
    return is_multi_version_table(table_type) || is_meta_major_sstable(table_type);
  }
  // row store sstable and corresponding column store sstable
  static bool is_twin_major_sstable(const TableKey &rs_key, const TableKey &cs_key)
  {
    return rs_key.is_true_major_sstable()
        && cs_key.is_true_major_sstable()
        && rs_key.tablet_id_ == cs_key.tablet_id_
        && rs_key.scn_range_ == cs_key.scn_range_;
  }
  OB_INLINE static const char* get_table_type_name(const TableType &table_type)
  {
    return is_table_type_valid(table_type) ? table_type_name_[table_type] : nullptr;
  }

protected:
  TableKey key_;
  int64_t ref_cnt_;
private:
  const static char * table_type_name_[];
  DISALLOW_COPY_AND_ASSIGN(ObITable);
};

inline void ObITable::inc_ref()
{
  int64_t cnt = ATOMIC_AAF(&ref_cnt_, 1);
  STORAGE_LOG(DEBUG, "table inc ref", KP(this), "table_key", key_, "ref_cnt", cnt, K(lbt()));
}

inline int64_t ObITable::dec_ref()
{
  int64_t cnt = ATOMIC_SAF(&ref_cnt_, 1 /* just sub 1 */);
  STORAGE_LOG(DEBUG, "table dec ref", KP(this), "table_key", key_, "ref_cnt", cnt, K(lbt()));

  return cnt;
}

class ObTableHandleV2 final
{
  friend class ObTablesHandleArray;
public:
  ObTableHandleV2();
  ~ObTableHandleV2();

  bool is_valid() const;
  void reset();

  OB_INLINE ObITable *get_table() { return table_; }
  OB_INLINE const ObITable *get_table() const { return table_; }
  OB_INLINE ObTenantMetaMemMgr *get_t3m() { return t3m_; }
  OB_INLINE common::ObIAllocator *get_allocator() { return allocator_; }

  int get_sstable(blocksstable::ObSSTable *&sstable);
  int get_sstable(const blocksstable::ObSSTable *&sstable) const;
  int get_memtable(ObIMemtable *&memtable);
  int get_memtable(const ObIMemtable *&memtable) const;
  int get_tablet_memtable(ObITabletMemtable *&memtable);
  int get_tablet_memtable(const ObITabletMemtable *&memtable) const;
  int get_data_memtable(memtable::ObMemtable *&memtable);
  int get_data_memtable(const memtable::ObMemtable *&memtable) const;
  int get_tx_data_memtable(ObTxDataMemtable *&memtable);
  int get_tx_data_memtable(const ObTxDataMemtable *&memtable) const;
  int get_tx_ctx_memtable(ObTxCtxMemtable *&memtable);
  int get_tx_ctx_memtable(const ObTxCtxMemtable *&memtable) const;
  int get_lock_memtable(transaction::tablelock::ObLockMemtable *&memtable);
  int get_lock_memtable(const transaction::tablelock::ObLockMemtable *&memtable) const;
  int get_direct_load_memtable(ObDDLKV *&memtable);
  int get_direct_load_memtable(const ObDDLKV *&memtable) const;

  ObTableHandleV2(const ObTableHandleV2 &other);
  ObTableHandleV2 &operator= (const ObTableHandleV2 &other);

  int set_table(ObITable *const table, ObTenantMetaMemMgr *const t3m, const ObITable::TableType table_type);
  // TODO: simplify set_sstable interfaces
  // only used when create stand alone sstable
  int set_sstable(ObITable *table, common::ObIAllocator *allocator);
  // set sstable when lifetime is guaranteed by tablet
  int set_sstable_with_tablet(ObITable *table);
  int set_sstable(ObITable *table, const ObStorageMetaHandle &meta_handle);

  TO_STRING_KV(KP_(table), KP(t3m_), KP(allocator_), K(table_type_),
      K_(meta_handle), K_(lifetime_guaranteed_by_tablet));
private:
  ObITable *table_;
  ObTenantMetaMemMgr *t3m_;
  common::ObIAllocator *allocator_;
  ObStorageMetaHandle meta_handle_;
  ObITable::TableType table_type_;
  bool lifetime_guaranteed_by_tablet_;
};

class ObTablesHandleArray final
{
public:
  typedef common::ObArray<storage::ObTableHandleV2> HandlesArray;
  typedef bool (*IS_RIGH_SSTABLE_TYPE_FUNC)(const ObITable::TableType table_type);
  ObTablesHandleArray();
  ObTablesHandleArray(const uint64_t tenant_id);
  ~ObTablesHandleArray();
  void reset();
  OB_INLINE bool empty() const { return handles_array_.empty(); }
  OB_INLINE int64_t get_count() const { return handles_array_.count(); }
  OB_INLINE ObITable *get_table(const int64_t idx) const
  {
    return (idx < 0 || idx >= handles_array_.count()) ? nullptr : handles_array_.at(idx).table_;
  }

  int get_table(const int64_t idx, ObTableHandleV2 &table_handle) const;
  int get_table(const ObITable::TableKey &table_key, ObTableHandleV2 &table_handle) const;
  // TODO: simplify this add_sstable interface
  int add_sstable(ObITable *table, const ObStorageMetaHandle &meta_handle);
  int add_memtable(ObITable *table);
  int add_table(const ObTableHandleV2 &handle);
  int assign(const ObTablesHandleArray &other);
  int get_tables(common::ObIArray<ObITable *> &tables) const;
  int get_first_memtable(ObIMemtable *&memtable) const;
  int get_all_minor_sstables(common::ObIArray<ObITable *> &tables) const;
  int get_all_remote_major_sstables(common::ObIArray<ObITable *> &tables) const;
  int get_all_ddl_sstables(common::ObIArray<ObITable *> &tables) const;
  int get_all_mds_sstables(common::ObIArray<ObITable *> &tables) const;
  int check_continues(const share::ObScnRange *scn_range) const;
  DECLARE_TO_STRING;

private:
  int tablet_id_check(const common::ObTabletID &tablet_id);
  int get_sstable_with_type_(
      IS_RIGH_SSTABLE_TYPE_FUNC is_right_sstable_type,
      common::ObIArray<ObITable *> &tables) const;

private:
  common::ObTabletID tablet_id_;
  HandlesArray handles_array_;
  DISALLOW_COPY_AND_ASSIGN(ObTablesHandleArray);
};

OB_INLINE bool ObITable::is_table_type_valid(const TableType &type)
{
  return (type >= ObITable::DATA_MEMTABLE && type < ObITable::MAX_MEMTABLE_TYPE)
       || (type >= ObITable::MAJOR_SSTABLE && type < ObITable::MAX_TABLE_TYPE);
}

OB_INLINE bool ObITable::TableKey::is_valid() const
{
  bool valid = true;

  if (!ObITable::is_table_type_valid(table_type_)) {
    valid = false;
  } else if (!tablet_id_.is_valid()) {
    valid = false;
  } else if (ObITable::is_table_with_scn_range(table_type_)
      && !scn_range_.is_valid()) {
    valid = false;
  } else if (ObITable::is_major_sstable(table_type_)
      && !version_range_.is_valid()) {
    valid = false;
  }

  return valid;
}


OB_INLINE share::SCN ObITable::get_start_scn() const
{
  return key_.get_start_scn();
}

OB_INLINE share::SCN ObITable::get_end_scn() const
{
  return key_.get_end_scn();
}

bool ObITable::TableKey::operator ==(const TableKey &table_key) const
{
  bool bret = table_type_ == table_key.table_type_
      && column_group_idx_ == table_key.column_group_idx_
      && tablet_id_ == table_key.tablet_id_
      && scn_range_ == table_key.scn_range_;
  return bret;
}

bool ObITable::TableKey::operator !=(const TableKey &table_key) const
{
  return !(this->operator ==(table_key));
}


}//storage
}//oceanbase


#endif /* SRC_STORAGE_OB_I_TABLE_H_ */
