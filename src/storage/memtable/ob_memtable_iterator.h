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

#ifndef OCEANBASE_MEMTABLE_OB_MEMTABLE_ITERATOR_
#define OCEANBASE_MEMTABLE_OB_MEMTABLE_ITERATOR_

#include "share/ob_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/profile/ob_active_resource_list.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"

#include "common/cell/ob_cell_reader.h"

#include "storage/memtable/ob_memtable_interface.h"
#include "storage/memtable/ob_memtable_key.h"
#include "storage/memtable/mvcc/ob_mvcc_iterator.h"
#include "storage/memtable/mvcc/ob_mvcc_engine.h"
#include "storage/memtable/mvcc/ob_crtp_util.h"
#include "storage/memtable/mvcc/ob_multi_version_iterator.h"
#include "storage/memtable/ob_nop_bitmap.h"
#include "storage/memtable/ob_memtable_row_reader.h"

#include "storage/ob_i_store.h"  //for ObStoreRow and ObStoreRowIterator

namespace oceanbase {
namespace memtable {

class ObIMemtableIterator : public storage::ObStoreRowIterator {
public:
  ObIMemtableIterator()
  {}
  virtual ~ObIMemtableIterator()
  {}
  virtual int get_next_row(const storage::ObStoreRow*& row)
  {
    int ret = common::OB_SUCCESS;
    if (OB_SUCC(inner_get_next_row(row))) {
      EVENT_INC(ObStatEventIds::MEMSTORE_READ_ROW_COUNT);
    }
    return ret;
  }

protected:
  virtual int inner_get_next_row(const storage::ObStoreRow*& row) = 0;
};

class ObIMemtableScanIterator : public ObIMemtableIterator {
public:
  ObIMemtableScanIterator()
  {}
  virtual ~ObIMemtableScanIterator()
  {}
  // virtual int init(
  //     const storage::ObTableIterParam &param,
  //     storage::ObTableAccessContext &context,
  //     ObMvccEngine &mvcc_engine,
  //     const common::ObStoreRange &range) = 0;
  virtual int get_next_row(const storage::ObStoreRow*& row)
  {
    int ret = common::OB_SUCCESS;
    if (OB_SUCC(inner_get_next_row(row))) {
      EVENT_INC(ObStatEventIds::MEMSTORE_READ_ROW_COUNT);
    }
    return ret;
  }

protected:
  virtual int inner_get_next_row(const storage::ObStoreRow*& row) = 0;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

struct ObIteratorAllocator {
  typedef ObWithArenaT<0> Row;
  typedef ObWithArenaT<1> Cell;
};

class ObMemtableGetIterator : public ObIMemtableIterator {
public:
  ObMemtableGetIterator();
  virtual ~ObMemtableGetIterator();
  int init(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context, ObIMemtable& memtable);
  void set_rowkey(const common::ObExtStoreRowkey& rowkey);
  virtual int init(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      storage::ObITable* table, const void* query_range) override;
  virtual int inner_get_next_row(const storage::ObStoreRow*& row);
  virtual void reset();
  virtual void reuse() override
  {
    reset();
  }

private:
  // means GETITER
  static const uint64_t VALID_MAGIC_NUM = 0x5245544954454700;
  DISALLOW_COPY_AND_ASSIGN(ObMemtableGetIterator);

private:
  const uint64_t MAGIC_;
  bool is_inited_;
  int32_t rowkey_iter_;
  const storage::ObTableIterParam* param_;
  storage::ObTableAccessContext* context_;
  ObIMemtable* memtable_;
  const common::ObExtStoreRowkey* rowkey_;
  storage::ObStoreRow cur_row_;
};

class ObMemtableScanIterator : public ObIMemtableScanIterator {
public:
  ObMemtableScanIterator();
  virtual ~ObMemtableScanIterator();

public:
  int init(ObIMemtable* memtable, const storage::ObTableIterParam& param, storage::ObTableAccessContext& context);
  int set_range(const common::ObExtStoreRange& range);
  virtual int init(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      storage::ObITable* table, const void* query_range) override;

public:
  virtual int inner_get_next_row(const storage::ObStoreRow*& row);
  virtual void reset();
  virtual void reuse() override
  {
    reset();
  }
  virtual int get_gap_end(int64_t& range_idx, const common::ObStoreRowkey*& gap_key, int64_t& gap_size)
  {
    int ret = common::OB_SUCCESS;
    range_idx = 0;
    transaction::ObTransSnapInfo snapshot_info;
    if (OB_SUCC(context_->store_ctx_->get_snapshot_info(snapshot_info))) {
      ret = row_iter_.get_end_gap_key(snapshot_info, gap_key, gap_size);
    }
    return ret;
  }
  virtual int skip_range(int64_t range_idx, const common::ObStoreRowkey* gap_key, const bool include_gap_key);
  ObIMemtable* get_memtable()
  {
    return memtable_;
  }
  int get_key_val(const ObMemtableKey*& key, ObMvccRow*& row)
  {
    return row_iter_.get_key_val(key, row);
  }
  int64_t get_read_snapshot() const
  {
    return (NULL == context_ || NULL == context_->store_ctx_ || NULL == context_->store_ctx_->mem_ctx_)
               ? 0
               : context_->store_ctx_->mem_ctx_->get_read_snapshot();
  }
  uint8_t get_iter_flag()
  {
    return iter_flag_;
  }

protected:
  int get_real_range(const common::ObStoreRange& range, common::ObStoreRange& real_range);
  int prepare_scan();

public:
  static const int64_t ROW_ALLOCATOR_PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  static const int64_t CELL_ALLOCATOR_PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;

private:
  // means SCANITER
  static const uint64_t VALID_MAGIC_NUM = 0x524554494e414353;
  DISALLOW_COPY_AND_ASSIGN(ObMemtableScanIterator);

protected:
  const uint64_t MAGIC_;
  common::ActiveResource active_resource_;
  bool is_inited_;
  bool is_scan_start_;
  const storage::ObTableIterParam* param_;
  storage::ObTableAccessContext* context_;
  ObIMemtable* memtable_;
  common::ObExtStoreRange cur_range_;
  const common::ObIArray<share::schema::ObColDesc>* columns_;
  const share::schema::ColumnMap* cols_map_;
  ObMvccRowIterator row_iter_;
  storage::ObStoreRow row_;
  ObNopBitMap bitmap_;
  uint8_t iter_flag_;
  bool last_row_has_null_;
  bool first_row_;
  bool key_has_null_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

class ObMemtableMGetIterator : public ObIMemtableIterator {
public:
  ObMemtableMGetIterator();
  virtual ~ObMemtableMGetIterator();

public:
  virtual int init(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      storage::ObITable* table, const void* query_range) override;

public:
  virtual int inner_get_next_row(const storage::ObStoreRow*& row);
  virtual void reset();
  virtual void reuse() override
  {
    reset();
  }

public:
  static const int64_t ROW_ALLOCATOR_PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;

private:
  // means MGETITER
  static const uint64_t VALID_MAGIC_NUM = 0x524554495445474d;
  DISALLOW_COPY_AND_ASSIGN(ObMemtableMGetIterator);

private:
  const uint64_t MAGIC_;
  bool is_inited_;
  const storage::ObTableIterParam* param_;
  storage::ObTableAccessContext* context_;
  ObIMemtable* memtable_;
  const common::ObIArray<common::ObExtStoreRowkey>* rowkeys_;
  const share::schema::ColumnMap* cols_map_;
  int64_t rowkey_iter_;
  storage::ObStoreRow cur_row_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////
class ObMemtableMScanIterator : public ObMemtableScanIterator {
public:
  static const int64_t ROW_ALLOCATOR_PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  static const int64_t CELL_ALLOCATOR_PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  ObMemtableMScanIterator();
  virtual ~ObMemtableMScanIterator();

public:
  int init(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context, storage::ObITable* table,
      const void* query_range) override;
  virtual int inner_get_next_row(const storage::ObStoreRow*& row);
  virtual void reset();
  virtual void reuse() override
  {
    reset();
  }
  virtual int skip_range(int64_t range_idx, const common::ObStoreRowkey* gap_key, const bool include_gap_key);
  virtual int get_gap_end(int64_t& range_idx, const common::ObStoreRowkey*& gap_key, int64_t& gap_size)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(ObMemtableScanIterator::get_gap_end(range_idx, gap_key, gap_size))) {
    } else {
      range_idx = cur_range_pos_;
    }
    return ret;
  }

private:
  int next_range();
  int is_range_scan(bool& range_scan);
  virtual int prepare_scan_range();
  virtual int get_next_row_for_get(const storage::ObStoreRow*& row);
  virtual int get_next_row_for_scan(const storage::ObStoreRow*& row);
  virtual int inner_get_next_row_for_scan(const storage::ObStoreRow*& row);

private:
  static const uint64_t VALID_MAGIC_NUM = 0x524554494e414353;
  static const int64_t LOCAL_ARRAY_SIZE = 16;
  const common::ObIArray<common::ObExtStoreRange>* ranges_;
  int64_t cur_range_pos_;
  DISALLOW_COPY_AND_ASSIGN(ObMemtableMScanIterator);
};

////////////////////////////////////////////////////////////////////////////////////////////////////

class ObMemtableMultiVersionScanIterator : public ObIMemtableScanIterator {
public:
  ObMemtableMultiVersionScanIterator();
  virtual ~ObMemtableMultiVersionScanIterator();

public:
  virtual int init(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      storage::ObITable* table, const void* query_range) override;

public:
  virtual int inner_get_next_row(const storage::ObStoreRow*& row);
  virtual void reset();
  virtual void reuse() override
  {
    reset();
  }
  enum ScanState { SCAN_BEGIN, SCAN_UNCOMMITTED_ROW, SCAN_COMPACT_ROW, SCAN_MULTI_VERSION_ROW, SCAN_END };
  // the iteration process is divided into 2 phases:
  // iterating Complement SSTable / iterating Mini SSTable
  enum ObIterStage { IS_ITER_COMPLEMENT, IS_ITER_MINI };
  TO_STRING_KV(KPC_(context), K_(row), KPC_(key), KPC_(value_iter), K_(scan_state), K_(iter_mode));

protected:
  virtual void row_reset();
  int get_compacted_multi_version_row(const storage::ObStoreRow*& row);
  int get_multi_version_row(const storage::ObStoreRow*& row);
  int get_uncommitted_row(const storage::ObStoreRow*& row);
  int switch_scan_state();
  void switch_to_committed_scan_state();
  virtual int init_next_value_iter();
  void row_state_reset();
  virtual int init_row_cells(ObIAllocator* allocator);
  int get_next_row();

protected:
  // iterate row
  virtual int iterate_compacted_row(const common::ObStoreRowkey& key, storage::ObStoreRow& row);
  virtual int iterate_uncommitted_row(const ObStoreRowkey& key, storage::ObStoreRow& row);
  virtual int iterate_multi_version_row(const ObStoreRowkey& rowkey, storage::ObStoreRow& row);
  int set_compacted_row_state();
  void set_flag_and_version_for_compacted_row(const ObMvccTransNode* tnode, storage::ObStoreRow& row);

  int iterate_compacted_row_value_(storage::ObStoreRow& row);
  int iterate_uncommitted_row_value_(storage::ObStoreRow& row);
  int iterate_multi_version_row_value_(storage::ObStoreRow& row);

protected:
  struct ObOuputRowValidateChecker {
    ObOuputRowValidateChecker();
    ~ObOuputRowValidateChecker();
    void reset();
    int check_row_flag_valid(const storage::ObStoreRow& row);

    bool output_compact_row_flag_;
    bool output_last_row_flag_;
  };

  // means SCANITER
  static const uint64_t VALID_MAGIC_NUM = 0x524554494e414353;
  DISALLOW_COPY_AND_ASSIGN(ObMemtableMultiVersionScanIterator);

protected:
  const uint64_t MAGIC_;
  common::ActiveResource active_resource_;
  bool is_inited_;
  storage::ObColDescArray columns_;
  const share::schema::ColumnMap* cols_map_;
  ObMemtableKey* start_key_;
  ObMemtableKey* end_key_;
  storage::ObTableAccessContext* context_;
  ObMultiVersionRowIterator row_iter_;
  storage::ObStoreRow row_;
  const ObMemtableKey* key_;
  ObMultiVersionValueIterator* value_iter_;
  bool key_has_null_;
  ObNopBitMap bitmap_;
  int64_t rowkey_count_;
  ScanState scan_state_;
  transaction::ObTransID trans_id_;
  int32_t trans_version_col_idx_;
  int32_t sql_sequence_col_idx_;
  ObMemtableIterRowReader iter_row_reader_;
  storage::ObTableIterParam::ObIterTransNodeMode iter_mode_;
  ObOuputRowValidateChecker row_checker_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////
class ObReadRow {
  DEFINE_ALLOCATOR_WRAPPER
public:
  static int iterate_row(const share::schema::ColumnMap& column_index, const storage::ObColDescIArray& columns,
      const common::ObStoreRowkey& key, ObIMvccValueIterator* value_iter, storage::ObStoreRow& row, ObNopBitMap& bitmap,
      bool& has_null, bool& key_has_null, int64_t& row_scn);
  static int exist(ObMvccValueIterator& value_iter, bool& is_exist, bool& has_found);
  static int get_row_header(ObMvccValueIterator& value_iter, uint32_t& modify_count, uint32_t& acc_checksum);
  static int iterate_row_key(bool& key_has_null, const common::ObStoreRowkey& rowkey, storage::ObStoreRow& row);

private:
  DISALLOW_COPY_AND_ASSIGN(ObReadRow);

private:
  static int iterate_row_value_(const share::schema::ColumnMap& column_index, const storage::ObColDescIArray& columns,
      ObIMvccValueIterator* value_iter, storage::ObStoreRow& row, ObNopBitMap& bitmap, bool& has_null,
      int64_t& row_scn);

private:
  /*static int iterate_trans_node_(
      bool& row_empty,
      const common::ObIArray<share::schema::ObColDesc> &columns,
      const share::schema::ColumnMap &column_index,
      common::ObCellReader &cci,
      storage::ObStoreRow &row,
      int64_t &filled_column_count,
      bool &has_null);*/
  static int column_cast_(const common::ObObjMeta& column_meta, common::ObObj& value);
};

}  // namespace memtable
}  // namespace oceanbase

#endif  // OCEANBASE_MEMTABLE_OB_MEMTABLE_ITERATOR_
