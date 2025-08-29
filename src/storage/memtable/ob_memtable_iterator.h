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
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/memtable/ob_memtable_key.h"
#include "storage/memtable/ob_memtable_single_row_reader.h"
#include "storage/memtable/mvcc/ob_mvcc_engine.h"
#include "storage/memtable/mvcc/ob_crtp_util.h"
#include "storage/memtable/mvcc/ob_multi_version_iterator.h"
#include "storage/memtable/ob_nop_bitmap.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "storage/access/ob_store_row_iterator.h"

namespace oceanbase
{

namespace storage
{
struct ObTransNodeDMLStat;
}

namespace memtable
{
class ObMemtableBlockRowScanner;

class ObIMemtableIterator : public storage::ObStoreRowIterator
{
public:
  ObIMemtableIterator() {}
  virtual ~ObIMemtableIterator() {}
  virtual int get_next_row(const blocksstable::ObDatumRow *&row) {
    int ret = common::OB_SUCCESS;
    if (OB_SUCC(inner_get_next_row(row))) {
      EVENT_INC(ObStatEventIds::MEMSTORE_READ_ROW_COUNT);
    }
    return ret;
  }
protected:
  virtual int inner_get_next_row(const blocksstable::ObDatumRow *&row) = 0;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

struct ObIteratorAllocator
{
  typedef ObWithArenaT<0> Row;
  typedef ObWithArenaT<1> Cell;
};

class ObMemtableGetIterator : public ObIMemtableIterator
{
public:
  ObMemtableGetIterator();
  virtual ~ObMemtableGetIterator();
  int init(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    ObIMemtable &memtable);
  void set_rowkey(const blocksstable::ObDatumRowkey &rowkey);
  virtual int init(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      storage::ObITable *table,
      const void *query_range) override;
  virtual void reset();
  virtual void reuse() override { reset(); }

protected:
  virtual int inner_get_next_row(const blocksstable::ObDatumRow *&row);

private:
  // means GETITER
  static const uint64_t VALID_MAGIC_NUM = 0x5245544954454700;
  DISALLOW_COPY_AND_ASSIGN(ObMemtableGetIterator);
  const uint64_t MAGIC_;
  bool is_inited_;
  int32_t rowkey_iter_;
  const storage::ObTableIterParam *param_;
  storage::ObTableAccessContext *context_;
  ObIMemtable *memtable_;
  const blocksstable::ObDatumRowkey *rowkey_;
  blocksstable::ObDatumRow cur_row_;
};

class ObMemtableScanIterator : public ObIMemtableIterator {
public:
  friend class ObMemtableBlockReader;
  ObMemtableScanIterator();
  virtual ~ObMemtableScanIterator();
  int set_range(const blocksstable::ObDatumRange &range);
  virtual int init(const storage::ObTableIterParam &param,
                   storage::ObTableAccessContext &context,
                   storage::ObITable *table,
                   const void *query_range) override;
  virtual void reset();
  virtual void reuse() override { reset(); }

protected:
  virtual int inner_get_next_row(const blocksstable::ObDatumRow *&row);
  int base_init_(const storage::ObTableIterParam &param,
                 storage::ObTableAccessContext &context,
                 storage::ObITable *table,
                 const void *query_range);

private:
  int enable_block_scan_(const storage::ObTableIterParam &param, storage::ObTableAccessContext &context);

public:
  static const int64_t ROW_ALLOCATOR_PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  static const int64_t CELL_ALLOCATOR_PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;

protected:
  const uint64_t MAGIC_;
  common::ActiveResource active_resource_;
  bool is_inited_;
  bool is_scan_start_;
  const storage::ObTableIterParam *param_;
  storage::ObTableAccessContext *context_;
  ObMemtableBlockRowScanner *mt_blk_scanner_;
  ObMemtableSingleRowReader single_row_reader_;
  blocksstable::ObDatumRange cur_range_;
private:
  static const uint64_t VALID_MAGIC_NUM = 0x524554494e414353;
  DISALLOW_COPY_AND_ASSIGN(ObMemtableScanIterator);
};

////////////////////////////////////////////////////////////////////////////////////////////////////


class ObMemtableMGetIterator : public ObIMemtableIterator
{
public:
  ObMemtableMGetIterator();
  virtual ~ObMemtableMGetIterator();
  virtual int init(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      storage::ObITable *table,
      const void *query_range) override;
  virtual void reset();
  virtual void reuse() override { reset(); }
  static const int64_t ROW_ALLOCATOR_PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;

protected:
  virtual int inner_get_next_row(const blocksstable::ObDatumRow *&row);

private:
  // means MGETITER
  static const uint64_t VALID_MAGIC_NUM = 0x524554495445474d;
  DISALLOW_COPY_AND_ASSIGN(ObMemtableMGetIterator);
  const uint64_t MAGIC_;
  bool is_inited_;
  const storage::ObTableIterParam *param_;
  storage::ObTableAccessContext *context_;
  ObIMemtable *memtable_;
  const common::ObIArray<blocksstable::ObDatumRowkey> *rowkeys_;
  share::schema::ColumnMap *cols_map_;
  int64_t rowkey_iter_;
  blocksstable::ObDatumRow cur_row_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////
class ObMemtableMScanIterator : public ObMemtableScanIterator
{
public:
  static const int64_t ROW_ALLOCATOR_PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  static const int64_t CELL_ALLOCATOR_PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  ObMemtableMScanIterator();
  virtual ~ObMemtableMScanIterator();
  int init(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      storage::ObITable *table,
      const void *query_range) override;
  virtual void reset();
  virtual void reuse() override { reset(); }

protected:
  virtual int inner_get_next_row(const blocksstable::ObDatumRow *&row);

private:
  static const uint64_t VALID_MAGIC_NUM = 0x524554494e414353;
  static const int64_t LOCAL_ARRAY_SIZE = 16;
  const common::ObIArray<blocksstable::ObDatumRange> *ranges_;
  int64_t cur_range_pos_;
  DISALLOW_COPY_AND_ASSIGN(ObMemtableMScanIterator);
};

////////////////////////////////////////////////////////////////////////////////////////////////////

class ObMemtableMultiVersionScanIterator : public ObIMemtableIterator
{
public:
  ObMemtableMultiVersionScanIterator();
  virtual ~ObMemtableMultiVersionScanIterator();
public:
  virtual int init(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      storage::ObITable *table,
      const void *query_range) override;
  int get_tnode_stat(storage::ObTransNodeDMLStat &tnode_stat) const;

public:
  virtual void reset();
  virtual void reuse() override { reset(); }
  enum ScanState
  {
    SCAN_BEGIN,
    SCAN_UNCOMMITTED_ROW,
    SCAN_COMPACT_ROW,
    SCAN_MULTI_VERSION_ROW,
    SCAN_END
  };
  TO_STRING_KV(KPC_(context), K_(row), KPC_(key), KPC_(value_iter), K_(scan_state), K_(enable_delete_insert));
protected:
  virtual int inner_get_next_row(const blocksstable::ObDatumRow *&row);
  virtual void row_reset();
  int get_compacted_multi_version_row(const blocksstable::ObDatumRow *&row);
  int get_multi_version_row(const blocksstable::ObDatumRow *&row);
  int get_uncommitted_row(const blocksstable::ObDatumRow *&row);
  int switch_scan_state();
  int switch_to_committed_scan_state();
  virtual int init_next_value_iter();
  int get_next_row();
protected:
  // iterate row
  virtual int iterate_compacted_row(const common::ObStoreRowkey &key, blocksstable::ObDatumRow &row);
  virtual int iterate_uncommitted_row(const ObStoreRowkey &key, blocksstable::ObDatumRow &row);
  virtual int iterate_multi_version_row(const ObStoreRowkey &rowkey, blocksstable::ObDatumRow &row);
  int set_compacted_row_state(ObDatumRow &row, const bool add_shadow_row);
  void set_flag_and_version_for_compacted_row(const ObMvccTransNode *tnode, blocksstable::ObDatumRow &row);

  int iterate_compacted_row_value_(blocksstable::ObDatumRow &row);
  int iterate_uncommitted_row_value_(blocksstable::ObDatumRow &row);
  int iterate_multi_version_row_value_(blocksstable::ObDatumRow &row);
  int iterate_delete_insert_compacted_row_value_(blocksstable::ObDatumRow &row);

protected:
  struct ObOuputRowValidateChecker
  {
    ObOuputRowValidateChecker();
    ~ObOuputRowValidateChecker();
    void reset();
    int check_row_flag_valid(const blocksstable::ObDatumRow &row, const int64_t rowkey_cnt);

    bool output_last_row_flag_;
  };

  // means SCANITER
  static const uint64_t VALID_MAGIC_NUM = 0x524554494e414353;
  DISALLOW_COPY_AND_ASSIGN(ObMemtableMultiVersionScanIterator);
protected:
  const uint64_t MAGIC_;
  bool is_inited_;
  bool enable_delete_insert_;
  const storage::ObITableReadInfo *read_info_;
  ObMemtableKey *start_key_;
  ObMemtableKey *end_key_;
  storage::ObTableAccessContext *context_;
  ObMultiVersionRowIterator row_iter_;
  blocksstable::ObDatumRow row_;
  const ObMemtableKey *key_;
  ObMultiVersionValueIterator *value_iter_;
  bool key_first_row_;
  ObNopBitMap bitmap_;
  ScanState scan_state_;
  int32_t trans_version_col_idx_;
  int32_t sql_sequence_col_idx_;
  blocksstable::ObCompatRowReader row_reader_;
  ObOuputRowValidateChecker row_checker_;
};

}  // namespace memtable
}  // namespace oceanbase

#endif //OCEANBASE_MEMTABLE_OB_MEMTABLE_ITERATOR_
