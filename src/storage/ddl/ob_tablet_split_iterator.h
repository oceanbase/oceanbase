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

#ifndef OB_TABLET_SPLIT_ITERATOR_H_
#define OB_TABLET_SPLIT_ITERATOR_H_

#include "storage/ob_i_store.h"
#include "storage/access/ob_multiple_scan_merge.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_sstable_row_whole_scanner.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"
#include "storage/compaction/ob_index_block_micro_iterator.h"
#include "storage/ddl/ob_tablet_split_util.h"

namespace oceanbase
{
using namespace blocksstable;

namespace storage
{

struct ObSplitScanParam final
{
public:
  ObSplitScanParam(
    const int64_t table_id,
    ObTablet &src_tablet,
    const ObDatumRange &query_range,
    const ObStorageSchema &storage_schema,
    const int64_t data_format_version) :
    table_id_(table_id), src_tablet_(src_tablet), query_range_(&query_range),
    storage_schema_(&storage_schema), data_format_version_(data_format_version)
  { }
  ~ObSplitScanParam() = default;
  bool is_valid() const {
    return table_id_ > 0 && src_tablet_.is_valid() && nullptr != query_range_
        && (nullptr != storage_schema_ && storage_schema_->is_valid());
  }
  TO_STRING_KV(K_(table_id), K_(src_tablet), KPC_(query_range), KPC_(storage_schema), K_(data_format_version));
public:
  int64_t table_id_;
  ObTablet &src_tablet_; // split source tablet.
  const ObDatumRange *query_range_; // whole_range for sstable scan.
  const ObStorageSchema *storage_schema_;
  int64_t data_format_version_;
};

class ObSplitIterator : public ObIStoreRowIterator
{
public:
  ObSplitIterator();
  virtual ~ObSplitIterator();
  VIRTUAL_TO_STRING_KV(K_(is_inited),
    K_(access_ctx), KPC_(table_read_info), K_(access_param));
protected:
  virtual int construct_access_param(
      const ObSplitScanParam &param,
      const ObSSTable &sstable);
  virtual int construct_access_ctx(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id);
private:
  int build_rowkey_read_info(
      const ObSplitScanParam &param,
      const ObSSTable &sstable);
  int build_normal_cg_read_info(
      const ObSplitScanParam &param,
      const ObSSTable &sstable);
protected:
  common::ObArenaAllocator allocator_;
  bool is_inited_;
  ObStoreCtx ctx_;
  ObTableAccessContext access_ctx_;
  ObITableReadInfo *table_read_info_; // rowkey_read_info for row_store/all/rowkey cg, cg_read_info for normal cg.
  ObTableAccessParam access_param_;
};

class ObRowScan : public ObSplitIterator
{
public:
  ObRowScan();
  virtual ~ObRowScan();
  // to scan the sstable with the specified query_range.
  int init(
      const ObSplitScanParam &param,
      ObSSTable &sstable);

  // to scan the specified whole macro block.
  int init(
      const ObSplitScanParam &param,
      const blocksstable::ObMacroBlockDesc &macro_desc,
      ObSSTable &sstable);

  virtual int get_next_row(const blocksstable::ObDatumRow *&tmp_row) override;

  const ObITableReadInfo *get_rowkey_read_info() const { return table_read_info_; }
  storage::ObTxTableGuards &get_tx_table_guards() { return ctx_.mvcc_acc_ctx_.get_tx_table_guards(); }

  INHERIT_TO_STRING_KV("ObSplitIterator", ObSplitIterator, KPC(row_iter_));
private:
  ObSSTableRowWholeScanner *row_iter_;
};

class ObSnapshotRowScan final : public ObIStoreRowIterator
{
public:
  ObSnapshotRowScan();
  virtual ~ObSnapshotRowScan();
  void reset();
  int init(
      const ObSplitScanParam &param,
      const ObIArray<share::schema::ObColDesc> &schema_store_col_descs,
      const int64_t schema_column_cnt,
      const int64_t schema_rowkey_cnt,
      const bool is_oracle_mode,
      const ObTabletHandle &tablet_handle,
      const int64_t snapshot_version);
  int construct_access_param(
      const uint64_t table_id,
      const common::ObTabletID &tablet_id,
      const ObITableReadInfo &read_info);
  int construct_range_ctx(ObQueryFlag &query_flag, const share::ObLSID &ls_id);
  int construct_multiple_scan_merge(const ObTabletTableIterator &table_iter, const ObDatumRange &range);
  int add_extra_rowkey(const ObDatumRow &row);
  virtual int get_next_row(const blocksstable::ObDatumRow *&tmp_row) override;
  TO_STRING_KV(K_(is_inited), K_(snapshot_version), K_(schema_rowkey_cnt),
      K_(range), K_(read_info), K_(out_cols_projector),
      K_(access_param), K_(access_ctx));
private:
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  int64_t snapshot_version_;
  int64_t schema_rowkey_cnt_;
  ObDatumRange range_;
  ObTableReadInfo read_info_;
  ObDatumRow write_row_;
  ObArray<int32_t> out_cols_projector_;
  ObTableAccessParam access_param_;
  ObStoreCtx ctx_;
  ObTableAccessContext access_ctx_;
  ObGetTableParam get_table_param_;
  ObMultipleScanMerge *scan_merge_;
};

class ObUncommittedRowScan : public ObIStoreRowIterator
{
public:
  ObUncommittedRowScan();
  virtual ~ObUncommittedRowScan();
  int init(
      const ObSplitScanParam param,
      ObSSTable &src_sstable,
      const int64_t major_snapshot_version,
      const int64_t schema_column_cnt);
  virtual int get_next_row(const blocksstable::ObDatumRow *&tmp_row) override;
private:
  int get_next_rowkey_rows();
  int row_queue_add(const ObDatumRow &row);
  void row_queue_reuse();
  int check_can_skip(const blocksstable::ObDatumRow &row, bool &can_skip);
private:
  ObRowScan row_scan_;
  bool row_scan_end_;
  const ObDatumRow *next_row_;
  int64_t major_snapshot_version_;
  int64_t trans_version_col_idx_;
  blocksstable::ObRowQueue row_queue_;
  ObArenaAllocator row_queue_allocator_;
  bool row_queue_has_unskippable_row_;
};

class ObSSTableSplitWriteHelper;
class ObRowSSTableSplitWriteHelper;
class ObColSSTableSplitWriteHelper;
class ObSplitReuseBlockIter final : public ObSplitIterator
{
public:
  ObSplitReuseBlockIter();
  virtual ~ObSplitReuseBlockIter();
  int init(
      ObIAllocator &allocator,
      const ObSplitScanParam &scan_param,
      const ObSSTableSplitWriteHelper *split_helper);
  int get_next_macro_block(
      ObMacroBlockDesc &block_desc,
      const ObMicroBlockData *&clustered_micro_block_data,
      int64_t &dest_tablet_index,
      bool &can_reuse);
  int get_next_micro_block(
      const ObMicroBlock *&micro_block,
      int64_t &dest_tablet_index,
      bool &can_reuse);
  int get_next_row(
      const ObDatumRow *&datum_row,
      int64_t &dest_tablet_index);
  virtual int get_next_row(const blocksstable::ObDatumRow *&tmp_row) override { return OB_NOT_SUPPORTED; }
  INHERIT_TO_STRING_KV("ObSplitIterator", ObSplitIterator,
    K_(cur_cmp_tablet_index), K_(is_macro_block_opened), K_(is_micro_block_opened),
    K_(cur_rowid_in_sstable), KPC_(scan_param), KPC_(split_helper));
private:
  int inner_open_cur_macro_block();
  int inner_open_cur_micro_block();
  int inner_init_micro_row_scanner(
      const ObSplitScanParam &param,
      const ObSSTable &sstable);
  int check_can_reuse_macro_block(
      int64_t &dest_tablet_index,
      bool &can_reuse) const;
  int check_can_reuse_micro_block(
      int64_t &dest_tablet_index,
      bool &can_reuse) const;
  int compare(
      const ObDatumRowkey &rs_rowkey/*row_store_rowkey*/,
      int &cmp_ret) const;
private:
  int64_t cur_cmp_tablet_index_;
  bool is_macro_block_opened_;
  bool is_micro_block_opened_;
  ObCSRowId cur_rowid_in_sstable_;
  // ObStorageDatum cur_rowid_datum_; // rowid in sstable.
  const ObSplitScanParam *scan_param_;
  const ObSSTableSplitWriteHelper *split_helper_;
  ObDualMacroMetaIterator macro_meta_iter_;
  ObMacroBlockDesc *cur_macro_block_desc_;
  compaction::ObIndexBlockMicroIterator micro_block_iter_;
  const ObMicroBlock *cur_micro_block_;
  ObIMicroBlockRowScanner *micro_row_scanner_;
  ObMacroBlockReader macro_reader_;
DISALLOW_COPY_AND_ASSIGN(ObSplitReuseBlockIter);
};

} //storage
} //oceanbase


#endif /* OB_TABLET_SPLIT_ITERATOR_H_ */
