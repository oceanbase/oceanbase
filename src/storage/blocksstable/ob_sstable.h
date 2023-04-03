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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SSTABLE_H
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SSTABLE_H

#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/blocksstable/ob_macro_block_meta.h"
#include "share/scn.h"
#include "ob_datum_range.h"
#include "storage/ddl/ob_ddl_struct.h"

namespace oceanbase
{
namespace common
{
}
namespace storage
{
class ObAllMicroBlockRangeIterator;
struct ObTabletCreateSSTableParam;
class ObStoreRowIterator;
}
namespace blocksstable
{
class ObSSTableSecMetaIterator;
class ObIMacroBlockIterator;
struct ObMacroBlocksWriteCtx;

// SSTable class after version 4.0
class ObSSTable : public ObITable
{
public:
  ObSSTable();
  virtual ~ObSSTable();

  int init(const ObTabletCreateSSTableParam &param, common::ObIAllocator *allocator);
  void reset();

  // Query interfaces
  virtual int scan(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const ObDatumRange &key_range,
      ObStoreRowIterator *&row_iter) override;
  virtual int get(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const ObDatumRowkey &rowkey,
      ObStoreRowIterator *&row_iter) override;
  virtual int multi_scan(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const common::ObIArray<ObDatumRange> &ranges,
      ObStoreRowIterator *&row_iter) override;
  virtual int multi_get(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const common::ObIArray<ObDatumRowkey> &rowkeys,
      ObStoreRowIterator *&row_iter) override;
  virtual int exist(
      ObStoreCtx &ctx,
      const uint64_t table_id,
      const storage::ObTableReadInfo &full_read_info,
      const ObDatumRowkey &rowkey,
      bool &is_exist,
      bool &has_found) override;
  virtual int exist(
      ObRowsInfo &rows_info,
      bool &is_exist,
      bool &all_rows_found) override;

  int scan_macro_block(
      const ObDatumRange &range,
      const ObTableReadInfo &index_read_info,
      ObIAllocator &allocator,
      blocksstable::ObIMacroBlockIterator *&macro_block_iter,
      const bool is_reverse_scan = false,
      const bool need_record_micro_info = false,
      const bool need_scan_sec_meta = false);
  int scan_micro_block(
      const ObDatumRange &range,
      const ObTableReadInfo &index_read_info,
      ObIAllocator &allocator,
      ObAllMicroBlockRangeIterator *&micro_iter,
      const bool is_reverse_scan = false);
  int scan_secondary_meta(
      ObIAllocator &allocator,
      const ObDatumRange &query_range,
      const ObTableReadInfo &index_read_info,
      const blocksstable::ObMacroBlockMetaType meta_type,
      blocksstable::ObSSTableSecMetaIterator *&meta_iter,
      const bool is_reverse_scan = false,
      const int64_t sample_step = 0) const;
  int bf_may_contain_rowkey(const ObDatumRowkey &rowkey, bool &contain);

  // For transaction
  int check_row_locked(
      ObStoreCtx &ctx,
      const storage::ObTableReadInfo &full_read_info,
      const ObDatumRowkey &rowkey,
      ObStoreRowLockState &lock_state);
  int set_upper_trans_version(const int64_t upper_trans_version);
  virtual int64_t get_upper_trans_version() const override
  {
    return meta_.basic_meta_.upper_trans_version_;
  }
  virtual int64_t get_max_merged_trans_version() const override
  {
    return meta_.basic_meta_.max_merged_trans_version_;
  }
  int64_t get_recycle_version() const
  {
    return meta_.basic_meta_.recycle_version_;
  }
  int16_t get_sstable_seq() const
  {
    return meta_.basic_meta_.sstable_logic_seq_;
  }
  share::SCN get_filled_tx_scn() const
  {
    return meta_.basic_meta_.filled_tx_scn_;
  }
  int64_t get_data_version() const
  {
    return is_major_sstable() ? get_snapshot_version() :
      is_ddl_sstable() ? get_upper_trans_version() :
      get_key().get_end_scn().get_val_for_tx();
  }
  virtual int get_frozen_schema_version(int64_t &schema_version) const override;
  int add_disk_ref();
  int dec_disk_ref();
  int pre_transform_root_block(const ObTableReadInfo &index_read_info);
  int set_status_for_read(const ObSSTableStatus status);
  OB_INLINE int64_t get_macro_offset() const { return meta_.get_macro_info().get_nested_offset(); }
  OB_INLINE bool is_valid() const { return valid_for_reading_; }
  OB_INLINE const blocksstable::ObSSTableMeta &get_meta() const { return meta_; }
  OB_INLINE int64_t get_macro_read_size() const { return meta_.get_macro_info().get_nested_size(); }
  OB_INLINE bool is_small_sstable() const {
      return OB_DEFAULT_MACRO_BLOCK_SIZE > meta_.get_macro_info().get_nested_size()
             && 0 < meta_.get_macro_info().get_nested_size(); }
  OB_INLINE int get_index_tree_root(
      const ObTableReadInfo &index_read_info,
      blocksstable::ObMicroBlockData &index_data,
      const bool need_transform = true)
  {
    return meta_.get_index_tree_root(index_read_info, index_data, need_transform);
  }
  int get_last_rowkey(
      const ObTableReadInfo &index_read_info,
      common::ObIAllocator &allocator,
      ObDatumRowkey &endkey);
  //TODO do not use this func except @hanhui
  int get_last_rowkey(
      const ObTableReadInfo &index_read_info,
      common::ObIAllocator &allocator,
      common::ObStoreRowkey &endkey);
  bool is_empty() const override
  {
    return meta_.is_empty();
  }

public:
  int dump2text(
      const char *dir_name,
      const ObStorageSchema &schema,
      const char *fname)
  {
    // TODO: print sstable
    UNUSEDx(dir_name, schema, fname);
    return OB_NOT_SUPPORTED;
  }
  virtual int64_t get_serialize_size() const override;
  virtual int serialize(char *buf, const int64_t buf_len, int64_t &pos) const override;
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int deserialize_post_work();

  INHERIT_TO_STRING_KV("ObITable", ObITable, KP(this), K_(meta), K_(valid_for_reading));

private:
  int check_valid_for_reading();
  int add_macro_ref();
  int add_used_size();
  int dec_used_size();
  int build_exist_iterator(
      const ObTableIterParam &iter_param,
      const ObDatumRowkey &rowkey,
      ObTableAccessContext &access_context,
      ObStoreRowIterator *&iter);
  int build_multi_exist_iterator(ObRowsInfo &rows_info, ObStoreRowIterator *&iter);
  void dec_macro_ref();
  int get_last_rowkey(const ObTableReadInfo &index_read_info, const ObDatumRowkey *&sstable_endkey);

protected:
  blocksstable::ObSSTableMeta meta_;
  bool valid_for_reading_;
  bool hold_macro_ref_;
  common::ObIAllocator *allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObSSTable);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SSTABLE_H
