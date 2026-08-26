/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_SPLIT_SSTABLE_HELPER_H
#define OCEANBASE_STORAGE_OB_TABLET_SPLIT_SSTABLE_HELPER_H

#include "lib/ob_define.h"
#include "share/ob_ddl_common.h"
#include "storage/ddl/ob_tablet_split_util.h"
#include "storage/blocksstable/ob_macro_seq_generator.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/column_store/ob_column_store_util.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/ob_datum_range.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/access/ob_table_read_info.h"

namespace oceanbase
{
namespace storage
{

// Forward declarations
class ObTabletSplitCtx;
class ObTabletSplitParam;
class ObIStoreRowIterator;
class ObSplitIndexBuilderCtx;
class ObTabletCreateSSTableParam;


class ObSplitIndexBuilderCtx final
{
public:
  ObSplitIndexBuilderCtx();
  ~ObSplitIndexBuilderCtx();
  void reset();
  int assign(const ObSplitIndexBuilderCtx &other);
  bool is_valid() const;
  TO_STRING_KV(KPC_(data_store_desc), KPC_(index_builder));
public:
  blocksstable::ObWholeDataStoreDesc *data_store_desc_;
  blocksstable::ObSSTableIndexBuilder *index_builder_;
};

class ObSSTSplitHelperInitParam
{
public:
  ObSSTSplitHelperInitParam();
  virtual ~ObSSTSplitHelperInitParam();
  virtual bool is_valid() const;
  VIRTUAL_TO_STRING_KV(KPC_(param), KPC_(context), K_(table_key),
    KPC_(sstable));
public:
  ObTabletSplitParam *param_;
  ObTabletSplitCtx *context_;
  ObITable::TableKey table_key_;
  blocksstable::ObSSTable *sstable_; // if table_key.table_type_ = mds, sstable_ = nullptr.
};

class ObColSSTSplitHelperInitParam : public ObSSTSplitHelperInitParam
{
public:
  ObColSSTSplitHelperInitParam();
  virtual ~ObColSSTSplitHelperInitParam();
  virtual bool is_valid() const override;
  INHERIT_TO_STRING_KV("ObSSTSplitHelperInitParam", ObSSTSplitHelperInitParam,
      K_(end_partkey_rowids));
public:
  ObArray<ObCSRowId> end_partkey_rowids_; // empty for co sstable, and non-empty for cgs.
};

class ObSSTableSplitHelper
{
public:
  ObSSTableSplitHelper();
  virtual ~ObSSTableSplitHelper();
  virtual int init(const ObSSTSplitHelperInitParam &init_param) = 0;
  virtual int split_data(
      ObIAllocator &allocator,
      const int64_t task_idx) = 0;
  virtual int generate_sstable() = 0;
  VIRTUAL_TO_STRING_KV(K_(is_inited), KPC_(param), KPC_(context));
protected:
  int prepare_index_builder_ctxs(
      ObIAllocator &allocator,
      const ObTabletSplitParam &param,
      const ObTabletSplitCtx &split_ctx,
      const ObSSTable &sstable,
      const ObStorageSchema &clipped_storage_schema,
      const ObStorageColumnGroupSchema *cg_schema,
      ObIArray<ObSplitIndexBuilderCtx> &index_builder_ctx_arr);
private:
  int get_merge_type(const ObSSTable &sstable, compaction::ObMergeType &merge_type);
protected:
  bool is_inited_;
  ObTabletSplitParam *param_;
  ObTabletSplitCtx *context_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableSplitHelper);
};

class ObSSTableSplitWriteHelper : public ObSSTableSplitHelper
{
public:
  ObSSTableSplitWriteHelper();
  virtual ~ObSSTableSplitWriteHelper();
  virtual int split_data(
      ObIAllocator &allocator,
      const int64_t task_idx) override;
  virtual int generate_sstable() override;
  OB_INLINE ObSSTable *get_sstable() const { return sstable_; }
  OB_INLINE const ObITableReadInfo *get_index_read_info() const { return index_read_info_; }
  INHERIT_TO_STRING_KV("ObSSTableSplitHelper", ObSSTableSplitHelper,
    KPC_(sstable), K_(default_row),
    KPC_(index_read_info), K_(index_builder_ctx_arr));
protected:
  int inner_init_common(
      const ObSSTSplitHelperInitParam &init_param);
  // prepare storage_schema, cg_schema, write_row.
  virtual int prepare_write_context(
      ObIAllocator &allocator,
      const int64_t task_idx,
      const ObStorageSchema *&clipped_storage_schema,
      ObIArray<ObDataStoreDesc *> &data_desc_arr,
      ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr);
  virtual int prepare_macro_seq_param(
      const int64_t task_idx,
      ObIArray<ObMacroSeqParam> &macro_seq_param_arr);
  virtual int build_create_sstable_param(
      const int64_t dest_tablet_index,
      ObTabletCreateSSTableParam &create_sstable_param);
  virtual int prepare_sstable_cg_infos(
      const ObStorageSchema &clipped_storage_schema,
      const ObSSTable &sstable,
      const ObStorageColumnGroupSchema *&cg_schema,
      uint16_t &table_cg_idx,
      ObIArray<ObColDesc> &multi_version_cols_desc) = 0;
private:
  int prepare_macro_block_writer(
      ObIAllocator &allocator,
      const int64_t task_idx,
      const ObIArray<ObMacroSeqParam> &macro_seq_param_arr,
      ObIArray<ObDataStoreDesc *> &data_desc_arr,
      ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr);
  int fill_tail_column_datums(
      const blocksstable::ObDatumRow &scan_row,
      blocksstable::ObDatumRow &write_row);
  int process_macro_blocks(
      ObIAllocator &allocator,
      const ObStorageSchema &clipped_storage_schema,
      const ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr);
  int process_rows(
      ObIAllocator &allocator,
      const ObStorageSchema &clipped_storage_schema,
      const ObIArray<ObMacroBlockWriter *> &macro_block_writer_arr,
      const ObDatumRange &query_range);
protected:
  common::ObArenaAllocator arena_allocator_; // multi-thread(rewrite task) unsafe.
  ObSSTable *sstable_; // row_store_sstale, or co, or cg.
  blocksstable::ObDatumRow default_row_;
  const ObITableReadInfo *index_read_info_;
  ObArray<ObSplitIndexBuilderCtx> index_builder_ctx_arr_;
  // ObSplitComparator *split_comparator_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableSplitWriteHelper);
};

// row-store sstable
class ObRowSSTableSplitWriteHelper : public ObSSTableSplitWriteHelper
{
public:
  ObRowSSTableSplitWriteHelper();
  virtual ~ObRowSSTableSplitWriteHelper();
  virtual int init(const ObSSTSplitHelperInitParam &init_param) override;
  OB_INLINE const ObIArray<ObDatumRowkey> &get_end_partkeys() const { return end_partkeys_; }
  INHERIT_TO_STRING_KV("ObSSTableSplitWriteHelper", ObSSTableSplitWriteHelper,
    K_(end_partkeys));
protected:
  virtual int prepare_sstable_cg_infos(
      const ObStorageSchema &clipped_storage_schema,
      const ObSSTable &sstable,
      const ObStorageColumnGroupSchema *&cg_schema,
      uint16_t &table_cg_idx,
      ObIArray<ObColDesc> &multi_version_cols_desc) override;
private:
  int prepare_split_partkeys(
      const ObSSTSplitHelperInitParam &init_param); // called by init.
private:
  ObArray<ObDatumRowkey> end_partkeys_;
  DISALLOW_COPY_AND_ASSIGN(ObRowSSTableSplitWriteHelper);
};

// column store cg, all-co, rowkey-co.
class ObColSSTableSplitWriteHelper : public ObSSTableSplitWriteHelper
{
public:
  ObColSSTableSplitWriteHelper();
  virtual ~ObColSSTableSplitWriteHelper();
  virtual int init(const ObSSTSplitHelperInitParam &init_param) override;
  OB_INLINE const ObIArray<ObCSRowId> &get_end_partkey_rowids() const { return end_partkey_rowids_; }
  INHERIT_TO_STRING_KV("ObSSTableSplitWriteHelper", ObSSTableSplitWriteHelper,
    K_(mocked_row_store_cg), K_(end_partkey_rowids));
protected:
  virtual int prepare_sstable_cg_infos(
      const ObStorageSchema &clipped_storage_schema,
      const ObSSTable &sstable,
      const ObStorageColumnGroupSchema *&cg_schema,
      uint16_t &table_cg_idx,
      ObIArray<ObColDesc> &multi_version_cols_desc) override;
private:
  int prepare_index_read_info(const ObSSTSplitHelperInitParam &init_param);
  int prepare_split_rowids(const ObSSTSplitHelperInitParam &init_param);
private:
  ObStorageColumnGroupSchema mocked_row_store_cg_; // to hold buf.
  ObArray<ObCSRowId> end_partkey_rowids_;
  DISALLOW_COPY_AND_ASSIGN(ObColSSTableSplitWriteHelper);
};

class ObSpecialSplitWriteHelper : public ObSSTableSplitHelper
{
public:
  ObSpecialSplitWriteHelper() {}
  virtual ~ObSpecialSplitWriteHelper() = default;
  virtual int init(const ObSSTSplitHelperInitParam &init_param) override;
  virtual int split_data(
      ObIAllocator &allocator,
      const int64_t task_idx) override;
  virtual int generate_sstable() override;
private:
  int create_empty_minor_sstable();
  int create_mds_sstable();
  DISALLOW_COPY_AND_ASSIGN(ObSpecialSplitWriteHelper);
};


}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_TABLET_SPLIT_SSTABLE_HELPER_H
