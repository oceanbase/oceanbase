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

#ifdef OB_BUILD_SHARED_STORAGE
class ObSSDataSplitHelper;
#endif

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
  OB_INLINE const ObTabletSplitParam *get_split_param() const { return param_; }
  OB_INLINE const ObTabletSplitCtx *get_split_context() const { return context_; }
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
protected:
  bool is_inited_;
  ObTabletSplitParam *param_;
  ObTabletSplitCtx *context_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableSplitHelper);
};

class ObSSTableSplitWriteHelper : public ObSSTableSplitHelper
{
  friend class ObSSSplitWriteHelperBase;
public:
  ObSSTableSplitWriteHelper();
  virtual ~ObSSTableSplitWriteHelper();
  virtual int split_data(
      ObIAllocator &allocator,
      const int64_t task_idx) override;
  virtual int generate_sstable() override;
  OB_INLINE ObSSTable *get_sstable() const { return sstable_; }
  OB_INLINE const ObIArray<MacroBlockId> &get_split_point_macros() const { return split_point_macros_; }
  OB_INLINE const ObITableReadInfo *get_index_read_info() const { return index_read_info_; }
  OB_INLINE const ObIArray<ObSplitIndexBuilderCtx> &get_index_builder_ctx_arr() const { return index_builder_ctx_arr_; }
  INHERIT_TO_STRING_KV("ObSSTableSplitHelper", ObSSTableSplitHelper,
    KPC_(sstable), K_(default_row), K_(split_point_macros),
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
  ObArray<MacroBlockId> split_point_macros_;
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

#ifdef OB_BUILD_SHARED_STORAGE
class ObSSSplitWriteHelperCommon final
{
public:
  ObSSSplitWriteHelperCommon() = default;
  ~ObSSSplitWriteHelperCommon() = default;
public:
  int prepare_macro_seq_param_impl(
      const int64_t task_idx,
      ObSSTableSplitWriteHelper &helper,
      ObIArray<ObMacroSeqParam> &macro_seq_param_arr);
  int build_create_sstable_param_impl(
      const int64_t dest_tablet_index,
      ObSSTableSplitWriteHelper &helper,
      ObTabletCreateSSTableParam &create_sstable_param);
};

class ObSSRowSSTableSplitWriteHelper : public ObRowSSTableSplitWriteHelper
{
public:
  ObSSRowSSTableSplitWriteHelper() : ObRowSSTableSplitWriteHelper() {}
  virtual ~ObSSRowSSTableSplitWriteHelper() = default;
protected:
  virtual int prepare_macro_seq_param(
    const int64_t task_idx,
    ObIArray<ObMacroSeqParam> &macro_seq_param_arr) override;
  virtual int build_create_sstable_param(
    const int64_t dest_tablet_index,
    ObTabletCreateSSTableParam &create_sstable_param) override;
private:
  ObSSSplitWriteHelperCommon ss_common_helper_;
  DISALLOW_COPY_AND_ASSIGN(ObSSRowSSTableSplitWriteHelper);
};

class ObSSColSSTableSplitWriteHelper : public ObColSSTableSplitWriteHelper
{
public:
  ObSSColSSTableSplitWriteHelper() : ObColSSTableSplitWriteHelper() {}
  virtual ~ObSSColSSTableSplitWriteHelper() = default;
protected:
  virtual int prepare_macro_seq_param(
    const int64_t task_idx,
    ObIArray<ObMacroSeqParam> &macro_seq_param_arr) override;
  virtual int build_create_sstable_param(
    const int64_t dest_tablet_index,
    ObTabletCreateSSTableParam &create_sstable_param) override;
private:
  ObSSSplitWriteHelperCommon ss_common_helper_;
  DISALLOW_COPY_AND_ASSIGN(ObSSColSSTableSplitWriteHelper);
};
#endif

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_TABLET_SPLIT_SSTABLE_HELPER_H
