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

#ifndef OCEANBASE_STORAGE_OB_TABLET_CREATE_SSTABLE_PARAM
#define OCEANBASE_STORAGE_OB_TABLET_CREATE_SSTABLE_PARAM

#include "lib/utility/ob_print_utils.h"
#include "storage/ob_i_table.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "share/scn.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/blocksstable/ob_table_flag.h"
#include "storage/compaction/ob_compaction_util.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObSSTableMergeRes;
struct ObBlockInfo;
struct ObSSTableBasicMeta;
class ObSSTableMacroInfo;
class ObSSTableMeta;
class ObMigrationSSTableParam;
class ObSSTable;
class ObSSTableIndexBuilder;
}
namespace compaction
{
struct ObBasicTabletMergeCtx;
}
namespace storage
{
struct ObTabletDDLParam;

class ObBlockMetaTree;

struct ObTabletCreateSSTableParam final
{
public:
  ObTabletCreateSSTableParam();
  ~ObTabletCreateSSTableParam() = default;
  ObTabletCreateSSTableParam(const ObTabletCreateSSTableParam &other) = delete;
  ObTabletCreateSSTableParam &operator=(ObTabletCreateSSTableParam &other) = delete;
public:
  bool is_valid() const;
  bool is_block_meta_valid(const ObMetaDiskAddr &addr,
                           const blocksstable::ObMicroBlockData &data) const;

  // Without checking the validity of the input parameters, necessary to ensure the correctness of the method call.
  int init_for_empty_major_sstable(const ObTabletID &tablet_id,
                                   const ObStorageSchema &storage_schema,
                                   const int64_t snapshot_version,
                                   const int64_t column_group_idx,
                                   const bool has_all_column_group,
                                   const bool is_shared);

  // Without checking the validity of the input parameters, necessary to ensure the correctness of the method call.
  int init_for_small_sstable(const blocksstable::ObSSTableMergeRes &res,
                             const ObITable::TableKey &table_key,
                             const blocksstable::ObSSTableMeta &sstable_meta,
                             const blocksstable::ObBlockInfo &block_info);

  // Without checking the validity of the input parameters, necessary to ensure the correctness of the method call.
  int init_for_merge(const compaction::ObBasicTabletMergeCtx &ctx,
                     const blocksstable::ObSSTableMergeRes &res,
                     const ObStorageColumnGroupSchema *cg_schema,
                     const int64_t column_group_idx);

  // Without checking the validity of the input parameters, necessary to ensure the correctness of the method call.
  int init_for_ddl(blocksstable::ObSSTableIndexBuilder *sstable_index_builder,
                   const ObTabletDDLParam &ddl_param,
                   const blocksstable::ObSSTable *first_ddl_sstable,
                   const ObStorageSchema &storage_schema,
                   const int64_t macro_block_column_count,
                   const int64_t create_schema_version_on_tablet,
                   const ObIArray<blocksstable::MacroBlockId> &macro_id_array);

  // Without checking the validity of the input parameters, necessary to ensure the correctness of the method call.
  int init_for_ddl_mem(const ObITable::TableKey &table_key,
                       const share::SCN &ddl_start_scn,
                       const ObStorageSchema &storage_schema,
                       ObBlockMetaTree &block_meta_tree);

  // Without checking the validity of the input parameters, necessary to ensure the correctness of the method call.
  int init_for_ss_ddl(blocksstable::ObSSTableMergeRes &res,
                      const ObITable::TableKey &table_key,
                      const ObStorageSchema &storage_schema,
                      const int64_t create_schema_version_on_tablet);

  // Without checking the validity of the input parameters, necessary to ensure the correctness of the method call.
  int init_for_split(const ObTabletID &dst_tablet_id,
                     const ObITable::TableKey &src_table_key,
                     const blocksstable::ObSSTableBasicMeta &basic_meta,
                     const int64_t schema_version,
                     const ObIArray<blocksstable::MacroBlockId> &split_point_macros_id,
                     const blocksstable::ObSSTableMergeRes &res);

  // Without checking the validity of the input parameters, necessary to ensure the correctness of the method call.
  int init_for_split_empty_minor_sstable(const ObTabletID &tablet_id,
                                         const share::SCN &start_scn,
                                         const share::SCN &end_scn,
                                         const blocksstable::ObSSTableBasicMeta &basic_meta);

  // Without checking the validity of the input parameters, necessary to ensure the correctness of the method call.
  int init_for_lob_split(const ObTabletID &new_tablet_id,
                         const ObITable::TableKey &table_key,
                         const blocksstable::ObSSTableBasicMeta &basic_meta,
                         const compaction::ObMergeType &merge_type,
                         const int64_t schema_version,
                         const int64_t dst_major_snapshot_version,
                         const int64_t uncommitted_tx_id,
                         const int64_t sstable_logic_seq,
                         const blocksstable::ObSSTableMergeRes &res);

  // Without checking the validity of the input parameters, necessary to ensure the correctness of the method call.
  int init_for_ha(const blocksstable::ObMigrationSSTableParam &migration_param,
                  const blocksstable::ObSSTableMergeRes &res);

  // Without checking the validity of the input parameters, necessary to ensure the correctness of the method call.
  int init_for_ha(
      const blocksstable::ObMigrationSSTableParam &migration_param,
      const common::ObIArray<blocksstable::MacroBlockId> &data_block_ids,
      const common::ObIArray<blocksstable::MacroBlockId> &other_block_ids);

  // Without checking the validity of the input parameters, necessary to ensure the correctness of the method call.
  int init_for_transfer_empty_mini_minor_sstable(const common::ObTabletID &tablet_id,
                                                 const share::SCN &start_scn,
                                                 const share::SCN &end_scn,
                                                 const ObStorageSchema &table_schema,
                                                 const ObITable::TableType &table_type);

  // Without checking the validity of the input parameters, necessary to ensure the correctness of the method call.
  int init_for_remote(const blocksstable::ObMigrationSSTableParam &migration_param);

  int init_for_mds(const compaction::ObBasicTabletMergeCtx &ctx,
                   const blocksstable::ObSSTableMergeRes &res,
                   const ObStorageSchema &mds_schema);

  inline const ObITable::TableKey& table_key() const { return table_key_; }
  inline const share::SCN& rec_scn() const { return rec_scn_; };
  inline bool is_ready_for_read() const { return is_ready_for_read_; }
  inline int64_t data_blocks_cnt() const { return data_blocks_cnt_; }
  inline share::SCN filled_tx_scn() const { return filled_tx_scn_; }
  inline int32_t co_base_type() const { return co_base_type_; }
  inline bool is_co_table_without_cgs() const { return is_co_table_without_cgs_; }
  inline int64_t column_group_cnt() const { return column_group_cnt_; }
  inline int64_t full_column_cnt() const { return full_column_cnt_; }
  inline int64_t co_base_snapshot_version() const { return co_base_snapshot_version_; }

  // TODO: delete this interface
  // ObTabletMergeInfo::record_start_tx_scn_for_tx_data
  inline void set_filled_tx_scn(const share::SCN &scn) { filled_tx_scn_ = scn; }

  TO_STRING_KV(K_(table_key),
      K_(sstable_logic_seq),
      K_(schema_version),
      K_(create_snapshot_version),
      K_(progressive_merge_round),
      K_(progressive_merge_step),
      K_(is_ready_for_read),
      K_(table_mode),
      K_(index_type),
      K_(root_block_addr),
      K_(root_block_data),
      K_(root_row_store_type),
      K_(latest_row_store_type),
      K_(data_index_tree_height),
      K_(data_block_macro_meta_addr),
      K_(data_block_macro_meta),
      K_(index_blocks_cnt),
      K_(data_blocks_cnt),
      K_(micro_block_cnt),
      K_(use_old_macro_block_count),
      K_(row_count),
      K_(column_group_cnt),
      K_(co_base_type),
      K_(rowkey_column_cnt),
      K_(column_cnt),
      K_(full_column_cnt),
      K_(column_checksums),
      K_(data_checksum),
      K_(occupy_size),
      K_(original_size),
      K_(max_merged_trans_version),
      K_(ddl_scn),
      K_(filled_tx_scn),
      K_(tx_data_recycle_scn),
      K_(is_co_table_without_cgs),
      K_(contain_uncommitted_row),
      K_(is_meta_root),
      K_(compressor_type),
      K_(encrypt_id),
      K_(master_key_id),
      K_(recycle_version),
      K_(root_macro_seq),
      K_(nested_offset),
      K_(nested_size),
      KPHEX_(encrypt_key, sizeof(encrypt_key_)),
      K_(table_backup_flag),
      K_(table_shared_flag),
      K_(uncommitted_tx_id),
      K_(co_base_snapshot_version),
      K_(rec_scn));
private:
  static const int64_t DEFAULT_MACRO_BLOCK_CNT = 64;
  int inner_init_with_merge_res(const blocksstable::ObSSTableMergeRes &res);
  int inner_init_with_shared_sstable(
      const blocksstable::ObMigrationSSTableParam &migration_param,
      const common::ObIArray<blocksstable::MacroBlockId> &data_block_ids,
      const common::ObIArray<blocksstable::MacroBlockId> &other_block_ids);
  void set_init_value_for_column_store_();
private:
  friend class blocksstable::ObSSTableMeta;
  friend class blocksstable::ObSSTableMacroInfo;
private:
  ObITable::TableKey table_key_;
  int16_t sstable_logic_seq_;
  int64_t schema_version_;
  int64_t create_snapshot_version_;
  int64_t progressive_merge_round_;
  int64_t progressive_merge_step_;
  bool is_ready_for_read_;
  share::schema::ObTableMode table_mode_;
  share::schema::ObIndexType index_type_;
  ObMetaDiskAddr root_block_addr_;
  blocksstable::ObMicroBlockData root_block_data_;
  common::ObRowStoreType root_row_store_type_;
  common::ObRowStoreType latest_row_store_type_;
  int16_t data_index_tree_height_;
  ObMetaDiskAddr data_block_macro_meta_addr_;
  blocksstable::ObMicroBlockData data_block_macro_meta_;
  int64_t index_blocks_cnt_;
  int64_t data_blocks_cnt_;
  int64_t micro_block_cnt_;
  int64_t use_old_macro_block_count_;
  int64_t row_count_;
  int64_t column_group_cnt_; // only used for column_store
  int32_t co_base_type_; // used for co sstable
  int64_t rowkey_column_cnt_;
  int64_t column_cnt_;
  int64_t full_column_cnt_;
  common::ObSEArray<int64_t, common::OB_ROW_DEFAULT_COLUMNS_COUNT> column_checksums_;
  int64_t data_checksum_;
  int64_t occupy_size_;
  int64_t original_size_;
  int64_t max_merged_trans_version_;
  share::SCN ddl_scn_; // saved into sstable meta
  share::SCN filled_tx_scn_;
  share::SCN tx_data_recycle_scn_;
  bool is_co_table_without_cgs_; // only used for creating co sstable without cg sstables
  bool contain_uncommitted_row_;
  bool is_meta_root_;
  common::ObCompressorType compressor_type_;
  int64_t encrypt_id_;
  int64_t master_key_id_;
  int64_t recycle_version_;
  int64_t nested_offset_;
  int64_t nested_size_;
  int64_t root_macro_seq_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  common::ObSEArray<blocksstable::MacroBlockId, DEFAULT_MACRO_BLOCK_CNT> data_block_ids_;
  common::ObSEArray<blocksstable::MacroBlockId, DEFAULT_MACRO_BLOCK_CNT> other_block_ids_;
  storage::ObTableBackupFlag table_backup_flag_; //ObTableBackupFlag will be updated by ObSSTableMergeRes
  storage::ObTableSharedFlag table_shared_flag_; //ObTableSharedFlag will be updated by ObTabletCreateSSTableParam
  int64_t uncommitted_tx_id_;
  int64_t co_base_snapshot_version_;
  share::SCN rec_scn_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_CREATE_SSTABLE_PARAM
