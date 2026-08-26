/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_DDL_OB_TABLET_SPLIT_UTIL_H
#define OCEANBASE_STORAGE_DDL_OB_TABLET_SPLIT_UTIL_H

#include "share/ob_ddl_common.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/tablet/ob_tablet_mds_table_mini_merger.h"


namespace oceanbase
{
namespace storage
{
class ObTabletHandle;
class ObLSHandle;
}
namespace storage
{
template <typename T>
void destroy_split_object(common::ObIAllocator &alloc, T *&obj)
{
  if (nullptr != obj) {
    obj->~T();
    alloc.free(obj);
    obj = nullptr;
  }
}

template <typename T>
void destroy_split_array(common::ObIAllocator &alloc, ObIArray<T *> &arr)
{
  for (int64_t i = 0; i < arr.count(); i++) {
    destroy_split_object(alloc, arr.at(i));
  }
  arr.reset();
}

struct ObTabletSplitRegisterMdsArg final
{
public:
  ObTabletSplitRegisterMdsArg()
    : is_no_logging_(false), tenant_id_(OB_INVALID_TENANT_ID), src_local_index_tablet_count_(0),
      ls_id_(), task_type_(), lob_schema_versions_(), split_info_array_()
    {}
  virtual ~ObTabletSplitRegisterMdsArg() = default;
  virtual bool is_valid() const;
  virtual int assign(const ObTabletSplitRegisterMdsArg &other);
  TO_STRING_KV(K_(parallelism), K_(is_no_logging), K_(tenant_id), K_(src_local_index_tablet_count),
      K_(ls_id), K_(task_type), K_(lob_schema_versions), K_(split_info_array), K_(table_schema));
public:
  int64_t parallelism_;
  bool is_no_logging_;
  uint64_t tenant_id_;
  int64_t src_local_index_tablet_count_;
  share::ObLSID ls_id_;
  share::ObDDLType task_type_;
  ObSArray<uint64_t> lob_schema_versions_;
  common::ObSArray<ObTabletSplitArg> split_info_array_;
  const ObTableSchema *table_schema_;
};

struct ObTabletSplitUtil final
{
public:
  // TODO, to move it to ObSpecialSplitWriteHelper
  static int check_need_fill_empty_sstable(
      ObLSHandle &ls_handle,
      const bool is_minor_sstable,
      const ObITable::TableKey &table_key,
      const ObTabletID &dst_tablet_id,
      bool &need_fill_empty_sstable,
      SCN &end_scn);
  static int get_clipped_storage_schema_on_demand(
      ObIAllocator &allocator,
      const ObTabletID &src_tablet_id,
      const ObSSTable &src_sstable,
      const ObStorageSchema &split_mds_storage_schema,
      const ObStorageSchema *&storage_schema);
  static int check_split_minors_can_be_accepted(
      const ObSSTableArray &old_store_minors,
      const ObIArray<ObITable *> &tables_array,
      bool &is_update_firstly);
  static int get_tablet(
      common::ObArenaAllocator &allocator,
      const storage::ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      const bool is_shared_mode,
      storage::ObTabletHandle &tablet_handle,
      const storage::ObMDSGetTabletMode mode = storage::ObMDSGetTabletMode::READ_ALL_COMMITED);
  static int get_participants(
      const share::ObSplitSSTableType &split_sstable_type,
      const ObTableStoreIterator &table_store_iterator,
      const bool is_table_restore,
      const ObIArray<ObITable::TableKey> &skipped_table_keys,
      const bool filter_normal_cg_sstables,
      const bool filter_meta_major_sstables,
      ObIArray<ObITable *> &participants);
  static int split_task_ranges(
      ObIAllocator &allocator,
      const share::ObDDLType ddl_type,
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const int64_t user_parallelism,
      const int64_t schema_tablet_size,
      ObIArray<blocksstable::ObDatumRowkey> &parallel_datum_rowkey_list);
  static int convert_rowkey_to_range(
      ObIAllocator &allocator,
      const ObIArray<blocksstable::ObDatumRowkey> &parallel_datum_rowkey_list,
      ObIArray<blocksstable::ObDatumRange> &datum_ranges_array);

  // only used for table recovery to build parallel tasks cross tenants.
  static int convert_datum_rowkey_to_range(
      ObIAllocator &allocator,
      const ObIArray<blocksstable::ObDatumRowkey> & datum_rowkey_list,
      ObIArray<blocksstable::ObDatumRange> &datum_ranges_array);

  // to check dest tablets data completed.
  // @param [in] check_tablets_id
  // @param [in] check_remote
  //   1. check_remote = true, means only to check the shared tablet.
  //   2. check_remote = false, means only to check the local tablet.
  // @param [out] is_completed, return true when all tablets data are completed.
  static int check_dest_data_completed(
      const ObLSHandle &ls_handle,
      const ObIArray<ObTabletID> &check_tablets_id,
      const bool check_remote,
      bool &is_completed);
  // to check whether the data split task finished by checking,
  // 1. the split dest tablets data completed.
  // 2. the split source tablet meta updated.
  static int check_data_split_finished(
      const share::ObLSID &ls_id,
      const ObTabletID &source_tablet_id,
      const ObIArray<ObTabletID> &dest_tablets_id,
      const bool can_reuse_macro_block,
      bool &is_finished);

  static int check_src_tablet_table_store_ready(
      const ObLSHandle &ls_handle,
      const ObTabletHandle &local_source_tablet_handle);

  static int check_satisfy_split_condition(
      const ObLSHandle &ls_handle,
      const ObTabletHandle &local_source_tablet_handle,
      const ObArray<ObTabletID> &dest_tablets_id,
      const int64_t compaction_scn,
      const share::SCN &min_split_start_scn);
  static int get_split_dest_tablets_info(
      const share::ObLSID &ls_id,
      const ObTabletID &source_tablet_id,
      ObIArray<ObTabletID> &dest_tablets_id,
      lib::Worker::CompatMode &compat_mode);
  static int check_medium_compaction_info_list_cnt(
      const obrpc::ObCheckMediumCompactionInfoListArg &arg,
      obrpc::ObCheckMediumCompactionInfoListResult &result);
  static int check_tablet_restore_status(
      const ObIArray<ObTabletID> &dest_tablets_id,
      const ObLSHandle &ls_handle,
      const ObTabletHandle &source_tablet_handle,
      bool &is_tablet_status_need_to_split);
  static int build_mds_sstable(
      const ObLSHandle &ls_handle,
      const ObTabletHandle &source_tablet_handle,
      const int64_t dest_tablet_index,
      const ObTabletID &dest_tablet_id,
      const share::SCN &reorganization_scn,
      ObMdsTableMiniMerger &mds_mini_merger,
      compaction::ObTabletMergeCtx &tablet_merge_ctx,
      bool &has_mds_row);
  static int check_sstables_skip_data_split(
      const ObLSHandle &ls_handle,
      const ObTableStoreIterator &source_table_store_iter,
      const ObIArray<ObTabletID> &dest_tablets_id,
      const int64_t lob_major_snapshot/*OB_INVALID_VERSION for non lob tablets*/,
      ObIArray<ObITable::TableKey> &skipped_split_major_keys);
  static int build_update_table_store_param(
      const share::SCN &reorg_scn,
      const int64_t ls_rebuild_seq,
      const int64_t snapshot_version,
      const int64_t multi_version_start,
      const ObTabletID &dst_tablet_id,
      const ObTablesHandleArray &tables_handle,
      const compaction::ObMergeType &merge_type,
      const ObIArray<ObITable::TableKey> &skipped_split_major_keys,
      ObBatchUpdateTableStoreParam &param);
  static int get_storage_schema_from_mds(
      const ObTabletHandle &tablet_handle,
      const int64_t data_format_version,
      ObStorageSchema *&storage_schema,
      ObIAllocator &allocator);
  static int register_split_info_mds(const ObTabletSplitRegisterMdsArg &arg,
                                     const ObPartitionSplitArg &partition_split_arg,
                                     const uint64_t data_format_version,
                                     rootserver::ObDDLService &ddl_service);
  static int persist_tablet_mds_on_demand(
      ObLS *ls,
      const ObTabletHandle &local_tablet_handle,
      bool &has_mds_table_for_dump);
  static int build_adjusted_col_layout_storage_schema(
      ObIAllocator &allocator,
      const ObSSTable &src_sstable,
      const ObStorageSchema &split_mds_storage_schema,
      const ObStorageSchema *target_storage_schema,
      const ObStorageSchema *&storage_schema);
private:
  static int build_split_base_storage_schema(
      ObIAllocator &allocator,
      const ObSSTable &src_sstable,
      const ObStorageSchema &split_mds_storage_schema,
      ObStorageSchema *&target_storage_schema);
  static int check_and_determine_mds_end_scn(
      const ObTabletHandle &dest_tablet_handle,
      share::SCN &end_scn);
  static int check_and_build_mds_sstable_merge_ctx(
      const ObLSHandle &ls_handle,
      const ObTabletHandle &dest_tablet_handle,
      const share::SCN &reorganization_scn,
      compaction::ObTabletMergeCtx &tablet_merge_ctx);
  static int check_tablet_ha_status(
      const ObLSHandle &ls_handle,
      const ObTabletHandle &source_tablet_handle,
      const ObIArray<ObTabletID> &dest_tablets_id);

};



}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_DDL_OB_TABLET_SPLIT_UTIL_H
