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

#ifndef OCEANBASE_STORAGE_DDL_OB_TABLET_SPLIT_UTIL_H
#define OCEANBASE_STORAGE_DDL_OB_TABLET_SPLIT_UTIL_H

#include "share/ob_ddl_common.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "close_modules/shared_storage/storage/incremental/atomic_protocol/ob_atomic_sstablelist_define.h"
#include "close_modules/shared_storage/storage/incremental/atomic_protocol/ob_atomic_sstablelist_op.h"
#include "close_modules/shared_storage/storage/incremental/ob_shared_meta_service.h"
#include "close_modules/shared_storage/storage/incremental/atomic_protocol/ob_atomic_file_handle.h"
#endif

namespace oceanbase
{
namespace storage
{
class ObTabletHandle;
class ObLSHandle;
}
namespace storage
{
class ObSSDataSplitHelper;

enum class ObSplitTabletInfoStatus : uint8_t
{
  CANT_EXEC_MINOR = 0,
  CANT_GC_MACROS = 1,
  STATUS_TYPE_MAX
};

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

static bool is_valid_tablet_split_info_status(const ObSplitTabletInfoStatus &type)
{
  return ObSplitTabletInfoStatus::CANT_EXEC_MINOR <= type
      && ObSplitTabletInfoStatus::STATUS_TYPE_MAX > type;
}

struct ObTabletSplitUtil final
{
public:
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
      const share::SCN &min_split_start_scn
#ifdef OB_BUILD_SHARED_STORAGE
      , bool &is_data_split_executor
#endif
      );
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
      common::ObArenaAllocator &allocator,
      const ObLSHandle &ls_handle,
      const ObTabletHandle &source_tablet_handle,
      const ObTabletID &dest_tablet_id,
      const share::SCN &reorganization_scn,
    #ifdef OB_BUILD_SHARED_STORAGE
      const ObSSDataSplitHelper &mds_ss_split_helper,
    #endif
      ObTableHandleV2 &mds_table_handle);
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
private:
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

#ifdef OB_BUILD_SHARED_STORAGE
class ObSSDataSplitHelper final
{
public:
  ObSSDataSplitHelper()
    : allocator_("SSSplitHelp", OB_MALLOC_NORMAL_BLOCK_SIZE /*8KB*/, MTL_ID()),
      split_sstable_list_handle_(allocator_), add_minor_op_handle_(allocator_)
    { }
  ~ObSSDataSplitHelper() { reset(); }
  void reset();
private:
  template <typename T>
  void release_handles_array(ObIArray<T *> &arr);
public:
  // 1. generate macro start sequence for data block.
  // 2. generate macro start sequence for index block when parallel_idx = parallel_cnt.
  int generate_major_macro_seq_info(
      const int64_t major_index /*the index in the generated majors, from 0*/,
      const int64_t parallel_cnt /*the parallel cnt in one sstable*/,
      const int64_t parallel_idx /*the parallel idx in one sstable*/,
      int64_t &macro_start_seq) const;

  // generate macro start sequence for tablet persist.
  static int get_major_macro_seq_by_stage(
      const compaction::ObGetMacroSeqStage &stage,
      const int64_t majors_cnt /*cnt of the dst_majors */,
      const int64_t parallel_cnt,
      int64_t &macro_start_seq);

  // 1. generate macro start sequence for data block.
  // 2. generate macro start sequence for index block when parallel_idx = parallel_cnt.
  int generate_minor_macro_seq_info(
      const int64_t dest_tablet_index/*index in dest_tablets_id array*/,
      const int64_t minor_index/*the index in generated minors, from 0*/,
      const int64_t parallel_cnt/*the parallel cnt in one sstable*/,
      const int64_t parallel_idx/*the parallel idx in one sstable*/,
      int64_t &macro_start_seq) const;

  int start_add_minor_op(
      const ObLSID &ls_id,
      const share::SCN &split_scn,
      const int64_t parallel_cnt_of_each_sstable,
      const ObTableStoreIterator &src_table_store_iterator,
      const ObIArray<ObTabletID> &dest_tablets_id);
  int start_add_mds_op(
      const ObLSID &ls_id,
      const share::SCN &split_scn,
      const int64_t parallel_cnt_of_each_sstable,
      const int64_t sstables_cnt_of_each_tablet,
      const ObTabletID &dst_tablet_id);
  int finish_add_op(
      const int64_t dest_tablet_index,
      const bool need_finish);

  int persist_majors_gc_rely_info(
      const ObLSID &ls_id,
      const ObIArray<ObTabletID> &dst_tablet_ids,
      const share::SCN &transfer_scn,
      const int64_t generated_majors_cnt,
      const int64_t max_majors_snapshot,
      const int64_t parallel_cnt_of_each_sstable);

  static int set_source_tablet_split_status(
      const ObLSHandle &ls_handle,
      const ObTabletHandle &local_source_tablet_hdl,
      const ObSplitTabletInfoStatus &expected_status);

  static int check_at_sswriter_lease(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      int64_t &epoch,
      bool &is_data_split_executor);

  static int check_satisfy_ss_split_condition(
      const ObLSHandle &ls_handle,
      const ObTabletHandle &local_source_tablet_handle,
      const ObArray<ObTabletID> &dest_tablets_id,
      bool &is_data_split_executor);

  static int check_ss_data_completed(
      const ObLSHandle &ls_handle,
      const ObIArray<ObTabletID> &dest_tablet_ids,
      bool &is_completed);

  static int create_shared_tablet_if_not_exist(
      const ObLSID &ls_id,
      const ObIArray<ObTabletID> &dest_tablet_ids,
      const share::SCN &transfer_scn);

  int get_op_id(
      const int64_t dest_tablet_index,
      int64_t &op_id) const;
private:
  int prepare_minor_gc_info_list(
      const int64_t parallel_cnt_of_each_sstable,
      const int64_t sstables_cnt_of_each_tablet,
      ObSSTableGCInfo &minor_gc_info);
  int start_add_op(
      const ObLSID &ls_id,
      const share::SCN &split_scn,
      const ObAtomicFileType &file_type,
      const int64_t parallel_cnt_of_each_sstable,
      const int64_t sstables_cnt_of_each_tablet,
      const ObIArray<ObTabletID> &dest_tablets_id);
private:
  static const int64_t SPLIT_MINOR_MACRO_DATA_SEQ_BITS = 40;
  common::ObArenaAllocator allocator_;
public:
  // for minor split.
  ObFixedArray<ObAtomicFileHandle<ObAtomicSSTableListFile>*, common::ObIAllocator> split_sstable_list_handle_;
  // array for each dest tablet, and corresponding to the each one in split_sstable_list_handle_.
  ObFixedArray<ObAtomicOpHandle<ObAtomicSSTableListAddOp>*, common::ObIAllocator> add_minor_op_handle_;
};
#endif


}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_DDL_OB_TABLET_SPLIT_UTIL_H
