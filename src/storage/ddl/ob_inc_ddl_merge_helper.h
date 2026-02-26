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

#ifndef OCEANBASE_STORAGE_INC_DDL_MERGE_HELPER_
#define OCEANBASE_STORAGE_INC_DDL_MERGE_HELPER_

#include "share/scn.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/ddl/ob_ddl_merge_helper.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "close_modules/shared_storage/storage/ddl/ob_ss_ddl_util.h"
#endif
namespace oceanbase
{
namespace storage
{
class ObIncMinDDLMergeHelper: public ObIDDLMergeHelper
{
public:
  int process_prepare_task(ObIDag *dag,
                           ObDDLTabletMergeDagParamV2 &ddl_merge_param,
                           ObIArray<ObTuple<int64_t, int64_t, int64_t>> &cg_slices) override;
  int merge_cg_slice(ObIDag* dag,
                     ObDDLTabletMergeDagParamV2 &merge_param,
                     const int64_t cg_idx,
                     const int64_t start_slice,
                     const int64_t end_slice) override;
  int assemble_sstable(ObDDLTabletMergeDagParamV2 &param) override;
  int get_rec_scn(ObDDLTabletMergeDagParamV2 &merge_param) override;
private:
  bool is_supported_direct_load_type(const ObDirectLoadType direct_load_type) override
  {
    return ObDirectLoadType::DIRECT_LOAD_INCREMENTAL == direct_load_type;
  }
};

class ObIncMajorDDLMergeHelper: public ObIDDLMergeHelper
{
public:
  ObIncMajorDDLMergeHelper();
  virtual ~ObIncMajorDDLMergeHelper();
  int check_need_merge(ObIDag *dag,
                       ObDDLTabletMergeDagParamV2 &ddl_merge_param,
                       bool &need_merge) override;
  int process_prepare_task(ObIDag *dag,
                           ObDDLTabletMergeDagParamV2 &ddl_merge_param,
                           common::ObIArray<ObTuple<int64_t, int64_t, int64_t>> &cg_slices) override;
  int merge_cg_slice(ObIDag* dag,
                     ObDDLTabletMergeDagParamV2 &merge_param,
                     const int64_t cg_idx,
                     const int64_t start_slice,
                     const int64_t end_slice) override;
  int assemble_sstable(ObDDLTabletMergeDagParamV2 &param) override;
  int get_rec_scn(ObDDLTabletMergeDagParamV2 &merge_param) override;
protected:
  bool is_supported_direct_load_type(const ObDirectLoadType direct_load_type) override ;
private:
  int calculate_scn_range(const common::ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
                          const common::ObIArray<blocksstable::ObSSTable *> &ddl_sstables,
                          const bool for_major,
                          ObTabletDDLParam &ddl_param);
  int calculate_rec_scn(const common::ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
                        ObTableStoreIterator &ddl_table_iter,
                        share::SCN &rec_scn);
  int check_sstables_empty(const ObDDLTabletMergeDagParamV2 &merge_param,
                           const ObTablesHandleArray &table_array,
                           bool &is_empty);
  int verify_inc_major_sstable(const ObSSTable &inc_major_sstable,
                               ObTabletHandle &tablet_handle);
  int update_tablet_table_store(
      ObDDLTabletMergeDagParamV2 &dag_merge_param,
      ObTablesHandleArray &table_array,
      ObSSTable *inc_major_sstable);
};

#ifdef OB_BUILD_SHARED_STORAGE
class ObSSIncMajorDDLMergeHelper: public ObIDDLMergeHelper
{
  using MacroBlockIdSet = hash::ObHashSet<MacroBlockId, hash::NoPthreadDefendMode>;
public:
  ObSSIncMajorDDLMergeHelper();
  virtual ~ObSSIncMajorDDLMergeHelper();
  int process_prepare_task(ObIDag *dag,
                           ObDDLTabletMergeDagParamV2 &ddl_merge_param,
                           common::ObIArray<ObTuple<int64_t, int64_t, int64_t>> &cg_slices) override;
  int merge_cg_slice(ObIDag* dag,
                     ObDDLTabletMergeDagParamV2 &merge_param,
                     const int64_t cg_idx,
                     const int64_t start_slice,
                     const int64_t end_slice);
  int assemble_sstable(ObDDLTabletMergeDagParamV2 &param) override;
  int get_rec_scn(ObDDLTabletMergeDagParamV2 &merge_param) override;
protected:
  bool is_supported_direct_load_type(const ObDirectLoadType direct_load_type) override;
  int set_ddl_complete(ObIDag *dag,
                       ObTablet &tablet,
                       ObDDLTabletMergeDagParamV2 &ddl_merge_param);
  int merge_dump_sstable(ObDDLTabletMergeDagParamV2 &merge_param);
  int merge_cg_sstable(ObIDag* dag,
                       ObDDLTabletMergeDagParamV2 &merge_param,
                       int64_t cg_idx);
private:
  int calculate_inc_major_end_scn(
      ObDDLTabletMergeDagParamV2 &dag_merge_param,
      ObTabletHandle &tablet_handle);
  int init_cg_sstable_array(
      ObDDLTabletMergeDagParamV2 &dag_merge_param,
      hash::ObHashSet<int64_t> &slice_idxes);
  int prepare_ss_merge_param(
      ObDDLTabletMergeDagParamV2 &dag_merge_param,
      ObLSID &ls_id,
      ObTabletID &tablet_id,
      ObTabletHandle &tablet_handle,
      ObStorageSchema *&storage_schema,
      ObWriteTabletParam *&tablet_param,
      ObDDLTabletContext::MergeCtx *&merge_ctx);
  int prepare_for_dump_sstable(
      const ObTabletHandle &tablet_handle,
      ObDDLTabletMergeDagParamV2 &dag_merge_param,
      ObDDLTabletContext::MergeCtx *merge_ctx,
      common::ObIArray<ObTuple<int64_t, int64_t, int64_t>> &cg_slices);
  int get_macro_id_from_sstable(
      ObTableStoreIterator &sstable_iter,
      MacroBlockIdSet &macro_id_set);
  int get_macro_id_from_ddl_kv(
      ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
      MacroBlockIdSet &macro_id_set);
  int persist_macro_id(
      const MacroBlockIdSet &macro_id_set,
      ObIArray<MacroBlockId> &macro_id_array);
  int prepare_cg_table_key(
      const ObDDLTabletMergeDagParamV2 &dag_merge_param,
      const int64_t cg_idx,
      const ObStorageSchema *storage_schema,
      const ObTabletHandle &tablet_handle,
      ObITable::TableKey &cur_cg_table_key);
  int rebuild_index(
      ObSSTableIndexBuilder &sstable_index_builder,
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      ObDDLTabletMergeDagParamV2 &dag_merge_param,
      ObWriteTabletParam *tablet_param,
      ObMacroSeqParam &macro_seq_param,
      ObDDLIncRedoLogWriterCallback &clustered_index_flush_callback,
      ObIArray<ObMacroMetaStoreManager::StoreItem> &sorted_meta_stores,
      ObMacroDataSeq &tmp_seq,
      ObDDLRedoLogWriterCallbackInitParam &init_param,
      ObSSTableMergeRes &res,
      ObDDLWriteStat &write_stat);
  int append_macro_rows_to_index_rebuilder(
      ObIArray<ObMacroMetaStoreManager::StoreItem> &sorted_meta_stores,
      ObIndexBlockRebuilder &index_block_rebuilder,
      ObDDLWriteStat &write_stat);
  int create_sstable(
      const ObDDLTabletMergeDagParamV2 &dag_merge_param,
      ObArenaAllocator &allocator,
      ObDDLTabletContext::MergeCtx *merge_ctx,
      ObSSTableMergeRes &res,
      const ObITable::TableKey &cur_cg_table_key,
      const ObStorageSchema *storage_schema,
      const int64_t create_schema_version_on_tablet,
      ObTableHandleV2 &cg_sstable_handle);
  int close_sstable_index_builder(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      ObDDLRedoLogWriterCallbackInitParam &init_param,
      ObIndexBlockRebuilder &index_block_rebuilder,
      ObSSTableIndexBuilder &sstable_index_builder,
      ObMacroDataSeq &tmp_seq,
      ObSSTableMergeRes &res);
  int record_root_seq(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      ObSSTableMergeRes &res);
  int update_tablet_table_store(
      ObDDLTabletMergeDagParamV2 &dag_merge_param,
      ObTablesHandleArray &table_array,
      ObSSTable *inc_major_sstable);
  int update_tablet_table_store_for_dump_sstable(
      ObDDLTabletMergeDagParamV2 &dag_merge_param,
      const ObLSID &ls_id,
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      const ObUpdateTableStoreParam &update_table_store_param,
      ObTabletHandle &new_tablet_handle);
  int update_tablet_table_store_for_inc_major(
      ObDDLTabletMergeDagParamV2 &dag_merge_param,
      const ObSSTable *inc_major_sstable);
  int release_ddl_kv_for_major(ObDDLTabletMergeDagParamV2 &dag_merge_param,
                               ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int get_ddl_kvs_and_ddl_dump_sstables(
      const ObTabletHandle &tablet_handle,
      const ObDDLTabletMergeDagParamV2 &dag_merge_param,
      const bool frozen_only,
      ObIArray<ObDDLKVHandle> &ddl_kvs,
      ObTableStoreIterator &ddl_dump_sstables);
  int get_min_max_scn(
      const ObIArray<ObDDLKVHandle> &ddl_kvs,
      ObTableStoreIterator &sstable_iter,
      SCN &min_scn,
      SCN &max_scn);
private:
  SCN end_scn_;
};
#endif

} // namespace storage
} // namespace oceanbase

#endif