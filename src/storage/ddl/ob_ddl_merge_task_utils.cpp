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

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/ddl/ob_ddl_merge_task_utils.h"
#include "share/ob_ddl_checksum.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/ob_ddl_sim_point.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/ob_storage_schema_util.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "observer/ob_server_event_history_table_operator.h"
using namespace oceanbase::observer;
using namespace oceanbase::share::schema;
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace storage
{
int ObDDLMergeTaskUtils::check_idempodency(const ObIArray<ObDDLBlockMeta> &input_metas,
                                           ObIArray<ObDDLBlockMeta> &output_metas,
                                           ObDDLWriteStat *write_stat)
{
  int ret = OB_SUCCESS;
  output_metas.reset();
  hash::ObHashMap<ObLogicMacroBlockId, int64_t> id_checksum_map;
  if (nullptr != write_stat) {
    write_stat->reset();
  }

  if (GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("shared storage mode do not need idempodency checker", K(ret), K(lbt()));
  } else if (input_metas.count() > 0 && OB_FAIL(id_checksum_map.create(input_metas.count(), ObMemAttr(MTL_ID(), "DDL_MER_IDEM")))) {
    LOG_WARN("failed to create checksum map", K(ret), K(input_metas.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < input_metas.count(); ++i) {
    ObDDLBlockMeta block_meta = input_metas.at(i);
    int64_t checksum = 0;
    if (OB_ISNULL(block_meta.block_meta_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block meta should not be null", K(ret));
    } else if (OB_FAIL(id_checksum_map.get_refactored(block_meta.block_meta_->val_.logic_id_, checksum))) {
      if (OB_HASH_NOT_EXIST == ret) {
        if (OB_FAIL(output_metas.push_back(input_metas.at(i)))) {
          LOG_WARN("failed to push back val", K(ret), K(input_metas.at(i)));
        } else if (OB_FAIL(id_checksum_map.set_refactored(block_meta.block_meta_->val_.logic_id_, block_meta.block_meta_->val_.data_checksum_))) {
          LOG_WARN("failed to sset refactored", K(ret), K(block_meta.block_meta_->val_));
        } else if (nullptr != write_stat){
          write_stat->row_count_ += block_meta.block_meta_->val_.row_count_;
        }
      } else {
        LOG_WARN("failed to get refactored", K(ret));
      }
    } else {
      if (checksum != block_meta.block_meta_->val_.data_checksum_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unequal checksum", K(ret), KPC(block_meta.block_meta_));
      }
    }
  }
  return ret;
}

int init_datum_utils(ObTablet &tablet,
                     const ObITable::TableKey &table_key,
                     const share::SCN &ddl_start_scn,
                     const uint64_t data_format_version,
                     const ObStorageSchema *storage_schema,
                     ObIAllocator &allocator,
                     blocksstable::ObWholeDataStoreDesc &data_desc,
                     blocksstable::ObStorageDatumUtils &row_id_datum_utils,
                     blocksstable::ObStorageDatumUtils *&datum_utils)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator ddl_table_iter;
  ObITable *first_ddl_sstable = nullptr; // get compressor_type of macro block for query
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(!table_key.is_valid() || data_format_version <= 0 || OB_ISNULL(storage_schema))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_key), KP(storage_schema));
  } else if (OB_FAIL(tablet.get_ddl_sstables(ddl_table_iter))) {
    LOG_WARN("get ddl sstable handles failed", K(ret));
  } else if (ddl_table_iter.count() > 0 && OB_FAIL(ddl_table_iter.get_boundary_table(false/*is_last*/, first_ddl_sstable))) {
    LOG_WARN("failed to get boundary table", K(ret));
  } else if (OB_FAIL(ObTabletDDLUtil::prepare_index_data_desc(tablet,
                                                              table_key,
                                                              table_key.get_snapshot_version(),
                                                              data_format_version,
                                                              static_cast<ObSSTable *>(first_ddl_sstable),
                                                              storage_schema,
                                                              tablet.get_reorganization_scn(),
                                                              data_desc))) {
    LOG_WARN("prepare data store desc failed", K(ret), K(table_key), K(data_format_version));
  } else {
    if (data_desc.get_desc().is_cg()) {
      schema::ObColDesc int_col_desc;
      int_col_desc.col_id_ = 0;
      int_col_desc.col_order_ = ObOrderType::ASC;
      int_col_desc.col_type_.set_int();
      ObSEArray<schema::ObColDesc, 1> col_descs;
      col_descs.set_attr(ObMemAttr(MTL_ID(), "DDL_Btree_descs"));
      const bool is_column_store = true;
      if (OB_FAIL(col_descs.push_back(int_col_desc))) {
        LOG_WARN("push back col desc failed", K(ret));
      } else if (OB_FAIL(row_id_datum_utils.init(col_descs, col_descs.count(), lib::is_oracle_mode(), allocator, is_column_store))) {
        LOG_WARN("init row id datum utils failed", K(ret), K(col_descs));
      } else {
        datum_utils = &row_id_datum_utils;
        LOG_INFO("block meta tree sort with row id", K(table_key));
      }
    } else {
      datum_utils = const_cast<blocksstable::ObStorageDatumUtils *>(&data_desc.get_desc().get_datum_utils());
    }
  }
  return ret;
}

struct EndkeyCmpLess
{
public:
  explicit EndkeyCmpLess(ObStorageDatumUtils *datum_utils) : datum_utils_(datum_utils) {}
  bool operator() (ObDDLBlockMeta &left, ObDDLBlockMeta &right)
  {
    int cmp_ret = 0;
    int ret = OB_SUCCESS;
    if (nullptr != datum_utils_ && nullptr != left.block_meta_ && nullptr != right.block_meta_) {
      ret = left.block_meta_->end_key_.compare(right.block_meta_->end_key_, *datum_utils_, cmp_ret);
    }
    return cmp_ret < 0;
  }
public:
  ObStorageDatumUtils *datum_utils_;
};

struct EndkeyCmpEqual
{
public:
  explicit EndkeyCmpEqual(ObStorageDatumUtils *datum_utils) : datum_utils_(datum_utils) {}
  bool operator() (ObDDLBlockMeta &left, ObDDLBlockMeta &right)
  {
    int cmp_ret = 0;
    int ret = OB_SUCCESS;
    if (nullptr != datum_utils_ && nullptr != left.block_meta_ && nullptr != right.block_meta_) {
      ret = left.block_meta_->end_key_.compare(right.block_meta_->end_key_, *datum_utils_, cmp_ret);
    }
    return cmp_ret == 0;
  }
public:
  ObStorageDatumUtils *datum_utils_;
};

int ObDDLMergeTaskUtils::get_slice_indexes(const ObIArray<const ObSSTable *> &ddl_sstables, hash::ObHashSet<int64_t> &slice_idxes)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ddl_sstables.count(); ++i) {
    const ObSSTable *cur_sstable = ddl_sstables.at(i);
    if (OB_ISNULL(cur_sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl memtable is null", K(ret), KP(cur_sstable), K(i));
    } else if (OB_FAIL(slice_idxes.set_refactored(cur_sstable->get_key().get_slice_idx(), 1/*over_write*/))) {
      LOG_WARN("put slice index into set failed", K(ret), K(cur_sstable->get_key()));
    }
  }
  return ret;
}

int ObDDLMergeTaskUtils::get_merge_slice_idx(const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs, int64_t &merge_slice_idx)
{
  int ret = OB_SUCCESS;
  merge_slice_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < frozen_ddl_kvs.count(); ++i) {
    const ObDDLKVHandle &cur_kv_handle = frozen_ddl_kvs.at(i);
    if (OB_ISNULL(cur_kv_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current ddl kv is null", K(ret), K(i), K(cur_kv_handle));
    } else {
      merge_slice_idx = MAX(merge_slice_idx, cur_kv_handle.get_obj()->get_merge_slice_idx());
    }
  }
  return ret;
}

int ObDDLMergeTaskUtils::get_ddl_memtables(const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
                                           ObIArray<const ObSSTable *> &all_ddl_memtables)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < frozen_ddl_kvs.count(); ++i) {
    const ObDDLKVHandle &cur_kv_handle = frozen_ddl_kvs.at(i);
    if (OB_ISNULL(cur_kv_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current ddl kv is null", K(ret), K(i), K(cur_kv_handle));
    } else {
      const ObIArray<ObDDLMemtable *> &ddl_memtables = cur_kv_handle.get_obj()->get_ddl_memtables();
      for (int64_t j = 0; OB_SUCC(ret) && j < ddl_memtables.count(); ++j) {
        const ObDDLMemtable *cur_ddl_memtable = ddl_memtables.at(j);
        if (OB_ISNULL(cur_ddl_memtable)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ddl memtable is null", K(ret), KP(cur_ddl_memtable), K(i), K(j), KPC(cur_kv_handle.get_obj()));
        } else if (OB_FAIL(all_ddl_memtables.push_back(cur_ddl_memtable))) {
          LOG_WARN("push back ddl memtable failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLMergeTaskUtils::freeze_ddl_kv(const ObLSID &ls_id,
                                           const ObTabletID &tablet_id,
                                           const ObDirectLoadType &direct_load_type,
                                           const share::SCN start_scn,
                                           const int64_t snapshot_version,
                                           const uint64_t tenant_data_version)

{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               tablet_id,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet failed", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl kv mgr not exist", K(ret), K(ls_id), K(tablet_id));
    } else {
      LOG_WARN("get ddl kv mgr failed", K(ret), K(ls_id), K(tablet_id));
    }
  } else if (is_full_direct_load(direct_load_type)
      && start_scn < tablet_handle.get_obj()->get_tablet_meta().ddl_start_scn_) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("ddl task expired, skip it", K(ret), K(tablet_id), "new_start_scn", tablet_handle.get_obj()->get_tablet_meta().ddl_start_scn_);
  // TODO(cangdi): do not freeze when ddl kv's min scn >= rec_scn
  } else if (!ddl_kv_mgr_handle.get_obj()->can_freeze()) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_WARN("cannot freeze now", K(tablet_id));
    }
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->freeze_ddl_kv(
      start_scn, snapshot_version, tenant_data_version, SCN::min_scn()/*freeze_scn*/,
      convert_direct_load_type_to_ddl_kv_type(direct_load_type)))) {
    LOG_WARN("ddl kv manager try freeze failed", K(ret), K(tablet_id),
        K(start_scn), K(snapshot_version), K(tenant_data_version), K(direct_load_type));
  }
  return ret;
}


int ObDDLMergeTaskUtils::get_ddl_tables_from_ddl_kvs(
    const ObArray<ObDDLKVHandle> &frozen_ddl_kvs,
    const int64_t cg_idx,
    const int64_t start_slice_idx,
    const int64_t end_slice_idx,
    ObIArray<ObSSTable*> &ddl_sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((cg_idx < 0) ||
                  (start_slice_idx < 0 || end_slice_idx < 0) ||
                  (start_slice_idx > end_slice_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cg_idx), K(start_slice_idx), K(end_slice_idx));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < frozen_ddl_kvs.count(); ++i) {
    ObDDLKV *cur_kv = frozen_ddl_kvs.at(i).get_obj();
    if (OB_ISNULL(cur_kv)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur kv should not be null", K(ret));
    } else {
      const ObIArray<ObDDLMemtable *> &ddl_memtables = cur_kv->get_ddl_memtables();
      for (int64_t j = 0; OB_SUCC(ret) && j < ddl_memtables.count(); ++j) {
        ObDDLMemtable *cur_sstable = ddl_memtables.at(j);
        if (OB_ISNULL(cur_sstable)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("current ddl memtable is null", K(ret), KP(cur_sstable));
        } else if (cur_sstable->get_key().column_group_idx_ == cg_idx &&
                   start_slice_idx <= cur_sstable->get_slice_idx()    &&
                   end_slice_idx   >= cur_sstable->get_slice_idx()) {
          if (OB_FAIL(ddl_sstable.push_back(cur_sstable))) {
            LOG_WARN("push back target sstable failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}


int sort_and_push_metas(ObArray<ObDDLBlockMeta> &meta_array, ObStorageDatumUtils *datum_utils, ObIArray<ObDDLBlockMeta> &sorted_metas)
{
  int ret = OB_SUCCESS;
  EndkeyCmpLess cmp_less(datum_utils);
  EndkeyCmpEqual cmp_equal(datum_utils);
  std::sort(meta_array.begin(), meta_array.end(), cmp_less);
  ObArray<ObDDLBlockMeta>::iterator unique_iter = std::unique(meta_array.begin(), meta_array.end(), cmp_equal);
  int64_t unique_count = unique_iter - meta_array.begin();
  for (int64_t i = 0; OB_SUCC(ret) && i < unique_count; ++i) {
    if (OB_FAIL(sorted_metas.push_back(meta_array.at(i)))) {
      LOG_WARN("push back sorted meta failed", K(ret));
    }
  }
  return ret;
}


int ObDDLMergeTaskUtils::get_ddl_tables_from_dump_tables(
    const bool for_row_store,
    ObTableStoreIterator &ddl_sstable_iter,
    const int64_t cg_idx,
    const int64_t start_slice_idx,
    const int64_t end_slice_idx,
    ObIArray<ObSSTable*> &ddl_sstable,
    ObIArray<ObStorageMetaHandle> &meta_handles)
{
  int ret = OB_SUCCESS;
  ddl_sstable_iter.resume();
  /* don't reset ddl sstable, which may have data before */
  meta_handles.reset();
  if (OB_UNLIKELY((cg_idx < 0) ||
                  (start_slice_idx < 0 || end_slice_idx < 0)||
                  (start_slice_idx > end_slice_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_slice_idx), K(end_slice_idx));
  }
  while (OB_SUCC(ret)) {
    ObITable *table = nullptr;
    if (OB_FAIL(ddl_sstable_iter.get_next(table))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next table failed", K(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, table is nullptr", K(ret), KPC(table));
    }

    /* for row store
     * add all table to ddl sstable array
     * table not null check before
     */
    if (OB_FAIL(ret)) {
    } else if (for_row_store) {
      if (OB_FAIL(ddl_sstable.push_back(static_cast<ObSSTable *>(table)))) {
         LOG_WARN("push back target sstable failed", K(ret));
      }
    }

    /* for column store
     * get sstable according to cg idx
     * check not null before
    */
    if (OB_FAIL(ret)) {
    } else if (!for_row_store) {
      ObCOSSTableV2 *cur_co_sstable = nullptr;
      ObSSTableWrapper cg_sstable_wrapper;
      ObSSTable *cg_sstable = nullptr;
      if (!table->is_co_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current table not co sstable", K(ret), KPC(table));
      } else if (FALSE_IT(cur_co_sstable = static_cast<ObCOSSTableV2 *>(table))) {
      } else if (cur_co_sstable->is_cgs_empty_co_table()) {
        /* skip */
      } else if (OB_FAIL(cur_co_sstable->fetch_cg_sstable(cg_idx, cg_sstable_wrapper))) {
        LOG_WARN("get all tables failed", K(ret));
      } else if (OB_FAIL(cg_sstable_wrapper.get_loaded_column_store_sstable(cg_sstable))) {
        LOG_WARN("get sstable failed", K(ret));
      } else if (OB_ISNULL(cg_sstable)) {
        /* skip */
      } else if (cg_sstable->get_slice_idx() < start_slice_idx ||
                 cg_sstable->get_slice_idx() > end_slice_idx) {
        /* skip */
      } else if (OB_FAIL(ddl_sstable.push_back(cg_sstable))) {
        LOG_WARN("push back cg sstable failed", K(ret));
      } else if (cg_sstable_wrapper.get_meta_handle().is_valid() &&
                 OB_FAIL(meta_handles.push_back(cg_sstable_wrapper.get_meta_handle()))) {
        LOG_WARN("push back meta handle failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLMergeTaskUtils::only_update_ddl_checkpoint(ObDDLTabletMergeDagParamV2 &dag_merge_param)
{
  int ret = OB_SUCCESS;

  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;
  bool for_major = dag_merge_param.for_major_;

  ObLSHandle ls_handle;
  ObLSService *ls_service = MTL(ObLSService*);
  ObTabletHandle tablet_handle, new_tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObSSTable *first_major_sstable = nullptr;

  int64_t rebuild_seq = -1;
  int64_t snapshot_version = 0;
  int64_t multi_version_start = 0;

  if (!dag_merge_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dag_merge_param));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(target_ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (!ls_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls handle should be valid", K(ret), K(ls_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FALSE_IT(first_major_sstable = static_cast<ObSSTable *>(
                                                table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/)))) {
  } else {
    rebuild_seq = ls_handle.get_ls()->get_rebuild_seq();
    snapshot_version    =  max(dag_merge_param.ddl_task_param_.snapshot_version_, tablet_handle.get_obj()->get_snapshot_version());
    multi_version_start =  max(dag_merge_param.ddl_task_param_.snapshot_version_, tablet_handle.get_obj()->get_multi_version_start());

    ObUpdateTableStoreParam param (snapshot_version, multi_version_start, tablet_param->storage_schema_, rebuild_seq);
    param.ddl_info_.update_with_major_flag_ = true;
    param.ddl_info_.keep_old_ddl_sstable_ = false;
    param.ddl_info_.data_format_version_ = dag_merge_param.ddl_task_param_.tenant_data_version_;
    param.ddl_info_.ddl_checkpoint_scn_ = dag_merge_param.rec_scn_;
    param.ddl_info_.ddl_replay_status_ = tablet_handle.get_obj()->get_tablet_meta().ddl_replay_status_; /* not change replay status*/
    if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(target_tablet_id, param, new_tablet_handle))) {
      LOG_WARN("failed to update table store", K(ret));
    }
  }
  return ret;
}

/* do not add any new logic to this function !!! */
int ObDDLMergeTaskUtils::update_tablet_table_store(ObDDLTabletMergeDagParamV2 &dag_merge_param,
                                                    ObTablesHandleArray &co_sstable_array,
                                                    ObSSTable *&major_sstable)
{
  int ret = OB_SUCCESS;

  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;
  bool for_major = dag_merge_param.for_major_;

  ObLSHandle ls_handle;
  ObLSService *ls_service = MTL(ObLSService*);
  ObTabletHandle tablet_handle, new_tablet_handle;

  int64_t rebuild_seq = -1;
  int64_t snapshot_version = 0;
  int64_t multi_version_start = 0;

  if (!dag_merge_param.is_valid()
      || (nullptr == major_sstable && co_sstable_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dag_merge_param), K(major_sstable), K(co_sstable_array));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(target_ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(target_ls_id));
  } else if (!ls_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls handle should be valid", K(ret), K(ls_handle));
  } else {
    rebuild_seq = ls_handle.get_ls()->get_rebuild_seq();
    snapshot_version    = for_major ? max(dag_merge_param.ddl_task_param_.snapshot_version_, tablet_handle.get_obj()->get_snapshot_version()) :
                                  tablet_handle.get_obj()->get_snapshot_version();
    multi_version_start = for_major ? max(dag_merge_param.ddl_task_param_.snapshot_version_, tablet_handle.get_obj()->get_multi_version_start()) :
                                          0;
  }

  if (OB_FAIL(ret)) {
  } else {
    ObUpdateTableStoreParam table_store_param(snapshot_version, multi_version_start, tablet_param->storage_schema_, rebuild_seq, major_sstable, true);
    if (OB_FAIL(table_store_param.init_with_compaction_info(ObCompactionTableStoreParam(for_major ? compaction::MEDIUM_MERGE : compaction::MINI_MERGE,
                                                                                        share::SCN::min_scn(),
                                                                                        for_major /* need_report*/,
                                                                                          false /* has truncate info*/)))) {
      LOG_WARN("init with compaction info failed", K(ret));
    } else {
      table_store_param.ddl_info_.update_with_major_flag_ =  for_major;
      table_store_param.ddl_info_.keep_old_ddl_sstable_ = !for_major;
      table_store_param.ddl_info_.data_format_version_ = dag_merge_param.ddl_task_param_.tenant_data_version_;
      table_store_param.ddl_info_.ddl_checkpoint_scn_ = dag_merge_param.rec_scn_;
      if (!for_major) {
        // data is not complete, now update ddl table store only for reducing count of ddl dump sstable.
        table_store_param.ddl_info_.ddl_replay_status_ = tablet_handle.get_obj()->get_tablet_meta().ddl_replay_status_;
      } else {
        // data is complete, mark ddl replay status finished
        table_store_param.ddl_info_.ddl_replay_status_ = dag_merge_param.table_key_.is_co_sstable() ? CS_REPLICA_REPLAY_COLUMN_FINISH : CS_REPLICA_REPLAY_ROW_STORE_FINISH;
      }

      for (int64_t i = 0; !for_major && OB_SUCC(ret) && i < co_sstable_array.get_count(); i++ ) {
        const ObSSTable *cur_slice_sstable = static_cast<ObSSTable *>(co_sstable_array.get_table(i));
        if (OB_ISNULL(cur_slice_sstable)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("slice sstable is null", K(ret), K(i), KP(cur_slice_sstable));
        } else if (OB_FAIL(table_store_param.ddl_info_.slice_sstables_.push_back(cur_slice_sstable))) {
          LOG_WARN("push back slice ddl sstable failed", K(ret), K(i), KPC(cur_slice_sstable));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(target_tablet_id, table_store_param, new_tablet_handle))) {
        LOG_WARN("failed to update tablet table store", K(ret), K(target_tablet_id), K(table_store_param));
      } else if (for_major && OB_FAIL(MTL(ObTabletTableUpdater*)->submit_tablet_update_task(target_ls_id, target_tablet_id))) {
        LOG_WARN("fail to submit tablet update task", K(ret), K(dag_merge_param));
      }
    }
  }
  return ret;
}

/* do not add any new logic to this function !!! */
int ObDDLMergeTaskUtils::build_sstable(ObDDLTabletMergeDagParamV2 &dag_merge_param,
                                       ObTablesHandleArray &co_sstable_array,
                                       ObSSTable *&major_sstable)
{
  int ret = OB_SUCCESS;

  co_sstable_array.reset();
  major_sstable = nullptr;

  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;
  hash::ObHashMap<int64_t, ObArray<ObTableHandleV2>*> *slice_idx_cg_sstables = nullptr;

  bool is_column_store_table = false;

  if (!dag_merge_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dag_merge_param));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_ISNULL(tablet_param->storage_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet param should not be null", K(ret));
  } else if (FALSE_IT(is_column_store_table = ObITable::is_column_store_sstable(dag_merge_param.table_key_.table_type_))) {
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(slice_idx_cg_sstables = dag_merge_param.for_lob_ ? &dag_merge_param.get_tablet_ctx()->lob_merge_ctx_.slice_cg_sstables_ :
                                                                         &dag_merge_param.get_tablet_ctx()->merge_ctx_.slice_cg_sstables_)) {
  } else {
    for (hash::ObHashMap<int64_t, ObArray<ObTableHandleV2>*>::const_iterator iter = slice_idx_cg_sstables->begin();
         OB_SUCC(ret) && iter != slice_idx_cg_sstables->end();
         iter++) {
      int64_t start_slice_idx = iter->first;
      ObArray<ObTableHandleV2> *sstable_handles = iter->second;

      /* get root sstable */
      ObTableHandleV2 &root_sstable  = sstable_handles->at(dag_merge_param.table_key_.get_column_group_id());
      ObArray<ObITable*> cg_sstables;
      for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx < sstable_handles->count(); cg_idx++) {
        if (cg_idx != dag_merge_param.table_key_.get_column_group_id() &&
            OB_FAIL(cg_sstables.push_back(sstable_handles->at(cg_idx).get_table()))) {
          LOG_WARN("failed to push back val", K(ret));
        }
      }
      /* fill cg sstable when sstable is not empty*/
      if (OB_FAIL(ret)) {
      } else if (is_column_store_table &&
                 !static_cast<ObCOSSTableV2*>(root_sstable.get_table())->is_cgs_empty_co_table() &&
                 OB_FAIL(static_cast<ObCOSSTableV2*>(root_sstable.get_table())->fill_cg_sstables(cg_sstables))) {
        LOG_WARN("failed to fill cg sstable", K(ret));
      } else if (OB_FAIL(co_sstable_array.add_table(root_sstable))) {
        LOG_WARN("failed to add table", K(ret));
      } else if (dag_merge_param.for_major_ && 0 == start_slice_idx) {
        major_sstable = static_cast<ObSSTable*>(root_sstable.get_table());
      }
    }
  }
  return ret;
}


 // for cg sstable, endkey is end row id, confirm read_info not used
int ObDDLMergeTaskUtils::get_sorted_meta_array(
    ObTablet &tablet,
    const ObTabletDDLParam &ddl_param,
    const ObStorageSchema *storage_schema,
    const ObIArray<ObSSTable *> &sstables,
    const ObITableReadInfo &read_info,
    ObIAllocator &allocator,
    ObArray<ObDDLBlockMeta> &sorted_metas)
{
  int ret = OB_SUCCESS;
  sorted_metas.reset();
  if (OB_UNLIKELY(!read_info.is_valid())) { // allow empty sstable array
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sstables), K(read_info));
  } else {
    // sort sstable by slice_idx
    ObArray<ObSSTable *> sorted_sstables;
    struct {
      bool operator() (ObSSTable *left, ObSSTable *right) {
        return left->get_slice_idx() < right->get_slice_idx();
      }
    } slice_idx_cmp;
    if (OB_FAIL(sorted_sstables.assign(sstables))) {
      LOG_WARN("assign sstable array failed", K(ret), K(sstables.count()));
    } else {
      lib::ob_sort(sorted_sstables.begin(), sorted_sstables.end(), slice_idx_cmp);
    }
    ObDatumRange query_range;
    query_range.set_whole_range();
    ObDataMacroBlockMeta data_macro_meta;
    blocksstable::ObWholeDataStoreDesc data_desc;
    blocksstable::ObStorageDatumUtils row_id_datum_utils;
    blocksstable::ObStorageDatumUtils *datum_utils = nullptr;
    ObArray<ObDDLBlockMeta> meta_array;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(init_datum_utils(tablet, ddl_param.table_key_, ddl_param.start_scn_, ddl_param.data_format_version_,
              storage_schema, allocator, data_desc, row_id_datum_utils, datum_utils))) {
        LOG_WARN("init datum utils failed", K(ret));
      }
    }
    int64_t last_slice_idx = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < sorted_sstables.count(); ++i) {
      ObSSTable *cur_sstable = sorted_sstables.at(i);
      ObDDLMacroBlockIterator block_iter;
      if (OB_ISNULL(cur_sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, table is nullptr", K(ret), KPC(cur_sstable));
      } else if (cur_sstable->get_slice_idx() < last_slice_idx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable should sorted by slice idx asc", K(ret), K(cur_sstable->get_key()), K(last_slice_idx));
      } else if (cur_sstable->get_slice_idx() > last_slice_idx) {
        // new slice, save sorted_metas and reset
        if (meta_array.count() > 0) {
          if (OB_FAIL(sort_and_push_metas(meta_array, datum_utils, sorted_metas))) {
            LOG_WARN("sort and push meta failed", K(ret));
          }
        }
        meta_array.reuse();
        last_slice_idx = cur_sstable->get_slice_idx();

      }
      if (OB_FAIL(ret)) {
      } else if (cur_sstable->is_empty()) {
        // do nothing, skip
      } else if (OB_FAIL(block_iter.open(cur_sstable, query_range, read_info, allocator))) {
        LOG_WARN("open macro block iterator failed", K(ret), K(read_info), KPC(cur_sstable));
      } else {
        ObDataMacroBlockMeta *copied_meta = nullptr; // copied meta will destruct in the meta tree
        int64_t end_row_offset = 0;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(block_iter.get_next(data_macro_meta, end_row_offset))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get data macro meta failed", K(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else {
            ObDDLBlockMeta ddl_block_meta;
            if (OB_FAIL(data_macro_meta.deep_copy(copied_meta, allocator))) {
              LOG_WARN("deep copy macro block meta failed", K(ret));
            } else if (FALSE_IT(ddl_block_meta.block_meta_ = copied_meta)) {
            } else if (FALSE_IT(ddl_block_meta.end_row_offset_ = end_row_offset)) {
            } else if (OB_FAIL(meta_array.push_back(ddl_block_meta))) {
              LOG_WARN("push back macro meta failed", K(ret), K(meta_array.count()), KPC(copied_meta));
            } else {
              FLOG_INFO("append meta tree success", K(ret), "table_key", cur_sstable->get_key(), "macro_block_id", data_macro_meta.get_macro_id(),
                  "data_checksum", copied_meta->val_.data_checksum_, K(meta_array.count()), "macro_block_end_key", copied_meta->end_key_,
                  "end_row_offset", end_row_offset);
            }
          }
        }
        ObCStringHelper helper;
        LOG_INFO("append meta tree finished", K(ret), "table_key", cur_sstable->get_key(), "data_macro_block_cnt_in_sstable", cur_sstable->get_data_macro_block_count(),
            K(meta_array.count()), "sstable_end_key", OB_ISNULL(copied_meta) ? "NOT_EXIST": helper.convert(copied_meta->end_key_), "end_row_offset", end_row_offset);
      }
    }
    if (OB_SUCC(ret) && meta_array.count() > 0) { // save sorted_metas from last meta array
      if (OB_FAIL(sort_and_push_metas(meta_array, datum_utils, sorted_metas))) {
        LOG_WARN("sort and push meta failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t sstable_checksum = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < sorted_metas.count(); ++i) {
      const ObDataMacroBlockMeta *cur_macro_meta = sorted_metas.at(i).block_meta_;
      sstable_checksum = ob_crc64_sse42(sstable_checksum, &cur_macro_meta->val_.data_checksum_, sizeof(cur_macro_meta->val_.data_checksum_));
      FLOG_INFO("sorted meta array", K(i), "macro_block_id", cur_macro_meta->get_macro_id(), "data_checksum", cur_macro_meta->val_.data_checksum_, K(sstable_checksum), "macro_block_end_key", cur_macro_meta->end_key_);
    }
  }
  return ret;
}
} //namespace storage
} //namespace oceanbase
