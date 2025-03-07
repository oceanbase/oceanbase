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

#include "storage/ddl/ob_ddl_merge_task.h"
#include "share/ob_ddl_checksum.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/ob_ddl_sim_point.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/ob_storage_schema_util.h"
#include "storage/compaction/ob_schedule_dag_func.h"

using namespace oceanbase::observer;
using namespace oceanbase::share::schema;
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace storage
{

/******************             ObDDLTableMergeDag             *****************/
ObDDLTableMergeDag::ObDDLTableMergeDag()
  : ObIDag(ObDagType::DAG_TYPE_DDL_KV_MERGE),
    is_inited_(false),
    ddl_param_()
{
}

ObDDLTableMergeDag::~ObDDLTableMergeDag()
{
}

int ObDDLTableMergeDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTablet *tablet = nullptr;
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(ddl_param_));
  } else if (OB_ISNULL(param) || !param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(param));
  } else {
    ddl_param_ = *static_cast<const ObDDLTableMergeDagParam *>(param);
    is_inited_ = true;
  }
  return ret;
}

int ObDDLTableMergeDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObArray<ObDDLKVHandle> ddl_kvs_handle;
  ObDDLTableMergeTask *merge_task = nullptr;
  if (OB_FAIL(ls_service->get_ls(ddl_param_.ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ddl_param_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               ddl_param_.tablet_id_,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet failed", K(ret), K(ddl_param_));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(ddl_param_));
  } else if (OB_FAIL(prepare_ddl_kvs(*tablet_handle.get_obj(), ddl_kvs_handle))) {
    LOG_WARN("fail to prepare load ddl kvs", K(ret));
  } else if (OB_FAIL(alloc_task(merge_task))) {
    LOG_WARN("Fail to alloc task", K(ret), K(ddl_param_));
  } else if (OB_FAIL(merge_task->init(ddl_param_, ddl_kvs_handle))) {
    LOG_WARN("failed to init ddl table merge task", K(ret), K(*this));
  } else if (OB_FAIL(add_task(*merge_task))) {
    LOG_WARN("Fail to add task", K(ret), K(ddl_param_));
  }
  return ret;
}

int ObDDLTableMergeDag::prepare_ddl_kvs(ObTablet &tablet, ObIArray<ObDDLKVHandle> &ddl_kvs_handle)
{
  int ret = OB_SUCCESS;
  if (is_full_direct_load(ddl_param_.direct_load_type_)) { // full direct load
    if (OB_FAIL(prepare_full_direct_load_ddl_kvs(tablet, ddl_kvs_handle))) {
      LOG_WARN("fail to prepare full direct load ddl kvs", K(ret));
    }
  } else { // incremental direct load
    if (OB_FAIL(prepare_incremental_direct_load_ddl_kvs(tablet, ddl_kvs_handle))) {
      LOG_WARN("fail to prepare incremental direct load ddl kvs", K(ret));
    }
  }
  return ret;
}

int ObDDLTableMergeDag::prepare_full_direct_load_ddl_kvs(ObTablet &tablet, ObIArray<ObDDLKVHandle> &ddl_kvs_handle)
{
  int ret = OB_SUCCESS;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ddl_kvs_handle.reset();
  if (OB_FAIL(tablet.get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl kv mgr not exist", K(ret), K(ddl_param_));
    } else {
      LOG_WARN("get ddl kv mgr failed", K(ret), K(ddl_param_));
    }
  } else if (ddl_param_.start_scn_ < tablet.get_tablet_meta().ddl_start_scn_) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("ddl task expired, skip it", K(ret), K(ddl_param_), "new_start_scn", tablet.get_tablet_meta().ddl_start_scn_);
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(true/*frozen_only*/, ddl_kvs_handle))) {
    LOG_WARN("get freezed ddl kv failed", K(ret), K(ddl_param_));
  }
  return ret;
}

int ObDDLTableMergeDag::prepare_incremental_direct_load_ddl_kvs(ObTablet &tablet, ObIArray<ObDDLKVHandle> &ddl_kvs_handle)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTableHandleV2, BASIC_MEMSTORE_CNT> memtable_handles;
  ObITable *table = nullptr;
  ObDDLKV *ddl_kv = nullptr;
  ObTableHandleV2 selected_ddl_kv_handle;
  ddl_kvs_handle.reset();
  if (OB_FAIL(tablet.get_all_memtables_from_memtable_mgr(memtable_handles))) {
    LOG_WARN("fail to get all memtable", K(ret), K(tablet));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < memtable_handles.count(); ++i) {
    ObTableHandleV2 &table_handle = memtable_handles.at(i);
    if (OB_ISNULL(table = table_handle.get_table())) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table must not null", K(ret), K(table_handle));
    } else if (OB_UNLIKELY(!table->is_direct_load_memtable())) {
      LOG_DEBUG("skip not direct load memtable", K(i), KPC(table), K(memtable_handles));
      break;
    } else if (OB_ISNULL(ddl_kv = static_cast<ObDDLKV *>(table))) {
      ret = OB_ERR_SYS;
      LOG_ERROR("table not ddl kv", K(ret), KPC(table), K(memtable_handles));
    } else if (OB_UNLIKELY(ddl_kv->is_active_memtable())) {
      LOG_DEBUG("skip active ddlkv", K(i), KPC(ddl_kv), K(memtable_handles));
      break;
    } else if (!ddl_kv->can_be_minor_merged()) {
      FLOG_INFO("ddlkv cannot mini merge now", K(i), KPC(ddl_kv), K(memtable_handles));
      break;
    } else {
      selected_ddl_kv_handle = table_handle;
      if (ddl_kv->get_end_scn() > tablet.get_tablet_meta().clog_checkpoint_scn_) {
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!selected_ddl_kv_handle.is_valid()) {
      ret = OB_NO_NEED_MERGE;
      LOG_DEBUG("ddlkv no need merge", K(tablet), K(memtable_handles));
    } else {
      ObDDLKVHandle ddl_kv_handle;
      if (OB_FAIL(ddl_kv_handle.set_obj(selected_ddl_kv_handle))) {
        LOG_WARN("fail to set obj", K(ret), K(selected_ddl_kv_handle));
      } else if (OB_FAIL(ddl_kvs_handle.push_back(ddl_kv_handle))) {
        LOG_WARN("fail to push back", K(ret), K(ddl_kv_handle));
      }
    }
  }
  return ret;
}

bool ObDDLTableMergeDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObDDLTableMergeDag &other_dag = static_cast<const ObDDLTableMergeDag&> (other);
    // each tablet has max 1 dag in running, so that the compaction task is unique and no need to consider concurrency
    is_same = ddl_param_.tablet_id_ == other_dag.ddl_param_.tablet_id_
      && ddl_param_.ls_id_ == other_dag.ddl_param_.ls_id_
      && ddl_param_.direct_load_type_ == other_dag.ddl_param_.direct_load_type_;
  }
  return is_same;
}

int64_t ObDDLTableMergeDag::hash() const
{
  return ddl_param_.tablet_id_.hash();
}

int ObDDLTableMergeDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLTableMergeDag has not been initialized", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  ddl_param_.ls_id_.id(),
                                  static_cast<int64_t>(ddl_param_.tablet_id_.id()),
                                  static_cast<int64_t>(ddl_param_.rec_scn_.get_val_for_inner_table_field()),
                                  "is_commit", ddl_param_.is_commit_))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

int ObDDLTableMergeDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "ddl table merge task: ls_id=%ld, tablet_id=%ld, rec_scn=%lu",
                              ddl_param_.ls_id_.id(), ddl_param_.tablet_id_.id(), ddl_param_.rec_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("fill dag key for ddl table merge dag failed", K(ret), K(ddl_param_));
  }
  return ret;
}

bool ObDDLTableMergeDag::ignore_warning()
{
  return OB_LS_NOT_EXIST == dag_ret_
    || OB_TABLET_NOT_EXIST == dag_ret_
    || OB_TASK_EXPIRED == dag_ret_
    || OB_EAGAIN == dag_ret_
    || OB_NEED_RETRY == dag_ret_;
}

ObDDLTableMergeTask::ObDDLTableMergeTask()
  : ObITask(ObITaskType::TASK_TYPE_DDL_KV_MERGE),
    is_inited_(false), merge_param_()
{

}

ObDDLTableMergeTask::~ObDDLTableMergeTask()
{
}

int ObDDLTableMergeTask::init(const ObDDLTableMergeDagParam &ddl_dag_param, const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(merge_param_));
  } else if (OB_UNLIKELY(!ddl_dag_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_dag_param));
  } else if (OB_FAIL(frozen_ddl_kvs_.assign(frozen_ddl_kvs))) {
    LOG_WARN("assign ddl kv handle array failed", K(ret), K(frozen_ddl_kvs.count()));
  } else {
    merge_param_ = ddl_dag_param;
    is_inited_ = true;
  }
  return ret;
}

int wait_lob_tablet_major_exist(ObLSHandle &ls_handle, ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  ObTabletBindingMdsUserData ddl_data;
  const ObTabletMeta &tablet_meta = tablet.get_tablet_meta();
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  ObTabletDirectLoadMgrHandle direct_load_mgr_handle;
  ObDDLTableMergeDagParam param;
  bool is_major_sstable_exist = false;
  if (OB_FAIL(tablet.ObITabletMdsInterface::get_ddl_data(share::SCN::max_scn(), ddl_data))) {
    LOG_WARN("failed to get ddl data from tablet", K(ret), K(tablet_meta));
  } else if (ddl_data.lob_meta_tablet_id_.is_valid()) {
    ObTabletHandle lob_tablet_handle;
    const ObTabletID lob_tablet_id = ddl_data.lob_meta_tablet_id_;
    if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, lob_tablet_id, lob_tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      LOG_WARN("get lob tablet handle failed", K(ret), K(lob_tablet_id));
    } else {
      bool is_major_sstable_exist = lob_tablet_handle.get_obj()->get_major_table_count() > 0
        || lob_tablet_handle.get_obj()->get_tablet_meta().table_store_flag_.with_major_sstable();
      if (!is_major_sstable_exist) {
        ret = OB_EAGAIN;
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(compaction::ObTenantTabletScheduler::schedule_tablet_ddl_major_merge(ls_handle, lob_tablet_handle))) {
          LOG_WARN("schedule ddl major merge for lob tablet failed", K(tmp_ret), K(lob_tablet_id));
        }
      }
    }
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObDDLTableMergeTask::dump_in_shared_storage_mode(
    const ObLSHandle &ls_handle,
    ObTablet &tablet,
    ObTableStoreIterator &ddl_table_iter,
    const ObDDLKvMgrHandle &ddl_kv_mgr_handle,
    ObArenaAllocator &allocator,
    ObTableHandleV2 &compacted_sstable_handle)
{
  int ret = OB_SUCCESS;

  DEBUG_SYNC(BEFORE_DDL_TABLE_MERGE_TASK);
  ObTabletDDLParam ddl_param;
  ddl_param.ls_id_ = merge_param_.ls_id_;
  ddl_param.table_key_.tablet_id_ = merge_param_.tablet_id_;
  ddl_param.table_key_.column_group_idx_ = 0;
  ddl_param.table_key_.table_type_ = ObITable::DDL_DUMP_SSTABLE;
  ddl_param.direct_load_type_ = merge_param_.direct_load_type_;
  ddl_param.data_format_version_ = merge_param_.data_format_version_;
  ddl_param.snapshot_version_ = merge_param_.snapshot_version_;
  ddl_param.start_scn_ = tablet.get_tablet_meta().ddl_start_scn_;

  SCN &compact_start_scn = ddl_param.table_key_.scn_range_.start_scn_;
  SCN &compact_end_scn = ddl_param.table_key_.scn_range_.end_scn_;
  if (OB_FAIL(ObTabletDDLUtil::get_compact_scn(ddl_param.start_scn_,
          ddl_table_iter, frozen_ddl_kvs_, compact_start_scn, compact_end_scn))) {
    LOG_WARN("get compact scn failed", K(ret), K(merge_param_), K(ddl_param), K(ddl_table_iter), K(frozen_ddl_kvs_));
  } else if (OB_FAIL(ObTabletDDLUtil::compact_ddl_kv(*ls_handle.get_ls(), tablet,
          ddl_table_iter, frozen_ddl_kvs_, ddl_param, allocator, compacted_sstable_handle))) {
    LOG_WARN("compact sstables failed", K(ret), K(ddl_param));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->release_ddl_kvs(compact_end_scn))) {
    LOG_WARN("release ddl kv failed", K(ret), K(ddl_param), K(compact_end_scn));
  }
  LOG_INFO("dump ddl sstable on shared storage mode finished", K(ddl_param),
      "compacted_sstable", PC(compacted_sstable_handle.get_table()));
  return ret;
}
#endif

int ObDDLTableMergeTask::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("ddl merge task start process", K(*this), "ddl_event_info", ObDDLEventInfo());
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(merge_param_.ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(merge_param_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               merge_param_.tablet_id_,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet failed", K(ret), K(merge_param_));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(merge_param_));
  } else if (OB_FAIL(merge_ddl_kvs(ls_handle, *tablet_handle.get_obj()))) {
    LOG_WARN("fail to merge ddl kvs", K(ret));
  }
  return ret;
}

int ObDDLTableMergeTask::merge_ddl_kvs(ObLSHandle &ls_handle, ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  if (is_full_direct_load(merge_param_.direct_load_type_)) {
    if (OB_FAIL(merge_full_direct_load_ddl_kvs(ls_handle, tablet))) {
      LOG_WARN("fail to merge full direct load ddl kvs", K(ret));
    }
  } else { // incremental direct load
    if (OB_FAIL(merge_incremental_direct_load_ddl_kvs(ls_handle, tablet))) {
      LOG_WARN("fail to merge incremental direct load ddl kvs", K(ret));
    }
  }
  return ret;
}

int ObDDLTableMergeTask::merge_full_direct_load_ddl_kvs(ObLSHandle &ls_handle, ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  int64_t MAX_DDL_SSTABLE = ObTabletDDLKvMgr::MAX_DDL_KV_CNT_IN_STORAGE * 0.5;
#ifdef ERRSIM
  if (0 != GCONF.errsim_max_ddl_sstable_count) {
    MAX_DDL_SSTABLE = GCONF.errsim_max_ddl_sstable_count;
  } else {
    MAX_DDL_SSTABLE = 2;
  }
  LOG_INFO("set max ddl sstable in errsim mode", K(MAX_DDL_SSTABLE));
#endif
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObTableStoreIterator ddl_table_iter;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const uint64_t tenant_id = MTL_ID();
  common::ObArenaAllocator allocator("DDLMergeTask", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTableHandleV2 old_sstable_handle;
  ObTableHandleV2 compacted_sstable_handle;
  ObSSTable *sstable = nullptr;
  bool is_major_exist = false;
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  ObTabletDirectLoadMgrHandle tablet_mgr_hdl;
  ObTabletFullDirectLoadMgr *tablet_direct_load_mgr = nullptr;
  if (OB_ISNULL(tenant_direct_load_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(tablet.get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl kv mgr not exist", K(ret), K(merge_param_));
    } else {
      LOG_WARN("get ddl kv mgr failed", K(ret), K(merge_param_));
    }
  } else if (OB_FAIL(tablet.get_ddl_sstables(ddl_table_iter))) {
    LOG_WARN("get ddl sstable handles failed", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(dump_in_shared_storage_mode(ls_handle, tablet,
            ddl_table_iter, ddl_kv_mgr_handle, allocator, compacted_sstable_handle))) {
      LOG_WARN("dump ddl kv in shared storage mode failed", K(ret), K(merge_param_));
    }
#endif
  } else {
    DEBUG_SYNC(BEFORE_DDL_TABLE_MERGE_TASK);
#ifdef ERRSIM
    if (GCONF.errsim_test_tablet_id.get_value() > 0 && merge_param_.tablet_id_.id() == GCONF.errsim_test_tablet_id.get_value()) {
      LOG_INFO("test tablet ddl merge start", K(ret), K(merge_param_));
      DEBUG_SYNC(BEFORE_LOB_META_TABELT_DDL_MERGE_TASK);
    }
#endif
#ifdef ERRSIM
    static int64_t counter = 0;
    counter++;
    if (counter >= 2) {
      DEBUG_SYNC(BEFORE_MIG_DDL_TABLE_MERGE_TASK);
    }
#endif

    ObTabletDDLParam ddl_param;
    bool is_data_complete = false;
    const ObSSTable *first_major_sstable = nullptr;
    SCN compact_start_scn, compact_end_scn;
    if (OB_FAIL(ObTabletDDLUtil::check_and_get_major_sstable(
        merge_param_.ls_id_, merge_param_.tablet_id_, first_major_sstable, table_store_wrapper))) {
      LOG_WARN("check if major sstable exist failed", K(ret));
    } else if (nullptr != first_major_sstable) {
      is_major_exist = true;
      LOG_INFO("major sstable has been created before", K(merge_param_));
    } else if (tablet.get_tablet_meta().table_store_flag_.with_major_sstable()) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("tablet me says with major but no major, meaning its a migrated deleted tablet, skip");
    } else if (OB_FAIL(tenant_direct_load_mgr->get_tablet_mgr(
        ObTabletDirectLoadMgrKey(merge_param_.tablet_id_, ObDirectLoadType::DIRECT_LOAD_DDL), tablet_mgr_hdl))) {
      LOG_WARN("get tablet direct load mgr failed", K(ret), K(merge_param_));
    } else if (OB_FAIL(tablet_mgr_hdl.get_full_obj()->prepare_major_merge_param(ddl_param))) {
      LOG_WARN("preare full direct load sstable param failed", K(ret));
    } else if (merge_param_.is_commit_ && OB_FAIL(wait_lob_tablet_major_exist(ls_handle, tablet))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("wait lob tablet major sstable exist faild", K(ret), K(merge_param_));
      } else {
        LOG_INFO("need wait lob tablet major sstable exist", K(ret), K(merge_param_));
      }
    } else if (merge_param_.start_scn_ > SCN::min_scn() && merge_param_.start_scn_ < ddl_param.start_scn_) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl merge task expired, do nothing", K(merge_param_), "new_start_scn", ddl_param.start_scn_);
    } else if (OB_FAIL(ObTabletDDLUtil::get_compact_scn(ddl_param.start_scn_, ddl_table_iter, frozen_ddl_kvs_, compact_start_scn, compact_end_scn))) {
      LOG_WARN("get compact scn failed", K(ret), K(merge_param_), K(ddl_param), K(ddl_table_iter), K(frozen_ddl_kvs_));
    } else if (ddl_param.commit_scn_.is_valid_and_not_min() && compact_end_scn > ddl_param.commit_scn_) {
      ret = OB_ERR_SYS;
      LOG_WARN("compact end scn is larger than commit scn", K(ret), K(ddl_param), K(compact_end_scn), K(frozen_ddl_kvs_), K(ddl_table_iter));
    } else {
      bool is_data_complete = merge_param_.is_commit_
        && compact_start_scn == SCN::scn_dec(merge_param_.start_scn_)
        && compact_end_scn == merge_param_.rec_scn_
#ifdef ERRSIM
        // skip build major until current time reach the delayed time
        && ObTimeUtility::current_time() > merge_param_.rec_scn_.convert_to_ts() + GCONF.errsim_ddl_major_delay_time
#endif
        ;
      if (!is_data_complete) {
        ddl_param.table_key_.table_type_ = ddl_param.table_key_.is_co_sstable() ? ObITable::DDL_MERGE_CO_SSTABLE : ObITable::DDL_DUMP_SSTABLE;
        ddl_param.table_key_.scn_range_.start_scn_ = compact_start_scn;
        ddl_param.table_key_.scn_range_.end_scn_ = compact_end_scn;
      } else {
        // use the final table key of major, do nothing
      }
      if (OB_FAIL(ObTabletDDLUtil::compact_ddl_kv(*ls_handle.get_ls(),
                                                  tablet,
                                                  ddl_table_iter,
                                                  frozen_ddl_kvs_,
                                                  ddl_param,
                                                  allocator,
                                                  compacted_sstable_handle))) {
        LOG_WARN("compact sstables failed", K(ret), K(ddl_param), K(is_data_complete));
      } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->release_ddl_kvs(compact_end_scn))) {
        LOG_WARN("release ddl kv failed", K(ret), K(ddl_param), K(compact_end_scn));
      }
      if (OB_SUCC(ret) && is_data_complete) {
        is_major_exist = true;
        LOG_INFO("create major sstable success", K(ret), K(ddl_param), KPC(compacted_sstable_handle.get_table()));
      }
    }

    if (OB_SUCC(ret) && merge_param_.is_commit_ && is_major_exist) {
      if (OB_FAIL(MTL(ObTabletTableUpdater*)->submit_tablet_update_task(merge_param_.ls_id_, merge_param_.tablet_id_))) {
        LOG_WARN("fail to submit tablet update task", K(ret), K(tenant_id), K(merge_param_));
      } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->release_ddl_kvs(SCN::max_scn()))) {
        LOG_WARN("release all ddl kv failed", K(ret), K(ddl_param));
      } else if (OB_FAIL(tenant_direct_load_mgr->remove_tablet_direct_load(ObTabletDirectLoadMgrKey(merge_param_.tablet_id_, true)))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("remove tablet mgr failed", K(ret), K(MTL_ID()), K(merge_param_));
        }
      }
      LOG_INFO("commit ddl sstable finished", K(ret), K(ddl_param), K(merge_param_), KPC(tablet_mgr_hdl.get_full_obj()), "ddl_event_info", ObDDLEventInfo());
    }
  }
  return ret;
}

static int refine_incremental_direct_load_merge_param(const ObTablet &tablet,
                                                      ObTabletDDLParam &ddl_param,
                                                      bool &need_check_tablet)
{
#define PRINT_TS_WRAPPER(x) (ObPrintTableStore(*(x.get_member())))

  int ret = OB_SUCCESS;
  need_check_tablet = false;
  ObITable *last_table = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_UNLIKELY(!table_store_wrapper.get_member()->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Table store not valid", K(ret), K(table_store_wrapper));
  } else if (OB_ISNULL(last_table =
      table_store_wrapper.get_member()->get_minor_sstables().get_boundary_table(true/*last*/))) {
    // no minor sstable, skip to cut memtable's boundary
  } else if (ddl_param.table_key_.scn_range_.start_scn_ > last_table->get_end_scn()) {
    need_check_tablet = true;
  } else if (ddl_param.table_key_.scn_range_.start_scn_ < last_table->get_end_scn()
      && !tablet.get_tablet_meta().tablet_id_.is_special_merge_tablet()) {
    // fix start_scn to make scn_range continuous in migrate phase
    if (ddl_param.table_key_.scn_range_.end_scn_ <= last_table->get_end_scn()) {
      ret = OB_NO_NEED_MERGE;
      LOG_WARN("No need mini merge memtable which is covered by existing sstable",
               K(ret), K(ddl_param), KPC(last_table), K(PRINT_TS_WRAPPER(table_store_wrapper)), K(tablet));
    } else {
      ddl_param.table_key_.scn_range_.start_scn_ = last_table->get_end_scn();
      FLOG_INFO("Fix mini merge result scn range", K(ret), K(ddl_param), KPC(last_table),
                K(PRINT_TS_WRAPPER(table_store_wrapper)), K(tablet));
    }
  }
  return ret;
}

int ObDDLTableMergeTask::merge_incremental_direct_load_ddl_kvs(ObLSHandle &ls_handle, ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("DDLMergeTask", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObDDLKV *ddl_kv = nullptr;
  ObTableStoreIterator ddl_table_iter;
  ObTabletDDLParam ddl_param;
  ObTableHandleV2 compacted_sstable_handle;
  const SCN &clog_checkpoint_scn = tablet.get_clog_checkpoint_scn();
  bool need_check_tablet = false;
  if (OB_UNLIKELY(frozen_ddl_kvs_.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected frozen ddl kvs", K(ret), K(merge_param_), K(frozen_ddl_kvs_));
  } else if (OB_ISNULL(ddl_kv = frozen_ddl_kvs_.at(0).get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid ddlkv handle", K(ret), K(merge_param_), K(frozen_ddl_kvs_));
  } else if (ddl_kv->get_end_scn() <= clog_checkpoint_scn) {
    // do nothing
  } else {
    ddl_param.direct_load_type_ = merge_param_.direct_load_type_;
    ddl_param.ls_id_ = merge_param_.ls_id_;
    ddl_param.start_scn_ = ddl_kv->get_ddl_start_scn();
    ddl_param.commit_scn_ = ddl_kv->get_ddl_start_scn();
    ddl_param.data_format_version_ = ddl_kv->get_data_format_version();
    ddl_param.table_key_.tablet_id_ = merge_param_.tablet_id_;
    ddl_param.table_key_.scn_range_.start_scn_ = ddl_kv->get_start_scn();
    ddl_param.table_key_.scn_range_.end_scn_ = ddl_kv->get_end_scn();
    ddl_param.table_key_.table_type_ = ObITable::MINI_SSTABLE;
    ddl_param.snapshot_version_ = ddl_kv->get_snapshot_version();
    ddl_param.trans_id_ = ddl_kv->get_trans_id();
    if (OB_FAIL(refine_incremental_direct_load_merge_param(tablet, ddl_param, need_check_tablet))) {
      if (OB_NO_NEED_MERGE != ret) {
        LOG_WARN("fail to refine incremental direct load merge param", K(ret), K(tablet),
                 K(ddl_param), K(frozen_ddl_kvs_));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_UNLIKELY(need_check_tablet)) {
      ret = OB_EAGAIN;
      int tmp_ret = OB_SUCCESS;
      ObTabletHandle tmp_tablet_handle;
      if (OB_TMP_FAIL(ls_handle.get_ls()->get_tablet(merge_param_.tablet_id_,
                                                     tmp_tablet_handle,
                                                     0/*timeout_us*/,
                                                     ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        LOG_WARN("failed to get tablet", K(tmp_ret), K(merge_param_));
      } else if (OB_UNLIKELY(!tmp_tablet_handle.is_valid())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid tablet", K(tmp_ret), K(merge_param_));
      } else if (tmp_tablet_handle.get_obj()->get_clog_checkpoint_scn() != clog_checkpoint_scn) {
        // do nothing, just retry the merge task
      } else {
        LOG_ERROR("Unexpected uncontinuous scn_range in mini merge", K(ret), K(clog_checkpoint_scn),
                  K(ddl_param), K(frozen_ddl_kvs_), K(tablet), KPC(tmp_tablet_handle.get_obj()));
      }
    } else if (OB_FAIL(ObTabletDDLUtil::compact_ddl_kv(*ls_handle.get_ls(),
                                                       tablet,
                                                       ddl_table_iter,
                                                       frozen_ddl_kvs_,
                                                       ddl_param,
                                                       allocator,
                                                       compacted_sstable_handle))) {
      LOG_WARN("compact sstables failed", K(ret), K(ddl_param), K(frozen_ddl_kvs_));
    }
  }
  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    ObTabletHandle new_tablet_handle;
    if (OB_TMP_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                              merge_param_.tablet_id_,
                                              new_tablet_handle,
                                              ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      LOG_WARN("failed to get tablet", K(tmp_ret), K(merge_param_));
    } else if (OB_TMP_FAIL(new_tablet_handle.get_obj()->release_memtables(new_tablet_handle.get_obj()->get_tablet_meta().clog_checkpoint_scn_))) {
      LOG_WARN("failed to release memtable", K(tmp_ret),
        "clog_checkpoint_scn", new_tablet_handle.get_obj()->get_tablet_meta().clog_checkpoint_scn_);
    }
  }
  return ret;
}

// the input ddl sstable is sorted by: slice_idx asc, start_scn asc, end_scn desc
// check continue by slice, every slice should be continue
int ObTabletDDLUtil::check_data_continue(
    ObTableStoreIterator &ddl_sstable_iter,
    bool &is_data_continue,
    share::SCN &compact_start_scn,
    share::SCN &compact_end_scn)
{
  int ret = OB_SUCCESS;
  is_data_continue = false;
  ddl_sstable_iter.resume();
  if (OB_UNLIKELY(!ddl_sstable_iter.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_sstable_iter.count()));
  } else if (1 == ddl_sstable_iter.count()) {
    ObITable *single_table = nullptr;
    if (OB_FAIL(ddl_sstable_iter.get_boundary_table(true/*is_last*/, single_table))) {
      LOG_WARN("get single table failed", K(ret));
    } else {
      is_data_continue = true;
      compact_start_scn = SCN::min(compact_start_scn, single_table->get_start_scn());
      compact_end_scn = SCN::max(compact_end_scn, single_table->get_end_scn());
    }
  } else {
    is_data_continue = true;
    int64_t last_slice_idx = -1;
    SCN last_end_scn = SCN::invalid_scn();
    ObITable *table = nullptr;
    while (OB_SUCC(ret) && is_data_continue) {
      if (OB_FAIL(ddl_sstable_iter.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next table from ddl_sstable_iter failed", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, table is nullptr", K(ret), KPC(table));
      } else if (table->get_slice_idx() < last_slice_idx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl sstable should sorted by slice idx asc", K(ret), K(table->get_key()), K(last_slice_idx));
      } else if (table->get_slice_idx() > last_slice_idx) { // slice idx changed
        last_end_scn = table->get_end_scn();
        last_slice_idx = table->get_slice_idx();
      }
      if (OB_SUCC(ret)) {
        // check scn range continue for each slice
        if (table->get_start_scn() > last_end_scn) {
          is_data_continue = false;
          LOG_INFO("ddl sstable not continue", K(table->get_key()), K(last_end_scn), K(last_slice_idx));
        } else {
          last_end_scn = SCN::max(last_end_scn, table->get_end_scn());
          compact_start_scn = SCN::min(compact_start_scn, table->get_start_scn());
          compact_end_scn = SCN::max(compact_end_scn, table->get_end_scn());
        }
      }
    }
  }
  return ret;
}


int ObTabletDDLUtil::check_data_continue(
    const ObIArray<ObDDLKVHandle> &ddl_kvs,
    bool &is_data_continue,
    share::SCN &compact_start_scn,
    share::SCN &compact_end_scn)
{
  int ret = OB_SUCCESS;
  is_data_continue = false;
  if (OB_UNLIKELY(ddl_kvs.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_kvs.count()));
  } else if (1 == ddl_kvs.count()) {
    is_data_continue = true;
    ObDDLKV *single_kv = ddl_kvs.at(0).get_obj();
    compact_start_scn = SCN::min(compact_start_scn, single_kv->get_start_scn());
    compact_end_scn = SCN::max(compact_end_scn, single_kv->get_end_scn());
  } else {
    ObDDLKVHandle first_kv_handle = ddl_kvs.at(0);
    ObDDLKVHandle last_kv_handle = ddl_kvs.at(ddl_kvs.count() - 1);
    is_data_continue = true;
    SCN last_end_scn = first_kv_handle.get_obj()->get_end_scn();
    for (int64_t i = 1; OB_SUCC(ret) && i < ddl_kvs.count(); ++i) {
      ObDDLKVHandle cur_kv = ddl_kvs.at(i);
      if (OB_ISNULL(cur_kv.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv is null", K(ret), K(i));
      } else if (cur_kv.get_obj()->get_start_scn() <= last_end_scn) {
        last_end_scn = SCN::max(last_end_scn, cur_kv.get_obj()->get_end_scn());
      } else {
        is_data_continue = false;
        LOG_INFO("ddl kv not continue", K(i), K(last_end_scn), KPC(cur_kv.get_obj()));
        break;
      }
    }
    if (OB_SUCC(ret) && is_data_continue) {
      compact_start_scn = SCN::min(compact_start_scn, first_kv_handle.get_obj()->get_start_scn());
      compact_end_scn = SCN::max(compact_end_scn, last_kv_handle.get_obj()->get_end_scn());
    }
  }
  return ret;
}

int ObTabletDDLUtil::prepare_index_data_desc(ObTablet &tablet,
                                             const ObITable::TableKey &table_key,
                                             const int64_t snapshot_version,
                                             const uint64_t data_format_version,
                                             const ObSSTable *first_ddl_sstable,
                                             const ObStorageSchema *storage_schema,
                                             ObWholeDataStoreDesc &data_desc)
{
  int ret = OB_SUCCESS;
  data_desc.reset();
  ObLSService *ls_service = MTL(ObLSService *);
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const int64_t cg_idx = table_key.is_column_store_sstable() ? table_key.get_column_group_id() : -1/*negative value means row store*/;
  const SCN end_scn = table_key.get_end_scn();
  const bool micro_index_clustered = tablet.get_tablet_meta().micro_index_clustered_;
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || snapshot_version <= 0 || data_format_version <= 0 || OB_ISNULL(storage_schema))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(data_format_version), KP(storage_schema));
  } else if (cg_idx >= 0) {
    const ObIArray<ObStorageColumnGroupSchema > &cg_schemas = storage_schema->get_column_groups();
    if (cg_idx >= cg_schemas.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid cg idx", K(ret), K(cg_idx), K(cg_schemas.count()));
    } else if (OB_UNLIKELY(table_key.is_minor_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table key is minor sstable", K(ret), K(table_key));
    } else {
      const ObStorageColumnGroupSchema &cur_cg_schema = cg_schemas.at(cg_idx);
      if (OB_FAIL(data_desc.init(true/*is_ddl*/, *storage_schema, ls_id, tablet_id,
                                 compaction::ObMergeType::MAJOR_MERGE,
                                 snapshot_version, data_format_version,
                                 tablet.get_tablet_meta().micro_index_clustered_,
                                 tablet.get_transfer_seq(),
                                 end_scn, &cur_cg_schema, cg_idx))) {
        LOG_WARN("init data desc for cg failed", K(ret));
      } else {
        data_desc.get_static_desc().micro_index_clustered_ = micro_index_clustered;
        LOG_DEBUG("get data desc from column group schema", K(ret),
                  K(tablet_id), K(cg_idx), K(data_desc), K(cur_cg_schema),
                  K(micro_index_clustered));
      }
    }
  } else if (OB_FAIL(data_desc.init(true/*is_ddl*/,
                                    *storage_schema,
                                    ls_id,
                                    tablet_id,
                                    table_key.is_minor_sstable() ? compaction::MINOR_MERGE : compaction::MAJOR_MERGE,
                                    snapshot_version,
                                    data_format_version,
                                    tablet.get_tablet_meta().micro_index_clustered_,
                                    tablet.get_transfer_seq(),
                                    end_scn))) {
    // use storage schema to init ObDataStoreDesc
    // all cols' default checksum will assigned to 0
    // means all macro should contain all columns in schema
    LOG_WARN("init data store desc failed", K(ret), K(tablet_id));
  } else {
    data_desc.get_static_desc().micro_index_clustered_ = micro_index_clustered;
  }
  if (OB_SUCC(ret) && nullptr != first_ddl_sstable) {
    // use the param in first ddl sstable, which persist the param when ddl start
    ObSSTableMetaHandle meta_handle;
    if (OB_FAIL(first_ddl_sstable->get_meta(meta_handle))) {
      LOG_WARN("get sstable meta handle fail", K(ret), KPC(first_ddl_sstable));
    } else {
      const ObSSTableBasicMeta &basic_meta = meta_handle.get_sstable_meta().get_basic_meta();
      if (OB_FAIL(data_desc.get_desc().update_basic_info_from_macro_meta(basic_meta))) {
        LOG_WARN("failed to update basic info from macro_meta", KR(ret), K(basic_meta));
      }
    }
  }
  LOG_DEBUG("prepare_index_data_desc", K(ret), K(data_desc));
  return ret;
}

int ObTabletDDLUtil::create_ddl_sstable(ObTablet &tablet,
                                        const ObTabletDDLParam &ddl_param,
                                        const ObIArray<ObDDLBlockMeta> &meta_array,
                                        const ObIArray<blocksstable::MacroBlockId> &macro_id_array,
                                        const ObSSTable *first_ddl_sstable,
                                        const ObStorageSchema *storage_schema,
                                        common::ObArenaAllocator &allocator,
                                        ObTableHandleV2 &sstable_handle)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObSSTableIndexBuilder, sstable_index_builder, true /*use buffer*/) {
    ObIndexBlockRebuilder index_block_rebuilder;
    ObWholeDataStoreDesc data_desc;
    int64_t macro_block_column_count = 0;
    if (OB_UNLIKELY(!ddl_param.is_valid() || OB_ISNULL(storage_schema))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(ddl_param), KP(storage_schema));
    } else if (OB_FAIL(ObTabletDDLUtil::prepare_index_data_desc(
            tablet,
            ddl_param.table_key_,
            ddl_param.snapshot_version_,
            ddl_param.data_format_version_,
            first_ddl_sstable,
            storage_schema,
            data_desc))) {
      LOG_WARN("prepare data store desc failed", K(ret), K(ddl_param));
    } else if (FALSE_IT(macro_block_column_count = meta_array.empty() ? 0 : meta_array.at(0).block_meta_->get_meta_val().column_count_)) {
    } else if (meta_array.count() > 0 && OB_FAIL(data_desc.get_col_desc().mock_valid_col_default_checksum_array(macro_block_column_count))) {
      LOG_WARN("mock valid column default checksum failed", K(ret), "firt_macro_block_meta", meta_array.at(0), K(ddl_param));
    } else if (OB_FAIL(sstable_index_builder.init(data_desc.get_desc(),
                                                   ddl_param.table_key_.is_major_sstable() ? ObSSTableIndexBuilder::ENABLE : ObSSTableIndexBuilder::DISABLE))) {
      LOG_WARN("init sstable index builder failed", K(ret), K(data_desc));
    } else if (OB_FAIL(index_block_rebuilder.init(sstable_index_builder,
            nullptr/*task_idx*/,
            ddl_param.table_key_))) {
      LOG_WARN("fail to alloc index builder", K(ret));
    } else if (meta_array.empty()) {
      // do nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < meta_array.count(); ++i) {
        if (OB_FAIL(index_block_rebuilder.append_macro_row(*meta_array.at(i).block_meta_))) {
          LOG_WARN("append block meta failed", K(ret), K(i), KPC(meta_array.at(i).block_meta_));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(index_block_rebuilder.close())) {
        LOG_WARN("close index block rebuilder failed", K(ret));
      } else if (OB_FAIL(ObTabletDDLUtil::create_ddl_sstable(tablet, &sstable_index_builder, macro_id_array, ddl_param, first_ddl_sstable,
              macro_block_column_count, storage_schema, allocator, sstable_handle))) {
        LOG_WARN("create ddl sstable failed", K(ret), K(ddl_param));
      }
    }
  }
  return ret;
}


int ObTabletDDLUtil::create_ddl_sstable(
    ObTablet &tablet,
    ObSSTableIndexBuilder *sstable_index_builder,
    const ObIArray<blocksstable::MacroBlockId> &macro_id_array,
    const ObTabletDDLParam &ddl_param,
    const ObSSTable *first_ddl_sstable,
    const int64_t macro_block_column_count,
    const ObStorageSchema *storage_schema,
    common::ObArenaAllocator &allocator,
    ObTableHandleV2 &sstable_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == sstable_index_builder || !ddl_param.is_valid() || OB_ISNULL(storage_schema))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sstable_index_builder), K(ddl_param), KP(storage_schema));
  } else {
    const int64_t create_schema_version_on_tablet = tablet.get_tablet_meta().create_schema_version_;
    ObTabletCreateSSTableParam param;
    if (OB_FAIL(param.init_for_ddl(sstable_index_builder, ddl_param, first_ddl_sstable,
        *storage_schema, macro_block_column_count, create_schema_version_on_tablet, macro_id_array))) {
      LOG_WARN("fail to init param for ddl",
          K(ret), K(macro_block_column_count), K(create_schema_version_on_tablet),
          KPC(sstable_index_builder), K(ddl_param),
          KPC(first_ddl_sstable), KPC(storage_schema), K(macro_id_array));
    } else if (ddl_param.table_key_.is_co_sstable()) {
      if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(param, allocator, sstable_handle))) {
        LOG_WARN("create sstable failed", K(ret), K(param));
      }
    } else {
      if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(param, allocator, sstable_handle))) {
        LOG_WARN("create sstable failed", K(ret), K(param));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("create ddl sstable success", K(ddl_param), K(sstable_handle),
          "create_schema_version", create_schema_version_on_tablet);
    }
  }
  return ret;
}

int ObTabletDDLUtil::update_ddl_table_store(
    ObLS &ls,
    ObTablet &tablet,
    const ObTabletDDLParam &ddl_param,
    const ObStorageSchema *storage_schema,
    common::ObArenaAllocator &allocator,
    blocksstable::ObSSTable *sstable,
    const ObTablesHandleArray &slice_sstable_handles)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ddl_param.is_valid() || OB_ISNULL(storage_schema)
        || (OB_ISNULL(sstable) && (slice_sstable_handles.empty() || is_incremental_direct_load(ddl_param.direct_load_type_))))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_param), KP(storage_schema), KP(sstable), K(slice_sstable_handles.get_count()));
  } else {
    const bool is_major_sstable = ddl_param.table_key_.is_major_sstable();
    const int64_t rebuild_seq = ls.get_rebuild_seq();
    int64_t snapshot_version = 0;
    int64_t multi_version_start = 0;
    if (is_full_direct_load(ddl_param.direct_load_type_)) {
      snapshot_version = is_major_sstable ? max(ddl_param.snapshot_version_, tablet.get_snapshot_version())
                                          : tablet.get_snapshot_version();
      multi_version_start = is_major_sstable ? max(ddl_param.snapshot_version_, tablet.get_multi_version_start())
                                             : 0;
    } else {
      snapshot_version = max(ddl_param.snapshot_version_, tablet.get_snapshot_version());
      multi_version_start = tablet.get_multi_version_start();
    }
    ObTabletHandle new_tablet_handle;
    ObUpdateTableStoreParam table_store_param(snapshot_version,
                                              multi_version_start,
                                              storage_schema,
                                              rebuild_seq,
                                              sstable);
    if (OB_FAIL(table_store_param.init_with_compaction_info(
            ObCompactionTableStoreParam(is_major_sstable ? compaction::MEDIUM_MERGE : compaction::MINOR_MERGE,
                                        share::SCN::min_scn(),
                                        is_major_sstable /*need_report*/)))) {
      /*DDL does not have verification between replicas,
        So using medium merge to force verification between replicas*/
      LOG_WARN("failed to init with compaction info", KR(ret));
    } else {
      if (is_full_direct_load(ddl_param.direct_load_type_)) { // full direct load
        table_store_param.ddl_info_.update_with_major_flag_ = is_major_sstable;
        table_store_param.ddl_info_.keep_old_ddl_sstable_ = !is_major_sstable;
        table_store_param.ddl_info_.data_format_version_ = ddl_param.data_format_version_;
        table_store_param.ddl_info_.ddl_commit_scn_ = ddl_param.commit_scn_;
        table_store_param.ddl_info_.ddl_checkpoint_scn_ = ddl_param.table_key_.is_ddl_dump_sstable() ? ddl_param.table_key_.get_end_scn() : ddl_param.commit_scn_;
        if (ddl_param.table_key_.is_ddl_dump_sstable()) {
          // data is not complete, now update ddl table store only for reducing count of ddl dump sstable.
          table_store_param.ddl_info_.ddl_replay_status_ = tablet.get_tablet_meta().ddl_replay_status_;
        } else {
          // data is complete, mark ddl replay status finished
          table_store_param.ddl_info_.ddl_replay_status_ = ddl_param.table_key_.is_co_sstable() ? CS_REPLICA_REPLAY_COLUMN_FINISH : CS_REPLICA_REPLAY_ROW_STORE_FINISH;
        }
        bool use_slice_ddl_sstable = !ObDDLUtil::need_rescan_column_store(ddl_param.data_format_version_);
        for (int64_t i = 0; OB_SUCC(ret) && use_slice_ddl_sstable && i < slice_sstable_handles.get_count(); ++i) {
          const ObSSTable *cur_slice_sstable = static_cast<ObSSTable *>(slice_sstable_handles.get_table(i));
          if (OB_ISNULL(cur_slice_sstable)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("slice sstable is null", K(ret), K(i), KP(cur_slice_sstable));
          } else if (OB_FAIL(table_store_param.ddl_info_.slice_sstables_.push_back(cur_slice_sstable))) {
            LOG_WARN("push back slice ddl sstable failed", K(ret), K(i), KPC(cur_slice_sstable));
          }
        }
      } else { // incremental direct load
        table_store_param.compaction_info_.clog_checkpoint_scn_ = sstable->get_end_scn();
        table_store_param.ha_info_.need_check_transfer_seq_ = true;
        table_store_param.ha_info_.transfer_seq_ = tablet.get_tablet_meta().transfer_info_.transfer_seq_;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ls.update_tablet_table_store(ddl_param.table_key_.get_tablet_id(), table_store_param, new_tablet_handle))) {
        LOG_WARN("failed to update tablet table store", K(ret), K(ddl_param.table_key_), K(table_store_param));
      } else {
        FLOG_INFO("ddl update table store success", K(ddl_param), KPC(new_tablet_handle.get_obj()), K(table_store_param));
      }
    }
  }
  return ret;
}

int get_sstables(ObTableStoreIterator &ddl_sstable_iter,
                 const int64_t start_slice_idx,
                 const int64_t end_slice_idx,
                 const int64_t merged_sliced_idx,
                 const int64_t cg_idx,
                 ObIArray<ObSSTable *> &target_sstables,
                 ObIArray<ObStorageMetaHandle> &meta_handles)
{
  int ret = OB_SUCCESS;
  ddl_sstable_iter.resume();
  if (OB_UNLIKELY(!(start_slice_idx < 0 && end_slice_idx < 0)
        && !(start_slice_idx >= 0 && start_slice_idx <= end_slice_idx))) {
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
    } else if (cg_idx < 0) { // row store
      if (OB_FAIL(target_sstables.push_back(static_cast<ObSSTable *>(table)))) {
        LOG_WARN("push back target sstable failed", K(ret));
      }
    } else if (!table->is_co_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current table not co sstable", K(ret), KPC(table));
    } else {
      ObCOSSTableV2 *cur_co_sstable = static_cast<ObCOSSTableV2 *>(table);
      ObSSTableWrapper cg_sstable_wrapper;
      ObSSTable *cg_sstable = nullptr;
      if (OB_ISNULL(cur_co_sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current co sstable is null", K(ret), KP(cur_co_sstable));
      } else if (!(start_slice_idx < 0 && end_slice_idx < 0) // negative means all, not satisfied
          // the slice idx range of current table is between start_slice_idx and end_slice_idx, not satisfied
          && !(start_slice_idx <= cur_co_sstable->get_key().slice_range_.start_slice_idx_ && cur_co_sstable->get_key().slice_range_.end_slice_idx_ <= end_slice_idx)) {
        // skip
      } else if (cur_co_sstable->is_cgs_empty_co_table()) {
        // skip
      } else if (OB_FAIL(cur_co_sstable->fetch_cg_sstable(cg_idx, cg_sstable_wrapper))) {
        LOG_WARN("get all tables failed", K(ret));
      } else if (OB_FAIL(cg_sstable_wrapper.get_loaded_column_store_sstable(cg_sstable))) {
        LOG_WARN("get sstable failed", K(ret));
      } else if (OB_ISNULL(cg_sstable)) {
        // skip
      } else if (merged_sliced_idx > 0 && cg_sstable->get_slice_idx() < merged_sliced_idx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should not exit sstable whose slice idx less than merged slice idx", K(ret), K(merged_sliced_idx), K(cg_sstable->get_key()));
      } else if (OB_FAIL(target_sstables.push_back(cg_sstable))) {
        LOG_WARN("push back cg sstable failed", K(ret));
      } else if (cg_sstable_wrapper.get_meta_handle().is_valid()
          && OB_FAIL(meta_handles.push_back(cg_sstable_wrapper.get_meta_handle()))) {
        LOG_WARN("push back meta handle failed", K(ret));
      }
    }
  }
  return ret;
}

int get_sstables(const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
                 const int64_t start_slice_idx,
                 const int64_t end_slice_idx,
                 const int64_t merged_sliced_idx,
                 const int64_t cg_idx,
                 ObIArray<ObSSTable *> &target_sstables)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!(start_slice_idx < 0 && end_slice_idx < 0)
        && !(start_slice_idx >= 0 && start_slice_idx <= end_slice_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_slice_idx), K(end_slice_idx));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < frozen_ddl_kvs.count(); ++i) {
    ObDDLKV *cur_kv = frozen_ddl_kvs.at(i).get_obj();
    if (OB_ISNULL(cur_kv)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (cg_idx < 0) { // row store
      ObDDLMemtable *target_sstable = nullptr;
      if (cur_kv->get_ddl_memtables().empty()) {
        // do nothing
      } else if (OB_ISNULL(target_sstable = cur_kv->get_ddl_memtables().at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current sstable is null", K(ret), KPC(cur_kv), K(target_sstable));
      } else if (OB_FAIL(target_sstables.push_back(target_sstable))) {
        LOG_WARN("push back target sstable failed", K(ret));
      }
    } else {
      const ObIArray<ObDDLMemtable *> &ddl_memtables = cur_kv->get_ddl_memtables();
      for (int64_t i = 0; OB_SUCC(ret) && i < ddl_memtables.count(); ++i) {
        ObDDLMemtable *cur_sstable = ddl_memtables.at(i);
        if (OB_ISNULL(cur_sstable)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("current ddl memtable is null", K(ret), KP(cur_sstable));
        } else if (cur_sstable->get_key().column_group_idx_ != cg_idx) {
          // skip
        } else if (merged_sliced_idx > 0 && cur_sstable->get_slice_idx() <= merged_sliced_idx) {
          // skip
        } else if ((start_slice_idx < 0 && end_slice_idx < 0) // negative means all
            // the slice idx range of current table is between start_slice_idx and end_slice_idx
            || (start_slice_idx <= cur_sstable->get_key().slice_range_.start_slice_idx_ && cur_sstable->get_key().slice_range_.end_slice_idx_ <= end_slice_idx)) {
          if (OB_FAIL(target_sstables.push_back(cur_sstable))) {
            LOG_WARN("push back target sstable failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

ObDDLMacroBlockIterator::ObDDLMacroBlockIterator()
  : is_inited_(false), sstable_(nullptr), allocator_(nullptr), macro_block_iter_(nullptr), sec_meta_iter_(nullptr)
{

}

ObDDLMacroBlockIterator::~ObDDLMacroBlockIterator()
{
  if ((nullptr != macro_block_iter_ || nullptr != sec_meta_iter_) && OB_ISNULL(allocator_)) {
    int ret = OB_ERR_SYS;
    LOG_ERROR("the iterator is allocated, but allocator is null", K(ret), KP(macro_block_iter_), KP(allocator_));
  } else if (nullptr != macro_block_iter_) {
    macro_block_iter_->~ObIMacroBlockIterator();
    allocator_->free(macro_block_iter_);
    macro_block_iter_ = nullptr;
  } else if (nullptr != sec_meta_iter_) {
    sec_meta_iter_->~ObSSTableSecMetaIterator();
    allocator_->free(sec_meta_iter_);
    sec_meta_iter_ = nullptr;
  }
}

int ObDDLMacroBlockIterator::open(ObSSTable *sstable, const ObDatumRange &query_range, const ObITableReadInfo &read_info, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(nullptr == sstable || !query_range.is_valid() || !read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(sstable), K(query_range), K(read_info));
  } else if (sstable->is_ddl_mem_sstable()) { // ddl mem, scan keybtree
    ObDDLMemtable *ddl_memtable = static_cast<ObDDLMemtable *>(sstable);
    if (OB_ISNULL(ddl_memtable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl memtable cast failed", K(ret));
    } else if (OB_FAIL(ddl_memtable->get_block_meta_tree()->get_keybtree().set_key_range(
            ddl_iter_,
            ObDatumRowkeyWrapper(&query_range.get_start_key(), &read_info.get_datum_utils()),
            query_range.is_left_open(),
            ObDatumRowkeyWrapper(&query_range.get_end_key(), &read_info.get_datum_utils()),
            query_range.is_right_open()))) {
      LOG_WARN("ddl memtable locate range failed", K(ret));
    }
  } else if (sstable->is_ddl_merge_sstable()) { // co ddl partial data, need scan macro block
    if (OB_FAIL(sstable->scan_macro_block(
            query_range,
            read_info,
            allocator,
            macro_block_iter_,
            false/*is_reverse_scan*/,
            false/*need_record_micro_info*/,
            true/*need_scan_sec_meta*/))) {
      LOG_WARN("scan macro block iterator open failed", K(ret));
    }
  } else {
    ObSSTableSecMetaIterator *sec_meta_iter;
    if (OB_ISNULL(sec_meta_iter = OB_NEWx(ObSSTableSecMetaIterator, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for sec meta iterator failed", K(ret));
    } else if (OB_FAIL(sec_meta_iter->open(query_range, ObMacroBlockMetaType::DATA_BLOCK_META, *sstable, read_info, allocator))) {
      LOG_WARN("open sec meta iterator failed", K(ret));
      sec_meta_iter->~ObSSTableSecMetaIterator();
      allocator.free(sec_meta_iter);
    } else {
      sec_meta_iter_ = sec_meta_iter;
    }
  }
  if (OB_SUCC(ret)) {
    sstable_ = sstable;
    allocator_ = &allocator;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLMacroBlockIterator::get_next(ObDataMacroBlockMeta &data_macro_meta, int64_t &end_row_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (sstable_->is_ddl_mem_sstable()) {
    ObDatumRowkeyWrapper tree_key;
    ObBlockMetaTreeValue *tree_value = nullptr;
    if (OB_FAIL(ddl_iter_.get_next(tree_key, tree_value))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next tree value failed", K(ret));
      }
    } else if (OB_FAIL(data_macro_meta.assign(*tree_value->block_meta_))) {
      LOG_WARN("assign block meta failed", K(ret));
    } else {
      end_row_offset = tree_value->co_sstable_row_offset_;
    }
  } else if (sstable_->is_ddl_merge_sstable()) {
    ObMacroBlockDesc block_desc;
    block_desc.macro_meta_ = &data_macro_meta;
    if (OB_FAIL(macro_block_iter_->get_next_macro_block(block_desc))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next macro block failed", K(ret));
      }
    } else {
      end_row_offset = block_desc.start_row_offset_ + block_desc.row_count_ - 1;
    }
  } else {
    if (OB_FAIL(sec_meta_iter_->get_next(data_macro_meta))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get data macro meta failed", K(ret));
      }
    } else {
      end_row_offset = -1;
    }
  }
  return ret;
}


 // for cg sstable, endkey is end row id, confirm read_info not used
int get_sorted_meta_array(
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
    HEAP_VAR(ObBlockMetaTree, meta_tree) {
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
        if (meta_tree.is_valid() && OB_FAIL(meta_tree.get_sorted_meta_array(sorted_metas))) {
          LOG_WARN("get sorted meta array failed", K(ret));
        } else {
          meta_tree.destroy();
          last_slice_idx = cur_sstable->get_slice_idx();
        }
      }
      if (OB_FAIL(ret)) {
      } else if (cur_sstable->is_empty()) {
        // do nothing, skip
      } else if (!meta_tree.is_valid() && OB_FAIL(meta_tree.init(tablet, ddl_param.table_key_, ddl_param.start_scn_, ddl_param.data_format_version_, storage_schema))) {
        LOG_WARN("init meta tree failed", K(ret), K(ddl_param));
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
            ObDDLMacroHandle macro_handle;
            bool is_exist = false;
            if (OB_FAIL(meta_tree.exist(&data_macro_meta.end_key_, is_exist))) {
              LOG_WARN("check block meta exist failed", K(ret), K(data_macro_meta));
            } else if (is_exist) {
              // skip
              FLOG_INFO("append meta tree skip", K(ret), "table_key", cur_sstable->get_key(), "macro_block_id", data_macro_meta.get_macro_id(),
                  "data_checksum", data_macro_meta.val_.data_checksum_, K(meta_tree.get_macro_block_cnt()), "macro_block_end_key", data_macro_meta.end_key_);
            } else if (OB_FAIL(macro_handle.set_block_id(data_macro_meta.get_macro_id()))) {
              LOG_WARN("hold macro block failed", K(ret));
            } else if (OB_FAIL(data_macro_meta.deep_copy(copied_meta, allocator))) {
              LOG_WARN("deep copy macro block meta failed", K(ret));
            } else if (OB_FAIL(meta_tree.insert_macro_block(macro_handle, &copied_meta->end_key_, copied_meta, end_row_offset))) {
              LOG_WARN("insert meta tree failed", K(ret), K(macro_handle), KPC(copied_meta));
              copied_meta->~ObDataMacroBlockMeta();
            } else {
              FLOG_INFO("append meta tree success", K(ret), "table_key", cur_sstable->get_key(), "macro_block_id", data_macro_meta.get_macro_id(),
                  "data_checksum", copied_meta->val_.data_checksum_, K(meta_tree.get_macro_block_cnt()), "macro_block_end_key", copied_meta->end_key_,
                  "end_row_offset", end_row_offset);
            }
          }
        }
        ObCStringHelper helper;
        LOG_INFO("append meta tree finished", K(ret), "table_key", cur_sstable->get_key(), "data_macro_block_cnt_in_sstable", cur_sstable->get_data_macro_block_count(),
            K(meta_tree.get_macro_block_cnt()), "sstable_end_key", OB_ISNULL(copied_meta) ? "NOT_EXIST": helper.convert(copied_meta->end_key_), "end_row_offset", end_row_offset);
      }
    }
    if (OB_SUCC(ret) && meta_tree.is_valid()) { // save sorted_metas from last meta tree
      if (OB_FAIL(meta_tree.get_sorted_meta_array(sorted_metas))) {
        LOG_WARN("get sorted meta array failed", K(ret));
      }
    }
    } // heap var meta_tree
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


int compact_sstables(
    ObTablet &tablet,
    ObIArray<ObSSTable *> &sstables,
    const ObTabletDDLParam &ddl_param,
    const ObITableReadInfo &read_info,
    const ObStorageSchema *storage_schema,
    ObArenaAllocator &allocator,
    ObTableHandleV2 &sstable_handle)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena("compact_sst", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObArray<ObDDLBlockMeta> sorted_metas;
  if (OB_FAIL(get_sorted_meta_array(tablet, ddl_param, storage_schema, sstables, read_info, arena, sorted_metas))) {
    LOG_WARN("get sorted meta array failed", K(ret), K(read_info), K(sstables));
  } else if (OB_FAIL(ObTabletDDLUtil::create_ddl_sstable(
          tablet,
          ddl_param,
          sorted_metas,
          ObArray<MacroBlockId>(),
          (sstables.empty() || is_incremental_direct_load(ddl_param.direct_load_type_)) ? nullptr : sstables.at(0)/*first ddl sstable*/,
          storage_schema,
          allocator,
          sstable_handle))) {
    LOG_WARN("create sstable failed", K(ret), K(ddl_param), K(sstables));
  }
  LOG_DEBUG("compact_sstables", K(ret), K(sstables), K(ddl_param), K(read_info), KPC(sstable_handle.get_table()));
  return ret;
}

int ObTabletDDLUtil::get_compact_meta_array(
    ObTablet &tablet,
    ObIArray<ObSSTable *> &sstables,
    const ObTabletDDLParam &ddl_param,
    const ObITableReadInfo &read_info,
    const ObStorageSchema *storage_schema,
    common::ObArenaAllocator &allocator,
    ObArray<ObDDLBlockMeta> &sorted_metas)
{
  int ret = OB_SUCCESS;
  sorted_metas.reset();
  if (OB_FAIL(get_sorted_meta_array(tablet, ddl_param, storage_schema, sstables, read_info, allocator, sorted_metas))) {
    LOG_WARN("get sorted meta array failed", K(ret), K(read_info), K(sstables));
  }
  return ret;
}

int get_merged_slice_idx(ObTableStoreIterator &ddl_sstable_iter, int64_t &merged_slice_idx)
{
  int ret = OB_SUCCESS;
  merged_slice_idx = -1;
  ObITable *first_table = nullptr;
  if (ddl_sstable_iter.count() > 0) {
    if (OB_FAIL(ddl_sstable_iter.get_boundary_table(false/*is_last*/, first_table))) {
      LOG_WARN("get first table failed", K(ret));
    } else if (first_table->get_key().slice_range_.is_merge_slice()) {
      merged_slice_idx = first_table->get_key().get_slice_idx();
    }
  }
  return ret;
}

int compact_co_ddl_sstable(
    ObTablet &tablet,
    ObTableStoreIterator &ddl_sstable_iter,
    const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
    const ObTabletDDLParam &ddl_param,
    const int64_t start_slice_idx,
    const int64_t end_slice_idx,
    const ObStorageSchema *storage_schema,
    common::ObArenaAllocator &allocator,
    ObTablesHandleArray &compacted_cg_sstable_handles,
    ObTableHandleV2 &co_sstable_handle)
{
  int ret = OB_SUCCESS;
  co_sstable_handle.reset();
  const ObITableReadInfo *cg_index_read_info = nullptr;
  if (OB_UNLIKELY(ddl_sstable_iter.count() == 0 && frozen_ddl_kvs.count() == 0) || OB_ISNULL(storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_sstable_iter.count()), K(frozen_ddl_kvs.count()), KP(storage_schema));
  } else {
    const int64_t base_cg_idx = ddl_param.table_key_.get_column_group_id();
    ObArray<ObSSTable *> base_sstables;
    ObArray<ObStorageMetaHandle> meta_handles; // hold loaded cg sstable
    ObTabletDDLParam cg_ddl_param = ddl_param;
    // negative slice_idx means all slice, ignored in table_key
    cg_ddl_param.table_key_.slice_range_.start_slice_idx_ = MAX(0, start_slice_idx);
    cg_ddl_param.table_key_.slice_range_.end_slice_idx_ = MAX(0, end_slice_idx);
    bool need_fill_cg_sstables = true;
    int64_t merged_slice_idx = -1;
    if (OB_FAIL(get_merged_slice_idx(ddl_sstable_iter, merged_slice_idx))) {
      LOG_WARN("get merged slice idx failed", K(ret));
    } else if (OB_FAIL(get_sstables(ddl_sstable_iter, start_slice_idx, end_slice_idx, merged_slice_idx, base_cg_idx, base_sstables, meta_handles))) {
      LOG_WARN("get base sstable from ddl sstables failed", K(ret), K(ddl_sstable_iter), K(base_cg_idx));
    } else if (OB_FAIL(get_sstables(frozen_ddl_kvs, start_slice_idx, end_slice_idx, merged_slice_idx, base_cg_idx, base_sstables))) {
      LOG_WARN("get base sstable from ddl kv array failed", K(ret), K(frozen_ddl_kvs), K(base_cg_idx));
    } else if (OB_FAIL(compact_sstables(tablet, base_sstables, cg_ddl_param, tablet.get_rowkey_read_info(), storage_schema, allocator, co_sstable_handle))) {
      LOG_WARN("compact base sstable failed", K(ret));
    } else {
      // empty major co sstable, no need fill cg sstables
      need_fill_cg_sstables = !(co_sstable_handle.get_table()->is_major_sstable() && static_cast<ObCOSSTableV2 *>(co_sstable_handle.get_table())->is_cgs_empty_co_table());
    }
    if (OB_SUCC(ret) && need_fill_cg_sstables) {
      ObTablesHandleArray cg_sstable_handles;
      if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(cg_index_read_info))) {
        LOG_WARN("failed to get index read info from ObTenantCGReadInfoMgr", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < storage_schema->get_column_group_count(); ++i) {
        const int64_t cur_cg_idx = i;
        ObArray<ObSSTable *> cur_cg_sstables;
        meta_handles.reset();
        ObTableHandleV2 target_table_handle;
        cg_ddl_param.table_key_.table_type_ = ObITable::TableType::DDL_MERGE_CO_SSTABLE == ddl_param.table_key_.table_type_
          ? ObITable::TableType::DDL_MERGE_CG_SSTABLE : ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE;
        cg_ddl_param.table_key_.column_group_idx_ = cur_cg_idx;
        if (cur_cg_idx == base_cg_idx) {
          // do nothing
        } else if (OB_FAIL(get_sstables(ddl_sstable_iter, start_slice_idx, end_slice_idx, merged_slice_idx, cur_cg_idx, cur_cg_sstables, meta_handles))) {
          LOG_WARN("get current cg sstables failed", K(ret));
        } else if (OB_FAIL(get_sstables(frozen_ddl_kvs, start_slice_idx, end_slice_idx, merged_slice_idx, cur_cg_idx, cur_cg_sstables))) {
          LOG_WARN("get current cg sstables failed", K(ret));
        } else if (OB_FAIL(compact_sstables(tablet, cur_cg_sstables, cg_ddl_param, *cg_index_read_info, storage_schema, allocator, target_table_handle))) {
          LOG_WARN("compact cg sstable failed", K(ret), K(cur_cg_idx), K(cur_cg_sstables.count()), K(cg_ddl_param), KPC(cg_index_read_info));
        } else if (OB_FAIL(cg_sstable_handles.add_table(target_table_handle))) {
          LOG_WARN("push back cg sstable failed", K(ret), K(i), KP(target_table_handle.get_table()));
        } else if (OB_FAIL(compacted_cg_sstable_handles.add_table(target_table_handle))) {
          LOG_WARN("push back compacted cg sstable failed", K(ret), K(i), KP(target_table_handle.get_table()));
        }
      }
      if (OB_SUCC(ret)) { // assemble the cg sstables into co sstable
        ObArray<ObITable *> cg_sstables;
        if (OB_FAIL(cg_sstable_handles.get_tables(cg_sstables))) {
          LOG_WARN("get cg sstables failed", K(ret));
        } else if (OB_FAIL(static_cast<ObCOSSTableV2 *>(co_sstable_handle.get_table())->fill_cg_sstables(cg_sstables))) {
          LOG_WARN("fill cg sstables failed", K(ret));
        }
      }
    }
  }
  LOG_INFO("compact_co_ddl_sstable", K(ret), K(ddl_param), K(start_slice_idx), K(end_slice_idx), K(ddl_sstable_iter.count()), KP(&tablet), KPC(co_sstable_handle.get_table()));
  return ret;
}

int get_ddl_memtables(const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs, ObIArray<const ObSSTable *> &all_ddl_memtables)
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

int get_slice_indexes(const ObIArray<const ObSSTable *> &ddl_sstables, hash::ObHashSet<int64_t> &slice_idxes)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ddl_sstables.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_sstables.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ddl_sstables.count(); ++i) {
      const ObSSTable *cur_sstable = ddl_sstables.at(i);
      if (OB_ISNULL(cur_sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl memtable is null", K(ret), KP(cur_sstable), K(i));
      } else if (OB_FAIL(slice_idxes.set_refactored(cur_sstable->get_key().get_slice_idx(), 1/*over_write*/))) {
        LOG_WARN("put slice index into set failed", K(ret), K(cur_sstable->get_key()));
      }
    }
  }
  return ret;
}

int get_merge_slice_idx(const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs, int64_t &merge_slice_idx)
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

int compact_slice_ddl_sstable(
    ObTablet &tablet,
    ObTableStoreIterator &ddl_sstable_iter,
    const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
    const ObTabletDDLParam &ddl_param,
    const ObStorageSchema *storage_schema,
    common::ObArenaAllocator &allocator,
    ObTablesHandleArray &compacted_cg_sstable_handles,
    ObTableHandleV2 &co_sstable_handle,
    ObTablesHandleArray &slice_sstable_handles)
{
  int ret = OB_SUCCESS;
  compacted_cg_sstable_handles.reset();
  co_sstable_handle.reset();
  slice_sstable_handles.reset();
  const ObITableReadInfo *cg_index_read_info = nullptr;
  if (OB_UNLIKELY(ddl_sstable_iter.count() == 0 && frozen_ddl_kvs.count() == 0) || OB_ISNULL(storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_sstable_iter.count()), K(frozen_ddl_kvs.count()), KP(storage_schema));
  } else if (ddl_param.table_key_.is_ddl_merge_sstable()) { // dump ddl kv into slice sstable
    // get slice_idx set from ddl kv
    // for each slice_idx
    //    get ddl memtables from ddl kv
    //    try to get slice sstable from table store
    //    create new slice sstable
    // put all slice sstable into table store
    ObArray<const ObSSTable*> ddl_sstables;
    hash::ObHashSet<int64_t> slice_idxes;
    bool is_data_from_start = (SCN::scn_dec(ddl_param.start_scn_) == ddl_param.table_key_.scn_range_.start_scn_);
    int64_t merge_slice_idx = 0;
    if (is_data_from_start && OB_FAIL(get_merge_slice_idx(frozen_ddl_kvs, merge_slice_idx))) {
      LOG_WARN("get merge slice idx failed", K(ret));
    } else if (OB_FAIL(slice_idxes.create(1007, ObMemAttr(MTL_ID(), "slice_idx_set")))) {
      LOG_WARN("create slice index set failed", K(ret));
    } else if (OB_FAIL(get_ddl_memtables(frozen_ddl_kvs, ddl_sstables))) {
      LOG_WARN("get ddl memtables failed", K(ret), K(frozen_ddl_kvs));
    } else if (OB_FAIL(get_slice_indexes(ddl_sstables, slice_idxes))) { // get slice idx from ddl memtable only
      LOG_WARN("get slice indexes failed", K(ret));
    }
    if (OB_SUCC(ret) && merge_slice_idx > 0) {
      ObTableHandleV2 slice_co_sstable;
      const int64_t start_slice_idx = 0; // only support merge slice from the start now
      if (OB_FAIL(compact_co_ddl_sstable(tablet, ddl_sstable_iter, frozen_ddl_kvs, ddl_param, start_slice_idx, merge_slice_idx, storage_schema, allocator, compacted_cg_sstable_handles, slice_co_sstable))) {
        LOG_WARN("compact slice co sstable failed", K(ret), K(start_slice_idx), K(merge_slice_idx));
      } else if (OB_FAIL(slice_sstable_handles.add_table(slice_co_sstable))) {
        LOG_WARN("add slice co sstable failed", K(ret), K(slice_co_sstable));
      }
    }
    for (hash::ObHashSet<int64_t>::const_iterator it = slice_idxes.begin(); OB_SUCC(ret) && it != slice_idxes.end(); ++it) {
      const int64_t cur_slice_idx = it->first;
      ObTableHandleV2 slice_co_sstable;
      if (merge_slice_idx > 0 && cur_slice_idx <= merge_slice_idx) {
        // skip
      } else if (OB_FAIL(compact_co_ddl_sstable(tablet, ddl_sstable_iter, frozen_ddl_kvs, ddl_param, cur_slice_idx, cur_slice_idx, storage_schema, allocator, compacted_cg_sstable_handles, slice_co_sstable))) {
        LOG_WARN("compact slice co sstable failed", K(ret), K(cur_slice_idx));
      } else if (OB_FAIL(slice_sstable_handles.add_table(slice_co_sstable))) {
        LOG_WARN("add slice co sstable failed", K(ret), K(slice_co_sstable));
      }
    }
  } else if (ddl_param.table_key_.is_major_sstable()) { // merge ddl kv into major sstable
    const int64_t slice_idx = -1; // negative means all slice
    if (OB_FAIL(compact_co_ddl_sstable(tablet, ddl_sstable_iter, frozen_ddl_kvs, ddl_param, slice_idx, slice_idx, storage_schema, allocator, compacted_cg_sstable_handles, co_sstable_handle))) {
      LOG_WARN("compact slice co sstable failed", K(ret), K(slice_idx));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table key", K(ret), K(ddl_param));
  }
  return ret;
}

int compact_ro_ddl_sstable(
    ObTablet &tablet,
    ObTableStoreIterator &ddl_sstable_iter,
    const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
    const ObTabletDDLParam &ddl_param,
    const ObStorageSchema *storage_schema,
    common::ObArenaAllocator &allocator,
    ObTableHandleV2 &ro_sstable_handle)
{
  int ret = OB_SUCCESS;
  ro_sstable_handle.reset();
  if (OB_UNLIKELY(ddl_sstable_iter.count() == 0 && frozen_ddl_kvs.count() == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_sstable_iter.count()), K(frozen_ddl_kvs.count()));
  } else {
    const int64_t slice_idx = 0;
    const int64_t merge_slice_idx = -1;
    const int64_t base_cg_idx = -1; // negative value means row store
    ObArray<ObSSTable *> base_sstables;
    ObArray<ObStorageMetaHandle> meta_handles; // hold loaded cg sstable, dummy here
    if (OB_FAIL(get_sstables(ddl_sstable_iter, slice_idx, slice_idx, merge_slice_idx,  base_cg_idx, base_sstables, meta_handles))) {
      LOG_WARN("get base sstable from ddl sstables failed", K(ret), K(ddl_sstable_iter), K(base_cg_idx));
    } else if (OB_FAIL(get_sstables(frozen_ddl_kvs, slice_idx, slice_idx, merge_slice_idx, base_cg_idx, base_sstables))) {
      LOG_WARN("get base sstable from ddl kv array failed", K(ret), K(frozen_ddl_kvs), K(base_cg_idx));
    } else if (OB_FAIL(compact_sstables(tablet, base_sstables, ddl_param, tablet.get_rowkey_read_info(), storage_schema, allocator, ro_sstable_handle))) {
      LOG_WARN("compact base sstable failed", K(ret));
    }
  }
  LOG_INFO("compact_ro_ddl_sstable", K(ret), K(ddl_sstable_iter), K(ddl_param), KP(&tablet), KPC(ro_sstable_handle.get_table()));
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int compact_ss_ddl_sstable(
    ObTablet &tablet,
    ObTableStoreIterator &ddl_sstable_iter,
    const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
    const ObTabletDDLParam &ddl_param,
    const ObStorageSchema *storage_schema,
    common::ObArenaAllocator &allocator,
    ObTableHandleV2 &ro_sstable_handle)
{
  int ret = OB_SUCCESS;
  ro_sstable_handle.reset();
  ObArray<MacroBlockId> macro_id_array;
  hash::ObHashSet<MacroBlockId, hash::NoPthreadDefendMode> macro_id_set;
  if (OB_UNLIKELY(ddl_sstable_iter.count() == 0 && frozen_ddl_kvs.count() == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_sstable_iter.count()), K(frozen_ddl_kvs.count()));
  } else if (OB_FAIL(macro_id_set.create(1023, ObMemAttr(MTL_ID(), "ss_ddl_id_arr")))) {
    LOG_WARN("create set of macro block id failed", K(ret));
  } else {
    // 1. get macro id from ddl sstable
    ddl_sstable_iter.resume();
    ObSSTableMetaHandle meta_handle;
    ObMacroIdIterator macro_id_iter;
    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      ObSSTable *sstable = nullptr;
      meta_handle.reset();
      macro_id_iter.reset();
      if (OB_FAIL(ddl_sstable_iter.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next table failed", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable()) || OB_ISNULL(sstable = static_cast<ObSSTable *>(table))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, table is nullptr", K(ret), KPC(table), KPC(sstable));
      } else if (OB_FAIL(sstable->get_meta(meta_handle))) {
        LOG_WARN("get sstable meta handle failed", K(ret));
      } else if (OB_FAIL(meta_handle.get_sstable_meta().get_macro_info().get_other_block_iter(macro_id_iter))) {
        LOG_WARN("get other block iterator failed", K(ret));
      } else {
        MacroBlockId macro_id;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(macro_id_iter.get_next_macro_id(macro_id))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next macro id failed", K(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(macro_id_set.set_refactored(macro_id))) {
            LOG_WARN("set macro id failed", K(ret), K(macro_id));
          }
        }
      }
    }

    // 2. get macro block id from ddl kv
    for (int64_t i = 0; OB_SUCC(ret) && i < frozen_ddl_kvs.count(); ++i) {
      ObDDLKV *cur_kv = frozen_ddl_kvs.at(i).get_obj();
      ObDDLMemtable *ddl_memtable = nullptr;
      ObArray<MacroBlockId> macro_id_array;
      if (OB_ISNULL(cur_kv)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (cur_kv->get_ddl_memtables().empty()) {
        // do nothing
      } else if (OB_ISNULL(ddl_memtable = cur_kv->get_ddl_memtables().at(0))) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(ddl_memtable->get_block_meta_tree()->get_macro_id_array(macro_id_array))) {
        LOG_WARN("get macro id array failed", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < macro_id_array.count(); ++j) {
          if (OB_FAIL(macro_id_set.set_refactored(macro_id_array.at(j)))) {
            LOG_WARN("set macro id failed", K(ret), K(macro_id_array.at(j)));
          }
        }
      }
    }

    // 3. persist macro id set into other_block_ids of compacted sstable
    if (OB_SUCC(ret)) {
      ObArray<ObDDLBlockMeta> empty_meta_array;
      hash::ObHashSet<MacroBlockId, hash::NoPthreadDefendMode>::iterator iter = macro_id_set.begin();
      if (OB_FAIL(macro_id_array.reserve(macro_id_set.size()))) {
        LOG_WARN("reserve macro id array failed", K(ret));
      }
      for (; OB_SUCC(ret) && iter != macro_id_set.end(); ++iter) {
        const MacroBlockId &cur_id = iter->first;
        if (OB_FAIL(macro_id_array.push_back(cur_id))) {
          LOG_WARN("push back macro id failed", K(ret), K(cur_id));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObTabletDDLUtil::create_ddl_sstable(tablet, ddl_param, empty_meta_array, macro_id_array,
                nullptr/*first ddl sstable*/, storage_schema, allocator, ro_sstable_handle))) {
          LOG_WARN("create sstable failed", K(ret), K(ddl_param));
        }
      }
    }

  }
  LOG_INFO("compact_ss_ddl_sstable", K(ret), K(ddl_param), K(ddl_sstable_iter), K(frozen_ddl_kvs), K(macro_id_array.count()), K(macro_id_array));
  return ret;
}
#endif

int ObTabletDDLUtil::compact_ddl_kv(
    ObLS &ls,
    ObTablet &tablet,
    ObTableStoreIterator &ddl_sstable_iter,
    const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
    const ObTabletDDLParam &ddl_param,
    common::ObArenaAllocator &allocator,
    ObTableHandleV2 &compacted_sstable_handle)
{
  int ret = OB_SUCCESS;
  compacted_sstable_handle.reset();
  ObArenaAllocator arena("compact_ddl_kv", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTablesHandleArray compacted_cg_sstable_handles; // for tmp hold handle of macro block until the tablet updated
  ObTablesHandleArray slice_sstable_handles; // for tmp hold handle of macro block until the tablet updated
  ObStorageSchema *storage_schema = nullptr;
  if (OB_UNLIKELY(!ddl_param.is_valid() || (0 == ddl_sstable_iter.count() && frozen_ddl_kvs.empty()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_param), K(ddl_sstable_iter.count()), K(frozen_ddl_kvs.count()));
  } else if (OB_FAIL(tablet.load_storage_schema(arena, storage_schema))) {
    LOG_WARN("load storage schema failed", K(ret), K(ddl_param));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < frozen_ddl_kvs.count(); ++i) {
      if (OB_FAIL(frozen_ddl_kvs.at(i).get_obj()->close())) {
        LOG_WARN("close ddl kv failed", K(ret), K(i));
      }
    }

#ifdef ERRSIM
    if (OB_SUCC(ret) && ddl_param.table_key_.is_major_sstable()) {
      ret = OB_E(EventTable::EN_DDL_COMPACT_FAIL) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        LOG_WARN("errsim compact ddl sstable failed", KR(ret));
      }
    }
#endif

    if (OB_SUCC(ret) && is_incremental_direct_load(ddl_param.direct_load_type_)) {
      if (OB_FAIL(update_storage_schema(tablet, ddl_param, arena, storage_schema, frozen_ddl_kvs))) {
        LOG_WARN("fail to update storage schema", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
#ifdef OB_BUILD_SHARED_STORAGE
    } else if (GCTX.is_shared_storage_mode() && !is_incremental_direct_load(ddl_param.direct_load_type_)) {
      if (OB_FAIL(compact_ss_ddl_sstable(tablet, ddl_sstable_iter, frozen_ddl_kvs, ddl_param, storage_schema, allocator, compacted_sstable_handle))) {
        LOG_WARN("compact ddl sstable on shared storage mode", K(ret), K(ddl_param));
      }
#endif
    } else if (ddl_param.table_key_.is_co_sstable()) {
      if (ObDDLUtil::need_rescan_column_store(ddl_param.data_format_version_)) {
        const int64_t slice_idx = 0;
        if (OB_FAIL(compact_co_ddl_sstable(tablet, ddl_sstable_iter, frozen_ddl_kvs, ddl_param, slice_idx, slice_idx, storage_schema, allocator, compacted_cg_sstable_handles, compacted_sstable_handle))) {
          LOG_WARN("compact co ddl sstable failed", K(ret), K(ddl_param));
        }
      } else {
        if (OB_FAIL(compact_slice_ddl_sstable(tablet, ddl_sstable_iter, frozen_ddl_kvs, ddl_param, storage_schema, allocator, compacted_cg_sstable_handles, compacted_sstable_handle, slice_sstable_handles))) {
          LOG_WARN("compact sliced ddl sstable failed", K(ret), K(ddl_param));
        }
      }
    } else {
      if (OB_FAIL(compact_ro_ddl_sstable(tablet, ddl_sstable_iter, frozen_ddl_kvs, ddl_param, storage_schema, allocator, compacted_sstable_handle))) {
        LOG_WARN("compact co ddl sstable failed", K(ret), K(ddl_param));
      }
    }
    if (OB_SUCC(ret)) { // update table store
      if (OB_FAIL(update_ddl_table_store(ls, tablet, ddl_param, storage_schema, allocator, static_cast<ObSSTable *>(compacted_sstable_handle.get_table()), slice_sstable_handles))) {
        LOG_WARN("update ddl table store failed", K(ret));
      } else {
        LOG_INFO("compact ddl sstable success", K(ddl_param));
      }
    }
  }
  ObTabletObjLoadHelper::free(arena, storage_schema);
  return ret;
}

int get_schema_info_from_ddl_kvs(
  ObTablet &tablet,
  const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
  const int64_t column_cnt_in_schema,
  int64_t &max_column_cnt_in_memtable,
  int64_t &max_schema_version_in_memtable)
{
  int ret = OB_SUCCESS;
  int64_t max_column_cnt_on_recorder = 0;
  ObDDLKV *ddl_kv = nullptr;
  for (int i = frozen_ddl_kvs.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    const ObDDLKVHandle &ddl_kv_handle = frozen_ddl_kvs.at(i);
    if (OB_ISNULL(ddl_kv = ddl_kv_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ddl kv is null", KR(ret), K(ddl_kv_handle));
    } else if (OB_FAIL(ddl_kv->get_schema_info(column_cnt_in_schema,
                                               max_schema_version_in_memtable,
                                               max_column_cnt_in_memtable))) {
      LOG_WARN("fail to get schema info from ddl kv", KR(ret), KPC(ddl_kv));
    }
  }
  if (FAILEDx(tablet.get_max_column_cnt_on_schema_recorder(max_column_cnt_on_recorder))) {
    LOG_WARN("fail to get max column cnt on schema recorder", KR(ret));
  } else {
    max_column_cnt_in_memtable = MAX(max_column_cnt_in_memtable, max_column_cnt_on_recorder);
  }
  return ret;
}

int ObTabletDDLUtil::update_storage_schema(
    ObTablet &tablet,
    const ObTabletDDLParam &ddl_param,
    ObArenaAllocator &allocator,
    ObStorageSchema *&storage_schema,
    const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == storage_schema || frozen_ddl_kvs.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(storage_schema), K(frozen_ddl_kvs));
  } else if (OB_UNLIKELY(!is_incremental_direct_load(ddl_param.direct_load_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not incremental direct load", KR(ret), K(ddl_param));
  } else {
    ObStorageSchema *schema_on_tablet = storage_schema;
    int64_t column_cnt_in_schema = 0;
    int64_t max_column_cnt_in_memtable = 0;
    int64_t max_schema_version_in_memtable = 0;
    bool column_info_simplified = false;
    if (OB_FAIL(schema_on_tablet->get_store_column_count(column_cnt_in_schema, true/*full_col*/))) {
      LOG_WARN("fail to get storage column count", KR(ret));
    } else if (OB_FAIL(get_schema_info_from_ddl_kvs(tablet,
                                                    frozen_ddl_kvs,
                                                    column_cnt_in_schema,
                                                    max_column_cnt_in_memtable,
                                                    max_schema_version_in_memtable))) {
      LOG_WARN("fail to get schema info from ddl kvs", KR(ret));
    } else if (FALSE_IT(column_info_simplified =
                          max_column_cnt_in_memtable > column_cnt_in_schema)) {
      // can't get new added column info from memtable, need simplify column info
    } else if (column_info_simplified ||
               max_schema_version_in_memtable > schema_on_tablet->get_schema_version()) {
      // need alloc new storage schema & set column cnt
      ObStorageSchema *new_storage_schema = nullptr;
      if (OB_FAIL(ObStorageSchemaUtil::alloc_storage_schema(allocator, new_storage_schema))) {
        LOG_WARN("failed to alloc storage schema", K(ret));
      } else if (OB_FAIL(new_storage_schema->init(allocator, *schema_on_tablet, column_info_simplified))) {
        LOG_WARN("fail to init storage schema", K(ret), K(schema_on_tablet));
        ObStorageSchemaUtil::free_storage_schema(allocator, new_storage_schema);
        new_storage_schema = nullptr;
      } else {
        // only update column cnt by memtable, use schema version on tablet_schema
        new_storage_schema->column_cnt_ = MAX(new_storage_schema->column_cnt_, max_column_cnt_in_memtable);
        new_storage_schema->store_column_cnt_ = MAX(column_cnt_in_schema, max_column_cnt_in_memtable);
        new_storage_schema->schema_version_ = MAX(max_schema_version_in_memtable, schema_on_tablet->get_schema_version());
        storage_schema = new_storage_schema;
      }
    }
    if (OB_SUCC(ret)) {
      FLOG_INFO("get storage schema to merge", KPC(storage_schema), KPC(schema_on_tablet),
                K(max_column_cnt_in_memtable), K(max_schema_version_in_memtable));
      if (schema_on_tablet != storage_schema) {
        ObStorageSchemaUtil::free_storage_schema(allocator, schema_on_tablet);
        schema_on_tablet = nullptr;
      }
    }
  }
  return ret;
}

int check_ddl_sstable_expired(const SCN &ddl_start_scn, ObTableStoreIterator &ddl_sstable_iter)
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  ObSSTable *ddl_sstable = nullptr;
  ObSSTableMetaHandle meta_handle;
  if (0 == ddl_sstable_iter.count()) {
    // do nothing
  } else if (OB_FAIL(ddl_sstable_iter.get_boundary_table(false, table))) {
    LOG_WARN("get first ddl sstable failed", K(ret));
  } else if (OB_ISNULL(ddl_sstable = static_cast<ObSSTable *>(table))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl sstable is null", K(ret), KPC(table));
  } else if (OB_FAIL(ddl_sstable->get_meta(meta_handle))) {
    LOG_WARN("get meta handle failed", K(ret));
  } else if (meta_handle.get_sstable_meta().get_ddl_scn() < ddl_start_scn) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("ddl sstable is expired", K(ret), K(meta_handle.get_sstable_meta()), K(ddl_start_scn));
  }
  return ret;
}

int check_ddl_kv_expired(const SCN &ddl_start_scn, const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs)
{
  int ret = OB_SUCCESS;
  ObDDLKV *ddl_kv = nullptr;
  if (frozen_ddl_kvs.empty()) {
    // do nothing
  } else if (OB_ISNULL(ddl_kv = frozen_ddl_kvs.at(0).get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl kv is null", K(ret), K(frozen_ddl_kvs));
  } else if (ddl_kv->get_ddl_start_scn() < ddl_start_scn) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("ddl sstable is expired", K(ret), KPC(ddl_kv), K(ddl_start_scn));
  }
  return ret;
}

int ObTabletDDLUtil::get_compact_scn(
    const SCN &ddl_start_scn,
    ObTableStoreIterator &ddl_sstable_iter,
    const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
    SCN &compact_start_scn,
    SCN &compact_end_scn)
{
  int ret = OB_SUCCESS;
  bool is_data_continue = true;
  compact_start_scn = SCN::max_scn();
  compact_end_scn = SCN::min_scn();
  SCN ddl_sstables_start_scn = SCN::max_scn();
  SCN ddl_sstables_end_scn = SCN::min_scn();
  SCN ddl_kvs_start_scn = SCN::max_scn();
  SCN ddl_kvs_end_scn = SCN::min_scn();
  ddl_sstable_iter.resume();
  if (OB_UNLIKELY((0 == ddl_sstable_iter.count() && frozen_ddl_kvs.empty()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_sstable_iter.count()), K(frozen_ddl_kvs.count()));
  } else if (OB_FAIL(check_ddl_sstable_expired(ddl_start_scn, ddl_sstable_iter))) {
    LOG_WARN("check ddl sstable expired failed", K(ret), K(ddl_start_scn), K(ddl_sstable_iter));
  } else if (ddl_sstable_iter.count() > 0 && OB_FAIL(check_data_continue(ddl_sstable_iter, is_data_continue, ddl_sstables_start_scn, ddl_sstables_end_scn))) {
    LOG_WARN("check ddl sstable continue failed", K(ret));
  } else if (!is_data_continue) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl sstable not continuous", K(ret), K(ddl_sstable_iter));
  } else if (OB_FAIL(check_ddl_kv_expired(ddl_start_scn, frozen_ddl_kvs))) {
    LOG_WARN("check ddl kv expired failed", K(ret), K(ddl_start_scn), K(frozen_ddl_kvs));
  } else if (frozen_ddl_kvs.count() > 0 && OB_FAIL(check_data_continue(frozen_ddl_kvs, is_data_continue, ddl_kvs_start_scn, ddl_kvs_end_scn))) {
    LOG_WARN("check ddl sstable continue failed", K(ret));
  } else if (!is_data_continue) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl kv not continuous", K(ret), K(frozen_ddl_kvs));
  } else {
    if (ddl_sstable_iter.count() > 0 && frozen_ddl_kvs.count() > 0) {
      // |___________________________________________________|
      // ddl_sstables_start_scn                              ddl_sstables_end_scn
      //                                 |____________________________________________________________|
      //                                 ddl_kvs_start_scn                                            ddl_kvs_end_scn
      is_data_continue = ddl_kvs_start_scn >= ddl_sstables_start_scn
        && ddl_kvs_start_scn <= ddl_sstables_end_scn
        && ddl_kvs_end_scn >= ddl_sstables_end_scn;
      if (!is_data_continue) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("scn range not continue between ddl sstable iter and ddl kv array", K(ret), K(ddl_sstables_start_scn), K(ddl_sstables_end_scn), K(ddl_kvs_start_scn), K(ddl_kvs_end_scn), K(ddl_sstable_iter), K(frozen_ddl_kvs));
      } else {
        compact_start_scn = ddl_sstables_start_scn;
        compact_end_scn = ddl_kvs_end_scn;
      }
    } else if (ddl_sstable_iter.count() > 0) {
      compact_start_scn = ddl_sstables_start_scn;
      compact_end_scn = ddl_sstables_end_scn;
    } else if (frozen_ddl_kvs.count() > 0) {
      compact_start_scn = ddl_kvs_start_scn;
      compact_end_scn = ddl_kvs_end_scn;
    }
    LOG_INFO("get compact scn", K(ret), K(compact_start_scn), K(compact_end_scn), K(ddl_sstable_iter), K(frozen_ddl_kvs));
  }
  return ret;
}

int ObTabletDDLUtil::report_ddl_checksum(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const uint64_t table_id,
    const int64_t execution_id,
    const int64_t ddl_task_id,
    const int64_t *column_checksums,
    const int64_t column_count,
    const uint64_t data_format_version)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  const uint64_t tenant_id = MTL_ID();
  if (OB_UNLIKELY(!tablet_id.is_valid() || OB_INVALID_ID == ddl_task_id
        || !is_valid_id(table_id) || 0 == table_id || execution_id < 0 || nullptr == column_checksums || column_count <= 0 || data_format_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(table_id), K(execution_id), KP(column_checksums), K(column_count), K(data_format_version));
  } else if (!is_valid_tenant_id(tenant_id) || OB_ISNULL(sql_proxy) || OB_ISNULL(schema_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("ls service or sql proxy is null", K(ret), K(tenant_id), KP(sql_proxy), KP(schema_service));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_INFO("table not exit", K(ret), K(tenant_id), K(table_id));
    ret = OB_TASK_EXPIRED; // for ignore warning
  } else if (OB_FAIL(DDL_SIM(tenant_id, ddl_task_id, REPORT_DDL_CHECKSUM_FAILED))) {
    LOG_WARN("ddl sim failure", K(tenant_id), K(ddl_task_id));
  } else {
    ObArray<ObColDesc> column_ids;
    ObArray<ObDDLChecksumItem> ddl_checksum_items;
    if (OB_FAIL(table_schema->get_multi_version_column_descs(column_ids))) {
      LOG_WARN("fail to get column ids", K(ret), K(tablet_id));
    } else if (OB_UNLIKELY(column_count > column_ids.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect error, column checksums count larger than column ids count", K(ret),
          K(tablet_id), K(column_count), K(column_ids.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      share::ObDDLChecksumItem item;
      item.execution_id_ = execution_id;
      item.tenant_id_ = tenant_id;
      item.table_id_ = table_id;
      item.tablet_id_ = tablet_id.id();
      item.ddl_task_id_ = ddl_task_id;
      item.column_id_ = column_ids.at(i).col_id_;
      item.task_id_ = tablet_id.id();
      item.checksum_ = column_checksums[i];
#ifdef ERRSIM
      if (OB_SUCC(ret)) {
        ret = OB_E(EventTable::EN_HIDDEN_CHECKSUM_DDL_TASK) OB_SUCCESS;
        // set the checksum of the second column inconsistent with the report checksum of data table. (report_ddl_column_checksum())
        if (OB_FAIL(ret) && 17 == item.column_id_) {
          item.checksum_ = i + 100;
        }
      }
#endif
      if (item.column_id_ >= OB_MIN_SHADOW_COLUMN_ID ||
          item.column_id_ == OB_HIDDEN_TRANS_VERSION_COLUMN_ID ||
          item.column_id_ == OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID) {
        continue;
      } else if (OB_FAIL(ddl_checksum_items.push_back(item))) {
        LOG_WARN("push back column checksum item failed", K(ret));
      }
    }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_DDL_REPORT_CHECKSUM_FAIL) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        LOG_WARN("errsim report checksum failed", KR(ret));
      }
    }
#endif
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDDLChecksumOperator::update_checksum(data_format_version, ddl_checksum_items, *sql_proxy))) {
      LOG_WARN("fail to update checksum", K(ret), K(tablet_id), K(table_id), K(ddl_checksum_items));
    } else {
      LOG_INFO("report ddl checkum success", K(tablet_id), K(table_id), K(execution_id), K(ddl_checksum_items), K(common::lbt()));
    }
  }
  return ret;
}

int ObTabletDDLUtil::check_and_get_major_sstable(const share::ObLSID &ls_id,
                                                 const ObTabletID &tablet_id,
                                                 const blocksstable::ObSSTable *&first_major_sstable,
                                                 ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  first_major_sstable = nullptr;
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               tablet_id,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(nullptr == tablet_handle.get_obj())) {
    ret = OB_ERR_SYS;
    LOG_WARN("tablet handle is null", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    first_major_sstable = static_cast<ObSSTable *>(
        table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/));
  }
  return ret;
}

int ObTabletDDLUtil::freeze_ddl_kv(const ObDDLTableMergeDagParam &param)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObArray<ObDDLKVHandle> ddl_kvs_handle;
  ObDDLTableMergeTask *merge_task = nullptr;
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  ObTabletDirectLoadMgrHandle tablet_mgr_hdl;
  if (OB_FAIL(ls_service->get_ls(param.ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(param));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               param.tablet_id_,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet failed", K(ret), K(param));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(param));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl kv mgr not exist", K(ret), K(param));
    } else {
      LOG_WARN("get ddl kv mgr failed", K(ret), K(param));
    }
  } else if (is_full_direct_load(param.direct_load_type_)
      && param.start_scn_ < tablet_handle.get_obj()->get_tablet_meta().ddl_start_scn_) {
    ret = OB_TASK_EXPIRED;
    LOG_WARN("ddl task expired, skip it", K(ret), K(param), "new_start_scn", tablet_handle.get_obj()->get_tablet_meta().ddl_start_scn_);
  // TODO(cangdi): do not freeze when ddl kv's min scn >= rec_scn
  } else if (!ddl_kv_mgr_handle.get_obj()->can_freeze()) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_WARN("cannot freeze now", K(param));
    }
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->freeze_ddl_kv(
      param.start_scn_, param.snapshot_version_, param.data_format_version_))) {
    LOG_WARN("ddl kv manager try freeze failed", K(ret), K(param));
  }
  return ret;
}


#ifdef OB_BUILD_SHARED_STORAGE
int ObTabletDDLUtil::schedule_ddl_minor_merge_on_demand(
      const bool need_freeze,
      const share::ObLSID &ls_id,
      ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  ObDDLTableMergeDagParam param;
  ObArray<ObDDLKVHandle> ddl_kvs_handle;
  if (OB_UNLIKELY(!ls_id.is_valid() || !ddl_kv_mgr_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(ddl_kv_mgr_handle));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(!need_freeze/*frozen_only*/, ddl_kvs_handle))) {
    LOG_WARN("get freezed ddl kv failed", K(ret));
  } else if (ddl_kvs_handle.empty()) {
    LOG_TRACE("empty ddl kv", "tablet_id", ddl_kv_mgr_handle.get_obj()->get_tablet_id());
  } else if (need_freeze && OB_FAIL(ddl_kv_mgr_handle.get_obj()->freeze_ddl_kv(
          ddl_kvs_handle.at(0).get_obj()->get_ddl_start_scn(),
          ddl_kvs_handle.at(0).get_obj()->get_snapshot_version(),
          ddl_kvs_handle.at(0).get_obj()->get_data_format_version()))) {
    LOG_WARN("failed to freeze kv", K(ret), "tablet_id", ddl_kv_mgr_handle.get_obj()->get_tablet_id());
  } else {
    param.direct_load_type_    = DIRECT_LOAD_DDL_V2;
    param.ls_id_               = ls_id;
    param.tablet_id_           = ddl_kv_mgr_handle.get_obj()->get_tablet_id();
    param.start_scn_           = ddl_kvs_handle.at(0).get_obj()->get_ddl_start_scn();
    param.snapshot_version_    = ddl_kvs_handle.at(0).get_obj()->get_snapshot_version();
    param.data_format_version_ = ddl_kvs_handle.at(0).get_obj()->get_data_format_version();
    if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("schedule ddl merge dag failed", K(ret), K(param));
      }
    }
  }
  LOG_TRACE("schedule tablet ddl minor merge", K(ret), K(param));
  return ret;
}
#endif

} // namespace storage
} // namespace oceanbase
