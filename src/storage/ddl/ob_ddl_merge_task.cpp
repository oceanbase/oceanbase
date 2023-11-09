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
#include "share/scn.h"
#include "share/ob_ddl_checksum.h"
#include "share/ob_get_compat_mode.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/blocksstable/ob_sstable_sec_meta_iterator.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/ls/ob_ls.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "share/schema/ob_multi_version_schema_service.h"

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
  ObTablesHandleArray ddl_kvs_handle;
  ObDDLTableMergeTask *merge_task = nullptr;
  if (OB_FAIL(ddl_param_.ddl_kv_mgr_handle_.get_obj()->get_ddl_kvs(true/*frozen_only*/, ddl_kvs_handle))) {
    LOG_WARN("get freezed ddl kv failed", K(ret), K(ddl_param_));
  } else if (OB_FAIL(alloc_task(merge_task))) {
    LOG_WARN("Fail to alloc task", K(ret), K(ddl_param_));
  } else if (OB_FAIL(merge_task->init(ddl_param_))) {
    LOG_WARN("failed to init ddl table merge task", K(ret), K(*this));
  } else if (OB_FAIL(add_task(*merge_task))) {
    LOG_WARN("Fail to add task", K(ret), K(ddl_param_));
  } else {
    // use chain task to ensure log ts continuious in table store
    ObDDLTableDumpTask *last_task = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < ddl_kvs_handle.get_count(); ++i) {
      ObDDLKV *ddl_kv = static_cast<ObDDLKV *>(ddl_kvs_handle.get_table(i));
      ObDDLTableDumpTask *task = nullptr;
      if (OB_ISNULL(ddl_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get ddl kv failed", K(ret), K(i));
      } else if (OB_FAIL(alloc_task(task))) {
        LOG_WARN("Fail to alloc task", K(ret));
      } else if (OB_FAIL(task->init(ddl_param_.ls_id_,
                                    ddl_param_.tablet_id_,
                                    ddl_kv->get_freeze_scn()))) {
        LOG_WARN("failed to init ddl dump task", K(ret), K(ddl_param_), K(ddl_kv->get_freeze_scn()));
      } else if (OB_FAIL(add_task(*task))) {
        LOG_WARN("Fail to add task", K(ret), K(ddl_param_));
      } else {
        if (nullptr != last_task && OB_FAIL(last_task->add_child(*task))) {
          LOG_WARN("add child task failed", K(ret), K(ddl_param_));
        }
        last_task = task;
      }
    }
    if (OB_SUCC(ret)) {
      if (nullptr != last_task && OB_FAIL(last_task->add_child(*merge_task))) {
        LOG_WARN("add child merge task failed", K(ret), K(ddl_param_));
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
      && ddl_param_.ls_id_ == other_dag.ddl_param_.ls_id_;
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
                                  static_cast<int64_t>(ddl_param_.rec_scn_.get_val_for_inner_table_field())))) {
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

/******************             ObDDLTableDumpTask             *****************/
ObDDLTableDumpTask::ObDDLTableDumpTask()
  : ObITask(ObITaskType::TASK_TYPE_DDL_KV_DUMP),
    is_inited_(false), ls_id_(), tablet_id_(), freeze_scn_(SCN::min_scn())
{

}

ObDDLTableDumpTask::~ObDDLTableDumpTask()
{
}

int ObDDLTableDumpTask::init(const share::ObLSID &ls_id,
                             const ObTabletID &tablet_id,
                             const SCN &freeze_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(tablet_id_));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || !freeze_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(freeze_scn));
  } else {
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    freeze_scn_ = freeze_scn;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLTableDumpTask::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("ddl dump task start process", K(*this));
  ObTabletHandle tablet_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObLSHandle ls_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               tablet_id_,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id_));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl kv mgr not exist", K(ret), K(ls_id_), K(tablet_id_));
    } else {
      LOG_WARN("get ddl kv mgr failed", K(ret), K(ls_id_), K(tablet_id_));
    }
  } else {
    ObTableHandleV2 ddl_kv_handle;
    ObDDLKV *ddl_kv = nullptr;
    ObTablesHandleArray ddl_sstable_handles;
    bool need_compact = false;
    ObArray<ObITable *> candidate_sstables;
    if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_freezed_ddl_kv(freeze_scn_, ddl_kv_handle))) {
      LOG_WARN("get ddl kv handle failed", K(ret), K(freeze_scn_));
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_NEED_RETRY; // dag is async, the ddl kv may be dumped, no need record in dag warning history
      }
    } else if (OB_ISNULL(ddl_kv = static_cast<ObDDLKV *>(ddl_kv_handle.get_table()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get ddl kv failed", K(ret));
    } else if (OB_FAIL(ddl_kv->close(*tablet_handle.get_obj()))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("close ddl kv failed", K(ret));
      }
    } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->release_ddl_kvs(freeze_scn_))) {
      LOG_WARN("release ddl kv failed", K(ret), K(freeze_scn_));
    }
  }
  return ret;
}

ObDDLTableMergeTask::ObDDLTableMergeTask()
  : ObITask(ObITaskType::TASK_TYPE_DDL_KV_MERGE),
    is_inited_(false), merge_param_()
{

}

ObDDLTableMergeTask::~ObDDLTableMergeTask()
{
}

int ObDDLTableMergeTask::init(const ObDDLTableMergeDagParam &ddl_dag_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(merge_param_));
  } else if (OB_UNLIKELY(!ddl_dag_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_dag_param));
  } else {
    merge_param_ = ddl_dag_param;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLTableMergeTask::process()
{
  int ret = OB_SUCCESS;
  int64_t MAX_DDL_SSTABLE = ObTabletDDLKvMgr::MAX_DDL_KV_CNT_IN_STORAGE * 0.5;
#ifdef ERRSIM
  if (0 != GCONF.errsim_max_ddl_sstable_count) {
    MAX_DDL_SSTABLE = GCONF.errsim_max_ddl_sstable_count;
    LOG_INFO("set max ddl sstable in errsim mode", K(MAX_DDL_SSTABLE));
  }
#endif
  LOG_INFO("ddl merge task start process", K(*this));
  ObTabletHandle tablet_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObLSHandle ls_handle;
  ObTableStoreIterator ddl_table_iter;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const uint64_t tenant_id = MTL_ID();
  common::ObArenaAllocator allocator("DDLMergeTask");
  ObSSTable compact_sstable;
  ObSSTable *sstable = nullptr;
  bool skip_major_process = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(merge_param_.ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(merge_param_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               merge_param_.tablet_id_,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("failed to get tablet", K(ret), K(merge_param_));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl kv mgr not exist", K(ret), K(merge_param_));
    } else {
      LOG_WARN("get ddl kv mgr failed", K(ret), K(merge_param_));
    }
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_sstables(ddl_table_iter))) {
    LOG_WARN("get ddl sstable handles failed", K(ret));
  } else if (ddl_table_iter.count() >= MAX_DDL_SSTABLE || merge_param_.is_commit_) {
    DEBUG_SYNC(BEFORE_DDL_TABLE_MERGE_TASK);
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
    if (OB_FAIL(ObTabletDDLUtil::check_and_get_major_sstable(
        merge_param_.ls_id_, merge_param_.tablet_id_, first_major_sstable, table_store_wrapper))) {
      LOG_WARN("check if major sstable exist failed", K(ret));
    } else if (nullptr != first_major_sstable) {
      LOG_INFO("major sstable has been created before", K(merge_param_), K(ddl_param.table_key_));
      if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
        LOG_WARN("failed to fetch table store", K(ret));
      } else {
        sstable = static_cast<ObSSTable *>(
            table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/));
      }
    } else if (tablet_handle.get_obj()->get_tablet_meta().table_store_flag_.with_major_sstable()) {
      skip_major_process = true;
      LOG_INFO("tablet me says with major but no major, meaning its a migrated deleted tablet, skip");
    } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_param(ddl_param))) {
      LOG_WARN("get tablet ddl param failed", K(ret));
      if (OB_STATE_NOT_MATCH == ret) {
        ret = OB_NEED_RETRY;
      }
    } else if (merge_param_.start_scn_ > SCN::min_scn() && merge_param_.start_scn_ < ddl_param.start_scn_) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl merge task expired, do nothing", K(merge_param_), "new_start_scn", ddl_param.start_scn_);
    } else if (OB_FAIL(ObTabletDDLUtil::compact_ddl_sstable(*tablet_handle.get_obj(),
                                                            ddl_table_iter,
                                                            tablet_handle.get_obj()->get_rowkey_read_info(),
                                                            merge_param_.is_commit_,
                                                            merge_param_.rec_scn_,
                                                            ddl_param,
                                                            allocator,
                                                            compact_sstable))) {
      LOG_WARN("compact sstables failed", K(ret));
    } else {
      sstable = &compact_sstable;
    }

    if (OB_SUCC(ret) && merge_param_.rec_scn_.is_valid_and_not_min()) {
      // when the ddl dag is self scheduled when ddl kv is full, the rec_scn is invalid
      // but no worry, the formmer ddl dump task will also release ddl kvs
      if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->release_ddl_kvs(merge_param_.rec_scn_))) {
        LOG_WARN("release ddl kv failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && merge_param_.is_commit_) {
      if (skip_major_process) {
      } else if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl major sstable is null", K(ret), K(ddl_param));
      } else if (OB_FAIL(GCTX.ob_service_->submit_tablet_update_task(tenant_id, merge_param_.ls_id_, merge_param_.tablet_id_))) {
        LOG_WARN("fail to submit tablet update task", K(ret), K(tenant_id), K(merge_param_));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->set_commit_success(merge_param_.start_scn_))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("set is commit success failed", K(ret));
        }
      } else {
        LOG_INFO("commit ddl sstable succ", K(ddl_param), K(merge_param_));
      }
    }
  }
  return ret;
}

// the input ddl sstable is sorted with start_scn
int ObTabletDDLUtil::check_data_integrity(ObTableStoreIterator &ddl_sstable_iter,
                                          const SCN &start_scn,
                                          const SCN &prepare_scn,
                                          bool &is_data_complete)
{
  int ret = OB_SUCCESS;
  is_data_complete = false;
  if (OB_UNLIKELY(!start_scn.is_valid_and_not_min() || !prepare_scn.is_valid_and_not_min()
      || prepare_scn <= start_scn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_sstable_iter.count()), K(start_scn), K(prepare_scn));
  } else if (0 == ddl_sstable_iter.count()) {
    is_data_complete = false;
  } else {
    ObITable *first_ddl_sstable = nullptr;
    ObITable *last_ddl_sstable = nullptr;
    if (OB_FAIL(ddl_sstable_iter.get_boundary_table(false, first_ddl_sstable))) {
      LOG_WARN("fail to get first ddl sstable", K(ret));
    } else if (OB_FAIL(ddl_sstable_iter.get_boundary_table(true, last_ddl_sstable))) {
      LOG_WARN("fail to get last ddl sstable", K(ret));
    } else if (first_ddl_sstable->get_start_scn() != SCN::scn_dec(start_scn)) {
      LOG_INFO("start log ts not match", K(first_ddl_sstable->get_key()), K(start_scn));
    } else if (last_ddl_sstable->get_end_scn() != prepare_scn) {
      LOG_INFO("prepare log ts not match", K(last_ddl_sstable->get_key()), K(prepare_scn));
    } else if (1 == ddl_sstable_iter.count()) {
      // Only one ddl table, skip
      is_data_complete = true;
    } else {
      is_data_complete = true;
      SCN last_end_scn = first_ddl_sstable->get_end_scn();
      ObITable *cur_ddl_sstable = nullptr;
      while (OB_SUCC(ddl_sstable_iter.get_next(cur_ddl_sstable))) {
        if (OB_ISNULL(cur_ddl_sstable) || OB_UNLIKELY(!cur_ddl_sstable->is_sstable())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, table is nullptr", K(ret), KPC(cur_ddl_sstable));
        } else if (cur_ddl_sstable->get_start_scn() <= last_end_scn) {
          last_end_scn = SCN::max(last_end_scn, cur_ddl_sstable->get_end_scn());
        } else {
          is_data_complete = false;
          LOG_INFO("ddl sstable not continue", K(cur_ddl_sstable->get_key()), K(last_end_scn));
          break;
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

ObTabletDDLParam::ObTabletDDLParam()
  : tenant_id_(0), ls_id_(), table_key_(), start_scn_(SCN::min_scn()), commit_scn_(SCN::min_scn()), snapshot_version_(0), data_format_version_(0)
{

}

ObTabletDDLParam::~ObTabletDDLParam()
{

}

bool ObTabletDDLParam::is_valid() const
{
  return tenant_id_ > 0  && tenant_id_ != OB_INVALID_ID
    && ls_id_.is_valid()
    && table_key_.is_valid()
    && start_scn_.is_valid_and_not_min()
    && commit_scn_.is_valid() && commit_scn_ != SCN::max_scn()
    && snapshot_version_ > 0
    && data_format_version_ >= 0;
}

int ObTabletDDLUtil::prepare_index_data_desc(ObTablet &tablet,
                                             const int64_t snapshot_version,
                                             const int64_t data_format_version,
                                             const ObSSTable *first_ddl_sstable,
                                             ObDataStoreDesc &data_desc)
{
  int ret = OB_SUCCESS;
  data_desc.reset();
  ObLSService *ls_service = MTL(ObLSService *);
  ObArenaAllocator tmp_arena("DDLIdxDescTmp");
  const ObStorageSchema *storage_schema = nullptr;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || snapshot_version <= 0 || data_format_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(data_format_version));
  } else if (OB_FAIL(tablet.load_storage_schema(tmp_arena, storage_schema))) {
    LOG_WARN("fail to get storage schema", K(ret));
  } else if (OB_FAIL(data_desc.init_as_index(*storage_schema,
                                    ls_id,
                                    tablet_id,
                                    MAJOR_MERGE,
                                    snapshot_version,
                                    data_format_version))) {
    // use storage schema to init ObDataStoreDesc
    // all cols' default checksum will assigned to 0
    // means all macro should contain all columns in schema
    LOG_WARN("init data store desc failed", K(ret), K(tablet_id));
  } else {
    if (nullptr != first_ddl_sstable) {
      // use the param in first ddl sstable, which persist the param when ddl start
      ObSSTableMetaHandle meta_handle;
      if (OB_FAIL(first_ddl_sstable->get_meta(meta_handle))) {
        LOG_WARN("get sstable meta handle fail", K(ret), KPC(first_ddl_sstable));
      } else {
        const ObSSTableBasicMeta &basic_meta = meta_handle.get_sstable_meta().get_basic_meta();
        data_desc.row_store_type_ = basic_meta.root_row_store_type_;
        data_desc.compressor_type_ = basic_meta.compressor_type_;
        data_desc.master_key_id_ = basic_meta.master_key_id_;
        data_desc.encrypt_id_ = basic_meta.encrypt_id_;
        data_desc.encoder_opt_.set_store_type(basic_meta.root_row_store_type_);
        MEMCPY(data_desc.encrypt_key_, basic_meta.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
        data_desc.need_prebuild_bloomfilter_ = false;

        data_desc.full_stored_col_cnt_ = basic_meta.column_cnt_;
        while (data_desc.col_default_checksum_array_.count() > basic_meta.column_cnt_) {
          data_desc.col_default_checksum_array_.pop_back();
        }
      }
    } else {
      int64_t column_count = 0;
      if (OB_FAIL(storage_schema->get_stored_column_count_in_sstable(column_count))) {
        LOG_WARN("fail to get stored column count in sstable", K(ret));
      } else {
        data_desc.full_stored_col_cnt_ = column_count;
        while (data_desc.col_default_checksum_array_.count() > column_count) {
          data_desc.col_default_checksum_array_.pop_back();
        }
      }

    }
    data_desc.is_ddl_ = true;
  }
  ObTablet::free_storage_schema(tmp_arena, storage_schema);
  return ret;
}

int ObTabletDDLUtil::try_get_first_ddl_sstable(ObTablet &tablet,
                                               blocksstable::ObSSTable *&first_sstable)
{
  int ret = OB_SUCCESS;
  first_sstable = nullptr;
  ObTableStoreIterator ddl_table_iter;

  ObITable *first_ddl_sstable = nullptr;
  if (OB_FAIL(tablet.get_ddl_sstables(ddl_table_iter))) {
    LOG_WARN("get ddl sstable handles failed", K(ret));
  } else if (ddl_table_iter.count() > 0) {
    if (OB_FAIL(ddl_table_iter.get_boundary_table(false/*is_last*/, first_ddl_sstable))) {
      LOG_WARN("failed to get boundary table", K(ret));
    } else if (OB_ISNULL(first_ddl_sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("first_ddl_sstable must not null", K(ret));
    }
  }

  if (OB_NOT_NULL(first_ddl_sstable)) {
    first_sstable = static_cast<ObSSTable *>(first_ddl_sstable);
  }
  return ret;
}

int ObTabletDDLUtil::create_ddl_sstable(ObTablet &tablet,
                                        const ObTabletDDLParam &ddl_param,
                                        const ObIArray<const ObDataMacroBlockMeta *> &meta_array,
                                        const ObSSTable *first_ddl_sstable,
                                        common::ObArenaAllocator &allocator,
                                        blocksstable::ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena("DdlCreateSST", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  void *buf = nullptr;
  ObSSTableIndexBuilder *sstable_index_builder = nullptr;
  ObIndexBlockRebuilder *index_block_rebuilder = nullptr;
  ObDataStoreDesc data_desc;
  if (OB_UNLIKELY(!ddl_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_param));
  } else if (OB_FAIL(ObTabletDDLUtil::prepare_index_data_desc(tablet,
                                                              ddl_param.table_key_.version_range_.snapshot_version_,
                                                              ddl_param.data_format_version_,
                                                              first_ddl_sstable,
                                                              data_desc))) {
      LOG_WARN("prepare data store desc failed", K(ret), K(ddl_param));
  } else if (OB_ISNULL(buf = arena.alloc(sizeof(ObSSTableIndexBuilder)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for sstable index builder failed", K(ret));
  } else if (FALSE_IT(sstable_index_builder = new (buf) ObSSTableIndexBuilder)) {
  } else if (OB_FAIL(sstable_index_builder->init(data_desc,
                                                 nullptr, // macro block flush callback
                                                 ddl_param.table_key_.is_major_sstable() ? ObSSTableIndexBuilder::AUTO : ObSSTableIndexBuilder::DISABLE))) {
    LOG_WARN("init sstable index builder failed", K(ret), K(data_desc));
  } else if (OB_ISNULL(buf = arena.alloc(sizeof(ObIndexBlockRebuilder)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (FALSE_IT(index_block_rebuilder = new (buf) ObIndexBlockRebuilder)) {
  } else if (OB_FAIL(index_block_rebuilder->init(*sstable_index_builder))) {
    LOG_WARN("fail to alloc index builder", K(ret));
  } else if (meta_array.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_array.count(); ++i) {
      if (OB_FAIL(index_block_rebuilder->append_macro_row(*meta_array.at(i)))) {
        LOG_WARN("append block meta failed", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(index_block_rebuilder->close())) {
      LOG_WARN("close index block rebuilder failed", K(ret));
    } else if (OB_FAIL(ObTabletDDLUtil::create_ddl_sstable(tablet, sstable_index_builder, ddl_param, first_ddl_sstable, allocator, sstable))) {
      LOG_WARN("create ddl sstable failed", K(ret), K(ddl_param));
    }
  }
  if (nullptr != index_block_rebuilder) {
    index_block_rebuilder->~ObIndexBlockRebuilder();
    arena.free(index_block_rebuilder);
    index_block_rebuilder = nullptr;
  }
  if (nullptr != sstable_index_builder) {
    sstable_index_builder->~ObSSTableIndexBuilder();
    arena.free(sstable_index_builder);
    sstable_index_builder = nullptr;
  }
  return ret;
}

int ObTabletDDLUtil::create_ddl_sstable(ObTablet &tablet,
                                        ObSSTableIndexBuilder *sstable_index_builder,
                                        const ObTabletDDLParam &ddl_param,
                                        const ObSSTable *first_ddl_sstable,
                                        common::ObArenaAllocator &allocator,
                                        blocksstable::ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_arena("CreateDDLSstTmp");
  const ObStorageSchema *storage_schema = nullptr;
  SMART_VAR(ObSSTableMergeRes, res) {
    if (OB_UNLIKELY(nullptr == sstable_index_builder || !ddl_param.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), KP(sstable_index_builder), K(ddl_param));
    } else if (OB_FAIL(tablet.load_storage_schema(tmp_arena, storage_schema))) {
      LOG_WARN("failed to load storage schema", K(ret), K(tablet));
    } else {
      int64_t column_count = 0;
      share::schema::ObTableMode table_mode = storage_schema->get_table_mode_struct();
      share::schema::ObIndexType index_type = storage_schema->get_index_type();
      int64_t rowkey_column_cnt = storage_schema->get_rowkey_column_num() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
      common::ObRowStoreType row_store_type = storage_schema->get_row_store_type();
      if (nullptr != first_ddl_sstable) {
        ObSSTableMetaHandle meta_handle;
        if (OB_FAIL(first_ddl_sstable->get_meta(meta_handle))) {
          LOG_WARN("get sstable meta handle fail", K(ret), KPC(first_ddl_sstable));
        } else {
          column_count = meta_handle.get_sstable_meta().get_column_count();
          table_mode = meta_handle.get_sstable_meta().get_basic_meta().table_mode_;
          index_type = static_cast<share::schema::ObIndexType>(meta_handle.get_sstable_meta().get_basic_meta().index_type_);
          rowkey_column_cnt = meta_handle.get_sstable_meta().get_basic_meta().rowkey_column_count_;
          row_store_type = meta_handle.get_sstable_meta().get_basic_meta().latest_row_store_type_;
        }
      } else if (OB_FAIL(storage_schema->get_stored_column_count_in_sstable(column_count))) {
        LOG_WARN("fail to get stored column count in sstable", K(ret));
      }
      if (FAILEDx(sstable_index_builder->close(res))) {
        LOG_WARN("close sstable index builder close failed", K(ret));
      } else if (OB_UNLIKELY((ddl_param.table_key_.is_major_sstable() ||
                              ddl_param.table_key_.is_ddl_sstable()) &&
                             res.row_count_ > 0 &&
                             res.max_merged_trans_version_ != ddl_param.snapshot_version_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("max_merged_trans_version_ in res is different from ddl snapshot version", K(ret),
                 K(res), K(ddl_param));
      } else {
        const int64_t create_schema_version_on_tablet = tablet.get_tablet_meta().create_schema_version_;
        ObTabletCreateSSTableParam param;
        param.table_key_ = ddl_param.table_key_;
        param.table_mode_ = table_mode;
        param.index_type_ = index_type;
        param.rowkey_column_cnt_ = rowkey_column_cnt;
        param.schema_version_ = create_schema_version_on_tablet;
        param.latest_row_store_type_ = row_store_type;
        param.create_snapshot_version_ = ddl_param.snapshot_version_;
        param.ddl_scn_ = ddl_param.start_scn_;
        ObSSTableMergeRes::fill_addr_and_data(res.root_desc_,
            param.root_block_addr_, param.root_block_data_);
        ObSSTableMergeRes::fill_addr_and_data(res.data_root_desc_,
            param.data_block_macro_meta_addr_, param.data_block_macro_meta_);
        param.is_meta_root_ = res.data_root_desc_.is_meta_root_;
        param.root_row_store_type_ = res.root_row_store_type_;
        param.data_index_tree_height_ = res.root_desc_.height_;
        param.index_blocks_cnt_ = res.index_blocks_cnt_;
        param.data_blocks_cnt_ = res.data_blocks_cnt_;
        param.micro_block_cnt_ = res.micro_block_cnt_;
        param.use_old_macro_block_count_ = res.use_old_macro_block_count_;
        param.row_count_ = res.row_count_;
        param.column_cnt_ = column_count;
        param.data_checksum_ = res.data_checksum_;
        param.occupy_size_ = res.occupy_size_;
        param.original_size_ = res.original_size_;
        param.max_merged_trans_version_ = ddl_param.snapshot_version_;
        param.contain_uncommitted_row_ = res.contain_uncommitted_row_;
        param.compressor_type_ = res.compressor_type_;
        param.encrypt_id_ = res.encrypt_id_;
        param.master_key_id_ = res.master_key_id_;
        param.nested_size_ = res.nested_size_;
        param.nested_offset_ = res.nested_offset_;
        param.data_block_ids_ = res.data_block_ids_;
        param.other_block_ids_ = res.other_block_ids_;
        MEMCPY(param.encrypt_key_, res.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);

        if (OB_FAIL(param.column_checksums_.assign(res.data_column_checksums_))) {
          LOG_WARN("fail to fill column checksum for empty major", K(ret), K(param));
        } else if (OB_UNLIKELY(param.column_checksums_.count() != column_count)) {
          // we have corrected the col_default_checksum_array_ in prepare_index_data_desc
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected column checksums", K(ret), K(column_count), K(param));
        } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param, allocator, sstable))) {
          LOG_WARN("create sstable failed", K(ret), K(param));
        } else {
          LOG_INFO("create ddl sstable success", K(ddl_param), K(sstable),
            "create_schema_version", create_schema_version_on_tablet);
        }
      }
    }
    ObTablet::free_storage_schema(tmp_arena, storage_schema);
  }
  return ret;
}

int ObTabletDDLUtil::update_ddl_table_store(ObTablet &tablet,
                                            const ObTabletDDLParam &ddl_param,
                                            common::ObArenaAllocator &allocator,
                                            blocksstable::ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ddl_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_param));
  } else {
    ObLSService *ls_service = MTL(ObLSService *);
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    ObArenaAllocator allocator;
    const ObStorageSchema *tablet_storage_schema = nullptr;
    if (OB_FAIL(ls_service->get_ls(ddl_param.ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("get ls failed", K(ret), K(ddl_param));
    } else if (OB_FAIL(tablet.load_storage_schema(allocator, tablet_storage_schema))) {
      LOG_WARN("fail to load storage schema failed", K(ret));
    } else {
      const bool is_major_sstable = ddl_param.table_key_.is_major_sstable();
      const int64_t rebuild_seq = ls_handle.get_ls()->get_rebuild_seq();
      const int64_t snapshot_version = is_major_sstable ? max(ddl_param.snapshot_version_, tablet.get_snapshot_version())
                                                       : tablet.get_snapshot_version();
      const int64_t multi_version_start = is_major_sstable ? max(ddl_param.snapshot_version_, tablet.get_multi_version_start())
                                              : 0;
      ObTabletHandle new_tablet_handle;
      ObUpdateTableStoreParam table_store_param(&sstable,
                                                snapshot_version,
                                                multi_version_start,
                                                rebuild_seq,
                                                tablet_storage_schema,
                                                is_major_sstable, // update_with_major_flag
                                                /*DDL does not have verification between replicas,
                                                  So using medium merge to force verification between replicas*/
                                                MEDIUM_MERGE,
                                                is_major_sstable// need report checksum
                                                );
      table_store_param.ddl_info_.keep_old_ddl_sstable_ = !is_major_sstable;
      table_store_param.ddl_info_.data_format_version_ = ddl_param.data_format_version_;
      table_store_param.ddl_info_.ddl_commit_scn_ = ddl_param.commit_scn_;
      if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(ddl_param.table_key_.get_tablet_id(), table_store_param, new_tablet_handle))) {
        LOG_WARN("failed to update tablet table store", K(ret), K(ddl_param.table_key_), K(table_store_param));
      } else {
        LOG_INFO("ddl update table store success", K(ddl_param), K(table_store_param), K(sstable));
      }
    }
    ObTablet::free_storage_schema(allocator, tablet_storage_schema);
  }
  return ret;
}

int ObTabletDDLUtil::compact_ddl_sstable(ObTablet &tablet,
                                         ObTableStoreIterator &ddl_sstable_iter,
                                         const ObITableReadInfo &read_info,
                                         const bool is_commit,
                                         const share::SCN &rec_scn,
                                         ObTabletDDLParam &ddl_param,
                                         common::ObArenaAllocator &allocator,
                                         blocksstable::ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena;
  ObBlockMetaTree meta_tree;
  ObArray<const ObDataMacroBlockMeta *> sorted_metas;
  bool is_data_complete = false;
  ObITable *first_ddl_sstable = nullptr;
  ObITable *last_ddl_sstable = nullptr;
  SCN sstable_end_scn = SCN::max_scn();

  if (OB_UNLIKELY(!ddl_param.is_valid() || (is_commit && !rec_scn.is_valid_and_not_min()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_param), K(is_commit), K(rec_scn));
  } else if (ddl_sstable_iter.count() > 0) {
    if (OB_FAIL(ddl_sstable_iter.get_boundary_table(false/*is_last*/, first_ddl_sstable))) {
      LOG_WARN("failed to get boundary table", K(ret));
    } else if (OB_ISNULL(first_ddl_sstable)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("first_ddl_sstable must not null", K(ret));
    } else if (OB_FAIL(ddl_sstable_iter.get_boundary_table(true/*is_last*/, last_ddl_sstable))) {
      LOG_WARN("failed to get boundary table", K(ret));
    } else if (OB_ISNULL(last_ddl_sstable)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("last_ddl_sstable must not null", K(ret));
    } else {
      sstable_end_scn = last_ddl_sstable->get_end_scn();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTabletDDLUtil::check_data_integrity(ddl_sstable_iter,
                                                           ddl_param.start_scn_,
                                                           is_commit ? rec_scn : sstable_end_scn,
                                                           is_data_complete))) {
    LOG_WARN("check ddl sstable integrity failed", K(ret), K(ddl_sstable_iter), K(ddl_param));
  } else if (!is_data_complete) {
    ret = OB_EAGAIN;
    if (TC_REACH_TIME_INTERVAL(10L * 1000L * 1000L)) {
      LOG_WARN("current ddl sstables not contain all data", K(ddl_sstable_iter), K(ddl_param));
    }
  } else if (OB_FAIL(meta_tree.init(tablet, ddl_param.table_key_, ddl_param.start_scn_, ddl_param.data_format_version_))) {
    LOG_WARN("init meta tree failed", K(ret), K(ddl_param));
  } else if (FALSE_IT(ddl_sstable_iter.resume())) {
  } else {
    ObDatumRowkey last_rowkey;
    SMART_VAR(ObSSTableSecMetaIterator, meta_iter) {
      ObDatumRange query_range;
      query_range.set_whole_range();
      ObDataMacroBlockMeta data_macro_meta;
      while (OB_SUCC(ret)) {
        ObITable *table = nullptr;
        const ObSSTable *cur_sstable = nullptr;
        meta_iter.reset();
        if (OB_FAIL(ddl_sstable_iter.get_next(table))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next ddl sstable failed", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, table is nullptr", K(ret), KPC(table));
        } else {
          cur_sstable = static_cast<const ObSSTable *>(table);
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(meta_iter.open(query_range,
                                          ObMacroBlockMetaType::DATA_BLOCK_META,
                                          *cur_sstable,
                                          read_info,
                                          arena))) {
          LOG_WARN("sstable secondary meta iterator open failed", K(ret));
        } else {
          while (OB_SUCC(ret)) {
            if (OB_FAIL(meta_iter.get_next(data_macro_meta))) {
              if (OB_ITER_END != ret) {
                LOG_WARN("get data macro meta failed", K(ret));
              } else {
                ret = OB_SUCCESS;
                break;
              }
            } else {
              ObDataMacroBlockMeta *copied_meta = nullptr; // copied meta will destruct in the meta tree
              ObDDLMacroHandle macro_handle;
              bool is_exist = false;
              if (OB_FAIL(meta_tree.exist(&data_macro_meta.end_key_, is_exist))) {
                LOG_WARN("check block meta exist failed", K(ret), K(data_macro_meta));
              } else if (is_exist) {
                // skip
              } else if (OB_FAIL(macro_handle.set_block_id(data_macro_meta.get_macro_id()))) {
                LOG_WARN("hold macro block failed", K(ret));
              } else if (OB_FAIL(data_macro_meta.deep_copy(copied_meta, arena))) {
                LOG_WARN("deep copy macro block meta failed", K(ret));
              } else if (OB_FAIL(meta_tree.insert_macro_block(macro_handle, &copied_meta->end_key_, copied_meta))) {
                LOG_WARN("insert meta tree failed", K(ret), K(macro_handle), KPC(copied_meta));
                copied_meta->~ObDataMacroBlockMeta();
              }
            }
          }
          LOG_INFO("append meta tree finished", K(ret),
                   //              "data_macro_block_cnt_in_sstable", cur_sstable->get_meta().get_basic_meta().get_data_macro_block_count(),
                   K(meta_tree.get_macro_block_cnt()));
#ifdef ERRSIM
          if (OB_SUCC(ret) && ddl_param.table_key_.is_major_sstable()) {
            ret = OB_E(EventTable::EN_DDL_COMPACT_FAIL) OB_SUCCESS;
            if (OB_FAIL(ret)) {
              LOG_WARN("errsim compact ddl sstable failed", KR(ret));
            }
          }
#endif
        }
      }
    }
  }
  // close
  if (OB_SUCC(ret) && is_data_complete) {
    if (is_commit) {
      ddl_param.table_key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
      ddl_param.table_key_.version_range_.base_version_ = 0;
      ddl_param.table_key_.version_range_.snapshot_version_ = ddl_param.snapshot_version_;
    } else {
      ddl_param.table_key_.table_type_ = ObITable::TableType::DDL_DUMP_SSTABLE;
      ddl_param.table_key_.scn_range_.start_scn_ = first_ddl_sstable->get_start_scn();
      ddl_param.table_key_.scn_range_.end_scn_ = last_ddl_sstable->get_end_scn();
    }
    if (OB_FAIL(meta_tree.build_sorted_rowkeys())) {
      LOG_WARN("build sorted rowkey failed", K(ret));
    } else if (OB_FAIL(meta_tree.get_sorted_meta_array(sorted_metas))) {
      LOG_WARN("get sorted metas failed", K(ret));
    } else if (OB_FAIL(create_ddl_sstable(tablet,
                                          ddl_param,
                                          sorted_metas,
                                          static_cast<ObSSTable *>(first_ddl_sstable),
                                          allocator,
                                          sstable))) {
      LOG_WARN("create ddl sstable failed", K(ret));
    } else if (OB_FAIL(update_ddl_table_store(tablet, ddl_param, allocator, sstable))) {
      LOG_WARN("update ddl table store failed", K(ret));
    } else {
      LOG_INFO("compact ddl sstable success", K(ddl_param));
    }
  }
  return ret;
}

int ObTabletDDLUtil::report_ddl_checksum(const share::ObLSID &ls_id,
                                         const ObTabletID &tablet_id,
                                         const uint64_t table_id,
                                         const int64_t execution_id,
                                         const int64_t ddl_task_id,
                                         const int64_t *column_checksums,
                                         const int64_t column_count)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  const uint64_t tenant_id = MTL_ID();
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || OB_INVALID_ID == ddl_task_id
        || !is_valid_id(table_id) || 0 == table_id || execution_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(table_id), K(execution_id));
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
  } else {
    ObArray<ObColDesc> column_ids;
    ObArray<ObDDLChecksumItem> ddl_checksum_items;
    if (OB_FAIL(table_schema->get_multi_version_column_descs(column_ids))) {
      LOG_WARN("fail to get column ids", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_UNLIKELY(column_count > column_ids.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect error, column checksums count larger than column ids count", K(ret),
          K(ls_id), K(tablet_id), K(column_count), K(column_ids.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      share::ObDDLChecksumItem item;
      item.execution_id_ = execution_id;
      item.tenant_id_ = tenant_id;
      item.table_id_ = table_id;
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
    } else if (OB_FAIL(ObDDLChecksumOperator::update_checksum(ddl_checksum_items, *sql_proxy))) {
      LOG_WARN("fail to update checksum", K(ret), K(ls_id), K(tablet_id), K(table_id), K(ddl_checksum_items));
    } else {
      LOG_INFO("report ddl checkum success", K(ls_id), K(tablet_id), K(table_id), K(execution_id));
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

} // namespace storage
} // namespace oceanbase
