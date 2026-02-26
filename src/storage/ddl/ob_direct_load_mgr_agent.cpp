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

#define USING_LOG_PREFIX STORAGE

#include "ob_direct_load_mgr_agent.h"
#include "storage/ddl/ob_direct_load_mgr_v3.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"
#include "storage/direct_load/ob_direct_load_insert_table_row_iterator.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;

ObDirectLoadMgrAgent::ObDirectLoadMgrAgent()
  : is_inited_(false), direct_load_type_(ObDirectLoadType::DIRECT_LOAD_INVALID), start_scn_(), execution_id_(-1), mgr_handle_(), lob_mgr_handle_(),
    cgs_count_(0), lob_writer_(), data_writer_(), idem_start_seq_(0), idem_next_seq_(0)
{
}

ObDirectLoadMgrAgent::~ObDirectLoadMgrAgent()
{
  reset();
}
void ObDirectLoadMgrAgent::reset()
{
  is_inited_ = false;
  direct_load_type_ = ObDirectLoadType::DIRECT_LOAD_INVALID;
  lob_mgr_handle_.reset();
  mgr_handle_.reset();
  start_scn_.reset();
  execution_id_ = -1;
  cgs_count_ = 0;
  lob_writer_.reset();
  data_writer_.reset();
  idem_start_seq_ = 0;
  idem_next_seq_ = 0;
}
int ObDirectLoadMgrAgent::init(ObBaseTabletDirectLoadMgr *direct_load_mgr, ObBaseTabletDirectLoadMgr *lob_direct_load_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(direct_load_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(direct_load_mgr), KP(lob_direct_load_mgr));
  } else if (!direct_load_mgr->is_valid() || !is_idem_type(direct_load_mgr->get_direct_load_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid direct load mgr_handle_", K(ret), KPC(direct_load_mgr), KPC(lob_direct_load_mgr));
  } else if (nullptr != lob_direct_load_mgr && !is_idem_type(lob_direct_load_mgr->get_direct_load_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob direct load mgr", K(ret), KPC(lob_direct_load_mgr));
  } else {
    direct_load_type_ = direct_load_mgr->get_direct_load_type();
    start_scn_        = static_cast<ObTabletDirectLoadMgrV3*>(direct_load_mgr)->get_start_scn();
    execution_id_     = static_cast<ObTabletDirectLoadMgrV3*>(direct_load_mgr)->get_execution_id();
    mgr_handle_.set_obj(direct_load_mgr);
    if (nullptr != lob_direct_load_mgr) {
      lob_mgr_handle_.set_obj(lob_direct_load_mgr);
    }
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadMgrAgent::init(
    const int64_t context_id,
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id/*always data tablet id.*/,
    const ObDirectLoadType &type)
{
  int ret = OB_SUCCESS;
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(context_id <= 0
      || !ls_id.is_valid()
      || !tablet_id.is_valid())
      || !is_valid_direct_load(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(context_id), K(ls_id), K(tablet_id), K(type));
  } else if (OB_ISNULL(tenant_direct_load_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(tenant_direct_load_mgr->get_agent_exec_context(
      context_id, ls_id, tablet_id, type, mgr_handle_, start_scn_, execution_id_))) {
    LOG_WARN("get agent exec context failed", K(ret), K(context_id), K(tablet_id));
  } else if (is_shared_storage_dempotent_mode(type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid direct load type which not used any more", K(ret));
  } else if (OB_FAIL(init_for_sn(ls_id, tablet_id))) {
    LOG_WARN("init for sn failed", K(ret), K(ls_id), K(tablet_id));
  }
  if (OB_SUCC(ret)) {
    direct_load_type_ = type;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadMgrAgent::init_for_sn(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObStorageSchema *storage_schema = nullptr;
  ObArenaAllocator tmp_arena("ddl_load_schema", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
      tablet_id, tablet_handle, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(nullptr == tablet_handle.get_obj())) {
    ret = OB_ERR_SYS;
    LOG_WARN("tablet handle is null", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(tmp_arena, storage_schema))) {
    LOG_WARN("load storage schema failed", K(ret));
  } else if (OB_FALSE_IT(cgs_count_ = storage_schema->get_column_groups().count())) {
  } else if (OB_LIKELY(mgr_handle_.is_valid())) {
    if (!start_scn_.is_valid_and_not_min() || execution_id_ < 0) {
      ret = OB_ERR_SYS;
      LOG_WARN("unexpected err", K(ret), K(tablet_id), K(start_scn_), K(execution_id_));
    }
  } else if (OB_UNLIKELY(is_incremental_direct_load(direct_load_type_))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("mgr handle not exist", K(ret), K(ls_id), K(tablet_id));
  } else {
    const blocksstable::ObSSTable *first_major_sstable = nullptr;
    ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
    if (OB_FAIL(ObTabletDDLUtil::check_and_get_major_sstable(
        ls_id, tablet_id, first_major_sstable, table_store_wrapper))) {
      LOG_WARN("check if major sstable exist failed", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_NOT_NULL(first_major_sstable)) {
      LOG_INFO("skip, mgr handle is invalid due to major exist under shared-nothing mode", K(tablet_id));
    } else {
      ret = OB_ERR_SYS;
      LOG_WARN("mgr handle is invalid but the major does not exist under shared-nothing mode", K(ret), K(tablet_id));
    }
  }
  ObTabletObjLoadHelper::free(tmp_arena, storage_schema);
  return ret;
}

int ObDirectLoadMgrAgent::open_sstable_slice(
    const blocksstable::ObMacroDataSeq &start_seq,
    ObDirectLoadSliceInfo &slice_info)
{
  int ret = OB_SUCCESS;
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!start_seq.is_valid())) { // slice id is invalid.
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(start_seq), K(slice_info));
  } else if (OB_ISNULL(tenant_direct_load_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(tenant_direct_load_mgr->alloc_slice_id(slice_info.slice_id_))) { // alloc slice id firstly.
    LOG_WARN("alloc slice id failed", K(ret));
  } else if (is_idem_type(direct_load_type_)) {
    idem_start_seq_ = start_seq;
    if (!slice_info.is_lob_slice_) {
      data_writer_.reset();
      if (OB_FAIL(mgr_handle_.get_base_obj()->prepare_index_builder())) {
        LOG_WARN("failed to prepare index builder", K(ret));
      } else if (OB_FAIL(data_writer_.init(mgr_handle_.get_base_obj(),
                                           start_seq,
                                           slice_info.slice_idx_,
                                           slice_info.merge_slice_idx_))) {
        LOG_WARN("failed to init data writer", K(ret));
      }
    } else if (slice_info.is_lob_slice_) {
      lob_writer_.reset();
      if (OB_FAIL(lob_mgr_handle_.get_base_obj()->prepare_index_builder())) {
        LOG_WARN("failed to prepare index builder", K(ret));
      } else if (OB_FAIL(lob_writer_.init(lob_mgr_handle_.get_base_obj(),
                                          start_seq,
                                          slice_info.slice_idx_,
                                          slice_info.merge_slice_idx_))) {
        LOG_WARN("failed to init lob writer", K(ret), K(idem_start_seq_));
      }
    }
  } else {
    if (!is_shared_storage_dempotent_mode(direct_load_type_)) {
      if (OB_FAIL(open_sstable_slice_for_sn(start_seq, slice_info))) {
        LOG_WARN("open slice failed", K(ret), K(slice_info), K(direct_load_type_));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("all ss direct load type should be idem type", K(ret));
    }
  }
  return ret;
}

int ObDirectLoadMgrAgent::open_sstable_slice_for_sn(
    const blocksstable::ObMacroDataSeq &start_seq,
    ObDirectLoadSliceInfo &slice_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mgr_handle_.is_valid())) {
    // already committed, do nothing.
  } else if (OB_FAIL(mgr_handle_.get_obj()->open_sstable_slice(slice_info.is_lob_slice_, start_seq, slice_info))) {
    LOG_WARN("open sstable slice failed", K(ret), K(start_seq), K(slice_info));
  }
  return ret;
}

int ObDirectLoadMgrAgent::fill_sstable_slice(
    const ObDirectLoadSliceInfo &slice_info,
    ObIStoreRowIterator *iter,
    int64_t &affected_rows,
    ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid() || nullptr == iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(slice_info), KP(iter));
  } else if (is_idem_type(direct_load_type_)) {
    if (OB_FAIL(fill_sstable_slice_for_idem(slice_info, iter, idem_start_seq_, idem_next_seq_, affected_rows, insert_monitor))) {
      LOG_WARN("fill slice for idem type failed", K(ret), K(direct_load_type_));
    }
  } else if (!is_shared_storage_dempotent_mode(direct_load_type_)) {
    if (OB_FAIL(fill_sstable_slice_for_sn(slice_info, iter, affected_rows, insert_monitor))) {
      LOG_WARN("fill slice for sn failed", K(ret), K(slice_info));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss direct load type should be idem direct load type", K(ret));
  }
  return ret;
}
/*
 * this function is used for ObTabletDirectMgrV2 and its sub class
 * compared with fill_sstable_slice,
 * this func add start_seq as param
*/
int ObDirectLoadMgrAgent::fill_sstable_slice_for_idem(const ObDirectLoadSliceInfo &slice_info,
                                                      ObIStoreRowIterator *iter,
                                                      const blocksstable::ObMacroDataSeq &start_seq,
                                                      blocksstable::ObMacroDataSeq &next_seq,
                                                      int64_t &affected_rows,
                                                      ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(slice_info), KP(iter));
  } else if (!is_idem_type(direct_load_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only idem type direct load mgr use this func", K(ret));
  } else if (OB_FAIL(mgr_handle_.get_base_obj()->fill_sstable_slice_v2(slice_info, iter, data_writer_, next_seq, insert_monitor, affected_rows))) {
    LOG_WARN("failed to fill sstable slice", K(ret), K(slice_info), K(start_seq));
  } else if (idem_next_seq_.is_valid()) {
    idem_start_seq_  = idem_next_seq_;
    idem_next_seq_ = ObMacroDataSeq();
  }
  return ret;
}

int ObDirectLoadMgrAgent::fill_sstable_slice_for_sn(
    const ObDirectLoadSliceInfo &slice_info,
    ObIStoreRowIterator *iter,
    int64_t &affected_rows,
    ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  bool need_consume_remained_rows = false;
  if (OB_UNLIKELY(!mgr_handle_.is_valid())) {
    // already committed, consume remained rows.
    need_consume_remained_rows = true;
  } else if (OB_FAIL(mgr_handle_.get_base_obj()->fill_sstable_slice(slice_info, start_scn_, iter, affected_rows, insert_monitor))) {
    if (OB_TRANS_COMMITED == ret && slice_info.is_full_direct_load_) {
      ret = OB_SUCCESS;
      need_consume_remained_rows = true;
      LOG_INFO("trans commited", K(slice_info));
    } else {
      LOG_WARN("fill slice failed", K(ret), K(slice_info));
    }
  }
  if (OB_SUCC(ret) && need_consume_remained_rows) {
    const ObDatumRow *row = nullptr;
    ObIDirectLoadRowIterator *interpret_iter = static_cast<ObIDirectLoadRowIterator *>(iter);
    while (OB_SUCC(ret)) {
      affected_rows++;
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(interpret_iter->get_next_row(true/*skip_lob*/, row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("iter row failed", K(ret), K(slice_info));
        }
      } else if ((affected_rows % 100 == 0) && OB_NOT_NULL(insert_monitor)) {
        (void) ATOMIC_AAF(&insert_monitor->scanned_row_cnt_, 100);
        (void) ATOMIC_AAF(&insert_monitor->inserted_row_cnt_, 100);
        (void) ATOMIC_AAF(&insert_monitor->inserted_cg_row_cnt_, cgs_count_ * 100);
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(insert_monitor)) {
      (void) ATOMIC_AAF(&insert_monitor->scanned_row_cnt_, affected_rows % 100);
      (void) ATOMIC_AAF(&insert_monitor->inserted_row_cnt_, affected_rows % 100);
      (void) ATOMIC_AAF(&insert_monitor->inserted_cg_row_cnt_, cgs_count_ * (affected_rows % 100));
    }
  }
  return ret;
}

int ObDirectLoadMgrAgent::fill_sstable_slice(
    const ObDirectLoadSliceInfo &slice_info,
    const ObBatchDatumRows &datum_rows,
    ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid() || 0 == datum_rows.row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(slice_info), K(datum_rows.row_count_));
  } else if (is_idem_type(direct_load_type_)) {
    if (OB_FAIL(mgr_handle_.get_base_obj()->fill_sstable_slice_v2(slice_info, datum_rows, data_writer_, insert_monitor))) {
      LOG_WARN("failed to fill sstable slice", K(ret));
    }
  } else if (!is_shared_storage_dempotent_mode(direct_load_type_)) {
    if (OB_FAIL(fill_sstable_slice_for_sn(slice_info, datum_rows, insert_monitor))) {
      LOG_WARN("fill slice for sn failed", K(ret), K(slice_info));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss direct load type should be idem type", K(ret));
  }
  return ret;
}

int ObDirectLoadMgrAgent::fill_sstable_slice_for_sn(
    const ObDirectLoadSliceInfo &slice_info,
    const ObBatchDatumRows &datum_rows,
    ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mgr_handle_.is_valid())) {
    // already committed, do nothing
  } else if (OB_FAIL(mgr_handle_.get_base_obj()->fill_sstable_slice(slice_info, start_scn_, datum_rows, insert_monitor))) {
    if (OB_TRANS_COMMITED == ret && slice_info.is_full_direct_load_) {
      ret = OB_SUCCESS;
      LOG_INFO("trans commited", K(slice_info));
    } else {
      LOG_WARN("fill slice failed", K(ret), K(slice_info));
    }
  }
  return ret;
}

int ObDirectLoadMgrAgent::fill_lob_sstable_slice(
    ObIAllocator &allocator,
    const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
    share::ObTabletCacheInterval &pk_interval,
    blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(slice_info));
  } else if (is_idem_type(direct_load_type_)) {
    if (OB_FAIL(fill_lob_sstable_slice_idem(allocator, slice_info, pk_interval, datum_row))) {
      LOG_WARN("fill slice failed", K(ret), K(slice_info));
    }
  } else if (!is_shared_storage_dempotent_mode(direct_load_type_)) {
    if (OB_FAIL(fill_lob_sstable_slice_for_sn(allocator, slice_info, pk_interval, datum_row))) {
      LOG_WARN("fill slice for sn failed", K(ret), K(slice_info));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss direct load type should be idem type", K(ret));
  }
  return ret;
}

int ObDirectLoadMgrAgent::fill_lob_sstable_slice_idem(ObIAllocator &allocator,
                                                      const ObDirectLoadSliceInfo &slice_info,
                                                      share::ObTabletCacheInterval &pk_interval,
                                                      blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (!mgr_handle_.is_valid() || !lob_mgr_handle_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(mgr_handle_.is_valid()), K(lob_mgr_handle_.is_valid()));
  } else if (OB_FAIL(lob_mgr_handle_.get_base_obj()->fill_lob_sstable_slice_row_v2(allocator, slice_info, pk_interval, datum_row, lob_writer_, mgr_handle_))) {
    LOG_WARN("failed to fill lob sstable slice ", K(ret));
  }
  return ret;
}

int ObDirectLoadMgrAgent::fill_lob_sstable_slice_for_sn(
    ObIAllocator &allocator,
    const ObDirectLoadSliceInfo &slice_info,
    share::ObTabletCacheInterval &pk_interval,
    blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mgr_handle_.is_valid())) {
    // already committed, do nothing.
  } else if (OB_FAIL(mgr_handle_.get_base_obj()->fill_lob_sstable_slice(allocator, slice_info, start_scn_, pk_interval, datum_row))) {
    LOG_WARN("fail to fill batch sstable slice", K(ret), K(slice_info), K(datum_row));
  }
  return ret;
}

int ObDirectLoadMgrAgent::fill_lob_sstable_slice(
    ObIAllocator &allocator,
    const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
    share::ObTabletCacheInterval &pk_interval,
    blocksstable::ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(slice_info));
  } else if (is_idem_type(direct_load_type_)) {
    if (OB_FAIL(fill_batch_lob_sstable_slice_idem(allocator, slice_info, pk_interval, datum_rows))) {
      LOG_WARN("failed to fill lob sstable slice", K(ret), K(slice_info));
    }
  } else if (!is_shared_storage_dempotent_mode(direct_load_type_)) {
    if (OB_FAIL(fill_lob_sstable_slice_for_sn(allocator, slice_info, pk_interval, datum_rows))) {
      LOG_WARN("fill slice for sn failed", K(ret), K(slice_info));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss should be idem type", K(ret));
  }
  return ret;
}

int ObDirectLoadMgrAgent::fill_batch_lob_sstable_slice_idem(ObIAllocator &allocator,
                                                            const ObDirectLoadSliceInfo &slice_info,
                                                            share::ObTabletCacheInterval &pk_interval,
                                                            blocksstable::ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (!slice_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid slice info", K(ret), K(slice_info));
  } else if (!lob_mgr_handle_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob mgr handle is invalid", K(ret));
  } else if (OB_FAIL(static_cast<ObTabletDirectLoadMgrV3*>(lob_mgr_handle_.get_base_obj())->fill_lob_sstable_slice_row_v2(allocator, slice_info, pk_interval, lob_writer_, mgr_handle_, datum_rows))) {
    LOG_WARN("failed to fill lob sstable slice", K(ret));
  }
  return ret;
}

int ObDirectLoadMgrAgent::fill_lob_sstable_slice_for_sn(
    ObIAllocator &allocator,
    const ObDirectLoadSliceInfo &slice_info,
    share::ObTabletCacheInterval &pk_interval,
    blocksstable::ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mgr_handle_.is_valid())) {
    // already committed, do nothing.
  } else if (OB_FAIL(mgr_handle_.get_base_obj()->fill_lob_sstable_slice(allocator, slice_info, start_scn_, pk_interval, datum_rows))) {
    LOG_WARN("fail to fill batch sstable slice", K(ret), K(slice_info));
  }
  return ret;
}

int ObDirectLoadMgrAgent::fill_lob_meta_sstable_slice(
    const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
    ObIStoreRowIterator *iter,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(slice_info));
  } else if (is_idem_type(direct_load_type_)) {
    if (!lob_mgr_handle_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to set lob mgr handle", K(ret));
    } else if (OB_FAIL(lob_mgr_handle_.get_base_obj()->fill_lob_meta_sstable_slice(slice_info, iter, lob_writer_, affected_rows))) {
      LOG_WARN("failed to fill lob meta sstable slice", K(ret));
    }
  } else if (!is_shared_storage_dempotent_mode(direct_load_type_)) {
    if (OB_FAIL(fill_lob_meta_sstable_slice_for_sn(slice_info, iter, affected_rows))) {
      LOG_WARN("fill lob meta sstable slice for sn failed", K(ret), K(slice_info));
    }
  }
  return ret;
}

int ObDirectLoadMgrAgent::fill_lob_meta_sstable_slice_for_sn(
    const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
    ObIStoreRowIterator *iter,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mgr_handle_.is_valid())) {
    // already committed, do nothing.
  } else if (OB_FAIL(mgr_handle_.get_base_obj()->fill_lob_meta_sstable_slice(slice_info, start_scn_, iter, affected_rows))) {
    LOG_WARN("fail to fill batch lob meta sstable slice", K(ret), K(slice_info));
  }
  return ret;
}

int ObDirectLoadMgrAgent::close_sstable_slice(
    const ObDirectLoadSliceInfo &slice_info,
    ObInsertMonitor *insert_monitor,
    blocksstable::ObMacroDataSeq &next_seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!slice_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(slice_info));
  } else if (is_idem_type(direct_load_type_)) {
    if (OB_FAIL(close_sstable_slice_for_idem(slice_info, idem_start_seq_, idem_next_seq_, insert_monitor))) {
      LOG_WARN("failed to close sstable slice", K(ret), K(direct_load_type_));
    } else {
      next_seq = idem_next_seq_;
    }
  } else if (!is_shared_storage_dempotent_mode(direct_load_type_)) {
    if (OB_FAIL(close_sstable_slice_for_sn(slice_info, insert_monitor, next_seq))) {
      LOG_WARN("close slice for sn failed", K(ret), K(slice_info), K(direct_load_type_));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ss should be idem direct load type", K(ret));
  }
  return ret;
}

int ObDirectLoadMgrAgent::close_sstable_slice_for_idem(const ObDirectLoadSliceInfo &slice_info,
                                                       const blocksstable::ObMacroDataSeq &start_seq,
                                                       blocksstable::ObMacroDataSeq &next_seq,
                                                       ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!slice_info.is_valid() || !start_seq.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(slice_info), K(start_seq));
  } else if (!mgr_handle_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mgr handle is invalid", K(ret));
  } else if (slice_info.is_lob_slice_) { /* only close lob sstable slice */
    bool unused_is_all_task_finish = false;
    if (!lob_mgr_handle_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob mgr is invalid", K(ret));
    } else if (OB_FAIL(lob_mgr_handle_.get_base_obj()->close_sstable_slice_v2(slice_info, lob_writer_, next_seq, insert_monitor, unused_is_all_task_finish))) {
      LOG_WARN("failed to close lob sstable slice", K(ret), K(slice_info));
    }
  } else {  /* close data sstable slice, close lob sstable slice firset*/
    bool is_all_task_finish = false;
    if (OB_FAIL(mgr_handle_.get_base_obj()->close_sstable_slice_v2(slice_info, data_writer_, next_seq, insert_monitor, is_all_task_finish))) {
      LOG_WARN("failed to close sstable slice", K(ret), K(slice_info));
    } else if (is_all_task_finish && OB_FAIL(close_for_idem())) {
      LOG_WARN("failed to close", K(ret));
    }
  }
  return ret;
}
int ObDirectLoadMgrAgent::close(const int64_t context_id, const bool need_commit, const int64_t execution_id)
{
  int ret = OB_SUCCESS;
  if (context_id < 0) { /* execution id may be -1, since tenant_direct_load_mgr.close() default execution_id is -1*/
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invaliad argument", K(ret), K(context_id));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("agent is not init", K(ret), KP(this));
  } else if (!mgr_handle_.is_valid()) {
    LOG_WARN("mgr handle is invalid", K(ret));
  } else if (!is_idem_type(direct_load_type_)) {
    ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
    if (OB_FAIL(tenant_direct_load_mgr->close_tablet_direct_load(context_id, direct_load_type_, mgr_handle_.get_base_obj()->get_ls_id(), mgr_handle_.get_base_obj()->get_tablet_id(), need_commit, true/*emergent_finish*/,
    mgr_handle_.get_base_obj()->get_ddl_task_id(), mgr_handle_.get_base_obj()->get_build_param().runtime_only_param_.table_id_, execution_id))) {
        LOG_WARN("close tablet direct load failed", K(ret), KPC(mgr_handle_.get_base_obj()));
      }
  } else if (need_commit && OB_FAIL(close_for_idem())) {
    LOG_WARN("failed to close tablet direct load mgr", K(ret));
  }
  return ret;
}

int ObDirectLoadMgrAgent::close_for_idem()
{
  int ret = OB_SUCCESS;
  if (lob_mgr_handle_.is_valid() && OB_FAIL(static_cast<ObTabletDirectLoadMgrV3*>(lob_mgr_handle_.get_base_obj())->close())) {
    LOG_WARN("failed to close lob direct load mgr", K(ret));
  } else if (OB_FAIL(static_cast<ObTabletDirectLoadMgrV3*>(mgr_handle_.get_base_obj())->close())) {
    LOG_WARN("failed to close directl load mgr", K(ret));
  }
  return ret;
}

int ObDirectLoadMgrAgent::close_sstable_slice_for_sn(
    const ObDirectLoadSliceInfo &slice_info,
    ObInsertMonitor *insert_monitor,
    blocksstable::ObMacroDataSeq &next_seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mgr_handle_.is_valid())) {
    // already committed, do nothing.
  } else if (OB_FAIL(mgr_handle_.get_base_obj()->close_sstable_slice(
      slice_info.is_lob_slice_, slice_info, start_scn_, execution_id_, insert_monitor, next_seq))) {
    LOG_WARN("close sstable slice failed", K(ret), K(slice_info), K_(start_scn), K_(execution_id));
  }
  return ret;
}

int ObDirectLoadMgrAgent::calc_range(const int64_t context_id, const int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!mgr_handle_.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret), KPC(this));
  } else if (OB_FAIL(mgr_handle_.get_base_obj()->calc_range(context_id, thread_cnt))) {
    LOG_WARN("calc range failed", K(ret));
  }
  return ret;
}

int ObDirectLoadMgrAgent::fill_column_group(
    const int64_t thread_cnt,
    const int64_t thread_id,
    ObInsertMonitor *insert_monitor)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!mgr_handle_.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret), KPC(this));
  } else if (OB_FAIL(mgr_handle_.get_obj()->fill_column_group(thread_cnt, thread_id, insert_monitor))) {
    LOG_WARN("fill column group failed", K(ret), K(thread_cnt), K(thread_id));
  }
  return ret;
}

int ObDirectLoadMgrAgent::cancel()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_idem_type(direct_load_type_)) {
    /* skip, idem type not need to use cacnel to control */
  } else if (OB_UNLIKELY(!mgr_handle_.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret), KPC(this));
  } else if (OB_FAIL(mgr_handle_.get_base_obj()->cancel())) {
    LOG_WARN("cancel failed", K(ret));
  }
  return ret;
}

int ObDirectLoadMgrAgent::get_lob_meta_tablet_id(ObTabletID &lob_meta_tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!mgr_handle_.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret), KPC(this));
  } else if (!is_idem_type(direct_load_type_)) {
    lob_meta_tablet_id = mgr_handle_.get_obj()->get_lob_meta_tablet_id();
  } else {
    lob_meta_tablet_id = lob_mgr_handle_.is_valid() ? lob_mgr_handle_.get_base_obj()->get_tablet_id() : ObTabletID();
  }
  return ret;
}

int ObDirectLoadMgrAgent::update_max_lob_id(const int64_t lob_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!mgr_handle_.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret), KPC(this));
  } else if (is_idem_type(direct_load_type_) && GCTX.is_shared_storage_mode()) {
    ObTabletDirectLoadMgrHandle lob_handle;
    if (OB_FAIL(get_lob_mgr_handle(lob_handle))) {
      LOG_WARN("failed to get lob mgr handle", K(ret));
    } else if (!lob_handle.is_valid()) {
      /* lob handle not exist, skip*/
    } else if (OB_FAIL(lob_handle.get_base_obj()->update_max_lob_id(lob_id))) {
      LOG_WARN("update max lob id failed", K(ret), K(lob_id));
    }
  }
  return ret;
}


int ObDirectLoadMgrAgent::get_tablet_cache_interval(const int64_t context_id,
                                                    const ObTabletID &tablet_id,
                                                    share::ObTabletCacheInterval &interval)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!mgr_handle_.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys", K(ret), KPC(this));
  } else if (is_idem_type(direct_load_type_)) {
    if (OB_FAIL(static_cast<ObTabletDirectLoadMgrV3*>(mgr_handle_.get_base_obj())->get_tablet_cache_interval(interval))) {
      LOG_WARN("failed to get tablet cache interval", K(ret));
    }
  } else {
    ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
    if (OB_ISNULL(tenant_direct_load_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant direct load mgr should not be null", K(ret));
    } else if (OB_FAIL(tenant_direct_load_mgr->get_tablet_cache_interval(context_id, tablet_id, interval))) {
      LOG_WARN("failed to get tablet cache interval", K(ret));
    }
  }
  return ret;
}

int ObDirectLoadMgrAgent::get_lob_mgr_handle(ObTabletDirectLoadMgrHandle &lob_mgr_handle)
{
  int ret = OB_SUCCESS;
  lob_mgr_handle.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("direct load mgr is not init", K(ret));
  } else if (is_idem_type(direct_load_type_)) {
    lob_mgr_handle.set_obj(lob_mgr_handle_.get_base_obj());
  } else {
    if (!mgr_handle_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mgr handle is invalid", K(ret));
    } else {
      lob_mgr_handle.set_obj(mgr_handle_.get_obj()->get_lob_mgr_handle().get_base_obj());
    }
  }
  return ret;
}

int ObDirectLoadMgrAgent::create_tablet_direct_load_mgr(int64_t tenant_id,
                                                        const int64_t execution_id,
                                                        const int64_t context_id,
                                                        const ObTabletDirectLoadInsertParam &build_param,
                                                        ObIAllocator &allocator,
                                                        bool &is_major_exist,
                                                        ObTabletDirectLoadMgrHandle &data_mgr_handle,
                                                        ObTabletDirectLoadMgrHandle &lob_mgr_handle)
{
  return ObDirectLoadMgrUtil::create_tablet_direct_load_mgr(tenant_id, execution_id, context_id, build_param, allocator, is_major_exist, data_mgr_handle, lob_mgr_handle);
}

ObDirectLoadType ObDirectLoadMgrAgent::load_data_get_direct_load_type(const bool is_incremental,
                                                                      const uint64_t data_format_version,
                                                                      const bool shared_storage_mode,
                                                                      const bool is_inc_major)
{
  return ObDirectLoadMgrUtil::load_data_get_direct_load_type(is_incremental, data_format_version, shared_storage_mode, is_inc_major);
}