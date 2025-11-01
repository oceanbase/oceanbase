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

#include "ob_ddl_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/ob_i_table.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"

using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;

ObDDLMacroHandle::ObDDLMacroHandle()
  : block_id_()
{

}

ObDDLMacroHandle::ObDDLMacroHandle(const ObDDLMacroHandle &other)
{
  *this = other;
}

ObDDLMacroHandle &ObDDLMacroHandle::operator=(const ObDDLMacroHandle &other)
{
  if (&other != this) {
    (void)set_block_id(other.get_block_id());
  }
  return *this;
}

ObDDLMacroHandle::~ObDDLMacroHandle()
{
  reset_macro_block_ref();
}

int ObDDLMacroHandle::set_block_id(const blocksstable::MacroBlockId &block_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(reset_macro_block_ref())) {
    LOG_WARN("reset macro block reference failed", K(ret), K(block_id_));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.inc_ref(block_id))) {
    LOG_ERROR("failed to increase macro block ref cnt", K(ret), K(block_id));
  } else {
    block_id_ = block_id;
  }
  return ret;
}

int ObDDLMacroHandle::reset_macro_block_ref()
{
  int ret = OB_SUCCESS;
  if (block_id_.is_valid()) {
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.dec_ref(block_id_))) {
      LOG_ERROR("failed to dec macro block ref cnt", K(ret), K(block_id_));
    } else {
      block_id_.reset();
    }
  }
  return ret;
}

ObDDLMacroBlock::ObDDLMacroBlock()
  : block_handle_(),
    logic_id_(),
    block_type_(DDL_MB_INVALID_TYPE),
    ddl_start_scn_(SCN::min_scn()),
    scn_(SCN::min_scn()),
    table_key_(),
    end_row_id_(-1),
    trans_id_(),
    data_macro_meta_(nullptr),
    buf_(nullptr),
    size_(0),
    merge_slice_idx_(0),
    seq_no_()
{
}

ObDDLMacroBlock::~ObDDLMacroBlock()
{
}

int ObDDLMacroBlock::deep_copy(ObDDLMacroBlock &dst_block, common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  void *tmp_buf = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(*this));
  } else if (OB_FAIL(data_macro_meta_->deep_copy(dst_block.data_macro_meta_, allocator))) {
    LOG_WARN("failed to copy", K(ret));
  } else {
    dst_block.block_type_ = block_type_;
    dst_block.block_handle_ = block_handle_;
    dst_block.logic_id_ = logic_id_;
    dst_block.ddl_start_scn_ = ddl_start_scn_;
    dst_block.scn_ = scn_;
    dst_block.table_key_ = table_key_;
    dst_block.end_row_id_ = end_row_id_;
    dst_block.buf_ = nullptr;
    dst_block.size_ = 0;
    dst_block.merge_slice_idx_ = merge_slice_idx_;
    dst_block.trans_id_ = trans_id_;
    dst_block.seq_no_ = seq_no_;
  }

  if (OB_FAIL(ret) || !GCTX.is_shared_storage_mode() || ObDDLMacroBlockType::DDL_MB_INDEX_TYPE != block_type_) {
    /* if fail or not shared storage, skip copy buf*/
  } else if (OB_ISNULL(tmp_buf = allocator.alloc(size_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for macro block buffer failed", K(ret));
  } else {
    memcpy(tmp_buf, buf_, size_);
    dst_block.buf_ = static_cast<const char*>(tmp_buf);
    dst_block.size_ = size_;
  }
  return ret;
}

int ObDDLMacroBlock::set_data_macro_meta(const MacroBlockId &macro_id, const char* macro_block_buf, const int64_t size, const ObDDLMacroBlockType &block_type,
                                         const bool force_set_macro_meta)
{
  int ret = OB_SUCCESS;
  if (!macro_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(macro_id));
  } else if (ObDDLMacroBlockType::DDL_MB_SS_EMPTY_DATA_TYPE == block_type) {
    /* skip no logging type, not need to set meta*/
  } else if (nullptr == macro_block_buf || 0 >= size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(macro_block_buf), K(size));
  } else {
    /* shared nothing need macro_meta*/
    if (GCTX.is_shared_storage_mode() && !force_set_macro_meta) {
    } else if (OB_FAIL(ObIndexBlockRebuilder::get_macro_meta(macro_block_buf, size, macro_id, allocator_, data_macro_meta_))) {
      LOG_WARN("failed to set macro meta", K(ret),  K(macro_id), KP(macro_block_buf), K(size));
    }

    /* shared nothing need buf*/
    void *tmp_buf = nullptr;
    if (OB_FAIL(ret)) {
    } else if (ObDDLMacroBlockType::DDL_MB_INDEX_TYPE != block_type || !GCTX.is_shared_storage_mode()) {
      /* skip only index need deep copy*/
    } else if (nullptr != buf_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buf should be null when set maro meta", K(ret), K(ret));
    } else if (OB_ISNULL(tmp_buf = static_cast<char*>(allocator_.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate mem", K(ret), K(size));
    } else {
      memcpy(tmp_buf, macro_block_buf, size);
      buf_ = static_cast<char*>(tmp_buf);
      size_ = size;
    }
  }
  return ret;
}

bool ObDDLMacroBlock::is_valid() const
{
  bool ret =  block_handle_.get_block_id().is_valid()
              && DDL_MB_INVALID_TYPE != block_type_
              && ddl_start_scn_.is_valid_and_not_min()
              && scn_.is_valid_and_not_min();
  if (!GCTX.is_shared_storage_mode()) {
    ret = ret && logic_id_.is_valid() && nullptr != data_macro_meta_ && data_macro_meta_->is_valid();
  }
  return ret;
}

bool ObDDLMacroBlock::is_column_group_info_valid() const
{
  return table_key_.is_column_store_sstable() && end_row_id_ >= 0;
}

ObDDLKVHandle &ObDDLKVHandle::operator =(const ObDDLKVHandle &other)
{
  if (this != &other) {
    reset();
    if (OB_NOT_NULL(other.ddl_kv_)) {
      ddl_kv_ = other.ddl_kv_;
      ddl_kv_->inc_ref();
      t3m_ = other.t3m_;
      allocator_ = other.allocator_;
    }
  }
  return *this;
}

DEF_TO_STRING(ObDDLKVHandle)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KPC_(ddl_kv), KP_(t3m), KP_(allocator));
  J_OBJ_END();
  return pos;
}

bool ObDDLKVHandle::is_valid() const
{
  bool bret = false;
  if (nullptr == ddl_kv_) {
  } else if (ddl_kv_->is_inc_minor_ddl_kv()) {
    bret = (nullptr != t3m_) ^ (nullptr != allocator_);
  } else {
    bret = (nullptr == t3m_) & (nullptr == allocator_);
  }
  return bret;
}

int ObDDLKVHandle::set_obj(ObDDLKV *ddl_kv)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_kv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ddl_kv));
  } else {
    ddl_kv->inc_ref();
    reset();
    ddl_kv_ = ddl_kv;
  }
  return ret;
}

int ObDDLKVHandle::set_obj(ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  ObDDLKV *ddl_kv = nullptr;
  if (OB_UNLIKELY(!table_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_handle));
  } else if (OB_FAIL(table_handle.get_direct_load_memtable(ddl_kv))) {
    LOG_WARN("fail to get direct load memtable", K(ret), K(table_handle));
  } else {
    reset();
    ddl_kv_ = ddl_kv;
    ddl_kv_->inc_ref();
    t3m_ = table_handle.get_t3m();
    allocator_ = table_handle.get_allocator();
  }
  return ret;
}

void ObDDLKVHandle::reset()
{
  if (nullptr != ddl_kv_) {
    if (OB_UNLIKELY(!is_valid())) {
      LOG_ERROR_RET(OB_INVALID_ERROR, "t3m or allocator is nullptr", KP_(ddl_kv), KP_(t3m), KP_(allocator));
      ob_abort();
    } else {
      const int64_t ref_cnt = ddl_kv_->dec_ref();
      if (0 == ref_cnt) {
        if (nullptr != t3m_) {
          t3m_->push_table_into_gc_queue(ddl_kv_, ObITable::DIRECT_LOAD_MEMTABLE);
        } else if (nullptr != allocator_) {
          ddl_kv_->~ObDDLKV();
          allocator_->free(ddl_kv_);
        } else {
          MTL(ObTenantMetaMemMgr *)->release_ddl_kv(ddl_kv_);
        }
      } else if (OB_UNLIKELY(ref_cnt < 0)) {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "table ref cnt may be leaked", K(ref_cnt), KP(ddl_kv_));
      }
    }
  }
  ddl_kv_ = nullptr;
  t3m_ = nullptr;
  allocator_ = nullptr;
}

ObDDLKVPendingGuard::ObDDLKVPendingGuard(
    ObTablet *tablet,
    const SCN &scn,
    const SCN &start_scn,
    const int64_t snapshot_version, // used for shared-storage mode.
    const uint64_t data_format_version, // used for shared-storage mode.
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
    const ObDirectLoadType direct_load_type,
    const transaction::ObTransID &trans_id,
    const transaction::ObTxSEQ &seq_no,
    const ObITable::TableType table_type)
  : tablet_(tablet), scn_(scn), kv_handle_(), ret_(OB_SUCCESS)
{
  int ret = OB_SUCCESS;
  ObDDLKV *curr_kv = nullptr;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  if (OB_UNLIKELY(nullptr == tablet
      || !scn.is_valid_and_not_min()
      || !start_scn.is_valid_and_not_min()
      || snapshot_version <= 0
      || data_format_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(tablet), K(scn), K(start_scn), K(snapshot_version), K(data_format_version));
  } else if (OB_UNLIKELY(!is_full_direct_load(direct_load_type)
      && !is_incremental_direct_load(direct_load_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only support full/incremental minor(major) direct load type", KR(ret), K(direct_load_type));
  } else if (is_incremental_major_direct_load(direct_load_type)) {
    if (OB_UNLIKELY(!trans_id.is_valid() || !seq_no.is_valid() || !ObITable::is_table_type_valid(table_type))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid trans_id or seq_no for inc_major ddl kv",
          KR(ret), K(direct_load_type), K(trans_id), K(seq_no));
    } else if (OB_FAIL(tablet->get_ddl_kv_mgr(ddl_kv_mgr_handle, true/*try_create*/))) {
      LOG_WARN("get ddl kv mgr failed", K(ret));
    } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_or_create_inc_major_ddl_kv(
        scn, start_scn, snapshot_version, data_format_version, trans_id, seq_no, table_type, kv_handle_))) {
      LOG_WARN("failed to get or create inc major ddl kv", KR(ret), K(scn), K(start_scn),
          K(snapshot_version), K(data_format_version), K(trans_id), K(seq_no), K(table_type));
    }
  } else if (ObDDLUtil::use_idempotent_mode(data_format_version)) {
    if (OB_FAIL(tablet->get_ddl_kv_mgr(ddl_kv_mgr_handle, true/*try_create*/))) {
      LOG_WARN("get ddl kv mgr failed", K(ret));
    } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_or_create_idem_ddl_kv(
        scn, start_scn, snapshot_version, data_format_version, kv_handle_))) {
      LOG_WARN("acquire ddl kv failed", K(ret));
    }
  } else {
    if (OB_FAIL(tablet->get_ddl_kv_mgr(ddl_kv_mgr_handle, true /*try_create*/))) {
      LOG_WARN("get ddl kv mgr failed", K(ret));
    } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_or_create_shared_nothing_ddl_kv(
        scn, start_scn, direct_load_mgr_handle, kv_handle_))) {
      LOG_WARN("acquire ddl kv failed", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(curr_kv = kv_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, active ddl kv must not be nullptr", K(ret));
  } else {
    curr_kv->inc_pending_cnt();
    can_freeze_ = ddl_kv_mgr_handle.get_obj()->can_freeze();
  }
  if (OB_FAIL(ret)) {
    kv_handle_.reset();
    ret_ = ret;
  }
}

int ObDDLKVPendingGuard::get_ddl_kv(ObDDLKV *&kv)
{
  int ret = OB_SUCCESS;
  kv = nullptr;
  if (OB_FAIL(ret_)) {
    // do nothing
  } else {
    kv = kv_handle_.get_obj();
  }
  return ret;
}

ObDDLKVPendingGuard::~ObDDLKVPendingGuard()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret_) {
    ObDDLKV *curr_kv = kv_handle_.get_obj();
    if (nullptr != curr_kv) {
      curr_kv->dec_pending_cnt();
    }
  }
  kv_handle_.reset();
  can_freeze_ = false;
}

 int ObDDLKVPendingGuard::set_skip_block_scn(ObTablet *tablet,
                                             const share::SCN &scn,
                                             const share::SCN &start_scn,
                                             const int64_t snapshot_version,
                                             const uint64_t data_format_version,
                                             const ObDirectLoadType direct_load_type)
 {
  int ret = OB_SUCCESS;
  static const int64_t MAX_RETRY_COUNT = 10;
  ObTabletDirectLoadMgrHandle direct_load_mgr_handle;
  if (nullptr == tablet || !scn.is_valid_and_not_min() ||
     !start_scn.is_valid_and_not_min() || !is_idem_type(direct_load_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(tablet), K(scn), K(direct_load_type));
  } else {
    int64_t try_count = 0;
    while ((OB_SUCCESS == ret || OB_EAGAIN == ret) && try_count < MAX_RETRY_COUNT) {
      ObDDLKV *ddl_kv = nullptr;
      ObDDLKVPendingGuard guard(tablet, scn, start_scn,
          snapshot_version, data_format_version, direct_load_mgr_handle,
          direct_load_type);
      if (OB_FAIL(guard.get_ddl_kv(ddl_kv))) {
        LOG_WARN("get ddl kv failed", K(ret));
      } else if (OB_ISNULL(ddl_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv is null", K(ret), KP(ddl_kv), K(guard));
      } else if (OB_FAIL(ddl_kv->idem_update_max_scn(scn, direct_load_type))) {
        LOG_WARN("fail to update max scn", K(ret), K(scn), K(direct_load_type));
      } else {
        break;
      }
      if (OB_EAGAIN == ret) {
        ++try_count;
        LOG_WARN("retry get ddl kv and set macro block", K(try_count));
      }
    }
  }

  return ret;
 }


int ObDDLKVPendingGuard::set_macro_block(
    ObTablet *tablet,
    const ObDDLMacroBlock &macro_block,
    const int64_t snapshot_version,
    const uint64_t data_format_version,
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
    const ObDirectLoadType direct_load_type)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_RETRY_COUNT = 10;
  if (OB_UNLIKELY(nullptr == tablet || !macro_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(tablet), K(macro_block));
  } else if (OB_UNLIKELY(!is_full_direct_load(direct_load_type)
      && !is_incremental_direct_load(direct_load_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only support full/incremental minor(major) direct load type", KR(ret), K(direct_load_type));
  } else {
    int64_t try_count = 0;
    while ((OB_SUCCESS == ret || OB_EAGAIN == ret) && try_count < MAX_RETRY_COUNT) {
      ObDDLKV *ddl_kv = nullptr;
      ObDDLKVPendingGuard guard(tablet, macro_block.scn_, macro_block.ddl_start_scn_,
          snapshot_version, data_format_version, direct_load_mgr_handle,
          direct_load_type, macro_block.trans_id_, macro_block.seq_no_, macro_block.table_key_.table_type_);
      if (OB_FAIL(guard.get_ddl_kv(ddl_kv))) {
        LOG_WARN("get ddl kv failed", K(ret));
      } else if (OB_ISNULL(ddl_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv is null", K(ret), KP(ddl_kv), K(guard));
      } else if (OB_FAIL(ddl_kv->set_macro_block(*tablet, macro_block, snapshot_version, data_format_version, guard.can_freeze()))) {
        LOG_WARN("fail to set macro block info", K(ret), K(macro_block), K(snapshot_version), K(data_format_version));
      } else {
        break;
      }
      if (OB_EAGAIN == ret) {
        ++try_count;
        LOG_WARN("retry get ddl kv and set macro block", K(try_count));
      }
    }
  }
  return ret;
}

ObDDLMacroBlockRedoInfo::ObDDLMacroBlockRedoInfo()
  : table_key_(),
    data_buffer_(),
    block_type_(ObDDLMacroBlockType::DDL_MB_INVALID_TYPE),
    start_scn_(SCN::min_scn()),
    data_format_version_(0/*for compatibility*/),
    end_row_id_(-1),
    type_(ObDirectLoadType::DIRECT_LOAD_DDL),
    trans_id_(),
    with_cs_replica_(false),
    macro_block_id_(MacroBlockId::mock_valid_macro_id()),
    parallel_cnt_(0),
    cg_cnt_(0),
    merge_slice_idx_(0),
    seq_no_()
{
}

void ObDDLMacroBlockRedoInfo::reset()
{
  table_key_.reset();
  data_buffer_.reset();
  block_type_ = ObDDLMacroBlockType::DDL_MB_INVALID_TYPE;
  logic_id_.reset();
  start_scn_ = SCN::min_scn();
  data_format_version_ = 0;
  end_row_id_ = -1;
  type_ = ObDirectLoadType::DIRECT_LOAD_DDL;
  trans_id_.reset();
  with_cs_replica_ = false;
  macro_block_id_ = MacroBlockId::mock_valid_macro_id();
  parallel_cnt_ = 0;
  cg_cnt_ = 0;
  merge_slice_idx_ = 0;
  seq_no_.reset();
}

bool ObDDLMacroBlockRedoInfo::is_valid() const
{
  bool ret = table_key_.is_valid() && block_type_ != ObDDLMacroBlockType::DDL_MB_INVALID_TYPE
              && start_scn_.is_valid_and_not_min() && data_format_version_ >= 0 && macro_block_id_.is_valid()
              // the type is default invalid, allow default value for compatibility
              && type_ >= ObDirectLoadType::DIRECT_LOAD_INVALID && type_ < ObDirectLoadType::DIRECT_LOAD_MAX;
  if (ret && is_incremental_direct_load(type_)) {
    ret = logic_id_.is_valid() && trans_id_.is_valid();
  }

  if (ret && ObDDLMacroBlockType::DDL_MB_SS_EMPTY_DATA_TYPE != block_type_){
    /* when in ss empty type, nullptr is allowded*/
    ret = ret && !((data_buffer_.ptr() == nullptr || data_buffer_.length() == 0));
  }

  if (ret && !GCTX.is_shared_storage_mode()) {  /* for shared nothing */
    ret = logic_id_.is_valid();
  #ifdef OB_BUILD_SHARED_STORAGE
  } else if (ret && GCTX.is_shared_storage_mode()) { /* for shared storage*/
    ret = ret && (parallel_cnt_ > 0 && cg_cnt_ >0);
  #endif
  }
  return ret;
}

bool ObDDLMacroBlockRedoInfo::is_column_group_info_valid() const
{
  return table_key_.is_column_store_sstable() && end_row_id_ >= 0;
}

bool ObDDLMacroBlockRedoInfo::is_not_compat_cs_replica() const
{
  return !with_cs_replica_;
}

bool ObDDLMacroBlockRedoInfo::is_cs_replica_row_store() const
{
  return with_cs_replica_ && !table_key_.is_column_store_sstable();
}

bool ObDDLMacroBlockRedoInfo::is_cs_replica_column_store() const
{
  return with_cs_replica_ && table_key_.is_column_store_sstable();
}

OB_SERIALIZE_MEMBER(ObDDLMacroBlockRedoInfo,
                    table_key_,
                    data_buffer_,
                    block_type_,
                    logic_id_,
                    start_scn_,
                    data_format_version_,
                    end_row_id_,
                    type_,
                    trans_id_,
                    with_cs_replica_,
                    macro_block_id_,
                    parallel_cnt_,
                    cg_cnt_,
                    merge_slice_idx_,
                    seq_no_);

ObTabletDirectLoadMgrHandle::ObTabletDirectLoadMgrHandle()
  : tablet_mgr_(nullptr)
{ }

ObTabletDirectLoadMgrHandle::~ObTabletDirectLoadMgrHandle()
{
  reset();
}

int ObTabletDirectLoadMgrHandle::set_obj(ObBaseTabletDirectLoadMgr *mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret));
  } else {
    mgr->inc_ref();
    reset();
    tablet_mgr_ = mgr;
  }
  return ret;
}

ObBaseTabletDirectLoadMgr* ObTabletDirectLoadMgrHandle::get_base_obj()
{
  return tablet_mgr_;
}

const ObBaseTabletDirectLoadMgr* ObTabletDirectLoadMgrHandle::get_base_obj() const
{
  return tablet_mgr_;
}

ObTabletDirectLoadMgr* ObTabletDirectLoadMgrHandle::get_obj()
{
  ObTabletDirectLoadMgr* res = nullptr;
  if (nullptr != tablet_mgr_ && !is_idem_type(tablet_mgr_->get_direct_load_type())) {
    res = static_cast<ObTabletDirectLoadMgr*>(tablet_mgr_);
  }
  return res;
}

const ObTabletDirectLoadMgr *ObTabletDirectLoadMgrHandle::get_obj() const
{
  ObTabletDirectLoadMgr* res = nullptr;
  if (nullptr != tablet_mgr_ && !is_idem_type(tablet_mgr_->get_direct_load_type())) {
    res = static_cast<ObTabletDirectLoadMgr*>(tablet_mgr_);
  }
  return res;
}

ObTabletFullDirectLoadMgr* ObTabletDirectLoadMgrHandle::get_full_obj() const
{
  ObTabletFullDirectLoadMgr* res = nullptr;
  if (nullptr != tablet_mgr_ && !is_idem_type(tablet_mgr_->get_direct_load_type())) {
    res = static_cast<ObTabletFullDirectLoadMgr*>(tablet_mgr_);
  }
  return res;
}

ObTabletIncDirectLoadMgr* ObTabletDirectLoadMgrHandle::get_inc_obj() const
{
  ObTabletIncDirectLoadMgr* res = nullptr;
  if (nullptr != tablet_mgr_ && !is_idem_type(tablet_mgr_->get_direct_load_type())) {
    res = static_cast<ObTabletIncDirectLoadMgr*>(tablet_mgr_);
  }
  return res;
}

bool ObTabletDirectLoadMgrHandle::is_valid() const
{
  return nullptr != tablet_mgr_;
}

void ObTabletDirectLoadMgrHandle::reset()
{
  if (nullptr != tablet_mgr_) {
    if (0 == tablet_mgr_->dec_ref()) {
      if (is_idem_type(tablet_mgr_->get_direct_load_type())) {
        tablet_mgr_->~ObBaseTabletDirectLoadMgr();
      } else {
        tablet_mgr_->~ObBaseTabletDirectLoadMgr();
        MTL(ObTenantDirectLoadMgr *)->get_allocator().free(tablet_mgr_);
      }
    }
    tablet_mgr_ = nullptr;
  }
}

int ObTabletDirectLoadMgrHandle::assign(const ObTabletDirectLoadMgrHandle &other)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_LIKELY(other.is_valid())) {
    if (OB_FAIL(set_obj(other.tablet_mgr_))) {
      LOG_WARN("set obj failed", K(ret));
    }
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
ObDDLFinishLogInfo::ObDDLFinishLogInfo()
 : ls_id_(), table_key_(), data_buffer_()
{
}

bool ObDDLFinishLogInfo::is_valid() const
{
  return ls_id_.is_valid() && table_key_.is_valid() && data_buffer_.ptr() != nullptr
         && data_format_version_ >= 0 ;
}

void ObDDLFinishLogInfo::reset()
{
  ls_id_.reset();
  table_key_.reset();
  data_buffer_.reset();
  data_format_version_ = 0;
}

int ObDDLFinishLogInfo::assign(const ObDDLFinishLogInfo &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else {
    ls_id_ = other.ls_id_;
    table_key_ = other.table_key_;
    data_buffer_ = other.data_buffer_;
    data_format_version_ = other.data_format_version_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDDLFinishLogInfo, // FARM COMPAT WHITELIST
    ls_id_, table_key_, data_buffer_, data_format_version_);
#endif

ObDDLWriteStat::ObDDLWriteStat() : row_count_(0)
{ }

ObDDLWriteStat::~ObDDLWriteStat()
{ }

void ObDDLWriteStat::reset()
{
  row_count_ = 0;
}

bool ObDDLWriteStat::is_valid() const
{
  return row_count_ >= 0;
}

int ObDDLWriteStat::assign(const ObDDLWriteStat &other)
{
  int ret  = OB_SUCCESS;
  row_count_ = other.row_count_;
  return ret;
}

bool ObDDLWriteStat::operator!=(const ObDDLWriteStat &other)
{
  return row_count_ != other.row_count_;
}
OB_SERIALIZE_MEMBER(ObDDLWriteStat, row_count_);

int ObDDLTableSchema::fill_vector_index_schema_item(
    const uint64_t tenant_id,
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema *table_schema,
    ObArenaAllocator &allocator,
    const ObIArray<ObColDesc> &column_descs,
    ObDDLTableSchema &ddl_table_schema)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t , 1> col_ids;
  uint64_t with_param_table_tid;
  // for hnsw, table_schema here is snapshot table, need to get related delta buffer table.
  ObIndexType index_type = INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL;

  ObTableSchemaItem &schema_item = ddl_table_schema.table_item_;
  const ObTableSchema *data_table_schema = nullptr;

  // ivf param is saved in centroid table's schema
  if (table_schema->is_vec_ivfflat_index()) {
    index_type = INDEX_TYPE_VEC_IVFFLAT_CENTROID_LOCAL;
  } else if (table_schema->is_vec_ivfsq8_index()) {
    index_type = INDEX_TYPE_VEC_IVFSQ8_CENTROID_LOCAL;
  } else if (table_schema->is_vec_ivfpq_index()) {
    index_type = INDEX_TYPE_VEC_IVFPQ_CENTROID_LOCAL;
  }
  const ObTableSchema *with_param_table_schema = nullptr;
  // get data schema
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_schema->get_data_table_id(), data_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(table_schema->get_data_table_id()));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(table_schema->get_data_table_id()));
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_id(*data_table_schema, *table_schema, col_ids))) {
    LOG_WARN("fail to get vector index id", K(ret));
  } else if (col_ids.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid col id array", K(ret), K(col_ids));
  } else {
    if (index_type == INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL) {
      ObString index_prefix;
      if (OB_FAIL(ObPluginVectorIndexUtils::get_vector_index_prefix(*table_schema, index_prefix))) {
        LOG_WARN("failed to get index prefix", K(ret));
      } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_tid_with_index_prefix(&schema_guard,
                                                                                   *data_table_schema,
                                                                                   index_type,
                                                                                   col_ids.at(0),
                                                                                   index_prefix,
                                                                                   with_param_table_tid))) {
        LOG_WARN("failed to get index prefix", K(ret), K(index_prefix));
      }
    } else { // ivf centroid tables
      if (OB_FAIL(ObVectorIndexUtil::get_vector_index_tid(&schema_guard,
                                                          *data_table_schema,
                                                          index_type,
                                                          col_ids.at(0),
                                                          with_param_table_tid))) {
        LOG_WARN("fail to get spec vector delta buffer table id", K(ret), K(col_ids), KPC(data_table_schema));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, with_param_table_tid, with_param_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(with_param_table_tid));
  } else if (OB_ISNULL(with_param_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(with_param_table_tid));
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_dim(*with_param_table_schema, *data_table_schema, schema_item.vec_dim_))) {
    LOG_WARN("fail to get vector col dim", K(ret));
  } else if (schema_item.vec_dim_ == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get vector dim is zero, fail to calc", K(ret), K(schema_item.vec_dim_), KPC(with_param_table_schema));
  } else if (OB_FAIL(ob_write_string(allocator, with_param_table_schema->get_index_params(), schema_item.vec_idx_param_))) {
    LOG_WARN("fail to write string", K(ret), K(with_param_table_schema->get_index_params()));
  } else {
    schema_item.lob_inrow_threshold_ = data_table_schema->get_lob_inrow_threshold();
    ObIArray<ObColumnSchemaItem> &column_items = ddl_table_schema.column_items_;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
       const schema::ObColumnSchemaV2 *data_column_schema = nullptr;
       ObColumnSchemaItem &column_item = column_items.at(i);
       if (i >= table_schema->get_rowkey_column_num() && i < table_schema->get_rowkey_column_num() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()) {
         // skip multi version column, keep item invalid
       } else if (i >= column_descs.count()) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WARN("error unexpected, index is invalid", K(ret), K(i), K(column_descs));
       } else if (OB_ISNULL(data_column_schema = data_table_schema->get_column_schema(column_descs.at(i).col_id_))) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WARN("data column schema is null", K(ret), K(i), K(column_descs.at(i).col_id_));
       } else {
         column_item.column_flags_ = data_column_schema->get_column_flags();
       }
    }
  }
  return ret;
}

int ObDDLTableSchema::fill_ddl_table_schema(
    const uint64_t tenant_id,
    const uint64_t table_id,
    ObArenaAllocator &allocator,
    ObDDLTableSchema &ddl_table_schema)
{
  int ret = OB_SUCCESS;
  ObArray<ObColDesc> column_descs;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  bool is_vector_data_complement = false;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(table_schema->get_multi_version_column_descs(column_descs))) {
    LOG_WARN("get column desc array failed", K(ret));
  } else if (OB_FAIL(table_schema->get_is_column_store(ddl_table_schema.table_item_.is_column_store_))) {
    LOG_WARN("fail to get is column store", K(ret));
  } else {
    ddl_table_schema.table_id_ = table_id;
    ddl_table_schema.table_item_.is_index_table_ = table_schema->is_index_table();
    ddl_table_schema.table_item_.is_unique_index_ = table_schema->is_unique_index();
    ddl_table_schema.table_item_.rowkey_column_num_ = table_schema->get_rowkey_column_num();
    ddl_table_schema.table_item_.lob_inrow_threshold_ = table_schema->get_lob_inrow_threshold();
    ddl_table_schema.table_item_.compress_type_ = table_schema->get_compressor_type();
    ddl_table_schema.table_item_.index_type_ = table_schema->get_index_type();
    ddl_table_schema.table_item_.is_vec_tablet_rebuild_ = !table_schema->is_unavailable_index() && table_schema->is_vec_hnsw_index();

    if (OB_FAIL(ddl_table_schema.column_descs_.assign(column_descs))) {
      LOG_WARN("assign column descs failed", K(ret));
    } else if (OB_FAIL(ObDDLUtil::convert_to_storage_schema(table_schema, allocator, ddl_table_schema.storage_schema_))) {
      LOG_WARN("fail to convert to storage schema", KR(ret), KPC(table_schema));
    } else if (OB_INVALID_ID != table_schema->get_aux_lob_meta_tid()) {
      const uint64_t lob_meta_table_id = table_schema->get_aux_lob_meta_tid();
      const ObTableSchema *lob_meta_table_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, lob_meta_table_id, lob_meta_table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(lob_meta_table_id));
      } else if (OB_ISNULL(lob_meta_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", K(ret), K(tenant_id), K(lob_meta_table_id));
      } else if (OB_FAIL(ObDDLUtil::convert_to_storage_schema(lob_meta_table_schema, allocator, ddl_table_schema.lob_meta_storage_schema_))) {
        LOG_WARN("fail to convert to storage schema", KR(ret), KPC(lob_meta_table_schema));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ddl_table_schema.column_items_.reserve(column_descs.count()))) {
      LOG_WARN("reserve column schema array failed", K(ret), K(column_descs.count()), K(ddl_table_schema.column_items_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_descs.count(); ++i) {
      const ObColDesc &col_desc = column_descs.at(i);
      const schema::ObColumnSchemaV2 *column_schema = nullptr;
      ObColumnSchemaItem column_item;
      if (i >= ddl_table_schema.table_item_.rowkey_column_num_
          && i < ddl_table_schema.table_item_.rowkey_column_num_ + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()) {
        column_item.col_type_ = col_desc.col_type_; // for append_batch, skip multi version column, keep item invalid
      } else if (OB_ISNULL(column_schema = table_schema->get_column_schema(col_desc.col_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", K(ret), K(i), K(column_descs), K(col_desc.col_id_));
      } else {
        column_item.is_valid_ = true;
        column_item.col_type_ = column_schema->get_meta_type();
        if (column_schema->is_decimal_int()) {
          column_item.col_type_.set_stored_precision(column_schema->get_accuracy().get_precision());
        }
        column_item.col_accuracy_ = column_schema->get_accuracy();
        column_item.column_flags_ = column_schema->get_column_flags();
        column_item.is_rowkey_column_ = i < ddl_table_schema.table_item_.rowkey_column_num_;
        column_item.is_nullable_ = column_schema->is_nullable();
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ddl_table_schema.column_items_.push_back(column_item))) {
          LOG_WARN("push back null column schema failed", K(ret));
        } else if (column_item.col_type_.is_lob_storage()) {
          if (OB_FAIL(ddl_table_schema.lob_column_idxs_.push_back(i))) {
            LOG_WARN("push back lob column idx failed", K(ret), K(i));
          } else if (i < ddl_table_schema.table_item_.rowkey_column_num_) {
            ddl_table_schema.table_item_.has_lob_rowkey_ = true;
          }
        } else if (ObDDLUtil::need_reshape(column_item.col_type_)) {
          if (OB_FAIL(ddl_table_schema.reshape_column_idxs_.push_back(i))) {
            LOG_WARN("push back lob column idx failed", K(ret), K(i));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (FALSE_IT(is_vector_data_complement = ObDDLUtil::is_vector_index_complement(table_schema->get_index_type()))) {
      } else if (is_vector_data_complement && OB_FAIL(fill_vector_index_schema_item(tenant_id,
          schema_guard,
          table_schema,
          allocator,
          column_descs,
          ddl_table_schema))) {
        LOG_WARN("fail to prepare vector index data", K(ret));
      }
    }
  }
  return ret;
}

void ObDDLTableSchema::reset()
{
  table_id_ = 0;
  table_item_.reset();
  storage_schema_ = nullptr;
  lob_meta_storage_schema_ = nullptr;
  column_items_.reset();
  reshape_column_idxs_.reset();
  lob_column_idxs_.reset();
  column_descs_.reset();
}

int ObDDLTableSchema::assign(const ObDDLTableSchema &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(column_items_.assign(other.column_items_))) {
    LOG_WARN("assign column items failed", K(ret));
  } else if (OB_FAIL(reshape_column_idxs_.assign(other.reshape_column_idxs_))) {
    LOG_WARN("assign reshape column idx failed", K(ret));
  } else if (OB_FAIL(lob_column_idxs_.assign(other.lob_column_idxs_))) {
    LOG_WARN("assign lob column idx failed", K(ret));
  } else if (OB_FAIL(column_descs_.assign(other.column_descs_))) {
    LOG_WARN("assign column descs failed", K(ret));
  } else {
    table_id_ = other.table_id_;
    table_item_ = other.table_item_;
    storage_schema_ = other.storage_schema_;
    lob_meta_storage_schema_ = other.lob_meta_storage_schema_;
  }
  return ret;
}
