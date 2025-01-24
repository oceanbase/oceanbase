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
    size_(0)
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

bool ObDDLKVHandle::is_valid() const
{
  bool bret = false;
  if (nullptr == ddl_kv_) {
  } else if (ddl_kv_->is_inc_ddl_kv()) {
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
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle)
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
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (ObDDLUtil::use_idempotent_mode(data_format_version)) {
    if (OB_FAIL(tablet->get_ddl_kv_mgr(ddl_kv_mgr_handle, true/*try_create*/))) {
      LOG_WARN("get ddl kv mgr failed", K(ret));
    } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_or_create_shared_storage_ddl_kv(
        scn, start_scn, snapshot_version, data_format_version, kv_handle_))) {
      LOG_WARN("acquire ddl kv failed", K(ret));
    }
#endif
  } else {
    if (OB_FAIL(tablet->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
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

int ObDDLKVPendingGuard::set_macro_block(
    ObTablet *tablet,
    const ObDDLMacroBlock &macro_block,
    const int64_t snapshot_version,
    const uint64_t data_format_version,
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_RETRY_COUNT = 10;
  if (OB_UNLIKELY(nullptr == tablet || !macro_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(tablet), K(macro_block));
  } else {
    int64_t try_count = 0;
    while ((OB_SUCCESS == ret || OB_EAGAIN == ret) && try_count < MAX_RETRY_COUNT) {
      ObDDLKV *ddl_kv = nullptr;
      ObDDLKVPendingGuard guard(tablet, macro_block.scn_, macro_block.ddl_start_scn_,
          snapshot_version, data_format_version, direct_load_mgr_handle);
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
    type_(ObDirectLoadType::DIRECT_LOAD_INVALID),
    trans_id_(),
    with_cs_replica_(false),
    macro_block_id_(MacroBlockId::mock_valid_macro_id()),
    parallel_cnt_(0),
    cg_cnt_(0)
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
  type_ = ObDirectLoadType::DIRECT_LOAD_INVALID;
  trans_id_.reset();
  with_cs_replica_ = false;
  macro_block_id_ = MacroBlockId::mock_valid_macro_id();
  parallel_cnt_ = 0;
  cg_cnt_ = 0;
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
                    cg_cnt_);

ObTabletDirectLoadMgrHandle::ObTabletDirectLoadMgrHandle()
  : tablet_mgr_(nullptr)
{ }

ObTabletDirectLoadMgrHandle::~ObTabletDirectLoadMgrHandle()
{
  reset();
}

int ObTabletDirectLoadMgrHandle::set_obj(ObTabletDirectLoadMgr *mgr)
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

ObTabletDirectLoadMgr* ObTabletDirectLoadMgrHandle::get_obj()
{
  return tablet_mgr_;
}

const ObTabletDirectLoadMgr *ObTabletDirectLoadMgrHandle::get_obj() const
{
  return tablet_mgr_;
}

ObTabletFullDirectLoadMgr* ObTabletDirectLoadMgrHandle::get_full_obj() const
{
  return static_cast<ObTabletFullDirectLoadMgr *>(tablet_mgr_);
}

ObTabletIncDirectLoadMgr* ObTabletDirectLoadMgrHandle::get_inc_obj() const
{
  return static_cast<ObTabletIncDirectLoadMgr *>(tablet_mgr_);
}

bool ObTabletDirectLoadMgrHandle::is_valid() const
{
  return nullptr != tablet_mgr_;
}

void ObTabletDirectLoadMgrHandle::reset()
{
  if (nullptr != tablet_mgr_) {
    if (0 == tablet_mgr_->dec_ref()) {
      tablet_mgr_->~ObTabletDirectLoadMgr();
      MTL(ObTenantDirectLoadMgr *)->get_allocator().free(tablet_mgr_);
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
 : ls_id_(), table_key_(), data_buffer_(), macro_block_id_()
{
}

bool ObDDLFinishLogInfo::is_valid() const
{
  return ls_id_.is_valid() && table_key_.is_valid() && data_buffer_.ptr() != nullptr
         &&  macro_block_id_.is_valid() && data_format_version_ >= 0 ;
}

void ObDDLFinishLogInfo::reset()
{
  ls_id_.reset();
  table_key_.reset();
  data_buffer_.reset();
  macro_block_id_.reset();
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
    macro_block_id_ = other.macro_block_id_;
    data_buffer_ = other.data_buffer_;
    data_format_version_ = other.data_format_version_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDDLFinishLogInfo, ls_id_, table_key_, data_buffer_, macro_block_id_, data_format_version_);
#endif
