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

#include "storage/ls/ob_ls.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"
#include "storage/ob_i_table.h"

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
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(block_id))) {
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
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(block_id_))) {
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
    buf_(nullptr),
    size_(0),
    table_key_(),
    end_row_id_(-1),
    trans_id_()
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
  } else if (OB_ISNULL(tmp_buf = allocator.alloc(size_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for macro block buffer failed", K(ret));
  } else {
    memcpy(tmp_buf, buf_, size_);
    dst_block.buf_ = reinterpret_cast<const char*>(tmp_buf);
    dst_block.size_ = size_;
    dst_block.block_type_ = block_type_;
    dst_block.block_handle_ = block_handle_;
    dst_block.logic_id_ = logic_id_;
    dst_block.ddl_start_scn_ = ddl_start_scn_;
    dst_block.scn_ = scn_;
    dst_block.table_key_ = table_key_;
    dst_block.end_row_id_ = end_row_id_;
  }
  return ret;
}

bool ObDDLMacroBlock::is_valid() const
{
  return block_handle_.get_block_id().is_valid()
    && logic_id_.is_valid()
    && DDL_MB_INVALID_TYPE != block_type_
    && ddl_start_scn_.is_valid_and_not_min()
    && scn_.is_valid_and_not_min()
    && nullptr != buf_
    && size_ > 0;
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

ObDDLKVPendingGuard::ObDDLKVPendingGuard(ObTablet *tablet, const SCN &scn, const SCN &start_scn,
  ObTabletDirectLoadMgrHandle &direct_load_mgr_handle)
  : tablet_(tablet), scn_(scn), kv_handle_(), ret_(OB_SUCCESS)
{
  int ret = OB_SUCCESS;
  ObDDLKV *curr_kv = nullptr;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  if (OB_UNLIKELY(nullptr == tablet || !scn.is_valid_and_not_min() || !direct_load_mgr_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(tablet), K(scn), KPC(direct_load_mgr_handle.get_obj()));
  } else if (OB_FAIL(tablet->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    LOG_WARN("get ddl kv mgr failed", K(ret));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_or_create_ddl_kv(
    scn, start_scn, direct_load_mgr_handle, kv_handle_))) {
    LOG_WARN("acquire ddl kv failed", K(ret));
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
      ObDDLKVPendingGuard guard(tablet, macro_block.scn_, macro_block.ddl_start_scn_, direct_load_mgr_handle);
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
    trans_id_()
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
}

bool ObDDLMacroBlockRedoInfo::is_valid() const
{
  return table_key_.is_valid() && data_buffer_.ptr() != nullptr && block_type_ != ObDDLMacroBlockType::DDL_MB_INVALID_TYPE
         && logic_id_.is_valid() && start_scn_.is_valid_and_not_min() && data_format_version_ >= 0
         && type_ < ObDirectLoadType::DIRECT_LOAD_MAX
         && (is_incremental_direct_load(type_) ? trans_id_.is_valid() : !trans_id_.is_valid());
}

bool ObDDLMacroBlockRedoInfo::is_column_group_info_valid() const
{
  return table_key_.is_column_store_sstable() && end_row_id_ >= 0;
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
                    trans_id_);

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