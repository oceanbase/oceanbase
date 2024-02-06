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

#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

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
  : block_handle_(), logic_id_(), block_type_(DDL_MB_INVALID_TYPE), ddl_start_scn_(SCN::min_scn()), scn_(SCN::min_scn()), buf_(nullptr), size_(0)
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


ObDDLKVPendingGuard::ObDDLKVPendingGuard(ObTablet *tablet, const SCN &start_scn, const SCN &scn)
  : tablet_(tablet), scn_(scn), kv_handle_(), ret_(OB_SUCCESS)
{
  int ret = OB_SUCCESS;
  ObDDLKV *curr_kv = nullptr;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  if (OB_UNLIKELY(nullptr == tablet || !scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(tablet), K(scn));
  } else if (OB_FAIL(tablet->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    LOG_WARN("get ddl kv mgr failed", K(ret));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_or_create_ddl_kv(*tablet, start_scn, scn, kv_handle_))) {
    LOG_WARN("acquire ddl kv failed", K(ret));
  } else if (OB_ISNULL(curr_kv = static_cast<ObDDLKV *>(kv_handle_.get_table()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, active ddl kv must not be nullptr", K(ret));
  } else {
    curr_kv->inc_pending_cnt();
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
    kv = static_cast<ObDDLKV *>(kv_handle_.get_table());
  }
  return ret;
}

ObDDLKVPendingGuard::~ObDDLKVPendingGuard()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret_) {
    ObDDLKV *curr_kv = static_cast<ObDDLKV *>(kv_handle_.get_table());
    if (nullptr != curr_kv) {
      curr_kv->dec_pending_cnt();
    }
  }
  kv_handle_.reset();
}

int ObDDLKVPendingGuard::set_macro_block(ObTablet *tablet, const ObDDLMacroBlock &macro_block)
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
      ObDDLKVPendingGuard guard(tablet, macro_block.ddl_start_scn_, macro_block.scn_);
      if (OB_FAIL(guard.get_ddl_kv(ddl_kv))) {
        LOG_WARN("get ddl kv failed", K(ret));
      } else if (OB_ISNULL(ddl_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv is null", K(ret), KP(ddl_kv), K(guard));
      } else if (OB_FAIL(ddl_kv->set_macro_block(*tablet, macro_block))) {
        LOG_WARN("fail to set macro block info", K(ret));
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
