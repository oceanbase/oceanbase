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

#include "ob_interm_macro_mgr.h"
#include "storage/blocksstable/ob_store_file.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;

ObIntermMacroKey::ObIntermMacroKey() : execution_id_(OB_INVALID_ID), task_id_(OB_INVALID_ID)
{}

ObIntermMacroKey::~ObIntermMacroKey()
{}

int64_t ObIntermMacroKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&execution_id_, sizeof(execution_id_), hash_val);
  hash_val = murmurhash(&task_id_, sizeof(task_id_), hash_val);
  return hash_val;
}

bool ObIntermMacroKey::operator==(const ObIntermMacroKey& other) const
{
  return execution_id_ == other.execution_id_ && task_id_ == other.task_id_;
}

OB_SERIALIZE_MEMBER(ObIntermMacroKey, execution_id_, task_id_);

ObIntermMacroValue::ObIntermMacroValue() : macro_block_write_ctx_()
{}

ObIntermMacroValue::~ObIntermMacroValue()
{
  reset();
}

int ObIntermMacroValue::set_macro_block(blocksstable::ObMacroBlocksWriteCtx& macro_block_ctx)
{
  int ret = OB_SUCCESS;

  if (!macro_block_write_ctx_.is_empty()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot set macro block twice", K(ret), K(macro_block_write_ctx_));
  } else if (!macro_block_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(macro_block_ctx));
  } else if (OB_FAIL(macro_block_write_ctx_.set(macro_block_ctx))) {
    LOG_WARN("failed to set macro block ctx", K(ret), K(macro_block_ctx));
  }
  return ret;
}

void ObIntermMacroValue::reset()
{
  macro_block_write_ctx_.reset();
}

int ObIntermMacroValue::deep_copy(char* buf, const int64_t buf_len, ObIntermMacroValue*& value)
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < deep_copy_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), K(deep_copy_size));
  } else {
    ObIntermMacroValue* pvalue = new (buf) ObIntermMacroValue();
    if (OB_FAIL(pvalue->macro_block_write_ctx_.set(macro_block_write_ctx_))) {
      LOG_WARN("failed to set macro block", K(ret), K(macro_block_write_ctx_));
    }
    value = pvalue;
  }
  return ret;
}

ObIntermMacroHandle::ObIntermMacroHandle() : ObResourceHandle<ObIntermMacroValue>()
{}

ObIntermMacroHandle::~ObIntermMacroHandle()
{
  reset();
}

void ObIntermMacroHandle::reset()
{
  if (NULL != ptr_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObIntermMacroMgr::get_instance().dec_handle_ref(*this))) {
      LOG_WARN("fail to dec handle ref", K(tmp_ret));
    }
  }
}

ObIntermMacroMgr::ObIntermMacroMgr() : map_(), is_inited_(false)
{}

ObIntermMacroMgr::~ObIntermMacroMgr()
{
  destroy();
}

int ObIntermMacroMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObIntermMacroMgr has already been inited", K(ret));
  } else if (OB_FAIL(
                 map_.init(DEFAULT_BUCKET_NUM, ObModIds::OB_INTERM_MACRO_MGR, TOTAL_LIMIT, HOLD_LIMIT, PAGE_SIZE))) {
    LOG_WARN("fail to init ObResourceMap", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObIntermMacroMgr::put(const ObIntermMacroKey& key, blocksstable::ObMacroBlocksWriteCtx& macro_block_ctx)
{
  int ret = OB_SUCCESS;
  ObIntermMacroValue value;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIntermMacroMgr has not been inited", K(ret));
  } else if (!macro_block_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_block_ctx));
  } else if (OB_FAIL(value.set_macro_block(macro_block_ctx))) {
    LOG_WARN("failed to set macro block", K(ret), K(macro_block_ctx));
  } else if (OB_FAIL(map_.set(key, value))) {
    LOG_WARN("fail to set to map", K(ret));
  }

  return ret;
}

int ObIntermMacroMgr::get(const ObIntermMacroKey& key, ObIntermMacroHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIntermMacroMgr has not been inited", K(ret));
  } else if (OB_FAIL(map_.get(key, handle))) {
    LOG_WARN("fail to get from map", K(ret));
  }
  return ret;
}

int ObIntermMacroMgr::remove(const ObIntermMacroKey& key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIntermMacroMgr has not been inited", K(ret));
  } else if (OB_FAIL(map_.erase(key))) {
    LOG_WARN("fail to erase from map", K(ret));
  }
  return ret;
}

void ObIntermMacroMgr::destroy()
{
  map_.destroy();
  is_inited_ = false;
}

int ObIntermMacroMgr::dec_handle_ref(ObIntermMacroHandle& handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObIntermMacroMgr has not been inited", K(ret));
  } else if (OB_FAIL(map_.dec_handle_ref(handle.ptr_))) {
    LOG_WARN("fail to dec handle ref", K(ret));
  }
  return ret;
}

ObIntermMacroMgr& ObIntermMacroMgr::get_instance()
{
  static ObIntermMacroMgr instance;
  return instance;
}
