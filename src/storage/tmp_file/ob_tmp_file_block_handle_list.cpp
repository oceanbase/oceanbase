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

#include "storage/tmp_file/ob_tmp_file_block_handle_list.h"
#include "storage/tmp_file/ob_tmp_file_block.h"

namespace oceanbase
{
namespace tmp_file
{

int PrintOperator::operator()(ObTmpFileBlkNode *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block node is null", KR(ret), KPC(node));
  } else {
    LOG_INFO("print_block", KCSTRING(hint_), K(node->block_));
  }
  return true;
}

void ObTmpFileBlockHandleList::destroy()
{
  if (ListType::INVALID != type_) {
    type_ = ListType::INVALID;
    list_.clear();
  }
}

int ObTmpFileBlockHandleList::init(ListType type)
{
  int ret = OB_SUCCESS;
  if (ListType::INVALID != type_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTmpFileBlockHandleList init twice", K(ret), K(type_), K(type));
  } else {
    type_ = type;
  }
  return ret;
}

int ObTmpFileBlockHandleList::append(ObTmpFileBlockHandle handle)
{
  int ret = OB_SUCCESS;
  if (!handle.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(handle));
  } else {
    ObSpinLockGuard guard(lock_);
    if (OB_UNLIKELY(!list_.add_last(get_blk_node_(handle)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to add block handle", KR(ret), K(handle));
    } else {
      handle->inc_ref_cnt();
    }
  }
  return ret;
}

int ObTmpFileBlockHandleList::remove(ObTmpFileBlockHandle handle)
{
  bool unused = false;
  return remove(handle, unused);
}

int ObTmpFileBlockHandleList::remove(ObTmpFileBlockHandle handle, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = true;
  if (!handle.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(handle));
  } else {
    ObSpinLockGuard guard(lock_);
    ObTmpFileBlkNode *node = get_blk_node_(handle);
    if (OB_ISNULL(node->get_prev()) && OB_ISNULL(node->get_next())) {
      is_exist = false;
    } else if (OB_UNLIKELY(!list_.remove(node))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to remove block handle", KR(ret), K(handle));
    } else {
      handle->dec_ref_cnt();
    }
  }
  return ret;
}

int ObTmpFileBlockHandleList::remove_without_lock_(ObTmpFileBlockHandle handle)
{
  int ret = OB_SUCCESS;
  if (!handle.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(handle));
  } else {
    ObTmpFileBlkNode *node = get_blk_node_(handle);
    if (OB_ISNULL(node->get_prev()) && OB_ISNULL(node->get_next())) {
      // do nothing
    } else if (OB_UNLIKELY(!list_.remove(node))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to remove block handle", KR(ret), K(handle));
    } else {
      handle->dec_ref_cnt();
    }
  }
  return ret;
}

ObTmpFileBlockHandle ObTmpFileBlockHandleList::pop_first()
{
  int ret = OB_SUCCESS;
  ObTmpFileBlockHandle handle;
  ObTmpFileBlkNode *node = nullptr;
  {
    ObSpinLockGuard guard(lock_);
    node = list_.remove_first();
  }

  if (OB_NOT_NULL(node)) {
    ObTmpFileBlock &block = node->block_;
    if (OB_FAIL(handle.init(&block))) {
      LOG_ERROR("fail to init block handle", KR(ret), K(block));
    } else {
      handle->dec_ref_cnt();
    }
  }

  return handle;
}

bool ObTmpFileBlockHandleList::is_empty()
{
  ObSpinLockGuard guard(lock_);
  return list_.is_empty();
}

int64_t ObTmpFileBlockHandleList::size()
{
  ObSpinLockGuard guard(lock_);
  return list_.get_size();
}

ObTmpFileBlkNode *ObTmpFileBlockHandleList::get_blk_node_(ObTmpFileBlockHandle handle)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlkNode *node = nullptr;
  if (ListType::PREALLOC_NODE == type_) {
    node = &(handle->get_prealloc_blk_node());
  } else if (ListType::FLUSH_NODE == type_) {
    node = &(handle->get_flush_blk_node());
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid list type", K(ret), K(type_));
  }
  return node;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
