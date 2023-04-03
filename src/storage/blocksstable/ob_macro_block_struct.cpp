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

#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_macro_block_meta.h"
#include "storage/blocksstable/ob_sstable_meta.h"

namespace oceanbase
{
namespace blocksstable
{
ObMacroBlocksWriteCtx::ObMacroBlocksWriteCtx()
  : allocator_(ObModIds::OB_MACRO_BLOCK_WRITE_CTX, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    macro_block_list_(),
    use_old_macro_block_count_(0)
{
}

ObMacroBlocksWriteCtx::~ObMacroBlocksWriteCtx()
{
  reset();
}

void ObMacroBlocksWriteCtx::clear()
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
   for (int64_t i = 0; i < macro_block_list_.count(); ++i) {
     if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_block_list_.at(i)))) {
       LOG_ERROR("failed to dec macro block ref cnt", K(ret), K(macro_block_list_.count()), K(i),
                                                   "macro id", macro_block_list_.at(i));
     }
   }
  }

  macro_block_list_.reset();
  allocator_.reset();
  use_old_macro_block_count_ = 0;
}

void ObMacroBlocksWriteCtx::reset()
{
  clear();
}

int ObMacroBlocksWriteCtx::set(ObMacroBlocksWriteCtx &src)
{
  int ret = OB_SUCCESS;

  if (!is_empty()) {
    ret = OB_NO_EMPTY_ENTRY;
    LOG_WARN("not empty, cannot transfer new macro blocks", K(ret), K(*this));
  } else {
    use_old_macro_block_count_ = src.use_old_macro_block_count_;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < src.macro_block_list_.count(); ++i) {
    if (OB_FAIL(add_macro_block_id(src.macro_block_list_.at(i)))) {
      LOG_WARN("fail to add macro block", K(ret), K(i));
    }
  }

  if (OB_SUCC(ret)) {
    src.clear();
  } else { // handle failed
    LOG_WARN("failed to assign macro blocks write ctx, clear dest ctx", K(ret));
    clear();
  }
  return ret;
}

int ObMacroBlocksWriteCtx::deep_copy(ObMacroBlocksWriteCtx *&dst, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObMacroBlocksWriteCtx &src = *this;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMacroBlocksWriteCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
  } else if (OB_ISNULL(dst = new (buf) ObMacroBlocksWriteCtx())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to new a ObMacroBlocksWriteCtx", K(ret));
  } else if (OB_FAIL(dst->set(src))) {
    STORAGE_LOG(WARN, "fail to set macro block write ctx", K(ret), K(src));
  }

  if (OB_FAIL(ret) && OB_UNLIKELY(dst != nullptr)) {
    dst->~ObMacroBlocksWriteCtx();
    allocator.free(buf);
    dst = nullptr;
  }
  return ret;
}

int ObMacroBlocksWriteCtx::get_macro_id_array(ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_list_.count(); ++i) {
    MacroBlockId &block_id= macro_block_list_.at(i);
    if (OB_FAIL(block_ids.push_back(block_id))) {
      LOG_WARN("failed to push back block id", K(ret));
    } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(block_id))) {
      block_ids.pop_back();
      LOG_ERROR("failed to inc macro block ref cnt", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < block_ids.count(); ++i) {
      const MacroBlockId &block_id = block_ids.at(i);
      if (OB_SUCCESS != (tmp_ret = OB_SERVER_BLOCK_MGR.dec_ref(block_id))) {
        STORAGE_LOG(ERROR, "fail to dec macro block ref cnt", K(tmp_ret), K(block_id), K(i));
      }
    }
  }
  return ret;
}

int ObMacroBlocksWriteCtx::add_macro_block_id(const MacroBlockId &macro_block_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(macro_block_id));
  } else if (OB_FAIL(macro_block_list_.push_back(macro_block_id))) {
    LOG_WARN("fail to push back macro block id", K(ret));
  } else {
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_block_id))) {
      LOG_ERROR("failed to inc macro block ref cnt", K(ret));
    }
    if (OB_FAIL(ret)) {
      macro_block_list_.pop_back();
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
