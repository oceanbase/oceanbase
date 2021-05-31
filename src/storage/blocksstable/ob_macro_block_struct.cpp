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

#include "ob_macro_block_struct.h"
#include "storage/ob_tenant_file_struct.h"

namespace oceanbase {
namespace blocksstable {
ObMacroBlocksWriteCtx::ObMacroBlocksWriteCtx()
    : allocator_(ObModIds::OB_MACRO_BLOCK_WRITE_CTX),
      file_ctx_(allocator_),
      macro_block_list_(),
      file_handle_(),
      file_(NULL)
{}

ObMacroBlocksWriteCtx::~ObMacroBlocksWriteCtx()
{
  reset();
}

bool ObMacroBlocksWriteCtx::is_valid() const
{
  return macro_block_meta_list_.count() >= macro_block_list_.count();
}

void ObMacroBlocksWriteCtx::clear()
{
  int ret = OB_SUCCESS;
  blocksstable::ObStorageFile* file = file_;
  if (macro_block_list_.count() != 0) {
    if (OB_ISNULL(file) && OB_ISNULL(file = file_handle_.get_storage_file())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get file", K(ret), K_(file_handle));
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < macro_block_list_.count(); ++i) {
      if (OB_FAIL(file->dec_ref(macro_block_list_.at(i)))) {
        LOG_ERROR("failed to dec ref of pg_file",
            K(ret),
            K(macro_block_list_.count()),
            K(i),
            K(*file),
            K(macro_block_list_.at(i)));
      }
    }
  }

  file_ = NULL;
  file_ctx_.reset();
  macro_block_list_.reset();
  for (int64_t i = 0; i < macro_block_meta_list_.count(); ++i) {
    ObMacroBlockMetaV2* meta = const_cast<ObMacroBlockMetaV2*>(macro_block_meta_list_.at(i).meta_);
    ObMacroBlockSchemaInfo* schema = const_cast<ObMacroBlockSchemaInfo*>(macro_block_meta_list_.at(i).schema_);
    if (nullptr != meta) {
      meta->~ObMacroBlockMetaV2();
      allocator_.free(meta);
    }
    if (nullptr != schema) {
      schema->~ObMacroBlockSchemaInfo();
      allocator_.free(schema);
    }
  }
  macro_block_meta_list_.reset();
  allocator_.reset();
}

void ObMacroBlocksWriteCtx::reset()
{
  clear();
  file_handle_.reset();
}

int ObMacroBlocksWriteCtx::set(ObMacroBlocksWriteCtx& src)
{
  int ret = OB_SUCCESS;

  if (!is_empty()) {
    ret = OB_NO_EMPTY_ENTRY;
    LOG_WARN("not empty, cannot transfer new macro blocks", K(ret), K(*this));
  } else if (!src.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObMacroBlocksWriteCtx is not valid", K(ret), K(src));
  } else if (OB_FAIL(file_ctx_.assign(src.file_ctx_))) {
    LOG_WARN("failed to assign file ctx", K(ret));
  } else if (OB_FAIL(file_handle_.assign(src.file_handle_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to assign file handle", K(ret), K(src));
  } else {
    file_ = src.file_;
    for (int64_t i = 0; OB_SUCC(ret) && i < src.macro_block_list_.count(); ++i) {
      if (OB_FAIL(add_macro_block(src.macro_block_list_.at(i), src.macro_block_meta_list_.at(i)))) {
        LOG_WARN("failed to add macro block", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    src.clear();
  } else {  // handle failed
    LOG_WARN("failed to assign macro blocks write ctx, clear dest ctx", K(ret));
    clear();
  }
  return ret;
}

int ObMacroBlocksWriteCtx::add_macro_block_id(const MacroBlockId& macro_block_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(macro_block_id));
  } else if (OB_FAIL(macro_block_list_.push_back(macro_block_id))) {
    LOG_WARN("fail to push back macro block id", K(ret));
  } else {
    blocksstable::ObStorageFile* file = file_;
    if (OB_ISNULL(file) && OB_ISNULL(file = file_handle_.get_storage_file())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get file", K(ret), K_(file_handle));
    } else if (OB_FAIL(file->inc_ref(macro_block_id))) {
      LOG_ERROR("failed to inc ref of pg_file", K(ret));
    }
    if (OB_FAIL(ret)) {
      macro_block_list_.pop_back();
    }
  }
  return ret;
}

int ObMacroBlocksWriteCtx::add_macro_block_meta(const blocksstable::ObFullMacroBlockMeta& full_meta)
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaV2* dst_macro_block_meta = nullptr;
  ObMacroBlockSchemaInfo* dst_macro_schema_info = nullptr;
  if (OB_UNLIKELY(!full_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(full_meta));
  } else if (OB_FAIL(full_meta.meta_->deep_copy(dst_macro_block_meta, allocator_))) {
    LOG_WARN("fail to deep copy macro block meta", K(ret));
  } else if (OB_FAIL(full_meta.schema_->deep_copy(dst_macro_schema_info, allocator_))) {
    LOG_WARN("fail to deep copy schema info", K(ret));
  } else if (OB_FAIL(
                 macro_block_meta_list_.push_back(ObFullMacroBlockMeta(dst_macro_schema_info, dst_macro_block_meta)))) {
    LOG_WARN("fail to push back macro block meta", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != dst_macro_block_meta) {
      dst_macro_block_meta->~ObMacroBlockMetaV2();
      dst_macro_block_meta = nullptr;
    }
    if (nullptr != dst_macro_schema_info) {
      dst_macro_schema_info->~ObMacroBlockSchemaInfo();
      dst_macro_schema_info = nullptr;
    }
  }
  return ret;
}

int ObMacroBlocksWriteCtx::add_macro_block(
    const MacroBlockId& macro_block_id, const blocksstable::ObFullMacroBlockMeta& macro_block_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_block_id.is_valid() || !macro_block_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments",
        K(ret),
        K(macro_block_id),
        K(macro_block_meta),
        K(macro_block_meta.meta_->is_valid()),
        K(macro_block_meta.schema_->is_valid()));
  } else if (OB_FAIL(add_macro_block_meta(macro_block_meta))) {
    LOG_WARN("fail to add macro block meta", K(ret));
  } else if (OB_FAIL(add_macro_block_id(macro_block_id))) {
    LOG_WARN("fail to add macro block id", K(ret));
  }
  return ret;
}
}  // end namespace blocksstable
}  // end namespace oceanbase
