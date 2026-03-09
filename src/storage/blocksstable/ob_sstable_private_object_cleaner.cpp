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

#include "ob_sstable_private_object_cleaner.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/incremental/ob_ss_compact_object_cleaner.h"
#endif

namespace oceanbase
{
namespace blocksstable
{

ObISSTableObjectCleaner::~ObISSTableObjectCleaner()
{
}
DEF_TO_STRING(ObISSTableObjectCleaner)
{
  return 0;
}

int ObISSTableObjectCleaner::get_cleaner_from_data_store_desc(const ObDataStoreDesc &data_store_desc, ObISSTableObjectCleaner *&cleaner)
{
  int ret = OB_SUCCESS;
  ObISSTableObjectCleaner *object_cleaner = nullptr;
  if (OB_UNLIKELY(!data_store_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data store desc is invalid", K(ret), K(data_store_desc));
  } else if (OB_ISNULL(data_store_desc.sstable_index_builder_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable index builder is null", K(ret), K(data_store_desc));
  } else if (OB_ISNULL(object_cleaner = data_store_desc.sstable_index_builder_->get_object_cleaner())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("object cleaner is null", K(ret), K(data_store_desc));
  } else {
    cleaner = object_cleaner;
  }
  return ret;
}

ObSSTablePrivateObjectCleaner::ObSSTablePrivateObjectCleaner()
    : new_macro_block_ids_(),
      lock_(common::ObLatchIds::OB_SSTABLE_PRIVATE_OBJECT_CLEANER_LOCK),
      is_ss_mode_(false),
      task_succeed_(false)
{
  new_macro_block_ids_.set_attr(ObMemAttr(MTL_ID(), "MaWriterCleaner"));
  is_ss_mode_ = GCTX.is_shared_storage_mode();
}

ObSSTablePrivateObjectCleaner::~ObSSTablePrivateObjectCleaner()
{
  reset();
}

void ObSSTablePrivateObjectCleaner::reset()
{
  if (OB_UNLIKELY(!ATOMIC_LOAD(&task_succeed_))) {
    clean();
  }
  ATOMIC_SET(&task_succeed_, false);
  new_macro_block_ids_.reset();
}

int ObSSTablePrivateObjectCleaner::add_new_macro_block_id(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  // Only GC private data or meta.
  if (is_ss_mode_ && macro_id.is_private_data_or_meta()) {
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(new_macro_block_ids_.push_back(macro_id))) {
      LOG_WARN("fail to add new macro block id", K(ret), K(macro_id));
    }
  }
#endif
  return ret;
}

int ObSSTablePrivateObjectCleaner::mark_succeed()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ATOMIC_LOAD(&task_succeed_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("double mark", K(ret), K(ATOMIC_LOAD(&task_succeed_)));
  } else {
    ATOMIC_SET(&task_succeed_, true);
  }
  return ret;
}

void ObSSTablePrivateObjectCleaner::clean()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  // Actively GC only enabled in shared storage mode.
  if (is_ss_mode_) {
    SpinRLockGuard guard(lock_);
    if (new_macro_block_ids_.count() == 0) {
      // do nothing.
    } else if (OB_FAIL(MTL(ObTenantFileManager*)->delete_files(new_macro_block_ids_))) {
      LOG_WARN("fail to clean in sstable private object cleaner", K(ret), KP(this), K(new_macro_block_ids_.count()));
    }
  }
#endif
}

int ObSSTableObjectCleanerFactory::build_object_cleaner(const ObDataStoreDesc &data_store_desc, ObIAllocator &allocator, ObISSTableObjectCleaner *&cleaner)
{
  int ret = OB_SUCCESS;
  if (GCTX.is_shared_storage_mode()) {
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_FAIL(ObSSTableSharedObjectCacheCleaner::build(data_store_desc, allocator, cleaner))) {
      LOG_WARN("fail to build shared object cleaner", K(ret), K(data_store_desc));
    }
#else
    cleaner = nullptr;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to build object cleaner", K(ret));
#endif
  } else {
    cleaner = OB_NEWx(ObSSTablePrivateObjectCleaner, &allocator);
    if (OB_ISNULL(cleaner)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  return ret;
}
} // namespace blocksstable
} // namespace oceanbase