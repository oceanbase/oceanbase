/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_sstable_private_object_cleaner.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"

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
}

int ObSSTableObjectCleanerFactory::build_object_cleaner(const ObDataStoreDesc &data_store_desc, ObIAllocator &allocator, ObISSTableObjectCleaner *&cleaner)
{
  int ret = OB_SUCCESS;
  if (GCTX.is_shared_storage_mode()) {
    cleaner = nullptr;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to build object cleaner", K(ret));
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
