/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL

#include "ob_pcached_external_file_service.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "share/backup/ob_backup_io_adapter.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#endif

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace sql
{
/*-----------------------------------------ObPCachedExtMacroKey-----------------------------------------*/
ObPCachedExtMacroKey::ObPCachedExtMacroKey()
    : url_(), file_version_(), offset_(-1)
{}

ObPCachedExtMacroKey::ObPCachedExtMacroKey(
    const common::ObString &url,
    const common::ObString &content_digest,
    const int64_t modify_time,
    const int64_t offset)
    : url_(url), file_version_(content_digest, modify_time), offset_(offset)
{}

bool ObPCachedExtMacroKey::is_valid() const
{
  return !url_.empty() && file_version_.is_valid() && offset_ >= 0
      && OB_STORAGE_OBJECT_MGR.get_macro_block_size() > 0;
}

int64_t ObPCachedExtMacroKey::offset_idx() const
{
  return offset_ / OB_STORAGE_OBJECT_MGR.get_macro_block_size();
}

int64_t ObPCachedExtMacroKey::block_start_offset() const
{
  return offset_idx() * OB_STORAGE_OBJECT_MGR.get_macro_block_size();
}

int ObPCachedExtMacroKey::assign(const ObPCachedExtMacroKey &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_UNLIKELY(!other.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(other), KPC(this));
    } else {
      url_ = other.url_;
      file_version_ = other.file_version_;
      offset_ = other.offset_;
    }
  }
  return ret;
}

/*-----------------------------------------ObExternalFilePathMap-----------------------------------------*/
ObExternalFilePathMap::ObExternalFilePathMap()
    : is_inited_(false),
      mem_limiter_(),
      allocator_(),
      path_id_map_(),
      macro_id_map_(),
      sn_path_id_(0)
{}

ObExternalFilePathMap::~ObExternalFilePathMap()
{
  destroy();
}

void ObExternalFilePathMap::destroy()
{
  int ret = OB_SUCCESS;
  sn_path_id_ = 0;
  is_inited_ = false;
  hash::ObHashMap<PathMapKey, uint64_t>::iterator it = path_id_map_.begin();
  while (OB_SUCC(ret) && it != path_id_map_.end()) {
    if (OB_LIKELY(!it->first.url_.empty())) {
      allocator_.free(it->first.url_.ptr());
      it->first.url_.reset();
    }
    if (OB_LIKELY(!it->first.content_digest_.empty())) {
      allocator_.free(it->first.content_digest_.ptr());
      it->first.content_digest_.reset();
    }
    it++;
  }
  path_id_map_.destroy();
  macro_id_map_.destroy();
  allocator_.destroy();
}

int64_t ObExternalFilePathMap::bucket_count() const
{
  return macro_id_map_.bucket_count();
}

int ObExternalFilePathMap::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t path_id_map_bucket_num = -1;
  int64_t macro_id_map_bucket_num = -1;
  const int64_t tenant_mem_in_gb = MTL_MEM_SIZE() / GB;
  if (tenant_mem_in_gb <= 4) { // tenant_mem <= 4GB
    // 12289 buckets. actually cal_next_prime return 12289 buckets, which cost 0.288MB memory
    path_id_map_bucket_num = 12289;
    // 49157 buckets. actually cal_next_prime return 24593 buckets, which cost 1.151MB memory
    macro_id_map_bucket_num = 49157;
  } else if (tenant_mem_in_gb <= 16) {
    // 49157 buckets. actually cal_next_prime return 49157 buckets, which cost 1.151MB memory
    path_id_map_bucket_num = 49157;
    // 196613 buckets. actually cal_next_prime return 196613 buckets, which cost 4.605MB memory
    macro_id_map_bucket_num = 196613;
  } else { // tenant_mem > 16GB
    // 196613 buckets. actually cal_next_prime return 196613 buckets, which cost 4.605MB memory
    path_id_map_bucket_num = 196613;
    // 90w buckets. actually cal_next_prime return 157.3w buckets, which cost 36.84MB memory
    macro_id_map_bucket_num = 900000;
  }

  ObMemAttr path_id_map_attr(tenant_id, "ExtPathIdMap");
  ObMemAttr macro_id_map_attr(tenant_id, "ExtMacroIdMap");
  ObMemAttr allocator_attr(tenant_id, "ExtPathAlloc");
  SET_IGNORE_MEM_VERSION(path_id_map_attr);
  SET_IGNORE_MEM_VERSION(macro_id_map_attr);
  SET_IGNORE_MEM_VERSION(allocator_attr);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObExternalFilePathMap init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id));
  } else if (OB_FAIL(path_id_map_.create(path_id_map_bucket_num, path_id_map_attr))) {
    LOG_WARN("fail to create path_id_map", KR(ret), K(tenant_id), K(path_id_map_bucket_num));
  } else if (OB_FAIL(macro_id_map_.create(macro_id_map_bucket_num, macro_id_map_attr))) {
    LOG_WARN("fail to create macro_id_map", KR(ret), K(tenant_id), K(macro_id_map_bucket_num));
  } else if (OB_FAIL(allocator_.init(DEFAULT_BLOCK_SIZE, mem_limiter_, allocator_attr))) {
    LOG_WARN("fail to init io callback allocator", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObExternalFilePathMap::get(
    const ObPCachedExtMacroKey &macro_key,
    blocksstable::MacroBlockId &macro_id) const
{
  int ret = OB_SUCCESS;
  macro_id.reset();
  uint64_t path_id = UINT64_MAX;
  PathMapKey path_map_key(macro_key);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!macro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_key));
  } else if (OB_FAIL(path_id_map_.get_refactored(path_map_key, path_id))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get path_id_map", KR(ret), K(path_map_key));
    }
  } else {
    MacroMapKey macro_map_key(path_id, macro_key.offset_idx());
    if (OB_FAIL(macro_id_map_.get_refactored(macro_map_key, macro_id))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get macro_id_map", KR(ret), K(macro_map_key), K(path_map_key));
      }
    }
  }
  return ret;
}

int ObExternalFilePathMap::get_or_generate(
    const ObPCachedExtMacroKey &macro_key,
    blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  macro_id.reset();
  uint64_t path_id = UINT64_MAX;
  PathMapKey path_map_key(macro_key);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported deploy mode", KR(ret), K(macro_key));
  } else if (OB_UNLIKELY(!macro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_key));
  } else if (OB_FAIL(get_or_generate_path_id_(path_map_key, path_id))) {
    LOG_WARN("fail to get_or_generate_path_id_", KR(ret), K(macro_key));
  } else {
    MacroMapKey macro_map_key(path_id, macro_key.offset_idx());
    if (OB_FAIL(get_or_generate_macro_id_(macro_map_key, macro_id))) {
      LOG_WARN("fail to get_or_generate_macro_id_", KR(ret), K(macro_map_key), K(macro_key));
    }
  }
  return ret;
}

int ObExternalFilePathMap::alloc_macro(
    const ObPCachedExtMacroKey &macro_key,
    blocksstable::ObStorageObjectHandle &macro_handle)
{
  int ret = OB_SUCCESS;
  macro_handle.reset_macro_id();
  uint64_t path_id = UINT64_MAX;
  PathMapKey path_map_key(macro_key);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!macro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_key));
  } else if (OB_FAIL(get_or_generate_path_id_(path_map_key, path_id))) {
    LOG_WARN("fail to get_or_generate_path_id_", KR(ret), K(macro_key));
  } else {
    MacroMapKey macro_map_key(path_id, macro_key.offset_idx());
    if (OB_FAIL(generate_macro_id_(macro_map_key, macro_handle))) {
      LOG_WARN("fail to generate_macro_id_", KR(ret), K(macro_map_key), K(macro_key));
    }
  }
  return ret;
}

int ObExternalFilePathMap::overwrite(
    const ObPCachedExtMacroKey &macro_key,
    const blocksstable::ObStorageObjectHandle &macro_handle)
{
  int ret = OB_SUCCESS;
  uint64_t path_id = UINT64_MAX;
  PathMapKey path_map_key(macro_key);
  const blocksstable::MacroBlockId &macro_id = macro_handle.get_macro_id();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!macro_key.is_valid() || !is_valid_external_macro_id(macro_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_key), K(macro_id));
  } else if (OB_FAIL(get_or_generate_path_id_(path_map_key, path_id))) {
    LOG_WARN("fail to get_or_generate_path_id_", KR(ret), K(macro_key), K(macro_id));
  } else {
    MacroMapKey macro_map_key(path_id, macro_key.offset_idx());
    if (OB_FAIL(macro_id_map_.set_refactored(macro_map_key, macro_id, 1/*overwrite*/))) {
      LOG_WARN("fail to set macro_id_map", KR(ret), K(macro_map_key), K(macro_id), K(macro_key));
    }
  }
  return ret;
}

int ObExternalFilePathMap::generate_path_id_(uint64_t &path_id)
{
  int ret = OB_SUCCESS;
  path_id = UINT64_MAX;
  if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(TENANT_SEQ_GENERATOR.get_write_seq(path_id))) {
      LOG_WARN("fail to get write seq", KR(ret));
    }
  } else {
    path_id = ATOMIC_AAF(&sn_path_id_, 1);
  }
  return ret;
}

// internal func, skips initialization check
int ObExternalFilePathMap::generate_macro_id_(
    const MacroMapKey &macro_map_key,
    ObStorageObjectHandle &macro_handle)
{
  int ret = OB_SUCCESS;
  macro_handle.reset_macro_id();
  ObStorageObjectOpt opt;
  opt.set_ss_external_table_file_opt(macro_map_key.path_id_, macro_map_key.offset_idx_);
  if (OB_UNLIKELY(!macro_map_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_map_key));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(opt, macro_handle))) {
    LOG_WARN("fail to alloc macro", KR(ret), K(macro_map_key));
  }
  return ret;
}

// internal func, skips initialization check
int ObExternalFilePathMap::get_or_generate_path_id_(
    const PathMapKey &path_map_key, uint64_t &path_id)
{
  int ret = OB_SUCCESS;
  path_id = UINT64_MAX;
  if (OB_UNLIKELY(!path_map_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path_map_key));
  } else if (OB_FAIL(path_id_map_.get_refactored(path_map_key, path_id))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get path_id_map", KR(ret), K(path_map_key));
    }
  }

  if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    EmptySetCallback<PathMapKey, uint64_t> empty_set_callback;
    GetExistingValueUpdateCallback<PathMapKey, uint64_t> callback(path_id);
    PathMapKey path_map_key_deep_copy;
    path_map_key_deep_copy.modify_time_ = path_map_key.modify_time_;

    if (OB_FAIL(ob_write_string(
        allocator_, path_map_key.url_, path_map_key_deep_copy.url_, true/*c_style*/))) {
      LOG_WARN("fail to deep copy path_map_key url", KR(ret), K(path_map_key), K(path_id));
    } else if (OB_FAIL(ob_write_string(allocator_,
        path_map_key.content_digest_, path_map_key_deep_copy.content_digest_, true/*c_style*/))) {
      LOG_WARN("fail to deep copy path_map_key content_digest", KR(ret), K(path_map_key), K(path_id));
    } else if (OB_FAIL(generate_path_id_(path_id))) {
      LOG_WARN("fail to generate server seq id", KR(ret), K(path_map_key));
    } else if (OB_FAIL(path_id_map_.set_or_update(
        path_map_key_deep_copy, path_id, empty_set_callback, callback))) {
      LOG_WARN("fail to insert seq id into path_id_map",
          KR(ret), K(path_map_key), K(path_map_key_deep_copy), K(path_id));
    }

    if (OB_FAIL(ret) || callback.is_exist()) {
      if (!path_map_key_deep_copy.url_.empty()) {
        allocator_.free(path_map_key_deep_copy.url_.ptr());
      }
      if (!path_map_key_deep_copy.content_digest_.empty()) {
        allocator_.free(path_map_key_deep_copy.content_digest_.ptr());
      }
      path_map_key_deep_copy.reset();
    }
  }
  return ret;
}

// internal func, skips initialization check
int ObExternalFilePathMap::get_or_generate_macro_id_(
    const MacroMapKey &macro_map_key,
    blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  macro_id.reset();
  if (OB_UNLIKELY(!macro_map_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_map_key));
  } else if (OB_FAIL(macro_id_map_.get_refactored(macro_map_key, macro_id))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get macro_id_map", KR(ret), K(macro_map_key));
    }
  }

  if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    EmptySetCallback<MacroMapKey, blocksstable::MacroBlockId> set_cb;
    GetExistingValueUpdateCallback<MacroMapKey, blocksstable::MacroBlockId> update_cb(macro_id);
    blocksstable::ObStorageObjectHandle macro_handle;

    if (OB_FAIL(generate_macro_id_(macro_map_key, macro_handle))) {
      LOG_WARN("fail to generate macro_id", KR(ret), K(macro_map_key));
    } else if (OB_FAIL(macro_id_map_.set_or_update(
        macro_map_key, macro_handle.get_macro_id(), set_cb, update_cb))) {
      LOG_WARN("fail to insert macro id into macro_id_map",
          KR(ret), K(macro_map_key), K(macro_handle));
    } else if (!update_cb.is_exist()) {
      macro_id = macro_handle.get_macro_id();
    }
  }

  return ret;
}

int ObExternalFilePathMap::build_reversed_path_mapping(
    ObExternalFileReversedPathMap &reversed_path_map)
{
  int ret = OB_SUCCESS;
  reversed_path_map.clear();
  const int64_t start_time_us = ObTimeUtility::fast_current_time();

  hash::ObHashMap<MacroMapKey, MacroBlockId> copied_macro_id_map;
  CopyMapCB<MacroMapKey, MacroBlockId> copied_macro_id_map_cb(copied_macro_id_map);
  hash::ObHashMap<uint64_t, PathMapKey> reversed_path_id_map;
  ConstructReversedMapCB<uint64_t, PathMapKey> reversed_path_id_map_cb(reversed_path_id_map);
  // Record path_id values that exist in both path_id_map_ and macro_id_map_
  // This set is used to identify which path entries should be preserved during cleanup
  hash::ObHashSet<uint64_t> path_id_in_both_map;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(copied_macro_id_map.create(
      macro_id_map_.bucket_count(), ObMemAttr(MTL_ID(), "CopiedMacroMap")))) {
    LOG_WARN("fail to create copied macro id map", KR(ret));
  } else if (OB_FAIL(reversed_path_id_map.create(
      path_id_map_.bucket_count(), ObMemAttr(MTL_ID(), "RvsPathIdMap")))) {
    LOG_WARN("fail to create reversed path id map", KR(ret));
  } else if (OB_FAIL(path_id_in_both_map.create(
      path_id_map_.bucket_count(), ObMemAttr(MTL_ID(), "PathIdInBothMap")))) {
    LOG_WARN("fail to create path id in both map", KR(ret));
  }

  // constructing reversed_path_id_map first, to minimize the probability of newly created
  // path entries being accidentally cleaned up during concurrent operations.
  if (FAILEDx(path_id_map_.foreach_refactored(reversed_path_id_map_cb))) {
    LOG_WARN("fail to construct reversed path id map", KR(ret));
  } else if (OB_FAIL(macro_id_map_.foreach_refactored(copied_macro_id_map_cb))) {
    LOG_WARN("fail to copy macro id map", KR(ret));
  }

  hash::ObHashMap<MacroMapKey, MacroBlockId>::const_iterator macro_map_it =
      copied_macro_id_map.begin();
  PathMapKey path_map_key;
  int64_t erased_macro_map_entry_num = 0;
  for (; OB_SUCC(ret) && macro_map_it != copied_macro_id_map.end(); ++macro_map_it) {
    path_map_key.reset();
    const MacroBlockId &macro_id = macro_map_it->second;
    const MacroMapKey &macro_map_key = macro_map_it->first;
    if (OB_FAIL(reversed_path_id_map.get_refactored(macro_map_key.path_id_, path_map_key))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        // erase entry in macro_id_map_ but not in path_id_map_
        if (OB_FAIL(macro_id_map_.erase_refactored(macro_map_key))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to erase macro_id_map", KR(ret), K(macro_id), K(macro_map_key));
          }
        } else {
          erased_macro_map_entry_num++;
        }
      } else {
        LOG_WARN("fail to get path_id_map", KR(ret), K(macro_id), K(macro_map_key));
      }
    } else if (OB_FAIL(path_id_in_both_map.set_refactored(
        macro_map_key.path_id_, 1/*overwrite*/))) {
      LOG_WARN("fail to record path id in both map", KR(ret), K(macro_id), K(macro_map_key));
    } else {
      const int64_t offset =
          macro_map_key.offset_idx_ * OB_STORAGE_OBJECT_MGR.get_macro_block_size();
      ObPCachedExtMacroKey ext_macro_key(
          path_map_key.url_, path_map_key.content_digest_, path_map_key.modify_time_, offset);
      if (OB_UNLIKELY(!ext_macro_key.is_valid())) {
        // ignore error
        LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid entry in path map",
            KR(ret), K(ext_macro_key), K(macro_id), K(macro_map_key));
      } else if (OB_FAIL(reversed_path_map.set_refactored(macro_id, ext_macro_key))) {
        LOG_WARN("fail to set reversed path map",
            KR(ret), K(ext_macro_key), K(macro_id), K(macro_map_key));
      }
    }
  }

  int64_t erased_path_num = 0;
  int64_t erased_path_mem_size = 0;
  if (FAILEDx(cleanup_orphaned_path_entries_(
      reversed_path_id_map, path_id_in_both_map, erased_path_num, erased_path_mem_size))) {
    LOG_WARN("fail to cleanup orphaned path entries", KR(ret));
  }

  const int64_t used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
  LOG_INFO("ObExternalFilePathMap: build reversed path mapping", KR(ret),
      K(used_time_us), K(start_time_us), K(reversed_path_map.size()),
      K(copied_macro_id_map.size()), K(reversed_path_id_map.size()), K(path_id_in_both_map.size()),
      K(erased_macro_map_entry_num), K(erased_path_num), K(erased_path_mem_size));
  return ret;
}

int ObExternalFilePathMap::erase_orphaned_macro_id(const ObPCachedExtMacroKey &macro_key)
{
  int ret = OB_SUCCESS;
  uint64_t path_id = UINT64_MAX;
  PathMapKey path_map_key(macro_key);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!macro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_key));
  } else if (OB_FAIL(path_id_map_.get_refactored(path_map_key, path_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get path_id_map", KR(ret), K(path_map_key));
    }
  } else {
    MacroMapKey macro_map_key(path_id, macro_key.offset_idx());
    if (OB_FAIL(macro_id_map_.erase_refactored(macro_map_key))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to erase macro_id_map", KR(ret), K(macro_map_key), K(path_map_key));
      }
    }

    // cannot delete path id directly here, because one path id corresponds to multiple macros,
    // need to check if all are cleared
  }
  return ret;
}

/**
 * Clean up orphaned path entries that exist in path_id_map_ but not in macro_id_map_.
 *
 * This function is called by build_reversed_path_mapping() to remove path entries that
 * have no corresponding macro blocks. The parameters are constructed within build_reversed_path_mapping()
 * to ensure data consistency and avoid race conditions.
 *
 * @param reversed_path_id_map - A reversed mapping from path_id to PathMapKey,
 *                              constructed by iterating path_id_map_ in build_reversed_path_mapping()
 * @param path_id_in_both_map - A set of path_id values that exist in both path_id_map_
 *                              and macro_id_map_, used to identify which path entries should be preserved
 *
 * The function iterates through reversed_path_id_map instead of directly iterating path_id_map_
 * to minimize the risk of data inconsistency between path_id_map_ and macro_id_map_ during
 * concurrent operations. This prevents newly created path entries from being accidentally cleaned up.
 *
 */
int ObExternalFilePathMap::cleanup_orphaned_path_entries_(
    const hash::ObHashMap<uint64_t, PathMapKey> &reversed_path_id_map,
    const hash::ObHashSet<uint64_t> &path_id_in_both_map,
    int64_t &erased_path_num, int64_t &erased_path_mem_size)
{
  int ret = OB_SUCCESS;
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  erased_path_num = 0;
  erased_path_mem_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  }

  hash::ObHashMap<uint64_t, PathMapKey>::const_iterator path_map_it =
      reversed_path_id_map.begin();
  for (; OB_SUCC(ret) && path_map_it != reversed_path_id_map.end(); ++path_map_it) {
    const uint64_t path_id = path_map_it->first;
    const PathMapKey &path_map_key = path_map_it->second;
    if (OB_FAIL(path_id_in_both_map.exist_refactored(path_id))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        bool is_erased = false;
        PathMapKeyDeletePred delete_pred;
        if (OB_FAIL(path_id_map_.erase_if(path_map_key, delete_pred, is_erased))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to erase path_id_map", KR(ret), K(path_id), K(path_map_key));
          }
        } else if (OB_FAIL(delete_pred.get_ret())) {
          LOG_WARN("fail to erase path_id_map", KR(ret), K(path_id), K(path_map_key));
        } else if (is_erased) {
          if (!delete_pred.shadow_copied_url_.empty()) {
            erased_path_mem_size += delete_pred.shadow_copied_url_.length();
            allocator_.free(delete_pred.shadow_copied_url_.ptr());
            delete_pred.shadow_copied_url_.reset();
          }
          if (!delete_pred.shadow_copied_content_digest_.empty()) {
            erased_path_mem_size += delete_pred.shadow_copied_content_digest_.length();
            allocator_.free(delete_pred.shadow_copied_content_digest_.ptr());
            delete_pred.shadow_copied_content_digest_.reset();
          }
          erased_path_num++;
        } else {
          // should not happen. do not return error
          LOG_WARN("fail to erase path", KR(ret),
              K(path_id), K(path_map_key), K(delete_pred.shadow_copied_url_));
        }
      } else {
        LOG_WARN("fail to check entry in both map", KR(ret), K(path_id), K(path_map_key));
      }
    }
  }

  const int64_t used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
  LOG_INFO("ObExternalFilePathMap: cleanup orphaned path entries", KR(ret),
      K(used_time_us), K(start_time_us),
      K(reversed_path_id_map.size()), K(erased_path_num), K(erased_path_mem_size));
  return ret;
}

/*-----------------------------------------ObPCachedExtLRUListEntry-----------------------------------------*/
ObPCachedExtLRUListEntry::ObPCachedExtLRUListEntry(
    ObPCachedExtLRU &lru,
    const blocksstable::MacroBlockId &macro_id,
    const uint32_t data_size)
    : common::ObDLinkBase<ObPCachedExtLRUListEntry>(),
      lru_(lru),
      lock_(ObLatchIds::EXT_DISK_CACHE_LOCK),
      ref_cnt_(0), access_cnt_(0), last_access_time_us_(ObTimeUtility::fast_current_time()),
      macro_id_(macro_id),
      data_size_(data_size)
{
}

ObPCachedExtLRUListEntry::~ObPCachedExtLRUListEntry()
{
  reset();
}

void ObPCachedExtLRUListEntry::reset()
{
  SpinWLockGuard w_guard(lock_);
  data_size_ = 0;
  macro_id_.reset();
  last_access_time_us_ = 0;
  access_cnt_ = 0;
  ref_cnt_ = 0;
}

bool ObPCachedExtLRUListEntry::is_valid() const
{
  SpinRLockGuard r_guard(lock_);
  return ref_cnt_ >= 0 && access_cnt_ >= 0 && last_access_time_us_ > 0
      && is_valid_external_macro_id(macro_id_)
      && is_valid_data_size(data_size_);
}

bool ObPCachedExtLRUListEntry::is_valid_data_size(const uint32_t data_size)
{
  return data_size > 0 && data_size <= OB_STORAGE_OBJECT_MGR.get_macro_block_size();
}

void ObPCachedExtLRUListEntry::inc_ref_count()
{
  SpinWLockGuard w_guard(lock_);
  ref_cnt_++;
}

void ObPCachedExtLRUListEntry::dec_ref_count()
{
  int32_t ref_cnt = -1;
  {
    SpinWLockGuard w_guard(lock_);
    ref_cnt_--;
    ref_cnt = ref_cnt_;
  }
  if (OB_UNLIKELY(ref_cnt < 0)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED,
        "ref count of ObPCachedExtLRUListEntry should not be less than 0",
        KPC(this), K(ref_cnt));
  } else if (0 == ref_cnt) {
    lru_.delete_lru_entry_(this);
  }
}

bool ObPCachedExtLRUListEntry::inc_and_check_access_cnt()
{
  bool b_ret = false;
  SpinWLockGuard w_guard(lock_);
  ++access_cnt_;
  if (access_cnt_ >= UPDATE_LRU_LIST_THRESHOLD) {
    b_ret = true;     // need to update lru list
    access_cnt_ = 0;  // reset access_cnt to zero
  }
  return b_ret;
}

void ObPCachedExtLRUListEntry::set_last_access_time_us(const int64_t last_access_time_us)
{
  SpinWLockGuard w_guard(lock_);
  last_access_time_us_ = last_access_time_us;
}

int64_t ObPCachedExtLRUListEntry::get_last_access_time_us() const
{
  SpinRLockGuard r_guard(lock_);
  return last_access_time_us_;
}

void ObPCachedExtLRUListEntry::set_macro_id(const MacroBlockId &macro_id)
{
  SpinWLockGuard w_guard(lock_);
  macro_id_ = macro_id;
}

const blocksstable::MacroBlockId &ObPCachedExtLRUListEntry::get_macro_id() const
{
  SpinRLockGuard r_guard(lock_);
  return macro_id_;
}

void ObPCachedExtLRUListEntry::set_data_size(const uint32_t data_size)
{
  SpinWLockGuard w_guard(lock_);
  data_size_ = data_size_;
}

uint32_t ObPCachedExtLRUListEntry::get_data_size() const
{
  SpinRLockGuard r_guard(lock_);
  return data_size_;
}

/*-----------------------------------------ObPCachedExtLRU callbacks-----------------------------------------*/
ObPCachedExtLRU::ObPCachedExtLRUBaseCB::ObPCachedExtLRUBaseCB(ObPCachedExtLRU &lru)
    : lru_(lru), ret_(OB_SUCCESS)
{
}

ObPCachedExtLRU::ObPCachedExtLRUBaseCB::~ObPCachedExtLRUBaseCB()
{
}

int ObPCachedExtLRU::ObPCachedExtLRUBaseCB::get_ret() const
{
  return ret_;
}

ObPCachedExtLRU::ObPCachedExtLRUSetCB::ObPCachedExtLRUSetCB(ObPCachedExtLRU &lru)
    : ObPCachedExtLRUBaseCB(lru)
{
}

ObPCachedExtLRU::ObPCachedExtLRUSetCB::~ObPCachedExtLRUSetCB()
{
}

int ObPCachedExtLRU::ObPCachedExtLRUSetCB::operator()(
    const ObPCachedExtLRUMapPairType &map_entry)
{
  int ret = OB_SUCCESS;
  ObPCachedExtLRUListEntry *lru_entry = nullptr;
  if (OB_ISNULL(lru_entry = map_entry.second.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lru_entry is null", KR(ret), K(map_entry));
  } else if (OB_UNLIKELY(!lru_entry->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lru_entry", KR(ret), K(map_entry), KPC(lru_entry));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.inc_ref(map_entry.first))) {
    LOG_WARN("fail to inc macro ref", KR(ret), K(map_entry), KPC(lru_entry));
  } else if (OB_FAIL(lru_.add_into_lru_list_(*lru_entry))) {
    LOG_WARN("fail to add into lru list", KR(ret), K(map_entry), KPC(lru_entry));

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(OB_STORAGE_OBJECT_MGR.dec_ref(map_entry.first))) {
      LOG_ERROR("fail to dec macro ref", KR(ret), KR(tmp_ret), K(map_entry), KPC(lru_entry));
    }
  } else {
    IGNORE_RETURN ATOMIC_AAF(&lru_.disk_size_in_use_, lru_entry->get_data_size());
    OB_EXTERNAL_FILE_DISK_SPACE_MGR.update_cached_macro_count(1);
  }
  return ret;
}

ObPCachedExtLRU::ObPCachedExtLRUDeletePred::ObPCachedExtLRUDeletePred(ObPCachedExtLRU &lru)
    : ObPCachedExtLRUBaseCB(lru)
{
}

ObPCachedExtLRU::ObPCachedExtLRUDeletePred::~ObPCachedExtLRUDeletePred()
{
}

bool ObPCachedExtLRU::ObPCachedExtLRUDeletePred::operator()(
    const ObPCachedExtLRUMapPairType &map_entry)
{
  int ret = OB_SUCCESS;
  ObPCachedExtLRUListEntry *lru_entry = nullptr;
  if (OB_ISNULL(lru_entry = map_entry.second.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lru_entry is null", KR(ret), K(map_entry));
  } else if (OB_FAIL(lru_.remove_from_lru_list_(*lru_entry))) {
    LOG_WARN("fail to remove from lru list", KR(ret), K(map_entry), KPC(lru_entry));
  } else {
    IGNORE_RETURN ATOMIC_SAF(&lru_.disk_size_in_use_, lru_entry->get_data_size());
    OB_EXTERNAL_FILE_DISK_SPACE_MGR.update_cached_macro_count(-1);
  }

  ret_ = ret;
  return OB_SUCC(ret);
}

ObPCachedExtLRU::ObPCachedExtLRUReadCB::ObPCachedExtLRUReadCB(
    ObPCachedExtLRU &lru, blocksstable::ObStorageObjectHandle &macro_handle)
    : ObPCachedExtLRUBaseCB(lru), macro_handle_(macro_handle), data_size_(0)
{
}

ObPCachedExtLRU::ObPCachedExtLRUReadCB::~ObPCachedExtLRUReadCB()
{
}

void ObPCachedExtLRU::ObPCachedExtLRUReadCB::operator()(
    const ObPCachedExtLRUMapPairType &map_entry)
{
  int ret = OB_SUCCESS;
  ObPCachedExtLRUListEntry *lru_entry = nullptr;
  if (OB_ISNULL(lru_entry = map_entry.second.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lru_entry is null", KR(ret), K(map_entry));
  } else if (OB_FAIL(lru_.update_lru_list_(*lru_entry))) {
    LOG_WARN("fail to update lru list", KR(ret), K(map_entry), KPC(lru_entry));
  } else if (OB_FAIL(ObStorageIOPipelineTaskInfo::set_macro_block_id(
      macro_handle_, map_entry.first))) {
    LOG_WARN("fail to set macro id", KR(ret), K(map_entry), KPC(lru_entry));
  } else {
    data_size_ = lru_entry->get_data_size();
  }
  ret_ = ret;
}

/*-----------------------------------------ObPCachedExtLRU-----------------------------------------*/
ObPCachedExtLRU::ObPCachedExtLRU()
    : lock_(ObLatchIds::EXT_DISK_CACHE_LOCK),
      is_inited_(false),
      lru_entry_allocator_(),
      lru_entry_alloc_cnt_(0),
      macro_map_(),
      lru_list_(),
      disk_size_in_use_(0)
{
}

ObPCachedExtLRU::~ObPCachedExtLRU()
{
  destroy();
}

void ObPCachedExtLRU::destroy()
{
  // NOTE: ObPCachedExtLRU does not implement stop/wait semantics to ensure no concurrent
  // requests are accessing the object during destruction. The thread safety guarantee
  // is provided by ObPCachedExternalFileService, which ensures all concurrent requests
  // have completed before calling this destroy function. This design avoids the need
  // for complex synchronization mechanisms within ObPCachedExtLRU itself.
  const int64_t start_us = ObTimeUtility::fast_current_time();
  LOG_INFO("start to destroy ObPCachedExtLRU");
  if (IS_INIT) {
    lru_list_.clear();
    destroy_macro_map_();
    while (get_lru_entry_alloc_cnt_() > 0) {
      if (TC_REACH_TIME_INTERVAL(10LL * 1000LL * 1000LL)) { // 10s
        LOG_WARN_RET(OB_ERR_UNEXPECTED,
            "there still exists lru entry which has not been destructed",
            "lru_entry_alloc_cnt", get_lru_entry_alloc_cnt_());
      }
      ob_usleep(100LL * 1000LL); // 100ms
      lru_entry_allocator_.destroy();
    }
  }

  disk_size_in_use_ = 0;
  lru_entry_alloc_cnt_ = 0;
  is_inited_ = false;
  const int64_t cost_us = ObTimeUtility::fast_current_time() - start_us;
  LOG_INFO("finish to destroy ObPCachedExtLRU", K(cost_us));
}

void ObPCachedExtLRU::destroy_macro_map_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_INIT) {
    const int64_t macro_num_before_destroy = macro_map_.size();
    int64_t succ_dec_ref_macro_num = 0;
    ObPCachedExtLRUMap::const_iterator it = macro_map_.begin();
    for (; OB_SUCC(ret) && it != macro_map_.end(); ++it) {
      tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(it->first))) {
        LOG_WARN("fail to dec macro ref", KR(ret), KR(tmp_ret), K(it->first), K(it->second));
      } else {
        succ_dec_ref_macro_num++;
      }
    }
    LOG_INFO("ObPCachedExtLRU: destroy macro map", KR(ret),
        K(macro_num_before_destroy), K(succ_dec_ref_macro_num));
    macro_map_.destroy();
  }
}

int ObPCachedExtLRU::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t macro_map_bucket_num = -1;
  const int64_t tenant_mem_in_gb = MTL_MEM_SIZE() / GB;
  if (tenant_mem_in_gb <= 4) { // tenant_mem <= 4GB
    // 24593 buckets. actually cal_next_prime return 24593 buckets, which cost 0.576MB memory
    macro_map_bucket_num = 24593;
  } else if (tenant_mem_in_gb <= 16) {
    // 90000 buckets. actually cal_next_prime return 9.8w buckets, which cost 2.3MB memory
    macro_map_bucket_num = 90000;
  } else { // tenant_mem > 16GB
    // 90w buckets. actually cal_next_prime return 157.3w buckets, which cost 36.84MB memory
    macro_map_bucket_num = 900000;
  }

  ObMemAttr lru_entry_allocator_attr(tenant_id, EXT_LRU_ALLOC_TAG);
  ObMemAttr macro_map_attr(tenant_id, EXT_LRU_ALLOC_TAG);
  SET_IGNORE_MEM_VERSION(lru_entry_allocator_attr);
  SET_IGNORE_MEM_VERSION(macro_map_attr);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPCachedExtLRU init twice", KR(ret));
  } else if (OB_UNLIKELY(GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ObPCachedExtLRU is only used in SN mode", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id));
  } else if (OB_FAIL(lru_entry_allocator_.init(
      sizeof(ObPCachedExtLRUListEntry), lru_entry_allocator_attr))) {
    LOG_WARN("fail to init lru entry allocator", KR(ret), K(sizeof(ObPCachedExtLRUListEntry)));
  } else if (OB_FAIL(macro_map_.create(macro_map_bucket_num, macro_map_attr))) {
    LOG_WARN("fail to create macro map", KR(ret), K(tenant_id), K(macro_map_bucket_num));
  } else {
    is_inited_ = true;
    lru_entry_alloc_cnt_ = 0;
    disk_size_in_use_ = 0;
  }
  return ret;
}

int ObPCachedExtLRU::put(const ObStorageObjectHandle &macro_handle, const uint32_t data_size)
{
  int ret = OB_SUCCESS;
  ObPCachedExtLRUListEntryHandle lru_entry_handle;
  ObPCachedExtLRUSetCB insert_cb(*this);
  const blocksstable::MacroBlockId &macro_id = macro_handle.get_macro_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPCachedExtLRU is not inited", KR(ret), K(macro_id), K(data_size));
  } else if (OB_UNLIKELY(!is_valid_external_macro_id(macro_id)
      || !ObPCachedExtLRUListEntry::is_valid_data_size(data_size))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(data_size));
  } else if (OB_FAIL(construct_lru_entry_handle_(macro_id, data_size, lru_entry_handle))) {
    LOG_WARN("fail to construct lru entry handle", KR(ret), K(macro_id), K(data_size));
  } else if (OB_UNLIKELY(!lru_entry_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid entry handle", KR(ret),
        K(macro_id), K(macro_id), K(data_size), K(lru_entry_handle));
  } else if (OB_FAIL(macro_map_.set_refactored(
      macro_id, lru_entry_handle,
      0/*overwrite*/, 0/*broadcast*/, 0/*overwrite_key*/, &insert_cb))) {
    LOG_WARN("fail to put into macro map", KR(ret),
        K(macro_id), K(data_size), K(lru_entry_handle));
  }
  return ret;
}

int ObPCachedExtLRU::construct_lru_entry_handle_(
    const blocksstable::MacroBlockId &macro_id,
    const uint32_t data_size,
    ObPCachedExtLRUListEntryHandle &entry_handle)
{
  int ret = OB_SUCCESS;
  entry_handle.reset();
  ObPCachedExtLRUListEntry *entry = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPCachedExtLRU is not inited", KR(ret), K(macro_id), K(data_size));
  } else if (OB_UNLIKELY(!is_valid_external_macro_id(macro_id)
      || !ObPCachedExtLRUListEntry::is_valid_data_size(data_size))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(data_size));
  } else if (OB_ISNULL(entry =
      static_cast<ObPCachedExtLRUListEntry *>(lru_entry_allocator_.alloc()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for lru entry", KR(ret), K(macro_id), K(data_size));
  } else {
    entry = new (entry) ObPCachedExtLRUListEntry(*this, macro_id, data_size);
    entry_handle.set_ptr(entry);
    update_lru_entry_alloc_cnt_(1);

    if (OB_UNLIKELY(!entry->is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("lry entry is invalid", KR(ret), K(macro_id), K(data_size), KPC(entry));
    }
  }
  return ret;
}

void ObPCachedExtLRU::delete_lru_entry_(ObPCachedExtLRUListEntry *lru_entry)
{
  if (OB_NOT_NULL(lru_entry)) {
    lru_entry->~ObPCachedExtLRUListEntry();
    lru_entry_allocator_.free(lru_entry);
    update_lru_entry_alloc_cnt_(-1);
  }
}

int ObPCachedExtLRU::add_into_lru_list_(ObPCachedExtLRUListEntry &lru_entry)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPCachedExtLRU is not inited", KR(ret), K(lru_entry));
  } else if (OB_UNLIKELY(!lru_entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lru entry", KR(ret), K(lru_entry));
  } else {
    ObMutexGuard mutex_guard(lock_);
    if (OB_UNLIKELY(!lru_list_.add_first(&lru_entry))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to add lru entry into list", KR(ret), K(lru_entry));
    }
  }
  return ret;
}

int ObPCachedExtLRU::remove_from_lru_list_(ObPCachedExtLRUListEntry &lru_entry)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPCachedExtLRU is not inited", KR(ret), K(lru_entry));
  } else if (OB_UNLIKELY(!lru_entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lru entry", KR(ret), K(lru_entry));
  } else {
    ObMutexGuard mutex_guard(lock_);
    if (OB_UNLIKELY(!lru_list_.remove(&lru_entry))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to remove lru entry from list", KR(ret), K(lru_entry));
    }
  }
  return ret;
}

int ObPCachedExtLRU::update_lru_list_(ObPCachedExtLRUListEntry &lru_entry)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPCachedExtLRU is not inited", KR(ret), K(lru_entry));
  } else if (OB_UNLIKELY(!lru_entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lru entry", KR(ret), K(lru_entry));
  } else {
    const bool need_move_to_head = lru_entry.inc_and_check_access_cnt();
    if (need_move_to_head) {
      ObMutexGuard mutex_guard(lock_);
      if (OB_UNLIKELY(!lru_list_.move_to_first(&lru_entry))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to move lru entry to head", KR(ret), K(lru_entry));
      }
    }
    lru_entry.set_last_access_time_us(ObTimeUtility::fast_current_time());
  }
  return ret;
}

int ObPCachedExtLRU::get(
    const blocksstable::MacroBlockId &macro_id,
    bool &is_exist,
    blocksstable::ObStorageObjectHandle &macro_handle,
    uint32_t &data_size)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  data_size = 0;
  macro_handle.reset_macro_id();
  ObPCachedExtLRUReadCB read_cb(*this, macro_handle);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPCachedExtLRU is not inited", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(!is_valid_external_macro_id(macro_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else if (OB_FAIL(macro_map_.read_atomic(macro_id, read_cb))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
    } else {
      LOG_WARN("fail to read atomic", KR(ret), K(macro_id));
    }
  } else if (OB_FAIL(read_cb.get_ret())) {
    LOG_WARN("fail to read macro", KR(ret), K(macro_id));
  } else {
    is_exist = true;
    data_size = read_cb.get_data_size();
  }
  return ret;
}

int ObPCachedExtLRU::exist(const blocksstable::MacroBlockId &macro_id, bool &is_exist) const
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObPCachedExtLRUListEntryHandle lru_entry_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPCachedExtLRU is not inited", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(!is_valid_external_macro_id(macro_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else if (OB_FAIL(macro_map_.get_refactored(macro_id, lru_entry_handle))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
    } else {
      LOG_WARN("fail to get refactored", KR(ret), K(macro_id));
    }
  } else {
    is_exist = true;
  }
  return ret;
}

int ObPCachedExtLRU::erase(const blocksstable::MacroBlockId &macro_id, bool &is_erased)
{
  int ret = OB_SUCCESS;
  ObPCachedExtLRUDeletePred delete_pred(*this);
  is_erased = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPCachedExtLRU is not inited", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(!is_valid_external_macro_id(macro_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else if (OB_FAIL(macro_map_.erase_if(macro_id, delete_pred, is_erased))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
      is_erased = true;
    } else {
      LOG_WARN("fail to erase macro_id from lru", KR(ret), K(macro_id));
    }
  } else if (OB_FAIL(delete_pred.get_ret())) {
    LOG_WARN("fail to erase macro", KR(ret), K(macro_id));
  } else if (is_erased) {
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_id))) {
      LOG_WARN("fail to dec macro ref", KR(ret), K(macro_id));
    }
  }
  return ret;
}

int ObPCachedExtLRU::evict(
    const int64_t expect_evict_num,
    int64_t &actual_evict_num,
    const bool force_evict)
{
  int ret = OB_SUCCESS;
  actual_evict_num = 0;
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPCachedExtLRU is not inited", KR(ret), K(expect_evict_num));
  } else if (OB_LIKELY(expect_evict_num > 0)) {
    ObSEArray<MacroBlockId, 64> evict_entries;
    evict_entries.set_attr(ObMemAttr(MTL_ID(), EXT_LRU_ALLOC_TAG));

    {
      ObMutexGuard mutex_guard(lock_);
      ObPCachedExtLRUListEntry *cur = lru_list_.get_last();
      ObPCachedExtLRUListEntry *header = lru_list_.get_header();
      int64_t i = 0;
      while (OB_SUCC(ret) && cur != header && i < expect_evict_num) {
        if (OB_ISNULL(cur)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cur is null", KR(ret), KP(header), K(i), K(evict_entries.count()));
        // no need to check validity of cur, evict it regardless of its state
        } else if (!force_evict
            && (start_time_us - cur->get_last_access_time_us()) <= MIN_RETENTION_DURATION_US) {
          // skip
        } else if (OB_FAIL(evict_entries.push_back(cur->get_macro_id()))) {
          LOG_WARN("fail to push back evict entry", KR(ret), KPC(cur), K(i));
        }

        if (OB_SUCC(ret)) {
          i++;
          cur = cur->get_prev();
        }
      }
    }

    int tmp_ret = OB_SUCCESS;
    bool is_erased = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < evict_entries.count(); i++) {
      tmp_ret = OB_SUCCESS;
      is_erased = false;
      const MacroBlockId &macro_id = evict_entries.at(i);
      if (OB_TMP_FAIL(erase(macro_id, is_erased))) {
        LOG_WARN("fail to erase macro", KR(ret), KR(tmp_ret), K(macro_id), K(i));
      } else if (is_erased) {
        actual_evict_num++;
      }
    }

    const int64_t used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
    LOG_INFO("ObPCachedExtLRU: evict macro", KR(ret), K(used_time_us), K(start_time_us),
        K(evict_entries.count()), K(expect_evict_num), K(actual_evict_num), K(force_evict));
  }

  return ret;
}

int ObPCachedExtLRU::expire(const int64_t expire_before_time_us, int64_t &actual_expire_num)
{
  int ret = OB_SUCCESS;
  actual_expire_num = 0;
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPCachedExtLRU is not inited", KR(ret), K(expire_before_time_us));
  } else if (OB_LIKELY(expire_before_time_us > 0)) {
    ObSEArray<MacroBlockId, 64> expire_entries;
    expire_entries.set_attr(ObMemAttr(MTL_ID(), EXT_LRU_ALLOC_TAG));

    {
      ObMutexGuard mutex_guard(lock_);
      ObPCachedExtLRUListEntry *cur = lru_list_.get_last();
      ObPCachedExtLRUListEntry *header = lru_list_.get_header();
      while (OB_SUCC(ret) && cur != header) {
        if (OB_ISNULL(cur)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cur is null", KR(ret), KP(header), K(expire_entries.count()));
        // no need to check validity of cur, expire it regardless of its state
        } else if (cur->get_last_access_time_us() < expire_before_time_us) {
          if (OB_FAIL(expire_entries.push_back(cur->get_macro_id()))) {
            LOG_WARN("fail to push back expire entry", KR(ret), KPC(cur), K(expire_before_time_us));
          }
        } else {
          ret = OB_ITER_END;
        }

        if (OB_SUCC(ret)) {
          cur = cur->get_prev();
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }

    int tmp_ret = OB_SUCCESS;
    bool is_erased = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < expire_entries.count(); i++) {
      tmp_ret = OB_SUCCESS;
      is_erased = false;
      const MacroBlockId &macro_id = expire_entries.at(i);
      if (OB_TMP_FAIL(erase(macro_id, is_erased))) {
        LOG_WARN("fail to erase macro", KR(ret), KR(tmp_ret), K(macro_id), K(i));
      } else if (is_erased) {
        actual_expire_num++;
      }
    }

    const int64_t used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
    LOG_INFO("ObPCachedExtLRU: expire macro", KR(ret), K(used_time_us), K(start_time_us),
        K(expire_entries.count()), K(expire_before_time_us), K(actual_expire_num));
  }

  return ret;
}

/*-----------------------------------------ObExternalFileDiskSpaceMgr-----------------------------------------*/
void ObExternalFileDiskSpaceMgr::ObExternalFileCheckDiskUsageTask::runTimerTask()
{
  if (!GCTX.is_shared_storage_mode()) {
    ObSCPTraceIdGuard trace_id_guard;
    OB_EXTERNAL_FILE_DISK_SPACE_MGR.check_disk_usage_and_evict_if_needed();
  }
}

ObExternalFileDiskSpaceMgr &ObExternalFileDiskSpaceMgr::get_instance()
{
  static ObExternalFileDiskSpaceMgr instance;
  return instance;
}

ObExternalFileDiskSpaceMgr::ObExternalFileDiskSpaceMgr()
    : is_inited_(false),
      tg_id_(OB_INVALID_TG_ID),
      cached_macro_count_(0),
      check_disk_usage_task_()
{
}

ObExternalFileDiskSpaceMgr::~ObExternalFileDiskSpaceMgr()
{
  destroy();
}

int ObExternalFileDiskSpaceMgr::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObExternalFileDiskSpaceMgr has been inited", KR(ret));
  } else if (!GCTX.is_shared_storage_mode()
      && OB_FAIL(TG_CREATE(lib::TGDefIDs::ExtDiskCacheServerTimer, tg_id_))) {
    LOG_WARN("create thread group id failed", KR(ret));
  } else {
    is_inited_ = true;
    cached_macro_count_ = 0;
  }
  LOG_INFO("ObExternalFileDiskSpaceMgr inited", KR(ret), K(tg_id_));
  return ret;
}

int ObExternalFileDiskSpaceMgr::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObExternalFileDiskSpaceMgr is not inited", KR(ret));
  } else if (!GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(TG_START(tg_id_))) {
      LOG_WARN("fail to start ObExternalFileDiskSpaceMgr", KR(ret), K(tg_id_));
    } else if (OB_FAIL(TG_SCHEDULE(
        tg_id_, check_disk_usage_task_, CHECK_DISK_USAGE_INTERVAL_US, true/*repeat*/))) {
      LOG_WARN("fail to schedule check disk usage task", KR(ret), K(tg_id_));
    }
  }
  LOG_INFO("ObExternalFileDiskSpaceMgr started", KR(ret), K(tg_id_));
  return ret;
}

void ObExternalFileDiskSpaceMgr::stop()
{
  if (!GCTX.is_shared_storage_mode() && tg_id_ != OB_INVALID_TG_ID) {
    TG_STOP(tg_id_);
  }
  LOG_INFO("ObExternalFileDiskSpaceMgr stopped", K(tg_id_));
}

void ObExternalFileDiskSpaceMgr::wait()
{
  if (!GCTX.is_shared_storage_mode() && tg_id_ != OB_INVALID_TG_ID) {
    TG_WAIT(tg_id_);
  }
  LOG_INFO("ObExternalFileDiskSpaceMgr waited", K(tg_id_));
}

void ObExternalFileDiskSpaceMgr::destroy()
{
  if (!GCTX.is_shared_storage_mode() && tg_id_ != OB_INVALID_TG_ID) {
    TG_DESTROY(tg_id_);
  }
  tg_id_ = OB_INVALID_TG_ID;
  cached_macro_count_ = 0;
  is_inited_ = false;
  LOG_INFO("ObExternalFileDiskSpaceMgr destroyed");
}

void ObExternalFileDiskSpaceMgr::update_cached_macro_count(const int64_t delta)
{
  IGNORE_RETURN ATOMIC_AAF(&cached_macro_count_, delta);
}

int64_t ObExternalFileDiskSpaceMgr::cached_macro_count() const
{
  return ATOMIC_LOAD(&cached_macro_count_);
}

int64_t ObExternalFileDiskSpaceMgr::disk_size_allocated() const
{
  return cached_macro_count() * OB_STORAGE_OBJECT_MGR.get_macro_block_size();
}

int ObExternalFileDiskSpaceMgr::check_disk_space(bool &is_full) const
{
  int ret = OB_SUCCESS;
  is_full = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObExternalFileDiskSpaceMgr is not inited", KR(ret));
  } else {
    is_full = is_total_disk_usage_exceed_limit_() || is_external_file_disk_space_full_();
  }
  return ret;
}

bool ObExternalFileDiskSpaceMgr::is_total_disk_usage_exceed_limit_() const
{
  const int64_t max_disk_usage_percentage = MAX(
      MAX_DISK_USAGE_PERCENT, GCONF.external_table_disk_cache_max_percentage);
  return OB_STORAGE_OBJECT_MGR.get_used_macro_block_count()
      >= OB_STORAGE_OBJECT_MGR.get_total_macro_block_count() * max_disk_usage_percentage / 100;
}

bool ObExternalFileDiskSpaceMgr::is_external_file_disk_space_full_() const
{
  const int64_t max_external_file_macro_count =
      OB_STORAGE_OBJECT_MGR.get_total_macro_block_count()
      * GCONF.external_table_disk_cache_max_percentage / 100;
  return cached_macro_count() >= max_external_file_macro_count;
}

int ObExternalFileDiskSpaceMgr::check_disk_usage_and_evict_if_needed()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObExternalFileDiskSpaceMgr is not inited", KR(ret));
  } else if (cached_macro_count() <= 0) {
    LOG_INFO("ObExternalFileDiskSpaceMgr has no cached macro", KR(ret));
  } else {
    const int64_t start_time_us = ObTimeUtility::fast_current_time();
    const int64_t total_macro_count = OB_STORAGE_OBJECT_MGR.get_total_macro_block_count();
    const int64_t used_macro_count = OB_STORAGE_OBJECT_MGR.get_used_macro_block_count();
    const int64_t pending_free_macro_count = OB_SERVER_BLOCK_MGR.get_pending_free_macro_block_count();
    int64_t expect_total_evict_num = 0;
    int64_t actual_evict_num = 0;
    const bool need_evict =
        ((used_macro_count - pending_free_macro_count >= total_macro_count * EVICT_TRIGGER_PERCENT / 100)
        || is_external_file_disk_space_full_());
    const bool force_evict = (need_evict && is_total_disk_usage_exceed_limit_());

    if (need_evict) {
      expect_total_evict_num =
          MIN(EVICT_BATCH_SIZE_LIMIT, total_macro_count * EVICT_BATCH_SIZE_RATIO / 100);
      TenantMacroNumMap tenant_macro_num_map;
      if (OB_FAIL(tenant_macro_num_map.create(64, ObMemAttr(OB_SERVER_TENANT_ID, "ExtSpaceMgr")))) {
        LOG_WARN("fail to create tenant macro num map", KR(ret));
      } else if (OB_FAIL(cal_tenant_macro_count_limit_(
          tenant_macro_num_map, expect_total_evict_num))) {
        LOG_WARN("fail to cal tenant macro count limit", KR(ret),
            K(total_macro_count), K(used_macro_count), K(expect_total_evict_num));
      }

      for (TenantMacroNumMap::const_iterator iter = tenant_macro_num_map.begin();
          OB_SUCC(ret) && iter != tenant_macro_num_map.end();
          ++iter) {
        const uint64_t tenant_id = iter->first;
        const int64_t tenant_macro_count_limit = iter->second;
        int64_t tenant_actual_evict_num = 0;
        if (OB_FAIL(evict_cached_macro_by_tenant_(
            tenant_id, tenant_macro_count_limit, force_evict, tenant_actual_evict_num))) {
          LOG_WARN("fail to evict cached macro by tenant", KR(ret), K(tenant_id),
              K(tenant_macro_count_limit), K(expect_total_evict_num), K(force_evict));
          ret = OB_SUCCESS; // ignore ret
        }
        actual_evict_num += tenant_actual_evict_num;
      } // end for
    }

    const int64_t used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
    LOG_INFO("ObExternalFileDiskSpaceMgr: check disk usage", KR(ret), K(used_time_us),
        K(total_macro_count), K(used_macro_count), K(cached_macro_count()),
        K(pending_free_macro_count), K(expect_total_evict_num), K(actual_evict_num),
        K(need_evict), K(force_evict));
  }
  return ret;
}

int ObExternalFileDiskSpaceMgr::cal_tenant_macro_count_limit_(
    TenantMacroNumMap &tenant_macro_num_map,
    const int64_t expect_total_evict_num) const
{
  int ret = OB_SUCCESS;
  tenant_macro_num_map.clear();
  omt::ObMultiTenant *omt = GCTX.omt_;
  TenantUnits tenant_units;
  const int64_t total_macro_limit = MAX(0, cached_macro_count() - expect_total_evict_num);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObExternalFileDiskSpaceMgr is not inited", KR(ret));
  } else if (OB_ISNULL(omt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("omt is null", KR(ret));
  } else if (OB_UNLIKELY(expect_total_evict_num <= 0 || total_macro_limit < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(expect_total_evict_num), K(total_macro_limit));
  } else if (OB_FAIL(omt->get_tenant_units(tenant_units, true/*include_hidden_sys*/))) {
    LOG_WARN("fail to get tenant units", KR(ret), K(expect_total_evict_num), K(total_macro_limit));
  }

  int64_t total_tenant_memory_size = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_units.count(); i++) {
    const uint64_t tenant_id = tenant_units.at(i).tenant_id_;
    if (OB_LIKELY(is_sys_tenant(tenant_id) || is_user_tenant(tenant_id))) {
      const int64_t tenant_memory_size =
          ObMallocAllocator::get_instance()->get_tenant_limit(tenant_units.at(i).tenant_id_);
      if (OB_UNLIKELY(tenant_memory_size <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant mem size is not positive", KR(ret),
            K(tenant_id), K(tenant_memory_size), K(tenant_units.at(i)));
      } else {
        total_tenant_memory_size += tenant_memory_size;
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(total_tenant_memory_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("total tenant memory size is not positive", KR(ret), K(total_tenant_memory_size));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_units.count(); i++) {
    const uint64_t tenant_id = tenant_units.at(i).tenant_id_;
    if (OB_LIKELY(is_sys_tenant(tenant_id) || is_user_tenant(tenant_id))) {
      const int64_t tenant_memory_size =
          ObMallocAllocator::get_instance()->get_tenant_limit(tenant_units.at(i).tenant_id_);
      const int64_t tenant_macro_count_limit = (total_macro_limit == 0
          ? 0
          : total_macro_limit * tenant_memory_size / total_tenant_memory_size + 1);

      if (OB_FAIL(tenant_macro_num_map.set_refactored(tenant_id, tenant_macro_count_limit))) {
        LOG_WARN("fail to set tenant macro count limit", KR(ret), K(tenant_id),
            K(total_macro_limit), K(tenant_macro_count_limit), K(tenant_units.at(i)),
            K(tenant_memory_size), K(total_tenant_memory_size));
      }
    }
  }

  return ret;
}

int ObExternalFileDiskSpaceMgr::evict_cached_macro_by_tenant_(
    const uint64_t tenant_id,
    const int64_t macro_count_limit,
    const bool force_evict,
    int64_t &actual_evict_num)
{
  int ret = OB_SUCCESS;
  actual_evict_num = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObExternalFileDiskSpaceMgr is not inited", KR(ret));
  } else if (OB_UNLIKELY(!is_sys_tenant(tenant_id) && !is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id), K(macro_count_limit), K(force_evict));
  } else if (OB_UNLIKELY(macro_count_limit < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro count limit", KR(ret),
        K(tenant_id), K(macro_count_limit), K(force_evict));
  } else {
    MTL_SWITCH(tenant_id)
    {
      ObPCachedExternalFileService *ext_file_service = nullptr;
      ObStorageCacheStat cache_stat;
      if (OB_ISNULL(ext_file_service = MTL(ObPCachedExternalFileService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ObPCachedExternalFileService is NULL",
            KR(ret), K(tenant_id), K(macro_count_limit), K(force_evict));
      } else if (OB_FAIL(ext_file_service->get_cache_stat(cache_stat))) {
        LOG_WARN("fail to get cache stat", KR(ret),
            K(tenant_id), K(macro_count_limit), K(force_evict));
      } else if (cache_stat.macro_count_ > macro_count_limit) {
        if (OB_FAIL(ext_file_service->evict_cached_macro(
            cache_stat.macro_count_ - macro_count_limit, actual_evict_num, force_evict))) {
          LOG_WARN("fail to evict cached macro", KR(ret),
              K(tenant_id), K(macro_count_limit), K(cache_stat), K(force_evict));
        }
      }

      LOG_INFO("ObExternalFileDiskSpaceMgr: evict cached macro by tenant", KR(ret),
          K(tenant_id), K(macro_count_limit), K(cache_stat), K(actual_evict_num), K(force_evict));
    }
  }
  return ret;
}

/*-----------------------------------------ObPCachedExternalFileService callbacks-----------------------------------------*/
ObPCachedExternalFileService::SNAddPrefetchTaskSetCb::SNAddPrefetchTaskSetCb(
    const ObPCachedExtMacroKey &macro_key)
    : macro_key_(macro_key)
{
}

int ObPCachedExternalFileService::SNAddPrefetchTaskSetCb::operator()(
    const common::hash::HashMapPair<uint64_t, bool> &entry)
{
  int ret = OB_SUCCESS;
  bool is_cached = false;
  ObPCachedExternalFileService *ext_file_service = nullptr;
  if (OB_ISNULL(ext_file_service = MTL(ObPCachedExternalFileService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPCachedExternalFileService is NULL", KR(ret), K(macro_key_), K(entry));
  } else if (OB_FAIL(ext_file_service->check_macro_cached(macro_key_, is_cached))) {
    LOG_WARN("fail to check macro cached", KR(ret), K(macro_key_), K(entry));
  } else if (is_cached) {
    ret = OB_HASH_EXIST;
  }
  return ret;
}

/*-----------------------------------------ObPCachedExternalFileService-----------------------------------------*/
ObPCachedExternalFileService::ObPCachedExternalFileService()
    : is_inited_(false), is_stopped_(false), tenant_id_(OB_INVALID_TENANT_ID),
      io_callback_allocator_(), path_map_(), lru_(),
      timer_task_scheduler_(), running_prefetch_tasks_map_(),
      hit_stat_()
{}

ObPCachedExternalFileService::~ObPCachedExternalFileService()
{}

int ObPCachedExternalFileService::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObMemAttr io_callback_allocator_attr(tenant_id, "ExtDataIOCB");
  ObMemAttr prefetch_set_attr(tenant_id, "ExtPrefetchSet");
  SET_IGNORE_MEM_VERSION(io_callback_allocator_attr);
  SET_IGNORE_MEM_VERSION(prefetch_set_attr);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPCachedExternalFileService init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id));
  } else if (OB_FAIL(io_callback_allocator_.init(
      OB_MALLOC_NORMAL_BLOCK_SIZE, io_callback_allocator_attr, IO_CALLBACK_MEM_LIMIT))) {
    LOG_WARN("fail to init io callback allocator", KR(ret), K(tenant_id));
  } else if (OB_FAIL(path_map_.init(tenant_id))) {
    LOG_WARN("fail to init path map", KR(ret), K(tenant_id));
  } else if (OB_FAIL(timer_task_scheduler_.init(tenant_id))) {
    LOG_WARN("fail to init scheduler", KR(ret), K(tenant_id));
  } else if (!GCTX.is_shared_storage_mode() && OB_FAIL(lru_.init(tenant_id))) {
    LOG_WARN("fail to init lru", KR(ret), K(tenant_id));
  } else if (OB_FAIL(running_prefetch_tasks_map_.create(
      MAX_RUNNING_PREFETCH_TASK_NUM, prefetch_set_attr))) {
    LOG_WARN("fail to create running prefetch macros set", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
    is_stopped_ = false;
    tenant_id_ = tenant_id;
  }
  return ret;
}

int ObPCachedExternalFileService::mtl_init(ObPCachedExternalFileService *&accesser)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (is_meta_tenant(tenant_id)) {
    // skip meta tenant
  } else if (OB_ISNULL(accesser)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("accesser is NULL", KR(ret), K(tenant_id));
  } else if (OB_FAIL(accesser->init(tenant_id))) {
    LOG_WARN("fail to init external data accesser", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObPCachedExternalFileService::start()
{
  int ret = OB_SUCCESS;
  if (is_meta_tenant(MTL_ID())) {
    // skip meta tenant
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPCachedExternalFileService not init", KR(ret));
  } else if (OB_FAIL(timer_task_scheduler_.start())) {
    LOG_WARN("fail to start scheduler", KR(ret));
  } else {
    ATOMIC_STORE(&is_stopped_, false);
  }
  return ret;
}

void ObPCachedExternalFileService::stop()
{
  if (is_meta_tenant(MTL_ID())) {
    // skip meta tenant
  } else if (IS_INIT) {
    timer_task_scheduler_.stop();
    ATOMIC_STORE(&is_stopped_, true);
  }
}

void ObPCachedExternalFileService::wait()
{
  ObTenantIOManager *io_manager = nullptr;
  if (is_meta_tenant(MTL_ID())) {
    // skip meta tenant
  } else if (IS_INIT) {
    timer_task_scheduler_.wait();
    if (is_virtual_tenant_id(tenant_id_)) {
      // do nothing
    } else if (OB_NOT_NULL(io_manager = MTL(ObTenantIOManager*))) {
      const int64_t start_time_us = ObTimeUtility::current_time();
      while (io_manager->get_ref_cnt() > 1) {
        if (REACH_TIME_INTERVAL(1000L * 1000L)) { // 1s
          LOG_INFO("wait tenant io manager quit",
              K(tenant_id_), K(start_time_us), KPC(io_manager));
        }
        ob_usleep((useconds_t)10L * 1000L); //10ms
      }
    }
  }
}

void ObPCachedExternalFileService::destroy()
{
  hit_stat_.reset();
  running_prefetch_tasks_map_.destroy();
  timer_task_scheduler_.destroy();
  lru_.destroy();
  path_map_.destroy();
  is_inited_ = false;
  is_stopped_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  io_callback_allocator_.destroy();
}

bool ObPCachedExternalFileService::is_read_range_valid(const int64_t offset, const int64_t size)
{
  bool bret = false;
  const int64_t MACRO_BLOCK_SIZE = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
  if (OB_LIKELY(MACRO_BLOCK_SIZE > 0 && offset >= 0 && size > 0)) {
    const int64_t offset_idx = offset / MACRO_BLOCK_SIZE;
    bret = ((offset + size) <= (offset_idx + 1) * MACRO_BLOCK_SIZE);
  }
  return bret;
}

int ObPCachedExternalFileService::check_init_and_stop_() const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPCachedExternalFileService not init", KR(ret));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ObPCachedExternalFileService is stopped", KR(ret));
  }
  return ret;
}

int ObPCachedExternalFileService::async_read(
    const ObExternalAccessFileInfo &external_file_info,
    const ObExternalReadInfo &external_read_info,
    blocksstable::ObStorageObjectHandle &io_handle)
{
  int ret = OB_SUCCESS;
  const ObString &url = external_file_info.get_url();
  const ObObjectStorageInfo *access_info = external_file_info.get_access_info();

  if (OB_FAIL(check_init_and_stop_())) {
  } else if (OB_ISNULL(access_info)
      || OB_UNLIKELY(!external_file_info.is_valid() || !external_read_info.is_valid())
      || OB_UNLIKELY(url.empty() || !access_info->is_valid())
      || OB_UNLIKELY(!is_read_range_valid(external_read_info.offset_, external_read_info.size_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(external_file_info), K(external_read_info));
  } else if (!external_read_info.io_desc_.is_buffered_read()) {
    if (OB_FAIL(async_read_from_object_storage_(
        url, access_info, external_read_info, io_handle.get_io_handle()))) {
      LOG_WARN("fail to async read from object storage",
          KR(ret), K(external_file_info), K(external_read_info));
    }
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(ss_async_read_(external_file_info, external_read_info, io_handle))) {
      LOG_WARN("fail to do async read in ss mode",
          KR(ret), K(external_file_info), K(external_read_info));
    }
  } else { // SN mode
    if (OB_FAIL(sn_async_read_(external_file_info, external_read_info, io_handle))) {
      LOG_WARN("fail to do async read in sn mode",
          KR(ret), K(external_file_info), K(external_read_info));
    }
  }
  return ret;
}

// internal func, skips initialization check
// params are validated in the async_read interface, no separate validation here
int ObPCachedExternalFileService::ss_async_read_(
    const ObExternalAccessFileInfo &external_file_info,
    const ObExternalReadInfo &external_read_info,
    blocksstable::ObStorageObjectHandle &io_handle)
{
  int ret = OB_SUCCESS;
  const ObString &url = external_file_info.get_url();
  const ObObjectStorageInfo *access_info = external_file_info.get_access_info();
  blocksstable::MacroBlockId macro_id;
  const ObPCachedExtMacroKey macro_key(url,
      external_file_info.get_file_content_digest(),
      external_file_info.get_modify_time(),
      external_read_info.offset_);

  if (OB_UNLIKELY(!GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported deploy mode", KR(ret), K(external_file_info), K(external_read_info));
  } else if (OB_FAIL(path_map_.get_or_generate(macro_key, macro_id))) {
    LOG_WARN("fail to get or create macro block id",
        KR(ret), K(external_file_info), K(external_read_info));
  } else if (OB_FAIL(async_read_object_manager_(
      url, access_info, external_read_info, macro_id, io_handle))) {
    LOG_WARN("fail to async read from local cache",
        KR(ret), K(external_file_info), K(external_read_info), K(macro_id));
  }
  return ret;
}

// internal func, skips initialization check
// params are validated in the async_read interface, no separate validation here
int ObPCachedExternalFileService::sn_async_read_(
    const ObExternalAccessFileInfo &external_file_info,
    const ObExternalReadInfo &external_read_info,
    blocksstable::ObStorageObjectHandle &io_handle)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t MACRO_BLOCK_SIZE = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
  const ObString &url = external_file_info.get_url();
  const ObObjectStorageInfo *access_info = external_file_info.get_access_info();
  const ObPCachedExtMacroKey macro_key(url,
      external_file_info.get_file_content_digest(),
      external_file_info.get_modify_time(),
      external_read_info.offset_);

  bool is_cache_hit = false;
  blocksstable::ObStorageObjectHandle macro_handle;
  bool nedd_add_prefetch_task = true;

  if (OB_UNLIKELY(GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported deploy mode", KR(ret), K(external_file_info), K(external_read_info));
  } else if (OB_FAIL(get_cached_macro(
      macro_key, external_read_info.offset_, external_read_info.size_,
      is_cache_hit, macro_handle))) {
    LOG_WARN("fail to get cached macro", KR(ret), K(external_file_info), K(external_read_info));
  }

  if (OB_FAIL(ret)) {
  } else if (is_cache_hit) {
    nedd_add_prefetch_task = false;
    if (OB_FAIL(async_read_object_manager_(
        url, access_info, external_read_info, macro_handle.get_macro_id(), io_handle))) {
      LOG_WARN("fail to async read from local cache",
          KR(ret), K(external_file_info), K(external_read_info), K(macro_handle));
    }
  } else { // cache miss
    ObExternalReadInfo new_external_read_info = external_read_info;
    if (external_read_info.size_ == MACRO_BLOCK_SIZE) {
      if (OB_TMP_FAIL(ObExternalRemoteIOCallback::construct_io_callback(
          external_file_info, external_read_info, new_external_read_info))) {
        LOG_WARN("fail to construct io callback", KR(ret), KR(tmp_ret),
            K(external_file_info), K(external_read_info), K(new_external_read_info));

        // if construct io callback failed, use the original external_read_info
        new_external_read_info = external_read_info;
      } else {
        nedd_add_prefetch_task = false;
      }
    }

    if (FAILEDx(async_read_from_object_storage_(
        url, access_info, new_external_read_info, io_handle.get_io_handle()))) {
      LOG_WARN("fail to async read from object storage",
          KR(ret), K(external_file_info), K(external_read_info), K(new_external_read_info));

      ObIOCallback *io_callback = new_external_read_info.io_callback_;
      if (OB_NOT_NULL(io_callback)
          && ObIOCallbackType::EXTERNAL_DATA_LOAD_FROM_REMOTE_CALLBACK == io_callback->get_type()) {
        ObExternalRemoteIOCallback::free_io_callback_and_detach_original(io_callback);
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObLakeTableIOMetrics *io_metrics = external_read_info.io_metrics_;
    if (OB_NOT_NULL(io_metrics)) {
      io_metrics->update_disk_cache_stat(is_cache_hit, external_read_info.size_);
    }
    if (nedd_add_prefetch_task && OB_TMP_FAIL(sn_add_prefetch_task(macro_key, access_info))) {
      LOG_WARN("fail to add prefetch task", KR(ret), KR(tmp_ret), K(external_file_info),
               K(external_read_info), K(macro_key));
    }
  }
  return ret;
}

// internal func, skips initialization check
int ObPCachedExternalFileService::async_read_from_object_storage_(
    const common::ObString &url,
    const common::ObObjectStorageInfo *access_info,
    const ObExternalReadInfo &external_read_info,
    common::ObIOHandle &io_handle) const
{
  int ret = OB_SUCCESS;
  ObIOFd fd;
  ObIODevice *io_device = nullptr;
  ObIOInfo io_info;
  CONSUMER_GROUP_FUNC_GUARD(share::PRIO_IMPORT);

  if (OB_ISNULL(access_info)
      || OB_UNLIKELY(!external_read_info.is_valid() || url.empty() || !access_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(url), KPC(access_info), K(external_read_info));
  } else if (OB_FAIL(ObExternalIoAdapter::open_with_access_type(
      io_device, fd, access_info, url,
      ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
      ObStorageIdMod::get_default_external_id_mod()))) {
    LOG_WARN("fail to open device", KR(ret), K(url), KPC(access_info), K(external_read_info));
  } else if (OB_FAIL(ObExternalIoAdapter::basic_init_read_info(
      *io_device, fd,
      static_cast<char *>(external_read_info.buffer_),
      external_read_info.offset_,
      external_read_info.size_,
      OB_INVALID_ID/*sys_module_id*/,
      io_info))) {
    LOG_WARN("fail to init read info", KR(ret),
        KPC(io_device), K(fd), K(url), KPC(access_info), K(external_read_info));
  } else {
    io_info.flag_.set_need_close_dev_and_fd();
    io_info.callback_ = external_read_info.io_callback_;
    io_info.timeout_us_ = external_read_info.io_timeout_ms_ * 1000LL;
    if (OB_FAIL(ObExternalIoAdapter::async_pread_with_io_info(io_info, io_handle))) {
      LOG_WARN("fail to async pread", KR(ret),
          KPC(io_device), K(fd), K(url), KPC(access_info), K(external_read_info));
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(io_device)) {
    ObExternalIoAdapter::close_device_and_fd(io_device, fd);
  }
  return ret;
};

// internal func, skips initialization check
int ObPCachedExternalFileService::async_read_object_manager_(
    const common::ObString &url,
    const common::ObObjectStorageInfo *access_info,
    const ObExternalReadInfo &external_read_info,
    const blocksstable::MacroBlockId &macro_id,
    blocksstable::ObStorageObjectHandle &io_handle) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(access_info)
      || OB_UNLIKELY(!external_read_info.is_valid() || url.empty() || !access_info->is_valid())
      || OB_UNLIKELY(!is_valid_external_macro_id(macro_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id.storage_object_type()),
        K(url), KPC(access_info), K(external_read_info), K(macro_id), K(macro_id.is_valid()));
  } else {
    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = macro_id;
    // In SS mode, the request may hit disk cache or miss it. If it hits, the offset will be
    // adjusted by ObSSExternalFileReader. If it misses, we need to use the original offset
    // to read from the specified location.
    // In SN mode, this indicates a disk cache hit, so we need to actively calibrate the offset here.
    if (GCTX.is_shared_storage_mode()) {
      read_info.offset_ = external_read_info.offset_;
    } else {
      read_info.offset_ = external_read_info.offset_ % OB_STORAGE_OBJECT_MGR.get_macro_block_size();
    }
    read_info.size_ = external_read_info.size_;
    read_info.io_timeout_ms_ = external_read_info.io_timeout_ms_;
    read_info.io_desc_ = external_read_info.io_desc_;
    read_info.io_callback_ = external_read_info.io_callback_;
    read_info.buf_ = static_cast<char *>(external_read_info.buffer_);
    read_info.mtl_tenant_id_ = tenant_id_;
    read_info.path_ = url;
    read_info.access_info_ = access_info;
    read_info.io_metrics_ = external_read_info.io_metrics_;

    if (OB_FAIL(ObObjectManager::async_read_object(read_info, io_handle))) {
      LOG_WARN("fail to async_read_object", KR(ret),
          K(url), KPC(access_info), K(external_read_info), K(io_handle));
    }
  }
  return ret;
}

int ObPCachedExternalFileService::ss_add_prefetch_task(
    const common::ObString &url,
    const common::ObObjectStorageInfo *access_info,
    const int64_t offset_idx,
    const blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  ObSSExternalDataPrefetchTaskInfo task_info;
  if (OB_FAIL(check_init_and_stop_())) {
  } else if (OB_UNLIKELY(!GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported deploy mode", KR(ret), K(url), K(macro_id));
  } else if (OB_ISNULL(access_info)
      || OB_UNLIKELY(url.empty() || !access_info->is_valid() || offset_idx < 0)
      || OB_UNLIKELY(!is_valid_external_macro_id(macro_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(url), KPC(access_info), K(offset_idx), K(macro_id));
  } else if (MAX_RUNNING_PREFETCH_TASK_NUM <= running_prefetch_tasks_map_.size()) {
    LOG_INFO("running prefetch tasks is full", KR(ret),
        K(url), K(offset_idx), K(macro_id), K(running_prefetch_tasks_map_.size()));
  } else if (OB_FAIL(task_info.init(
      url, access_info, offset_idx * OB_STORAGE_OBJECT_MGR.get_macro_block_size(), macro_id))) {
    LOG_WARN("fail to init prefetch task info", KR(ret),
        K(url), KPC(access_info), K(offset_idx), K(macro_id));
  } else {
    const uint64_t hash_val = task_info.hash();
    if (OB_FAIL(running_prefetch_tasks_map_.set_refactored(hash_val, true, 0/*overwrite*/))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to add prefetch task", KR(ret), K(url), K(task_info), K(macro_id));
      }
    } else if (OB_FAIL(timer_task_scheduler_.prefetch_timer_.add_task(task_info))) {
      // add_task failure will automatically remove elements from running_prefetch_tasks_map_,
      // so no separate handling needed here
      LOG_WARN("fail to add prefetch task", KR(ret),
          K(task_info), K(url), KPC(access_info), K(offset_idx), K(macro_id));
    }
  }

  return ret;
}

int ObPCachedExternalFileService::ss_add_prefetch_task(
    const char *data,
    const int64_t data_size,
    const blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  ObSSExternalDataPrefetchTaskInfo task_info;
  if (OB_FAIL(check_init_and_stop_())) {
  } else if (OB_UNLIKELY(!GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported deploy mode", KR(ret), K(macro_id));
  } else if (OB_ISNULL(data) || OB_UNLIKELY(!is_valid_external_macro_id(macro_id))
      || OB_UNLIKELY(data_size != OB_STORAGE_OBJECT_MGR.get_macro_block_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(data), K(data_size), K(macro_id));
  } else if (MAX_RUNNING_PREFETCH_TASK_NUM <= running_prefetch_tasks_map_.size()) {
    LOG_INFO("running prefetch tasks is full", KR(ret),
        KP(data), K(data_size), K(macro_id), K(running_prefetch_tasks_map_.size()));
  } else if (OB_FAIL(task_info.init(data, data_size, macro_id))) {
    LOG_WARN("fail to init prefetch task info", KR(ret), KP(data), K(data_size), K(macro_id));
  } else {
    const uint64_t hash_val = task_info.hash();
    if (OB_FAIL(running_prefetch_tasks_map_.set_refactored(hash_val, true, 0/*overwrite*/))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to add prefetch task", KR(ret), KP(data), K(data_size), K(macro_id));
      }
    } else if (OB_FAIL(timer_task_scheduler_.prefetch_timer_.add_task(task_info))) {
      // add_task failure will automatically remove elements from running_prefetch_tasks_map_,
      // so no separate handling needed here
      LOG_WARN("fail to add prefetch task", KR(ret),
          K(task_info), KP(data), K(data_size), K(macro_id));
    }
  }

  return ret;
}

int ObPCachedExternalFileService::sn_add_prefetch_task(
    const ObPCachedExtMacroKey &macro_key,
    const common::ObObjectStorageInfo *access_info)
{
  int ret = OB_SUCCESS;
  blocksstable::ObStorageObjectHandle macro_handle;
  ObSNExternalDataPrefetchTaskInfo task_info;
  if (OB_FAIL(check_init_and_stop_())) {
  } else if (OB_UNLIKELY(GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported deploy mode", KR(ret), K(macro_key));
  } else if (OB_ISNULL(access_info)
      || OB_UNLIKELY(!access_info->is_valid() || !macro_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(access_info), K(macro_key));
  } else if (MAX_RUNNING_PREFETCH_TASK_NUM <= running_prefetch_tasks_map_.size()) {
    LOG_INFO("running prefetch tasks is full", KR(ret),
        KPC(access_info), K(macro_key), K(running_prefetch_tasks_map_.size()));
  } else if (OB_FAIL(alloc_macro(macro_key, macro_handle))) {
    if (OB_SERVER_OUTOF_DISK_SPACE != ret) {
      LOG_WARN("fail to alloc macro", KR(ret), KPC(access_info), K(macro_key));
    }
  } else if (OB_FAIL(task_info.init(
      macro_key.url(), access_info,
      macro_key.block_start_offset(),
      macro_handle.get_macro_id(),
      macro_key.file_version()))) {
    LOG_WARN("fail to init prefetch task info", KR(ret),
        KPC(access_info), K(macro_key), K(macro_handle));
  } else {
    const uint64_t hash_val = task_info.hash();
    SNAddPrefetchTaskSetCb set_cb(macro_key);
    if (OB_FAIL(running_prefetch_tasks_map_.set_refactored(hash_val, true,
        0/*overwrite*/, 0/*broadcast*/, 0/*overwrite_key*/, &set_cb))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to add prefetch task", KR(ret), K(macro_key), K(task_info), K(macro_handle));
      }
    } else if (OB_FAIL(timer_task_scheduler_.prefetch_timer_.add_task(task_info))) {
      // add_task failure will automatically remove elements from running_prefetch_tasks_map_,
      // so no separate handling needed here
      LOG_WARN("fail to add prefetch task", KR(ret),
          K(task_info), KPC(access_info), K(macro_key), K(macro_handle));
    }
  }

  if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
    // external file disk space is full, skip add prefetch task
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObPCachedExternalFileService::sn_add_prefetch_task(
    const ObPCachedExtMacroKey &macro_key,
    const char *data,
    const int64_t data_size)
{
  int ret = OB_SUCCESS;
  blocksstable::ObStorageObjectHandle macro_handle;
  ObSNExternalDataPrefetchTaskInfo task_info;
  if (OB_FAIL(check_init_and_stop_())) {
  } else if (OB_UNLIKELY(GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported deploy mode", KR(ret), K(macro_key));
  } else if (OB_ISNULL(data) || OB_UNLIKELY(!macro_key.is_valid())
      || OB_UNLIKELY(data_size != OB_STORAGE_OBJECT_MGR.get_macro_block_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(data), K(data_size), K(macro_key));
  } else if (MAX_RUNNING_PREFETCH_TASK_NUM <= running_prefetch_tasks_map_.size()) {
    LOG_INFO("running prefetch tasks is full", KR(ret),
        KP(data), K(data_size), K(macro_key), K(running_prefetch_tasks_map_.size()));
  } else if (OB_FAIL(alloc_macro(macro_key, macro_handle))) {
    if (OB_SERVER_OUTOF_DISK_SPACE != ret) {
      LOG_WARN("fail to alloc macro", KR(ret), KP(data), K(data_size), K(macro_key));
    }
  } else if (OB_FAIL(task_info.init(
      data, data_size,
      macro_key.url(),
      macro_key.block_start_offset(),
      macro_handle.get_macro_id(),
      macro_key.file_version()))) {
    LOG_WARN("fail to init prefetch task info", KR(ret),
        KP(data), K(data_size), K(macro_key), K(macro_handle));
  } else {
    const uint64_t hash_val = task_info.hash();
    SNAddPrefetchTaskSetCb set_cb(macro_key);
    if (OB_FAIL(running_prefetch_tasks_map_.set_refactored(hash_val, true,
        0/*overwrite*/, 0/*broadcast*/, 0/*overwrite_key*/, &set_cb))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to add prefetch task", KR(ret), K(macro_key), K(task_info), K(macro_handle));
      }
    } else if (OB_FAIL(timer_task_scheduler_.prefetch_timer_.add_task(task_info))) {
      // add_task failure will automatically remove elements from running_prefetch_tasks_map_,
      // so no separate handling needed here
      LOG_WARN("fail to add prefetch task", KR(ret),
          K(task_info), K(macro_key), K(macro_handle));
    }
  }

  if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
    // external file disk space is full, skip add prefetch task
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObPCachedExternalFileService::finish_running_prefetch_task(const uint64_t task_hash_key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_init_and_stop_())) {
  } else if (OB_FAIL(running_prefetch_tasks_map_.erase_refactored(task_hash_key))) {
    LOG_WARN("fail to erase prefetch task", KR(ret), K(task_hash_key));
  }
  return ret;
}

int ObPCachedExternalFileService::get_cached_macro(
    const ObPCachedExtMacroKey &macro_key,
    const int64_t offset,
    const int64_t read_size,
    bool &is_cached,
    blocksstable::ObStorageObjectHandle &macro_handle)
{
  int ret = OB_SUCCESS;
  is_cached = false;
  macro_handle.reset_macro_id();
  blocksstable::MacroBlockId macro_id;
  uint32_t data_size = 0;
  const int64_t MACRO_BLOCK_SIZE = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
  if (OB_FAIL(check_init_and_stop_())) {
  } else if (OB_UNLIKELY(GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported deploy mode", KR(ret), K(macro_key), K(offset), K(read_size));
  } else if (OB_UNLIKELY(!macro_key.is_valid() || !is_read_range_valid(offset, read_size))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
        K(macro_key), K(offset), K(read_size), K(MACRO_BLOCK_SIZE));
  } else if (OB_FAIL(path_map_.get(macro_key, macro_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get macro id", KR(ret), K(macro_key), K(offset), K(read_size));
    }
  } else if (OB_FAIL(lru_.get(macro_id, is_cached, macro_handle, data_size))) {
    LOG_WARN("fail to check macro exist", KR(ret),
        K(macro_key), K(macro_id), K(offset), K(read_size));
  } else if (is_cached && OB_UNLIKELY((offset % MACRO_BLOCK_SIZE) + read_size > data_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("read range is invalid", KR(ret), K(macro_key),
        K(offset), K(read_size), K(data_size), K(macro_id), K(MACRO_BLOCK_SIZE));
  }

  if (OB_SUCC(ret) && is_cached) {
    hit_stat_.update_cache_hit(1, read_size);
  } else {  // OB_FAIL or is_cached == false
    hit_stat_.update_cache_miss(1, read_size);
  }
  return ret;
}

int ObPCachedExternalFileService::check_macro_cached(
    const ObPCachedExtMacroKey &macro_key,
    bool &is_cached) const
{
  int ret = OB_SUCCESS;
  is_cached = false;
  blocksstable::MacroBlockId macro_id;
  if (OB_FAIL(check_init_and_stop_())) {
  } else if (OB_UNLIKELY(GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported deploy mode", KR(ret), K(macro_key));
  } else if (OB_FAIL(path_map_.get(macro_key, macro_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get macro id", KR(ret), K(macro_key));
    }
  } else if (OB_FAIL(lru_.exist(macro_id, is_cached))) {
    LOG_WARN("fail to check macro id exist", KR(ret), K(macro_key), K(macro_id));
  }
  return ret;
}

int ObPCachedExternalFileService::alloc_macro(
    const ObPCachedExtMacroKey &macro_key,
    blocksstable::ObStorageObjectHandle &macro_handle)
{
  int ret = OB_SUCCESS;
  bool is_full = false;
  if (OB_FAIL(check_init_and_stop_())) {
  } else if (OB_UNLIKELY(GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported deploy mode", KR(ret), K(macro_key), K(macro_handle));
  } else if (OB_FAIL(OB_EXTERNAL_FILE_DISK_SPACE_MGR.check_disk_space(is_full))) {
    LOG_WARN("fail to check disk space", KR(ret), K(macro_key), K(macro_handle));
  } else if (is_full) {
    ret = OB_SERVER_OUTOF_DISK_SPACE;
    LOG_WARN("external file disk space is full", KR(ret), K(macro_key), K(macro_handle));
  } else if (OB_FAIL(path_map_.alloc_macro(macro_key, macro_handle))) {
    LOG_WARN("fail to alloc macro", KR(ret), K(macro_key), K(macro_handle));
  }
  return ret;
}

int ObPCachedExternalFileService::cache_macro(
    const ObPCachedExtMacroKey &macro_key,
    const blocksstable::ObStorageObjectHandle &macro_handle,
    const uint32_t data_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_init_and_stop_())) {
  } else if (OB_UNLIKELY(GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported deploy mode", KR(ret), K(macro_key), K(macro_handle), K(data_size));
  } else if (OB_UNLIKELY(!macro_key.is_valid()
      || !is_valid_external_macro_id(macro_handle.get_macro_id())
      || !ObPCachedExtLRUListEntry::is_valid_data_size(data_size))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_key), K(macro_handle), K(data_size));
  } else if (OB_FAIL(path_map_.overwrite(macro_key, macro_handle))) {
    LOG_WARN("fail to overwrite macro into path map",
        KR(ret), K(macro_key), K(macro_handle), K(data_size));
  } else if (OB_FAIL(lru_.put(macro_handle, data_size))) {
    LOG_WARN("fail to put macro into lru", KR(ret), K(macro_key), K(macro_handle), K(data_size));
  }
  return ret;
}

int ObPCachedExternalFileService::expire_cached_macro(
    const int64_t expire_before_time_us,
    int64_t &actual_expire_num)
{
  int ret = OB_SUCCESS;
  actual_expire_num = 0;
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  if (OB_FAIL(check_init_and_stop_())) {
  } else if (OB_UNLIKELY(GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported deploy mode", KR(ret), K(expire_before_time_us));
  } else if (OB_UNLIKELY(expire_before_time_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(expire_before_time_us));
  } else if (OB_FAIL(lru_.expire(expire_before_time_us, actual_expire_num))) {
    LOG_WARN("fail to expire macro from lru",
        KR(ret), K(expire_before_time_us), K(actual_expire_num));
  }
  const int64_t used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
  LOG_INFO("ObPCachedExternalFileService: expire cached macro", KR(ret),
      K(used_time_us), K(start_time_us), K(expire_before_time_us), K(actual_expire_num));
  return ret;
}

int ObPCachedExternalFileService::evict_cached_macro(
    const int64_t expect_evict_num,
    int64_t &actual_evict_num,
    const bool force_evict)
{
  int ret = OB_SUCCESS;
  actual_evict_num = 0;
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  if (OB_FAIL(check_init_and_stop_())) {
  } else if (OB_UNLIKELY(GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported deploy mode", KR(ret), K(expect_evict_num), K(force_evict));
  } else if (OB_UNLIKELY(expect_evict_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(expect_evict_num), K(force_evict));
  } else if (OB_FAIL(lru_.evict(expect_evict_num, actual_evict_num, force_evict))) {
    LOG_WARN("fail to evict macro from lru", KR(ret), K(expect_evict_num), K(force_evict));
  }
  const int64_t used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
  LOG_INFO("ObPCachedExternalFileService: evict cached macro", KR(ret),
      K(used_time_us), K(start_time_us), K(expect_evict_num), K(force_evict), K(actual_evict_num));
  return ret;
}

int ObPCachedExternalFileService::cleanup_orphaned_path()
{
  int ret = OB_SUCCESS;
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  int64_t erased_num = 0;
  int64_t failed_erase_num = 0;
  ObExternalFileReversedPathMap reversed_path_map;
  if (OB_FAIL(check_init_and_stop_())) {
  } else if (OB_FAIL(reversed_path_map.create(
      path_map_.bucket_count(), ObMemAttr(MTL_ID(), "ExtRvsPathMap")))) {
    LOG_WARN("fail to create reversed path map", KR(ret), K(path_map_.bucket_count()));
  } else if (OB_FAIL(path_map_.build_reversed_path_mapping(reversed_path_map))) {
    LOG_WARN("fail to construct reversed path map", KR(ret));
  } else {
    bool is_exist = false;
    for (ObExternalFileReversedPathMap::const_iterator it = reversed_path_map.begin();
        OB_SUCC(ret) && it != reversed_path_map.end(); ++it) {
      is_exist = false;
      const blocksstable::MacroBlockId &macro_id = it->first;
      const ObPCachedExtMacroKey &macro_key = it->second;
      if (GCTX.is_shared_storage_mode()) {
#ifdef OB_BUILD_SHARED_STORAGE
        ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
        if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ObSSMacroCacheMgr is null", KR(ret), K(macro_id), K(macro_key));
        } else if (OB_FAIL(macro_cache_mgr->exist(macro_id, is_exist))) {
          LOG_WARN("fail to check macro exist", KR(ret), K(macro_id), K(macro_key));
        }
#endif
      } else {
        if (OB_FAIL(lru_.exist(macro_id, is_exist))) {
          LOG_WARN("fail to check macro exist", KR(ret), K(macro_id), K(macro_key));
        }
      }

      if (OB_SUCC(ret) && !is_exist) {
        if (OB_FAIL(path_map_.erase_orphaned_macro_id(macro_key))) {
          LOG_WARN("fail to erase macro from path map", KR(ret), K(macro_id), K(macro_key));
          failed_erase_num++;
        } else {
          erased_num++;
        }
      }

      ret = OB_SUCCESS; // ignore ret
    }
  }

  const int64_t used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
  LOG_INFO("ObPCachedExternalFileService: cleanup orphaned path", KR(ret),
      K(used_time_us), K(start_time_us), K(erased_num), K(failed_erase_num),
      K(reversed_path_map.size()));
  return ret;
}

int ObPCachedExternalFileService::get_cache_stat(storage::ObStorageCacheStat &cache_stat) const
{
  int ret = OB_SUCCESS;
  cache_stat.reset();
  if (OB_FAIL(check_init_and_stop_())) {
  } else if (!GCTX.is_shared_storage_mode()) {
    cache_stat.update_cache_stat(
        lru_.cached_macro_count(), lru_.disk_size_allocated(), lru_.disk_size_in_use());
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
    if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObSSMacroCacheMgr is null", KR(ret));
    } else if (OB_FAIL(macro_cache_mgr->get_external_table_cache_stat(cache_stat))) {
      LOG_WARN("fail to get external table cache stat", KR(ret));
    }
#endif
  }
  return ret;
}

int ObPCachedExternalFileService::get_hit_stat(storage::ObStorageCacheHitStat &hit_stat) const
{
  int ret = OB_SUCCESS;
  hit_stat.reset();
  if (OB_FAIL(check_init_and_stop_())) {
  } else if (!GCTX.is_shared_storage_mode()) {
    hit_stat = hit_stat_;
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
    if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObSSMacroCacheMgr is null", KR(ret));
    } else if (OB_FAIL(macro_cache_mgr->get_macro_cache_hit_stat_by_block_type(
        ObSSMacroBlockType::EXTERNAL_TABLE, hit_stat))) {
      LOG_WARN("fail to get hit stat", KR(ret));
    }
#endif
  }
  return ret;
}

int ObPCachedExternalFileService::get_io_callback_allocator(common::ObIAllocator *&allocator)
{
  int ret = OB_SUCCESS;
  allocator = nullptr;
  if (OB_FAIL(check_init_and_stop_())) {
  } else {
    allocator = &io_callback_allocator_;
  }
  return ret;
}

/*-----------------------------------------ObExternalRemoteIOCallback-----------------------------------------*/
int ObExternalRemoteIOCallback::construct_io_callback(
    const blocksstable::ObStorageObjectReadInfo *read_info,
    common::ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  ObPCachedExternalFileService *accesser = nullptr;
  ObIAllocator *allocator = nullptr;
  ObExternalRemoteIOCallback *load_callback = nullptr;

  if (OB_ISNULL(read_info) || OB_UNLIKELY(!read_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid read info", KR(ret), KPC(read_info));
  } else if (OB_UNLIKELY(ObStorageObjectType::EXTERNAL_TABLE_FILE !=
      read_info->macro_block_id_.storage_object_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid read info", KR(ret), KPC(read_info));
  } else if (OB_ISNULL(accesser = MTL(ObPCachedExternalFileService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPCachedExternalFileService is null", KR(ret), KPC(read_info), K(io_info));
  } else if (OB_FAIL(accesser->get_io_callback_allocator(allocator))) {
    LOG_WARN("fail to get io callback allocator", KR(ret), KPC(read_info), K(io_info));
  } else if (OB_ISNULL(load_callback =
      OB_NEWx(ObExternalRemoteIOCallback, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObExternalRemoteIOCallback", KR(ret),
        KP(allocator), KPC(read_info), K(io_info));
  } else if (OB_FAIL(load_callback->init(allocator,
      read_info->io_callback_,
      read_info->buf_,
      read_info->offset_ / OB_STORAGE_OBJECT_MGR.get_macro_block_size(),
      read_info->path_, read_info->access_info_, read_info->macro_block_id_))) {
    LOG_WARN("fail to init load callback", KR(ret), KP(allocator), KPC(read_info), K(io_info));
  }

  io_info.callback_ = load_callback;
  if (OB_FAIL(ret) && OB_NOT_NULL(load_callback)) {
    free_io_callback_and_detach_original(io_info.callback_);
    load_callback = nullptr;
  }
  return ret;
}

int ObExternalRemoteIOCallback::construct_io_callback(
    const ObExternalAccessFileInfo &external_file_info,
    const ObExternalReadInfo &external_read_info,
    ObExternalReadInfo &new_external_read_info)
{
  int ret = OB_SUCCESS;
  ObPCachedExternalFileService *accesser = nullptr;
  ObIAllocator *allocator = nullptr;
  ObExternalRemoteIOCallback *load_callback = nullptr;

  if (OB_UNLIKELY(!external_file_info.is_valid() || !external_read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid read info", KR(ret), K(external_file_info), K(external_read_info));
  } else if (OB_ISNULL(accesser = MTL(ObPCachedExternalFileService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPCachedExternalFileService is null", KR(ret),
        K(external_file_info), K(external_read_info));
  } else if (OB_FAIL(accesser->get_io_callback_allocator(allocator))) {
    LOG_WARN("fail to get io callback allocator", KR(ret), K(external_file_info), K(external_read_info));
  } else if (OB_ISNULL(load_callback =
      OB_NEWx(ObExternalRemoteIOCallback, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObExternalRemoteIOCallback", KR(ret),
        KP(allocator), K(external_file_info), K(external_read_info));
  } else if (OB_FAIL(load_callback->init(allocator,
      external_read_info.io_callback_,
      static_cast<char *>(external_read_info.buffer_),
      external_read_info.offset_ / OB_STORAGE_OBJECT_MGR.get_macro_block_size(),
      external_file_info.get_url(), external_file_info.get_access_info(),
      external_file_info.get_file_content_digest(),
      external_file_info.get_modify_time()))) {
    LOG_WARN("fail to init load callback", KR(ret),
        KP(allocator), K(external_file_info), K(external_read_info));
  }

  new_external_read_info.io_callback_ = load_callback;
  if (OB_FAIL(ret) && OB_NOT_NULL(load_callback)) {
    free_io_callback_and_detach_original(new_external_read_info.io_callback_);
    load_callback = nullptr;
  }
  return ret;
}

void ObExternalRemoteIOCallback::free_io_callback_and_detach_original(
    common::ObIOCallback *&io_callback)
{
  if (OB_NOT_NULL(io_callback)) {
    if (ObIOCallbackType::EXTERNAL_DATA_LOAD_FROM_REMOTE_CALLBACK == io_callback->get_type()) {
      ObExternalRemoteIOCallback *load_callback =
          static_cast<ObExternalRemoteIOCallback *>(io_callback);
      ObIOCallback *original_callback = load_callback->clear_original_io_callback();
      free_io_callback<ObExternalRemoteIOCallback>(io_callback);
      io_callback = original_callback;
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected io callback type", KPC(io_callback));
    }
  }
}

ObExternalRemoteIOCallback::ObExternalRemoteIOCallback()
    : ObIOCallback(ObIOCallbackType::EXTERNAL_DATA_LOAD_FROM_REMOTE_CALLBACK),
      is_inited_(false),
      allocator_(nullptr),
      ori_callback_(nullptr),
      user_data_buf_(nullptr),
      offset_idx_(-1),
      url_(nullptr),
      access_info_(nullptr),
      macro_id_(),
      content_digest_(MTL_ID()),
      modify_time_(-1)
{
}

ObExternalRemoteIOCallback::~ObExternalRemoteIOCallback()
{
  if (OB_NOT_NULL(ori_callback_) && OB_NOT_NULL(ori_callback_->get_allocator())) {
    free_io_callback<ObIOCallback>(ori_callback_);
  }
  if (OB_NOT_NULL(allocator_)) {
    if (!url_.empty()) {
      allocator_->free(url_.ptr());
      url_.reset();
    }

    if (OB_NOT_NULL(access_info_)) {
      OB_DELETEx(ObObjectStorageInfo, allocator_, access_info_);
      access_info_ = nullptr;
    }
  }
}

int ObExternalRemoteIOCallback::base_init_(
    common::ObIAllocator *allocator,
    common::ObIOCallback *original_callback,
    char *user_data_buf,
    const int64_t offset_idx,
    const common::ObString &url,
    const ObObjectStorageInfo *access_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObExternalRemoteIOCallback init twice", KR(ret));
  } else if (OB_ISNULL(allocator) || OB_ISNULL(access_info)
      || OB_UNLIKELY(url.empty() || !access_info->is_valid() || offset_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
        KP(allocator), K(offset_idx), K(url), KPC(access_info));
  } else if (OB_ISNULL(original_callback) && OB_ISNULL(user_data_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("both callback and user data buf are NULL", KR(ret), K(offset_idx),
        KP(allocator), KPC(original_callback), KP(user_data_buf), K(url), KPC(access_info));
  } else if (OB_FAIL(ob_write_string(*allocator, url, url_, true/*c_style*/))) {
    LOG_WARN("fail to deep copy url", KR(ret), K(offset_idx),
        KP(allocator), KPC(original_callback), KP(user_data_buf), K(url), KPC(access_info));
  } else if (OB_FAIL(access_info->clone(*allocator, access_info_))) {
    LOG_WARN("fail to deep copy storage info", KR(ret), K(offset_idx),
        KP(allocator), KPC(original_callback), KP(user_data_buf), K(url), KPC(access_info));
  } else {
    allocator_ = allocator;
    ori_callback_ = original_callback;
    user_data_buf_ = user_data_buf;
    offset_idx_ = offset_idx;
    // base init, do not set is_inited_
  }
  return ret;
}

int ObExternalRemoteIOCallback::init(
    common::ObIAllocator *allocator,
    common::ObIOCallback *original_callback,
    char *user_data_buf,
    const int64_t offset_idx,
    const common::ObString &url,
    const ObObjectStorageInfo *access_info,
    const blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObExternalRemoteIOCallback init twice", KR(ret));
  } else if (OB_UNLIKELY(!GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported shared storage mode", KR(ret),
        KP(allocator), K(offset_idx), K(url), KPC(access_info), K(macro_id));
  } else if (OB_UNLIKELY(!is_valid_external_macro_id(macro_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
        KP(allocator), K(offset_idx), K(url), KPC(access_info), K(macro_id));
  } else if (OB_FAIL(base_init_(
      allocator, original_callback, user_data_buf, offset_idx, url, access_info))) {
    LOG_WARN("fail to base init", KR(ret),
        KP(allocator), K(offset_idx), K(url), KPC(access_info), K(macro_id));
  } else {
    macro_id_ = macro_id;
    is_inited_ = true;
  }
  return ret;
}

int ObExternalRemoteIOCallback::init(
    common::ObIAllocator *allocator,
    common::ObIOCallback *original_callback,
    char *user_data_buf,
    const int64_t offset_idx,
    const common::ObString &url,
    const ObObjectStorageInfo *access_info,
    const common::ObString &content_digest,
    const int64_t modify_time)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObExternalRemoteIOCallback init twice", KR(ret));
  } else if (OB_UNLIKELY(GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported shared storage mode", KR(ret),
        KP(allocator), K(offset_idx), K(url), KPC(access_info), K(content_digest), K(modify_time));
  } else if (OB_UNLIKELY(content_digest.empty() && modify_time <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, content_digest is empty and modify_time is not set", KR(ret),
        KP(allocator), K(offset_idx), K(url), KPC(access_info), K(content_digest), K(modify_time));
  } else if (OB_FAIL(base_init_(
      allocator, original_callback, user_data_buf, offset_idx, url, access_info))) {
    LOG_WARN("fail to base init", KR(ret),
        KP(allocator), K(offset_idx), K(url), KPC(access_info), K(content_digest), K(modify_time));
  } else {
    if (modify_time > 0) {
      modify_time_ = modify_time;
    }
    if (!content_digest.empty()) {
      if (OB_FAIL(content_digest_.set(content_digest))) {
        LOG_WARN("fail to set content digest", KR(ret), KP(allocator),
            K(offset_idx), K(url), KPC(access_info), K(content_digest), K(modify_time));
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObExternalRemoteIOCallback::inner_process(
    const char *data_buffer, const int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t ori_callback_us = 0;
  int64_t load_callback_us = 0;
  const int64_t WARNING_TIME_LIMT_US = 2 * S_US;  // 2s

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObExternalRemoteIOCallback not init", KR(ret));
  } else if (OB_ISNULL(data_buffer) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data buffer size", KR(ret), KP(data_buffer), K(size), KPC(this));
  } else if (OB_ISNULL(ori_callback_)) {
    if (OB_ISNULL(user_data_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("both callback and user data buf are null", KR(ret), KPC(this));
    } else {
      MEMCPY(user_data_buf_, data_buffer, size);
    }
  } else {  // nullptr != callback_
    const int64_t ori_callback_start_us = ObTimeUtility::current_time();
    if (OB_FAIL(ori_callback_->inner_process(data_buffer, size))) {
      LOG_WARN("fail to inner process", KR(ret), KPC(this), KP(data_buffer), K(size));
    }
    ori_callback_us = ObTimeUtility::current_time() - ori_callback_start_us;
  }

  if (OB_SUCC(ret)) {
    // generate preread task
    const int64_t load_callback_start_us = ObTimeUtility::current_time();
    int tmp_ret = OB_SUCCESS;
    ObPCachedExternalFileService *accesser = nullptr;
    if (OB_ISNULL(accesser = MTL(ObPCachedExternalFileService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObPCachedExternalFileService is null", KR(ret), "tenant_id", MTL_ID(), KPC(this));
    } else if (GCTX.is_shared_storage_mode()) {
      if (size == OB_STORAGE_OBJECT_MGR.get_macro_block_size()) {
        if (OB_TMP_FAIL(accesser->ss_add_prefetch_task(data_buffer, size, macro_id_))) {
          LOG_WARN("fail to add preread task for external table", KR(ret), KPC(this));
        }
      } else {
        if (OB_TMP_FAIL(accesser->ss_add_prefetch_task(
            url_, access_info_, offset_idx_, macro_id_))) {
          LOG_WARN("fail to add preread task for external table", KR(ret), KPC(this));
        }
      }
    } else { // SN mode
      const ObString content_digest_obstr(content_digest_.length(), content_digest_.ptr());
      const ObPCachedExtMacroKey macro_key(
          url_, content_digest_obstr, modify_time_,
          offset_idx_ * OB_STORAGE_OBJECT_MGR.get_macro_block_size());
      if (size == OB_STORAGE_OBJECT_MGR.get_macro_block_size()) {
        if (OB_TMP_FAIL(accesser->sn_add_prefetch_task(macro_key, data_buffer, size))) {
          LOG_WARN("fail to add preread task for external table", KR(ret), KPC(this), K(macro_key));
        }
      } else {
        if (OB_TMP_FAIL(accesser->sn_add_prefetch_task(macro_key, access_info_))) {
          LOG_WARN("fail to add preread task for external table", KR(ret), KPC(this), K(macro_key));
        }
      }
    }
    load_callback_us = ObTimeUtility::current_time() - load_callback_start_us;
  }

  if (OB_UNLIKELY((ori_callback_us + load_callback_us) > WARNING_TIME_LIMT_US)) {
    LOG_INFO("callback cost too much time",
        K(ori_callback_us), K(load_callback_us), KP(data_buffer), K(size), KPC(this));
  }
  return ret;
}

} // sql
} // oceanbase