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

#include "ob_macro_meta_replay_map.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObMacroBlockKey::ObMacroBlockKey() : table_key_(), macro_block_id_()
{}

ObMacroBlockKey::ObMacroBlockKey(const ObITable::TableKey& table_key, const blocksstable::MacroBlockId& macro_block_id)
    : table_key_(table_key), macro_block_id_(macro_block_id)
{}

uint64_t ObMacroBlockKey::hash() const
{
  uint64_t hash_val = table_key_.hash();
  uint64_t hash_macro_block_id = macro_block_id_.hash();
  hash_val = common::murmurhash2(&hash_macro_block_id, sizeof(hash_macro_block_id), hash_val);
  return hash_val;
}

bool ObMacroBlockKey::operator==(const ObMacroBlockKey& other) const
{
  bool bret = table_key_ == other.table_key_ && macro_block_id_ == other.macro_block_id_;
  return bret;
}

ObMacroMetaReplayMap::ObMacroMetaReplayMap() : map_(), map_lock_(), is_inited_(false)
{}

ObMacroMetaReplayMap::~ObMacroMetaReplayMap()
{
  destroy();
}

int ObMacroMetaReplayMap::init()
{
  int ret = OB_SUCCESS;
  const char* label = "MacroMetaReplay";
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObMacroMetaReplayMap has been inited twice", K(ret));
  } else if (OB_FAIL(map_.create(REPLAY_BUCKET_CNT, label))) {
    LOG_WARN("fail to create macro meta map", K(ret));
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_BIG_BLOCK_SIZE, label))) {
    LOG_WARN("fail to init allocator", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObMacroMetaReplayMap::set(const ObMacroBlockKey& key, ObMacroBlockMetaV2& meta, const bool overwrite)
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaV2* new_meta = nullptr;
  lib::ObMutexGuard guard(map_lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMacroMetaReplayMap has not been inited", K(ret));
  } else if (OB_FAIL(meta.deep_copy(new_meta, allocator_))) {
    LOG_WARN("fail to deep copy macro meta", K(ret));
  } else if (OB_FAIL(map_.set_refactored(key, new_meta))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      if (overwrite) {
        ObMacroBlockMetaV2* delete_meta = nullptr;
        if (OB_FAIL(map_.erase_refactored(key, &delete_meta))) {
          LOG_WARN("fail to erase from map", K(ret), K(key));
        } else if (OB_FAIL(map_.set_refactored(key, new_meta))) {
          LOG_WARN("fail to overwrite map", K(ret), K(key), K(overwrite), K(meta));
        } else {
          new_meta = nullptr;
          delete_meta->~ObMacroBlockMetaV2();
          allocator_.free(delete_meta);
          delete_meta = nullptr;
          LOG_INFO("succeed to overwrite replay macro meta", K(key), KP(new_meta), K(key.hash()));
        }
      }
    } else {
      LOG_WARN("fail to set map", K(ret), K(key), K(overwrite), K(meta));
    }
  } else {
    new_meta = nullptr;
    LOG_INFO("succeed to set replay macro meta", K(key), KP(new_meta), K(key.hash()));
  }
  if (nullptr != new_meta) {
    new_meta->~ObMacroBlockMetaV2();
    new_meta = nullptr;
  }
  return ret;
}

int ObMacroMetaReplayMap::get(const ObMacroBlockKey& key, ObMacroBlockMetaV2*& meta)
{
  int ret = OB_SUCCESS;
  meta = nullptr;
  lib::ObMutexGuard guard(map_lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMacroMetaReplayMap has not been inited", K(ret));
  } else if (OB_FAIL(map_.get_refactored(key, meta))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("fail to get from hashmap", K(ret), K(key));
    }
  }
  return ret;
}

int ObMacroMetaReplayMap::remove(const ObITable::TableKey& table_key, const blocksstable::MacroBlockId& block_id)
{
  int ret = OB_SUCCESS;
  ObMacroBlockKey macro_key;
  macro_key.table_key_ = table_key;
  macro_key.macro_block_id_ = block_id;
  ObMacroBlockMetaV2* meta = nullptr;
  lib::ObMutexGuard guard(map_lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMacroMetaReplayMap has not been inited", K(ret));
  } else if (OB_FAIL(map_.erase_refactored(macro_key, &meta))) {
    LOG_WARN("fail to erase from map", K(ret), K(macro_key));
  } else {
    meta->~ObMacroBlockMetaV2();
    allocator_.free(meta);
    meta = nullptr;
  }
  return ret;
}

int ObMacroMetaReplayMap::remove(
    const ObITable::TableKey& table_key, const common::ObIArray<blocksstable::MacroBlockId>& block_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMacroMetaReplayMap has not been inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < block_ids.count(); ++i) {
      if (OB_FAIL(remove(table_key, block_ids.at(i)))) {
        LOG_WARN("fail to remove macro key", K(ret), K(table_key));
      }
    }
  }
  return ret;
}

void ObMacroMetaReplayMap::destroy()
{
  if (is_inited_) {
    for (MAP::iterator iter = map_.begin(); iter != map_.end(); ++iter) {
      ObMacroBlockMetaV2* meta = iter->second;
      if (nullptr != meta) {
        meta->~ObMacroBlockMetaV2();
        allocator_.free(meta);
        meta = nullptr;
      }
    }
    map_.destroy();
    allocator_.destroy();
    is_inited_ = false;
  }
}
