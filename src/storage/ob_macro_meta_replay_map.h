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

#ifndef OB_MACRO_META_REPLAY_MAP_H_
#define OB_MACRO_META_REPLAY_MAP_H_

#include "lib/hash/ob_hashmap.h"
#include "blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_i_table.h"

namespace oceanbase {
namespace storage {

struct ObMacroBlockKey final {
public:
  ObMacroBlockKey();
  ObMacroBlockKey(const ObITable::TableKey& table_key, const blocksstable::MacroBlockId& macro_block_id);
  ~ObMacroBlockKey() = default;
  uint64_t hash() const;
  bool operator==(const ObMacroBlockKey& other) const;
  TO_STRING_KV(K_(table_key), K_(macro_block_id));
  ObITable::TableKey table_key_;
  blocksstable::MacroBlockId macro_block_id_;
};

class ObMacroMetaReplayMap final {
public:
  ObMacroMetaReplayMap();
  ~ObMacroMetaReplayMap();

  int init();
  void destroy();
  int set(const ObMacroBlockKey& key, blocksstable::ObMacroBlockMetaV2& meta, const bool overwrite);
  int get(const ObMacroBlockKey& key, blocksstable::ObMacroBlockMetaV2*& meta);
  int remove(const ObITable::TableKey& table_key, const blocksstable::MacroBlockId& block_id);
  int remove(const ObITable::TableKey& table_key, const common::ObIArray<blocksstable::MacroBlockId>& block_ids);

private:
  static const int64_t REPLAY_BUCKET_CNT = 10000;
  typedef common::hash::ObHashMap<ObMacroBlockKey, blocksstable::ObMacroBlockMetaV2*> MAP;
  MAP map_;
  common::ObLfFIFOAllocator allocator_;
  lib::ObMutex map_lock_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObMacroMetaReplayMap);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_MACRO_META_REPLAY_MAP_H_
