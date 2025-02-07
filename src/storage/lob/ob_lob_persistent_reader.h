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

#ifndef OCEABASE_STORAGE_OB_LOB_PERSISTENT_READER_
#define OCEABASE_STORAGE_OB_LOB_PERSISTENT_READER_

#include "storage/lob/ob_lob_access_param.h"

namespace oceanbase
{
namespace storage
{


class ObLobMetaIterator;

struct ObPersistLobReaderCacheKey
{
  ObPersistLobReaderCacheKey():
    ls_id_(),
    tablet_id_(),
    snapshot_(0),
    is_get_(false)
  {}

  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  int64_t snapshot_;
  bool is_get_;
  bool operator==(const ObPersistLobReaderCacheKey &other) const
  {
    return snapshot_ == other.snapshot_ && tablet_id_ == other.tablet_id_ && ls_id_ == other.ls_id_ && is_get_ == other.is_get_;
  }

  TO_STRING_KV(K(ls_id_), K(tablet_id_), K(snapshot_));
};

struct ObPersistLobReaderCacheNode : public ObDLinkBase<ObPersistLobReaderCacheNode>
{
  ObPersistLobReaderCacheNode():
    key_(),
    reader_(nullptr)
  {}

  ObPersistLobReaderCacheKey key_;
  ObLobMetaIterator *reader_;
};

class ObPersistLobReaderCache
{
public:
  static const int DEFAULT_CAP = 10;

public:
  ObPersistLobReaderCache(int32_t cap = DEFAULT_CAP):
    allocator_(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE/*8KB*/, MTL_ID()),
    cap_(cap)
  {}
  ~ObPersistLobReaderCache();

  int get(ObPersistLobReaderCacheKey key, ObLobMetaIterator *&reader);
  int put(ObPersistLobReaderCacheKey key, ObLobMetaIterator *reader);

  ObLobMetaIterator* alloc_reader(const ObLobAccessCtx *access_ctx);
  ObIAllocator& get_allocator() { return allocator_; }

private:
  int remove_first();

private:
  ObArenaAllocator allocator_;
  const int32_t cap_;
  ObDList<ObPersistLobReaderCacheNode> list_;
};


struct ObLobAccessCtx
{
  ObLobAccessCtx():
    reader_cache_()
  {}
  ObPersistLobReaderCache reader_cache_;
};


} // storage
} // oceanbase

#endif