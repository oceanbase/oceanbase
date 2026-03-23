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

class ObPersistLobReaderCache
{
public:
  static const uint32_t TIMEOUT_CHECK_THROTTLE_INTERVAL = 1024;

  ObPersistLobReaderCache():
    allocator_(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE/*8KB*/, MTL_ID()),
    cached_key_(),
    cached_reader_(nullptr),
    last_cached_time_us_(0),
    timeout_check_call_count_(0)
  {}
  ~ObPersistLobReaderCache();

  // If cached reader exists and param.tablet_id matches, return it; else return nullptr.
  int get(ObPersistLobReaderCacheKey key, ObLobMetaIterator *&reader);
  // Replace cached reader: reset and free old one if exists, then store (key, reader).
  int put(ObPersistLobReaderCacheKey key, ObLobMetaIterator *reader);

  ObLobMetaIterator* alloc_reader(const ObLobAccessCtx *access_ctx);
  ObIAllocator& get_allocator() { return allocator_; }

  // If cached iter has been held longer than timeout_us (default 10s), reset it to release memtable ref.
  void check_and_release_if_timeout(const int64_t timeout_us = 10 * 1000 * 1000);

private:
  void reset_and_clear();

private:
  ObArenaAllocator allocator_;
  ObPersistLobReaderCacheKey cached_key_;
  ObLobMetaIterator *cached_reader_;
  int64_t last_cached_time_us_;
  uint32_t timeout_check_call_count_;  // throttle: only do real check every TIMEOUT_CHECK_THROTTLE_INTERVAL calls
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