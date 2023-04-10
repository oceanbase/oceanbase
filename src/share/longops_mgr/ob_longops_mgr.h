// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_SHARE_LONGOPS_MGR_LONGOPS_MGR_H_
#define OCEANBASE_SHARE_LONGOPS_MGR_LONGOPS_MGR_H_

#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/container/ob_array.h"
#include "lib/lock/ob_bucket_lock.h"
#include "share/longops_mgr/ob_i_longops.h"

namespace oceanbase
{
namespace share
{
class ObLongopsIterator;
class ObLongopsMgr final
{
public:
  static ObLongopsMgr &get_instance();
  int init();
  void destroy();
  template<typename T>
  int alloc_longops(T *&longops);
  void free_longops(ObILongopsStat *stat);
  int register_longops(ObILongopsStat *stat);
  int unregister_longops(ObILongopsStat *stat);
  int get_longops(const ObILongopsKey &key, ObLongopsValue &value);
  int begin_iter(ObLongopsIterator &iter);
  template <typename Callback>
  int foreach(Callback &callback);
  TO_STRING_KV(K_(is_inited), K(map_.size()));
private:
  ObLongopsMgr();
  ~ObLongopsMgr() { destroy(); }
private:
  typedef common::hash::ObHashMap<ObILongopsKey, ObILongopsStat*, common::hash::SpinReadWriteDefendMode> LongopsMap;
  static const int64_t DEFAULT_BUCKET_NUM = 1543L;
  static const int64_t DEFAULT_ALLOCATOR_PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  bool is_inited_;
  common::ObConcurrentFIFOAllocator allocator_;
  common::ObBucketLock bucket_lock_;
  LongopsMap map_;
};

template<typename T>
int ObLongopsMgr::alloc_longops(T *&longops)
{
  int ret = OB_SUCCESS;
  longops = nullptr;
  void *tmp_buf = nullptr;
  if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(T)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    RS_LOG(WARN, "alloc longops failed", K(ret));
  } else {
    longops = new (tmp_buf) T;
  }
  return ret;
}

using PAIR = common::hash::HashMapPair<ObILongopsKey, ObILongopsStat*>;
class ObLongopsIterator
{
public:
  class ObKeySnapshotCallback
  {
  public:
    explicit ObKeySnapshotCallback(common::ObIArray<ObILongopsKey> &key_snapshot);
    virtual ~ObKeySnapshotCallback() = default;
    int operator()(PAIR &pair);
  private:
    common::ObIArray<ObILongopsKey> &key_snapshot_;
  };
  ObLongopsIterator();
  ~ObLongopsIterator();
  void reset();
  int init(ObLongopsMgr *longops_mgr);
  int get_next(const uint64_t tenant_id, ObLongopsValue &value);
  inline bool is_inited() const { return is_inited_; }
private:
  bool is_inited_;
  common::ObArray<ObILongopsKey> key_snapshot_;
  int64_t key_cursor_;
  ObLongopsMgr *longops_mgr_;
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_LONGOPS_MGR_LONGOPS_MGR_H_
