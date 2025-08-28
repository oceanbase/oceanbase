/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBSERVER_TABLE_OB_HTABLE_ROWKEY_MGR_H
#define OBSERVER_TABLE_OB_HTABLE_ROWKEY_MGR_H
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_rbtree.h"

namespace oceanbase
{
namespace table
{

// record the hbase expired rowkey
struct HRowkeyQueueNode
{
  HRowkeyQueueNode()
  : ls_id_(),
    table_id_(),
    tablet_id_(),
    hrowkey_()
  {}
  ~HRowkeyQueueNode() = default;
  RBNODE(HRowkeyQueueNode, rblink);
  // for rb tree node compare
  OB_INLINE int compare(const HRowkeyQueueNode *other) const
  {
    return hrowkey_.compare(other->hrowkey_);
  }
  share::ObLSID ls_id_;
  int64_t table_id_;
  common::ObTabletID tablet_id_;
  ObString hrowkey_;
  TO_STRING_KV(K_(ls_id), K_(table_id), K_(tablet_id), K_(hrowkey));
};
class HRowkeyQueue
{
public:
  const static int MAX_HROWKEY_QUEUE_SIZE = 10240;
public:
  HRowkeyQueue() = default;
  ~HRowkeyQueue() { destroy(); }
  int push(HRowkeyQueueNode *node);
  int pop(HRowkeyQueueNode *&node);
  int init();
  void destroy();
public:
  int alloc_queue_node(const share::ObLSID &ls_id, int64_t table_id, common::ObTabletID tablet_id,
                       const common::ObString &rowkey, HRowkeyQueueNode *&queue_node);
  void free_queue_node(HRowkeyQueueNode *queue_node);
private:
  common::ObConcurrentFIFOAllocator allocator_;
  common::ObFixedQueue<HRowkeyQueueNode> queue_;
  DISALLOW_COPY_AND_ASSIGN(HRowkeyQueue);
};

class ObHTableRowkeyMgr
{
public:
  typedef common::hash::ObHashMap<share::ObLSID, HRowkeyQueue *> HRowkeyQueueMap;
  const static int DEFAULT_HROWKEY_MAP_SIZE = 1024;
public:
  ObHTableRowkeyMgr() : is_inited_(false) {}
  ~ObHTableRowkeyMgr() = default;
  static int mtl_init(ObHTableRowkeyMgr *&service);
  int init();
  void destroy();
  void record_htable_rowkey(const share::ObLSID &ls_id, int64_t table_id, 
                            common::ObTabletID tablet_id, const common::ObString &rowkey);
  void record_htable_rowkey(const share::ObLSID &ls_id, int64_t table_id, 
                            const ObIArray<ObTabletID> &tablet_ids,
                            const ObString &rowkey);
  int register_rowkey_queue(const share::ObLSID &ls_id, HRowkeyQueue &queue);
  int unregister_rowkey_queue(const share::ObLSID &ls_id, HRowkeyQueue &queue);
private:
  bool is_inited_;
  common::ObConcurrentFIFOAllocator allocator_; // multi-way, thread_safe, but need free by hand
  HRowkeyQueueMap rowkey_queue_map_;
  DISALLOW_COPY_AND_ASSIGN(ObHTableRowkeyMgr);
};

class HTableRowkeyInserter
{
public:
  HTableRowkeyInserter(share::ObLSID ls_id, int64_t table_id, common::ObTabletID tablet_id, common::ObString rowkey)
  : ls_id_(ls_id), table_id_(table_id), tablet_id_(tablet_id), rowkey_(rowkey)
  {
  }
  ~HTableRowkeyInserter() = default;
  void operator() (common::hash::HashMapPair<share::ObLSID, HRowkeyQueue *> &entry);
private:
  share::ObLSID ls_id_;
  int64_t table_id_;
  common::ObTabletID tablet_id_;
  common::ObString rowkey_;
  DISALLOW_COPY_AND_ASSIGN(HTableRowkeyInserter);
};

class HTableRowkeyDeleter
{
public:
  explicit HTableRowkeyDeleter(HRowkeyQueue &rowkey_queue)
  : target_queue_(rowkey_queue)
  {
  }
  ~HTableRowkeyDeleter() = default;
  bool operator() (common::hash::HashMapPair<share::ObLSID, HRowkeyQueue *> &entry);
private:
  HRowkeyQueue &target_queue_;
  DISALLOW_COPY_AND_ASSIGN(HTableRowkeyDeleter);
};

} // end namespace table
} // end namespace oceanbase

#endif
