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

#define USING_LOG_PREFIX SERVER
#include "ob_htable_rowkey_mgr.h"
#include "share/table/ob_ttl_util.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace table
{

int ObHTableRowkeyMgr::mtl_init(ObHTableRowkeyMgr *&hrowkey_mgr)
{
  return hrowkey_mgr->init();
}

int ObHTableRowkeyMgr::init()
{
  int ret = OB_SUCCESS;
  const int64_t memory_limit = 100 * 1024L * 1024L; // 100MB
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), "tenant_id", MTL_ID());
  } else if (OB_FAIL(rowkey_queue_map_.create(DEFAULT_HROWKEY_MAP_SIZE, "HTblRkMapBkt", "HTblRkMapNode", MTL_ID()))) {
    LOG_WARN("fail to create htable rowkey queue map", K(ret), "tenant_id", MTL_ID());
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_NORMAL_BLOCK_SIZE, "HTblRkMgrAlloc", MTL_ID(), memory_limit))) {
    LOG_WARN("fail to init allocator", K(ret), "tenant_id", MTL_ID());
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObHTableRowkeyMgr::destroy()
{
  rowkey_queue_map_.destroy();
  is_inited_ = false;
}

void ObHTableRowkeyMgr::record_htable_rowkey(const ObLSID &ls_id, int64_t table_id,
                                             ObTabletID tablet_id, const ObString &rowkey)
{
  if (ObTTLUtil::is_enable_ttl(MTL_ID())) {
    int ret = OB_SUCCESS;
    HTableRowkeyInserter inserter(ls_id, table_id, tablet_id, rowkey);
    if (OB_FAIL(rowkey_queue_map_.read_atomic(ls_id, inserter))) {
      LOG_WARN("fail to insert queue map", K(ret), K(ls_id));
    }
  }
}

int ObHTableRowkeyMgr::register_rowkey_queue(const share::ObLSID &ls_id, HRowkeyQueue &queue)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rowkey_queue_map_.set_refactored(ls_id, &queue))) {
    LOG_WARN("fail to insert rowkey queue into map", K(ret), K(ls_id));
  } else {
    LOG_INFO("register htable rowkey queue in queue map", K(ret), K(ls_id));
  }
  return ret;
}

// because the unregister is finished by ttl timer task after leader switch to follower
// we should not erase the queue when it register by others
int ObHTableRowkeyMgr::unregister_rowkey_queue(const share::ObLSID &ls_id, HRowkeyQueue &queue)
{
  int ret = OB_SUCCESS;
  HTableRowkeyDeleter delete_op(queue);
  bool is_erased = true;
  if (OB_FAIL(rowkey_queue_map_.erase_if(ls_id, delete_op, is_erased))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to delete rowkey queue from map", K(ret), K(ls_id));
    }
  } else if (is_erased) {
    LOG_INFO("unregister htable rowkey queue in queue map", K(ret), K(ls_id));
  }
  return ret;
}

void HTableRowkeyInserter::operator() (common::hash::HashMapPair<share::ObLSID, HRowkeyQueue *> &entry)
{
  int ret = common::OB_SUCCESS;
  HRowkeyQueue *queue = entry.second;
  HRowkeyQueueNode *queue_node = nullptr;
  if (OB_ISNULL(queue)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null rowkey queue", K(ret), "ls_id", entry.first);
  } else {
    if (OB_FAIL(queue->alloc_queue_node(ls_id_, table_id_, tablet_id_, rowkey_, queue_node))) {
      LOG_WARN("fail to alloc queue node", K(ret), K_(ls_id), K_(table_id), K_(tablet_id), K_(rowkey));
    } else if (OB_FAIL(queue->push(queue_node))) {
      queue->free_queue_node(queue_node);
      LOG_WARN("fail to push queue node", K(ret));
    }
  }
}

bool HTableRowkeyDeleter::operator() (common::hash::HashMapPair<share::ObLSID, HRowkeyQueue *> &entry)
{
  int bret = false;
  if (entry.second == &target_queue_) {
    bret = true;
  }
  return bret;
}

int HRowkeyQueue::init()
{
  int ret = OB_SUCCESS;
  const int64_t memory_limit = 100 * 1024L * 1024L; // 100MB
  if (OB_FAIL(allocator_.init(OB_MALLOC_NORMAL_BLOCK_SIZE, "HTblRkQueAlloc", MTL_ID(), memory_limit))) {
    LOG_WARN("fail to init allocator", K(ret), "tenant_id", MTL_ID());
  } else if (OB_FAIL(queue_.init(MAX_HROWKEY_QUEUE_SIZE, &allocator_, "HTblRkQueue"))) {
    LOG_WARN("fail to init htabel rowkey queue", K(ret));
  } else {}

  return ret;
}

int HRowkeyQueue::alloc_queue_node(const share::ObLSID &ls_id, int64_t table_id, common::ObTabletID tablet_id,
                                   const common::ObString &rowkey, HRowkeyQueueNode *&queue_node)
{
  int ret = OB_SUCCESS;
  HRowkeyQueueNode *new_node = OB_NEWx(HRowkeyQueueNode, &allocator_);
  if (OB_ISNULL(new_node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, rowkey, new_node->hrowkey_))) {
    free_queue_node(new_node);
    LOG_WARN("fail to copy string", K(ret), K(rowkey));
  } else {
    new_node->ls_id_ = ls_id;
    new_node->table_id_ = table_id;
    new_node->tablet_id_ = tablet_id;
    queue_node = new_node;
  }
  return ret;
}
void HRowkeyQueue::free_queue_node(HRowkeyQueueNode *queue_node)
{
  if (OB_NOT_NULL(queue_node)) {
    queue_node->~HRowkeyQueueNode();
    allocator_.free(queue_node->hrowkey_.ptr());
    allocator_.free(queue_node);
  }
}


int HRowkeyQueue::push(HRowkeyQueueNode *node)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(queue_.push(node))) {
    LOG_WARN("fail to push queue node", K(ret));
  }
  return ret;
}

// return OB_ENTRY_NOT_EXIST if queue is empty
int HRowkeyQueue::pop(HRowkeyQueueNode *&node)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(queue_.pop(node))) {
    if (ret != OB_ENTRY_NOT_EXIST) {
      LOG_WARN("fail to pop queue node", K(ret));
    }
  }
  return ret;
}

void HRowkeyQueue::destroy()
{
  int ret = OB_SUCCESS;
  HRowkeyQueueNode *node = nullptr;
  while (OB_SUCC(queue_.pop(node))) {
    free_queue_node(node);
  }
  queue_.destroy();
}

} // end namespace table
} // end namespace oceanbase