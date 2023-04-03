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

#define USING_LOG_PREFIX SHARE
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"
#include "lib/rc/ob_rc.h"
#include "lib/thread/thread_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::lib;

const char *ObPlanMonitorNodeList::MOD_LABEL = "SqlPlanMon";

int ObMonitorNode::add_rt_monitor_node(ObMonitorNode *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) ||
      OB_NOT_NULL(node->prev_) ||
      OB_NOT_NULL(node->next_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected node", K(node));
  } else {
    next_ = node;
    node->prev_ = this;
  }
  return ret;
}

ObPlanMonitorNodeList::~ObPlanMonitorNodeList()
{
  if (inited_) {
    destroy();
  }
}

int ObPlanMonitorNodeList::init(uint64_t tenant_id,
                                const int64_t max_mem_size,
                                const int64_t queue_size)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(queue_.init(MOD_LABEL, queue_size, tenant_id))) {
    SERVER_LOG(WARN, "Failed to init ObMySQLRequestQueue", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::ReqMemEvict, tg_id_))) {
    SERVER_LOG(WARN, "create failed", K(ret));
  } else if (OB_FAIL(node_map_.create(!lib::is_mini_mode() ? DEFAULT_BUCKETS_COUNT :
        DEFAULT_BUCKETS_COUNT / 100,
        "SqlPlanMonMap",
        "SqlPlanMonMap"))) {
    LOG_WARN("failed to create hash map", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    SERVER_LOG(WARN, "init timer fail", K(ret));
  } else if (OB_FAIL(allocator_.init(MONITOR_NODE_PAGE_SIZE,
                                     MOD_LABEL,
                                     tenant_id,
                                     INT64_MAX))) {
    SERVER_LOG(WARN, "failed to init allocator", K(ret));
  } else {
    //check FIFO mem used and sql audit records every 1 seconds
    if (OB_FAIL(task_.init(this))) {
      SERVER_LOG(WARN, "fail to init sql audit time tast", K(ret));
    } else if (OB_FAIL(TG_SCHEDULE(tg_id_, task_, EVICT_INTERVAL, true))) {
      SERVER_LOG(WARN, "start eliminate task failed", K(ret));
    } else {
      rt_node_id_ = -1;
      mem_limit_ = max_mem_size;
      tenant_id_ = tenant_id;
      inited_ = true;
      destroyed_ = false;
    }
  }
  if ((OB_FAIL(ret)) && (!inited_)) {
    destroy();
  }
  return ret;
}

void ObPlanMonitorNodeList::destroy()
{
  if (!destroyed_) {
    TG_DESTROY(tg_id_);
    clear_queue();
    queue_.destroy();
    allocator_.destroy();
    inited_ = false;
    destroyed_ = true;
  }
}

int ObPlanMonitorNodeList::mtl_init(ObPlanMonitorNodeList* &node_list)
{
  int ret = OB_SUCCESS;
  node_list = OB_NEW(ObPlanMonitorNodeList, MOD_LABEL);
  if (nullptr == node_list) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for ObPlanMonitorNodeList", K(ret));
  } else {
    uint64_t tenant_id = lib::current_resource_owner_id();
    int64_t mem_limit = get_tenant_memory_limit(tenant_id);
    if (OB_FAIL(node_list->init(tenant_id, mem_limit, MAX_QUEUE_SIZE))) {
      LOG_WARN("failed to init event list", K(ret));
    }
  }
  if (OB_FAIL(ret) && node_list != nullptr) {
    // cleanup
    common::ob_delete(node_list);
    node_list = nullptr;
  }
  return ret;
}

void ObPlanMonitorNodeList::mtl_destroy(ObPlanMonitorNodeList* &node_list)
{
  common::ob_delete(node_list);
  node_list = nullptr;
}

int ObPlanMonitorNodeList::submit_node(ObMonitorNode &node)
{
  int ret = OB_SUCCESS;
  ObMonitorNode *deep_cp_node = nullptr;
  char *buf = nullptr;
  // todo: add extra string memory for string field
  int64_t mem_size = sizeof(ObMonitorNode);
  if (nullptr == (buf = (char*)alloc_mem(mem_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail alloc mem", K(mem_size), K(ret));
  } else {
    deep_cp_node = new(buf) ObMonitorNode(node);
    int64_t req_id = 0;
    if(OB_FAIL(queue_.push(deep_cp_node, req_id))) {
      //sql audit槽位已满时会push失败, 依赖后台线程进行淘汰获得可用槽位
      if (REACH_TIME_INTERVAL(2 * 1000 * 1000)) {
        SERVER_LOG(WARN, "push into queue failed", K(get_size_used()), K(get_size()), K(ret));
      }
      free_mem(deep_cp_node);
      deep_cp_node = NULL;
    }
  }
  return ret;
}

int ObPlanMonitorNodeList::register_monitor_node(ObMonitorNode &node)
{
  int ret = OB_SUCCESS;
  ObMonitorNodeKey key;
  key.node_id_ = ATOMIC_AAF(&rt_node_id_, 1);
  LOG_TRACE("register monitor node", K(key.node_id_), K(&node), K(lbt()));
  node.set_rt_node_id(key.node_id_);
  if (OB_FAIL(node_map_.set_refactored(key, &node))) {
    LOG_WARN("fail to set moniotr node", K(ret), K(key));
  }
  return ret;
}

int ObPlanMonitorNodeList::revert_monitor_node(ObMonitorNode &node)
{
  int ret = OB_SUCCESS;
  ObMonitorNodeKey key;
  key.node_id_ = node.get_rt_node_id();
  ObMonitorNode *node_ptr = NULL;
  LOG_TRACE("revert monitor node", K(key.node_id_), K(&node));
  if (OB_FAIL(node_map_.erase_refactored(key, &node_ptr))) {
    LOG_WARN("fail to erase moniotr node", K(ret), K(key));
  }
  return ret;
}


int ObPlanMonitorNodeList::convert_node_map_2_array(common::ObIArray<ObMonitorNode> &array)
{
  int ret = OB_SUCCESS;
  ObPlanMonitorNodeList::ObMonitorNodeTraverseCall call(array);
  if (OB_FAIL(node_map_.foreach_refactored(call))) {
    LOG_WARN("fail to traverse node map", K(ret));
  }
  return ret;
}

int ObPlanMonitorNodeList::ObMonitorNodeTraverseCall::operator() (
    common::hash::HashMapPair<ObMonitorNodeKey,
    ObMonitorNode *> &entry)
{
  int ret = OB_SUCCESS;
  ObMonitorNode *head = entry.second;
  while (OB_NOT_NULL(head) && OB_SUCC(ret)) {
    if (OB_FAIL(node_array_.push_back(*head))) {
      LOG_WARN("fail to push back mointor node", K(ret));
    } else {
      head = head->next_;
    }
  }
  return ret;
}

void ObSqlPlanMonitorRecycleTask::runTimerTask()
{
  if (node_list_) {
    node_list_->recycle_old(node_list_->get_recycle_count());
  }
}

int ObSqlPlanMonitorRecycleTask::init(ObPlanMonitorNodeList *node_list)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node_list)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    node_list_ = node_list;
  }
  return ret;
}

