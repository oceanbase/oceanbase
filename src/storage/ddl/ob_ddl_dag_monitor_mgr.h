/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef __OB_STORAGE_DDL_DAG_MONITOR_MGR_H__
#define __OB_STORAGE_DDL_DAG_MONITOR_MGR_H__

#include "lib/lock/ob_spin_lock.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/task/ob_timer.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/time/ob_time_utility.h"
#include "lib/hash/ob_hashmap.h"
#include "storage/ddl/ob_ddl_dag_monitor_node.h"

namespace oceanbase
{
namespace storage
{

// Tenant-level independent DAG monitor manager.
class ObDDLDagMonitorMgr
{
public:
  static const int64_t DEFAULT_MAX_NODE_COUNT = 10000L * 100L; //100w
  static const int64_t DEFAULT_TTL_US = 24 * 60 * 60 * 1000000L; // 24 hours
  static const int64_t DEFAULT_ALLOCATOR_SIZE = 16 * 1024 * 1024; // 16MB
  static const int64_t DEFAULT_CLEAN_INTERVAL_US = 60 * 1000000L; // 60s

  ObDDLDagMonitorMgr();
  virtual ~ObDDLDagMonitorMgr();

  // MTL interface.
  static int mtl_init(ObDDLDagMonitorMgr *&m);
  int init();
  void destroy();

  // Node lifecycle APIs (one node per independent DAG execution).
  int register_node(const void *dag_ptr,
                    const common::ObCurTraceId::TraceId &trace_id,
                    ObDDLDagMonitorNode *&node);

  enum CleanMode
  {
    CLEAN_ALL_NODE = 0,
    CLEAN_EXPIRED_NODE,
    CLEAN_FINISHED_INFO
  };

  // Cleanup API:
  // - CLEAN_ALL_NODE: remove all nodes
  // - CLEAN_EXPIRED_NODE: remove finished & expired nodes
  // - CLEAN_FINISHED_INFO: if node expired, remove node; otherwise clean finished infos in node
  int clean_nodes(const CleanMode mode);

  // Stats.
  int64_t get_node_count() const;
  int64_t get_allocated_size() const;

  // Config.
  void set_max_node_count(const int64_t max_node_count) { max_node_count_ = max_node_count; }
  void set_ttl_us(const int64_t ttl_us) { ttl_us_ = ttl_us; }

  // Display APIs for virtual table.
  int get_all_nodes(common::ObIArray<ObDDLDagMonitorNode *> &nodes);

private:
  typedef ObDDLDagMonitorNode::Key NodeKey;
  typedef common::hash::ObHashMap<NodeKey,
                                  ObDDLDagMonitorNode *,
                                  common::hash::NoPthreadDefendMode> NodeMap;

  class MonitorAllocator : public common::ObIAllocator
  {
  public:
    MonitorAllocator();
    virtual ~MonitorAllocator() = default;
    int init(ObDDLDagMonitorMgr *mgr,
             const int64_t block_size,
             const common::ObMemAttr &attr,
             const int64_t total_limit);
    void destroy();
    void *alloc(const int64_t size) override;
    void *alloc(const int64_t size, const common::ObMemAttr &attr) override;
    void *inner_alloc(const int64_t size);
    void *inner_alloc(const int64_t size, const common::ObMemAttr &attr);
    void free(void *ptr) override;
    int64_t total() const override;
    int64_t used() const override;
    int64_t allocated() const;
    void set_total_limit(const int64_t total_limit);
  private:
    ObDDLDagMonitorMgr *mgr_;
    common::ObConcurrentFIFOAllocator fifo_allocator_;
  };

private:
  // NOTE: caller must already hold rwlock_ write lock.
  int clean_nodes_unlock(const CleanMode mode);
  void update_memory_limit();

  int inner_create_node(const NodeKey &key,
                        ObDDLDagMonitorNode *&node);
  int inner_remove_node(ObDDLDagMonitorNode *node);

  class CleanTimerTask : public common::ObTimerTask
  {
  public:
    explicit CleanTimerTask(ObDDLDagMonitorMgr &mgr) : mgr_(mgr) {}
    virtual ~CleanTimerTask() = default;
    void runTimerTask() override { (void)mgr_.clean_nodes(CLEAN_EXPIRED_NODE); }
  private:
    ObDDLDagMonitorMgr &mgr_;
  };

private:
  bool is_inited_;
  MonitorAllocator allocator_;
  int64_t max_node_count_;
  int64_t ttl_us_;
  int64_t memory_limit_;
  mutable common::SpinRWLock rwlock_;
  NodeMap node_map_;
  common::ObTimer clean_timer_;
  CleanTimerTask clean_task_;
  int64_t clean_interval_us_;
};

} // namespace storage
} // namespace oceanbase

#endif // __OB_STORAGE_DDL_DAG_MONITOR_MGR_H__
