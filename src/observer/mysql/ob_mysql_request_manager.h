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

#ifndef SRC_OBSERVER_MYSQL_OB_MYSQL_REQUEST_MANAGER_H_
#define SRC_OBSERVER_MYSQL_OB_MYSQL_REQUEST_MANAGER_H_

#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/stat/ob_diagnose_info.h"
#include "observer/mysql/ob_mysql_result_set.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_define.h"
#include "sql/ob_result_set.h"
#include "ob_eliminate_task.h"
#include "ob_ra_queue.h"
#include "observer/mysql/ob_dl_queue.h"
#include "ob_construct_queue.h"

namespace oceanbase
{
namespace common
{
  class ObConcurrentFIFOAllocator;
}

namespace obmysql
{

enum ObMySQLRequestStatus
{
  REQUEST_SUCC = 0,
  REQUEST_FAIL,
};

class ObMySQLRequestRecord
{
public:
  common::ObConcurrentFIFOAllocator *allocator_;
  sql::ObAuditRecordData data_;

public:
  ObMySQLRequestRecord()
    : allocator_(nullptr) {}
  virtual ~ObMySQLRequestRecord();

public:
  virtual void destroy()
  {
    if (NULL != allocator_) {
      allocator_->free(this);
    }
  }

public:
  int64_t get_self_size() const
  {
    return sizeof(ObMySQLRequestRecord) + data_.get_extra_size();
  }
};

class ObMySQLRequestManager
{
public:
  static const int64_t SQL_AUDIT_PAGE_SIZE = (1LL << 21) - ACHUNK_PRESERVE_SIZE; // 2M - 17k
  static const int64_t MAX_PARAM_BUF_SIZE = 64 * 1024;
  static const int32_t MAX_RELEASE_TIME = 5 * 1000; //5ms
  static const int64_t US_PER_HOUR = 3600000000;
  //初始化queue大小，常规模式1000w，mini模式10w
  static const int64_t MAX_QUEUE_SIZE = 10000000; //1000w
  static const int64_t MINI_MODE_MAX_QUEUE_SIZE = 100000; // 10w
  //按行淘汰的高低水位线，以queue大小的百分比设定
  static constexpr float HIGH_LEVEL_EVICT_PERCENTAGE = 0.9; // 90%
  static constexpr float LOW_LEVEL_EVICT_PERCENTAGE = 0.8; // 80%
  //每进行一次release_old操作删除的sql_audit百分比
  static const int64_t BATCH_RELEASE_SIZE = 64 * 1024; //32k
  static const int64_t MINI_MODE_BATCH_RELEASE_SIZE = 4 * 1024; //4k
  static const int64_t CONSTRUCT_EVICT_INTERVAL = 500000; //0.5s
  //启动淘汰检查的时间间隔
  static const int64_t EVICT_INTERVAL = 500000; //1s
  typedef common::ObDlQueue::DlRef DlRef;
  typedef lib::ObLockGuard<common::ObRecursiveMutex> LockGuard;
public:
  ObMySQLRequestManager();
  virtual ~ObMySQLRequestManager();

public:
  int init(uint64_t tenant_id, const int64_t max_mem_size, const int64_t queue_size);
  int start();
  void wait();
  void stop();
  void destroy();

public:

  static int mtl_new(ObMySQLRequestManager* &req_mgr);
  static int mtl_init(ObMySQLRequestManager* &req_mgr);
  static void mtl_destroy(ObMySQLRequestManager* &req_mgr);

  common::ObConcurrentFIFOAllocator *get_allocator() { return &allocator_; }
  int64_t get_request_id() { ATOMIC_INC(&request_id_); return request_id_; }

  int record_request(const ObAuditRecordData &audit_record,
                     const bool enable_query_response_time_stats,
                     const int64_t query_record_size_limit,
                     bool is_sensitive = false);
  int64_t get_start_idx();
  int64_t get_end_idx();
  int64_t get_capacity();
  int64_t get_size_used();
  common::ObRecursiveMutex &get_destroy_second_queue_lock() { return destroy_second_level_mutex_; }
  common::ObDlQueue &get_queue() { return queue_; }
  int get(const int64_t idx, void *&record, DlRef* ref)
  {
    int ret = OB_SUCCESS;
    if (NULL == (record = queue_.get(idx, ref))) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    return ret;
  }

  int revert(DlRef* ref)
  {
    queue_.revert(ref);
    return common::OB_SUCCESS;
  }

  /**
   * called when memory limit exceeded
   */

  int release_old(int64_t limit) {
    void* req = NULL;
    int64_t count = 0;
    while(count++ < limit && NULL != (req = queue_.pop())) {
      free(req);
    }
    return common::OB_SUCCESS;
  }

  int release_record(int64_t release_cnt);

  int clear_leaf_queue(int64_t idx, int64_t size);
  void freeCallback(void* ptr);

  void* alloc(const int64_t size)
  {
    void * ret = allocator_.alloc(size);
    return ret;
  }

  void free(void *ptr) { allocator_.free(ptr); ptr = NULL;}

  void clear_queue()
  {
    while (queue_.get_pop_idx() < queue_.get_cur_idx()) {
      (void)release_record(INT64_MAX);
    }
    (void)release_record(INT64_MAX);
    allocator_.purge();
  }

  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }

  bool is_valid() const
  {
    return inited_ && !destroyed_;
  }

  static int get_mem_limit(uint64_t tenant_id, int64_t &mem_limit);

private:
  DISALLOW_COPY_AND_ASSIGN(ObMySQLRequestManager);

private:
  bool inited_;
  bool destroyed_;
  uint64_t request_id_;
  int64_t mem_limit_;
  common::ObConcurrentFIFOAllocator allocator_;//alloc mem for string buf
  common::ObDlQueue queue_;
  ObEliminateTask task_;

  // tenant id of this request manager
  uint64_t tenant_id_;
  int tg_id_;
  volatile bool stop_flag_;
  //Control concurrency when destroying the secondary queue.
  common::ObRecursiveMutex destroy_second_level_mutex_;
  ObConstructQueueTask construct_task_;
};

} // end of namespace obmysql
} // end of namespace oceanbase



#endif /* SRC_OBSERVER_MYSQL_OB_MYSQL_REQUEST_MANAGER_H_ */
