/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_OB_SQL_STAT_MANAGER_H_
#define OCEANBASE_SQL_OB_SQL_STAT_MANAGER_H_

#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/ob_define.h"
#include "sql/monitor/ob_sql_stat_record.h"
#include <cstdint>

namespace oceanbase {
namespace sql {

class ObSqlStatTask : public common::ObTimerTask
{
public:
  ObSqlStatTask() {}
  virtual ~ObSqlStatTask() = default;
  virtual void runTimerTask() override;
  constexpr static int64_t REFRESH_INTERVAL = 1 * 60 * 1000L * 1000L;
};

template <typename T, typename N>
class ObSqlStatRecordNodeAlloc
{
public:
  ObSqlStatRecordNodeAlloc(ObIAllocator *allocator, int64_t alloc_limit)
      : alloc_count_(0), alloc_limit_(alloc_limit), allocator_(allocator)
  {}
  ~ObSqlStatRecordNodeAlloc()
  {}
  T *alloc_value()
  {
    int ret = OB_SUCCESS;
    T *sqlstat = nullptr;
    if (ATOMIC_LOAD(&alloc_count_) > alloc_limit_) {
      ret = OB_EAGAIN;
      COMMON_LOG(WARN, "sql stat record node alloc count exceed limit", K(alloc_count_), K(alloc_limit_));
    } else {
      void *buf = allocator_->alloc(sizeof(T));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "failed to alloc sql stat record value", K(ret));
      } else {
        sqlstat = new (buf) T();
        ATOMIC_INC(&alloc_count_);
      }
    }
    return sqlstat;
  }

  void free_value(T *sqlstat)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(sqlstat)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "sql stat record value is null", K(sqlstat));
    } else {
      sqlstat->~T();
      allocator_->free(sqlstat);
      ATOMIC_DEC(&alloc_count_);
    }
  }

  void free_node(common::LinkHashNode<N> *node)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "sql stat record node is null", K(node));
    } else {
      allocator_->free(node);
    }
  }

  common::LinkHashNode<N> *alloc_node(T *value)
  {
    int ret = OB_SUCCESS;
    common::LinkHashNode<N> *node = nullptr;
    void *buf = allocator_->alloc(sizeof(common::LinkHashNode<N>));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to alloc sql stat record node", K(ret));
    } else {
      node = new (buf) common::LinkHashNode<N>();
      node->hash_val_ = value;
    }
    return node;
  }

private:
  int64_t alloc_count_;
  int64_t alloc_limit_;
  ObIAllocator *allocator_;
};

class ObSqlStatManager {
  typedef ObLinkHashMap<ObSqlStatRecordKey, ObExecutedSqlStatRecord,
      ObSqlStatRecordNodeAlloc<ObExecutedSqlStatRecord, ObSqlStatRecordKey>>
      ObSqlStatInfoHashMap;
  typedef common::ObSEArray<ObExecutedSqlStatRecord *, 16> ObExecutedSqlStatRecordArray;
  typedef ObSqlStatRecordNodeAlloc<ObExecutedSqlStatRecord, ObSqlStatRecordKey> ObSqlStatAlloc;
  friend class ObSqlStatTask;
  friend class ObSqlStatEvictOp;

public:
  constexpr static int64_t MEMORY_LIMIT_DEFAULT = 1;
  constexpr static int64_t MEMORY_EVICT_HIGH_DEFAULT = 80;
  constexpr static int64_t MEMORY_EVICT_LOW_DEFAULT = 30;
  ObSqlStatManager(int64_t tenant_id);
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  static int mtl_new(ObSqlStatManager *&sql_stat_manager);
  static int mtl_init(ObSqlStatManager *&sql_stat_manager);
  static void mtl_destroy(ObSqlStatManager *&sql_stat_manager);
  template <typename Function>
  int foreach_sql_stat_record(Function &fn)
  {
    int ret = OB_SUCCESS;
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "sql stat manager is not inited", K(ret));
    } else if (OB_FAIL(sql_stat_infos_.for_each(fn))) {
      COMMON_LOG(WARN, "failed to foreach sql stat record", K(ret));
    }
    return ret;
  }
  int sum_sql_stat_record(const ObSqlStatRecordKey &key, ObExecutingSqlStatRecord &record,
      const ObSQLSessionInfo &session_info, const ObString &sql, const ObPhysicalPlan *plan);
  int get_or_create_sql_stat_record(
      const ObSqlStatRecordKey &key, ObExecutedSqlStatRecord *&record, bool &is_create, bool for_select = false);
  int get_sql_stat_record(const ObSqlStatRecordKey &key, ObExecutedSqlStatRecord *&record);
  int revert_sql_stat_record(ObExecutedSqlStatRecord *record);
  int create_sql_stat_record(const ObSqlStatRecordKey &key, ObExecutedSqlStatRecord *&record);
  int update_last_snap_record_value();
  int check_if_need_evict();
  int do_evict();
  int64_t calculate_evict_num() const;
  int64_t get_memory_used() const;
  int64_t get_memory_limit() const;
  void set_memory_limit(int64_t memory_limit_percentage, int64_t memory_evict_high_percentage, int64_t memory_evict_low_percentage);
  ObSqlStatInfoHashMap *get_sql_stat_infos() { return &sql_stat_infos_; }

  ~ObSqlStatManager();

private:
  bool is_inited_;
  int64_t tenant_id_;
  int64_t memory_limit_percentage_;
  int64_t memory_evict_high_percentage_;
  int64_t memory_evict_low_percentage_;
  common::ObConcurrentFIFOAllocator allocator_;
  ObSqlStatAlloc alloc_handle_;
  ObSqlStatInfoHashMap sql_stat_infos_;
  ObSqlStatTask stat_task_;
  lib::ObMutex mutex_;
};

class ObUpdateSqlStatOp {
public:
  ObUpdateSqlStatOp()
  {}
  int operator()(ObSqlStatRecordKey &key, ObExecutedSqlStatRecord *sqlstat);
};

inline bool sqlstat_compare(const ObExecutedSqlStatRecord *left, const ObExecutedSqlStatRecord *right)
{
  return left->get_latest_active_time() < right->get_latest_active_time();
}

class ObSqlStatEvictOp {
public:
  ObSqlStatEvictOp(ObSqlStatManager::ObExecutedSqlStatRecordArray *evict_values, int64_t evict_num);
  int operator()(ObSqlStatRecordKey &key, ObExecutedSqlStatRecord *sqlstat);

private:
  ObSqlStatManager::ObExecutedSqlStatRecordArray *evict_values_;
  int64_t evict_num_;
  ObSqlStatManager::ObSqlStatInfoHashMap *hashmap_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_SQL_STAT_MANAGER_H_