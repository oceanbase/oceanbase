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

#ifndef __SQL_ENG_PX_ADMISSION_H__
#define __SQL_ENG_PX_ADMISSION_H__

#include "share/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/rc/ob_rc.h"

namespace oceanbase {
namespace sql {

class ObSqlCtx;
class ObSQLSessionInfo;
class ObPhysicalPlan;
class ObExecContext;

class ObPxAdmission {
public:
  ObPxAdmission() = default;
  ~ObPxAdmission() = default;
  static bool admit(int64_t cnt, int64_t& admit_cnt);
  static void release(int64_t cnt);
  static int enter_query_admission(
      sql::ObSQLSessionInfo& session, sql::ObExecContext& exec_ctx, sql::ObPhysicalPlan& plan, int64_t& worker_count);
  static void exit_query_admission(int64_t worker_count);

private:
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObPxAdmission);
};

class ObPxPoolStat {
public:
  static int mtl_init(ObPxPoolStat*& pool)
  {
    int ret = common::OB_SUCCESS;
    pool = OB_NEW(ObPxPoolStat, common::ObModIds::OMT_TENANT);
    if (OB_ISNULL(pool)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else {
      pool->pool_size_ = 0;
      pool->max_parallel_servers_used_ = 0;
      pool->target_ = 0;
      pool->parallel_servers_target_used_ = 0;
      SQL_LOG(INFO, "ObPxPoolStat for tenant init success", "tenant_id", oceanbase::lib::current_tenant_id());
    }
    return ret;
  }
  static void mtl_destroy(ObPxPoolStat*& pool)
  {
    common::ob_delete(pool);
    pool = nullptr;
  }

public:
  // PARALLEL_SERVERS_TARGET
  inline int64_t get_target() const
  {
    return target_;
  }
  inline int64_t get_parallel_servers_target_used() const
  {
    return parallel_servers_target_used_;
  }
  void set_target(int64_t size);
  void acquire_parallel_servers_target(int64_t max, int64_t& acquired_cnt);
  void release_parallel_servers_target(int64_t acquired_cnt);

  // MAX_PARALLEL_SERVERS
  inline int64_t get_pool_size() const
  {
    return pool_size_;
  }
  inline int64_t get_max_parallel_servers_used() const
  {
    return max_parallel_servers_used_;
  }
  void set_pool_size(int64_t size);
  void acquire_max_parallel_servers(int64_t max, int64_t min, int64_t& acquired_cnt);
  void release_max_parallel_servers(int64_t acquired_cnt);

  TO_STRING_KV(K_(target), K_(parallel_servers_target_used), K_(pool_size), K_(max_parallel_servers_used));

private:
  // PARALLEL_SERVERS_TARGET
  int64_t target_;
  int64_t parallel_servers_target_used_;  // The number of threads currently occupied by the worker
  // MAX_PARALLEL_SERVERS
  int64_t pool_size_;
  int64_t max_parallel_servers_used_;

  common::SpinRWLock max_parallel_servers_lock_;
  common::SpinRWLock parallel_servers_target_lock_;
};

class ObPxSubAdmission {
public:
  ObPxSubAdmission() = default;
  ~ObPxSubAdmission() = default;
  static void acquire(int64_t max, int64_t min, int64_t& acquired_cnt);
  static void release(int64_t acquired_cnt);

private:
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObPxSubAdmission);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* __SQL_ENG_PX_ADMISSION_H__ */
//// end of header file
