/**
 * Copyright (c) 2021 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_DDL_TRANS_CONTROLLER_H
#define OCEANBASE_DDL_TRANS_CONTROLLER_H

#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/task/ob_timer.h"
#include "lib/hash/ob_hashset.h"
#include "common/ob_queue_thread.h"

namespace oceanbase
{

namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;

struct TaskDesc
{
  uint64_t tenant_id_;
  int64_t task_id_;
  bool task_end_;
  TO_STRING_KV(K_(tenant_id), K_(task_id), K_(task_end));
};

// impl for ddl schema change trans commit in order with schema_version
class ObDDLTransController : public lib::ThreadPool
{
public:
  ObDDLTransController() : inited_(false), schema_service_(NULL) {}
  ~ObDDLTransController();
  int init(share::schema::ObMultiVersionSchemaService *schema_service);
  void stop();
  void wait();
  void destroy();
  static const int DDL_TASK_COND_SLOT = 128;
  int create_task_and_assign_schema_version(
      const uint64_t tenant_id,
      const uint64_t schema_version_count,
      int64_t &task_id,
      ObIArray<int64_t> &schema_version_res);
  int wait_task_ready(const uint64_t tenant_id, const int64_t task_id, const int64_t wait_us);
  int remove_task(const uint64_t tenant_id, const int64_t task_id);
  int check_enable_ddl_trans_new_lock(const uint64_t tenant_id, bool &res);
  int set_enable_ddl_trans_new_lock(const uint64_t tenant_id);
  int broadcast_consensus_version(const int64_t tenant_id,
                                  const int64_t schema_version,
                                  const ObArray<ObAddr> &server_list);
private:
  virtual void run1() override;
  int check_task_ready_(const uint64_t tenant_id, const int64_t task_id, bool &ready);
private:
  bool inited_;
  common::ObThreadCond cond_slot_[DDL_TASK_COND_SLOT];
  ObSEArray<TaskDesc, 32> tasks_;
  common::SpinRWLock lock_;
  share::schema::ObMultiVersionSchemaService *schema_service_;

  common::hash::ObHashSet<uint64_t> tenants_;

  // for compat
  common::SpinRWLock lock_for_tenant_set_;
  common::hash::ObHashSet<uint64_t> tenant_for_ddl_trans_new_lock_;

  common::ObCond wait_cond_;
};

} // end schema
} // end share
} // end oceanbase

#endif
