/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_TABLE_OB_TENANT_VECTOR_INDEX_ASYNC_TASK_H_
#define OCEANBASE_OBSERVER_TABLE_OB_TENANT_VECTOR_INDEX_ASYNC_TASK_H_

#include "share/vector_index/ob_plugin_vector_index_util.h"

namespace oceanbase
{
namespace share
{
class ObRootService;

typedef common::hash::ObHashSet<common::ObAddr> ServerSet;

/**
 * the task for clear vec history task in __all_vector_index_task_history
*/
class ObVectorIndexHistoryTask : public common::ObTimerTask
{
public:
  ObVectorIndexHistoryTask()
  : sql_proxy_(nullptr),
    is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    is_paused_(false)
  {}
  ~ObVectorIndexHistoryTask() {}
  int init(const uint64_t tenant_id, common::ObMySQLProxy &sql_proxy);
  virtual void runTimerTask() override;
  void destroy() {}
  void pause();
  void resume();
  void do_work();

  static const int64_t OB_VEC_INDEX_TASK_HISTORY_SAVE_TIME_US = 7 * 24 * 60 * 60 * 1000 * 1000ll; // 7 day
  static const int64_t OB_VEC_INDEX_TASK_MOVE_BATCH_SIZE = 1024L;
  static const int64_t OB_VEC_INDEX_TASK_DEL_COUNT_PER_TASK = 4096L;

private:
  int clear_history_task();
  int move_task_to_history_table();

private:
  common::ObMySQLProxy *sql_proxy_;
  bool is_inited_;
  uint64_t tenant_id_;
  bool is_paused_;
};

class ObTenantVecAsyncTaskScheduler
{
public:
  static const int64_t VEC_INDEX_OPT_TASK_PERIOD = 10 * 1000L * 1000L; // 10s 
  static const int64_t VEC_INDEX_CLEAR_TASK_PERIOD = 10 * 1000L * 1000L; // 60s
  explicit ObTenantVecAsyncTaskScheduler() 
    : is_inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      tg_id_(-1),
      vec_history_task_()
  {}

  virtual ~ObTenantVecAsyncTaskScheduler() {}
  int init(const uint64_t tenant_id, ObMySQLProxy &sql_proxy);
  int start();
  void wait();
  void stop();
  void destroy();
  void resume();
  void pause();
private:
  bool is_inited_;
  uint64_t tenant_id_;
  int tg_id_;
  ObVectorIndexHistoryTask vec_history_task_;
};

} // end namespace share
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_TABLE_OB_TENANT_VECTOR_INDEX_ASYNC_TASK_H_ */