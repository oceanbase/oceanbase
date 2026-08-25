/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OB_LAKE_TABLE_EXECUTOR_H
#define OB_LAKE_TABLE_EXECUTOR_H

#include "lib/lock/ob_thread_cond.h"
#include "lib/thread/thread_mgr_interface.h"

#include <functional>

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace sql
{
namespace lake_table
{

class ObLakeTableTaskGroup final
{
public:
  ObLakeTableTaskGroup();
  ~ObLakeTableTaskGroup() = default;

  int init(const int64_t task_cnt);
  void finish(int task_ret);
  int wait();

private:
  bool is_inited_;
  int64_t pending_cnt_;
  int ret_code_;
  common::ObThreadCond cond_;
};

using ObLakeTableTaskGroupHandle = ObLakeTableTaskGroup *;


/// Tenant-local QUEUE_THREAD executor that backs parallel work for all lake
/// table formats. Format-specific adapters wrap this
/// executor so the common layer carries no format-specific dependencies.
///
/// Lifecycle is driven by MTL (init/start/stop/wait/destroy).
class ObLakeTableExecutor final : public lib::TGTaskHandler
{
public:
  ObLakeTableExecutor();
  ~ObLakeTableExecutor();

  static int mtl_init(ObLakeTableExecutor *&exec);

  int init();
  int start();
  void stop();
  void wait();
  void destroy();

  int64_t get_queue_num() const;
  int get_thread_cnt() const;
  static double get_tenant_cpu_count();
  int reload_config();
  int set_adaptive_thread(int64_t min_thread_cnt, int64_t max_thread_cnt);
  int adjust_thread_count();

  void Add(std::function<void()> func);
  int Add(ObLakeTableTaskGroupHandle task_group, std::function<int()> func);

  int create_task_group(common::ObIAllocator &allocator,
                        const int64_t task_cnt,
                        ObLakeTableTaskGroupHandle &task_group);
  void destroy_task_group(common::ObIAllocator &allocator,
                          ObLakeTableTaskGroupHandle &task_group);

  // lib::TGTaskHandler
  void handle(void *task) override;

private:
  int get_config_thread_cnt_(int64_t &thread_cnt) const;
  int apply_thread_cnt_(const int64_t thread_cnt);

  static constexpr int64_t MIN_WORKER_THREAD_NUM = 4L;
  static constexpr int64_t MAX_WORKER_THREAD_NUM = 128L;

  bool is_inited_;
  int tg_id_;
  uint64_t tenant_id_;
};

} // namespace lake_table
} // namespace sql
} // namespace oceanbase

#endif
