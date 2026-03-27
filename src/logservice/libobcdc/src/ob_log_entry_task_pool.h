/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_LIBOBLOG_OB_LOG_ENTRY_TASK_POOL_
#define OCEANBASE_SRC_LIBOBLOG_OB_LOG_ENTRY_TASK_POOL_

#include "lib/allocator/ob_slice_alloc.h"       // ObSliceAlloc
#include "ob_log_part_trans_task.h"             // ObLogEntryTask

namespace oceanbase
{
namespace libobcdc
{
class IObLogEntryTaskPool
{
public:
  virtual ~IObLogEntryTaskPool() {}

public:
  virtual int alloc(
      const bool is_direct_load_inc_log,
      ObLogEntryTask *&task,
      PartTransTask &host) = 0;
  virtual void free(ObLogEntryTask *task) = 0;
  virtual int64_t get_alloc_count() const = 0;
  virtual void print_stat_info() = 0;
  virtual void try_purge_pool() = 0;
};

//////////////////////////////////////////////////////////////////////////////

// ObLogEntryTaskPool
class ObLogEntryTaskPool : public IObLogEntryTaskPool
{
  typedef ObBlockAllocMgr BlockAlloc;
public:
  ObLogEntryTaskPool();
  virtual ~ObLogEntryTaskPool();

public:
  int alloc(
      const bool is_direct_load_inc_log,
      ObLogEntryTask *&log_entry_task,
      PartTransTask &host) override;
  void free(ObLogEntryTask *log_entry_task) override;
  int64_t get_alloc_count() const override;
  void print_stat_info() override;
  void try_purge_pool() override;

public:
  int init(const int64_t fixed_task_count);
  void destroy();

private:
  bool              inited_;
  int64_t           alloc_cnt_;
  BlockAlloc        block_alloc_;
  ObSliceAlloc      allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogEntryTaskPool);
};

} // namespace libobcdc
} // namespace oceanbase
#endif
