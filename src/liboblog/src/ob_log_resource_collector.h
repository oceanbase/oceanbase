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

#ifndef OCEANBASE_LIBOBLOG_RESOURCE_COLLECTOR_H__
#define OCEANBASE_LIBOBLOG_RESOURCE_COLLECTOR_H__

#include "lib/thread/ob_multi_fixed_queue_thread.h"   // ObMQThread
#include "ob_log_resource_recycle_task.h"             // ObLogResourceRecycleTask
#include "ob_log_utils.h"                             // _SEC_

namespace oceanbase
{
namespace liboblog
{

class PartTransTask;
class ObLogEntryTask;
class ObLogBR;
class ObLogRowDataIndex;

class IObLogResourceCollector
{
public:
  enum { MAX_THREAD_NUM = 64 };

public:
  virtual ~IObLogResourceCollector() {}

public:
  /// Recycle PartTransTask
  /// @note: Require that all Binlog Records in the PartTransTask have been recycled
  virtual int revert(PartTransTask *task) = 0;

  // recycle Binlog Record
  virtual int revert(const int record_type, ObLogBR *br) = 0;

  virtual int revert_unserved_task(const bool is_rollback_row,
      ObLogRowDataIndex &row_data_index) = 0;

  virtual int revert_log_entry_task(ObLogEntryTask *log_entry_task) = 0;

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
  virtual int64_t get_part_trans_task_count() const = 0;
  virtual void print_stat_info() const = 0;
};

////////////////////////////////////////////////////////////

typedef common::ObMQThread<IObLogResourceCollector::MAX_THREAD_NUM, IObLogResourceCollector> RCThread;

class IObLogBRPool;
class IObLogTransCtxMgr;
class IObLogMetaManager;
class IObStoreService;

class ObLogResourceCollector : public IObLogResourceCollector, public RCThread
{
  static const int64_t DATA_OP_TIMEOUT = 10L * 1000L * 1000L;
  static const int64_t PRINT_INTERVAL = 5 * _SEC_;

public:
  ObLogResourceCollector();
  virtual ~ObLogResourceCollector();

public:
  int init(const int64_t thread_num,
      const int64_t thread_num_for_br,
      const int64_t queue_size,
      IObLogBRPool *br_pool,
      IObLogTransCtxMgr *trans_ctx_mgr,
      IObLogMetaManager *meta_manager,
      IObStoreService *store_service);
  void destroy();

public:
  int revert(PartTransTask *task);
  int revert(const int record_type, ObLogBR *br);
  int revert_unserved_task(const bool is_rollback_row,
      ObLogRowDataIndex &row_data_index);
  int revert_log_entry_task(ObLogEntryTask *log_entry_task);

public:
  int start();
  void stop();
  void mark_stop_flag();
  int handle(void *data, const int64_t thread_index, volatile bool &stop_flag);
  int64_t get_part_trans_task_count() const;
  void print_stat_info() const;

private:
  int push_task_into_queue_(ObLogResourceRecycleTask &task);
  int revert_participants_(const int64_t thread_idx, PartTransTask *participants);
  // Reclaiming resources for partitioned tasks
  int recycle_part_trans_task_(const int64_t thread_idx, PartTransTask *task);
  int revert_dll_all_binlog_records_(PartTransTask *task);
  int revert_single_binlog_record_(ObLogBR *br);
  int revert_dml_binlog_record_(ObLogBR &br);
  int dec_ref_cnt_and_try_to_revert_task_(PartTransTask *part_trans_task);
  int del_store_service_data_(ObLogRowDataIndex &row_data_index);
  int dec_ref_cnt_and_try_to_recycle_log_entry_task_(ObLogBR &br);
  int revert_log_entry_task_(ObLogEntryTask *log_entry_task);

  void do_stat_(PartTransTask &task,
      const bool need_accumulate_stat);

private:
  bool                inited_;
  IObLogBRPool        *br_pool_;
  IObLogTransCtxMgr   *trans_ctx_mgr_;
  IObLogMetaManager   *meta_manager_;
  IObStoreService     *store_service_;
  // BinlogRecord and PartTransTask need handle separately in order to avoid maybe deadlock
  int64_t             br_thread_num_;
  // Count the number of binlog record
  int64_t             br_count_;

  // Count the number of partition transaction tasks
  int64_t             total_part_trans_task_count_ CACHE_ALIGNED;
  int64_t             ddl_part_trans_task_count_ CACHE_ALIGNED;
  int64_t             dml_part_trans_task_count_ CACHE_ALIGNED;
  int64_t             hb_part_trans_task_count_ CACHE_ALIGNED;
  int64_t             other_part_trans_task_count_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogResourceCollector);
};

} // namespace liboblog
} // namespace oceanbase

#endif
