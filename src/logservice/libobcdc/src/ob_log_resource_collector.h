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
 *
 * ResourceCollector
 * Uniform recovery of resources such as Binlog Record, PartTransTask and TransCtx
 *
 * I. The DDL transaction recycling strategy is as follows.
 *   1. mark a Binlog Record as recyclable
 *   2. When all Binlog Records in a PartTransTask are marked recyclable, the PartTransTask is marked recyclable
 *  3. When all PartTransTasks in a TransCtx are marked recyclable, then the TransCtx is actually recycled
 *  4. when the TransCtx is actually recycled, start actually recycling all the PartTransTasks in the TransCtx
 *  5. When the PartTransTask is actually reclaimed, start actually reclaiming the Binlog Record
 *
 * II. The DML transaction recycling strategy is as follows.
 *    1. When a Binlog Record is received, it is actually recycled asynchronously, and the reference count of the PartTransTask is decremented
 *   2. When all Binlog Records in a PartTransTask are marked as recyclable, the PartTransTask is marked as recyclable
 *   3. When all the PartTransTasks in a TransCtx are marked recyclable, the TransCtx is actually recycled
 *   4. When the TransCtx is actually recycled, start actually recycling all the PartTransTasks in the TransCtx
 *
 * III. HEARTBEAT/BEGIN/COMMIT direct asynchronous recycling
 */

#ifndef OCEANBASE_LIBOBCDC_RESOURCE_COLLECTOR_H__
#define OCEANBASE_LIBOBCDC_RESOURCE_COLLECTOR_H__

#include "lib/thread/ob_multi_fixed_queue_thread.h"   // ObMQThread
#include "ob_log_resource_recycle_task.h"             // ObLogResourceRecycleTask
#include "ob_log_utils.h"                             // _SEC_

namespace oceanbase
{
namespace libobcdc
{

class PartTransTask;
class ObLogEntryTask;
class ObLogBR;
class IObLogErrHandler;

class IObLogResourceCollector
{
public:
  enum { MAX_THREAD_NUM = 64 };

public:
  virtual ~IObLogResourceCollector() {}

public:
  /// Recycle PartTransTask
  /// @note: Require that all Binlog Records in the PartTransTask have been recycled
  //
  // ResourceCollector asynchronous recycle
  // virtual int revert(PartTransTask *task) = 0;
  // 1. Delete log in PartTransTask
  // 2. Recycler PartTransTask
  virtual int revert(PartTransTask *task) = 0;

  // recycle Binlog Record
  virtual int revert(const int record_type, ObLogBR *br) = 0;

  // called by DmlParser and Formatter(the log_entry_task doesn't have any valid stmt or binlog record)
  //
  // @param  log_entry_task  log_entry_task to recycle
  // @retval OB_SUCCESS      success to recycle the task
  // @retval other_err_code  Fail to recycle the task
  virtual int revert_log_entry_task(ObLogEntryTask *log_entry_task) = 0;

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
  virtual void get_task_count(int64_t &part_trans_task_count, int64_t &br_count) const = 0;
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
      IObStoreService *store_service,
      IObLogErrHandler *err_handler);
  void destroy();

public:
  int revert(PartTransTask *task);
  int revert(const int record_type, ObLogBR *br);
  int revert_log_entry_task(ObLogEntryTask *log_entry_task);

public:
  int start();
  void stop();
  void mark_stop_flag();
  int handle(void *data, const int64_t thread_index, volatile bool &stop_flag);
  void get_task_count(int64_t &part_trans_task_count, int64_t &br_count) const;
  void print_stat_info() const;

private:
  int push_task_into_queue_(ObLogResourceRecycleTask &task);
  int revert_participants_(const int64_t thread_idx, PartTransTask *participants);
  // Reclaiming resources for partitioned tasks
  // thread_idx is used for log debug info
  int recycle_part_trans_task_(const int64_t thread_idx, PartTransTask *task);
  int revert_dll_all_binlog_records_(PartTransTask *task);
  int revert_single_binlog_record_(ObLogBR *br);
  int revert_dml_binlog_record_(ObLogBR &br, volatile bool &stop_flag);
  int dec_ref_cnt_and_try_to_revert_task_(PartTransTask *part_trans_task);
  int del_trans_(const uint64_t tenant_id,
      const ObString &trans_id_str);
  int push_lob_data_clean_task_(const uint64_t tenant_id, const int64_t commit_version);
  int dec_ref_cnt_and_try_to_recycle_log_entry_task_(ObLogBR &br);
  int revert_log_entry_task_(ObLogEntryTask *log_entry_task);
  int del_store_service_data_(const uint64_t tenant_id,
      std::string &key);
  int revert_unserved_part_trans_task_(const int64_t thread_idx, PartTransTask &task);

  void do_stat_(PartTransTask &task,
      const bool need_accumulate_stat);

private:
  bool                inited_;
  IObLogBRPool        *br_pool_;
  IObLogTransCtxMgr   *trans_ctx_mgr_;
  IObLogMetaManager   *meta_manager_;
  IObStoreService     *store_service_;
  IObLogErrHandler    *err_handler_;
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

} // namespace libobcdc
} // namespace oceanbase

#endif
