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
 * ResourceCollector — Facade over 5 specialized sub-pools
 *
 * Architecture:
 *   ObLogResourceCollector (facade, implements IObLogResourceCollector)
 *     ├─ ObRCBRPool            : ObMQThread     — recycles BinlogRecords
 *     ├─ ObRCLogEntryTaskPool  : ObMQThread     — recycles LogEntryTasks
 *     ├─ ObRCPartTransTaskPool : ObCdcSharedQueueThread<PartTransTask>  — recycles PartTransTasks
 *     ├─ ObRCStoreDeletePool   : ObMQThread     — async RocksDB batch delete
 *     └─ ObRCLobCleanPool      : ObCdcSharedQueueThread<ObCDCLobAuxDataCleanTask> — LOB cleanup
 *
 * Cascade recycling:
 *   BRPool / LogEntryTaskPool ──(dec_ref_cnt=0)──> PartTransTaskPool
 *   PartTransTaskPool ──(push lob clean)──> LobCleanPool
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
#include "ob_cdc_shared_queue_thread.h"               // ObCdcSharedQueueThread
#include "ob_log_resource_recycle_task.h"             // ObLogResourceRecycleTask
#include "ob_log_utils.h"                             // _SEC_
#include "lib/hash/ob_hashmap.h"                      // ObHashMap
#include "lib/container/ob_array.h"                   // ObArray
#include "lib/lock/ob_small_spin_lock.h"              // ObByteLock
#include "lib/lock/ob_spin_rwlock.h"                  // SpinRWLock
#include "lib/allocator/ob_vslice_alloc.h"            // ObVSliceAlloc
#include "lib/allocator/ob_block_alloc_mgr.h"         // ObBlockAllocMgr
#include "ob_log_store_key.h"                        // ObLogStoreKey

namespace oceanbase
{
namespace libobcdc
{

class PartTransTask;
class ObLogEntryTask;
class ObLogBR;
class IObLogErrHandler;
class ObCDCLobAuxDataCleanTask;
class ObLogConfig;

/////////////////////////////////////////////////////////////////////
// Interface
/////////////////////////////////////////////////////////////////////

class IObLogResourceCollector
{
public:
  virtual ~IObLogResourceCollector() {}

public:
  /// Recycle PartTransTask
  /// @note: Require that all Binlog Records in the PartTransTask have been recycled
  //
  // ResourceCollector asynchronous recycle
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

  virtual int revert_dll_all_binlog_records(const bool is_build_baseline, PartTransTask *part_trans_task) = 0;

  // Initialize batch delete buffer for a tenant (called when tenant is created)
  virtual int init_tenant_batch_delete_buffer(const uint64_t tenant_id) = 0;

  // Clean up batch delete buffers for a tenant (called when tenant is dropped)
  virtual void clean_tenant_batch_delete_buffers(const uint64_t tenant_id) = 0;

  // Flush batch delete buffers based on time interval (called by external timer)
  virtual int flush_batch_delete_by_time() = 0;

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
  // Dynamic reconfiguration (thread counts, batch params)
  virtual void configure(const ObLogConfig &config) = 0;
  virtual void get_task_count(
      int64_t &part_trans_task_count,
      int64_t &br_count,
      int64_t &log_entry_task_count,
      int64_t &store_delete_task_count,
      int64_t &queue_task_count) const = 0;
  virtual void print_stat_info() const = 0;
};

/////////////////////////////////////////////////////////////////////
// Sub-pool class declarations
/////////////////////////////////////////////////////////////////////

class IObLogBRPool;
class IObLogTransCtxMgr;
class IObLogMetaManager;
class IObStoreService;

// Forward-declare the facade for back-references
class ObLogResourceCollector;

///
/// BR (BinlogRecord) recycling pool — ObMQThread, per-thread queues, hash routing.
/// Handles HEARTBEAT, BEGIN, COMMIT, DML BinlogRecords.
///
class ObRCBRPool : public common::ObMQThread<64, ObRCBRPool>
{
public:
  ObRCBRPool() : host_(NULL) {}
  void set_host(ObLogResourceCollector *host) { host_ = host; }
  int handle(void *data, const int64_t thread_index, volatile bool &stop_flag) override;
private:
  ObLogResourceCollector *host_;
  DISALLOW_COPY_AND_ASSIGN(ObRCBRPool);
};

///
/// LogEntryTask recycling pool — ObMQThread, per-thread queues, hash routing.
/// Handles empty LogEntryTasks pushed by DmlParser / Formatter.
///
class ObRCLogEntryTaskPool : public common::ObMQThread<64, ObRCLogEntryTaskPool>
{
public:
  ObRCLogEntryTaskPool() : host_(NULL) {}
  void set_host(ObLogResourceCollector *host) { host_ = host; }
  int handle(void *data, const int64_t thread_index, volatile bool &stop_flag) override;
private:
  ObLogResourceCollector *host_;
  DISALLOW_COPY_AND_ASSIGN(ObRCLogEntryTaskPool);
};

///
/// PartTransTask recycling pool — shared-queue, dynamic thread count.
/// Handles TransCtx coordination, participant recycling, cascade to LobClean.
///
class ObRCPartTransTaskPool : public ObCdcSharedQueueThread<PartTransTask>
{
public:
  ObRCPartTransTaskPool() : host_(NULL) {}
  void set_host(ObLogResourceCollector *host) { host_ = host; }
protected:
  int handle(PartTransTask *task, const int64_t thread_index) override;
private:
  ObLogResourceCollector *host_;
  DISALLOW_COPY_AND_ASSIGN(ObRCPartTransTaskPool);
};

///
/// Store (RocksDB) batch delete pool — ObMQThread, per-thread queues.
/// Handles async rocksdb batch delete operations.
///
class ObRCStoreDeletePool : public common::ObMQThread<64, ObRCStoreDeletePool>
{
public:
  ObRCStoreDeletePool() : host_(NULL) {}
  void set_host(ObLogResourceCollector *host) { host_ = host; }
  int handle(void *data, const int64_t thread_index, volatile bool &stop_flag) override;
private:
  ObLogResourceCollector *host_;
  DISALLOW_COPY_AND_ASSIGN(ObRCStoreDeletePool);
};

///
/// LOB auxiliary data cleanup pool — shared-queue, typically 1 thread.
///
class ObRCLobCleanPool : public ObCdcSharedQueueThread<ObCDCLobAuxDataCleanTask>
{
public:
  ObRCLobCleanPool() : host_(NULL) {}
  void set_host(ObLogResourceCollector *host) { host_ = host; }
protected:
  int handle(ObCDCLobAuxDataCleanTask *task, const int64_t thread_index) override;
private:
  ObLogResourceCollector *host_;
  DISALLOW_COPY_AND_ASSIGN(ObRCLobCleanPool);
};

/////////////////////////////////////////////////////////////////////
// ObLogResourceCollector — Facade
/////////////////////////////////////////////////////////////////////

class ObLogResourceCollector : public IObLogResourceCollector
{
  static const int64_t DATA_OP_TIMEOUT = 10L * 1000L * 1000L;
  static const int64_t PRINT_INTERVAL = 5 * _SEC_;

public:
  ObLogResourceCollector();
  virtual ~ObLogResourceCollector();

public:
  int init(const int64_t thread_num_for_br,
      const int64_t thread_num_for_log_entry_task,
      const int64_t thread_num_for_part_trans,
      const int64_t thread_num_for_store_delete,
      const int64_t thread_num_for_lob_clean,
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
  int revert_dll_all_binlog_records(const bool is_build_baseline, PartTransTask *task);
  int init_tenant_batch_delete_buffer(const uint64_t tenant_id) override;
  void clean_tenant_batch_delete_buffers(const uint64_t tenant_id) override;
  int flush_batch_delete_by_time() override;

public:
  int start();
  void stop();
  void mark_stop_flag();
  void configure(const ObLogConfig &config);
  void get_task_count(
      int64_t &part_trans_task_count,
      int64_t &br_count,
      int64_t &log_entry_task_count,
      int64_t &store_delete_task_count,
      int64_t &queue_task_count) const;
  void print_stat_info() const;

private:
  // ---- Nested types (must be declared before use in method signatures) ----

  struct TenantDeleteBuffer {
    common::ObArray<ObLogStoreKey> keys_;
    int64_t last_update_time_us_;
    common::ObByteLock lock_;

    TenantDeleteBuffer() : last_update_time_us_(0) {
      lock_.init();
    }
    void reset() {
      keys_.reset();
      last_update_time_us_ = 0;
    }
    TO_STRING_KV("key_count", keys_.count(), "last_update_time_us", last_update_time_us_);
  };

  class StoreDeleteTask : public ObLogResourceRecycleTask
  {
  public:
    StoreDeleteTask() :
      ObLogResourceRecycleTask(ObLogResourceRecycleTask::STORE_DELETE_TASK),
      tenant_id_(common::OB_INVALID_TENANT_ID)
    {}

    int init(const uint64_t tenant_id, const common::ObArray<ObLogStoreKey> &keys);

    void reset() {
      tenant_id_ = common::OB_INVALID_TENANT_ID;
      keys_.reset();
    }

    TO_STRING_KV(K_(tenant_id), "key_count", keys_.count());

  public:
    uint64_t tenant_id_;
    common::ObArray<ObLogStoreKey> keys_;
  };

  // ---- Handle methods called by sub-pools (friend access) ----
  int handle_br_task_(void *data, const int64_t thread_index);
  int handle_log_entry_task_dispatch_(void *data, const int64_t thread_index);
  int handle_part_trans_task_(PartTransTask *task, const int64_t thread_index);
  int handle_store_delete_task_dispatch_(void *data, const int64_t thread_index);
  int handle_lob_clean_task_(ObCDCLobAuxDataCleanTask *task, const int64_t thread_index);

  // ---- Helper methods (business logic) ----
  int revert_participants_(const int64_t thread_idx, PartTransTask *participants);
  int recycle_part_trans_task_(const int64_t thread_idx, PartTransTask *task);
  int revert_single_binlog_record_(ObLogBR *br);
  int revert_dml_binlog_record_(ObLogBR &br);
  int dec_ref_cnt_and_try_to_revert_task_(PartTransTask *part_trans_task);
  int del_trans_(const uint64_t tenant_id,
      const ObString &trans_id_str);
  int push_lob_data_clean_task_(const uint64_t tenant_id, const int64_t commit_version);
  int dec_ref_cnt_and_try_to_recycle_log_entry_task_(ObLogBR &br);
  int revert_log_entry_task_(ObLogEntryTask *log_entry_task);
  int del_store_service_data_(const ObLogStoreKey &store_key);
  int recycle_stored_redo_(PartTransTask &task);
  int flush_batch_delete_by_time_();
  int revert_unserved_part_trans_task_(const int64_t thread_idx, PartTransTask &task);

  void do_stat_(PartTransTask &task,
      const bool need_accumulate_stat);

  // ---- Sub-pool push helpers ----
  int push_to_br_pool_(ObLogBR *br);
  int push_to_log_entry_task_pool_(ObLogEntryTask *task);
  int push_store_delete_task_(StoreDeleteTask *task);

  // ---- Batch delete buffer helpers ----
  int handle_store_delete_task_(StoreDeleteTask *task);
  int submit_batch_delete_keys_(const uint64_t tenant_id,
      common::ObArray<ObLogStoreKey> &keys, const bool is_size_flush);

  // ---- Inline utility ----
  bool is_stopped_() const { return ATOMIC_LOAD(&stop_flag_); }

private:
  // ---- Sub-pool members ----
  ObRCBRPool             br_thread_pool_;
  ObRCLogEntryTaskPool   log_entry_thread_pool_;
  ObRCPartTransTaskPool  part_trans_thread_pool_;
  ObRCStoreDeletePool    store_delete_thread_pool_;
  ObRCLobCleanPool       lob_clean_thread_pool_;

  // ---- Shared state ----
  bool                inited_;
  volatile bool       stop_flag_;        // Global stop signal, shared with ObCdcSharedQueueThread pools as external_stop_flag
  IObLogBRPool        *br_pool_;
  IObLogTransCtxMgr   *trans_ctx_mgr_;
  IObLogMetaManager   *meta_manager_;
  IObStoreService     *store_service_;
  IObLogErrHandler    *err_handler_;

  // ---- Statistics ----
  int64_t             br_count_;
  int64_t             log_entry_task_count_ CACHE_ALIGNED;
  int64_t             total_part_trans_task_count_ CACHE_ALIGNED;
  int64_t             ddl_part_trans_task_count_ CACHE_ALIGNED;
  int64_t             dml_part_trans_task_count_ CACHE_ALIGNED;
  int64_t             hb_part_trans_task_count_ CACHE_ALIGNED;
  int64_t             other_part_trans_task_count_ CACHE_ALIGNED;

  // Statistics for delete operations
  int64_t             total_delete_keys_ CACHE_ALIGNED;
  int64_t             total_delete_operations_ CACHE_ALIGNED;
  int64_t             total_delete_tasks_ CACHE_ALIGNED;
  int64_t             pending_delete_tasks_ CACHE_ALIGNED;
  int64_t             total_delete_time_us_ CACHE_ALIGNED;

  // Snapshot values for calculating incremental averages
  mutable int64_t     last_stat_delete_keys_ CACHE_ALIGNED;
  mutable int64_t     last_stat_delete_operations_ CACHE_ALIGNED;
  mutable int64_t     last_stat_delete_time_us_ CACHE_ALIGNED;

  // ---- Batch delete buffers ----
  common::hash::ObHashMap<uint64_t, TenantDeleteBuffer *> batch_delete_buffers_;
  mutable common::SpinRWLock batch_delete_buffers_lock_;

  // Allocator for StoreDeleteTask
  typedef common::ObBlockAllocMgr BlockAlloc;
  BlockAlloc store_delete_task_block_alloc_;
  common::ObVSliceAlloc store_delete_task_allocator_;

  // Configuration parameters
  int64_t batch_delete_size_;
  int64_t flush_interval_ms_;

  // ---- Friend declarations for sub-pools to access private handle methods and err_handler_ ----
  friend class ObRCBRPool;
  friend class ObRCLogEntryTaskPool;
  friend class ObRCPartTransTaskPool;
  friend class ObRCStoreDeletePool;
  friend class ObRCLobCleanPool;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogResourceCollector);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
