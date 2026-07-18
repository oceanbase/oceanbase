/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_MIGRATION_VECTOR_INDEX_PROCESSOR_H_
#define OCEANBASE_STORAGE_MIGRATION_VECTOR_INDEX_PROCESSOR_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "ob_migration_sliding_window_controller.h"
#include "ob_storage_ha_dag.h"
#include "storage/ob_storage_rpc.h"

namespace oceanbase
{
namespace storage
{

class ObLS;
class ObMigrationVectorIndexProcessor;
class ObVectorIndexMigrationProcessorMgr;

// RAII holder; last release destroys processor when refcount hits 0.
class ObVectorIndexMigrationProcessorGuard
{
public:
  ObVectorIndexMigrationProcessorGuard() : proc_(nullptr), mgr_(nullptr) {}
  ObVectorIndexMigrationProcessorGuard(ObVectorIndexMigrationProcessorMgr *mgr, ObMigrationVectorIndexProcessor *proc);
  ObVectorIndexMigrationProcessorGuard(const ObVectorIndexMigrationProcessorGuard &other);
  ObVectorIndexMigrationProcessorGuard &operator=(const ObVectorIndexMigrationProcessorGuard &other);
  ~ObVectorIndexMigrationProcessorGuard() { reset(); }

  void reset();
  bool is_valid() const { return proc_ != nullptr; }
  int set_processor(ObVectorIndexMigrationProcessorMgr *mgr, ObMigrationVectorIndexProcessor *proc);
  ObMigrationVectorIndexProcessor *get() const { return proc_; }
  ObMigrationVectorIndexProcessor *operator->() const { return proc_; }
  ObMigrationVectorIndexProcessor &operator*() const { return *proc_; }

private:
  friend class ObVectorIndexMigrationProcessorMgr;
  ObMigrationVectorIndexProcessor *proc_;
  ObVectorIndexMigrationProcessorMgr *mgr_;
};

class ObVectorIndexMigrationUtil
{
public:
  // Locate a segment by segment_idx in snap_data (bases then incrs).
  static int resolve_vec_idx_segment_for_migration(
      share::ObPluginVectorIndexAdaptor *adaptor,
      const int64_t segment_idx,
      share::ObVectorIndexSegmentHandle &out_handle);

private:
  DISALLOW_COPY_AND_ASSIGN(ObVectorIndexMigrationUtil);
};

// ======================== Serialize DAG / Task ========================
// Runs Processor::do_serialize() in DAG scheduler worker threads.
class ObVectorIndexSerializeDag : public share::ObIDag
{
public:
  ObVectorIndexSerializeDag();
  virtual ~ObVectorIndexSerializeDag();
  int init(const ObVectorIndexMigrationProcessorGuard &processor_guard);
  virtual int create_first_task() override;

  virtual bool operator==(const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                              common::ObIAllocator &allocator) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }

  ObMigrationVectorIndexProcessor *get_processor() const { return processor_guard_.get(); }

private:
  bool is_inited_;
  ObVectorIndexMigrationProcessorGuard processor_guard_;
  DISALLOW_COPY_AND_ASSIGN(ObVectorIndexSerializeDag);
};

struct VecMigSerializeCbParam : public share::ObHNSWSerializeCallback::CbParam
{
  VecMigSerializeCbParam()
    : ctrl_(nullptr), timeout_us_(0), slot_cap_(0), slot_chunk_(nullptr),
      processor_id_(common::OB_INVALID_ID), header_tenant_id_(common::OB_INVALID_TENANT_ID),
      ls_id_()
  {}
  virtual CbParamType get_type() const override { return CbParamType::VEC_MIG_SERIALIZE; }
  bool is_valid() const;

  ObMigrationSlidingWindowSourceController *ctrl_;
  int64_t timeout_us_;
  int64_t slot_cap_;
  char *slot_chunk_;
  // Per-chunk header fields.
  int64_t processor_id_;
  uint64_t header_tenant_id_;
  share::ObLSID ls_id_;
};

class ObVectorIndexSerializeTask : public share::ObITask
{
public:
  ObVectorIndexSerializeTask();
  virtual ~ObVectorIndexSerializeTask();
  int init();
  virtual int process() override;

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObVectorIndexSerializeTask);
};

// ======================== Processor ========================
// One Processor per segment serialization task on the source side.
// Responsibilities:
//   1. Hold source adaptor alive via AdapterGuard
//   2. Provide serialization interface for DAG worker threads
//   3. Maintain source-side sliding window controller
//   4. Serve dest-side Get RPC via the controller
class ObMigrationVectorIndexProcessor final
{
  friend class ObVectorIndexMigrationProcessorMgr;
public:
  enum class State : int64_t
  {
    INIT = 0,
    SERIALIZING = 1,     // serialization in progress
    MAX_STATE,
  };

  ObMigrationVectorIndexProcessor();
  ~ObMigrationVectorIndexProcessor();

  int init(const int64_t processor_id,
           const share::ObLSID &ls_id,
           const common::ObTabletID &data_tablet_id,
           const int64_t segment_idx,
           const common::ObAddr &dest_addr,
           const uint64_t tenant_id,
           const int64_t adaptor_handle_id);
  void destroy();
  bool is_inited() const { return is_inited_; }
  int set_adaptor_from_guard(share::ObPluginVectorIndexAdapterGuard &guard);

  int do_serialize();
  int try_get_data(const int64_t seq_idx, char *out_buf, const int64_t out_buf_len, int64_t &data_len);

  int64_t get_processor_id() const { return processor_id_; }
  const share::ObLSID &get_ls_id() const { return ls_id_; }
  const common::ObTabletID &get_data_tablet_id() const { return data_tablet_id_; }
  int64_t get_segment_idx() const { return segment_idx_; }
  const common::ObAddr &get_dest_addr() const { return dest_addr_; }
  int64_t get_adaptor_handle_id() const { return adaptor_handle_id_; }

  State get_state() const
  {
    common::ObSpinLockGuard g(state_lock_);
    return state_;
  }
  bool is_failed() const { return result_mgr_.is_failed(); }
  int get_fail_ret_code() const;
  int64_t get_last_access_time() const
  {
    common::ObSpinLockGuard g(state_lock_);
    return last_access_time_;
  }
  bool is_timeout(const int64_t timeout_us) const;

  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(processor_id), K_(ls_id),
               K_(data_tablet_id), K_(segment_idx), K_(dest_addr),
               K_(adaptor_handle_id), K_(dag_prio), "state",
               static_cast<int64_t>(get_state()), "last_access_time",
               get_last_access_time(), K_(result_mgr), K_(ref_count));

private:
  void set_state(const State new_state);
  void set_failed(const int ret_code);
  void update_last_access_time();
  void stop();

  bool is_inited_;
  int64_t processor_id_;
  share::ObLSID ls_id_;
  common::ObTabletID data_tablet_id_;
  int64_t segment_idx_;
  common::ObAddr dest_addr_;
  uint64_t tenant_id_;
  // Failure-batch key: processors under one adaptor-dag attempt share this id, so a single notify can flip them all to FAILED.
  int64_t adaptor_handle_id_;
  share::ObDagPrio::ObDagPrioEnum dag_prio_;

  mutable common::ObSpinLock state_lock_;
  State state_;
  int64_t last_access_time_;
  mutable ObStorageHAResultMgr result_mgr_;
  // ref_count_: reachability for processor lifetime (ob_delete at 0).
  int64_t ref_count_;

  share::ObPluginVectorIndexAdapterGuard adaptor_guard_;
  ObMigrationSlidingWindowSourceHandle src_controller_handle_;

  DISALLOW_COPY_AND_ASSIGN(ObMigrationVectorIndexProcessor);
};

// ======================== Processor Manager ========================
// Source-side manager for all active Processors within one LS.
// Handles registration, routing, timeout cleanup, and failure propagation.

class ObVectorIndexMigrationProcessorMgr final
{
public:
  struct VecMigAdaptorHoldEntry
  {
    share::ObPluginVectorIndexAdapterGuard *guard_;
    int64_t hold_ts_;
    VecMigAdaptorHoldEntry() : guard_(nullptr), hold_ts_(0) {}
  };

  ObVectorIndexMigrationProcessorMgr();
  ~ObVectorIndexMigrationProcessorMgr();

  int init(ObLS *ls);
  void destroy();
  bool is_inited() const { return is_inited_; }

  int register_processor(const common::ObTabletID &data_tablet_id,
                         const int64_t segment_idx,
                         const common::ObAddr &dest_addr,
                         const int64_t adaptor_handle_id,
                         int64_t &processor_id);
  int setup_and_start_processor(const int64_t processor_id,
                                share::ObPluginVectorIndexAdapterGuard &adaptor_guard,
                                const int64_t segment_idx);
  // Non-blocking variant; returns OB_EAGAIN when data not ready.
  int try_fetch_segment_data(const int64_t processor_id,
                             const int64_t seq_idx,
                             char *out_buf,
                             const int64_t out_buf_len,
                             int64_t &data_len);
  int release_processor(const int64_t processor_id);

  int mark_processor_failed(const int64_t processor_id, const int ret_code);

  int cleanup_timeout_processors(const int64_t timeout_us);

  int cleanup_timeout_adaptor_handles(const int64_t timeout_us);

  int hold_adaptor(share::ObPluginVectorIndexAdapterGuard &guard,
                   int64_t &adaptor_handle_id);
  int get_held_adaptor(const int64_t adaptor_handle_id,
                       share::ObPluginVectorIndexAdapterGuard &out_guard);
  int release_adaptor(const int64_t adaptor_handle_id);

  void refresh_adaptor_hold_ts(const int64_t adaptor_handle_id);

  int64_t get_processor_count() const;

  int64_t get_adaptor_handle_count() const;

  static const int64_t PROCESSOR_AND_ADAPTOR_TIMEOUT_US = 30L * 60L * 1000L * 1000L; // 30 min
  static const int64_t FIRST_VALID_PROCESSOR_ID = 1;
  static const int64_t FIRST_VALID_ADAPTOR_HANDLE_ID = 1;

  static bool is_valid_processor_id(const int64_t processor_id);
  static bool is_valid_adaptor_handle_id(const int64_t adaptor_handle_id);

  TO_STRING_KV(K_(is_inited), K_(next_processor_id), K_(next_adaptor_handle_id));

private:
  friend class ObVectorIndexMigrationProcessorGuard;            // calls inc_ref_ / dec_ref_

  int acquire_guard_(const int64_t processor_id, ObVectorIndexMigrationProcessorGuard &out_guard);

  // dec_ref_ at zero frees the processor.
  static void inc_ref_(ObMigrationVectorIndexProcessor *processor);
  static void dec_ref_(ObMigrationVectorIndexProcessor *processor);

  bool is_inited_;
  ObLS *ls_;
  int64_t next_processor_id_;
  int64_t next_adaptor_handle_id_;
  mutable common::SpinRWLock lock_;          // protects processor_map_
  mutable common::SpinRWLock adaptor_lock_;  // protects adaptor_guard_map_

  static const int64_t PROCESSOR_MAP_BUCKET_NUM = 64;
  common::hash::ObHashMap<int64_t, ObMigrationVectorIndexProcessor *> processor_map_;
  common::hash::ObHashMap<int64_t, VecMigAdaptorHoldEntry> adaptor_guard_map_;

  DISALLOW_COPY_AND_ASSIGN(ObVectorIndexMigrationProcessorMgr);
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_MIGRATION_VECTOR_INDEX_PROCESSOR_H_
