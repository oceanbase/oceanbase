/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_MIGRATION_VECTOR_INDEX_H_
#define OCEANBASE_STORAGE_MIGRATION_VECTOR_INDEX_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/ob_ls_id.h"
#include "share/vector_index/ob_plugin_vector_index_serialize.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/container/ob_se_array.h"
#include "ob_ls_complete_migration.h"
#include "ob_migration_sliding_window_controller.h"

namespace oceanbase
{
namespace obrpc
{
class ObStorageRpcProxy;
}
namespace storage
{

struct ObVecIdxMigChunkHeader;

// ======================== ObVecIdxFetchRpcCtx ========================
// Immutable RPC targeting parameters for vector-index segment fetch RPCs.
// Pure value type with no ownership semantics.
struct ObVecIdxFetchRpcCtx
{
  ObVecIdxFetchRpcCtx();
  void reset();
  bool is_valid() const;

  obrpc::ObStorageRpcProxy *rpc_proxy_;
  common::ObAddr src_addr_;
  int64_t cluster_id_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int64_t processor_id_;
  int64_t rpc_timeout_us_;
  int64_t ls_rebuild_seq_;

  TO_STRING_KV(KP_(rpc_proxy), K_(src_addr), K_(cluster_id), K_(tenant_id),
               K_(ls_id), K_(processor_id), K_(rpc_timeout_us),
               K_(ls_rebuild_seq));
};

class ObVectorIndexAdaptorMigrationDag;
class ObVecIdxSegFetchDriver;

// Refcounted handle to ObVecIdxSegFetchDriver.
typedef ObVectorIndexMigrationHandleT<ObVecIdxSegFetchDriver> ObVecIdxSegFetchDriverHandle;

// ======================== ObVecIdxSegFetchDriver ========================
// Per-processor async segment fetch: dest_controller_handle_/rpc_ctx_, on_window_slid sends,
// ObFetchVecIdxSegDataCB calls here; stop() halts new work, last handle ref cleans driver.
class ObVecIdxSegFetchDriver final : public ObIMigrationWindowSlidCallback
{
public:
  ObVecIdxSegFetchDriver();

  static int create(const ObVecIdxFetchRpcCtx &rpc_ctx,
                    ObMigrationTenantWindowMgr *window_mgr,
                    const share::ObDagPrio::ObDagPrioEnum dag_prio,
                    ObVecIdxSegFetchDriverHandle &out_handle);
  int init(const ObVecIdxFetchRpcCtx &rpc_ctx,
           ObMigrationTenantWindowMgr *window_mgr,
           const share::ObDagPrio::ObDagPrioEnum dag_prio);

  void stop();

  // Send the first wave of RPCs for the initial window.
  int start_initial_window();

  // Called from async rpc callbacks, fill received data into the dest controller.
  int fill_data(const int64_t seq_idx, const char *data, const int64_t data_len);
  void notify_src_exhausted(const int64_t seq_idx);
  void abort_segment_fetch(const int failure_ret);
  int enqueue_retry(const int64_t seq_idx);

  int64_t get_slot_buf_size();
  // Block until data for the next slot arrives (or timeout / abort).
  int wait_and_get_data(char *out_buf, const int64_t out_buf_len, int64_t &data_len);

  // ObIMigrationWindowSlidCallback override. When the consumer slides the
  // window head forward, dispatch RPCs for the newly granted seq range.
  void on_window_slid(const int64_t granted_start_seq,
                      const int64_t granted_slot_count) override;

  const ObMigrationSlidingWindowDestHandle &get_dest_handle() const { return dest_controller_handle_; }
  const ObVecIdxFetchRpcCtx &get_rpc_ctx() const { return rpc_ctx_; }

  int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }



private:
  template <typename T> friend class ObVectorIndexMigrationHandleT;
  int send_range_(const int64_t start_seq, const int64_t count);
  int send_retry_(const int64_t seq_idx);
  int send_one_(const int64_t seq_idx);
  int do_retry_();
  int64_t inc_ref_() { return ATOMIC_AAF(&ref_count_, 1); }
  int64_t dec_ref_();

  static constexpr uint64_t CONSUME_POLL_INTERVAL_US = 1000;  // 1ms, per-slice timeout for do_retry_ driving
  bool is_inited_;
  ObVecIdxFetchRpcCtx rpc_ctx_;
  ObMigrationSlidingWindowDestHandle dest_controller_handle_;

  mutable common::ObSpinLock retry_lock_;
  common::ObArray<int64_t> pending_retry_seqs_;
  bool stopped_;
  int64_t fetch_abort_reason_;
  int64_t ref_count_;
};

class ObVectorIndexMigrationDag : public ObCompleteMigrationDag
{
public:
  ObVectorIndexMigrationDag();
  virtual ~ObVectorIndexMigrationDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                              common::ObIAllocator &allocator) const override;
  virtual int create_first_task() override;
  virtual int generate_next_dag(share::ObIDag *&dag) override;
  virtual int inner_reset_status_for_retry() override;
  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObCompleteMigrationDag", ObCompleteMigrationDag, KP(this));

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObVectorIndexMigrationDag);
};

class ObVectorIndexMigrationTask : public share::ObITask
{
public:
  ObVectorIndexMigrationTask();
  virtual ~ObVectorIndexMigrationTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObVectorIndexMigrationTask"), KP(this), KPC(ctx_));

private:
  int fetch_adaptor_list_from_src_();
  int check_vsag_version_match_(const common::ObString &src_vsag_version);
  int batch_create_adaptor_shells_();
  int generate_first_adaptor_dag_();
  int record_server_event_();

private:
  bool is_inited_;
  ObLSCompleteMigrationCtx *ctx_;
  share::ObIDagNet *dag_net_;
  obrpc::ObStorageRpcProxy *rpc_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObVectorIndexMigrationTask);
};

class ObVecIndexFinishTask;

class ObVectorIndexAdaptorMigrationDag : public ObCompleteMigrationDag
{
public:
  static constexpr int64_t RETRY_BACKOFF_US = 1L * 1000L * 1000L; // 1s

  ObVectorIndexAdaptorMigrationDag();
  virtual ~ObVectorIndexAdaptorMigrationDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                              common::ObIAllocator &allocator) const override;
  virtual int create_first_task() override;
  virtual int generate_next_dag(share::ObIDag *&dag) override;
  virtual int inner_reset_status_for_retry() override;
  int init(share::ObIDagNet *dag_net,
           const ObMigrationVectorIndexAdaptorMeta &adaptor_meta);

  const ObMigrationVectorIndexAdaptorMeta &get_adaptor_meta() const { return adaptor_meta_; }
  int64_t get_adaptor_handle_id() const { return adaptor_handle_id_; }
  void set_adaptor_handle_id(const int64_t id) { adaptor_handle_id_ = id; }

  int64_t get_total_segment_count() const { return vec_index_meta_.segment_count(); }

  const share::ObVectorIndexMeta &get_vec_index_meta() const { return vec_index_meta_; }
  int set_vec_index_meta(const share::ObVectorIndexMeta &meta);

  const share::ObVectorIndexMetaHeader &get_meta_header() const { return vec_index_meta_.header_; }

  void record_migration_failed_event(const int32_t result);

  // True if this adaptor dag or its LS dag-net ctx has failed (external cancel).
  bool is_migration_cancelled() const;

  void mark_adaptor_migrate_start_ts_if_unset();
  int release_src_adaptor_handle();
  int64_t get_adaptor_migrate_cost_us() const;
  INHERIT_TO_STRING_KV("ObCompleteMigrationDag", ObCompleteMigrationDag, KP(this),
                       K_(adaptor_meta), K_(adaptor_handle_id));

private:
  int clear_migrated_segments_from_dest_adaptor_(); // Wipes segments installed in dest adaptor on retry

  bool is_inited_;
  ObMigrationVectorIndexAdaptorMeta adaptor_meta_;
  int64_t adaptor_handle_id_;
  int64_t adaptor_migrate_start_ts_; // Time-tracks single-adaptor migration cost for vector_index_migration_adaptor_copy_finish (Prepare→Finish).
  share::ObVectorIndexMeta vec_index_meta_;
  ObVecIndexFinishTask *finish_task_;
  DISALLOW_COPY_AND_ASSIGN(ObVectorIndexAdaptorMigrationDag);
};

class ObVectorIndexAdaptorMigrationTask : public share::ObITask
{
public:
  ObVectorIndexAdaptorMigrationTask();
  virtual ~ObVectorIndexAdaptorMigrationTask();
  int init(ObVecIndexFinishTask *finish_task);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObVectorIndexAdaptorMigrationTask"), KP(this), KPC(ctx_));

private:
  int try_skip_fetch_by_tenant_map_(const ObMigrationVectorIndexAdaptorMeta &adaptor_meta,
                                    bool &skip_fetch);
  int hold_adapter_and_fetch_segment_metas_from_src_();
  int create_copy_and_finish_tasks_();
  int record_server_event_(const bool skip_fetch);
  ObVectorIndexAdaptorMigrationDag *get_adaptor_dag_() const {
    return static_cast<ObVectorIndexAdaptorMigrationDag *>(this->get_dag());
  }

private:
  bool is_inited_;
  ObLSCompleteMigrationCtx *ctx_;
  share::ObIDagNet *dag_net_;
  ObVecIndexFinishTask *finish_task_;
  obrpc::ObStorageRpcProxy *rpc_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObVectorIndexAdaptorMigrationTask);
};

struct ObVecIdxMigVsagStreamCbParam : public share::ObIStreamBuf::CbParam
{
  ObVecIdxMigVsagStreamCbParam()
      : driver_handle_(),
        slot_buf_(nullptr),
        slot_cap_(0),
        expected_processor_id_(common::OB_INVALID_ID),
        expected_tenant_id_(common::OB_INVALID_TENANT_ID),
        expected_ls_id_(),
        adaptor_dag_(nullptr)
  {}

  virtual CbParamType get_type() const override { return CbParamType::VEC_MIG_VSAG_STREAM; }
  bool is_valid() const;

  ObVecIdxSegFetchDriverHandle driver_handle_;
  char *slot_buf_;
  int64_t slot_cap_;
  int64_t expected_processor_id_;
  uint64_t expected_tenant_id_;
  share::ObLSID expected_ls_id_;
  ObVectorIndexAdaptorMigrationDag *adaptor_dag_; // For dag/dag-net cancel check before blocking on driver.

};

class ObVecIdxMigMetaAccumulator
{
public:
  explicit ObVecIdxMigMetaAccumulator(common::ObIAllocator &alloc)
      : alloc_(alloc), meta_buf_(nullptr), meta_size_(0), meta_len_(0)
  {}

  int feed(const ObVecIdxMigMetaChunkHeader &header,
           const char *body, int64_t body_len);
  bool is_complete() const { return nullptr != meta_buf_ && meta_size_ == meta_len_; }
  char *meta_buf() const { return meta_buf_; }
  int64_t meta_size() const { return meta_size_; }
  int64_t meta_len() const { return meta_len_; }

private:
  common::ObIAllocator &alloc_;
  char *meta_buf_;
  int64_t meta_size_;
  int64_t meta_len_;
  DISALLOW_COPY_AND_ASSIGN(ObVecIdxMigMetaAccumulator);
};

class ObVecIdxMigVsagStreamReader
{
public:
  ObVecIdxMigVsagStreamReader() = default;
  int operator()(
      char *&data,
      const int64_t data_size,
      int64_t &read_size,
      share::ObIStreamBuf::CbParam &param);
};

class ObVecIndexSegmentCopyTask : public share::ObITask
{
public:
  ObVecIndexSegmentCopyTask();
  virtual ~ObVecIndexSegmentCopyTask();
  int init(const int64_t segment_idx);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObVecIndexSegmentCopyTask"), KP(this), K_(segment_idx), K_(processor_id));

private:
  virtual int generate_next_task(share::ObITask *&next_task) override;
  int migrate_vec_index_segment_();
  int register_src_segment_processor_();
  int create_fetch_handle_(int64_t &deser_buf_size);
  int get_dest_adaptor_guard_(
      share::ObPluginVectorIndexAdapterGuard &adaptor_guard,
      share::ObPluginVectorIndexAdaptor *&adaptor,
      bool &skip_while_adaptor_deleted);

  int validate_and_create_dest_segment_(
      share::ObPluginVectorIndexAdaptor *adaptor,
      share::ObVectorIndexSegmentHandle &dest_segment_handle);
  int read_and_deserialize_meta_(
      ObVecIdxSegFetchDriverHandle &handle,
      int64_t buf_size,
      common::ObIAllocator &alloc,
      share::ObVectorIndexSegmentHandle &dest_segment_handle);
  int stream_deserialize_vsag_(
      ObVecIdxSegFetchDriverHandle &handle,
      int64_t buf_size,
      common::ObIAllocator &alloc,
      share::ObVectorIndexSegmentHandle &dest_segment_handle,
      const int64_t segment_idx);

private:
  ObVectorIndexAdaptorMigrationDag *get_adaptor_dag_() const {
    return static_cast<ObVectorIndexAdaptorMigrationDag *>(this->get_dag());
  }

  bool is_inited_;
  int64_t segment_idx_;
  ObLSCompleteMigrationCtx *ctx_;
  share::ObIDagNet *dag_net_;
  obrpc::ObStorageRpcProxy *rpc_proxy_;
  int64_t processor_id_;
  ObVecIdxSegFetchDriverHandle fetch_handle_;
  DISALLOW_COPY_AND_ASSIGN(ObVecIndexSegmentCopyTask);
};

class ObVecIndexFinishTask : public share::ObITask
{
public:
  ObVecIndexFinishTask();
  virtual ~ObVecIndexFinishTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObVecIndexFinishTask"), KP(this));

private:
  int enqueue_mem_sync_task_(const ObMigrationVectorIndexAdaptorMeta &adaptor_meta);
  int record_server_event_();
  ObVectorIndexAdaptorMigrationDag *get_adaptor_dag_() const {
    return static_cast<ObVectorIndexAdaptorMigrationDag *>(this->get_dag());
  }

private:
  bool is_inited_;
  ObLSCompleteMigrationCtx *ctx_;
  share::ObIDagNet *dag_net_;
  obrpc::ObStorageRpcProxy *rpc_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObVecIndexFinishTask);
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_MIGRATION_VECTOR_INDEX_H_
