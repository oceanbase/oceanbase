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

#pragma once

#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_link_hashmap.h"
#include "observer/table_load/ob_table_load_object_allocator.h"
#include "share/ob_autoincrement_param.h"
#include "share/table/ob_table_load_array.h"
#include "share/table/ob_table_load_define.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/direct_load/ob_direct_load_trans_param.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_assigned_memory_manager.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadTransParam;
class ObDirectLoadInsertTableContext;
class ObDirectLoadTmpFileManager;
}  // namespace storage
namespace share
{
class ObSequenceCache;
} // namespace share
namespace observer
{
class ObTableLoadTableCtx;
class ObTableLoadStoreTrans;
class ObTableLoadTransCtx;
class ObTableLoadTransStore;
class ObITableLoadTaskScheduler;
class ObTableLoadMerger;
class ObTableLoadErrorRowHandler;

class ObTableLoadStoreCtx
{
static const int64_t MACRO_BLOCK_WRITER_MEM_SIZE = 10 * 1024LL * 1024LL;
public:
  ObTableLoadStoreCtx(ObTableLoadTableCtx *ctx);
  ~ObTableLoadStoreCtx();
  int init(
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &partition_id_array,
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &target_partition_id_array);
  void stop();
  void destroy();
  bool is_valid() const { return is_inited_; }
  TO_STRING_KV(K_(is_inited));
public:
  OB_INLINE lib::ObMutex &get_op_lock()
  {
    return op_lock_;
  }
  OB_INLINE table::ObTableLoadStatusType get_status() const
  {
    obsys::ObRLockGuard guard(status_lock_);
    return status_;
  }
  OB_INLINE void get_status(table::ObTableLoadStatusType &status, int &error_code) const
  {
    obsys::ObRLockGuard guard(status_lock_);
    status = status_;
    error_code = error_code_;
  }
  OB_INLINE int set_status_inited()
  {
    return advance_status(table::ObTableLoadStatusType::INITED);
  }
  OB_INLINE int set_status_loading()
  {
    return advance_status(table::ObTableLoadStatusType::LOADING);
  }
  OB_INLINE int set_status_frozen()
  {
    return advance_status(table::ObTableLoadStatusType::FROZEN);
  }
  OB_INLINE int set_status_merging()
  {
    return advance_status(table::ObTableLoadStatusType::MERGING);
  }
  OB_INLINE int set_status_merged()
  {
    return advance_status(table::ObTableLoadStatusType::MERGED);
  }
  OB_INLINE int set_status_commit()
  {
    return advance_status(table::ObTableLoadStatusType::COMMIT);
  }
  int set_status_error(int error_code);
  int set_status_abort();
  int check_status(table::ObTableLoadStatusType status) const;
  OB_INLINE bool enable_heart_beat_check() const { return enable_heart_beat_check_; }
  OB_INLINE void set_enable_heart_beat_check(bool enable_heart_beat_check)
  {
    enable_heart_beat_check_ = enable_heart_beat_check;
  }
  void heart_beat();
  bool check_heart_beat_expired(const uint64_t expired_time_us);
private:
  int advance_status(table::ObTableLoadStatusType status);
public:
  int start_trans(const table::ObTableLoadTransId &trans_id, ObTableLoadStoreTrans *&trans);
  int commit_trans(ObTableLoadStoreTrans *trans);
  int abort_trans(ObTableLoadStoreTrans *trans);
  void put_trans(ObTableLoadStoreTrans *trans);
  int get_trans(const table::ObTableLoadTransId &trans_id, ObTableLoadStoreTrans *&trans);
  int get_trans_ctx(const table::ObTableLoadTransId &trans_id,
                    ObTableLoadTransCtx *&trans_ctx) const;
  int get_segment_trans(const table::ObTableLoadSegmentID &segment_id,
                        ObTableLoadStoreTrans *&trans);
  int get_active_trans_ids(common::ObIArray<table::ObTableLoadTransId> &trans_id_array) const;
  int get_committed_trans_ids(table::ObTableLoadArray<table::ObTableLoadTransId> &trans_id_array,
                              common::ObIAllocator &allocator) const;
  int get_committed_trans_stores(
    common::ObIArray<ObTableLoadTransStore *> &trans_store_array) const;
  int check_exist_trans(bool &exist) const;
  // release disk space
  void clear_committed_trans_stores();
private:
  int alloc_trans_ctx(const table::ObTableLoadTransId &trans_id, ObTableLoadTransCtx *&trans_ctx);
  int alloc_trans(const table::ObTableLoadTransId &trans_id, ObTableLoadStoreTrans *&trans);
  int init_session_ctx_array();
  int init_trans_param();
  int generate_autoinc_params(share::AutoincParam &autoinc_param);
  int init_sequence();
public:
  int commit_autoinc_value();
  int get_next_insert_tablet_ctx(ObTabletID &tablet_id);
  void handle_open_insert_tablet_ctx_finish(bool &is_finish);
public:
  ObTableLoadTableCtx * const ctx_;
  common::ObArenaAllocator allocator_;
  common::ObArray<table::ObTableLoadLSIdAndPartitionId> ls_partition_ids_;
  common::ObArray<table::ObTableLoadLSIdAndPartitionId> target_ls_partition_ids_;
  storage::ObDirectLoadTableDataDesc table_data_desc_;
  storage::ObDirectLoadTableDataDesc lob_id_table_data_desc_;
  storage::ObDirectLoadTransParam trans_param_;
  table::ObTableLoadResultInfo result_info_;
  ObITableLoadTaskScheduler *task_scheduler_;
  ObTableLoadMerger *merger_;
  storage::ObDirectLoadInsertTableContext *insert_table_ctx_;
  int64_t next_tablet_idx_;
  int64_t opened_insert_tablet_count_;
  bool is_multiple_mode_;
  bool is_fast_heap_table_;
  int64_t px_writer_count_;
  storage::ObDirectLoadTmpFileManager *tmp_file_mgr_;
  ObTableLoadErrorRowHandler *error_row_handler_;
  share::schema::ObSequenceSchema sequence_schema_;
  uint64_t next_session_id_ CACHE_ALIGNED;
  struct SessionContext
  {
    SessionContext() : extra_buf_(nullptr), extra_buf_size_(0) {}
    share::AutoincParam autoinc_param_;
    // for multiple mode
    char *extra_buf_;
    int64_t extra_buf_size_;
  };
  SessionContext *session_ctx_array_;
private:
  struct SegmentCtx : public common::LinkHashValue<table::ObTableLoadSegmentID>
  {
  public:
    SegmentCtx() : segment_id_(0), current_trans_(nullptr), committed_trans_store_(nullptr) {}
    TO_STRING_KV(K_(segment_id), KP_(current_trans), KP_(committed_trans_store));
  public:
    table::ObTableLoadSegmentID segment_id_;
    ObTableLoadStoreTrans *current_trans_;
    ObTableLoadTransStore *committed_trans_store_;
  };
private:
  typedef common::hash::ObHashMap<table::ObTableLoadTransId, ObTableLoadStoreTrans *,
                                  common::hash::NoPthreadDefendMode>
    TransMap;
  typedef common::hash::ObHashMap<table::ObTableLoadTransId, ObTableLoadTransCtx *,
                                  common::hash::NoPthreadDefendMode>
    TransCtxMap;
  typedef common::ObLinkHashMap<table::ObTableLoadSegmentID, SegmentCtx> SegmentCtxMap;
private:
  ObTableLoadObjectAllocator<ObTableLoadStoreTrans> trans_allocator_; // 多线程安全
  lib::ObMutex op_lock_;
  mutable obsys::ObRWLock status_lock_;
  table::ObTableLoadStatusType status_;
  int error_code_;
  mutable obsys::ObRWLock rwlock_;
  TransMap trans_map_;
  TransCtxMap trans_ctx_map_;
  SegmentCtxMap segment_ctx_map_;
  common::ObArray<ObTableLoadTransStore *> committed_trans_store_array_;
  uint64_t last_heart_beat_ts_;
  bool enable_heart_beat_check_;
  bool is_inited_;
};

}  // namespace observer
}  // namespace oceanbase
