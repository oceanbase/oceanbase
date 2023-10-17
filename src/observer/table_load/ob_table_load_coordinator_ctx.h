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
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_link_hashmap.h"
#include "observer/table_load/ob_table_load_exec_ctx.h"
#include "observer/table_load/ob_table_load_object_allocator.h"
#include "observer/table_load/ob_table_load_partition_calc.h"
#include "observer/table_load/ob_table_load_partition_location.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "share/ob_autoincrement_param.h"
#include "share/table/ob_table_load_array.h"
#include "share/table/ob_table_load_define.h"

namespace oceanbase
{
namespace share
{
class ObSequenceCache;
} // namespace share
namespace observer
{
class ObTableLoadTableCtx;
class ObTableLoadTransCtx;
class ObTableLoadCoordinatorTrans;
class ObITableLoadTaskScheduler;
class ObTableLoadErrorRowHandler;

class ObTableLoadCoordinatorCtx
{
public:
  ObTableLoadCoordinatorCtx(ObTableLoadTableCtx *ctx);
  ~ObTableLoadCoordinatorCtx();
  int init(const common::ObIArray<int64_t> &idx_array, ObTableLoadExecCtx *exec_ctx);
  void stop();
  void destroy();
  bool is_valid() const { return is_inited_; }
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
  OB_INLINE bool enable_heart_beat() const { return enable_heart_beat_; }
  OB_INLINE void set_enable_heart_beat(bool enable_heart_beat)
  {
    enable_heart_beat_ = enable_heart_beat;
  }
private:
  int advance_status(table::ObTableLoadStatusType status);
public:
  int start_trans(const table::ObTableLoadSegmentID &segment_id,
                  ObTableLoadCoordinatorTrans *&trans);
  int commit_trans(ObTableLoadCoordinatorTrans *trans);
  int abort_trans(ObTableLoadCoordinatorTrans *trans);
  void put_trans(ObTableLoadCoordinatorTrans *trans);
  int get_trans(const table::ObTableLoadTransId &trans_id,
                ObTableLoadCoordinatorTrans *&trans) const;
  int get_trans_ctx(const table::ObTableLoadTransId &trans_id,
                    ObTableLoadTransCtx *&trans_ctx) const;
  int get_segment_trans_ctx(const table::ObTableLoadSegmentID &segment_id,
                            ObTableLoadTransCtx *&trans_ctx);
  int get_active_trans_ids(common::ObIArray<table::ObTableLoadTransId> &trans_id_array) const;
  int get_committed_trans_ids(table::ObTableLoadArray<table::ObTableLoadTransId> &trans_id_array,
                              common::ObIAllocator &allocator) const;
  int check_exist_trans(bool &is_exist) const;
  int check_exist_committed_trans(bool &is_exist) const;
private:
  int alloc_trans_ctx(const table::ObTableLoadTransId &trans_id, ObTableLoadTransCtx *&trans_ctx);
  int alloc_trans(const table::ObTableLoadSegmentID &segment_id,
                  ObTableLoadCoordinatorTrans *&trans);
  int init_session_ctx_array();
  int generate_autoinc_params(share::AutoincParam &autoinc_param);
  int init_sequence();
  int init_partition_location();
public:
  ObTableLoadTableCtx * const ctx_;
  common::ObArenaAllocator allocator_;
  ObTableLoadSchema target_schema_;
  ObTableLoadPartitionLocation partition_location_;
  ObTableLoadPartitionLocation target_partition_location_;
  ObTableLoadPartitionCalc partition_calc_;
  ObITableLoadTaskScheduler *task_scheduler_;
  common::ObArray<int64_t> idx_array_;
  ObTableLoadExecCtx *exec_ctx_;
  table::ObTableLoadResultInfo result_info_;
  ObTableLoadErrorRowHandler *error_row_handler_;
  share::schema::ObSequenceSchema sequence_schema_;
  struct SessionContext
  {
    SessionContext() {}
    ~SessionContext() {}
    share::AutoincParam autoinc_param_;
  };
  SessionContext *session_ctx_array_;
private:
  struct SegmentCtx : public common::LinkHashValue<table::ObTableLoadSegmentID>
  {
  public:
    SegmentCtx() : segment_id_(0), current_trans_(nullptr), committed_trans_ctx_(nullptr) {}
    TO_STRING_KV(K_(segment_id), KP_(current_trans), KP_(committed_trans_ctx));
  public:
    table::ObTableLoadSegmentID segment_id_;
    ObTableLoadCoordinatorTrans *current_trans_;
    ObTableLoadTransCtx *committed_trans_ctx_;
  };
private:
  typedef common::hash::ObHashMap<table::ObTableLoadTransId, ObTableLoadCoordinatorTrans *,
                                  common::hash::NoPthreadDefendMode>
    TransMap;
  typedef common::hash::ObHashMap<table::ObTableLoadTransId, ObTableLoadTransCtx *,
                                  common::hash::NoPthreadDefendMode>
    TransCtxMap;
  typedef common::ObLinkHashMap<table::ObTableLoadSegmentID, SegmentCtx> SegmentCtxMap;
private:
  ObTableLoadObjectAllocator<ObTableLoadCoordinatorTrans> trans_allocator_; // 多线程安全
  uint64_t last_trans_gid_ CACHE_ALIGNED;
  uint64_t next_session_id_ CACHE_ALIGNED;
  lib::ObMutex op_lock_;
  mutable obsys::ObRWLock status_lock_;
  table::ObTableLoadStatusType status_;
  int error_code_;
  mutable obsys::ObRWLock rwlock_;
  TransMap trans_map_;
  TransCtxMap trans_ctx_map_;
  SegmentCtxMap segment_ctx_map_;
  common::ObSEArray<ObTableLoadTransCtx *, 64> commited_trans_ctx_array_;
  bool enable_heart_beat_;
  bool is_inited_;
};

}  // namespace observer
}  // namespace oceanbase
