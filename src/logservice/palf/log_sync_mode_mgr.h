/**
* Copyright (c) 2023-present OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*/

#ifndef OCEANBASE_LOGSERVICE_LOG_SYNC_MODE_MGR_H_
#define OCEANBASE_LOGSERVICE_LOG_SYNC_MODE_MGR_H_

#include "lib/lock/ob_spin_rwlock.h"
#include "lib/ob_define.h"
#include "lsn.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "share/ob_cluster_role.h"
#include "../ob_log_base_header.h"
#include "../ob_append_callback.h"
#include "palf_options.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
}
namespace share
{
class ObSyncStandbyStatusAttr;
}
namespace logservice
{

enum ObSyncModeLogType
{
  SYNC_MODE_UNKNOWN_TYPE = 0,
  SYNC = 1,
  ASYNC = 2,
  PRE_ASYNC = 3,
  SYNC_MODE_LOG_TYPE_MAX_TYPE = 4,
};

class ObSyncModeLog
{
public:
  ObSyncModeLog();
  ObSyncModeLog(const int16_t log_type);
  ~ObSyncModeLog();
  void reset();
  int16_t get_log_type() const;
  void set_protection_info(const share::ObSyncStandbyStatusAttr &protection_info);
  const share::ObSyncStandbyStatusAttr &get_protection_info() const { return protection_info_; }
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K(header_), K(version_), K(log_type_), K(protection_info_));
private:
  static const int16_t SYNC_MODE_LOG_VERSION = 1;
  ObLogBaseHeader header_;
  int16_t version_;
  int16_t log_type_;
  share::ObSyncStandbyStatusAttr protection_info_; //TODO by ziqi: 恢复类型实现
};

// SyncModeLogCb: Callback for sync mode log with reference counting mechanism.
//
// Design Purpose:
//   This callback uses reference counting to solve the use-after-free problem when
//   callbacks are submitted to the asynchronous applyservice queue. The callback
//   can be safely used regardless of whether it's pushed to the apply queue or not.
//
// Reference Counting Strategy:
//   1. When created via create(): ref_cnt = 1 (held by caller)
//   2. If pushed to applyservice: ref_cnt = 2 (caller + applyservice)
//   3. When applyservice calls on_success/on_failure: ref_cnt decreases by 1
//   4. When caller calls dec_ref(): ref_cnt decreases by 1
//   5. When ref_cnt reaches 0: callback is automatically freed
//
// Usage Pattern:
//   SyncModeLogCb *cb = nullptr;
//   if (OB_SUCC(SyncModeLogCb::create(cb))) {
//     if (need_push_cb) {
//       cb->inc_ref();  // Add reference for applyservice
//     }
//     // ... use cb ...
//     cb->dec_ref();  // Release caller's reference
//   }
//
// Thread Safety:
//   All reference counting operations use atomic operations, making this class
//   thread-safe for concurrent access.
class SyncModeLogCb : public AppendCb
{
public:
  // Callback execution state
  enum CbState : uint8_t
  {
    STATE_INIT = 0,      // Initial state, callback not executed yet
    STATE_SUCCESS = 1,   // Log append succeeded
    STATE_FAILED = 2,    // Log append failed
  };

  SyncModeLogCb() : state_(CbState::STATE_INIT), ref_cnt_(0) {}
  virtual ~SyncModeLogCb() {}

  // Creates a callback instance on heap with reference counting.
  //
  // @param [out] cb Pointer to the created callback (ref_cnt = 1 after creation)
  // @return OB_SUCCESS if creation succeeds, OB_ALLOCATE_MEMORY_FAILED otherwise
  //
  // Note: The caller must call dec_ref() when done with the callback to release
  //       its reference. If the callback is pushed to applyservice, call inc_ref()
  //       before submission to add a reference for applyservice.
  static int create(SyncModeLogCb *&cb)
  {
    int ret = OB_SUCCESS;
    void *buf = ob_malloc(sizeof(SyncModeLogCb), "SyncModeLogCb");
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      cb = new (buf) SyncModeLogCb();
      cb->inc_ref();  // Initial reference for caller
    }
    return ret;
  }

  // Increases the reference count atomically.
  //
  // Call this before submitting the callback to applyservice if need_push_cb=true.
  // This ensures the callback remains valid even if the caller returns or times out.
  void inc_ref()
  {
    ATOMIC_INC(&ref_cnt_);
  }

  // Decreases the reference count atomically and frees the callback if count reaches zero.
  //
  // This method is thread-safe and can be called from any thread. The callback
  // will be automatically freed when the last reference is released.
  void dec_ref()
  {
    int64_t ref = ATOMIC_SAF(&ref_cnt_, 1);
    if (0 == ref) {
      this->~SyncModeLogCb();
      ob_free(this);
    }
  }

  // Called by applyservice when log append succeeds.
  //
  // Sets state to STATE_SUCCESS and releases the reference held by applyservice.
  // If this was the last reference, the callback will be automatically freed.
  //
  // @return OB_SUCCESS
  virtual int on_success() override
  {
    ATOMIC_STORE(&state_, CbState::STATE_SUCCESS);
    dec_ref();  // Release reference held by applyservice or manual caller
    return OB_SUCCESS;
  }

  // Called by applyservice when log append fails.
  //
  // Sets state to STATE_FAILED and releases the reference held by applyservice.
  // If this was the last reference, the callback will be automatically freed.
  //
  // @return OB_SUCCESS
  virtual int on_failure() override
  {
    ATOMIC_STORE(&state_, CbState::STATE_FAILED);
    dec_ref();  // Release reference held by applyservice or manual caller
    return OB_SUCCESS;
  }

  // Checks if the callback execution succeeded.
  //
  // @return true if state is STATE_SUCCESS, false otherwise
  inline bool is_succeed() const { return CbState::STATE_SUCCESS == ATOMIC_LOAD(&state_); }

  // Checks if the callback execution failed.
  //
  // @return true if state is STATE_FAILED, false otherwise
  inline bool is_failed() const { return CbState::STATE_FAILED == ATOMIC_LOAD(&state_); }

  // Gets the current reference count (for debugging).
  //
  // @return Current reference count
  inline int64_t get_ref_cnt() const { return ATOMIC_LOAD(&ref_cnt_); }

  const char *get_cb_name() const override { return "SyncModeLogCb"; }
  TO_STRING_KV(K(state_), K(ref_cnt_));

private:
  CbState state_;        // Current execution state of the callback
  int64_t ref_cnt_;      // Reference counter: caller + applyservice (if pushed to queue)
};

class ObSyncModeLogHandler : public ObIReplaySubHandler,
                        public ObIRoleChangeSubHandler,
                        public ObICheckpointSubHandler
{
public:
  ObSyncModeLogHandler() : is_inited_(false), ls_(nullptr) {}
  ~ObSyncModeLogHandler() { reset(); }

public:
  int init(storage::ObLS *ls);
  void reset();

  // ObIReplaySubHandler interface
  int replay(const void *buffer,
            const int64_t nbytes,
            const palf::LSN &lsn,
            const share::SCN &scn) override final;

  // ObIRoleChangeSubHandler interface
  void switch_to_follower_forcedly() override final;
  int switch_to_leader() override final;
  int switch_to_follower_gracefully() override final;
  int resume_leader() override final;

  // ObICheckpointSubHandler interface
  int flush(share::SCN &rec_scn) override final;
  share::SCN get_rec_scn() override final;

private:
  int replay_sync_mode_log(const void *buffer,
                        const int64_t nbytes,
                        const palf::LSN &lsn,
                        const share::SCN &scn);

private:
  bool is_inited_;
  storage::ObLS *ls_;
};

// Forward declaration
class ObLogHandler;

// Manager class for sync mode operations
// Encapsulates all sync mode related logic to keep ObLogHandler clean
class ObSyncModeManager
{
public:
  ObSyncModeManager();
  ~ObSyncModeManager() { reset(); }

  // Initialize with log handler
  int init(ObLogHandler *log_handler);
  void reset();

  // Write sync mode log synchronously
  int write_sync_mode_log(const ObSyncModeLogType log_type,
                          const share::SCN &ref_scn,
                          const share::ObSyncStandbyStatusAttr &protection_info,
                          palf::LSN &lsn,
                          share::SCN &scn,
                          const int64_t expected_proposal_id,
                          const int64_t abs_timeout_us = 0);

  // Handle sync mode upgrade (ASYNC -> SYNC)
  int handle_sync_mode_upgrade(const int64_t mode_version,
                               const bool need_write_log,
                               const share::ObLSID &ls_id,
                               const share::SCN &ref_scn,
                               const share::ObSyncStandbyStatusAttr &protection_info,
                               share::SCN &end_scn,
                               int64_t &new_mode_version,
                               const int64_t abs_timeout_us = 0);

  // Handle sync mode downgrade (SYNC -> PRE_ASYNC)
  int handle_sync_mode_downgrade(const int64_t mode_version,
                                 const bool need_write_log,
                                 const share::ObLSID &ls_id,
                                 const share::SCN &ref_scn,
                                 const share::ObSyncStandbyStatusAttr &protection_info,
                                 share::SCN &end_scn,
                                 int64_t &new_mode_version,
                                 const int64_t abs_timeout_us = 0);
  // Handle sync mode after downgrade (PRE_ASYNC -> ASYNC)
  int handle_sync_mode_downgrade_finish(const int64_t mode_version,
                                        const bool need_write_log,
                                        const share::ObLSID &ls_id,
                                        const share::SCN &ref_scn,
                                        const share::ObSyncStandbyStatusAttr &protection_info,
                                        share::SCN &end_scn,
                                        int64_t &new_mode_version,
                                        const int64_t abs_timeout_us = 0);

  // Change sync mode with transport service and write special log
  int process_upgrade_and_downgrade(const int64_t mode_version,
                                      const palf::SyncMode &sync_mode,
                                      const share::SCN &ref_scn,
                                      const share::ObSyncStandbyStatusAttr &protection_info,
                                      share::SCN &end_scn,
                                      int64_t &new_mode_version,
                                      const int64_t abs_timeout_us = 0);

  bool is_inited() const { return is_inited_; }

private:
  // Wait for apply service to finish
  // int wait_apply_service_done();

  // Notify apply service with standby LSN
  int notify_apply_service_with_standby_lsn();

  // Wait for apply status to become leader
  int wait_log_handler_leader_(const int64_t proposal_id, const int64_t abs_timeout_us = 0);

private:
  bool is_inited_;
  ObLogHandler *log_handler_;
};

} // end namespace logservice
} // end namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_SYNC_MODE_MGR_H_
