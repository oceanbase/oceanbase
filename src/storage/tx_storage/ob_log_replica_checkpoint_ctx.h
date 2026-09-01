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

#ifndef OCEANBASE_STORAGE_OB_LOG_REPLICA_CHECKPOINT_CTX_
#define OCEANBASE_STORAGE_OB_LOG_REPLICA_CHECKPOINT_CTX_

#include "lib/hash/ob_hashmap.h"
#include "lib/utility/ob_print_utils.h"
#include "logservice/palf/lsn.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
namespace checkpoint
{

#ifdef ERRSIM
bool need_errsim_block_clog_checkpoint(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id);
#endif

// Records the complete lifecycle of one log replica checkpoint update:
// calculate the target checkpoint, observe the LS meta before the update,
// execute the update and PALF retry, then observe the LS meta after the update.
// Create a new context for each update attempt.
class ObLogReplicaCheckpointCtx final
{
public:
  explicit ObLogReplicaCheckpointCtx(ObLS &ls);
  ~ObLogReplicaCheckpointCtx();

  // log_disk_pressure is a tenant-local snapshot shared by one checkpoint
  // timer round, so every log replica in that round follows the same policy.
  int update_checkpoint(const bool log_disk_pressure);

  // Returns true only when a completed attempt has enough observations to
  // calculate the distance between the local log end and LS checkpoint.
  bool get_checkpoint_delay(
      const share::SCN &checkpoint_scn,
      int64_t &checkpoint_delay_us) const;

  TO_STRING_KV("ls", reinterpret_cast<const void *>(&ls_),
      K_(is_strict_clog_recycle_mode),
      K_(target_checkpoint_scn),
      K_(majority_min_replica_checkpoint_scn),
      K_(pure_readable_scn),
      K_(local_end_scn),
      K_(disk_pressure_safe_checkpoint_scn));

private:
  int get_majority_min_replica_checkpoint_scn_from_leader_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      share::SCN &checkpoint_scn);
  int cal_logonly_replica_checkpoint_(const bool log_disk_pressure);
  int cal_logonly_replica_checkpoint_strict_mode_(const bool log_disk_pressure);
  int raise_target_checkpoint_for_disk_pressure_();
  static int calculate_disk_pressure_safe_lsn_(
      const palf::LSN &palf_disk_begin_lsn,
      const palf::LSN &local_end_lsn,
      palf::LSN &safe_lsn);
  int update_log_replica_checkpoint_();

private:
  ObLS &ls_;
  bool is_strict_clog_recycle_mode_;
  share::SCN target_checkpoint_scn_;
  share::SCN majority_min_replica_checkpoint_scn_;
  share::SCN pure_readable_scn_;
  share::SCN local_end_scn_;
  share::SCN disk_pressure_safe_checkpoint_scn_;

  DISALLOW_COPY_AND_ASSIGN(ObLogReplicaCheckpointCtx);
};

class ObLogReplicaCheckpointDelayReporter final
{
public:
  ObLogReplicaCheckpointDelayReporter()
    : log_disk_pressure_(false), checkpoint_delay_report_info_map_()
  {}
  ~ObLogReplicaCheckpointDelayReporter() { destroy(); }

  void destroy();
  void prepare_for_next_round();
  void report_if_needed(
      const share::ObLSID &ls_id,
      const share::SCN &checkpoint_scn,
      const ObLogReplicaCheckpointCtx &checkpoint_ctx);
  bool is_log_disk_under_pressure() const { return log_disk_pressure_; }

  TO_STRING_KV(K_(log_disk_pressure));

private:
  struct CheckpointDelayReportInfo
  {
    CheckpointDelayReportInfo()
      : checkpoint_scn_(share::SCN::invalid_scn()),
        report_base_ts_(0),
        last_seen_ts_(0)
    {}
    share::SCN checkpoint_scn_;
    int64_t report_base_ts_;
    int64_t last_seen_ts_;
  };

  bool need_report_log_replica_checkpoint_delay_(
      const share::ObLSID &ls_id,
      const share::SCN &checkpoint_scn,
      const int64_t current_ts,
      const int64_t report_interval_us);
  int get_log_disk_pressure_(bool &log_disk_pressure);
  void remove_expired_report_info_();

private:
  typedef common::hash::ObHashMap<share::ObLSID, CheckpointDelayReportInfo,
      common::hash::NoPthreadDefendMode> CheckpointDelayReportInfoMap;
  bool log_disk_pressure_;
  // Accessed only by checkpoint_timer_'s single worker thread.
  CheckpointDelayReportInfoMap checkpoint_delay_report_info_map_;
};

} // namespace checkpoint
} // namespace storage
} // namespace oceanbase

#endif
