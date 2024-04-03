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

#ifndef OCENABASE_SHARE_OB_LS_RESTORE_STATUS_H
#define OCENABASE_SHARE_OB_LS_RESTORE_STATUS_H

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{

// log stream restore state machine
// when create ls which need to restore, set the restore status = RESTORE_START
class ObLSRestoreStatus final
{
public:
  enum Status : uint8_t
  {
    // default status
    NONE = 0,
    // restore
    // log stream restore initial state
    RESTORE_START = 1,
    // restore sys tablets and create user tables
    RESTORE_SYS_TABLETS = 2,
    // wait followers to restore sys tablets
    WAIT_RESTORE_SYS_TABLETS = 3,
    // restore tablets meta
    RESTORE_TABLETS_META = 4,
    // wait restore tablets meta
    WAIT_RESTORE_TABLETS_META = 5,
    // replay log to consistent_scn
    RESTORE_TO_CONSISTENT_SCN = 6,
    // wait followers to replay log to consistent_scn
    WAIT_RESTORE_TO_CONSISTENT_SCN = 7,
    // restore major sst meta, minor sst and clog
    QUICK_RESTORE = 8,
    // wait followers to do quick restore
    WAIT_QUICK_RESTORE = 9,
    // finish quick restore, major macro blocks are in remote reference state
    QUICK_RESTORE_FINISH = 10,
    // restore major macro blocks
    RESTORE_MAJOR_DATA = 11,
    // wait followers to restore major macro blocks
    WAIT_RESTORE_MAJOR_DATA = 12,
    // restore failed
    RESTORE_FAILED = 13,

    // clone
    // log stream clone initial state
    CLONE_START = 101,
    // copy all tablets meta from tenant_snapshot
    CLONE_COPY_ALL_TABLET_META = 102,
    // copy ls meta from tenant snapshot
    CLONE_COPY_LS_META = 103,
    // wait clog replay finished
    CLONE_CLOG_REPLAY = 104,
    // clone failed
    CLONE_FAILED = 105,

    LS_RESTORE_STATUS_MAX = 255
  };

public:
  ObLSRestoreStatus() : status_(Status::NONE) {}
  ~ObLSRestoreStatus() = default;
  explicit ObLSRestoreStatus(const Status &status);
  ObLSRestoreStatus &operator=(const ObLSRestoreStatus &restore_status);
  ObLSRestoreStatus &operator=(const Status &status);
  bool operator ==(const ObLSRestoreStatus &other) const { return status_ == other.status_; }
  bool operator !=(const ObLSRestoreStatus &other) const { return status_ != other.status_; }
  operator Status() const { return status_; }
  static const char *get_restore_status_str(const ObLSRestoreStatus &status);
  bool is_valid() const { return is_valid_(status_); }
  bool is_restore_start() const { return Status::RESTORE_START == status_; }
  bool is_quick_restore() const { return Status::QUICK_RESTORE == status_; }
  bool is_restore_major_data() const { return Status::RESTORE_MAJOR_DATA == status_; }
  bool is_none() const { return Status::NONE == status_; }
  bool is_failed() const { return Status::RESTORE_FAILED == status_ || Status::CLONE_FAILED == status_; }
  bool is_restore_sys_tablets() const { return Status::RESTORE_SYS_TABLETS == status_; }
  bool is_restore_tablets_meta() const { return Status::RESTORE_TABLETS_META == status_; }
  bool is_restore_to_consistent_scn() const { return Status::RESTORE_TO_CONSISTENT_SCN == status_; }
  bool is_wait_restore_consistent_scn() const { return Status::WAIT_RESTORE_TO_CONSISTENT_SCN == status_; }
  bool is_wait_restore_sys_tablets() const { return Status::WAIT_RESTORE_SYS_TABLETS == status_; }
  bool is_wait_restore_tablets_meta() const { return Status::WAIT_RESTORE_TABLETS_META == status_; }
  bool is_wait_quick_restore() const { return Status::WAIT_QUICK_RESTORE == status_; }
  bool is_wait_restore_major_data() const { return Status::WAIT_RESTORE_MAJOR_DATA == status_; }
  bool is_quick_restore_finish() const { return Status::QUICK_RESTORE_FINISH == status_;}
  bool is_in_restore() const { return status_ >= Status::RESTORE_START && status_ <= Status::RESTORE_FAILED; }
  bool is_in_restore_or_none() const { return is_none() || is_in_restore(); }
  bool is_wait_status() const
  {
    return is_wait_restore_sys_tablets()
           || is_wait_restore_consistent_scn()
           || is_wait_restore_tablets_meta()
           || is_wait_quick_restore()
           || is_wait_restore_major_data();
  }

  // offline ls and enable sync and online ls restore handler in [RESTORE_START, RESTORE_SYS_TABLETS] or RESTORE_FAILED
  bool need_online() const;
  // enable sync and online ls restore handler in [RESTORE_START, RESTORE_SYS_TABLETS] or RESTORE_FAILED
  bool is_restore_first_step() const
  {
    return ((status_ >= Status::RESTORE_START && status_ <= Status::RESTORE_SYS_TABLETS) ||
             status_ == Status::RESTORE_FAILED);
  }

  bool is_in_clone() const { return status_ >= Status::CLONE_START && status_ <= Status::CLONE_FAILED; }
  bool is_in_clone_or_none() const { return is_none() || is_in_clone(); }
  bool is_in_clone_and_tablet_meta_incomplete() const
  {
    return status_ >= Status::CLONE_START && status_ <= Status::CLONE_COPY_ALL_TABLET_META;
  }
  bool is_clone_first_step() const
  {
    return ((status_ >= Status::CLONE_START && status_ <= Status::CLONE_COPY_LS_META) ||
             Status::CLONE_FAILED == status_);
  }

  // if restore status is not in [RESTORE_START, RESTORE_SYS_TABLETS], log_replay_service can replay log.
  // if restore status is not in [RESTORE_START, RESTORE_SYS_TABLETS] or restore_failed, log_replay_service can replay log.
  bool can_replay_log() const { return !(status_ >= Status::RESTORE_START && status_ <= Status::RESTORE_SYS_TABLETS) &&
                                       !(status_ >= Status::CLONE_START && status_ <= Status::CLONE_COPY_LS_META) &&
                                       status_ != Status::RESTORE_FAILED &&
                                       status_ != Status::CLONE_FAILED; }

  bool can_restore_log() const { return status_ == NONE ||
    (status_ >= RESTORE_TO_CONSISTENT_SCN && status_ < RESTORE_FAILED) ||
    (status_ >= CLONE_CLOG_REPLAY && status_ < CLONE_FAILED); }

  bool can_migrate() const
  {
    return !(status_ >= RESTORE_START && status_ < WAIT_RESTORE_TABLETS_META) &&
           !(status_ >= Status::CLONE_START && status_ <= Status::CLONE_CLOG_REPLAY);
  }
  bool is_in_restore_and_before_quick_restore() const
  {
    return status_ >= RESTORE_START && status_ <= WAIT_RESTORE_TO_CONSISTENT_SCN;
  }
  bool is_before_restore_to_consistent_scn() const
  {
    return status_ >= RESTORE_START && status_ < WAIT_RESTORE_TO_CONSISTENT_SCN;
  }
  Status get_status() const { return status_; }
  int set_status(int32_t status);

  int serialize(char *buf, const int64_t len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t len, int64_t &pos);
  int64_t get_serialize_size() const;

  TO_STRING_KV(K_(status));
private:
  bool is_valid_(int32_t status) const;
private:
  Status status_;
};


}
}

#endif
