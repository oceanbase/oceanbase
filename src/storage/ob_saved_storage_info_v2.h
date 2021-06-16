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

#ifndef OCEANBASE_STORAGE_OB_SAVED_STORAGE_INFO_V2_
#define OCEANBASE_STORAGE_OB_SAVED_STORAGE_INFO_V2_

#include "storage/ob_base_storage_info.h"
#include "common/ob_range.h"
#include "storage/ob_data_storage_info.h"

namespace oceanbase {

namespace common {
struct ObPartitionKey;
}

namespace storage {
class ObSavedStorageInfo;

struct ObRecoverPoint {
public:
  OB_UNIS_VERSION(1);

public:
  int64_t snapshot_version_;
  uint64_t recover_log_id_;
  int64_t checksum_;
  int64_t epoch_id_;
  int64_t submit_timestamp_;

  ObRecoverPoint()
      : snapshot_version_(0), recover_log_id_(common::OB_INVALID_ID), checksum_(0), epoch_id_(0), submit_timestamp_(0)
  {}
  ObRecoverPoint(const int64_t snapshot_version, const uint64_t recover_id, const int64_t checksum,
      const int64_t epoch_id, const int64_t submit_timestamp)
      : snapshot_version_(snapshot_version),
        recover_log_id_(recover_id),
        checksum_(checksum),
        epoch_id_(epoch_id),
        submit_timestamp_(submit_timestamp)
  {}
  void set(const int64_t snapshot_version, const uint64_t recover_id, const int64_t checksum, const int64_t epoch_id,
      const int64_t submit_timestamp)
  {
    snapshot_version_ = snapshot_version;
    recover_log_id_ = recover_id;
    checksum_ = checksum;
    epoch_id_ = epoch_id;
    submit_timestamp_ = submit_timestamp;
  }
  void reset()
  {
    snapshot_version_ = 0;
    recover_log_id_ = common::OB_INVALID_ID;
    checksum_ = 0;
    epoch_id_ = 0;
    submit_timestamp_ = 0;
  }
  bool operator<(const ObRecoverPoint& other)
  {
    return snapshot_version_ < other.snapshot_version_;
  }

  TO_STRING_KV(K(snapshot_version_), K(recover_log_id_), K(checksum_), K_(epoch_id), K_(submit_timestamp));
};

class ObRecoverVec {
public:
  OB_UNIS_VERSION(1);

public:
  static const int64_t OB_RECOVER_ARRAY_COUNT = 16;
  typedef common::ObSEArray<ObRecoverPoint, OB_RECOVER_ARRAY_COUNT> RecoverVec;
  typedef common::ObSEArray<ObRecoverPoint, OB_RECOVER_ARRAY_COUNT>::iterator RecoverIter;
  int add_recover_point(const ObRecoverPoint& point);
  int gc_recover_points(const int64_t gc_snapshot_version, const int64_t backup_version, bool& changed);
  int get_lower_bound_point(const int64_t snapshot_version, ObRecoverPoint& point);
  int get_lower_bound_point_(const int64_t snapshot_version, int64_t& id);
  int record_major_recover_point(const int64_t prev_version, const int64_t version, bool& changed);
  int assign_recover_points(const ObRecoverVec& vec);
  int reboot_recover_point(const ObRecoverPoint& info);
  bool can_gc_recover_point_(const int64_t gc_snapshot_version, const int64_t backup_version);

  int clear_recover_points_for_physical_flashback(const int64_t version, const ObRecoverPoint& point);
  void reset()
  {
    common::ObSpinLockGuard guard(lock_);
    recover_vec_.reset();
  }
  int assign(const ObRecoverVec& vec)
  {
    common::ObSpinLockGuard guard(lock_);
    return recover_vec_.assign(vec.recover_vec_);
  }
  int64_t count()
  {
    common::ObSpinLockGuard guard(lock_);
    return recover_vec_.count();
  }
  TO_STRING_KV(K(recover_vec_));

private:
  int record_major_recover_point_(const int64_t prev_version, const int64_t version);
  int add_recover_point_(const ObRecoverPoint& point);

private:
  common::ObSpinLock lock_;
  RecoverVec recover_vec_;
};

class ObSavedStorageInfoV2 {
public:
  ObSavedStorageInfoV2()
      : version_(STORAGE_INFO_VERSION_V3), clog_info_(), data_info_(), pg_file_id_(common::OB_INVALID_DATA_FILE_ID)
  {}
  ~ObSavedStorageInfoV2()
  {}

  bool is_valid() const;
  void reset();
  int convert(const ObSavedStorageInfo& save_storage_info);    // Convert the old structure, used by 2.0 compatible
                                                               // debugging environment
  int deep_copy(const ObSavedStorageInfo& save_storage_info);  // For 1.4 Compatibility Problem
  int deep_copy(const ObSavedStorageInfoV2& save_storage_info);

  int set_clog_info(const common::ObBaseStorageInfo& base_storage_info);
  common::ObBaseStorageInfo& get_clog_info();
  const common::ObBaseStorageInfo& get_clog_info() const;
  int set_data_info(const ObDataStorageInfo& data_storage_info);
  ObDataStorageInfo& get_data_info();
  const ObDataStorageInfo& get_data_info() const;

  // Timeout should be set to prevent thread from hanging under concurrent conditions, if meta lock is not held.
  int update_last_replay_log_info(const common::ObPartitionKey& pkey, const bool replica_with_data,
      const common::ObBaseStorageInfo& old_clog_info, const int64_t timeout, const bool log_info_usable);
  int update_and_fetch_log_info(const common::ObPartitionKey& pkey, const bool replica_with_data,
      const common::ObBaseStorageInfo& old_clog_info, const int64_t timeout, const bool log_info_usable);
  void set_pg_file_id(const int64_t pg_file_id)
  {
    pg_file_id_ = pg_file_id;
  }
  int64_t get_pg_file_id() const
  {
    return pg_file_id_;
  }
  // This function is called by physical flashback.
  // Unnecessary recovery points will be cleaned up after physical flashback to prevent
  // these points to be used by backup& restore process.
  int clear_recover_points_for_physical_flashback(const int64_t version, const ObRecoverPoint& point);

  TO_STRING_KV("clog_info", clog_info_, "data_info", data_info_, K_(pg_file_id));
  OB_UNIS_VERSION(1);

private:
  int query_log_info_with_log_id(const common::ObPartitionKey& pkey, const int64_t log_id, const int64_t timeout,
      int64_t& accum_checksum, int64_t& submit_timestamp, int64_t& epoch_id);
  int update_last_replay_log_info_(const common::ObPartitionKey& pkey, const bool replica_with_data,
      const common::ObBaseStorageInfo& old_clog_info, const int64_t timeout, const bool log_info_usable);
  int get_last_replay_log_info_(const common::ObPartitionKey& pkey, const int64_t timeout, ObRecoverPoint& point);

private:
  static const int16_t STORAGE_INFO_VERSION_V1 = 1;
  static const int16_t STORAGE_INFO_VERSION_V2 = 2;
  static const int16_t STORAGE_INFO_VERSION_V3 = 3;

  int16_t version_;
  // Persistence information of clog.
  common::ObBaseStorageInfo clog_info_;
  // Persistence information of data.
  ObDataStorageInfo data_info_;
  // for ofs mode
  int64_t pg_file_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSavedStorageInfoV2);
};

inline bool ObSavedStorageInfoV2::is_valid() const
{
  return (clog_info_.is_valid() || data_info_.is_valid());
}

inline int ObSavedStorageInfoV2::set_clog_info(const common::ObBaseStorageInfo& base_storage_info)
{
  return clog_info_.deep_copy(base_storage_info);
}

inline common::ObBaseStorageInfo& ObSavedStorageInfoV2::get_clog_info()
{
  return clog_info_;
}

inline const common::ObBaseStorageInfo& ObSavedStorageInfoV2::get_clog_info() const
{
  return clog_info_;
}

inline ObDataStorageInfo& ObSavedStorageInfoV2::get_data_info()
{
  return data_info_;
}

inline const ObDataStorageInfo& ObSavedStorageInfoV2::get_data_info() const
{
  return data_info_;
}

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_SAVED_STORAGE_INFO_V2_
