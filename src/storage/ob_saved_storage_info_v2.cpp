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

#include <algorithm>

#include "storage/ob_saved_storage_info_v2.h"
#include "common/ob_partition_key.h"
#include "share/backup/ob_backup_info_mgr.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_saved_storage_info.h"

namespace oceanbase {

using namespace common;
using namespace share;

namespace storage {

OB_DEF_SERIALIZE(ObSavedStorageInfoV2)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, STORAGE_INFO_VERSION_V3, clog_info_, data_info_, pg_file_id_);

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSavedStorageInfoV2)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, STORAGE_INFO_VERSION_V3, clog_info_, data_info_, pg_file_id_);
  return len;
}

OB_DEF_DESERIALIZE(ObSavedStorageInfoV2)
{
  int ret = OB_SUCCESS;

  int64_t tmp_pos = pos;
  int16_t version = 0;

  if (OB_ISNULL(buf) || data_len <= 0 || pos < 0 || pos >= data_len) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode(buf, data_len, tmp_pos, version))) {
    STORAGE_LOG(WARN, "deserialize version failed.", K(ret));
  } else if (STORAGE_INFO_VERSION_V1 == version) {
    ret = OB_NOT_SUPPORTED;
  } else if (STORAGE_INFO_VERSION_V2 == version) {
    ObRecoverVec tmp_recover_vec;
    bool tmp_from_14x = false;
    ObVersion tmp_version(STORAGE_INFO_VERSION_V3, 0);
    LST_DO_CODE(
        OB_UNIS_DECODE, version_, clog_info_, data_info_, tmp_from_14x, tmp_version, tmp_recover_vec, pg_file_id_);
  } else if (STORAGE_INFO_VERSION_V3 == version) {
    LST_DO_CODE(OB_UNIS_DECODE, version_, clog_info_, data_info_, pg_file_id_);
  } else {
    ret = OB_DESERIALIZE_ERROR;
    STORAGE_LOG(ERROR, "version not recognized", K(ret), K(version));
  }

  return ret;
}

int ObSavedStorageInfoV2::deep_copy(const ObSavedStorageInfoV2& save_storage_info)
{
  int ret = OB_SUCCESS;

  if (!save_storage_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(save_storage_info));
  } else if (OB_FAIL(set_clog_info(save_storage_info.clog_info_))) {
    STORAGE_LOG(WARN, "base storage info copy failed", K(ret));
  } else if (OB_FAIL(set_data_info(save_storage_info.data_info_))) {
    STORAGE_LOG(WARN, "data storage info copy failed", K(ret));
  }

  if (OB_INVALID_DATA_FILE_ID == pg_file_id_) {
    pg_file_id_ = save_storage_info.pg_file_id_;
  }

  return ret;
}

int ObSavedStorageInfoV2::convert(const ObSavedStorageInfo& save_storage_info)
{
  int ret = OB_SUCCESS;

  if (!save_storage_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(save_storage_info));
  } else if (OB_FAIL(set_clog_info(save_storage_info))) {
    STORAGE_LOG(WARN, "base storage info copy failed", K(ret));
  } else {
    ObDataStorageInfo tmp_info;
    tmp_info.set_last_replay_log_id(save_storage_info.get_last_replay_log_id());
    tmp_info.set_publish_version(save_storage_info.get_publish_version());
    tmp_info.set_schema_version(save_storage_info.get_schema_version());

    if (OB_FAIL(set_data_info(tmp_info))) {
      STORAGE_LOG(WARN, "data storage info copy failed", K(ret));
    }
  }

  return ret;
}

int ObSavedStorageInfoV2::deep_copy(const ObSavedStorageInfo& save_storage_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(convert(save_storage_info))) {
    STORAGE_LOG(WARN, "fail to convert V1 storage info", K(ret));
  }

  return ret;
}

void ObSavedStorageInfoV2::reset()
{
  version_ = STORAGE_INFO_VERSION_V3;
  clog_info_.reset();
  data_info_.reset();
  pg_file_id_ = OB_INVALID_DATA_FILE_ID;
}

int ObSavedStorageInfoV2::set_data_info(const ObDataStorageInfo& data_storage_info)
{
  int ret = OB_SUCCESS;
  if (!data_storage_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(data_storage_info));
  } else {
    data_info_ = data_storage_info;
  }

  return ret;
}

int ObSavedStorageInfoV2::query_log_info_with_log_id(const ObPartitionKey& pkey, const int64_t log_id,
    const int64_t timeout, int64_t& accum_checksum, int64_t& submit_timestamp, int64_t& epoch_id)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();

  if (OB_FAIL(ObPartitionService::get_instance().query_log_info_with_log_id(
          pkey, log_id, timeout, accum_checksum, submit_timestamp, epoch_id))) {
    STORAGE_LOG(WARN, "failed to query log info", K(ret), K(pkey), K(log_id));
  }

  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;

  if (cost_ts > 1000 * 1000 /*1s*/) {
    STORAGE_LOG(WARN, "query_log_info_with_log_id cost too much time", K(pkey), K(log_id), K(cost_ts));
  } else {
    STORAGE_LOG(INFO, "query_log_info_with_log_id cost time", K(pkey), K(log_id), K(cost_ts));
  }

  return ret;
}

int ObSavedStorageInfoV2::get_last_replay_log_info_(
    const ObPartitionKey& pkey, const int64_t timeout, ObRecoverPoint& point)
{
  int ret = OB_SUCCESS;
  int64_t submit_timestamp = 0;
  int64_t epoch_id = 0;
  int64_t checksum = 0;

  if (0 == data_info_.get_last_replay_log_id()) {
    checksum = 0;
  } else if (data_info_.get_last_replay_log_id() < clog_info_.get_last_replay_log_id()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "filter log id is smaller than last replay log id", K(*this), K(ret));
  } else if (data_info_.get_last_replay_log_id() == clog_info_.get_last_replay_log_id()) {
    checksum = clog_info_.get_accumulate_checksum();
  } else if (OB_FAIL(query_log_info_with_log_id(
                 pkey, data_info_.get_last_replay_log_id(), timeout, checksum, submit_timestamp, epoch_id))) {
    STORAGE_LOG(WARN, "failed to query accum checksum", K(ret), K(pkey), K(*this));
  } else {
    point.set(
        data_info_.get_publish_version(), data_info_.get_last_replay_log_id(), checksum, epoch_id, submit_timestamp);
  }

  return ret;
}

int ObSavedStorageInfoV2::update_last_replay_log_info_(const ObPartitionKey& pkey, const bool replica_with_data,
    const ObBaseStorageInfo& old_clog_info, const int64_t timeout, const bool log_info_usable)
{
  int ret = OB_SUCCESS;

  int64_t accum_checksum = 0;
  int64_t submit_timestamp = 0;
  int64_t epoch_id = 0;
  bool can_skip_query = log_info_usable;

  if (replica_with_data && data_info_.get_last_replay_log_id() < clog_info_.get_last_replay_log_id()) {
    clog_info_.set_last_replay_log_id(data_info_.get_last_replay_log_id());
    can_skip_query = false;
  }

  if (can_skip_query) {
    STORAGE_LOG(INFO, "clog info unchanged", K(clog_info_));
  } else if (clog_info_.get_last_replay_log_id() <= old_clog_info.get_last_replay_log_id()) {
    // For the case that the log id clog info may be recycled, the clog info of memtable dump
    // point is used. In addition, it should be guaranteed that the last_replay_log_id of clog info
    // is smaller than data info.
    if (replica_with_data && data_info_.get_last_replay_log_id() < old_clog_info.get_last_replay_log_id()) {
      ret = OB_EAGAIN;
      STORAGE_LOG(WARN, "data info too old, try again", K(ret), K(pkey), K(old_clog_info), K(*this));
    } else {
      STORAGE_LOG(INFO, "use saved clog info", K(clog_info_), K(old_clog_info));
      if (OB_FAIL(set_clog_info(old_clog_info))) {
        STORAGE_LOG(WARN, "base storage info copy failed", K(ret));
      }
    }
  } else if (OB_FAIL(query_log_info_with_log_id(
                 pkey, clog_info_.get_last_replay_log_id(), timeout, accum_checksum, submit_timestamp, epoch_id))) {
    STORAGE_LOG(WARN, "failed to query accum checksum", K(ret), K(pkey), K(*this));
  } else {
    clog_info_.set_accumulate_checksum(accum_checksum);
    clog_info_.set_submit_timestamp(submit_timestamp);
    clog_info_.set_epoch_id(epoch_id);
  }

  return ret;
}

int ObSavedStorageInfoV2::update_last_replay_log_info(const ObPartitionKey& pkey, const bool replica_with_data,
    const ObBaseStorageInfo& old_clog_info, const int64_t timeout, const bool log_info_usable)
{
  return update_last_replay_log_info_(pkey, replica_with_data, old_clog_info, timeout, log_info_usable);
}

int ObSavedStorageInfoV2::update_and_fetch_log_info(const ObPartitionKey& pkey, const bool replica_with_data,
    const ObBaseStorageInfo& old_clog_info, const int64_t timeout, const bool log_info_usable)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(update_last_replay_log_info_(pkey, replica_with_data, old_clog_info, timeout, log_info_usable))) {
    STORAGE_LOG(WARN, "failed to update log info", K(ret), K(pkey), K(old_clog_info));
  }

  return ret;
}

int ObSavedStorageInfoV2::clear_recover_points_for_physical_flashback(const int64_t, const ObRecoverPoint&)
{
  return OB_NOT_IMPLEMENT;
}

OB_SERIALIZE_MEMBER(ObRecoverPoint, snapshot_version_, recover_log_id_, checksum_, epoch_id_, submit_timestamp_);

OB_SERIALIZE_MEMBER(ObRecoverVec, recover_vec_);

int ObRecoverVec::add_recover_point_(const ObRecoverPoint& point)
{
  int ret = OB_SUCCESS;

  if (recover_vec_.count() > 0 && point.snapshot_version_ <= recover_vec_[recover_vec_.count() - 1].snapshot_version_) {
    STORAGE_LOG(INFO, "add recover point with lower recover point", K(ret), K(point), K(recover_vec_));
    ret = OB_SUCCESS;
  } else if (OB_FAIL(recover_vec_.push_back(point))) {
    STORAGE_LOG(WARN, "push back to vec failed", K(ret), K(point));
  }

  return ret;
}

int ObRecoverVec::add_recover_point(const ObRecoverPoint& point)
{
  ObLockGuard<ObSpinLock> lock_guard(lock_);

  return add_recover_point_(point);
}

int ObRecoverVec::reboot_recover_point(const ObRecoverPoint& point)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> lock_guard(lock_);

  if (0 == recover_vec_.count() && OB_FAIL(add_recover_point_(point))) {
    STORAGE_LOG(WARN, "reboot recover point failed", K(point), K(ret), K(*this));
  }

  return ret;
}

int ObRecoverVec::gc_recover_points(const int64_t gc_snapshot_version, const int64_t kept_backup_version, bool& changed)
{
  int ret = OB_SUCCESS;
  changed = false;

  ObLockGuard<ObSpinLock> lock_guard(lock_);

  while (OB_SUCC(ret) && can_gc_recover_point_(gc_snapshot_version, kept_backup_version)) {
    ObRecoverPoint point = recover_vec_[0];
    if (OB_FAIL(recover_vec_.remove(0))) {
      STORAGE_LOG(WARN,
          "gc recover points fail",
          K(ret),
          K(gc_snapshot_version),
          K(recover_vec_),
          K(kept_backup_version),
          K(point));
    } else {
      STORAGE_LOG(INFO,
          "gc recover points succeed",
          K(ret),
          K(gc_snapshot_version),
          K(recover_vec_),
          K(kept_backup_version),
          K(point));
      changed = true;
    }
  }

  return ret;
}

int ObRecoverVec::clear_recover_points_for_physical_flashback(const int64_t version, const ObRecoverPoint& point)
{
  int ret = OB_SUCCESS;
  int size = 0;

  ObLockGuard<ObSpinLock> lock_guard(lock_);
  size = recover_vec_.count();

  for (int64_t i = size - 1; i >= 0 && OB_SUCC(ret) && recover_vec_[i].snapshot_version_ > version; i--) {
    if (OB_FAIL(recover_vec_.remove(i))) {
      STORAGE_LOG(ERROR, "remove recover points failed", K(ret), K(version), K(recover_vec_), K(point));
    }
  }

  if (0 == recover_vec_.count() && OB_FAIL(add_recover_point_(point))) {
    STORAGE_LOG(ERROR, "remove recover points failed", K(ret), K(version), K(recover_vec_), K(point));
  }

  return ret;
}

bool ObRecoverVec::can_gc_recover_point_(const int64_t gc_snapshot_version, const int64_t kept_backup_version)
{
  // There must be at least one recovery point in the vector.
  return recover_vec_.count() > 1 &&
         // If the snapshot_version of recover point is not greater than gc point.
         recover_vec_[0].snapshot_version_ <= gc_snapshot_version &&
         // There must be at least one recovery point's snapshot_version is not greater than backup.
         (kept_backup_version == 0 || recover_vec_[1].snapshot_version_ <= kept_backup_version);
}

int ObRecoverVec::get_lower_bound_point(const int64_t snapshot_version, ObRecoverPoint& point)
{
  int ret = OB_SUCCESS;
  int64_t id = 0;
  ObLockGuard<ObSpinLock> lock_guard(lock_);

  if (OB_FAIL(get_lower_bound_point_(snapshot_version, id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(ERROR, "get lower bound point error", K(snapshot_version));
    } else {
      STORAGE_LOG(INFO, "get lower bound point not exist", K(snapshot_version));
    }
  } else {
    point = recover_vec_[id];
  }

  return ret;
}

int ObRecoverVec::get_lower_bound_point_(const int64_t snapshot_version, int64_t& id)
{
  int ret = OB_SUCCESS;
  ObRecoverPoint tmp_point(snapshot_version, 0, 0, 0, 0);
  RecoverIter iter;
  if (recover_vec_.count() == 0) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (recover_vec_.end() == (iter = std::lower_bound(recover_vec_.begin(), recover_vec_.end(), tmp_point))) {
    id = recover_vec_.count() - 1;
  } else if (iter->snapshot_version_ == snapshot_version) {
    id = iter - recover_vec_.begin();
  } else if (recover_vec_.begin() == iter) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    id = iter - recover_vec_.begin() - 1;
  }

  return ret;
}

int ObRecoverVec::record_major_recover_point(const int64_t prev_version, const int64_t version, bool& changed)
{
  int ret = OB_SUCCESS;
  int64_t id = 0;
  changed = false;

  ObLockGuard<ObSpinLock> lock_guard(lock_);
  if (OB_FAIL(get_lower_bound_point_(version, id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(ERROR, "get lower bound point error", K(version));
    } else {
      STORAGE_LOG(INFO, "get lower bound point not exist", K(version));
    }
  } else {
    for (int64_t i = id - 1; i >= 0 && OB_SUCC(ret) && recover_vec_[i].snapshot_version_ > prev_version; i--) {
      if (OB_FAIL(recover_vec_.remove(i))) {
        STORAGE_LOG(WARN, "gc recover points failed", K(ret), K(prev_version), K(version), K(recover_vec_));
      } else {
        changed = true;
      }
    }

    if (OB_FAIL(ret)) {
      STORAGE_LOG(WARN, "record major recover point failed", K(ret), K(prev_version), K(version), K(recover_vec_));
    } else {
      if (changed) {
        STORAGE_LOG(INFO, "record major recover point success", K(prev_version), K(version), K(recover_vec_));
      }
    }
  }

  return ret;
}

int ObRecoverVec::assign_recover_points(const ObRecoverVec& vec)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> lock_guard(lock_);

  recover_vec_.reset();
  if (OB_FAIL(recover_vec_.assign(vec.recover_vec_))) {
    STORAGE_LOG(WARN, "assign vec failed", K(recover_vec_), K(vec));
  }

  return ret;
}

}  // namespace storage
}  // namespace oceanbase
