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

#ifndef OCEANBASE_STORAGE_OB_SAVED_STORAGE_INFO_
#define OCEANBASE_STORAGE_OB_SAVED_STORAGE_INFO_

#include "storage/ob_base_storage_info.h"
#include "common/ob_range.h"
#include "share/ob_define.h"

namespace oceanbase {
namespace storage {
class ObSavedStorageInfo : public common::ObBaseStorageInfo {
public:
  ObSavedStorageInfo()
      : common::ObBaseStorageInfo(),
        memstore_version_(0),
        publish_version_(0),
        schema_version_(0),
        frozen_version_(0),
        frozen_timestamp_(0)
  {}
  virtual ~ObSavedStorageInfo()
  {}

public:
  void set_memstore_version(const common::ObVersion& version);
  const common::ObVersion& get_memstore_version() const;
  void set_publish_version(const int64_t publish_version);
  int64_t get_publish_version() const;
  void set_schema_version(const int64_t schema_version);
  int64_t get_schema_version() const;
  void set_frozen_version(const common::ObVersion& frozen_version);
  const common::ObVersion& get_frozen_version() const;
  void set_frozen_timestamp(const int64_t frozen_timestamp);
  int64_t get_frozen_timestamp() const;
  void set_replica_num(const int64_t replica_num);
  int deep_copy(const ObSavedStorageInfo& save_storage_info);
  int deep_copy(const common::ObBaseStorageInfo& base_storage_info);
  bool is_valid() const;
  TO_STRING_KV("version", version_, "epoch_id", epoch_id_, "proposal_id", proposal_id_, "last_replay_log_id",
      last_replay_log_id_, "last_submit_timestamp", last_submit_timestamp_, "accumulate_checksum", accumulate_checksum_,
      "replica_num", replica_num_, "membership_timestamp", membership_timestamp_, "membership_log_id",
      membership_log_id_, "curr_member_list", curr_member_list_, "memstore_version", memstore_version_,
      "publish_version", publish_version_, "schema_version", schema_version_, "frozen_version", frozen_version_,
      "frozen_timestamp", frozen_timestamp_);
  OB_UNIS_VERSION(1);

private:
  common::ObVersion memstore_version_;
  int64_t publish_version_;
  int64_t schema_version_;
  common::ObVersion frozen_version_;
  int64_t frozen_timestamp_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSavedStorageInfo);
};

inline void ObSavedStorageInfo::set_replica_num(const int64_t replica_num)
{
  replica_num_ = replica_num;
}

inline void ObSavedStorageInfo::set_memstore_version(const common::ObVersion& version)
{
  memstore_version_ = version;
}

inline const common::ObVersion& ObSavedStorageInfo::get_memstore_version() const
{
  return memstore_version_;
}

inline void ObSavedStorageInfo::set_publish_version(const int64_t publish_version)
{
  publish_version_ = publish_version;
}

inline int64_t ObSavedStorageInfo::get_publish_version() const
{
  return publish_version_;
}

inline void ObSavedStorageInfo::set_schema_version(const int64_t schema_version)
{
  schema_version_ = schema_version;
}

inline int64_t ObSavedStorageInfo::get_schema_version() const
{
  return schema_version_;
}

inline void ObSavedStorageInfo::set_frozen_version(const common::ObVersion& frozen_version)
{
  frozen_version_ = frozen_version;
}

inline const common::ObVersion& ObSavedStorageInfo::get_frozen_version() const
{
  return frozen_version_;
}

inline void ObSavedStorageInfo::set_frozen_timestamp(const int64_t frozen_timestamp)
{
  frozen_timestamp_ = frozen_timestamp;
}

inline int64_t ObSavedStorageInfo::get_frozen_timestamp() const
{
  return frozen_timestamp_;
}

inline bool ObSavedStorageInfo::is_valid() const
{
  return (ObBaseStorageInfo::is_valid() && common::is_valid_trans_version(publish_version_) && schema_version_ >= 0 &&
          frozen_timestamp_ >= 0);
}

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_SAVED_STORAGE_INFO_
