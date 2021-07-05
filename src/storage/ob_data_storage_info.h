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

#ifndef OCEANBASE_STORAGE_OB_DATA_STORAGE_INFO_
#define OCEANBASE_STORAGE_OB_DATA_STORAGE_INFO_

#include <cstdint>

#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_define.h"

namespace oceanbase {
namespace storage {

class ObDataStorageInfo {
public:
  ObDataStorageInfo()
      : last_replay_log_id_(0),
        publish_version_(0),
        schema_version_(0),
        for_filter_log_compat_(0),
        created_by_new_minor_freeze_(false),
        last_replay_log_ts_(0)
  {}

  ~ObDataStorageInfo()
  {}

  bool is_valid() const;
  void reset();
  void reset_for_no_memtable_replica();

  void set_last_replay_log_id(const uint64_t last_replay_log_id);
  uint64_t get_last_replay_log_id() const;
  void set_publish_version(const int64_t publish_version);
  int64_t get_publish_version() const;
  void set_schema_version(const int64_t schema_version);
  int64_t get_schema_version() const;
  void inc_update_schema_version(const int64_t schema_version);
  bool is_created_by_new_minor_freeze() const
  {
    return created_by_new_minor_freeze_;
  }
  void set_created_by_new_minor_freeze()
  {
    created_by_new_minor_freeze_ = true;
  }
  int64_t get_last_replay_log_ts() const;
  void set_last_replay_log_ts(const int64_t last_replay_log_ts);

  TO_STRING_KV("last_replay_log_id", last_replay_log_id_, "last_replay_log_ts", last_replay_log_ts_, "publish_version",
      publish_version_, "schema_version", schema_version_, "created_by_new_minor_freeze", created_by_new_minor_freeze_);
  OB_UNIS_VERSION(1);

private:
  // Log point to start replay.
  uint64_t last_replay_log_id_;
  int64_t publish_version_;
  int64_t schema_version_;
  // In order to be compatible with the filter log id of 225.
  uint64_t for_filter_log_compat_;
  bool created_by_new_minor_freeze_;
  int64_t last_replay_log_ts_;
};

inline bool ObDataStorageInfo::is_valid() const
{
  return (common::is_valid_trans_version(publish_version_) && schema_version_ >= 0);
}

inline void ObDataStorageInfo::set_last_replay_log_id(const uint64_t last_replay_log_id)
{
  last_replay_log_id_ = last_replay_log_id;
}

inline uint64_t ObDataStorageInfo::get_last_replay_log_id() const
{
  return last_replay_log_id_;
}

inline void ObDataStorageInfo::set_publish_version(const int64_t publish_version)
{
  publish_version_ = publish_version;
}

inline int64_t ObDataStorageInfo::get_publish_version() const
{
  return publish_version_;
}

inline void ObDataStorageInfo::set_schema_version(const int64_t schema_version)
{
  schema_version_ = schema_version;
}

inline int64_t ObDataStorageInfo::get_schema_version() const
{
  return schema_version_;
}

inline void ObDataStorageInfo::inc_update_schema_version(const int64_t schema_version)
{
  if (schema_version > schema_version_) {
    schema_version_ = schema_version;
  }
}

inline int64_t ObDataStorageInfo::get_last_replay_log_ts() const
{
  return last_replay_log_ts_;
}

inline void ObDataStorageInfo::set_last_replay_log_ts(const int64_t last_replay_log_ts)
{
  last_replay_log_ts_ = last_replay_log_ts;
}

}  // namespace storage
}  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_DATA_STORAGE_INFO_ */
