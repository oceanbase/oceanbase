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

#ifndef OCEANBASE_LOGSERVICE_LOG_META_
#define OCEANBASE_LOGSERVICE_LOG_META_

#include "log_meta_info.h"

namespace oceanbase
{
namespace palf
{
// LogMeta is not a mutil version data strucate, therefore,
// we must discard old message
//
// NB: not thread safe
class LogMeta
{
public:
  LogMeta();
  ~LogMeta();
  LogMeta(const LogMeta &rmeta);

public:
  int generate_by_palf_base_info(const PalfBaseInfo &palf_base_info,
                                 const AccessMode &access_mode,
                                 const LogReplicaType &replica_type);

  int load(const char *buf, int64_t buf_len);
  bool is_valid() const;
  void reset();

  LogPrepareMeta get_log_prepare_meta() const { return log_prepare_meta_; }
  LogConfigMeta get_log_config_meta() const { return log_config_meta_; }
  LogModeMeta get_log_mode_meta() const { return log_mode_meta_; }
  LogSnapshotMeta get_log_snapshot_meta() const { return log_snapshot_meta_; }
  LogReplicaPropertyMeta get_log_replica_property_meta() const { return log_replica_property_meta_; }
  void operator=(const LogMeta &log_meta);

  // The follow functions used to set few fields of this object
  int update_log_prepare_meta(const LogPrepareMeta &log_prepare_meta);
  int update_log_config_meta(const LogConfigMeta &log_config_meta);
  int update_log_snapshot_meta(const LogSnapshotMeta &log_snapshot_meta);
  int update_log_replica_property_meta(const LogReplicaPropertyMeta &log_replica_property_meta);
  int update_log_mode_meta(const LogModeMeta &log_mode_meta);

  TO_STRING_KV(K_(version), K_(log_prepare_meta), K_(log_config_meta), K_(log_snapshot_meta),
      K_(log_replica_property_meta), K_(log_mode_meta));
  NEED_SERIALIZE_AND_DESERIALIZE;

private:
  int64_t version_;
  LogPrepareMeta log_prepare_meta_;
  LogConfigMeta log_config_meta_;
  LogModeMeta log_mode_meta_;
  LogSnapshotMeta log_snapshot_meta_;
  LogReplicaPropertyMeta log_replica_property_meta_;
  static constexpr int64_t LOG_META_VERSION = 1;
};
} // end namespace palf
} // end namespace oceanbase

#endif
