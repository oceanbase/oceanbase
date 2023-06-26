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

#include "log_meta.h"
#include "lib/ob_errno.h"
#include "lsn.h"

namespace oceanbase
{
namespace palf
{
using namespace common;
using namespace share;
LogMeta::LogMeta() : version_(-1),
                     log_prepare_meta_(),
                     log_config_meta_(),
                     log_mode_meta_(),
                     log_snapshot_meta_(),
                     log_replica_property_meta_()
{
}

LogMeta::~LogMeta() { reset(); }

LogMeta::LogMeta(const LogMeta &rmeta) { *this = rmeta; }

int LogMeta::generate_by_palf_base_info(const PalfBaseInfo &palf_base_info,
                                        const AccessMode &access_mode,
                                        const LogReplicaType &replica_type)
{
  int ret = OB_SUCCESS;
  if (false == is_valid_access_mode(access_mode) || false == palf_base_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(INFO, "invalid argument", KPC(this), K(access_mode), K(palf_base_info));
  } else if (OB_FAIL(log_snapshot_meta_.generate(palf_base_info.curr_lsn_, palf_base_info.prev_log_info_))) {
    PALF_LOG(WARN, "generate snapshot_meta failed", K(ret), K(palf_base_info));
  } else {
    const int64_t prev_log_proposal_id = palf_base_info.prev_log_info_.log_proposal_id_;
    const SCN &prev_scn = palf_base_info.prev_log_info_.scn_;
    const int64_t init_log_proposal_id = (prev_log_proposal_id != INVALID_PROPOSAL_ID)? \
        prev_log_proposal_id: PALF_INITIAL_PROPOSAL_ID;
    const SCN init_ref_scn = (prev_scn.is_valid() ? prev_scn: SCN::min_scn());
    LogConfigInfoV2 init_config_info;
    LogConfigVersion init_config_version;
    init_config_version.generate(init_log_proposal_id, 0);
    init_config_info.generate(init_config_version);
    version_ = LOG_META_VERSION;
    log_prepare_meta_.generate(LogVotedFor(), init_log_proposal_id);
    log_config_meta_.generate_for_default(init_log_proposal_id, init_config_info, init_config_info);
    log_mode_meta_.generate(init_log_proposal_id, init_log_proposal_id, access_mode, init_ref_scn);
    const bool allow_vote = (replica_type != ARBITRATION_REPLICA);
    log_replica_property_meta_.generate(allow_vote, replica_type);
    PALF_LOG(INFO, "generate_by_palf_base_info success", KPC(this));
  }
  return ret;
}

int LogMeta::load(const char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (NULL == buf || 0 >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(buf), K(buf_len));
  } else if (OB_FAIL(this->deserialize(buf, buf_len, pos))) {
    PALF_LOG(ERROR, "deserialize failed", K(ret));
  }
  return ret;
}

bool LogMeta::is_valid() const
{
  return true == log_prepare_meta_.is_valid()
         && true == log_config_meta_.is_valid()
         && true == log_mode_meta_.is_valid()
         && true == log_snapshot_meta_.is_valid()
         && true == log_replica_property_meta_.is_valid();
}

void LogMeta::reset()
{
  version_ = -1;
  log_prepare_meta_.reset();
  log_config_meta_.reset();
  log_mode_meta_.reset();
  log_snapshot_meta_.reset();
  log_replica_property_meta_.reset();
  version_ = -1;
}

int LogMeta::update_log_prepare_meta(const LogPrepareMeta &log_prepare_meta)
{
  int ret = OB_SUCCESS;
  if (false == log_prepare_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(log_prepare_meta));
  } else {
    log_prepare_meta_ = log_prepare_meta;
  }
  return ret;
}

int LogMeta::update_log_config_meta(const LogConfigMeta &log_config_meta)
{
  int ret = OB_SUCCESS;
  if (false == log_config_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(log_config_meta));
  } else {
    log_config_meta_ = log_config_meta;
  }
  return ret;
}

int LogMeta::update_log_mode_meta(const LogModeMeta &log_mode_meta)
{
  int ret = OB_SUCCESS;
  if (false == log_mode_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(log_mode_meta));
  } else {
    log_mode_meta_ = log_mode_meta;
  }
  return ret;
}

int LogMeta::update_log_snapshot_meta(const LogSnapshotMeta &log_snapshot_meta)
{
  int ret = OB_SUCCESS;
  if (false == log_snapshot_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(log_snapshot_meta));
  } else {
    log_snapshot_meta_ = log_snapshot_meta;
  }
  return ret;
}

int LogMeta::update_log_replica_property_meta(const LogReplicaPropertyMeta &log_replica_property_meta)
{
  int ret = OB_SUCCESS;
  if (false == log_replica_property_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(log_replica_property_meta));
  } else {
    log_replica_property_meta_ = log_replica_property_meta;
  }
  return ret;
}

void LogMeta::operator=(const LogMeta &log_meta)
{
  version_ = log_meta.version_;
  log_prepare_meta_ = log_meta.log_prepare_meta_;
  log_config_meta_ = log_meta.log_config_meta_;
  log_mode_meta_ = log_meta.log_mode_meta_;
  log_snapshot_meta_ = log_meta.log_snapshot_meta_;
  log_replica_property_meta_ = log_meta.log_replica_property_meta_;
}

DEFINE_SERIALIZE(LogMeta)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf || buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, version_))
             || OB_FAIL(log_prepare_meta_.serialize(buf, buf_len, new_pos))
             || OB_FAIL(log_config_meta_.serialize(buf, buf_len, new_pos))
             || OB_FAIL(log_mode_meta_.serialize(buf, buf_len, new_pos))
             || OB_FAIL(log_snapshot_meta_.serialize(buf, buf_len, new_pos))
             || OB_FAIL(log_replica_property_meta_.serialize(buf, buf_len, new_pos))) {
    PALF_LOG(ERROR, "LogMeta serialize failed", K(ret), K(buf), K(buf_len), K(pos), K(new_pos));
  } else {
    pos = new_pos;
    PALF_LOG(INFO, "LogMeta serialize", K(*this), K(buf), KP(buf), K(pos));
  }
  return ret;
}

DEFINE_DESERIALIZE(LogMeta)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf || data_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &version_))
             || OB_FAIL(log_prepare_meta_.deserialize(buf, data_len, new_pos))
             || OB_FAIL(log_config_meta_.deserialize(buf, data_len, new_pos))
             || OB_FAIL(log_mode_meta_.deserialize(buf, data_len, new_pos))
             || OB_FAIL(log_snapshot_meta_.deserialize(buf, data_len, new_pos))
             || OB_FAIL(log_replica_property_meta_.deserialize(buf, data_len, new_pos))) {
    PALF_LOG(ERROR, "LogMeta deserialize failed", K(ret), K(buf), K(data_len), K(pos), K(new_pos));
  } else {
    PALF_LOG(INFO, "LogMeta deserialize", K(buf), K(buf + pos), K(pos), K(new_pos), K(*this));
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(LogMeta)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(version_);
  size += log_prepare_meta_.get_serialize_size();
  size += log_config_meta_.get_serialize_size();
  size += log_mode_meta_.get_serialize_size();
  size += log_snapshot_meta_.get_serialize_size();
  size += log_replica_property_meta_.get_serialize_size();
  return size;
}
} // end namespace palf
} // end namespace oceanbase
