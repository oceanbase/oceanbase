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

#include "lib/ob_define.h"
#include "palf_base_info.h"

namespace oceanbase
{
namespace palf
{

LogInfo::LogInfo()
    : version_(-1),
      log_id_(OB_INVALID_LOG_ID),
      lsn_(),
      scn_(),
      log_proposal_id_(INVALID_PROPOSAL_ID),
      accum_checksum_(-1)
{}

LogInfo::~LogInfo()
{
  reset();
}

void LogInfo::reset()
{
  version_ = -1;
  log_id_ = OB_INVALID_LOG_ID;
  lsn_.reset();
  scn_.reset();
  log_proposal_id_ = INVALID_PROPOSAL_ID;
  accum_checksum_ = -1;
}

void LogInfo::operator=(const LogInfo &log_info)
{
  this->version_ = log_info.version_;
  this->log_id_ = log_info.log_id_;
  this->lsn_ = log_info.lsn_;
  this->scn_ = log_info.scn_;
  this->log_proposal_id_ = log_info.log_proposal_id_;
  this->accum_checksum_ = log_info.accum_checksum_;
}


bool LogInfo::operator!=(const LogInfo &log_info) const
{
  return false == (*this == log_info);
}

bool LogInfo::operator==(const LogInfo &log_info) const
{
  return log_id_ == log_info.log_id_ &&
         lsn_ == log_info.lsn_ &&
         scn_ == log_info.scn_ &&
         log_proposal_id_ == log_info.log_proposal_id_ &&
         accum_checksum_ == log_info.accum_checksum_;
}

bool LogInfo::is_valid() const
{
  return (lsn_.is_valid());
}

void LogInfo::generate_by_default()
{
  reset();
  version_ = LOG_INFO_VERSION;
  log_id_ = 0;
  LSN default_prev_lsn(PALF_INITIAL_LSN_VAL);
  lsn_ = default_prev_lsn;
  scn_.set_min();
  log_proposal_id_ = INVALID_PROPOSAL_ID;
  accum_checksum_ = -1;
}

OB_SERIALIZE_MEMBER(LogInfo, version_, log_id_, scn_, lsn_, log_proposal_id_, accum_checksum_);

PalfBaseInfo::PalfBaseInfo()
    : version_(-1),
      prev_log_info_(),
      curr_lsn_()
{}

PalfBaseInfo::~PalfBaseInfo()
{
  reset();
}

void PalfBaseInfo::reset()
{
  version_ = -1;
  prev_log_info_.reset();
  curr_lsn_.reset();
}

bool PalfBaseInfo::is_valid() const
{
  // NB: prev_log_info_、curr_lsn_预期是有效值，其他字段可能无效(全局第一条日志的场景)
  return (prev_log_info_.is_valid() && curr_lsn_.is_valid() && curr_lsn_ >= prev_log_info_.lsn_);
}

void PalfBaseInfo::operator=(const PalfBaseInfo &base_info)
{
  version_ = base_info.version_;
  prev_log_info_ = base_info.prev_log_info_;
  curr_lsn_ = base_info.curr_lsn_;
}

void PalfBaseInfo::generate_by_default()
{
  reset();
  version_ = PALF_BASE_INFO_VERSION;
  LSN default_prev_lsn(PALF_INITIAL_LSN_VAL);
  prev_log_info_.generate_by_default();
  curr_lsn_ = default_prev_lsn;
}

OB_SERIALIZE_MEMBER(PalfBaseInfo, version_, prev_log_info_, curr_lsn_);

} // end namespace palf
} // end namespace oceanbase
