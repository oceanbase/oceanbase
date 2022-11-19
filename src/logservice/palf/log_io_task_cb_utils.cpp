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

#include "log_io_task_cb_utils.h"
#include "lib/ob_define.h"
#include "lsn.h"

namespace oceanbase
{
namespace palf
{
FlushLogCbCtx::FlushLogCbCtx()
    : log_id_(OB_INVALID_LOG_ID),
      log_ts_(OB_INVALID_TIMESTAMP),
      lsn_(),
      log_proposal_id_(INVALID_PROPOSAL_ID),
      total_len_(0),
      curr_proposal_id_(INVALID_PROPOSAL_ID),
      begin_ts_(OB_INVALID_TIMESTAMP)
{
}

FlushLogCbCtx::FlushLogCbCtx(const int64_t log_id, const int64_t log_ts, const LSN &lsn,
                             const int64_t &log_proposal_id, const int64_t total_len,
                             const int64_t &curr_proposal_id, const int64_t begin_ts)
    : log_id_(log_id),
      log_ts_(log_ts),
      lsn_(lsn),
      log_proposal_id_(log_proposal_id),
      total_len_(total_len),
      curr_proposal_id_(curr_proposal_id),
      begin_ts_(begin_ts)
{
}

FlushLogCbCtx::~FlushLogCbCtx()
{
  reset();
}

void FlushLogCbCtx::reset()
{
  log_id_ = OB_INVALID_LOG_ID;
  log_ts_ = OB_INVALID_TIMESTAMP;
  lsn_.reset();
  log_proposal_id_ = INVALID_PROPOSAL_ID;
  total_len_ = 0;
  curr_proposal_id_ = INVALID_PROPOSAL_ID;
  begin_ts_ = OB_INVALID_TIMESTAMP;
}

FlushLogCbCtx& FlushLogCbCtx::operator=(const FlushLogCbCtx &arg)
{
  log_id_ = arg.log_id_;
  log_ts_ = arg.log_ts_;
  lsn_ = arg.lsn_;
  log_proposal_id_ = arg.log_proposal_id_;
  total_len_ = arg.total_len_;
  curr_proposal_id_ = arg.curr_proposal_id_;
  begin_ts_ = arg.begin_ts_;
  return *this;
}

TruncateLogCbCtx::TruncateLogCbCtx()
    : lsn_()
{
}

TruncateLogCbCtx::TruncateLogCbCtx(const LSN &lsn)
    : lsn_(lsn)
{
}

TruncateLogCbCtx::~TruncateLogCbCtx()
{
  reset();
}

void TruncateLogCbCtx::reset()
{
  lsn_.reset();
}

TruncateLogCbCtx& TruncateLogCbCtx::operator=(const TruncateLogCbCtx &arg)
{
  lsn_ = arg.lsn_;
  return *this;
}

FlushMetaCbCtx::FlushMetaCbCtx()
    : type_ (INVALID_META_TYPE),
      proposal_id_(INVALID_PROPOSAL_ID),
      config_version_(),
      base_lsn_(),
      allow_vote_(true),
      log_mode_meta_()
{
}

FlushMetaCbCtx::~FlushMetaCbCtx()
{
  reset();
}

void FlushMetaCbCtx::reset()
{
  type_ = INVALID_META_TYPE;
  proposal_id_ = INVALID_PROPOSAL_ID;
  config_version_.reset();
  base_lsn_.reset();
  allow_vote_ = true;
  log_mode_meta_.reset();
}

FlushMetaCbCtx &FlushMetaCbCtx::operator=(const FlushMetaCbCtx &arg)
{
  this->type_ = arg.type_;
  this->proposal_id_ = arg.proposal_id_;
  this->config_version_ = arg.config_version_;
  this->base_lsn_ = arg.base_lsn_;
  this->allow_vote_ = arg.allow_vote_;
  this->log_mode_meta_ = arg.log_mode_meta_;
  return *this;
}

TruncatePrefixBlocksCbCtx::TruncatePrefixBlocksCbCtx(const LSN &lsn) : lsn_(lsn)
{
}

TruncatePrefixBlocksCbCtx::TruncatePrefixBlocksCbCtx() : lsn_()
{
}

TruncatePrefixBlocksCbCtx::~TruncatePrefixBlocksCbCtx()
{
}

void TruncatePrefixBlocksCbCtx::reset()
{
  lsn_.reset();
}

TruncatePrefixBlocksCbCtx& TruncatePrefixBlocksCbCtx::operator=(const TruncatePrefixBlocksCbCtx& truncate_prefix_blocks_ctx)
{
  lsn_ = truncate_prefix_blocks_ctx.lsn_;
  return *this;
}
} // end of logservice
} // end of oceanbase
