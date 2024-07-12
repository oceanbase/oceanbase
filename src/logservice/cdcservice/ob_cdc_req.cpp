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

#include "ob_cdc_req.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace obrpc
{
/*
 *
 * Request start LSN by start timestamp.
 *
 */

void ObCdcReqStartLSNByTsReq::LocateParam::reset()
{
  ls_id_.reset();
  start_ts_ns_ = OB_INVALID_TIMESTAMP;
}

void ObCdcReqStartLSNByTsReq::LocateParam::reset(const ObLSID &ls_id, const int64_t start_ts_ns)
{
  ls_id_ = ls_id;
  start_ts_ns_ = start_ts_ns;
}

bool ObCdcReqStartLSNByTsReq::LocateParam::is_valid() const
{
  return ls_id_.is_valid() && (start_ts_ns_ > 0);
}

OB_SERIALIZE_MEMBER(ObCdcReqStartLSNByTsReq::LocateParam, ls_id_, start_ts_ns_);

ObCdcReqStartLSNByTsReq::ObCdcReqStartLSNByTsReq()
    : rpc_ver_(CUR_RPC_VER), params_(), client_id_(), flag_(0)
{ }

ObCdcReqStartLSNByTsReq::~ObCdcReqStartLSNByTsReq()
{
  reset();
}

void ObCdcReqStartLSNByTsReq::reset()
{
  params_.reset();
  flag_ = 0;
}

bool ObCdcReqStartLSNByTsReq::is_valid() const
{
  int64_t count = params_.count();
  bool bool_ret = (count > 0);

  for (int64_t i = 0; bool_ret && i < count; i++) {
    if (!params_[i].ls_id_.is_valid()
        || params_[i].start_ts_ns_ <= 0) {
      bool_ret = false;
    }
  }

  return bool_ret;
}

int ObCdcReqStartLSNByTsReq::set_params(const LocateParamArray &params)
{
  int ret = OB_SUCCESS;

  if (ITEM_CNT_LMT < params.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err set params, buf not enough", K(ret),
               LITERAL_K(ITEM_CNT_LMT),
               "count", params.count());
  } else if (OB_SUCCESS != (ret = params_.assign(params))) {
    EXTLOG_LOG(ERROR, "err assign params", K(ret));
  }

  return ret;
}

int ObCdcReqStartLSNByTsReq::append_param(const LocateParam &param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "invalid param", K(ret), K(param));
  } else if (ITEM_CNT_LMT <= params_.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err append param, buf not enough", K(ret),
               LITERAL_K(ITEM_CNT_LMT),
               "count", params_.count());
  } else if (OB_SUCCESS != (ret = params_.push_back(param))) {
    EXTLOG_LOG(ERROR, "err push back param", K(ret));
  }

  return ret;
}

const ObCdcReqStartLSNByTsReq::LocateParamArray &
ObCdcReqStartLSNByTsReq::get_params() const
{
  return params_;
}

OB_DEF_SERIALIZE(ObCdcReqStartLSNByTsReq)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, params_, client_id_, flag_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObCdcReqStartLSNByTsReq)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, params_, client_id_, flag_);
  return len;
}

OB_DEF_DESERIALIZE(ObCdcReqStartLSNByTsReq)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE, params_, client_id_, flag_);
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match",
               K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return ret;
}

void ObCdcReqStartLSNByTsResp::LocateResult::reset()
{
  err_ = OB_SUCCESS;
  start_lsn_.reset();
  start_ts_ns_ = OB_INVALID_TIMESTAMP;
}

void ObCdcReqStartLSNByTsResp::LocateResult::reset(const int err,
    const LSN start_lsn,
    const int64_t start_ts_ns)
{
  err_ = err;
  start_lsn_ = start_lsn;
  start_ts_ns_ = start_ts_ns;
}

OB_SERIALIZE_MEMBER(ObCdcReqStartLSNByTsResp::LocateResult, err_, start_lsn_, start_ts_ns_);

ObCdcReqStartLSNByTsResp::ObCdcReqStartLSNByTsResp()
    : rpc_ver_(CUR_RPC_VER),
      err_(OB_SUCCESS),
      res_()
{ }

ObCdcReqStartLSNByTsResp::~ObCdcReqStartLSNByTsResp()
{
  reset();
}

void ObCdcReqStartLSNByTsResp::reset()
{
  err_ = OB_SUCCESS;
  res_.reset();
}

int ObCdcReqStartLSNByTsResp::set_results(const LocateResultArray &results)
{
  int ret = OB_SUCCESS;

  if (ITEM_CNT_LMT < results.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err set results, buf not enough", K(ret),
               LITERAL_K(ITEM_CNT_LMT),
               "count", results.count());
  } else if (OB_SUCCESS != (ret = res_.assign(results))) {
    EXTLOG_LOG(ERROR, "err assign results", K(ret));
  }

  return ret;
}

int ObCdcReqStartLSNByTsResp::append_result(const LocateResult &result)
{
  int ret = OB_SUCCESS;

  if (ITEM_CNT_LMT <= res_.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err append result, buf not enough", K(ret),
               LITERAL_K(ITEM_CNT_LMT),
               "count", res_.count());
  } else if (OB_SUCCESS != (ret = res_.push_back(result))) {
    EXTLOG_LOG(ERROR, "err push back result", K(ret));
  }

  return ret;
}

const ObCdcReqStartLSNByTsResp::LocateResultArray &
ObCdcReqStartLSNByTsResp::get_results() const
{
  return res_;
}

OB_DEF_SERIALIZE(ObCdcReqStartLSNByTsResp)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, err_, res_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObCdcReqStartLSNByTsResp)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, err_, res_);
  return len;
}

OB_DEF_DESERIALIZE(ObCdcReqStartLSNByTsResp)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE, err_, res_);
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match",
               K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return ret;
}

/*
 *
 * Fetch Group LogEntry
 *
 */
OB_SERIALIZE_MEMBER(ObCdcLSFetchLogReq, rpc_ver_, ls_id_, start_lsn_, upper_limit_ts_, client_pid_,
                    client_id_, progress_, flag_, compressor_type_, tenant_id_, client_type_);
OB_SERIALIZE_MEMBER(ObCdcFetchStatus,
                    is_reach_max_lsn_,
                    is_reach_upper_limit_ts_,
                    scan_round_count_,
                    l2s_net_time_,
                    svr_queue_time_,
                    log_fetch_time_,
                    ext_process_time_);

OB_DEF_SERIALIZE(ObCdcLSFetchLogResp)
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, err_, debug_err_,
              ls_id_, feedback_type_, fetch_status_, next_req_lsn_,
              log_num_, pos_);

  if (OB_SUCCESS == ret && pos_ > 0) {
    if (buf_len - pos < pos_) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf + pos, log_entry_buf_, pos_);
      pos += pos_;
    }
  }
  LST_DO_CODE(OB_UNIS_ENCODE, server_progress_);

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObCdcLSFetchLogResp)
{
  int64_t len = 0;
  int tmp_ret = OB_SUCCESS;

  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, err_, debug_err_,
                ls_id_, feedback_type_, fetch_status_, next_req_lsn_,
                log_num_, pos_);
    len += pos_;

    LST_DO_CODE(OB_UNIS_ADD_LEN, server_progress_);
  } else {
    tmp_ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG_RET(ERROR, tmp_ret, "get serialize size error, version not match",
               K(tmp_ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }

  return len;
}

OB_DEF_DESERIALIZE(ObCdcLSFetchLogResp)
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE, err_, debug_err_,
                ls_id_, feedback_type_, fetch_status_, next_req_lsn_,
                log_num_, pos_);

    if (OB_UNLIKELY(! is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      EXTLOG_LOG(ERROR, "pos_ is not valid", K(ret), K(pos_));
    } else if (pos_ > 0) {
      MEMCPY(log_entry_buf_, buf + pos, pos_);
      pos += pos_;
    }

    LST_DO_CODE(OB_UNIS_DECODE, server_progress_);
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match",
               K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }

  return ret;
}

void ObCdcLSFetchLogReq::reset()
{
  rpc_ver_ = CUR_RPC_VER;
  ls_id_.reset();
  start_lsn_.reset();
  upper_limit_ts_ = 0;
  client_pid_ = 0;
  client_id_.reset();
  progress_ = OB_INVALID_TIMESTAMP;
  flag_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
  client_type_ = ObCdcClientType::CLIENT_TYPE_UNKNOWN;
}

ObCdcLSFetchLogReq& ObCdcLSFetchLogReq::operator=(const ObCdcLSFetchLogReq &other)
{
  rpc_ver_ = other.rpc_ver_;
  ls_id_ = other.ls_id_;
  start_lsn_ = other.start_lsn_;
  upper_limit_ts_ = other.upper_limit_ts_;
  client_pid_ = other.client_pid_;
  client_id_ = other.client_id_;
  progress_ = other.progress_;
  flag_ = other.flag_;
  tenant_id_ = other.tenant_id_;
  compressor_type_ = other.compressor_type_;
  client_type_ = other.client_type_;
  return *this;
}

bool ObCdcLSFetchLogReq::operator==(const ObCdcLSFetchLogReq &that) const
{
  return rpc_ver_ == that.rpc_ver_
    && ls_id_ == that.ls_id_
    && start_lsn_ == that.start_lsn_
    && upper_limit_ts_ == that.upper_limit_ts_
    && client_pid_ == that.client_pid_
    && client_id_ == that.client_id_
    && progress_ == that.progress_
    && flag_ == that.flag_
    && tenant_id_ == that.tenant_id_
    && compressor_type_ == that.compressor_type_
    && client_type_ == that.client_type_;
}

bool ObCdcLSFetchLogReq::operator!=(const ObCdcLSFetchLogReq &that) const
{
  return ! (*this == that);
}

void ObCdcLSFetchLogReq::reset(const ObLSID ls_id,
    const LSN start_lsn,
    const int64_t upper_limit)
{
  reset();

  ls_id_ = ls_id;
  start_lsn_ = start_lsn;
  upper_limit_ts_ = upper_limit;
}

bool ObCdcLSFetchLogReq::is_valid() const
{
  return ls_id_.is_valid()
    && start_lsn_.is_valid()
    && upper_limit_ts_ > 0
    && common::OB_INVALID_TIMESTAMP != upper_limit_ts_;
}

int ObCdcLSFetchLogReq::set_upper_limit_ts(const int64_t ts)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ts <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    upper_limit_ts_ = ts;
  }

  return ret;
}

/*
int ObCdcRespBuf::append_log_group_entry(const LogGroupEntry &entry)
{
  int ret = OB_SUCCESS;
  int64_t old_pos = pos_;

  if (OB_FAIL(entry.serialize(log_entry_buf_, FETCH_BUF_LEN, pos_))) {
    ret = OB_BUF_NOT_ENOUGH;
    pos_ = old_pos;
  } else {
    log_num_++;
  }

  return ret;
}

int ObCdcRespBuf::append_log_entry(const LogEntry &entry)
{
  int ret = OB_SUCCESS;
  int64_t old_pos = pos_;

  if (OB_FAIL(entry.serialize(log_entry_buf_, FETCH_BUF_LEN, pos_))) {
    ret = OB_BUF_NOT_ENOUGH;
    pos_ = old_pos;
  } else {
    log_num_++;
  }

  return ret;
}
*/

int ObCdcLSFetchLogResp::assign(const ObCdcLSFetchLogResp &other)
{
  int ret = OB_SUCCESS;

  if (this != &other) {
    rpc_ver_ = other.rpc_ver_;
    err_ = other.err_;
    debug_err_ = other.debug_err_;
    ls_id_ = other.ls_id_;
    feedback_type_ = other.feedback_type_;
    fetch_status_ = other.fetch_status_;
    next_req_lsn_ = other.next_req_lsn_;
    log_num_ = other.log_num_;
    pos_ = other.pos_;
    log_entry_buf_[0] = '\0';
    server_progress_ = OB_INVALID_TIMESTAMP;

    if (log_num_ > 0 && pos_ > 0) {
      (void)MEMCPY(log_entry_buf_, other.log_entry_buf_, pos_);
    }
  }

  return ret;
}

void ObCdcLSFetchLogResp::reset()
{
  rpc_ver_ = CUR_RPC_VER;
  err_ = common::OB_NOT_INIT;
  debug_err_ = common::OB_NOT_INIT;
  ls_id_.reset();
  feedback_type_ = FeedbackType::INVALID_FEEDBACK;
  fetch_status_.reset();
  next_req_lsn_.reset();
  log_num_ = 0;
  pos_ = 0;
  log_entry_buf_[0] = '\0';
  server_progress_ = OB_INVALID_TIMESTAMP;
}

/*
 *
 * Fetch Missing LogEntry
 *
 */
OB_SERIALIZE_MEMBER(ObCdcLSFetchMissLogReq::MissLogParam, miss_lsn_);
OB_SERIALIZE_MEMBER(ObCdcLSFetchMissLogReq, rpc_ver_, ls_id_, miss_log_array_,
                    client_pid_, client_id_, flag_, compressor_type_, tenant_id_, progress_);

void ObCdcLSFetchMissLogReq::reset()
{
  rpc_ver_ = CUR_RPC_VER;
  ls_id_.reset();
  miss_log_array_.reset();
  client_pid_ = 0;
  client_id_.reset();
  flag_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  compressor_type_ = common::ObCompressorType::INVALID_COMPRESSOR;
  progress_ = OB_INVALID_TIMESTAMP;
}

int ObCdcLSFetchMissLogReq::append_miss_log(const MissLogParam &param)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(miss_log_array_.push_back(param))) {
    EXTLOG_LOG(WARN, "miss_log_array_ push_back fail", KR(ret), K(param));
  }

  return ret;
}

} // obrpc
} // namespace oceanbase
