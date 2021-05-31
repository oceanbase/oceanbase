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

#include "ob_log_external_rpc.h"
#include "ob_clog_mgr.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace common;
using namespace clog;
using namespace storage;
namespace obrpc {

OB_SERIALIZE_MEMBER(BreakInfo, break_file_id_, min_greater_log_id_);

// Request start log id by start timestamp.
void ObLogReqStartLogIdByTsRequest::Param::reset()
{
  pkey_.reset();
  start_tstamp_ = OB_INVALID_TIMESTAMP;
}

OB_SERIALIZE_MEMBER(ObLogReqStartLogIdByTsRequest::Param, pkey_, start_tstamp_);

ObLogReqStartLogIdByTsRequest::ObLogReqStartLogIdByTsRequest() : rpc_ver_(CUR_RPC_VER), params_()
{}

ObLogReqStartLogIdByTsRequest::~ObLogReqStartLogIdByTsRequest()
{
  reset();
}

void ObLogReqStartLogIdByTsRequest::reset()
{
  params_.reset();
}

bool ObLogReqStartLogIdByTsRequest::is_valid() const
{
  int64_t count = params_.count();
  bool bool_ret = (count > 0);
  for (int64_t i = 0; bool_ret && i < count; i++) {
    if (!params_[i].pkey_.is_valid() || params_[i].start_tstamp_ <= 0) {
      bool_ret = false;
    }
  }
  return bool_ret;
}

int ObLogReqStartLogIdByTsRequest::set_params(const ParamArray& params)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT < params.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err set params, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", params.count());
  } else if (OB_SUCCESS != (ret = params_.assign(params))) {
    EXTLOG_LOG(ERROR, "err assign params", K(ret));
  }
  return ret;
}

int ObLogReqStartLogIdByTsRequest::append_param(const Param& param)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT <= params_.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err append param, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", params_.count());

  } else if (OB_SUCCESS != (ret = params_.push_back(param))) {
    EXTLOG_LOG(ERROR, "err push back param", K(ret));
  }
  return ret;
}

const ObLogReqStartLogIdByTsRequest::ParamArray& ObLogReqStartLogIdByTsRequest::get_params() const
{
  return params_;
}

int64_t ObLogReqStartLogIdByTsRequest::rpc_ver() const
{
  return rpc_ver_;
}

OB_DEF_SERIALIZE(ObLogReqStartLogIdByTsRequest)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, params_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogReqStartLogIdByTsRequest)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, params_);
  return len;
}

OB_DEF_DESERIALIZE(ObLogReqStartLogIdByTsRequest)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE, params_);
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match", K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return ret;
}

void ObLogReqStartLogIdByTsResponse::Result::reset()
{
  err_ = OB_SUCCESS;
  start_log_id_ = OB_INVALID_ID;
  predict_ = false;
}

OB_SERIALIZE_MEMBER(ObLogReqStartLogIdByTsResponse::Result, err_, start_log_id_, predict_);

ObLogReqStartLogIdByTsResponse::ObLogReqStartLogIdByTsResponse() : rpc_ver_(CUR_RPC_VER), err_(OB_SUCCESS), res_()
{}

ObLogReqStartLogIdByTsResponse::~ObLogReqStartLogIdByTsResponse()
{
  reset();
}

void ObLogReqStartLogIdByTsResponse::reset()
{
  err_ = OB_SUCCESS;
  res_.reset();
}

void ObLogReqStartLogIdByTsResponse::set_err(const int err)
{
  err_ = err;
}

int ObLogReqStartLogIdByTsResponse::set_results(const ResultArray& results)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT < results.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err set results, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", results.count());
  } else if (OB_SUCCESS != (ret = res_.assign(results))) {
    EXTLOG_LOG(ERROR, "err assign results", K(ret));
  }
  return ret;
}

int ObLogReqStartLogIdByTsResponse::append_result(const Result& result)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT <= res_.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err append result, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", res_.count());
  } else if (OB_SUCCESS != (ret = res_.push_back(result))) {
    EXTLOG_LOG(ERROR, "err push back result", K(ret));
  }
  return ret;
}

int ObLogReqStartLogIdByTsResponse::get_err() const
{
  return err_;
}

const ObLogReqStartLogIdByTsResponse::ResultArray& ObLogReqStartLogIdByTsResponse::get_results() const
{
  return res_;
}

int64_t ObLogReqStartLogIdByTsResponse::rpc_ver() const
{
  return rpc_ver_;
}

void ObLogReqStartLogIdByTsResponse::set_rpc_ver(const int64_t ver)
{
  rpc_ver_ = ver;
}

OB_DEF_SERIALIZE(ObLogReqStartLogIdByTsResponse)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, err_, res_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogReqStartLogIdByTsResponse)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, err_, res_);
  return len;
}

OB_DEF_DESERIALIZE(ObLogReqStartLogIdByTsResponse)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE, err_, res_);
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match", K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return ret;
}

int ObLogReqStartLogIdByTsProcessor::process()
{
  int ret = OB_SUCCESS;
  const ObLogReqStartLogIdByTsRequest& req_msg = arg_;
  ObLogReqStartLogIdByTsResponse& response = result_;
  ObICLogMgr* clog_mgr = NULL;
  if (OB_FAIL(ObExternalProcessorHelper::get_clog_mgr(partition_service_, clog_mgr))) {
    EXTLOG_LOG(WARN, "get clog mgr error", K(ret), KP(partition_service_));
  } else if (OB_ISNULL(clog_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "clog_mgr is null", K(ret));
  } else {
    ret = clog_mgr->req_start_log_id_by_ts(req_msg, response);
    EXTLOG_LOG(TRACE, "clog mgr req_start_log_id_by_ts", K(ret), K(req_msg), K(response));
  }
  // rewrite ret for rpc
  ret = OB_SUCCESS;
  return ret;
}

// Request i-log start positions by log ids.
void ObLogReqStartPosByLogIdRequest::Param::reset()
{
  pkey_.reset();
  start_log_id_ = OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObLogReqStartPosByLogIdRequest::Param, pkey_, start_log_id_);

ObLogReqStartPosByLogIdRequest::ObLogReqStartPosByLogIdRequest() : rpc_ver_(CUR_RPC_VER), params_()
{}

ObLogReqStartPosByLogIdRequest::~ObLogReqStartPosByLogIdRequest()
{
  reset();
}

void ObLogReqStartPosByLogIdRequest::reset()
{
  params_.reset();
}

bool ObLogReqStartPosByLogIdRequest::is_valid() const
{
  return (0 < params_.count());
}

int ObLogReqStartPosByLogIdRequest::set_params(const ParamArray& params)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT < params.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err set params, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", params.count());
  } else if (OB_SUCCESS != (ret = params_.assign(params))) {
    EXTLOG_LOG(ERROR, "err assign params", K(ret));
  }
  return ret;
}

int ObLogReqStartPosByLogIdRequest::append_param(const Param& param)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT <= params_.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err append param, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", params_.count());
  } else if (OB_SUCCESS != (ret = params_.push_back(param))) {
    EXTLOG_LOG(ERROR, "err push back param", K(ret));
  }
  return ret;
}

const ObLogReqStartPosByLogIdRequest::ParamArray& ObLogReqStartPosByLogIdRequest::get_params() const
{
  return params_;
}

int64_t ObLogReqStartPosByLogIdRequest::rpc_ver() const
{
  return rpc_ver_;
}

OB_DEF_SERIALIZE(ObLogReqStartPosByLogIdRequest)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, params_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogReqStartPosByLogIdRequest)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, params_);
  return len;
}

OB_DEF_DESERIALIZE(ObLogReqStartPosByLogIdRequest)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE, params_);
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match", K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return ret;
}

void ObLogReqStartPosByLogIdResponse::Result::reset()
{
  err_ = OB_SUCCESS;
  file_id_ = OB_INVALID_FILE_ID;
  offset_ = OB_INVALID_OFFSET;
  first_log_id_ = OB_INVALID_ID;
  last_log_id_ = OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObLogReqStartPosByLogIdResponse::Result, err_, file_id_, offset_, first_log_id_, last_log_id_);

ObLogReqStartPosByLogIdResponse::ObLogReqStartPosByLogIdResponse() : rpc_ver_(CUR_RPC_VER), err_(OB_SUCCESS), res_()
{}

ObLogReqStartPosByLogIdResponse::~ObLogReqStartPosByLogIdResponse()
{
  reset();
}

void ObLogReqStartPosByLogIdResponse::reset()
{
  err_ = OB_SUCCESS;
  res_.reset();
}

void ObLogReqStartPosByLogIdResponse::set_err(const int err)
{
  err_ = err;
}

int ObLogReqStartPosByLogIdResponse::set_results(const ResultArray& results)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT < results.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err set results, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", results.count());
  } else if (OB_SUCCESS != (ret = res_.assign(results))) {
    EXTLOG_LOG(ERROR, "err assign results", K(ret));
  }
  return ret;
}

int ObLogReqStartPosByLogIdResponse::append_result(const Result& result)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT <= res_.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err append result, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", res_.count());
  } else if (OB_SUCCESS != (ret = res_.push_back(result))) {
    EXTLOG_LOG(ERROR, "err push back result", K(ret));
  }
  return ret;
}

int ObLogReqStartPosByLogIdResponse::get_err() const
{
  return err_;
}

const ObLogReqStartPosByLogIdResponse::ResultArray& ObLogReqStartPosByLogIdResponse::get_results() const
{
  return res_;
}

int64_t ObLogReqStartPosByLogIdResponse::rpc_ver() const
{
  return rpc_ver_;
}

void ObLogReqStartPosByLogIdResponse::set_rpc_ver(const int64_t ver)
{
  rpc_ver_ = ver;
}

OB_DEF_SERIALIZE(ObLogReqStartPosByLogIdResponse)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, err_, res_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogReqStartPosByLogIdResponse)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, err_, res_);
  return len;
}

OB_DEF_DESERIALIZE(ObLogReqStartPosByLogIdResponse)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE, err_, res_);
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match", K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return ret;
}

int ObLogReqStartPosByLogIdProcessor::process()
{
  int ret = OB_SUCCESS;
  const ObLogReqStartPosByLogIdRequest& req_msg = arg_;
  ObLogReqStartPosByLogIdResponse& response = result_;
  ObICLogMgr* clog_mgr = NULL;
  if (OB_FAIL(ObExternalProcessorHelper::get_clog_mgr(partition_service_, clog_mgr))) {
    EXTLOG_LOG(WARN, "get clog mgr error", K(ret), KP(partition_service_));
  } else if (OB_ISNULL(clog_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "clog_mgr is null", K(ret));
  } else {
    ret = clog_mgr->req_start_pos_by_log_id(req_msg, response);
    EXTLOG_LOG(TRACE, "clog mgr req_start_pos_by_log_id", K(ret), K(req_msg), K(response));
  }
  // rewrite ret for rpc
  ret = OB_SUCCESS;
  return ret;
}

// Fetch log and return offline partitions with their last log id.
void ObLogExternalFetchLogRequest::Param::reset()
{
  pkey_.reset();
  start_log_id_ = OB_INVALID_ID;
  last_log_id_ = OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObLogExternalFetchLogRequest::Param, pkey_, start_log_id_, last_log_id_);

ObLogExternalFetchLogRequest::ObLogExternalFetchLogRequest()
    : rpc_ver_(CUR_RPC_VER),
      params_(),
      file_id_(OB_INVALID_FILE_ID),
      offset_(OB_INVALID_OFFSET),
      offline_timeout_(DEFAULT_OFFLINE_TIMEOUT)
{}

ObLogExternalFetchLogRequest::~ObLogExternalFetchLogRequest()
{
  reset();
}

void ObLogExternalFetchLogRequest::reset()
{
  params_.reset();
  file_id_ = OB_INVALID_FILE_ID;
  offset_ = OB_INVALID_OFFSET;
}

bool ObLogExternalFetchLogRequest::is_valid() const
{
  bool bool_ret = false;
  int64_t count = params_.count();
  bool_ret = count > 0 && OB_INVALID_FILE_ID != file_id_ && OB_INVALID_OFFSET != offset_ &&
             is_valid_offline_timeout(offline_timeout_);
  for (int64_t i = 0; i < count && bool_ret; i++) {
    const ObPartitionKey& pkey = params_[i].pkey_;
    const uint64_t start = params_[i].start_log_id_;
    const uint64_t last = params_[i].last_log_id_;
    bool_ret = (pkey.is_valid() && (OB_INVALID_ID != start) && (OB_INVALID_ID == last || start <= last));
  }
  return bool_ret;
}

int ObLogExternalFetchLogRequest::set_params(const ParamArray& params)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT < params.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err set params, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", params.count());
  } else if (OB_SUCCESS != (ret = params_.assign(params))) {
    EXTLOG_LOG(ERROR, "err assign params", K(ret));
  }
  return ret;
}

int ObLogExternalFetchLogRequest::append_param(const Param& param)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT <= params_.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err append param, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", params_.count());
  } else if (OB_SUCCESS != (ret = params_.push_back(param))) {
    EXTLOG_LOG(ERROR, "err push param", K(ret));
  }
  return ret;
}

int ObLogExternalFetchLogRequest::set_file_id_offset(const file_id_t& file_id, const offset_t& offset)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_FILE_ID == file_id || OB_INVALID_OFFSET == offset) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "invalid argument", K(ret), K(file_id), K(offset));
  } else {
    file_id_ = file_id;
    offset_ = offset;
  }
  return ret;
}

bool ObLogExternalFetchLogRequest::is_valid_offline_timeout(const int64_t offline_timestamp)
{
  return offline_timestamp > 0 && offline_timestamp < MAX_OFFLINE_TIMEOUT;
}

int ObLogExternalFetchLogRequest::set_offline_timeout(const int64_t offline_timeout)
{
  int ret = OB_SUCCESS;
  if (offline_timeout < 0 || offline_timeout > MAX_OFFLINE_TIMEOUT) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "set offline timeout error", K(ret), K(offline_timeout));
  } else {
    offline_timeout_ = offline_timeout;
  }
  return ret;
}

const ObLogExternalFetchLogRequest::ParamArray& ObLogExternalFetchLogRequest::get_params() const
{
  return params_;
}

file_id_t ObLogExternalFetchLogRequest::get_file_id() const
{
  return file_id_;
}

offset_t ObLogExternalFetchLogRequest::get_offset() const
{
  return offset_;
}

int64_t ObLogExternalFetchLogRequest::get_offline_timeout() const
{
  return offline_timeout_;
}

int64_t ObLogExternalFetchLogRequest::rpc_ver() const
{
  return rpc_ver_;
}

OB_DEF_SERIALIZE(ObLogExternalFetchLogRequest)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, params_, file_id_, offset_, offline_timeout_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogExternalFetchLogRequest)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, params_, file_id_, offset_, offline_timeout_);
  return len;
}

OB_DEF_DESERIALIZE(ObLogExternalFetchLogRequest)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE, params_, file_id_, offset_);
    if (OB_SUCCESS == ret && data_len > pos) {
      LST_DO_CODE(offline_timeout_);
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match", K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return ret;
}

void ObLogExternalFetchLogResponse::OfflinePartition::reset()
{
  pkey_.reset();
  sync_ts_ = OB_INVALID_TIMESTAMP;
}

OB_SERIALIZE_MEMBER(ObLogExternalFetchLogResponse::OfflinePartition, pkey_, sync_ts_);

ObLogExternalFetchLogResponse::ObLogExternalFetchLogResponse()
    : rpc_ver_(CUR_RPC_VER),
      err_(OB_SUCCESS),
      offline_partitions_(),
      file_id_(OB_INVALID_FILE_ID),
      offset_(OB_INVALID_OFFSET),
      log_num_(0),
      pos_(0)
{
  memset(log_entry_buf_, 0, FETCH_BUF_LEN);
}

ObLogExternalFetchLogResponse::~ObLogExternalFetchLogResponse()
{
  reset();
}

void ObLogExternalFetchLogResponse::reset()
{
  err_ = OB_SUCCESS;
  offline_partitions_.reset();
  file_id_ = OB_INVALID_FILE_ID;
  offset_ = OB_INVALID_OFFSET;
  log_num_ = 0;
  pos_ = 0;
  // This memset costs too much CPU, so disable it.
  //  memset(log_entry_buf_, 0, FETCH_BUF_LEN);
}

int64_t ObLogExternalFetchLogResponse::offline_partition_cnt_lmt_()
{
  // Caution: cnt limit is based on the rpc_ver_ of this response,
  //          so set rpc_ver_ before calling this function.
  int64_t lmt = 0;
  if (CUR_RPC_VER == rpc_ver_) {
    // Lmt for CUR_RPC_VER.
    lmt = cur_ver_offline_partition_cnt_lmt_();
  } else {
    EXTLOG_LOG(ERROR, "invalid rpc version", K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
    lmt = 0;
  }
  return lmt;
}

int ObLogExternalFetchLogResponse::set_offline_partitions(const OfflinePartitionArray& offlines)
{
  // Err.
  // - OB_BUF_NOT_ENOUGH: no space
  int ret = OB_SUCCESS;
  if (offline_partition_cnt_lmt_() < offlines.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(ERROR,
        "err assign offline partitions, buf size limited",
        K(ret),
        "limit",
        offline_partition_cnt_lmt_(),
        "size",
        offlines.count());
  } else if (OB_SUCCESS != (ret = offline_partitions_.assign(offlines))) {
    EXTLOG_LOG(ERROR, "err assign offline partitions", K(ret));
  }
  return ret;
}

int ObLogExternalFetchLogResponse::append_offline_partition(const ObPartitionKey& pkey, const int64_t sync_ts)
{
  int ret = OB_SUCCESS;
  OfflinePartition offline;
  if (!pkey.is_valid() || sync_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "append offline parittion error", K(ret), K(pkey), K(sync_ts));
  } else {
    offline.pkey_ = pkey;
    offline.sync_ts_ = sync_ts;
    ret = append_offline_partition(offline);
  }
  return ret;
}

int ObLogExternalFetchLogResponse::append_offline_partition(const OfflinePartition& offline)
{
  // Err.
  // - OB_BUF_NOT_ENOUGH: no space
  int ret = OB_SUCCESS;
  if (offline_partition_cnt_lmt_() <= offline_partitions_.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(ERROR,
        "err append offline partition, buf size limited",
        K(ret),
        "limit",
        offline_partition_cnt_lmt_(),
        "count",
        offline_partitions_.count());
  } else if (OB_SUCCESS != (ret = offline_partitions_.push_back(offline))) {
    EXTLOG_LOG(ERROR, "err push back offline partition", K(ret));
  }
  return ret;
}

int ObLogExternalFetchLogResponse::set_file_id_offset(const file_id_t& file_id, const offset_t& offset)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_FILE_ID == file_id || OB_INVALID_OFFSET == offset) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "invalid argument", K(ret), K(file_id), K(offset));
  } else {
    file_id_ = file_id;
    offset_ = offset;
  }
  return ret;
}

int ObLogExternalFetchLogResponse::append_log(const clog::ObLogEntry& log_entry)
{
  int ret = OB_SUCCESS;
  int64_t old_pos = pos_;
  ret = log_entry.serialize(log_entry_buf_, FETCH_BUF_LEN, pos_);
  if (OB_SUCC(ret)) {
    ++log_num_;
    EXTLOG_LOG(TRACE, "append result log success", K(log_num_), K(pos_), "clog entry", log_entry.get_header());
  } else {
    EXTLOG_LOG(TRACE, "append result log error", K(ret), K(pos_));
    pos_ = old_pos;
    ret = OB_BUF_NOT_ENOUGH;
  }
  return ret;
}

int ObLogExternalFetchLogResponse::get_err() const
{
  return err_;
}

const ObLogExternalFetchLogResponse::OfflinePartitionArray& ObLogExternalFetchLogResponse::get_offline_partitions()
    const
{
  return offline_partitions_;
}

file_id_t ObLogExternalFetchLogResponse::get_file_id() const
{
  return file_id_;
}

offset_t ObLogExternalFetchLogResponse::get_offset() const
{
  return offset_;
}

int32_t ObLogExternalFetchLogResponse::get_log_num() const
{
  return log_num_;
}

int64_t ObLogExternalFetchLogResponse::get_pos() const
{
  return pos_;
}

const char* ObLogExternalFetchLogResponse::get_log_entry_buf() const
{
  return log_entry_buf_;
}

int64_t ObLogExternalFetchLogResponse::rpc_ver() const
{
  return rpc_ver_;
}

void ObLogExternalFetchLogResponse::set_rpc_ver(const int64_t ver)
{
  rpc_ver_ = ver;
}

// serialize(buf, buf_len, pos)
OB_DEF_SERIALIZE(ObLogExternalFetchLogResponse)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, err_, offline_partitions_, file_id_, offset_, log_num_, pos_);
  if (OB_SUCCESS == ret && pos_ > 0) {
    if (buf_len - pos < pos_) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf + pos, log_entry_buf_, pos_);
      pos += pos_;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogExternalFetchLogResponse)
{
  int64_t len = 0;
  int tmp_ret = OB_SUCCESS;
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, err_, offline_partitions_, file_id_, offset_, log_num_, pos_);
    len += pos_;
  } else {
    tmp_ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "get serialize size error, version not match", K(tmp_ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return len;
}

int64_t ObLogExternalFetchLogResponse::cur_ver_offline_partition_cnt_lmt_()
{
  // Get offline partition array size limit base on rpc version.
  static const int64_t SPARE = 256;
  static const int64_t UNINIT = -1;
  static int64_t lmt = UNINIT;
  if (UNINIT == ATOMIC_LOAD(&(lmt))) {
    int64_t avail_space = OB_MAX_PACKET_LENGTH;
    avail_space -= FETCH_BUF_LEN + SPARE;
    int64_t per_item_space = 0;
    OfflinePartition offline_sample;
    per_item_space += offline_sample.get_serialize_size();
    int64_t cnt = avail_space / per_item_space;
    (void)ATOMIC_SET(&(lmt), cnt);
  }
  return lmt;
}

OB_DEF_DESERIALIZE(ObLogExternalFetchLogResponse)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE, err_, offline_partitions_, file_id_, offset_, log_num_, pos_);
    if (pos_ < 0 || pos_ > FETCH_BUF_LEN) {
      ret = OB_ERR_UNEXPECTED;
      EXTLOG_LOG(ERROR, "pos_ is not valid", K(ret), K(pos_));
    } else if (pos_ > 0) {
      MEMCPY(log_entry_buf_, buf + pos, pos_);
      pos += pos_;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match", K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return ret;
}

int ObLogExternalFetchLogProcessor::process()
{
  int ret = OB_SUCCESS;
  const ObLogExternalFetchLogRequest& req_msg = arg_;
  ObLogExternalFetchLogResponse& response = result_;
  ObICLogMgr* clog_mgr = NULL;
  if (OB_FAIL(ObExternalProcessorHelper::get_clog_mgr(partition_service_, clog_mgr))) {
    EXTLOG_LOG(WARN, "get clog mgr error", K(ret), KP(partition_service_));
  } else if (OB_ISNULL(clog_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "clog_mgr is null", K(ret));
  } else {
    ret = clog_mgr->fetch_log(req_msg, response);
    EXTLOG_LOG(TRACE, "clog mgr fetch log", K(ret), K(req_msg), K(response));
  }
  // rewrite ret for rpc
  ret = OB_SUCCESS;
  return ret;
}

//  Request heartbeat information of given partitions.
void ObLogReqHeartbeatInfoRequest::Param::reset()
{
  pkey_.reset();
  log_id_ = OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObLogReqHeartbeatInfoRequest::Param, pkey_, log_id_);

ObLogReqHeartbeatInfoRequest::ObLogReqHeartbeatInfoRequest() : rpc_ver_(CUR_RPC_VER), params_()
{}

ObLogReqHeartbeatInfoRequest::~ObLogReqHeartbeatInfoRequest()
{
  reset();
}

void ObLogReqHeartbeatInfoRequest::reset()
{
  params_.reset();
}

bool ObLogReqHeartbeatInfoRequest::is_valid() const
{
  return (0 < params_.count());
}

int ObLogReqHeartbeatInfoRequest::set_params(const ParamArray& params)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT < params.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err set params, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", params.count());
  } else if (OB_SUCCESS != (ret = params_.assign(params))) {
    EXTLOG_LOG(ERROR, "err assign params", K(ret));
  }
  return ret;
}

int ObLogReqHeartbeatInfoRequest::append_param(const Param& param)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT <= params_.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err append param, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", params_.count());
  } else if (OB_SUCCESS != (ret = params_.push_back(param))) {
    EXTLOG_LOG(ERROR, "err push param", K(ret));
  }
  return ret;
}

const ObLogReqHeartbeatInfoRequest::ParamArray& ObLogReqHeartbeatInfoRequest::get_params() const
{
  return params_;
}

int64_t ObLogReqHeartbeatInfoRequest::rpc_ver() const
{
  return rpc_ver_;
}

OB_DEF_SERIALIZE(ObLogReqHeartbeatInfoRequest)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, params_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogReqHeartbeatInfoRequest)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, params_);
  return len;
}

OB_DEF_DESERIALIZE(ObLogReqHeartbeatInfoRequest)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE, params_);
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match", K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return ret;
}

void ObLogReqHeartbeatInfoResponse::Result::reset()
{
  err_ = OB_SUCCESS;
  tstamp_ = OB_INVALID_TIMESTAMP;
}

OB_SERIALIZE_MEMBER(ObLogReqHeartbeatInfoResponse::Result, err_, tstamp_);

ObLogReqHeartbeatInfoResponse::ObLogReqHeartbeatInfoResponse() : rpc_ver_(CUR_RPC_VER), err_(OB_SUCCESS), res_()
{}

ObLogReqHeartbeatInfoResponse::~ObLogReqHeartbeatInfoResponse()
{
  reset();
}

void ObLogReqHeartbeatInfoResponse::reset()
{
  err_ = OB_SUCCESS;
  res_.reset();
}

void ObLogReqHeartbeatInfoResponse::set_err(const int err)
{
  err_ = err;
}

int ObLogReqHeartbeatInfoResponse::set_results(const ResultArray& results)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT < results.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err set results, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", results.count());
  } else if (OB_SUCCESS != (ret = res_.assign(results))) {
    EXTLOG_LOG(ERROR, "err assign results", K(ret));
  }
  return ret;
}

int ObLogReqHeartbeatInfoResponse::append_result(const Result& result)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT <= res_.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err append result, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", res_.count());
  } else if (OB_SUCCESS != (ret = res_.push_back(result))) {
    EXTLOG_LOG(ERROR, "err push back results", K(ret));
  }
  return ret;
}

int ObLogReqHeartbeatInfoResponse::get_err() const
{
  return err_;
}

const ObLogReqHeartbeatInfoResponse::ResultArray& ObLogReqHeartbeatInfoResponse::get_results() const
{
  return res_;
}

int64_t ObLogReqHeartbeatInfoResponse::rpc_ver() const
{
  return rpc_ver_;
}

void ObLogReqHeartbeatInfoResponse::set_rpc_ver(const int64_t ver)
{
  rpc_ver_ = ver;
}

OB_DEF_SERIALIZE(ObLogReqHeartbeatInfoResponse)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, err_, res_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogReqHeartbeatInfoResponse)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, err_, res_);
  return len;
}

OB_DEF_DESERIALIZE(ObLogReqHeartbeatInfoResponse)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE, err_, res_);
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match", K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return ret;
}

int ObLogReqHeartbeatInfoProcessor::process()
{
  int ret = OB_SUCCESS;
  const ObLogReqHeartbeatInfoRequest& req = arg_;
  ObLogReqHeartbeatInfoResponse& resp = result_;
  clog::ObICLogMgr* clog_mgr = NULL;
  logservice::ObExtLogService* els = NULL;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "partition_service_ is null", K(ret));
  } else if (OB_ISNULL(clog_mgr = partition_service_->get_clog_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "clog_mgr is null", K(ret));
  } else if (OB_ISNULL(els = clog_mgr->get_external_log_service())) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "els is null", K(ret));
  } else {
    ret = els->req_heartbeat_info(req, resp);
  }
  // rewrite ret for rpc
  ret = OB_SUCCESS;
  return ret;
}

int ObExternalProcessorHelper::get_clog_mgr(ObPartitionService* ps, ObICLogMgr*& clog_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ps)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(WARN, "get clog mgr error. ps is null", K(ret));
  } else {
    clog_mgr = ps->get_clog_mgr();
  }
  return ret;
}

// -------------- for breakpoint ------------
// Request start log id by start timestamp.
void ObLogReqStartLogIdByTsRequestWithBreakpoint::Param::reset()
{
  pkey_.reset();
  start_tstamp_ = OB_INVALID_TIMESTAMP;
  break_info_.reset();
}

OB_SERIALIZE_MEMBER(ObLogReqStartLogIdByTsRequestWithBreakpoint::Param, pkey_, start_tstamp_, break_info_);

ObLogReqStartLogIdByTsRequestWithBreakpoint::ObLogReqStartLogIdByTsRequestWithBreakpoint()
    : rpc_ver_(CUR_RPC_VER), params_()
{}

ObLogReqStartLogIdByTsRequestWithBreakpoint::~ObLogReqStartLogIdByTsRequestWithBreakpoint()
{
  reset();
}

void ObLogReqStartLogIdByTsRequestWithBreakpoint::reset()
{
  params_.reset();
}

bool ObLogReqStartLogIdByTsRequestWithBreakpoint::is_valid() const
{
  int64_t count = params_.count();
  bool bool_ret = (count > 0);
  for (int64_t i = 0; bool_ret && i < count; i++) {
    if (!params_[i].pkey_.is_valid() || params_[i].start_tstamp_ <= 0) {
      bool_ret = false;
    }
  }
  return bool_ret;
}

int ObLogReqStartLogIdByTsRequestWithBreakpoint::set_params(const ParamArray& params)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT < params.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err set params, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", params.count());
  } else if (OB_SUCCESS != (ret = params_.assign(params))) {
    EXTLOG_LOG(ERROR, "err assign params", K(ret));
  }
  return ret;
}

int ObLogReqStartLogIdByTsRequestWithBreakpoint::append_param(const Param& param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "invalid param", K(ret), K(param));
  } else if (ITEM_CNT_LMT <= params_.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err append param, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", params_.count());
  } else if (OB_SUCCESS != (ret = params_.push_back(param))) {
    EXTLOG_LOG(ERROR, "err push back param", K(ret));
  }
  return ret;
}

const ObLogReqStartLogIdByTsRequestWithBreakpoint::ParamArray& ObLogReqStartLogIdByTsRequestWithBreakpoint::get_params()
    const
{
  return params_;
}

int64_t ObLogReqStartLogIdByTsRequestWithBreakpoint::rpc_ver() const
{
  return rpc_ver_;
}

OB_DEF_SERIALIZE(ObLogReqStartLogIdByTsRequestWithBreakpoint)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, params_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogReqStartLogIdByTsRequestWithBreakpoint)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, params_);
  return len;
}

OB_DEF_DESERIALIZE(ObLogReqStartLogIdByTsRequestWithBreakpoint)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE, params_);
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match", K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return ret;
}

void ObLogReqStartLogIdByTsResponseWithBreakpoint::Result::reset()
{
  err_ = OB_SUCCESS;
  start_log_id_ = OB_INVALID_ID;
  start_log_ts_ = OB_INVALID_TIMESTAMP;
  break_info_.reset();
}

OB_SERIALIZE_MEMBER(
    ObLogReqStartLogIdByTsResponseWithBreakpoint::Result, err_, start_log_id_, start_log_ts_, break_info_);

ObLogReqStartLogIdByTsResponseWithBreakpoint::ObLogReqStartLogIdByTsResponseWithBreakpoint()
    : rpc_ver_(CUR_RPC_VER), err_(OB_SUCCESS), res_()
{}

ObLogReqStartLogIdByTsResponseWithBreakpoint::~ObLogReqStartLogIdByTsResponseWithBreakpoint()
{
  reset();
}

void ObLogReqStartLogIdByTsResponseWithBreakpoint::reset()
{
  err_ = OB_SUCCESS;
  res_.reset();
}

void ObLogReqStartLogIdByTsResponseWithBreakpoint::set_err(const int err)
{
  err_ = err;
}

int ObLogReqStartLogIdByTsResponseWithBreakpoint::set_results(const ResultArray& results)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT < results.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err set results, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", results.count());
  } else if (OB_SUCCESS != (ret = res_.assign(results))) {
    EXTLOG_LOG(ERROR, "err assign results", K(ret));
  }
  return ret;
}

int ObLogReqStartLogIdByTsResponseWithBreakpoint::append_result(const Result& result)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT <= res_.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err append result, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", res_.count());
  } else if (OB_SUCCESS != (ret = res_.push_back(result))) {
    EXTLOG_LOG(ERROR, "err push back result", K(ret));
  }
  return ret;
}

int ObLogReqStartLogIdByTsResponseWithBreakpoint::get_err() const
{
  return err_;
}

const ObLogReqStartLogIdByTsResponseWithBreakpoint::ResultArray&
    ObLogReqStartLogIdByTsResponseWithBreakpoint::get_results() const
{
  return res_;
}

int64_t ObLogReqStartLogIdByTsResponseWithBreakpoint::rpc_ver() const
{
  return rpc_ver_;
}

void ObLogReqStartLogIdByTsResponseWithBreakpoint::set_rpc_ver(const int64_t ver)
{
  rpc_ver_ = ver;
}

OB_DEF_SERIALIZE(ObLogReqStartLogIdByTsResponseWithBreakpoint)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, err_, res_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogReqStartLogIdByTsResponseWithBreakpoint)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, err_, res_);
  return len;
}

OB_DEF_DESERIALIZE(ObLogReqStartLogIdByTsResponseWithBreakpoint)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE, err_, res_);
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match", K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return ret;
}

int ObLogReqStartLogIdByTsProcessorWithBreakpoint::process()
{
  int ret = OB_SUCCESS;
  const ObLogReqStartLogIdByTsRequestWithBreakpoint& req = arg_;
  ObLogReqStartLogIdByTsResponseWithBreakpoint& resp = result_;
  clog::ObICLogMgr* clog_mgr = NULL;
  logservice::ObExtLogService* els = NULL;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "partition_service_ is null", K(ret));
  } else if (OB_ISNULL(clog_mgr = partition_service_->get_clog_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "clog_mgr is null", K(ret));
  } else if (OB_ISNULL(els = clog_mgr->get_external_log_service())) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "els is null", K(ret));
  } else {
    ret = els->req_start_log_id_by_ts_with_breakpoint(req, resp);
  }
  // rewrite ret for rpc
  ret = OB_SUCCESS;
  return ret;
}

// Request i-log start positions by log ids.
void ObLogReqStartPosByLogIdRequestWithBreakpoint::Param::reset()
{
  pkey_.reset();
  start_log_id_ = OB_INVALID_ID;
  break_info_.reset();
}

OB_SERIALIZE_MEMBER(ObLogReqStartPosByLogIdRequestWithBreakpoint::Param, pkey_, start_log_id_, break_info_);

ObLogReqStartPosByLogIdRequestWithBreakpoint::ObLogReqStartPosByLogIdRequestWithBreakpoint()
    : rpc_ver_(CUR_RPC_VER), params_()
{}

ObLogReqStartPosByLogIdRequestWithBreakpoint::~ObLogReqStartPosByLogIdRequestWithBreakpoint()
{
  reset();
}

void ObLogReqStartPosByLogIdRequestWithBreakpoint::reset()
{
  params_.reset();
}

bool ObLogReqStartPosByLogIdRequestWithBreakpoint::is_valid() const
{
  return (0 < params_.count());
}

int ObLogReqStartPosByLogIdRequestWithBreakpoint::set_params(const ParamArray& params)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT < params.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err set params, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", params.count());
  } else if (OB_SUCCESS != (ret = params_.assign(params))) {
    EXTLOG_LOG(ERROR, "err assign params", K(ret));
  }
  return ret;
}

int ObLogReqStartPosByLogIdRequestWithBreakpoint::append_param(const Param& param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "invalid param", K(ret), K(param));
  } else if (ITEM_CNT_LMT <= params_.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err append param, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", params_.count());
  } else if (OB_SUCCESS != (ret = params_.push_back(param))) {
    EXTLOG_LOG(ERROR, "err push back param", K(ret));
  }
  return ret;
}

const ObLogReqStartPosByLogIdRequestWithBreakpoint::ParamArray&
    ObLogReqStartPosByLogIdRequestWithBreakpoint::get_params() const
{
  return params_;
}

int64_t ObLogReqStartPosByLogIdRequestWithBreakpoint::rpc_ver() const
{
  return rpc_ver_;
}

OB_DEF_SERIALIZE(ObLogReqStartPosByLogIdRequestWithBreakpoint)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, params_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogReqStartPosByLogIdRequestWithBreakpoint)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, params_);
  return len;
}

OB_DEF_DESERIALIZE(ObLogReqStartPosByLogIdRequestWithBreakpoint)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE, params_);
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match", K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return ret;
}

void ObLogReqStartPosByLogIdResponseWithBreakpoint::Result::reset()
{
  err_ = OB_SUCCESS;
  file_id_ = OB_INVALID_FILE_ID;
  offset_ = OB_INVALID_OFFSET;
  first_log_id_ = OB_INVALID_ID;
  last_log_id_ = OB_INVALID_ID;
  break_info_.reset();
}

OB_SERIALIZE_MEMBER(ObLogReqStartPosByLogIdResponseWithBreakpoint::Result, err_, file_id_, offset_, first_log_id_,
    last_log_id_, break_info_);

ObLogReqStartPosByLogIdResponseWithBreakpoint::ObLogReqStartPosByLogIdResponseWithBreakpoint()
    : rpc_ver_(CUR_RPC_VER), err_(OB_SUCCESS), res_()
{}

ObLogReqStartPosByLogIdResponseWithBreakpoint::~ObLogReqStartPosByLogIdResponseWithBreakpoint()
{
  reset();
}

void ObLogReqStartPosByLogIdResponseWithBreakpoint::reset()
{
  err_ = OB_SUCCESS;
  res_.reset();
}

void ObLogReqStartPosByLogIdResponseWithBreakpoint::set_err(const int err)
{
  err_ = err;
}

int ObLogReqStartPosByLogIdResponseWithBreakpoint::set_results(const ResultArray& results)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT < results.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err set results, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", results.count());
  } else if (OB_SUCCESS != (ret = res_.assign(results))) {
    EXTLOG_LOG(ERROR, "err assign results", K(ret));
  }
  return ret;
}

int ObLogReqStartPosByLogIdResponseWithBreakpoint::append_result(const Result& result)
{
  int ret = OB_SUCCESS;
  if (ITEM_CNT_LMT <= res_.count()) {
    ret = OB_BUF_NOT_ENOUGH;
    EXTLOG_LOG(WARN, "err append result, buf not enough", K(ret), LITERAL_K(ITEM_CNT_LMT), "count", res_.count());
  } else if (OB_SUCCESS != (ret = res_.push_back(result))) {
    EXTLOG_LOG(ERROR, "err push back result", K(ret));
  }
  return ret;
}

int ObLogReqStartPosByLogIdResponseWithBreakpoint::get_err() const
{
  return err_;
}

const ObLogReqStartPosByLogIdResponseWithBreakpoint::ResultArray&
    ObLogReqStartPosByLogIdResponseWithBreakpoint::get_results() const
{
  return res_;
}

int64_t ObLogReqStartPosByLogIdResponseWithBreakpoint::rpc_ver() const
{
  return rpc_ver_;
}

void ObLogReqStartPosByLogIdResponseWithBreakpoint::set_rpc_ver(const int64_t ver)
{
  rpc_ver_ = ver;
}

OB_DEF_SERIALIZE(ObLogReqStartPosByLogIdResponseWithBreakpoint)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, err_, res_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogReqStartPosByLogIdResponseWithBreakpoint)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, err_, res_);
  return len;
}

OB_DEF_DESERIALIZE(ObLogReqStartPosByLogIdResponseWithBreakpoint)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE, err_, res_);
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match", K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return ret;
}

int ObLogReqStartPosByLogIdProcessorWithBreakpoint::process()
{
  int ret = OB_SUCCESS;
  const ObLogReqStartPosByLogIdRequestWithBreakpoint& req_msg = arg_;
  ObLogReqStartPosByLogIdResponseWithBreakpoint& response = result_;
  ObICLogMgr* clog_mgr = NULL;
  if (OB_FAIL(ObExternalProcessorHelper::get_clog_mgr(partition_service_, clog_mgr))) {
    EXTLOG_LOG(WARN, "get clog mgr error", K(ret), KP(partition_service_));
  } else if (OB_ISNULL(clog_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "clog_mgr is null", K(ret));
  } else {
    ret = clog_mgr->req_start_pos_by_log_id_with_breakpoint(req_msg, response);
    EXTLOG_LOG(TRACE, "clog mgr req_start_pos_by_log_id_with_breakpoint", K(ret), K(req_msg), K(response));
  }
  // rewrite ret for rpc
  ret = OB_SUCCESS;
  return ret;
}

int64_t ObLogStreamFetchLogResp::cur_ver_array_cnt_lmt_()
{
  // Get offline partition array size limit base on rpc version.
  static const int64_t SPARE = 256;
  static const int64_t UNINIT = -1;
  static int64_t lmt = UNINIT;
  if (UNINIT == ATOMIC_LOAD(&(lmt))) {
    int64_t avail_space = OB_MAX_PACKET_LENGTH;
    avail_space -= FETCH_BUF_LEN + SPARE;
    int64_t per_item_space = 0;
    FeedbackPartition fb_sample;
    FetchLogHeartbeatItem hb_sample;
    per_item_space += std::max(fb_sample.get_serialize_size(), hb_sample.get_serialize_size());
    if (per_item_space <= 0) {
      EXTLOG_LOG(ERROR, "invalid item size", K(per_item_space));
    } else {
      int64_t cnt = avail_space / per_item_space;
      (void)ATOMIC_SET(&(lmt), cnt);
    }
  }
  return lmt;
}

OB_SERIALIZE_MEMBER(ObLogOpenStreamReq::Param, pkey_, start_log_id_);
OB_SERIALIZE_MEMBER(ObLogOpenStreamReq, rpc_ver_, params_, stream_lifetime_, liboblog_pid_, stale_stream_);

OB_SERIALIZE_MEMBER(ObStreamSeq, self_, seq_ts_);
OB_SERIALIZE_MEMBER(ObLogOpenStreamResp, rpc_ver_, err_, debug_err_, seq_);

ObLogStreamFetchLogResp::ObLogStreamFetchLogResp()
    : fetch_log_heartbeat_arr_(
          common::ObModIds::OB_FETCH_LOG_HEARTBEAT_ARRAY_FOR_LIBOBLOG, common::OB_MALLOC_NORMAL_BLOCK_SIZE)
{
  reset();
}

int ObLogStreamFetchLogResp::assign(const ObLogStreamFetchLogResp& other)
{
  int ret = OB_SUCCESS;

  if (this != &other) {
    rpc_ver_ = other.rpc_ver_;
    err_ = other.err_;
    debug_err_ = other.debug_err_;
    fetch_status_ = other.fetch_status_;
    feedback_partition_arr_ = other.feedback_partition_arr_;
    fetch_log_heartbeat_arr_ = other.fetch_log_heartbeat_arr_;
    log_num_ = other.log_num_;
    pos_ = other.pos_;

    log_entry_buf_[0] = '\0';

    if (log_num_ > 0 && pos_ > 0) {
      (void)MEMCPY(log_entry_buf_, other.log_entry_buf_, pos_);
    }
  }

  return ret;
}

int ObLogOpenStreamProcessor::process()
{
  int ret = common::OB_SUCCESS;
  const ObLogOpenStreamReq& req = arg_;
  ObLogOpenStreamResp& resp = result_;
  clog::ObICLogMgr* clog_mgr = NULL;
  logservice::ObExtLogService* els = NULL;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "partition_service_ is null", K(ret));
  } else if (OB_ISNULL(clog_mgr = partition_service_->get_clog_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "clog_mgr is null", K(ret));
  } else if (OB_ISNULL(els = clog_mgr->get_external_log_service())) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "els is null", K(ret));
  } else {
    ObAddr liboblog_addr = get_peer();
    ret = els->open_stream(req, liboblog_addr, resp);
  }
  // rewrite ret for rpc framework
  return OB_SUCCESS;
}

OB_SERIALIZE_MEMBER(ObFetchStatus, touched_pkey_count_, need_fetch_pkey_count_, reach_max_log_id_pkey_count_,
    reach_upper_limit_ts_pkey_count_, scan_round_count_, l2s_net_time_, svr_queue_time_, ext_process_time_);
OB_SERIALIZE_MEMBER(
    ObLogStreamFetchLogReq, rpc_ver_, seq_, enable_feedback_, upper_limit_ts_, log_cnt_per_part_per_round_);
OB_SERIALIZE_MEMBER(ObLogStreamFetchLogResp::FeedbackPartition, pkey_, feedback_type_);
OB_SERIALIZE_MEMBER(ObLogStreamFetchLogResp::FetchLogHeartbeatItem, pkey_, next_log_id_, heartbeat_ts_);

// serialize(buf, buf_len, pos)
OB_DEF_SERIALIZE(ObLogStreamFetchLogResp)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      rpc_ver_,
      err_,
      debug_err_,
      fetch_status_,
      feedback_partition_arr_,
      fetch_log_heartbeat_arr_,
      log_num_,
      pos_);
  if (OB_SUCCESS == ret && pos_ > 0) {
    if (buf_len - pos < pos_) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf + pos, log_entry_buf_, pos_);
      pos += pos_;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogStreamFetchLogResp)
{
  int64_t len = 0;
  int tmp_ret = OB_SUCCESS;
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_ADD_LEN,
        rpc_ver_,
        err_,
        debug_err_,
        fetch_status_,
        feedback_partition_arr_,
        fetch_log_heartbeat_arr_,
        log_num_,
        pos_);
    len += pos_;
  } else {
    tmp_ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "get serialize size error, version not match", K(tmp_ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return len;
}

OB_DEF_DESERIALIZE(ObLogStreamFetchLogResp)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE,
        err_,
        debug_err_,
        fetch_status_,
        feedback_partition_arr_,
        fetch_log_heartbeat_arr_,
        log_num_,
        pos_);
    if (pos_ < 0 || pos_ > FETCH_BUF_LEN) {
      ret = OB_ERR_UNEXPECTED;
      EXTLOG_LOG(ERROR, "pos_ is not valid", K(ret), K(pos_));
    } else if (pos_ > 0) {
      MEMCPY(log_entry_buf_, buf + pos, pos_);
      pos += pos_;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match", K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return ret;
}

int ObLogStreamFetchLogProcessor::process()
{
  int ret = common::OB_SUCCESS;
  const ObLogStreamFetchLogReq& req = arg_;
  ObLogStreamFetchLogResp& resp = result_;
  clog::ObICLogMgr* clog_mgr = NULL;
  logservice::ObExtLogService* els = NULL;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "partition_service_ is null", K(ret));
  } else if (OB_ISNULL(clog_mgr = partition_service_->get_clog_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "clog_mgr is null", K(ret));
  } else if (OB_ISNULL(els = clog_mgr->get_external_log_service())) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "els is null", K(ret));
  } else {
    ret = els->fetch_log(req, resp, get_send_timestamp(), get_receive_timestamp());
  }
  // rewrite ret for rpc framework
  return OB_SUCCESS;
}

// leader heartbeat
OB_SERIALIZE_MEMBER(ObLogLeaderHeartbeatReq::Param, pkey_, next_log_id_);
OB_DEF_SERIALIZE(ObLogLeaderHeartbeatReq)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, params_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogLeaderHeartbeatReq)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, params_);
  return len;
}

OB_DEF_DESERIALIZE(ObLogLeaderHeartbeatReq)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE, params_);
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match", K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObLogLeaderHeartbeatResp::Result, err_, next_served_log_id_, next_served_ts_);

OB_DEF_SERIALIZE(ObLogLeaderHeartbeatResp)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, err_, debug_err_, res_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogLeaderHeartbeatResp)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, err_, debug_err_, res_);
  return len;
}

OB_DEF_DESERIALIZE(ObLogLeaderHeartbeatResp)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_);
  if (CUR_RPC_VER == rpc_ver_) {
    LST_DO_CODE(OB_UNIS_DECODE, err_, debug_err_, res_);
  } else {
    ret = OB_NOT_SUPPORTED;
    EXTLOG_LOG(ERROR, "deserialize error, version not match", K(ret), K(rpc_ver_), LITERAL_K(CUR_RPC_VER));
  }
  return ret;
}

int ObLogLeaderHeartbeatProcessor::process()
{
  int ret = OB_SUCCESS;
  const ObLogLeaderHeartbeatReq& req = arg_;
  ObLogLeaderHeartbeatResp& resp = result_;
  clog::ObICLogMgr* clog_mgr = NULL;
  logservice::ObExtLogService* els = NULL;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "partition_service_ is null", K(ret));
  } else if (OB_ISNULL(clog_mgr = partition_service_->get_clog_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "clog_mgr is null", K(ret));
  } else if (OB_ISNULL(els = clog_mgr->get_external_log_service())) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "els is null", K(ret));
  } else {
    ret = els->leader_heartbeat(req, resp);
  }
  // rewrite ret for rpc framework
  return OB_SUCCESS;
}

}  // namespace obrpc
}  // namespace oceanbase
