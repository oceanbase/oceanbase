/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_cdc_raw_log_req.h"

namespace oceanbase
{
namespace obrpc
{

//////////////////////////////////////// ObCdcFetchRawLogReq //////////////////////////////////////

void ObCdcFetchRawLogReq::reset()
{
  rpc_ver_ = 0;
  client_id_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  start_lsn_.reset();
  file_id_ = -1;
  size_ = 0;
  progress_ = OB_INVALID_TIMESTAMP;
  seq_ = -1;
  cur_round_rpc_count_ = 0;
  compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
  flag_ = 0;
  client_type_ = ObCdcClientType::CLIENT_TYPE_UNKNOWN;
}

void ObCdcFetchRawLogReq::reset(const ObCdcRpcId &client_id,
     const uint64_t tenant_id,
     const share::ObLSID &ls_id,
     const palf::LSN &start_lsn,
     const uint64_t size,
     const int64_t progress)
{
  // only reset specified members for update rpc req, don't reset all
  client_id_ = client_id;
  tenant_id_ = tenant_id;
  ls_id_ = ls_id;
  start_lsn_ = start_lsn;
  size_ = size;
  progress_ = progress;
}

OB_SERIALIZE_MEMBER(ObCdcFetchRawLogReq, rpc_ver_, client_id_,
    tenant_id_, ls_id_, start_lsn_, file_id_, size_, progress_,
    seq_, cur_round_rpc_count_, compressor_type_, flag_, client_type_);

//////////////////////////////////////// ObCdcFetchRawStatus //////////////////////////////////////

OB_SERIALIZE_MEMBER(ObCdcFetchRawStatus, source_, read_active_file_, process_time_,
    read_palf_time_, read_archive_time_, local_to_svr_time_, queue_time_);

//////////////////////////////////////// ObCdcFetchRawLogResp //////////////////////////////////////

void ObCdcFetchRawLogResp::reset()
{
  rpc_ver_ = 0;
  err_ = OB_SUCCESS;
  ls_id_.reset();
  file_id_ = -1;
  seq_ = -1;
  cur_round_rpc_count_ = 0;
  feedback_type_ = FeedbackType::INVALID_FEEDBACK;
  fetch_status_.reset();
  server_progress_.reset();
  read_size_ = 0;
  replayable_point_scn_.reset();
}

void ObCdcFetchRawLogResp::reset(const share::ObLSID &ls_id,
      const int64_t file_id,
      const int32_t seq_no,
      const int32_t cur_round_rpc_count)
{
  reset();
  ls_id_ = ls_id;
  file_id_ = file_id;
  seq_ = seq_no;
  cur_round_rpc_count_ = cur_round_rpc_count;
}

OB_DEF_SERIALIZE(ObCdcFetchRawLogResp)
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_ENCODE, rpc_ver_, err_, ls_id_, file_id_, seq_, cur_round_rpc_count_,
      cur_round_rpc_count_, feedback_type_, fetch_status_, server_progress_,
      read_size_, replayable_point_scn_);

  if (OB_SUCCESS == ret && read_size_ > 0) {
    if (buf_len - pos < read_size_) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf + pos, log_entry_buf_, read_size_);
      pos += read_size_;
    }
  }

  return ret;
}

OB_DEF_DESERIALIZE(ObCdcFetchRawLogResp)
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_DECODE, rpc_ver_, err_, ls_id_, file_id_, seq_, cur_round_rpc_count_,
      cur_round_rpc_count_, feedback_type_, fetch_status_, server_progress_,
      read_size_, replayable_point_scn_);

  if (OB_SUCC(ret)) {
    if (read_size_ > 0) {
      if (read_size_ + pos > data_len) {
        ret = OB_INVALID_DATA;
        EXTLOG_LOG(WARN, "get invalid fetch raw log resp, read_size is beyond data_len",
            K(read_size_), K(pos), K(data_len));
      } else if (read_size_ > sizeof(log_entry_buf_)) {
        ret = OB_BUF_NOT_ENOUGH;
        EXTLOG_LOG(WARN, "log_entry_buf_ is not enough", K(read_size_));
      } else {
        MEMCPY(log_entry_buf_, buf + pos, read_size_);
      }
    } else if (read_size_ < 0) {
      ret = OB_INVALID_DATA;
      EXTLOG_LOG(WARN, "get invalid fetch raw log resp, read_size is below 0",
          K(read_size_), K(pos), K(data_len));
    }
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObCdcFetchRawLogResp)
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN, rpc_ver_, err_, ls_id_, file_id_, seq_, cur_round_rpc_count_,
      cur_round_rpc_count_, feedback_type_, fetch_status_, server_progress_,
      read_size_, replayable_point_scn_);

  if (read_size_ >= 0 && read_size_ < sizeof(log_entry_buf_)) {
    len += read_size_;
  } else {
    EXTLOG_LOG_RET(WARN, OB_INVALID_DATA, "get invalid read_size in FetchRawLogResp", K(read_size_));
  }

  return len;
}

}
}