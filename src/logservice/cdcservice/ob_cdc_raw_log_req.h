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

#ifndef OCEANBASE_CDC_RAW_LOG_REQ_H_
#define OCEANBASE_CDC_RAW_LOG_REQ_H_

#include "lib/compress/ob_compress_util.h"
#include "logservice/palf/lsn.h"
#include "ob_cdc_req_struct.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"

namespace oceanbase
{

namespace obrpc
{

class ObCdcFetchRawLogReq
{
public:
  OB_UNIS_VERSION(1);
public:
  ObCdcFetchRawLogReq() { reset(); }
  ~ObCdcFetchRawLogReq() { reset(); }
  void reset();

  // would not reset other fields
  // if caller want to get a brand new rpc request
  // use reset above
  void reset(const ObCdcRpcId &client_id,
       const uint64_t tenant_id,
       const share::ObLSID &ls_id,
       const palf::LSN &start_lsn,
       const uint64_t size,
       const int64_t progress);

  void set_aux_param(const int64_t file_id,
       const int32_t seq_no,
       const int32_t rpc_count)
  {
    set_file_id(file_id);
    set_seq_no(seq_no),
    set_current_round_rpc_count(rpc_count);
  }

  const ObCdcRpcId &get_client_id() const {
    return client_id_;
  }

  uint64_t get_tenant_id() const {
    return tenant_id_;
  }

  const share::ObLSID& get_ls_id() const {
    return ls_id_;
  }

  const palf::LSN& get_start_lsn() const {
    return start_lsn_;
  }

  void set_file_id(const int64_t file_id) {
    file_id_ = file_id;
  }

  int64_t get_file_id() const {
    return file_id_;
  }

  uint64_t get_req_size() const {
    return size_;
  }

  int64_t get_progress() const {
    return progress_;
  }

  void set_seq_no(const int32_t seq) {
    seq_ = seq;
  }

  int32_t get_seq_no() const {
    return seq_;
  }

  void set_current_round_rpc_count(const int32_t rpc_count) {
    cur_round_rpc_count_ = rpc_count;
  }

  int32_t get_current_round_rpc_count() const {
    return cur_round_rpc_count_;
  }

  void set_compressor_type(const common::ObCompressorType comp_type) {
    compressor_type_ = comp_type;
  }

  common::ObCompressorType get_compressor_type() const {
    return compressor_type_;
  }

  void set_flag(int8_t flag) {
    flag_ = flag;
  }

  int8_t get_flag() const {
    return flag_;
  }

  TO_STRING_KV(
    K(client_id_),
    K(tenant_id_),
    K(ls_id_),
    K(start_lsn_),
    K(file_id_),
    K(size_),
    K(progress_),
    K(seq_),
    K(cur_round_rpc_count_),
    K(compressor_type_),
    K(flag_)
  );

private:
	int64_t rpc_ver_;
	ObCdcRpcId client_id_;
	uint64_t tenant_id_;
	share::ObLSID ls_id_;
	palf::LSN start_lsn_;
	int64_t file_id_;
	uint64_t size_;
	int64_t progress_;
	int32_t seq_;
	int32_t cur_round_rpc_count_;
	ObCompressorType compressor_type_;
	int8_t flag_;
  ObCdcClientType client_type_;
};

enum class ObCdcFetchRawSource
{
  UNKNOWN,
  PALF,
  ARCHIVE,
};

class ObCdcFetchRawStatus
{
public:
  OB_UNIS_VERSION(1);
public:
  ObCdcFetchRawStatus() { reset(); }
  ~ObCdcFetchRawStatus() { reset(); }
  void reset() {
    source_ = ObCdcFetchRawSource::UNKNOWN;
    read_active_file_ = false;
    process_time_ = 0;
    read_palf_time_ = 0;
    local_to_svr_time_ = 0;
    queue_time_ = 0;
  }

  void set_source(const ObCdcFetchRawSource source) {
    source_ = source;
  }

  ObCdcFetchRawSource get_source() const {
    return source_;
  }

  void set_process_time(const int64_t ptime) {
    process_time_ = ptime;
  }

  int64_t get_process_time() const {
    return process_time_;
  }
  void set_read_palf_time(const int64_t rtime) {
    read_palf_time_ = rtime;
  }

  int64_t get_read_palf_time() const {
    return read_palf_time_;
  }

  void set_read_archive_time(const int64_t rtime) {
    read_archive_time_ = rtime;
  }

  int64_t get_read_archive_time() const {
    return read_archive_time_;
  }

  void set_local_to_svr_time(const int64_t l2s_time) {
    local_to_svr_time_ = l2s_time;
  }

  int64_t get_local_to_svr_time() const {
    return local_to_svr_time_;
  }

  void set_queue_time(const int64_t qtime) {
    queue_time_ = qtime;
  }

  int64_t get_queue_time() const {
    return queue_time_;
  }

  TO_STRING_KV(
    K(source_),
    K(read_active_file_),
    K(process_time_),
    K(read_palf_time_),
    K(read_archive_time_),
    K(local_to_svr_time_),
    K(queue_time_)
  );

private:
	ObCdcFetchRawSource source_;
	bool read_active_file_;
	int64_t process_time_;
	int64_t read_palf_time_;
  int64_t read_archive_time_;
  int64_t local_to_svr_time_;
	int64_t queue_time_;
};

class ObCdcFetchRawLogResp
{
private:
  static const int64_t FETCH_BUF_LEN = 1 << 24; // 16MB
public:
  OB_UNIS_VERSION(1);
public:
  ObCdcFetchRawLogResp() { reset(); }
  ~ObCdcFetchRawLogResp() { reset(); }

  void reset();

  // these info is get from rpc request, set them once.
  void reset(const share::ObLSID &ls_id,
       const int64_t file_id,
       const int32_t seq_no,
       const int32_t cur_round_rpc_count);

  void set_err(const int32_t err) {
    err_ = err;
  }

  int32_t get_err() const {
    return err_;
  }

  void set_feedback(const FeedbackType feedback) {
    feedback_type_ = feedback;
  }

  FeedbackType get_feedback() const {
    return feedback_type_;
  }

  ObCdcFetchRawStatus &get_fetch_status() {
    return fetch_status_;
  }

  const ObCdcFetchRawStatus &get_fetch_status() const {
    return fetch_status_;
  }

  void set_progress(const share::SCN progress) {
    server_progress_ = progress;
  }

  share::SCN get_progress() const {
    return server_progress_;
  }

  void set_read_size(const int64_t read_size) {
    read_size_ = read_size;
  }

  int64_t get_read_size() const {
    return read_size_;
  }

  void set_replayable_point_scn(const share::SCN &scn) {
    replayable_point_scn_ = scn;
  }

  const share::SCN &get_replayable_point_scn() const {
    return replayable_point_scn_;
  }

  const char *get_log_data() const {
    return log_entry_buf_;
  }

  char *get_log_buffer() {
    return log_entry_buf_;
  }

  int64_t get_buffer_len() {
    // assume that log_entry_buf is a array
    return sizeof(log_entry_buf_);
  }

  TO_STRING_KV(
    K(err_),
    K(ls_id_),
    K(file_id_),
    K(seq_),
    K(cur_round_rpc_count_),
    K(feedback_type_),
    K(fetch_status_),
    K(server_progress_),
    K(read_size_),
    K(replayable_point_scn_)
  );

private:
  int64_t rpc_ver_;
  int32_t err_;
  share::ObLSID ls_id_;
  int64_t file_id_;
  int32_t seq_;
  int32_t cur_round_rpc_count_;
  FeedbackType feedback_type_;
  ObCdcFetchRawStatus fetch_status_;
  share::SCN server_progress_;
  int64_t read_size_;
  share::SCN replayable_point_scn_;
  char log_entry_buf_[FETCH_BUF_LEN+palf::LOG_DIO_ALIGN_SIZE*2];
};

}

}

#endif