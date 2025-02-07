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
 *
 * Fetching log-related RPC implementation
 */

#ifndef OCEANBASE_LOG_FILE_BUFFER_POOL_H_
#define OCEANBASE_LOG_FILE_BUFFER_POOL_H_

#include "lib/container/ob_ext_ring_buffer.h"
#include "lib/queue/ob_link_queue.h"
#include "ob_log_fetch_log_rpc_result.h"
#include "ob_log_fetch_log_rpc_req.h"
#include "logservice/palf/lsn.h"
#include "share/scn.h"
namespace oceanbase
{
namespace logfetcher
{
////////////////////////////// LogFileDataBufferPool //////////////////////////////

struct BufferWriteMeta
{
  BufferWriteMeta():
    is_valid_(false),
    write_lsn_(),
    write_size_(0),
    next_req_lsn_(),
    replayable_point_(),
    sub_rpc_status_()
    {}

  TO_STRING_KV(
    K(write_lsn_),
    K(write_size_),
    K(next_req_lsn_),
    K(replayable_point_),
    K(sub_rpc_status_)
  )

  void reset() {
    is_valid_ = false;
    write_lsn_.reset();
    write_size_ = 0;
    next_req_lsn_.reset();
    replayable_point_.reset();
    sub_rpc_status_.reset();
  }

  void reset(const palf::LSN write_lsn,
      const palf::LSN next_req_lsn,
      const palf::offset_t write_size,
      const share::SCN replayable_point,
      const obrpc::ObCdcFetchRawStatus &status,
      const obrpc::FeedbackType feed_back,
      const int err,
      const obrpc::ObRpcResultCode &rcode,
      const int64_t rpc_cb_start_time,
      const int64_t sub_rpc_send_time)
  {
    const int64_t cur_time = get_timestamp();
    is_valid_ = true;
    write_lsn_ = write_lsn;
    write_size_ = write_size;
    next_req_lsn_ = next_req_lsn;
    replayable_point_ = replayable_point;
    sub_rpc_status_.reset(rcode, err, feed_back, status,
        cur_time - rpc_cb_start_time, cur_time - sub_rpc_send_time);
  }

  bool is_valid_;
  palf::LSN write_lsn_;
  palf::offset_t write_size_;
  palf::LSN next_req_lsn_;
  share::SCN replayable_point_;
  RawLogDataRpcStatus sub_rpc_status_;
};

class LogFileDataBufferPool;

class LogFileDataBuffer: public ObLink
{
private:
  static constexpr int64_t EXPIRED_THRESHOLD = 10L * 1000 * 1000;
public:
  LogFileDataBuffer();
  ~LogFileDataBuffer() { destroy(); }
  int init(LogFileDataBufferPool *host,
      const int64_t max_wr_cnt,
      const uint64_t tenant_id,
      const bool is_dynamic);

  // data buffers could be reused via this method
  int prepare(const int64_t cur_wr_cnt,
      const palf::LSN &start_lsn,
      const void *owner);

  void reset_param();

  void deliver_owner(const void *new_owner) {
    owner_ = new_owner;
  }

  // cannot access after revert
  void revert();

  void destroy();

  // write data to buffer, also write meta.
  // data is not a must, but meta is necessary.
  int write_data(const int32_t seq_no,
      const char *data,
      const int64_t data_len,
      const palf::LSN data_start_lsn,
      const palf::LSN next_req_lsn,
      const share::SCN replayable_point,
      const obrpc::ObCdcFetchRawStatus &fetch_status,
      const obrpc::FeedbackType feed_back,
      const int err,
      const obrpc::ObRpcResultCode &rcode,
      const int64_t rpc_cb_start_time,
      const int64_t sub_rpc_send_time);

  // there may be some write operation not continuous.
  // assemble write logs, get continuous log data and real data len
  int get_data(const char *&data,
      int64_t &data_len,
      int32_t &valid_rpc_cnt,
      bool &is_readable,
      bool &is_active,
      obrpc::ObCdcFetchRawSource &data_end_source,
      share::SCN &replayable_point,
      ObIArray<RawLogDataRpcStatus> &sub_rpc_status_arr);

  bool is_dynamic() const {
    return is_dynamic_;
  }

  bool is_expired() const {
    return ObTimeUtility::current_time() - access_ts_ >= EXPIRED_THRESHOLD;
  }

  TO_STRING_KV(
    K(tenant_id_),
    K(access_ts_),
    K(max_write_cnt_),
    K(cur_write_cnt_),
    K(complete_cnt_),
    K(start_lsn_),
    K(is_dynamic_),
    KP(owner_),
    K(buffer_size_),
    KP(data_buffer_)
  );

private:
  void reset_write_log_arr_();

  int prepare_write_buffer_();

  int set_buffer_(char *buf, const size_t buf_size);

private:
  LogFileDataBufferPool *host_;
  uint64_t tenant_id_;
  int64_t access_ts_;
  int64_t max_write_cnt_;
  int64_t cur_write_cnt_;
  int64_t complete_cnt_;
  palf::LSN start_lsn_;
  bool is_dynamic_;
  const void *owner_;
  BufferWriteMeta write_logs_[RawLogFileRpcRequest::MAX_SEND_REQ_CNT];
  size_t buffer_size_;
  char *data_buffer_;
};

class LogFileDataBufferPool
{
public:
  LogFileDataBufferPool();
  ~LogFileDataBufferPool() { destroy(); }

  int init(const uint64_t tenant_id,
      const int64_t max_write_cnt,
      const int64_t max_hold_buffer_cnt);
  void destroy();

  int get(const int64_t cur_wr_cnt,
          const palf::LSN &start_lsn,
          const void *owner,
          LogFileDataBuffer *&buffer);

  void revert(LogFileDataBuffer *buffer);

  void try_recycle_expired_buffer();

  TO_STRING_KV(
    K(is_inited_),
    K(tenant_id_),
    K(max_write_cnt_),
    K(max_hold_buffer_cnt_),
    K(allocated_req_cnt_)
  )

private:
  int alloc_static_buffer_(LogFileDataBuffer *&buffer);

  void update_allocated_req_cnt_(const int64_t delta);

private:
  bool is_inited_;
  SpinRWLock lock_;
  uint64_t tenant_id_;
  int64_t max_write_cnt_;
  int64_t max_hold_buffer_cnt_ CACHE_ALIGNED;
  int64_t allocated_req_cnt_ CACHE_ALIGNED;
  ObSpLinkQueue free_list_;
};

}
}

#endif