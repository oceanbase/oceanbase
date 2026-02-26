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

#ifndef OCEANBASE_LOGSERVICE_OB_CDC_SERVICE_STRUCT_H_
#define OCEANBASE_LOGSERVICE_OB_CDC_SERVICE_STRUCT_H_

#include "logservice/restoreservice/ob_remote_log_iterator.h" // ObRemoteLogGroupEntryIterator
#include "share/backup/ob_backup_struct.h" // ObBackupPathString
#include "ob_cdc_req_struct.h"
#include "share/ob_ls_id.h" // ObLSID

namespace oceanbase
{
namespace logservice
{
class ObRemoteLogParent;
class ObLogArchivePieceContext;
class ObRemoteSourceGuard;
}
namespace cdc
{

class ClientLSCtx;

class ObCdcGetSourceFunctor {
public:
  explicit ObCdcGetSourceFunctor(ClientLSCtx &ctx, int64_t &version): ctx_(ctx), version_(version) {}
  int operator()(const share::ObLSID &id, logservice::ObRemoteSourceGuard &guard);

private:
  ClientLSCtx &ctx_;
  int64_t &version_;
};

class ObCdcUpdateSourceFunctor {
public:
  explicit ObCdcUpdateSourceFunctor(ClientLSCtx &ctx, int64_t &version):
      ctx_(ctx),
      version_(version) {}
  int operator()(const share::ObLSID &id, logservice::ObRemoteLogParent *source);
private:
  ClientLSCtx &ctx_;
  int64_t &version_;
};

// Temporarily not supported
class ObCdcRefreshStorageInfoFunctor {
public:
  int operator()(share::ObBackupDest &dest) {
    UNUSED(dest);
    return OB_SUCCESS;
  }
};

class ClientLSKey
{
public:
  ClientLSKey():
      client_addr_(),
      client_pid_(0),
      tenant_id_(OB_INVALID_TENANT_ID),
      ls_id_(share::ObLSID::INVALID_LS_ID)
      { }
  ClientLSKey(const common::ObAddr &client_addr,
              const uint64_t client_pid,
              const uint64_t tenant_id,
              const share::ObLSID &ls_id);
  ~ClientLSKey() { reset(); }
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  bool operator==(const ClientLSKey &that) const;
  bool operator!=(const ClientLSKey &that) const;
  ClientLSKey &operator=(const ClientLSKey &that);
  int compare(const ClientLSKey &key) const;
  void reset();

  const common::ObAddr &get_client_addr() const {
    return client_addr_;
  }

  uint64_t get_client_pid() const {
    return client_pid_;
  }

  uint64_t get_tenant_id() const {
    return tenant_id_;
  }

  const ObLSID &get_ls_id() const {
    return ls_id_;
  }


  TO_STRING_KV(K_(client_addr), K_(client_pid), K_(ls_id))

private:
  common::ObAddr client_addr_;
  uint64_t client_pid_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
};

enum FetchMode {
  FETCHMODE_UNKNOWN = 0,

  FETCHMODE_ONLINE,
  FETCHMODE_ARCHIVE,

  FETCHMODE_MAX
};

struct ClientLSTimeStat
{
  ClientLSTimeStat() { reset(); }
  ~ClientLSTimeStat() { reset(); }
  void reset() {
    rpc_process_time_ = 0;
    queue_time_ = 0;
    read_log_time_ = 0;
    read_log_size_ = 0;
  }

  void add_time(const int64_t rpc_process_time,
      const int64_t queue_time,
      const int64_t read_log_time,
      const int64_t read_log_size)
  {
    rpc_process_time_ += rpc_process_time;
    queue_time_ += queue_time;
    read_log_time_ += read_log_time;
    read_log_size_ += read_log_size;
  }

  void snapshot(ClientLSTimeStat &stat)
  {
    stat.rpc_process_time_ = rpc_process_time_;
    stat.queue_time_ = queue_time_;
    stat.read_log_time_ = read_log_time_;
    stat.read_log_size_ = read_log_size_;
  }

  ClientLSTimeStat operator-(const ClientLSTimeStat &rhs) const
  {
    ClientLSTimeStat rs;
    rs.rpc_process_time_ = rpc_process_time_ - rhs.rpc_process_time_;
    rs.queue_time_ = queue_time_ - rhs.queue_time_;
    rs.read_log_time_ = read_log_time_ - rhs.read_log_time_;
    rs.read_log_size_ = read_log_size_ - rhs.read_log_size_;
    return rs;
  }

  ClientLSTimeStat &operator=(const ClientLSTimeStat &that)
  {
    rpc_process_time_ = that.rpc_process_time_;
    queue_time_ = that.queue_time_;
    read_log_time_ = that.read_log_time_;
    read_log_size_ = that.read_log_size_;
    return *this;
  }

  TO_STRING_KV(K(rpc_process_time_), K(queue_time_), K(read_log_time_), K(read_log_size_));

  int64_t rpc_process_time_;
  int64_t queue_time_;
  int64_t read_log_time_;
  int64_t read_log_size_;
};

struct ClientLSTrafficStat
{
  ClientLSTrafficStat() { reset(); }
  ~ClientLSTrafficStat() { reset(); }
  void reset()
  {
    last_snapshot_time_ = ObTimeUtility::current_time();
    rpc_cnt_ = 0;
    time_stat_.reset();
  }
  void record_rpc(const int64_t rpc_process_time,
      const int64_t queue_time,
      const int64_t read_log_time,
      const int64_t read_log_size);

  ClientLSTrafficStat& operator=(const ClientLSTrafficStat& that);

  void snapshot(ClientLSTrafficStat &that)
  {
    that = *this;
    that.last_snapshot_time_ = ObTimeUtility::current_time();
  }

  int64_t last_snapshot_time_;
  int64_t rpc_cnt_;
  ClientLSTimeStat time_stat_;
};

class ClientLSCtx: public common::LinkHashValue<ClientLSKey>
{
public:
  ClientLSCtx();
  ~ClientLSCtx();

  class RpcRequestInfo
  {
  public:
    RpcRequestInfo():
      req_lock_() { reset(); }
    ~RpcRequestInfo() { reset(); }
    void reset()
    {
      req_start_time_ = OB_INVALID_TIMESTAMP;
      trace_id_.reset();
      req_ret_ = OB_SUCCESS;
      req_feed_back_ = obrpc::FeedbackType::INVALID_FEEDBACK;
    }

    bool is_valid() const {
      SpinRLockGuard guard(req_lock_);
      return req_start_time_ != OB_INVALID_TIMESTAMP;
    }

    void invalidate(const int64_t req_start_time,
        const ObCurTraceId::TraceId &trace_id,
        const int req_ret,
        const obrpc::FeedbackType feed_back) {
      SpinWLockGuard guard(req_lock_);
      if (trace_id_ != trace_id) {
        reset();
      }
    }

    void record_rpc(const int64_t req_start_time,
        const ObCurTraceId::TraceId &trace_id,
        const int req_ret,
        const obrpc::FeedbackType feed_back)
    {
      SpinWLockGuard guard(req_lock_);
      req_start_time_ = req_start_time;
      trace_id_ = trace_id;
      req_ret_ = req_ret;
      req_feed_back_ = feed_back;
    }

    void get_req_info(int64_t &req_start_time,
        ObCurTraceId::TraceId &trace_id,
        int &req_ret,
        obrpc::FeedbackType &feed_back) const
    {
      SpinRLockGuard guard(req_lock_);
      req_start_time = req_start_time_;
      trace_id = trace_id_;
      req_ret = req_ret_;
      feed_back = req_feed_back_;
    }

    int to_string_rdlock(char *buf, const int64_t buf_len) const
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;

      SpinRLockGuard guard(req_lock_);

      if (OB_FAIL(databuff_print_kv(buf, buf_len, pos,
          "request_start_time", req_start_time_,
          "trace_id", trace_id_,
          "request_ret", req_ret_,
          "request_feedback", obrpc::feedback_type_str(req_feed_back_)))) {
      }

      return ret;
    }

  private:
    SpinRWLock req_lock_;
    int64_t req_start_time_;
    ObCurTraceId::TraceId trace_id_;
    int req_ret_;
    obrpc::FeedbackType req_feed_back_;
  };

  class TrafficStatInfo
  {
  public:
    static constexpr int SNAPSHOT_NUM = 2;
    TrafficStatInfo():
      traffic_lock_()
    { reset(); }
    ~TrafficStatInfo() { reset(); }
    void reset()
    {
      cur_snapshot_idx_ = 0;
      for (int i = 0; i < SNAPSHOT_NUM; i++) {
        traffic_stat_snapshot_[i].reset();
      }
      cur_traffic_stat_.reset();
    }

    void record_rpc(const int64_t rpc_process_time,
        const int64_t queue_time,
        const int64_t read_log_time,
        const int64_t read_log_size);
    void snapshot();

    void calc_avg_traffic_stat(int64_t &avg_process_time,
        int64_t &avg_queue_time,
        int64_t &avg_read_log_time,
        int64_t &avg_read_log_size,
        int64_t &avg_log_transport_bandwidth) const;

  private:
    SpinRWLock traffic_lock_;
    int64_t cur_snapshot_idx_;
    ClientLSTrafficStat traffic_stat_snapshot_[SNAPSHOT_NUM];
    ClientLSTrafficStat cur_traffic_stat_;
  };

public:
  int init(const int64_t client_progress,
      const obrpc::ObCdcFetchLogProtocolType proto,
      const obrpc::ObCdcClientType client_type);

  // thread safe method,
  // OB_INIT_TWICE: archive source has been inited;
  // OB_ALLOCATE_MEMORY_FAILED
  // OB_NO_NEED_UPDATE: there is a newer archive dest, which is unexpected
  // other
  int try_init_archive_source(const ObLSID &ls_id,
      const ObBackupDest &archive_dest,
      const int64_t dest_version);

  // thread safe method
  // deep copy source in ctx to target_src, would allocate memory for target_src.
  // caller should call logservice::ObResSrcAlloctor::free to free target_src
  int try_deep_copy_source(const ObLSID &ls_id,
      logservice::ObRemoteLogParent *&target_src,
      int64_t &version) const;

  // thread safe method
  // write the info of current source to source in ctx.
  int try_update_archive_source(logservice::ObRemoteLogParent *source,
      const int64_t source_ver);

  // thread safe method
  int try_change_archive_source(const ObLSID &ls_id,
      const ObBackupDest &dest,
      const int64_t source_ver);

  bool archive_source_inited() const {
    SpinRLockGuard guard(source_lock_);
    return nullptr != source_;
  }

  // make sure only one thread would call this method.
  void reset();

  void set_proto_type(const obrpc::ObCdcFetchLogProtocolType type) {
    obrpc::ObCdcFetchLogProtocolType from = proto_type_, to = type;
    ATOMIC_STORE(&proto_type_, type);
    if (from != to) {
      EXTLOG_LOG(INFO, "set fetch protocol ", K(from), K(to));
    }
  }

  // non-thread safe method
  obrpc::ObCdcFetchLogProtocolType get_proto_type() const {
    return ATOMIC_LOAD(&proto_type_);
  }

  // non-thread safe method
  void set_fetch_mode(FetchMode mode, const char *reason) {
    FetchMode from = fetch_mode_, to = mode;
    ATOMIC_STORE(&fetch_mode_, mode);
    EXTLOG_LOG(INFO, "set fetch mode ", K(from), K(to), K(reason));
  }

  // non-thread safe method
  FetchMode get_fetch_mode() const { return ATOMIC_LOAD(&fetch_mode_); }

  // non-thread safe method
  void update_touch_ts() { ATOMIC_STORE(&last_touch_ts_, ObTimeUtility::current_time()); }

  // non-thread safe method
  int64_t get_touch_ts() const { return ATOMIC_LOAD(&last_touch_ts_); }

  // non-thread safe method
  void set_progress(int64_t progress) { ATOMIC_STORE(&client_progress_, progress); }

  // non-thread safe method
  int64_t get_progress() const { return ATOMIC_LOAD(&client_progress_); }

  void record_rpc_stat(const int64_t queue_time,
      const int64_t process_time,
      const int64_t read_log_time,
      const int64_t read_log_size)
  {
    traffic_stat_info_.record_rpc(process_time, queue_time, read_log_time, read_log_size);
  }

  void record_rpc_info(const int64_t rpc_start_time,
      const ObCurTraceId::TraceId &trace_id,
      const int ret_code,
      const obrpc::FeedbackType feed_back)
  {
    if (OB_SUCCESS == ret_code && feed_back == obrpc::FeedbackType::INVALID_FEEDBACK) {
      if (failed_rpc_info_.is_valid()) {
        failed_rpc_info_.invalidate(rpc_start_time, trace_id, ret_code, feed_back);
      }
    } else {
      failed_rpc_info_.record_rpc(rpc_start_time, trace_id, ret_code, feed_back);
    }
  }

  void set_client_type(const obrpc::ObCdcClientType client_type) {
    ATOMIC_STORE(&client_type_, client_type);
  }

  obrpc::ObCdcClientType get_client_type() const {
    return client_type_;
  }

  int64_t get_create_ts() const {
    return create_ts_;
  }

  void set_req_lsn(const palf::LSN lsn) {
    ATOMIC_STORE(&client_lsn_.val_, lsn.val_);
  }

  palf::LSN get_req_lsn() const {
    return palf::LSN(ATOMIC_LOAD(&client_lsn_.val_));
  }

  const RpcRequestInfo &get_failed_rpc_info() const {
    return failed_rpc_info_;
  }

  const TrafficStatInfo &get_traffic_stat_info() const {
    return traffic_stat_info_;
  }

  void snapshot_for_traffic_stat() {
    traffic_stat_info_.snapshot();
  }

  TO_STRING_KV(KP(source_),
               K(source_version_),
               K(proto_type_),
               K(fetch_mode_),
               K(last_touch_ts_),
               K(client_progress_),
               K(create_ts_),
               K(client_lsn_))

private:
  // caller need to hold source_lock_ before invoke this method
  int set_source_version_(logservice::ObRemoteLogParent *source, const int64_t version);
  // caller need to hold source_lock_ before invoke this method
  void set_source_(logservice::ObRemoteLogParent *source);

private:
  SpinRWLock source_lock_;
  // GUARDED_BY(source_lock_)
  int64_t source_version_;
  // GUARDED_BY(source_lock_)
  logservice::ObRemoteLogParent *source_;
  // stat, it's ok even if it's not correct.
  // only set when init, can hardly be wrong
  obrpc::ObCdcFetchLogProtocolType proto_type_;
  // it concerns about the thread num of log_ext_storage_handler,
  // should be eventually correct.
  FetchMode fetch_mode_;
  // it's used for recycling context, would be updated when fetching log
  // though it may not be precise, it wouldn't deviate a lot, unless some
  // threads get stuck for a long time or clock drift occurs.
  int64_t last_touch_ts_;
  // for fetch_raw_log protocol, it's not used.
  int64_t client_progress_;
  const int64_t create_ts_;
  obrpc::ObCdcClientType client_type_;

  palf::LSN client_lsn_;

  RpcRequestInfo failed_rpc_info_;

  TrafficStatInfo traffic_stat_info_;
};

typedef common::ObSEArray<std::pair<int64_t, share::ObBackupPathString>, 1> ObArchiveDestInfo;
typedef ObLinkHashMap<ClientLSKey, ClientLSCtx> ClientLSCtxMap;
}
}

#endif
