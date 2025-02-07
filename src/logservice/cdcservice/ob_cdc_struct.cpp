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

#define USING_LOG_PREFIX EXTLOG

#include "ob_cdc_struct.h"
#include "logservice/restoreservice/ob_remote_log_source_allocator.h"

namespace oceanbase
{

namespace cdc
{
///////////////////////////////////////////ClientLSKey///////////////////////////////////////////

ClientLSKey::ClientLSKey(
    const common::ObAddr &client_addr,
    const uint64_t client_pid,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id)
    : client_addr_(client_addr),
      client_pid_(client_pid),
      tenant_id_(tenant_id),
      ls_id_(ls_id)
{
}

uint64_t ClientLSKey::hash() const
{
  uint64_t hash_val = client_pid_;
  hash_val = murmurhash(&hash_val , sizeof(hash_val), client_addr_.hash());
  hash_val = murmurhash(&hash_val, sizeof(hash_val), ls_id_.hash());
  hash_val = murmurhash(&hash_val, sizeof(hash_val), tenant_id_);
  return hash_val;
}

int ClientLSKey::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

bool ClientLSKey::operator==(const ClientLSKey &that) const
{
  return client_addr_ == that.client_addr_ &&
         client_pid_ == that.client_pid_ &&
         tenant_id_ == that.tenant_id_ &&
         ls_id_ == that.ls_id_;
}

bool ClientLSKey::operator!=(const ClientLSKey &that) const
{
  return !(*this == that);
}

ClientLSKey &ClientLSKey::operator=(const ClientLSKey &that)
{
  client_addr_ = that.client_addr_;
  client_pid_ = that.client_pid_;
  tenant_id_ = that.tenant_id_;
  ls_id_ = that.ls_id_;
  return *this;
}

int ClientLSKey::compare(const ClientLSKey &key) const
{
  int ret = ls_id_.compare(key.ls_id_);
  if (0 == ret) {
    ret = client_addr_.compare(key.client_addr_);
  }
  if (0 == ret) {
    if (client_pid_ > key.client_pid_) {
      ret = 1;
    } else if (client_pid_ == key.client_pid_) {
      ret = 0;
    } else {
      ret = -1;
    }
  }
  return ret;
}

void ClientLSKey::reset()
{
  client_addr_.reset();
  client_pid_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_ = ObLSID::INVALID_LS_ID;
}

///////////////////////////////////////////ClientLSTrafficStat///////////////////////////////////////////

void ClientLSTrafficStat::record_rpc(const int64_t rpc_process_time,
    const int64_t queue_time,
    const int64_t read_log_time,
    const int64_t read_log_size)
{
  rpc_cnt_++;
  time_stat_.add_time(rpc_process_time, queue_time, read_log_time, read_log_size);
}

ClientLSTrafficStat& ClientLSTrafficStat::operator=(const ClientLSTrafficStat &that)
{
  last_snapshot_time_ = that.last_snapshot_time_;
  rpc_cnt_ = that.rpc_cnt_;
  time_stat_ = that.time_stat_;
  return *this;
}

///////////////////////////////////////////ClientLSCtx///////////////////////////////////////////

void ClientLSCtx::TrafficStatInfo::record_rpc(const int64_t rpc_process_time,
    const int64_t queue_time,
    const int64_t read_log_time,
    const int64_t read_log_size)
{
  SpinWLockGuard guard(traffic_lock_);
  cur_traffic_stat_.record_rpc(rpc_process_time, queue_time, read_log_time, read_log_size);
}

void ClientLSCtx::TrafficStatInfo::snapshot()
{
  SpinWLockGuard guard(traffic_lock_);
  cur_snapshot_idx_ = (cur_snapshot_idx_ + 1) % SNAPSHOT_NUM;
  cur_traffic_stat_.snapshot(traffic_stat_snapshot_[cur_snapshot_idx_]);
}

void ClientLSCtx::TrafficStatInfo::calc_avg_traffic_stat(
    int64_t &avg_process_time,
    int64_t &avg_queue_time,
    int64_t &avg_read_log_time,
    int64_t &avg_read_log_size,
    int64_t &avg_log_transport_bandwidth) const
{
  SpinRLockGuard guard(traffic_lock_);
  constexpr int64_t SECOND = 1000L * 1000;
  int64_t last_snapshot_idx = (cur_snapshot_idx_ + SNAPSHOT_NUM - 1) % SNAPSHOT_NUM;
  const ClientLSTrafficStat &cur_snapshot_info = traffic_stat_snapshot_[cur_snapshot_idx_];
  const ClientLSTrafficStat &last_snapshot_info = traffic_stat_snapshot_[last_snapshot_idx];
  const ClientLSTimeStat &cur_time_stat = cur_snapshot_info.time_stat_;
  const ClientLSTimeStat &last_time_stat = last_snapshot_info.time_stat_;
  const int64_t time_delta = cur_snapshot_info.last_snapshot_time_ - last_snapshot_info.last_snapshot_time_;
  const int64_t rpc_cnt_delta = cur_snapshot_info.rpc_cnt_ - last_snapshot_info.rpc_cnt_;
  const ClientLSTimeStat time_stat_delta = cur_time_stat - last_time_stat;
  if (0 == rpc_cnt_delta) {
    avg_process_time = 0;
    avg_queue_time = 0;
    avg_read_log_time = 0;
    avg_read_log_size = 0;
    avg_log_transport_bandwidth = 0;
    LOG_TRACE("rpc_cnt_delta is zero", K(cur_snapshot_idx_), K(last_snapshot_idx),
        "cur_snapshot_rpc_cnt", cur_snapshot_info.rpc_cnt_,
        "last_snapshot_rpc_cnt", last_snapshot_info.rpc_cnt_,
        "cur_snapshot_time", cur_snapshot_info.last_snapshot_time_,
        "last_snapshot_time", last_snapshot_info.last_snapshot_time_);
  } else if (0 < rpc_cnt_delta) {
    avg_process_time = time_stat_delta.rpc_process_time_ / rpc_cnt_delta;
    avg_queue_time = time_stat_delta.queue_time_ / rpc_cnt_delta;
    avg_read_log_time = time_stat_delta.read_log_time_ / rpc_cnt_delta;
    avg_read_log_size = time_stat_delta.read_log_size_ / rpc_cnt_delta;
    avg_log_transport_bandwidth = 0 >= time_delta ? 0 : time_stat_delta.read_log_size_ * SECOND / time_delta ;
    LOG_TRACE("finish calc_avg_traffic_stat",
        K(cur_time_stat), K(last_time_stat), K(time_stat_delta), K(cur_snapshot_idx_), K(last_snapshot_idx),
        "cur_snapshot_rpc_cnt", cur_snapshot_info.rpc_cnt_,
        "last_snapshot_rpc_cnt", last_snapshot_info.rpc_cnt_,
        "cur_snapshot_time", cur_snapshot_info.last_snapshot_time_,
        "last_snapshot_time", last_snapshot_info.last_snapshot_time_);
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "negtive rpc_cnt_delta, unexpected", K(rpc_cnt_delta));
  }
}

ClientLSCtx::ClientLSCtx()
  : source_lock_(ObLatchIds::CDC_SERVICE_LS_CTX_LOCK),
    source_version_(0),
    source_(NULL),
    proto_type_(obrpc::ObCdcFetchLogProtocolType::UnknownProto),
    fetch_mode_(FetchMode::FETCHMODE_UNKNOWN),
    last_touch_ts_(OB_INVALID_TIMESTAMP),
    client_progress_(OB_INVALID_TIMESTAMP),
    create_ts_(ObTimeUtility::current_time()),
    client_type_(obrpc::ObCdcClientType::CLIENT_TYPE_UNKNOWN),
    client_lsn_(palf::LOG_INVALID_LSN_VAL),
    failed_rpc_info_(),
    traffic_stat_info_()
{
  update_touch_ts();
}

ClientLSCtx::~ClientLSCtx()
{
  reset();
}

int ClientLSCtx::init(int64_t client_progress,
    const obrpc::ObCdcFetchLogProtocolType type,
    const obrpc::ObCdcClientType client_type)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TIMESTAMP != client_progress) {
    set_progress(client_progress);
    set_fetch_mode(FetchMode::FETCHMODE_ONLINE, "ClientLSCtxInit");
    set_client_type(client_type);
    update_touch_ts();
    set_proto_type(type);
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("client progress is invalid", KR(ret), K(client_progress));
  }
  return ret;
}

int ClientLSCtx::try_init_archive_source(const ObLSID &ls_id,
    const ObBackupDest &archive_dest,
    const int64_t dest_version)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(source_)) {
    ret = OB_INIT_TWICE;
    LOG_TRACE("archive source is not null, no need to init");
  } else {
    SpinWLockGuard ctx_source_guard(source_lock_);
    logservice::ObRemoteLogParent *tmp_source = nullptr;

    // double check to avoid concurrency issue
    if (OB_NOT_NULL(source_)) {
      ret = OB_INIT_TWICE;
      LOG_INFO("archive source is not null, no need to init");
    } else if (! archive_dest.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("archive dest is not valid", K(archive_dest), K(dest_version));
    } else if (OB_ISNULL(tmp_source = logservice::ObResSrcAlloctor::alloc(ObLogRestoreSourceType::LOCATION, ls_id))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc RemoteLocationParent failed", KR(ret), K(ls_id));
    } else if (OB_FAIL(static_cast<logservice::ObRemoteLocationParent*>(tmp_source)->set(
        archive_dest, SCN::max_scn()))) {
        LOG_WARN("source set archive dest info failed", KR(ret), K(archive_dest));
    } else if (OB_FAIL(set_source_version_(tmp_source, dest_version))) {
      // expect set success, source should be null and dest_info_version should be 0
      LOG_WARN("failed to set source and version", K(dest_version),
          K(archive_dest), K(ls_id));
    } else {
      LOG_INFO("init archive source succ", K(archive_dest), K(dest_version), K(ls_id));
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(tmp_source)) {
      logservice::ObResSrcAlloctor::free(tmp_source);
    }
  }

  return ret;
}

int ClientLSCtx::try_deep_copy_source(const ObLSID &ls_id,
    logservice::ObRemoteLogParent *&target_src,
    int64_t &version) const
{
  int ret = OB_SUCCESS;

  SpinRLockGuard ctx_source_guard(source_lock_);
  if (OB_ISNULL(source_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source in ClientLSCtx is null, cannot get source from ctx", KR(ret), K(ls_id));
  } else {
    logservice::ObRemoteLogParent *tmp_source = logservice::ObResSrcAlloctor::alloc(
        source_->get_source_type(), ls_id);
    if (OB_ISNULL(tmp_source)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("source allocated is null, allocate failed", KR(ret), K(ls_id));
    } else if (OB_FAIL(source_->deep_copy_to(*tmp_source))) {
      LOG_WARN("deep copy from source in ctx failed", KR(ret), K(ls_id));
    } else {
      target_src = tmp_source;
      version = source_version_;
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(tmp_source)) {
      logservice::ObResSrcAlloctor::free(tmp_source);
    }
  }

  return ret;
}

int ClientLSCtx::try_update_archive_source(logservice::ObRemoteLogParent *target_source,
    const int64_t source_ver)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(target_source)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get null source when trying to update archive source in ctx", KPC(this), K(source_ver), KP(target_source));
  } else {
    SpinWLockGuard ctx_source_guard(source_lock_);
    if (OB_ISNULL(source_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null source in ctx, unexpected", KPC(this));
    } else if (source_ver < source_version_) {
      LOG_INFO("no need to update archive source whose version is higher", K(source_ver), K(source_version_));
    } else if (source_ver > source_version_) {
      // target source should come from ctx some time ago, if it's newer than the source in ctx,
      // it should be some concurrency issue which is unexpected, because the source in ctx should
      // never rollback.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("target source is newer than the source in ctx, unexpected", K(source_ver), K(source_version_));
    } else if (source_->get_source_type() != target_source->get_source_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("source type doesn't match", KPC(source_), KPC(target_source));
    } else if (OB_FAIL(source_->update_locate_info(*target_source))) {
      LOG_WARN("update locate info failed", KPC(source_), KPC(target_source));
    }
  }

  return ret;
}

int ClientLSCtx::try_change_archive_source(const ObLSID &ls_id,
    const ObBackupDest &dest,
    const int64_t source_ver)
{
  int ret = OB_SUCCESS;

  SpinWLockGuard ctx_source_guard(source_lock_);
  if (nullptr == source_) {
    // ignore & continue
  } else if (source_ver > source_version_) {
    logservice::ObRemoteLocationParent *new_source = static_cast<logservice::ObRemoteLocationParent*>(
        logservice::ObResSrcAlloctor::alloc(ObLogRestoreSourceType::LOCATION, ls_id));
    if (OB_ISNULL(new_source)) {
      // continue
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc new source", K(ls_id));
    } else if (OB_FAIL(new_source->set(dest, SCN::max_scn()))) {
      // fatal error, not continue
      LOG_WARN("failed to set archive dest", K(dest), K(ls_id));
    } else if (OB_FAIL(set_source_version_(new_source, source_ver))) {
      if (OB_NO_NEED_UPDATE == ret) {
        LOG_INFO("no need update source", K(source_ver), K(source_version_));
      } else {
        LOG_WARN("failed to set source version", K(dest), K(ls_id), K(source_ver));
      }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(new_source)) {
      logservice::ObResSrcAlloctor::free(new_source);
    }
  } else {
    // old or equal version, not update
  }

  return ret;
}

void ClientLSCtx::reset()
{
  if (NULL != source_) {
    logservice::ObResSrcAlloctor::free(source_);
    source_ = NULL;
    source_version_ = 0;
  }
  proto_type_ = obrpc::ObCdcFetchLogProtocolType::UnknownProto;
  fetch_mode_ = FetchMode::FETCHMODE_UNKNOWN;
  last_touch_ts_ = OB_INVALID_TIMESTAMP;
  client_progress_ = OB_INVALID_TIMESTAMP;
  client_type_ = obrpc::ObCdcClientType::CLIENT_TYPE_UNKNOWN;
  client_lsn_.reset();
  failed_rpc_info_.reset();
  traffic_stat_info_.reset();
}

void ClientLSCtx::set_source_(logservice::ObRemoteLogParent *source)
{
  logservice::ObRemoteLogParent *origin_source = source_;
  source_ = source;
  if (NULL != origin_source) {
    logservice::ObResSrcAlloctor::free(origin_source);
  }

}

int ClientLSCtx::set_source_version_(logservice::ObRemoteLogParent *source, const int64_t version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(source)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get null source when set source", KP(source), K(version));
  } else if (version > source_version_) {
    set_source_(source);
    source_version_ = version;
  } else {
    ret = OB_NO_NEED_UPDATE;
  }

  return ret;
}

//////////////////////////////////////////////////////////////////////////
int ObCdcGetSourceFunctor::operator()(const share::ObLSID &id, logservice::ObRemoteSourceGuard &guard) {
  int ret = OB_SUCCESS;
  logservice::ObRemoteLogParent *source = nullptr;
  if (OB_FAIL(ctx_.try_deep_copy_source(id, source, version_))) {
    LOG_WARN("failed to deep copy source", K(id), K(ctx_));
  } else if (OB_ISNULL(source)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null source after deep copy, unexpected", KP(source), K(id));
  } else if (OB_FAIL(guard.set_source(source))) {
    LOG_WARN("RemoteSourceGuard set source failed", KR(ret));
  } else { }
  return ret;
}

int ObCdcUpdateSourceFunctor::operator()(const share::ObLSID &id, logservice::ObRemoteLogParent *source) {
  int ret = OB_SUCCESS;
  UNUSED(id);
  if (OB_ISNULL(source)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source is null when updating source", KR(ret), K(id));
  } else if (OB_FAIL(ctx_.try_update_archive_source(source, version_))) {
    LOG_WARN("failed to update source in ctx", KR(ret), K(id));
  }
  return ret;
}

} // cdc
} // oceanbase
