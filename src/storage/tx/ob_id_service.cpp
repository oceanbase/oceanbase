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

#include "ob_id_service.h"
#include "ob_trans_service.h"
#include "ob_timestamp_service.h"
#include "ob_trans_id_service.h"
#include "ob_tx_ls_log_writer.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/slog/ob_storage_log.h"
#include "storage/slog/ob_storage_log_replayer.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "logservice/ob_log_base_header.h"
#include "share/scn.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "sql/das/ob_das_id_service.h"

namespace oceanbase
{
using namespace share;
using namespace palf;
namespace transaction
{
void ObIDService::reset()
{
  service_type_ = INVALID_ID_SERVICE_TYPE;
  pre_allocated_range_ = 0;
  last_id_ = MIN_LAST_ID;
  tmp_last_id_ = 0;
  limited_id_ = MIN_LAST_ID;
  is_logging_ = false;
  rec_log_ts_.set_max();
  latest_log_ts_.reset();
  submit_log_ts_ = OB_INVALID_TIMESTAMP;
  ls_ = NULL;
}

int ObIDService::submit_log_with_lock_(const int64_t last_id, const int64_t limited_id)
{
  int ret = OB_SUCCESS;
  int locked = false;
  if (OB_UNLIKELY(!(locked = rwlock_.try_wrlock()))) {
    ret = OB_EAGAIN;
  } else if (OB_FAIL(submit_log_(last_id, limited_id))) {
    if (OB_EAGAIN == ret) {
      if (EXECUTE_COUNT_PER_SEC(10)) {
        TRANS_LOG(INFO, "submit log with lock", K(ret), K(service_type_), K(last_id), K(limited_id));
      }
    } else {
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        TRANS_LOG(WARN, "submit log fail", K(ret), K(service_type_), K(last_id), K(limited_id));
      }
    }
  } else {
    TRANS_LOG(INFO, "submit log with lock success", K(service_type_), K(last_id), K(limited_id));
  }
  if (locked) {
    rwlock_.unlock();
  }
  return ret;
}

int ObIDService::check_and_fill_ls()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ls_)) {
    ObLSService *ls_svr =  MTL(ObLSService *);
    ObLSHandle handle;
    ObLS *ls = nullptr;

    if (OB_ISNULL(ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "log stream service is NULL", K(ret));
    } else if (OB_FAIL(ls_svr->get_ls(IDS_LS, handle, ObLSGetMod::TRANS_MOD))) {
      TRANS_LOG(WARN, "get id service log stream failed");
    } else if (OB_ISNULL(ls = handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "id service log stream not exist");
    } else {
      ls_ = ls;
      TRANS_LOG(INFO, "ls set success");
    }
  }
  return ret;
}

void ObIDService::reset_ls()
{
  WLockGuard guard(rwlock_);
  ls_ = NULL;
}

int ObIDService::submit_log_(const int64_t last_id, const int64_t limited_id)
{
  int ret = OB_SUCCESS;
  if (is_logging_) {
    ret = OB_EAGAIN;
    if (EXECUTE_COUNT_PER_SEC(20)) {
      TRANS_LOG(INFO, "is logging");
    }
    if (submit_log_ts_ == OB_INVALID_TIMESTAMP) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "invalid argument", K(ret), K_(service_type), K_(latest_log_ts), K_(self));
    } else if (ObTimeUtility::current_time() - submit_log_ts_ > SUBMIT_LOG_ALARM_INTERVAL) {
      if (log_interval_.reach()) {
        // ignore ret
        TRANS_LOG(WARN, "submit log callback use too mush time", K_(submit_log_ts), K_(cb), K_(service_type), K_(latest_log_ts), K_(self));
      }
    }
  } else if (last_id < 0 || limited_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(last_id), K(limited_id));
  } else if (limited_id <= ATOMIC_LOAD(&limited_id_)) {
    if (EXECUTE_COUNT_PER_SEC(10)) {
      TRANS_LOG(INFO, "no log required", K(limited_id), K(ATOMIC_LOAD(&limited_id_)));
    }
  } else if (OB_FAIL(check_and_fill_ls())) {
    TRANS_LOG(WARN, "ls set fail", K(ret));
  } else {
    ObPresistIDLog ls_log(last_id, limited_id);
    palf::LSN lsn;
    SCN log_ts, base_scn;
    int64_t base_ts = 0;
    if (TimestampService == service_type_) {
      if (ATOMIC_LOAD(&tmp_last_id_) != 0) {
        base_ts = ATOMIC_LOAD(&tmp_last_id_);
      } else {
        base_ts = ATOMIC_LOAD(&last_id_);
      }
    }
    if (OB_FAIL(base_scn.convert_for_gts(base_ts))) {
      TRANS_LOG(ERROR, "failed to convert scn", KR(ret), K(base_ts));
    } else if (OB_FAIL(cb_.serialize_ls_log(ls_log, service_type_))) {
      TRANS_LOG(WARN, "serialize ls log error", KR(ret), K(cb_));
    } else {
      cb_.set_srv_type(service_type_);
      if (OB_FAIL(ls_->get_log_handler()->append(cb_.get_log_buf(), cb_.get_log_pos(), base_scn,
                                                 false, false/*allow_compression*/, &cb_, lsn, log_ts))) {
        cb_.reset();
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          TRANS_LOG(WARN, "submit ls log failed", KR(ret), K(service_type_));
        }
      } else {
        submit_log_ts_ = ObTimeUtility::current_time();
        cb_.set_log_ts(log_ts);
        cb_.set_limited_id(limited_id);
        is_logging_ = true;
        TRANS_LOG(INFO, "submit log success", K(service_type_), K(last_id), K(limited_id));
      }
    }
  }

  return ret;
}

int ObIDService::handle_submit_callback(const bool success, const int64_t limited_id, const SCN log_ts)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);
  if (limited_id < 0 || !log_ts.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(limited_id), K(log_ts));
  } else if (success) {
    if (ATOMIC_LOAD(&tmp_last_id_) != 0) {
      ATOMIC_STORE(&last_id_, tmp_last_id_);
      ATOMIC_STORE(&tmp_last_id_, 0);
    }
    (void)inc_update(&limited_id_, limited_id);
    rec_log_ts_.atomic_set(log_ts);
    latest_log_ts_.atomic_set(log_ts);
    if (OB_FAIL(update_ls_id_meta(false))) {
      TRANS_LOG(WARN, "update id meta of ls meta fail", K(ret), K(service_type_), K(limited_id), K(log_ts));
    }
  } else {
    // do nothing
  }
  is_logging_ = false;
  const int64_t used_ts = ObTimeUtility::current_time() - submit_log_ts_;
  submit_log_ts_ = OB_INVALID_TIMESTAMP;
  cb_.reset();
  TRANS_LOG(INFO, "handle submit callback", K(service_type_), K(success), K(used_ts), K(log_ts), K(limited_id));
  return ret;
}

int ObIDService::replay(const void *buffer, const int64_t buf_size,
                        const palf::LSN &lsn, const SCN &log_scn)
{
  //TODO(scn)
  int ret = OB_SUCCESS;
  logservice::ObLogBaseHeader base_header;
  int64_t tmp_pos = 0;
  const char *log_buf = static_cast<const char *>(buffer);
  ObPresistIDLog ls_log;
  if (OB_FAIL(base_header.deserialize(log_buf, buf_size, tmp_pos))) {
   TRANS_LOG(WARN, "log base header deserialize error", K(ret), KP(buffer), K(buf_size), K(lsn), K(log_scn));
  } else if (OB_FAIL(ls_log.deserialize((char *)buffer, buf_size, tmp_pos))) {
    TRANS_LOG(WARN, "desrialize tx_log_body error", K(ret), KP(buffer), K(buf_size), K(lsn), K(log_scn));
  } else if (OB_FAIL(handle_replay_result(ls_log.get_last_id(), ls_log.get_limit_id(), log_scn))) {
    TRANS_LOG(WARN, "handle replay result fail", K(ret), K(ls_log), K(log_scn));
  } else {
    // do nothing
  }
  TRANS_LOG(INFO, "ObIDService replay", K(service_type_), K(ret), K(ls_log), K(base_header));
  return ret;
}

int ObIDService::handle_replay_result(const int64_t last_id, const int64_t limited_id, const SCN log_ts)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);
  if (last_id < 0 || limited_id < 0 || !log_ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(last_id), K(limited_id), K(log_ts));
  } else {
    if (last_id != limited_id && limited_id == ATOMIC_LOAD(&limited_id_)) {
      (void)inc_update(&tmp_last_id_, last_id);
    } else {
      ATOMIC_STORE(&tmp_last_id_, 0);
    }
    (void)inc_update(&last_id_, limited_id);
    (void)inc_update(&limited_id_, limited_id);
    if (log_ts > latest_log_ts_.atomic_get()) {
      rec_log_ts_.atomic_set(log_ts);
      latest_log_ts_.atomic_set(log_ts);
      if (OB_FAIL(update_ls_id_meta(false))) {
        TRANS_LOG(WARN, "update id meta of ls meta fail", K(ret), K(service_type_), K(last_id), K(limited_id), K(log_ts));
      }
    }
  }
  TRANS_LOG(INFO, "handle reply result", K(ret), K(service_type_), K(last_id), K(limited_id), K(log_ts));
  return ret;
}

int ObIDService::update_ls_id_meta(const bool write_slog)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_and_fill_ls())) {
    TRANS_LOG(WARN, "ls set fail", K(ret));
  } else if (write_slog) {
    ret = ls_->update_id_meta(service_type_,
                              ATOMIC_LOAD(&limited_id_),
                              latest_log_ts_.atomic_load(),
                              true /* write slog */);
  } else {
    ret = ls_->update_id_meta(service_type_,
                              ATOMIC_LOAD(&limited_id_),
                              latest_log_ts_.atomic_load(),
                              false /* not write slog */);
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "update id meta fail", K(ret), K_(service_type));
  }

  return ret;
}

int ObIDService::flush(SCN &rec_scn)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);
  SCN latest_rec_log_ts = rec_log_ts_.atomic_get();
  if (latest_rec_log_ts <= rec_scn) {
    latest_rec_log_ts = rec_log_ts_.atomic_get();
    if (OB_FAIL(update_ls_id_meta(true))) {
      TRANS_LOG(WARN, "update id meta of ls meta fail", K(ret), K(service_type_));
    } else {
      rec_log_ts_.atomic_bcas(latest_rec_log_ts, SCN::max_scn());
    }
    TRANS_LOG(INFO, "flush", K(ret), K(service_type_), K(rec_log_ts_), K(limited_id_));
  }

  return ret;
}

int ObIDService::check_leader(bool &leader)
{
  int ret = OB_SUCCESS;
  common::ObRole role = common::ObRole::INVALID_ROLE;
  int64_t proposal_id = 0;

  if (OB_FAIL(check_and_fill_ls())) {
    TRANS_LOG(WARN, "ls set fail", K(ret));
  } else if (OB_FAIL(ls_->get_log_handler()->get_role(role, proposal_id))) {
    TRANS_LOG(WARN, "get ls role fail", K(ret));
  } else if (common::ObRole::LEADER == role) {
    leader = true;
  } else {
    leader = false;
  }

  return ret;
}

SCN ObIDService::get_rec_scn()
{
  const SCN rec_log_ts = rec_log_ts_.atomic_get();
  TRANS_LOG(INFO, "get rec log scn", K(service_type_), K(rec_log_ts));
  return rec_log_ts;
}

int ObIDService::switch_to_follower_gracefully()
{
  // int ret = OB_SUCCESS;
  // const int64_t expire_time = ObTimeUtility::current_time() + 100000;
  // int locked = false;
  // if (OB_SUCC(rwlock_.wrlock(expire_time))) {
  //   locked = true;
  // }
  // while (is_logging_ && locked) {
  //   rwlock_.unlock();
  //   locked = false;
  //   ObClockGenerator::usleep(1000);
  //   if (ObTimeUtility::current_time() > expire_time) {
  //     break;
  //   }
  //   if (OB_SUCC(rwlock_.wrlock(expire_time))) {
  //     locked = true;
  //   }
  // }
  // if (locked) {
  //   const int64_t last_id = ATOMIC_LOAD(&last_id_);
  //   const int64_t limited_id = ATOMIC_LOAD(&limited_id_);
  //   // Caution: set limit id before submit log, make sure limit id <= last id
  //   ATOMIC_STORE(&limited_id_, last_id);
  //   //提交日志但不要求一定成功，防止阻塞卸任
  //   submit_log_(ATOMIC_LOAD(&last_id_), limited_id);
  //   rwlock_.unlock();
  //   locked = false;
  // }
  // TRANS_LOG(INFO, "switch to follower gracefully", K(ret), K(service_type_), K(locked));
  TRANS_LOG(INFO, "switch to follower gracefully", K(last_id_), K(limited_id_), K(service_type_));
  // There is no failure scenario for ObIDService to switch the follower,
  // so return OB_SUCCESS directly.
  return OB_SUCCESS;
}

int ObIDService::get_number(const int64_t range, const int64_t base_id, int64_t &start_id, int64_t &end_id)
{
  int ret = OB_SUCCESS;
  bool leader = false;
  if (OB_FAIL(check_leader(leader))) {
    TRANS_LOG(WARN, "check leader fail", K(ret));
  } else if (!leader) {
    ret = OB_NOT_MASTER;
    if (EXECUTE_COUNT_PER_SEC(10)) {
      TRANS_LOG(WARN, "id service is not leader", K(ret), K_(service_type));
    }
  } else {
    int64_t tmp_id = 0;
    const int64_t last_id = ATOMIC_LOAD(&last_id_);
    int64_t limit_id = ATOMIC_LOAD(&limited_id_);
    const int64_t allocated_range = min(min(limit_id - base_id, limit_id - last_id), range);
    if (allocated_range <= 0) {
      ret = OB_EAGAIN;
    } else {
      if (base_id > last_id) {
        if (ATOMIC_BCAS(&last_id_, last_id, base_id + allocated_range)) {
          tmp_id = base_id;
        } else {
          tmp_id = ATOMIC_FAA(&last_id_, allocated_range);
        }
      } else {
        tmp_id = ATOMIC_FAA(&last_id_, allocated_range);
      }
      // Caution: get limit id again, compete with switch_to_follower_gracefully
      limit_id = ATOMIC_LOAD(&limited_id_);
      if (tmp_id >= limit_id) {
        ret = OB_EAGAIN;
      } else {
        start_id = tmp_id;
        end_id = min(start_id + allocated_range, limit_id);
      }
    }
    if (OB_EAGAIN == ret || (limited_id_ - last_id_) < (pre_allocated_range_ * 2 / 3)) {
      const int64_t pre_allocated_id = min(max_pre_allocated_id_(base_id), max(base_id, limited_id_) + max(range * 10, pre_allocated_range_));
      submit_log_with_lock_(pre_allocated_id, pre_allocated_id);
    }
  }
  if (TC_REACH_TIME_INTERVAL(100000)) {
    TRANS_LOG(INFO, "get number", K(ret), K(service_type_), K(range), K(base_id), K(start_id), K(end_id));
  }
	return ret;
}


int64_t ObIDService::max_pre_allocated_id_(const int64_t base_id)
{
  int64_t max_pre_allocated_id = INT64_MAX;
  if (TimestampService == service_type_) {
    if (base_id > ATOMIC_LOAD(&limited_id_)) {
      (void)inc_update(&last_id_, base_id);
    }
    max_pre_allocated_id = ATOMIC_LOAD(&last_id_) + 2 * pre_allocated_range_;
  }
  return max_pre_allocated_id;
}

void ObIDService::get_virtual_info(int64_t &last_id, int64_t &limited_id, SCN &rec_scn,
                                   SCN &latest_log_ts, int64_t &pre_allocated_range,
                                   int64_t &submit_log_ts, bool &is_master)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);
  last_id = last_id_;
  limited_id = limited_id_;
  rec_scn = rec_log_ts_;
  latest_log_ts = latest_log_ts_;
  pre_allocated_range = pre_allocated_range_;
  submit_log_ts = submit_log_ts_;
  if (OB_FAIL(check_leader(is_master))) {
    is_master = false;
  }
  TRANS_LOG(INFO, "id service get virtual info", K_(last_id), K_(limited_id), K_(rec_log_ts),
            K_(latest_log_ts), K_(pre_allocated_range), K_(submit_log_ts), K(is_master), K(ret));
}

void ObIDService::get_all_id_service_type(int64_t *service_type)
{
  for(int i=0; i<MAX_SERVICE_TYPE; i++) {
    service_type[i] = i;
  }
}

int ObIDService::get_id_service(const int64_t id_service_type, ObIDService *&id_service)
{
  int ret = OB_SUCCESS;
  switch (id_service_type) {
  case transaction::ObIDService::TimestampService:
    id_service = (ObIDService *)MTL(transaction::ObTimestampService *);
    break;
  case transaction::ObIDService::TransIDService:
    id_service = (ObIDService *)MTL(transaction::ObTransIDService *);
    break;
  case transaction::ObIDService::DASIDService:
    id_service = (ObIDService *)MTL(sql::ObDASIDService *);
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "get wrong id_service_type", K(ret), K(MTL_ID()), K(id_service_type));
  }

  return ret;
}

int ObIDService::update_id_service(const ObAllIDMeta &id_meta)
{
  int ret = OB_SUCCESS;

  for(int i = 0; i < ObIDService::MAX_SERVICE_TYPE && OB_SUCC(ret); i++) {
    ObIDService *id_service = NULL;
    int64_t limited_id = 0;
    SCN latest_log_ts = SCN::min_scn();
    if (OB_FAIL(id_meta.get_id_meta(i, limited_id, latest_log_ts))) {
      TRANS_LOG(WARN, "get id meta fail", K(ret), K(id_meta));
    } else if (OB_FAIL(get_id_service(i, id_service))) {
      TRANS_LOG(WARN, "get id service fail", K(ret), K(i));
    } else if (OB_ISNULL(id_service)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "id service is null", K(ret));
    } else {
      (void) id_service->update_limited_id(limited_id, latest_log_ts);
    }
  }

  return ret;
}

void ObIDService::update_limited_id(const int64_t limited_id, const SCN latest_log_ts)
{
  WLockGuard guard(rwlock_);
  (void)inc_update(&last_id_, limited_id);
  (void)inc_update(&limited_id_, limited_id);
  rec_log_ts_.set_max();
  latest_log_ts_.atomic_set(latest_log_ts);
}

OB_SERIALIZE_MEMBER(ObPresistIDLog, last_id_, limit_id_);

void ObPresistIDLogCb::reset()
{
  id_srv_type_ = ObIDService::INVALID_ID_SERVICE_TYPE;
  limited_id_ = 0;
  log_ts_.reset();
  pos_ = 0;
  memset(log_buf_, 0, sizeof(log_buf_));
}

int ObPresistIDLogCb::serialize_ls_log(ObPresistIDLog &ls_log, int64_t service_type)
{
  int ret = OB_SUCCESS;
  switch (service_type) {
    case ObIDService::ServiceType::TimestampService: {
      logservice::ObLogBaseHeader
        base_header(logservice::ObLogBaseType::TIMESTAMP_LOG_BASE_TYPE,
                    logservice::ObReplayBarrierType::NO_NEED_BARRIER,
                    service_type);
      if (OB_FAIL(base_header.serialize(log_buf_, MAX_LOG_BUFF_SIZE, pos_))) {
        TRANS_LOG(WARN, "ObPresistIDLogCb serialize base header error", KR(ret), KP(log_buf_), K(pos_));
      }
      break;
    }
    case ObIDService::ServiceType::TransIDService: {
      logservice::ObLogBaseHeader
        base_header(logservice::ObLogBaseType::TRANS_ID_LOG_BASE_TYPE,
                    logservice::ObReplayBarrierType::NO_NEED_BARRIER,
                    service_type);
      if (OB_FAIL(base_header.serialize(log_buf_, MAX_LOG_BUFF_SIZE, pos_))) {
        TRANS_LOG(WARN, "ObPresistIDLogCb serialize base header error", KR(ret), KP(log_buf_), K(pos_));
      }
      break;
    }
    case ObIDService::ServiceType::DASIDService: {
      logservice::ObLogBaseHeader
              base_header(logservice::ObLogBaseType::DAS_ID_LOG_BASE_TYPE,
                          logservice::ObReplayBarrierType::NO_NEED_BARRIER,
                          service_type);
      if (OB_FAIL(base_header.serialize(log_buf_, MAX_LOG_BUFF_SIZE, pos_))) {
        TRANS_LOG(WARN, "ObPresistIDLogCb serialize base header error", KR(ret), KP(log_buf_), K(pos_));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unknown service type", K(service_type));
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ls_log.serialize(log_buf_, MAX_LOG_BUFF_SIZE, pos_))) {
      TRANS_LOG(WARN, "ObPresistIDLogCb serialize ls_log error", KR(ret), KP(log_buf_), K(pos_));
    }
  }
  return ret;
}

int ObPresistIDLogCb::on_success()
{
  int ret = OB_SUCCESS;

  switch (id_srv_type_) {
    case ObIDService::ServiceType::TimestampService: {
      transaction::ObTimestampService *timestamp_service = nullptr;
      if (OB_ISNULL(timestamp_service = MTL(transaction::ObTimestampService *))) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "timestamp service is null", K(ret));
      } else {
        timestamp_service->test_lock();
        if (OB_FAIL(timestamp_service->handle_submit_callback(true, limited_id_, log_ts_))) {
          TRANS_LOG(WARN, "timestamp service handle log callback fail", K(ret), K_(limited_id), K_(log_ts));
        }
        TRANS_LOG(INFO, "timestamp service handle log callback", K(ret), K_(limited_id), K_(log_ts));
      }
      break;
    }
    case ObIDService::ServiceType::TransIDService: {
      transaction::ObTransIDService *trans_id_service = nullptr;
      if (OB_ISNULL(trans_id_service = MTL(transaction::ObTransIDService *))) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "trans id service is null", K(ret));
      } else {
        trans_id_service->test_lock();
        if (OB_FAIL(trans_id_service->handle_submit_callback(true, limited_id_, log_ts_))) {
          TRANS_LOG(WARN, "trans id  service handle log callback fail", K(ret), K_(limited_id), K_(log_ts));
        }
        TRANS_LOG(INFO, "trans id service handle log callback", K(ret), K_(limited_id), K_(log_ts));
      }
      break;
    }
    case ObIDService::ServiceType::DASIDService: {
      sql::ObDASIDService *das_id_service = nullptr;
      if (OB_ISNULL(das_id_service = MTL(sql::ObDASIDService *))) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "das id service is null", K(ret));
      } else {
        das_id_service->test_lock();
        if (OB_FAIL(das_id_service->handle_submit_callback(true, limited_id_, log_ts_))) {
          TRANS_LOG(WARN, "das id service handle log callback fail", K(ret), K_(limited_id), K_(log_ts));
        }
        TRANS_LOG(INFO, "das id service handle log callback", K(ret), K_(limited_id), K_(log_ts));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unknown service type", K(*this));
      break;
    }
  }
  TRANS_LOG(INFO, "ObPresistIDLogCb on success", KR(ret), K(*this));
  return ret;
}

int ObPresistIDLogCb::on_failure()
{
  int ret = OB_SUCCESS;

  switch (id_srv_type_) {
    case ObIDService::ServiceType::TimestampService: {
      transaction::ObTimestampService *timestamp_service = nullptr;
      if (OB_ISNULL(timestamp_service = MTL(transaction::ObTimestampService *))) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "timestamp service is null", K(ret));
      } else {
        if (OB_FAIL(timestamp_service->handle_submit_callback(false, limited_id_, log_ts_))) {
          TRANS_LOG(WARN, "timestamp service handle log callback fail", K(ret), K_(limited_id), K_(log_ts));
        }
        TRANS_LOG(INFO, "timestamp service handle log callback", K(ret), K_(limited_id), K_(log_ts));
      }
      break;
    }
    case ObIDService::ServiceType::TransIDService: {
      transaction::ObTransIDService *trans_id_service = nullptr;
      if (OB_ISNULL(trans_id_service = MTL(transaction::ObTransIDService *))) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "trans id service is null", K(ret));
      } else {
        if (OB_FAIL(trans_id_service->handle_submit_callback(false, limited_id_, log_ts_))) {
          TRANS_LOG(WARN, "trans id  service handle log callback fail", K(ret), K_(limited_id), K_(log_ts));
        }
        TRANS_LOG(INFO, "trans id service handle log callback", K(ret), K_(limited_id), K_(log_ts));
      }
      break;
    }
    case ObIDService::ServiceType::DASIDService: {
      sql::ObDASIDService *das_id_service = nullptr;
      if (OB_ISNULL(das_id_service = MTL(sql::ObDASIDService *))) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "das id service is null", K(ret));
      } else {
       if (OB_FAIL(das_id_service->handle_submit_callback(false, limited_id_, log_ts_))) {
          TRANS_LOG(WARN, "das id service handle log callback fail", K(ret), K_(limited_id), K_(log_ts));
        }
        TRANS_LOG(INFO, "das id service handle log callback", K(ret), K_(limited_id), K_(log_ts));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unknown service type", K(*this));
      break;
    }
  }
  TRANS_LOG(INFO, "ObPresistIDLogCb on failure", KR(ret), K(*this));
  return ret;
}

OB_SERIALIZE_MEMBER(ObIDMeta, limited_id_, latest_log_ts_);

OB_DEF_SERIALIZE(ObAllIDMeta)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(id_meta_, count_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObAllIDMeta)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(id_meta_, count_);
  return len;
}

OB_DEF_DESERIALIZE(ObAllIDMeta)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;

  OB_UNIS_DECODE(count);
  if (OB_SUCC(ret)) {
    count_ = min(count_, count);
  }
  OB_UNIS_DECODE_ARRAY(id_meta_, count_);
  return ret;
}

void ObAllIDMeta::update_all_id_meta(const ObAllIDMeta &all_id_meta)
{
  ObSpinLockGuard lock_guard(lock_);
  for(int i=0; i<ObIDService::MAX_SERVICE_TYPE; i++) {
    (void)inc_update(&id_meta_[i].limited_id_, all_id_meta.id_meta_[i].limited_id_);
    id_meta_[i].latest_log_ts_.inc_update(all_id_meta.id_meta_[i].latest_log_ts_);
  }
}

int ObAllIDMeta::update_id_meta(const int64_t service_type,
                                const int64_t limited_id,
                                const SCN &latest_log_ts)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard lock_guard(lock_);
  if (service_type <= ObIDService::INVALID_ID_SERVICE_TYPE ||
      service_type >= ObIDService::MAX_SERVICE_TYPE) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(service_type));
  } else {
    (void)inc_update(&id_meta_[service_type].limited_id_, limited_id);
    id_meta_[service_type].latest_log_ts_.inc_update(latest_log_ts);
  }
  return ret;
}

int ObAllIDMeta::get_id_meta(const int64_t service_type,
                             int64_t &limited_id,
                             SCN &latest_log_ts) const
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard lock_guard(lock_);
  if (service_type <= ObIDService::INVALID_ID_SERVICE_TYPE ||
      service_type >= ObIDService::MAX_SERVICE_TYPE) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(service_type));
  } else {
    limited_id = id_meta_[service_type].limited_id_;
    latest_log_ts = id_meta_[service_type].latest_log_ts_;
  }
  return ret;
}

}
}
