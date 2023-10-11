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

#ifndef OCEANBASE_LOGSERVICE_OB_CDC_SERVICE_
#define OCEANBASE_LOGSERVICE_OB_CDC_SERVICE_

#include "ob_cdc_req.h"                         // RPC Request and Response
#include "ob_cdc_fetcher.h"                     // ObCdcFetcher
#include "ob_cdc_start_lsn_locator.h"
#include "ob_cdc_struct.h"                      // ClientLSKey, ClientLSCtx, ClientLSCtxMap
#include "logservice/restoreservice/ob_remote_log_iterator.h"
#include "logservice/ob_log_external_storage_handler.h"   // ObLogExternalStorageHandler

namespace oceanbase
{
namespace cdc
{
class RecycleCtxFunctor {
public:
  static const int64_t CTX_EXPIRE_INTERVAL = 30L * 60 * 1000 * 1000;
public:
  RecycleCtxFunctor(int64_t ts): cur_ts_(ts) {}
  ~RecycleCtxFunctor() = default;
  // recycle when the context hasn't been accessed for 30 min
  bool operator()(const ClientLSKey &key, ClientLSCtx *value) {
    UNUSED(key);

    bool bret = false;

    if (OB_ISNULL(value) || cur_ts_ - value->get_touch_ts() >= CTX_EXPIRE_INTERVAL) {
      bret = true;
    }
    return bret;
  }
private:
  int64_t cur_ts_;
};

class ExpiredArchiveClientLSFunctor
{
  static constexpr int64_t LS_ARCHIVE_ENTRY_EXPIRED_TIME = 10L * 60 * 1000 * 1000; // 10 min;
public:
  explicit ExpiredArchiveClientLSFunctor(const int64_t current_time);
  ~ExpiredArchiveClientLSFunctor();

  bool operator()(const ClientLSKey &key, ClientLSCtx *value);

  int64_t get_other_client_ls_cnt() const {
    return other_client_ls_cnt_;
  }

  int64_t get_valid_client_ls_cnt() const {
    return valid_client_ls_cnt_;
  }

private:
  int64_t current_time_us_;
  int64_t valid_client_ls_cnt_;
  int64_t other_client_ls_cnt_;
};

class ObCdcService: public lib::TGRunnable
{
public:
  static int get_backup_dest(const share::ObLSID &ls_id, share::ObBackupDest &backup_dest);
public:
  ObCdcService();
  ~ObCdcService();
  int init(const uint64_t tenant_id,
      ObLSService *ls_service);

public:
  void run1() override;
  int start();
  void stop();
  void wait();
  void destroy();
  bool is_stoped() const { return ATOMIC_LOAD(&stop_flag_); }

public:
  // The following interface is the entrance to service CDC-Connector,
  // and the internal implementation is to call the corresponding interface
  // of the corresponding component
  int req_start_lsn_by_ts_ns(const obrpc::ObCdcReqStartLSNByTsReq &req_msg,
      obrpc::ObCdcReqStartLSNByTsResp &resp);

  int fetch_log(const obrpc::ObCdcLSFetchLogReq &req,
      obrpc::ObCdcLSFetchLogResp &resp,
      const int64_t send_ts,
      const int64_t recv_ts);

  int fetch_missing_log(const obrpc::ObCdcLSFetchMissLogReq &req,
      obrpc::ObCdcLSFetchLogResp &resp,
      const int64_t send_ts,
      const int64_t recv_ts);

  ObArchiveDestInfo get_archive_dest_info() {
    // need to get the lock and **NOT** return the reference
    // if we return reference, there may be some thread-safe issues,
    // because there is a background thread updating dest_info_ periodically
    ObSpinLockGuard lock_guard(dest_info_lock_);
    return dest_info_;
  }

  ClientLSCtxMap &get_ls_ctx_map() {
    return ls_ctx_map_;
  }

  TO_STRING_KV(K_(is_inited));


private:
  int query_tenant_archive_info_();
  int recycle_expired_ctx_(const int64_t cur_ts);

  int resize_log_ext_handler_();

  void do_monitor_stat_(const int64_t start_ts,
      const int64_t end_ts,
      const int64_t send_ts,
      const int64_t recv_ts);
  // the thread group is not needed for meta tenant,
  // wrapping the macro with methods below to filter meta tenant.
  int create_tenant_tg_(const int64_t tenant_id);
  int start_tenant_tg_(const int64_t tenant_id);
  void wait_tenant_tg_(const int64_t tenant_id);
  void stop_tenant_tg_(const int64_t tenant_id);
  void destroy_tenant_tg_(const int64_t tenant_id);
private:
  bool is_inited_;
  volatile bool stop_flag_ CACHE_ALIGNED;
  ObCdcStartLsnLocator locator_;
  ObCdcFetcher fetcher_;

  int tg_id_;
  ObArchiveDestInfo dest_info_;
  common::ObSpinLock dest_info_lock_;
  ClientLSCtxMap ls_ctx_map_;
  archive::LargeBufferPool large_buffer_pool_;
  logservice::ObLogExternalStorageHandler log_ext_handler_;
};

} // namespace cdc
} // namespace oceanbase

#endif
