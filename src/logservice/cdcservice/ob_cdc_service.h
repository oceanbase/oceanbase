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

class UpdateCtxFunctor {
public:
  UpdateCtxFunctor():
      is_inited_(false),
      dest_(),
      dest_ver_(0) { }
  ~UpdateCtxFunctor() {
    is_inited_ = false;
    dest_.reset();
    dest_ver_ = 0;
  }

  int init(const ObBackupPathString &dest_str, const int64_t version);

  bool operator()(const ClientLSKey &key, ClientLSCtx *value);

private:
  bool is_inited_;
  ObBackupDest dest_;
  int64_t dest_ver_;
};

class ObCdcService: public lib::TGRunnable
{
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

  int fetch_raw_log(const obrpc::ObCdcFetchRawLogReq &req,
      obrpc::ObCdcFetchRawLogResp &resp,
      const int64_t send_ts,
      const int64_t recv_ts);

  int get_archive_dest_snapshot(const ObLSID &ls_id,
      ObBackupDest &archive_dest);

  ClientLSCtxMap &get_ls_ctx_map() {
    return ls_ctx_map_;
  }

  void get_ls_ctx_map(ClientLSCtxMap *&ctx_map) {
    ctx_map = &ls_ctx_map_;
  }

  int get_or_create_client_ls_ctx(const obrpc::ObCdcRpcId &client_id,
      const uint64_t client_tenant_id,
      const ObLSID &ls_id,
      const int8_t flag,
      const int64_t client_progress,
      const FetchLogProtocolType proto_type,
      ClientLSCtx *&ctx);

  int revert_client_ls_ctx(ClientLSCtx *ctx);

  int init_archive_source_if_needed(const ObLSID &ls_id, ClientLSCtx &ctx);

  TO_STRING_KV(K_(is_inited));


private:
  int query_tenant_archive_info_(bool &archive_dest_changed);

  // no lock protection, make sure it's called only in CdcService::run1
  bool is_archive_dest_changed_(const ObArchiveDestInfo &info);

  // no lock protection, make sure it's called only in CdcService::run1
  int update_archive_dest_for_ctx_();

  int get_archive_dest_path_snapshot_(ObBackupPathString &archive_dest_str);

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
  uint64_t tenant_id_;

  ObCdcStartLsnLocator locator_;
  ObCdcFetcher fetcher_;

  int tg_id_;
  int64_t dest_info_version_;
  ObArchiveDestInfo dest_info_;
  SpinRWLock dest_info_lock_;
  ClientLSCtxMap ls_ctx_map_;
  archive::LargeBufferPool large_buffer_pool_;
  logservice::ObLogExternalStorageHandler log_ext_handler_;
};

} // namespace cdc
} // namespace oceanbase

#endif
