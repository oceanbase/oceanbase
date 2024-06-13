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

#pragma once
#include "observer/ob_server_struct.h"
#include "share/ob_ls_id.h"
#include "storage/tx/ob_tx_log_adapter.h"
#include "storage/tx/ob_trans_ctx_mgr_v4.h"

#define LOGI(format, ...) {time_t now=time(NULL);tm* local = localtime(&now);char buf[128] = {0};\
        strftime(buf, 128,"%Y-%m-%d %H:%M:%S", local);printf("[%s] [INFO] [%s:%d] [%s] " format "\n",buf, __FILENAME__,__LINE__, __FUNCTION__,##__VA_ARGS__);}

#define LOGE(format, ...) {time_t now=time(NULL);tm* local = localtime(&now);char buf[128] = {0};\
        strftime(buf, 128,"%Y-%m-%d %H:%M:%S", local);printf("[%s] [ERROR] [%s:%d] [%s] " format "\n",buf, __FILENAME__,__LINE__, __FUNCTION__,##__VA_ARGS__);}

namespace oceanbase
{
using namespace share;
using namespace transaction;

class SimpleServerHelper
{
public:
  static int create_ls(uint64_t tenant_id, ObAddr add);

  static int select_int64(common::ObMySQLProxy &sql_proxy, const char *sql, int64_t &val);
  static int g_select_int64(uint64_t tenant_id, const char *sql, int64_t &val);

  static int select_uint64(common::ObMySQLProxy &sql_proxy, const char *sql, uint64_t &val);
  static int g_select_uint64(uint64_t tenant_id, const char *sql, uint64_t &val);

  static int select_int64(sqlclient::ObISQLConnection *conn, const char *sql, int64_t &val);
  static int select_int64(ObMySQLTransaction &trans, uint64_t tenant_id, const char *sql, int64_t &val);

  static int select_varchar(sqlclient::ObISQLConnection *conn, const char *sql, ObString &val);
  static int g_select_varchar(uint64_t tenant_id, const char *sql, ObString &val);

  static int find_trace_id(sqlclient::ObISQLConnection *conn, ObString &trace_id);
  static int find_request(uint64_t tenant_id, int64_t session_id,
      int64_t &request_id,ObTransID &tx_id, ObString &trace_id, int64_t &retry_cnt);


  static int select_table_loc(uint64_t tenant_id, const char* table_name, ObLSID &ls_id);
  static int select_table_tablet(uint64_t tenant_id, const char* table_name, ObTabletID &tablet_id);
  static int do_balance(uint64_t tenant_id);
  static int remove_tx(uint64_t tenant_id, ObLSID ls_id, ObTransID tx_id);
  static int get_tx_ctx(uint64_t tenant_id,
                        ObLSID ls_id,
                        ObTransID tx_id,
                        ObPartTransCtx *&ctx);
  static int revert_tx_ctx(uint64_t tenant_id,
                           ObLSID ls_id,
                           ObPartTransCtx *ctx);
  static int abort_tx(uint64_t tenant_id, ObLSID ls_id, ObTransID tx_id);
  static int submit_redo(uint64_t tenant_id, ObLSID ls_id);
  static int find_session(sqlclient::ObISQLConnection *conn, int64_t &session_id);
  static int find_tx(sqlclient::ObISQLConnection *conn, ObTransID &tx_id);
  static int ls_resume(uint64_t tenant_id, ObLSID ls_id);
  static int ls_reboot(uint64_t tenant_id, ObLSID ls_id);
  static int freeze(uint64_t tenant_id, ObLSID ls_id, ObTabletID tablet_id);
  static int find_tx_info(uint64_t tenant_id, ObLSID ls_id, ObTransID tx_id, ObPartTransCtx &ctx_info);
  static int get_ls_end_scn(uint64_t tenant_id, ObLSID ls_id, SCN &end_scn);
  static int wait_replay_advance(uint64_t tenant_id, ObLSID ls_id, SCN end_scn);
  static int wait_checkpoint_newest(uint64_t tenant_id, ObLSID ls_id);
  static int freeze_tx_ctx(uint64_t tenant_id, ObLSID ls_id);
  static int freeze_tx_data(uint64_t tenant_id, ObLSID ls_id);
  static int wait_tx(uint64_t tenant_id, ObLSID ls_id, ObTransID tx_id, ObTxState tx_state);
  static int wait_tx_exit(uint64_t tenant_id, ObLSID ls_id, ObTransID tx_id);
  static int wait_flush_finish(uint64_t tenant_id, ObLSID ls_id, ObTabletID tablet_id);
  static int write(sqlclient::ObISQLConnection *conn, const char *sql);
  static int write(sqlclient::ObISQLConnection *conn, const char *sql, int64_t &affected_rows);
  static int wait_weak_read_ts_advance(uint64_t tenant_id, ObLSID ls_id1, ObLSID ls_id2);
  static int enable_wrs(uint64_t tenant_id, ObLSID ls_id, bool enable);
  static int modify_wrs(uint64_t tenant_id, ObLSID ls_id, int64_t add_ns = 10 * 1000 * 1000 * 1000L);
};

class InjectTxFaultHelper : public transaction::ObLSTxLogAdapter
{
public:
  InjectTxFaultHelper() : mgr_(NULL) {
    tx_injects_.create(1024, "tx_inject");
  }
  ~InjectTxFaultHelper() {
    release();
  }
  void release();
  int inject_tx_block(uint64_t tenant_id, ObLSID ls_id, ObTransID tx_id, ObTxLogType log_type);
  virtual int submit_log(const char *buf,
                         const int64_t size,
                         const share::SCN &base_ts,
                         ObTxBaseLogCb *cb,
                         const bool need_nonblock,
                         const int64_t retry_timeout_us = 1000) override;
private:
  transaction::ObLSTxCtxMgr *mgr_;
  hash::ObHashMap<ObTransID, ObTxLogType> tx_injects_;
};

#define SSH SimpleServerHelper

}
