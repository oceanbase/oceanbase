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

#ifndef OCEANBASE_TRANSACTION_KEEP_ALIVE_LS_HANDLER
#define OCEANBASE_TRANSACTION_KEEP_ALIVE_LS_HANDLER

#include "logservice/palf/palf_callback.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/ob_append_callback.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{

namespace logservice
{
class ObLogHandler;
}

namespace transaction
{
class ObKeepAliveLogBody
{
public:
  OB_UNIS_VERSION(1);

public:
  ObKeepAliveLogBody() : compat_bit_(1) {}
  ObKeepAliveLogBody(int64_t compat_bit) : compat_bit_(compat_bit) {}

  static int64_t get_max_serialize_size();
  TO_STRING_KV(K_(compat_bit));

private:
  int64_t compat_bit_; // not used, only for compatibility
};

class ObLSKeepAliveStatInfo
{
public:
  ObLSKeepAliveStatInfo() { reset(); }
  void reset()
  {
    cb_busy_cnt = 0;
    not_master_cnt = 0;
    near_to_gts_cnt = 0;
    other_error_cnt = 0;
    submit_succ_cnt = 0;
    last_log_ts_.reset();
    last_lsn_.reset();
  }

  int64_t cb_busy_cnt;
  int64_t not_master_cnt;
  int64_t near_to_gts_cnt;
  int64_t other_error_cnt;
  int64_t submit_succ_cnt;
  share::SCN last_log_ts_;
  palf::LSN last_lsn_;

private:
  // none
};

// after init , we should register in logservice
class ObKeepAliveLSHandler : public logservice::ObIReplaySubHandler,
                             public logservice::ObICheckpointSubHandler,
                             public logservice::ObIRoleChangeSubHandler,
                             public logservice::AppendCb 
{
public:
  const int64_t KEEP_ALIVE_GTS_INTERVAL = 100 * 1000;
public:
  ObKeepAliveLSHandler() : submit_buf_(nullptr) { reset(); }
  int init(const share::ObLSID &ls_id,logservice::ObLogHandler * log_handler_ptr);

  void stop();
  // false - can not safe destroy
  bool check_safe_destory();
  void destroy();

  void reset();
  
  int try_submit_log();
  void print_stat_info();
public:

  bool is_busy() { return ATOMIC_LOAD(&is_busy_); }
  int on_success() {ATOMIC_STORE(&is_busy_, false); return OB_SUCCESS;}
  int on_failure() {ATOMIC_STORE(&is_busy_, false); return OB_SUCCESS;}

  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &scn)
  {
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }
  void switch_to_follower_forcedly()
  {
   ATOMIC_STORE(&is_master_, false); 
  }
  int switch_to_leader() { ATOMIC_STORE(&is_master_,true); return OB_SUCCESS;}
  int switch_to_follower_gracefully() { ATOMIC_STORE(&is_master_,false); return OB_SUCCESS;}
  int resume_leader() { ATOMIC_STORE(&is_master_,true);return OB_SUCCESS; }
  share::SCN get_rec_scn() { return share::SCN::max_scn(); }
  int flush(share::SCN &rec_scn) { return OB_SUCCESS;}

private:
  bool check_gts_();
private : 
  bool is_busy_;
  bool is_master_;
  bool is_stopped_;

  share::ObLSID ls_id_;
  logservice::ObLogHandler * log_handler_ptr_;

  char *submit_buf_;
  int64_t submit_buf_len_;
  int64_t submit_buf_pos_;

  share::SCN last_gts_;

  ObLSKeepAliveStatInfo stat_info_;
};

}
}
#endif
