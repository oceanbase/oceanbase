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

#ifndef OCEANBASE_TRANSACTION_OB_XA_DEFINE_
#define OCEANBASE_TRANSACTION_OB_XA_DEFINE_

#include "lib/string/ob_string.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "ob_trans_timer.h"

namespace oceanbase
{


namespace transaction
{

class ObXACtx;

extern const int64_t LOCK_FORMAT_ID;
extern const int64_t XA_INNER_TABLE_TIMEOUT;

extern const bool ENABLE_NEW_XA;

static const ObString PL_XA_IMPLICIT_SAVEPOINT = "__PL_XA_IMPLICIT_SAVEPOINT";

class ObXATransState
{
public:
  static const int32_t UNKNOWN = -1;
  static const int32_t NON_EXISTING = 0;
  static const int32_t ACTIVE = 1;
  static const int32_t IDLE = 2;
  static const int32_t PREPARED = 3;
  static const int32_t COMMITTED = 4;
  static const int32_t ROLLBACKED = 5;
  static const int32_t PREPARING = 6;
  static const int32_t COMMITTING = 7;
  static const int32_t ROLLBACKING = 8;
public:
  static bool is_valid(const int32_t state)
  {
    return state >= NON_EXISTING && state <= ROLLBACKING;
  }
  static bool is_prepared(const int32_t state)
  {
    return state == PREPARED;
  }
  static bool has_submitted(const int32_t state)
  {
    return COMMITTING == state
           || ROLLBACKING == state
           || PREPARING == state
           || ROLLBACKED == state
           || COMMITTED == state
           || PREPARED == state;
  }
  static bool can_convert(const int32_t src_state, const int32_t dst_state);
  static const char* to_string(int32_t state) {
    const char* state_str = NULL;
    switch (state) {
      case NON_EXISTING:
        state_str = "NON-EXISTING";
        break;
      case ACTIVE:
        state_str = "ACTIVE";
        break;
      case IDLE:
        state_str = "IDLE";
        break;
      case PREPARED:
        state_str = "PREPARED";
        break;
      case COMMITTED:
        state_str = "COMMITTED";
        break;
      case ROLLBACKED:
        state_str = "ROLLBACKED";
        break;
      case PREPARING:
        state_str = "PREPARING";
        break;
      default:
        state_str = "UNKNOW";
        break;
    }
    return state_str;
  }
};

class ObXAFlag
{
public:
  enum
  {
    OBTMNOFLAGS = 0,
    // non-standard xa protocol, to denote a readonly xa trans
    OBTMREADONLY = 0x100,
    // non-standard xa protocol, to denote a serializable xa trans
    OBTMSERIALIZABLE = 0x400,
    // non-standard xa protocol, to denote loosely coupled xa trans

    OBLOOSELY = 0x10000,
    OBTMJOIN = 0x200000,
    OBTMSUSPEND = 0x2000000,
    OBTMSUCCESS = 0x4000000,
    OBTMRESUME = 0x8000000,
    OBTMFAIL = 0x20000000,
    OBTMONEPHASE = 0x40000000,
    // non-standard xa protocol, to denote temp table xa trans
    OBTEMPTABLE = 0x100000000,
  };
public:
  // 用于检查xa请求传入的flag
  // check the flag brought by xa calls
  static bool is_valid(const int64_t flag, const int64_t xa_req_type);
  // check the flag stored in inner table
  static bool is_valid_inner_flag(const int64_t flag);
  static bool contain_tmreadonly(const int64_t flag) { return flag & OBTMREADONLY; }
  static bool contain_tmserializable(const int64_t flag) { return flag & OBTMSERIALIZABLE; }
  static bool is_tmnoflags(const int64_t flag, const int64_t xa_req_type);
  static bool contain_loosely(const int64_t flag) { return flag & OBLOOSELY; }
  static bool contain_tmjoin(const int64_t flag) { return flag & OBTMJOIN; }
  static bool is_tmjoin(const int64_t flag) { return flag == OBTMJOIN; }
  static bool contain_tmresume(const int64_t flag) { return flag & OBTMRESUME; }
  static bool is_tmresume(const int64_t flag) { return flag == OBTMRESUME; }
  static bool contain_tmsuccess(const int64_t flag) { return flag & OBTMSUCCESS; }
  static bool contain_tmsuspend(const int64_t flag) { return flag & OBTMSUSPEND; }
  static bool contain_tmonephase(const int64_t flag) { return flag & OBTMONEPHASE; }
  static bool is_tmonephase(const int64_t flag) { return flag == OBTMONEPHASE; }
  static bool contain_tmfail(const int64_t flag) { return flag & OBTMFAIL; }
  static int64_t add_end_flag(const int64_t flag, const int64_t end_flag)
  {
    int64_t ret = end_flag;
    if (contain_loosely(flag)) {
      ret |= OBLOOSELY;
    }
    return ret;
  }
  static bool contain_temp_table(const int64_t flag)
  {
    return flag & ObXAFlag::OBTEMPTABLE;
  }
};

enum ObXAReqType
{
  XA_START = 1,
  XA_END,
  XA_PREPARE,
  XA_COMMIT,
  XA_ROLLBACK,
};

class ObXATransID
{
  OB_UNIS_VERSION(1);
public:
  ObXATransID() { reset(); }
  ObXATransID(const ObXATransID &xid);
  ~ObXATransID() { destroy(); }
  void reset();
  void destroy() { reset(); }
  // set xid, regardless of whether xid is empty
  int set(const common::ObString &gtrid_str,
          const common::ObString &bqual_str,
          const int64_t format_id);
  int set(const ObXATransID &xid);
  const common::ObString &get_gtrid_str() const { return gtrid_str_; }
  const common::ObString &get_bqual_str() const { return bqual_str_; }
  int64_t get_format_id() const { return format_id_; }
  uint64_t get_gtrid_hash() const { return g_hv_; }
  uint64_t get_bqual_hash() const { return b_hv_; }
  bool empty() const;
  // empty xid is also valid
  bool is_valid() const;
  ObXATransID &operator=(const ObXATransID &xid);
  bool operator==       (const ObXATransID &xid) const;
  bool operator!=       (const ObXATransID &xid) const;
  bool all_equal_to     (const ObXATransID &other) const;
  bool gtrid_equal_to   (const ObXATransID &other) const;
  int32_t to_full_xid_string(char *buffer, const int64_t capacity) const;
  int to_yson(char *buf, const int64_t buf_len, int64_t &pos) const;
  TO_STRING_KV(K_(gtrid_str), K_(bqual_str), K_(format_id),
      KPHEX(gtrid_str_.ptr(), gtrid_str_.length()),
      KPHEX(bqual_str_.ptr(), bqual_str_.length()),
      K_(g_hv), K_(b_hv));
public:
  static const int32_t HASH_SIZE = 1000000000;
  static const int32_t MAX_GTRID_LENGTH = 64;
  static const int32_t MAX_BQUAL_LENGTH = 64;
  static const int32_t MAX_XID_LENGTH = MAX_GTRID_LENGTH + MAX_BQUAL_LENGTH;
private:
  char gtrid_buf_[MAX_GTRID_LENGTH];
  common::ObString gtrid_str_;
  char bqual_buf_[MAX_BQUAL_LENGTH];
  common::ObString bqual_str_;
  int64_t format_id_;
  uint64_t g_hv_;
  uint64_t b_hv_;
};

struct ObXABranchInfo
{
  ObXABranchInfo() {}
  virtual ~ObXABranchInfo() {}
  int init(const ObXATransID &xid,
           const int64_t state,
           const int64_t timeout_seconds,
           const int64_t abs_expired_time,
           const common::ObAddr &addr,
           const int64_t unrespond_msg_cnt,
           const int64_t last_hb_ts,
           const int64_t end_flag = ObXAFlag::OBTMNOFLAGS);
  TO_STRING_KV(K_(xid), K_(state), K_(timeout_seconds), K_(addr),
               K_(unrespond_msg_cnt), K_(last_hb_ts), K_(end_flag));
  ObXATransID xid_;
  int64_t state_;
  int64_t timeout_seconds_;
  int64_t abs_expired_time_;
  common::ObAddr addr_;
  int64_t unrespond_msg_cnt_;
  int64_t last_hb_ts_;
  //ATTENTION, newly added
  int64_t end_flag_;
};

struct ObXAStmtInfo
{
  ObXAStmtInfo() : xid_(), is_first_stmt_(true) {}
  ObXAStmtInfo(const ObXATransID xid) : xid_(xid), is_first_stmt_(true) {}
  ~ObXAStmtInfo() {}
  TO_STRING_KV(K_(xid), K_(is_first_stmt));
  ObXATransID xid_;
  bool is_first_stmt_;
};

typedef common::ObSEArray<ObXABranchInfo, 4> ObXABranchInfoArray;
typedef common::ObSEArray<ObXAStmtInfo, 1> ObXAStmtInfoArray;

class ObXATimeoutTask : public ObITimeoutTask
{
public:
  ObXATimeoutTask() : is_inited_(false), ctx_(NULL) {}
  virtual ~ObXATimeoutTask() {}
  int init(ObXACtx *ctx);
  void destroy();
  void reset();
public:
  void runTimerTask();
  uint64_t hash() const;
public:
  TO_STRING_KV(K_(is_inited), K_(is_registered), K_(is_running), K_(delay), KP_(ctx),
      K_(bucket_idx), K_(run_ticket), K_(is_scheduled), KP_(prev), KP_(next));
private:
  bool is_inited_;
  ObXACtx *ctx_;
};

// format id of dblink trans
// from Oracle
static const int32_t DBLINK_FORMAT_ID = 830487;
class ObXADefault
{
public:
  static constexpr int64_t OB_XA_TIMEOUT_SECONDS = 60; /*60s*/
  static constexpr const char* OB_XA_TIMEOUT_NAME = "ob_xa_timeout";
};

class ObXAStatistics
{
public:
  ObXAStatistics() : last_stat_ts_(0), total_active_xa_ctx_count_(0)
  {
    reset();
  }
  ~ObXAStatistics() {}
  void reset();
  void inc_ctx_count() { ATOMIC_INC(&total_active_xa_ctx_count_); }
  void dec_ctx_count() { ATOMIC_DEC(&total_active_xa_ctx_count_); }
  void inc_cleanup_tx_count() { ATOMIC_INC(&total_standby_clearup_count_); }
  void inc_success_xa_start() { ATOMIC_INC(&total_success_xa_start_); }
  void inc_failure_xa_start() { ATOMIC_INC(&total_failure_xa_start_); }
  void inc_success_xa_prepare() { ATOMIC_INC(&total_success_xa_prepare_); }
  void inc_failure_xa_prepare() { ATOMIC_INC(&total_failure_xa_prepare_); }
  void inc_success_xa_1pc_commit() { ATOMIC_INC(&total_success_xa_1pc_commit_); }
  void inc_failure_xa_1pc_commit() { ATOMIC_INC(&total_failure_xa_1pc_commit_); }
  void inc_success_xa_2pc_commit() { ATOMIC_INC(&total_success_xa_2pc_commit_); }
  void inc_failure_xa_2pc_commit() { ATOMIC_INC(&total_failure_xa_2pc_commit_); }
  void inc_xa_rollback() { ATOMIC_INC(&total_xa_rollback_); }
  void inc_success_dblink_promotion() { ATOMIC_INC(&total_success_dblink_promotion_); }
  void inc_failure_dblink_promotion() { ATOMIC_INC(&total_failure_dblink_promotion_); }
  void inc_success_dblink() { ATOMIC_INC(&total_success_dblink_); }
  void inc_failure_dblink() { ATOMIC_INC(&total_failure_dblink_); }
  void print_statistics(int64_t cur_ts);
  TO_STRING_KV(K_(total_active_xa_ctx_count), K_(total_standby_clearup_count),
               K_(total_success_xa_start), K_(total_failure_xa_start),
               K_(total_success_xa_prepare), K_(total_failure_xa_prepare),
               K_(total_success_xa_1pc_commit), K_(total_failure_xa_1pc_commit),
               K_(total_success_xa_2pc_commit), K_(total_failure_xa_2pc_commit),
               K_(total_xa_rollback),
               K_(total_success_dblink_promotion), K_(total_failure_dblink_promotion),
               K_(total_success_dblink), K_(total_failure_dblink));

private:
  DISALLOW_COPY_AND_ASSIGN(ObXAStatistics);
  static const int64_t STAT_INTERVAL = 10 * 1000 * 1000;
  int64_t last_stat_ts_;
  int64_t total_active_xa_ctx_count_;
  int64_t total_standby_clearup_count_;
  int64_t total_success_xa_start_;
  int64_t total_failure_xa_start_;
  int64_t total_success_xa_prepare_;
  int64_t total_failure_xa_prepare_;
  int64_t total_success_xa_1pc_commit_;
  int64_t total_failure_xa_1pc_commit_;
  int64_t total_success_xa_2pc_commit_;
  int64_t total_failure_xa_2pc_commit_;
  int64_t total_xa_rollback_;
  int64_t total_success_dblink_promotion_;
  int64_t total_failure_dblink_promotion_;
  int64_t total_success_dblink_;
  int64_t total_failure_dblink_;
};

}//transaction

}//oceanbase


#endif
