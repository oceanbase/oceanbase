/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_TABLE_TRANS_UTILS_H
#define _OB_TABLE_TRANS_UTILS_H

#include "src/storage/tx/ob_trans_define_v4.h"
#include "share/table/ob_table.h"
#include "sql/ob_sql_trans_control.h"
#include "ob_htable_lock_mgr.h"
#include "ob_table_end_trans_cb.h"


namespace oceanbase
{
namespace table
{
class ObTableCreateCbFunctor;
struct ObTableTransParam final
{
public:
  ObTableTransParam()
      : trans_state_()
  {
    reset();
  }
  virtual ~ObTableTransParam() {}
  void reset()
  {
    trans_state_.reset();
    trans_state_ptr_ = &trans_state_; // refer to inner trans_state_ default
    trans_desc_ = nullptr;
    had_do_response_ = false;
    req_ = nullptr;
    is_readonly_ = false;
    consistency_level_ = ObTableConsistencyLevel::EVENTUAL;
    ls_id_ = share::ObLSID::INVALID_LS_ID;
    need_global_snapshot_ = false;
    lock_handle_ = nullptr;
    use_sync_ = false;
    is_rollback_ = false;
    create_cb_functor_ = nullptr;
    timeout_ts_ = -1;
    did_async_commit_ = false;
    if (OB_NOT_NULL(trans_state_ptr_)) {
      trans_state_ptr_->reset();
    }
  }
  TO_STRING_KV(KPC_(trans_desc),
               K_(tx_snapshot),
               K_(had_do_response),
               K_(is_readonly),
               K_(consistency_level),
               K_(ls_id),
               K_(timeout_ts),
               K_(need_global_snapshot),
               K_(use_sync),
               K_(is_rollback),
               K_(did_async_commit),
               K_(create_cb_functor));
  int init(const ObTableConsistencyLevel consistency_level,
           const share::ObLSID &ls_id,
           int64_t timeout_ts,
           bool need_global_snapshot);
  int init(bool is_readonly,
           const ObTableConsistencyLevel consistency_level,
           const share::ObLSID &ls_id,
           int64_t timeout_ts,
           bool need_global_snapshot);
public:
  transaction::ObTxDesc *trans_desc_;
  transaction::ObTxReadSnapshot tx_snapshot_;
  rpc::ObRequest *req_;
  bool had_do_response_;
  bool is_readonly_;
  ObTableConsistencyLevel consistency_level_;
  sql::TransState trans_state_;
  sql::TransState *trans_state_ptr_;
  share::ObLSID ls_id_;
  int64_t timeout_ts_;
  bool need_global_snapshot_;
  ObHTableLockHandle *lock_handle_;
  bool use_sync_;
  bool is_rollback_;
  ObTableCreateCbFunctor *create_cb_functor_;
  bool did_async_commit_;
};

class ObTableTransUtils final
{
public:
  static const uint32_t OB_KV_DEFAULT_SESSION_ID = 0;
public:
  static int init_read_trans(ObTableTransParam &trans_param);
  static void release_read_trans(transaction::ObTxDesc *&trans_desc);
  static int start_trans(ObTableTransParam &trans_param);
  static int end_trans(ObTableTransParam &trans_param);
  static int sync_end_trans(ObTableTransParam &trans_param);
private:
  static int setup_tx_snapshot(ObTableTransParam &trans_param);
  static int async_commit_trans(ObTableTransParam &trans_param);
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_TRANS_UTILS_H */
