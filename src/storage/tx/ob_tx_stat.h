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

#ifndef OCEANBASE_TRANSACTION_OB_TX_STAT_
#define OCEANBASE_TRANSACTION_OB_TX_STAT_

#include "ob_trans_define.h"
#include "common/ob_range.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace memtable
{
struct ObTxCallbackListStat
{
  int id_;
  share::SCN sync_scn_;
  int length_;
  int logged_;
  int removed_;
  int branch_removed_;
  void reset() {}
  DECLARE_TO_STRING
  {
    int64_t pos = 0;
    BUF_PRINTF("[%d,%d,%d,%d,%d,%ld]",
               id_,
               length_,
               logged_,
               removed_,
               branch_removed_,
               (sync_scn_.is_valid() ? sync_scn_.get_val_for_inner_table_field() : 0));
    return pos;
  }
};

} // memtable namespace

namespace transaction
{

struct ObTxStat
{
  ObTxStat() { reset(); }
  ~ObTxStat() { }
  void reset();
  int init(const common::ObAddr &addr, const ObTransID &tx_id,
           const uint64_t tenant_id,  const bool has_decided,
           const share::ObLSID &ls_id, const share::ObLSArray &participants,
           const int64_t tx_ctx_create_time, const int64_t tx_expired_time,
           const int64_t ref_cnt, const int64_t last_op_sn,
           const int64_t pending_write, const int64_t state,
           const int tx_type, const int64_t part_tx_action,
           const void* const tx_ctx_addr,
           const int64_t pending_log_size, const int64_t flushed_log_size,
           const int64_t role_state,
           const int64_t session_id, const common::ObAddr &scheduler,
           const bool is_exiting, const ObXATransID &xid,
           const share::ObLSID &coord, const int64_t last_request_ts,
           share::SCN start_scn, share::SCN end_scn, share::SCN rec_scn, bool transfer_blocking,
           const int busy_cbs_cnt,
           int replay_completeness,
           share::SCN serial_final_scn);
  TO_STRING_KV(K_(addr), K_(tx_id), K_(tenant_id),
               K_(has_decided), K_(ls_id), K_(participants),
               K_(tx_ctx_create_time), K_(tx_expired_time), K_(ref_cnt),
               K_(last_op_sn), K_(pending_write), K_(state), K_(tx_type),
               KP_(tx_ctx_addr),
               K_(pending_log_size), K_(flushed_log_size),
               K_(role_state), K_(session_id),
               K_(scheduler_addr), K_(is_exiting),
               K_(xid), K_(coord), K_(last_request_ts),
               K_(xid), K_(coord), K_(last_request_ts),
               K_(start_scn), K_(end_scn), K_(rec_scn), K_(transfer_blocking),
               K_(busy_cbs_cnt),
               K_(serial_final_scn),
               K_(replay_completeness),
               K_(callback_list_stats));
public:
  bool is_inited_;
  common::ObAddr addr_;
  ObTransID tx_id_;
  uint64_t tenant_id_;
  bool has_decided_;
  share::ObLSID ls_id_;
  share::ObLSArray participants_;
  int64_t tx_ctx_create_time_;
  int64_t tx_expired_time_;
  int64_t ref_cnt_;
  int64_t last_op_sn_;
  int64_t pending_write_;
  int64_t state_;
  int tx_type_;
  int64_t part_tx_action_;
  const void *tx_ctx_addr_;
  int64_t pending_log_size_;
  int64_t flushed_log_size_;
  int64_t role_state_;
  int64_t session_id_;
  common::ObAddr scheduler_addr_;
  bool is_exiting_;
  ObXATransID xid_;
  share::ObLSID coord_;
  int64_t last_request_ts_;
  share::SCN start_scn_;
  share::SCN end_scn_;
  share::SCN rec_scn_;
  bool transfer_blocking_;
  int busy_cbs_cnt_;
  int replay_completeness_;
  share::SCN serial_final_scn_;
  ObSEArray<memtable::ObTxCallbackListStat, 1> callback_list_stats_;
  struct CLStatsDisplay {
    CLStatsDisplay(ObSEArray<memtable::ObTxCallbackListStat, 1> &stats): stats_(stats) {}
    ObSEArray<memtable::ObTxCallbackListStat, 1> &stats_;
    DECLARE_TO_STRING
    {
      int64_t pos = 0;
      if (stats_.count() > 0) {
        J_ARRAY_START();
        BUF_PRINTF("\"id, length, logged, removed, branch_removed, sync_scn\"");
        for (int i =0; i < stats_.count(); i++) {
          if (stats_.at(i).id_ >= 0) {
            J_COMMA();
            BUF_PRINTO(stats_.at(i));
          }
        }
        J_ARRAY_END();
      }
      return pos;
    }
  };
  CLStatsDisplay get_callback_list_stats_displayer()
  {
    return CLStatsDisplay(callback_list_stats_);
  }
};

class ObTxLockStat
{
public:
  ObTxLockStat() { reset(); }
  ~ObTxLockStat() {}
  int init(const common::ObAddr &addr,
            uint64_t tenant_id,
            const share::ObLSID &ls_id,
            const ObMemtableKeyInfo &memtable_key_info,
            uint32_t session_id,
            uint64_t proxy_session_id,
            const ObTransID &tx_id,
            int64_t tx_ctx_create_time,
            int64_t tx_expired_time);
  void reset();
  const common::ObAddr &get_addr() const { return addr_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  const share::ObLSID &get_ls_id() const { return ls_id_; }
  const ObMemtableKeyInfo &get_memtable_key_info() const { return memtable_key_info_; }
  uint32_t get_session_id() const { return session_id_; }
  uint64_t get_proxy_session_id() const { return proxy_session_id_; }
  const ObTransID &get_tx_id() const { return tx_id_; }
  int64_t get_tx_ctx_create_time() const { return tx_ctx_create_time_; }
  int64_t get_tx_expired_time() const { return tx_expired_time_; }

  TO_STRING_KV(K_(addr),
               K_(tenant_id),
               K_(ls_id),
               K_(memtable_key_info),
               K_(session_id),
               K_(proxy_session_id),
               K_(tx_id),
               K_(tx_ctx_create_time),
               K_(tx_expired_time));

private:
  bool is_inited_;
  common::ObAddr addr_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObMemtableKeyInfo memtable_key_info_;
  uint32_t session_id_;
  uint64_t proxy_session_id_;
  ObTransID tx_id_;
  int64_t tx_ctx_create_time_;
  int64_t tx_expired_time_;
};

class ObTxSchedulerStat
{
public:
  ObTxSchedulerStat() { reset(); }
  ~ObTxSchedulerStat() { }
  void reset();
  int init(const uint64_t tenant_id,
            const common::ObAddr &addr,
            const uint32_t sess_id,
            const ObTransID &tx_id,
            const int64_t state,
            const int64_t cluster_id,
            const ObXATransID &xid,
            const share::ObLSID &coord_id,
            const ObTxPartList &parts,
            const ObTxIsolationLevel &isolation,
            const share::SCN &snapshot_version,
            const ObTxAccessMode &access_mode,
            const uint64_t op_sn,
            const uint64_t flag,
            const int64_t active_ts,
            const int64_t expire_ts,
            const int64_t timeout_us,
            const int32_t ref_cnt,
            const void* const tx_desc_addr,
            const ObTxSavePointList &savepoints,
            const int16_t abort_cause,
            const bool can_elr);
  TO_STRING_KV(K_(tenant_id), K_(addr), K_(sess_id),
               K_(tx_id), K_(state), K_(cluster_id),
               K_(xid), K_(coord_id), K_(parts),
               K_(isolation), K_(snapshot_version),
               K_(access_mode), K_(op_sn),
               K_(flag), K_(active_ts), K_(expire_ts),
               K_(timeout_us), K_(ref_cnt),
               K_(tx_desc_addr), K_(savepoints),
               K_(abort_cause), K_(can_elr));
  int64_t get_parts_str(char* buf, const int64_t buf_len);
  int get_valid_savepoints(const ObTxSavePointList &savepoints);

public:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObAddr addr_;
  uint32_t sess_id_;
  ObTransID tx_id_;
  int64_t state_;
  int64_t cluster_id_;
  ObXATransID xid_;
  share::ObLSID coord_id_;
  ObTxPartList parts_;
  ObTxIsolationLevel isolation_;
  share::SCN snapshot_version_;
  ObTxAccessMode access_mode_;
  uint64_t op_sn_;
  uint64_t flag_;
  int64_t active_ts_;
  int64_t expire_ts_;
  int64_t timeout_us_;
  int32_t ref_cnt_;
  const void *tx_desc_addr_;
  ObTxSavePointList savepoints_;
  int16_t abort_cause_;
  bool can_elr_;
};

} // transaction
} // oceanbase
#endif // OCEANABAE_TRANSACTION_OB_TX_STAT_
