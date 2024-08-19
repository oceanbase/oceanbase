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

#ifndef OCEANBASE_UNITTEST_STORAGE_TX_OB_MOCK_TX_CTX
#define OCEANBASE_UNITTEST_STORAGE_TX_OB_MOCK_TX_CTX

#include "storage/tx/ob_trans_part_ctx.h"
#include "ob_mailbox.h"
#include "ob_mock_2pc_ctx.h"

namespace oceanbase
{
namespace transaction
{

class MockObTxCtx : public ObMailHandler<ObTxMsg>,
                    public ObPartTransCtx
{
public:
  ~MockObTxCtx() { destroy(); }
  int init(const share::ObLSID &ls_id,
           const ObTransID &trans_id,
           ObLSTxCtxMgr *ctx_mgr,
           ObTxData *tx_data,
           ObMailBoxMgr<ObTxMsg> *mgr);
  virtual int submit_log(const ObTwoPhaseCommitLogType& log_type) override;
  int handle(const bool must_have = true);
  int handle_all();
  int apply();
  void change_to_leader() { role_state_ = TxCtxRoleState::LEADER; }
  int handle(const ObMail<ObTxMsg>& mail);
  int64_t get_mailbox_addr() { return addr_; }
  static int mock_tx_commit_msg(const ObTransID &trans_id,
                                const share::ObLSID &ls_id,
                                const std::vector<share::ObLSID> &participants,
                                ObTxCommitMsg &msg);
  static int build_scheduler_mailbox(ObMailBoxMgr<ObTxMsg>* mailbox_mgr);
  static int check_mail(ObMailBox<ObTxMsg> mailbox,
                        int64_t from,
                        int64_t to,
                        int64_t type);
  bool check_status_valid(const bool should_commit);
  void destroy();
  void set_exiting_();
  virtual int register_timeout_task_(const int64_t interval_us);
  virtual int unregister_timeout_task_();
  INHERIT_TO_STRING_KV("ObPartTransCtx", ObPartTransCtx,
                       K_(addr), K_(mailbox), K_(log_queue), K_(collected));
public:
  int64_t scheduler_addr_ = 0;
  static ObMailBox<ObTxMsg> scheduler_mailbox_;
protected:
  virtual int post_msg_(const share::ObLSID &receiver, ObTxMsg &msg) override;
  virtual int post_msg_(const ObAddr &receiver, ObTxMsg &msg) override;
  virtual int get_gts_(share::SCN &gts) override;
  virtual int wait_gts_elapse_commit_version_(bool &need_wait) override;
  virtual int get_local_max_read_version_(share::SCN &local_max_read_version) override;
  virtual int update_local_max_commit_version_(const share::SCN &) override;
private:
  MockObLogQueue log_queue_;
  int64_t addr_;
  ObMailBox<ObTxMsg> mailbox_;
  ObMailBoxMgr<ObTxMsg>* mailbox_mgr_;
  std::map<share::ObLSID, int64_t> addr_memo_;
  std::map<int64_t, share::ObLSID> ls_memo_;
};

const static int64_t MOCK_SCHEDULER_ADDR = INT64_MAX;


} // transaction
} // oceanbase

#endif // OCEANBASE_UNITTEST_STORAGE_TX_OB_MOCK_TX_CTX

