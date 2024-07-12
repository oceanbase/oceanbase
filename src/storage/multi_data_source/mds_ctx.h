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

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_CTX_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_CTX_H

#include "buffer_ctx.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_small_spin_lock.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "mds_node.h"
#include "mds_writer.h"
#include "meta_programming/ob_type_traits.h"
namespace oceanbase
{
namespace share
{
class ObLSID;
}
namespace common
{
class ObTabletID;
}
namespace storage
{
namespace mds
{
class MdsTableHandle;
class MdsCtx : public BufferCtx
{
  friend class MdsNode;
  OB_UNIS_VERSION(1);
public:
  MdsCtx();
  explicit MdsCtx(const MdsWriter &writer,
                  const transaction::ObTxSEQ start_seq = transaction::ObTxSEQ::MIN_VAL());
  virtual ~MdsCtx();
  MdsCtx(const MdsCtx &) = delete;
  MdsCtx(MdsCtx &&) = delete;
  MdsCtx &operator=(const MdsCtx &) = delete;
  MdsCtx &operator=(MdsCtx &&) = delete;
  int assign(const MdsCtx &);
  int set_writer(const MdsWriter &writer);
  void set_seq_no(const transaction::ObTxSEQ seq_no);
  int inc_seq_no();
  transaction::ObTxSEQ get_seq_no() const;
  bool can_write() const;
  TO_STRING_KV(K_(writer), K_(write_list), K(obj_to_string(state_)));
  void record_written_node(ListNode<MdsNode> *node);
  virtual const MdsWriter get_writer() const override;
  virtual void before_prepare() override;
  virtual void on_redo(const share::SCN &redo_scn) override;
  virtual void on_prepare(const share::SCN &prepare_version) override;
  virtual void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn) override;
  virtual void on_abort(const share::SCN &abort_scn) override;
  void single_log_commit(const share::SCN commit_version, const share::SCN commit_scn);
  void single_log_abort();
  void remove_node(ListNode<MdsNode> *node);
private:
  template <typename OP, ENABLE_IF_LIKE_FUNCTION(OP, bool())>
  void do_while_retry_with_lock_until_success_for_all_(OP &&op, TwoPhaseCommitState new_state) {// if do op success, advance state to new_state
    bool operate_all_nodes_succeed = true;
    int64_t try_times = 0;
    do {
      MdsWLockGuard lg(lock_);
      if (state_ == TwoPhaseCommitState::ON_PREPARE && new_state == TwoPhaseCommitState::BEFORE_PREPARE) {// due to force majeure
        // do nothing, just accept it
        operate_all_nodes_succeed = true;
      } else {
        operate_all_nodes_succeed = op();
        if (OB_LIKELY(operate_all_nodes_succeed)) {
          if (new_state != TwoPhaseCommitState::STATE_END) {
            check_and_advance_two_phase_commit(state_, new_state);
          }
        } else if (OB_UNLIKELY((++try_times % 10000) == 0)) {
          MDS_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "do-while retry too much times", K(try_times), K(*this));
        }
      }
    } while (!operate_all_nodes_succeed && ({PAUSE(); true;}));// keep trying lock until success, while-try is for avoid thread deadlock
  }
  template <typename OP, ENABLE_IF_LIKE_FUNCTION(OP, bool(MdsNode &node))>
  bool for_each_node_try_(OP &&op) {// this is for ON_REDO/BEFORE_PREPARE/ON_PREPARE
    bool operate_all_nodes_succeed = true;
      write_list_.for_each_node_from_head_to_tail_until_true(
      [&operate_all_nodes_succeed, &op](const MdsNode &node) {
        MdsNode &cast_node = const_cast<MdsNode &>(node);
        operate_all_nodes_succeed = op(cast_node);
        return !operate_all_nodes_succeed;
      }
    );
    return operate_all_nodes_succeed;
  }
  template <typename OP, ENABLE_IF_LIKE_FUNCTION(OP, bool(MdsNode &node))>
  bool for_each_node_fetch_to_try_(OP &&op) {// this is for ON_COMMIT/ON_ABORT/SINGLE_LOG_COMMIT/SINGLE_LOG_ABORT
    bool operate_all_nodes_succeed = true;
    ListNode<MdsNode> *head = nullptr;
    while (operate_all_nodes_succeed && OB_NOT_NULL(head = write_list_.fetch_from_head())) {
      MdsNode *cast_head = static_cast<MdsNode *>(head);
      operate_all_nodes_succeed = op(*cast_head);
      if (!operate_all_nodes_succeed) {// rollback
        write_list_.insert_into_head(cast_head);
      }
    };
    return operate_all_nodes_succeed;
  }
private:
  List<MdsNode> write_list_;
  TwoPhaseCommitState state_;
  MdsLock lock_;
protected:
  MdsWriter writer_;
  transaction::ObTxSEQ seq_no_;
};
OB_SERIALIZE_MEMBER_TEMP(inline, MdsCtx, writer_);
}
}
}
#endif
