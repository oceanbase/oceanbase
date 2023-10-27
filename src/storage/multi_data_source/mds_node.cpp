/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "mds_node.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/list/ob_dlist.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "mds_table_base.h"
#include "mds_ctx.h"
#include "storage/multi_data_source/runtime_utility/mds_lock.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

MdsNodeStatus::MdsNodeStatus() { union_.value_ = 0; }

MdsNodeStatus::~MdsNodeStatus() { union_.value_ = 0; }

MdsNodeStatus::MdsNodeStatus(MdsNodeType node_type, WriterType writer_type)
{
  union_.field_.node_type_ = node_type;
  union_.field_.writer_type_ = writer_type;
  union_.field_.state_ = TwoPhaseCommitState::STATE_INIT;
  union_.field_.is_dumped_ = false;
}

MdsNodeStatus::MdsNodeStatus(const MdsNodeStatus &rhs) { union_.value_ = rhs.union_.value_; }

void MdsNodeStatus::set_dumped()// ATOMIC
{
  MdsNodeStatus old_node_status, new_node_status;
  do {
    old_node_status.union_.value_ = ATOMIC_LOAD(&union_.value_);
    new_node_status.union_.value_ = old_node_status.union_.value_;
    new_node_status.union_.field_.is_dumped_ = true;
  } while (!ATOMIC_BCAS(&union_.value_, old_node_status.union_.value_, new_node_status.union_.value_));
}

bool MdsNodeStatus::is_dumped() const// ATOMIC
{
  MdsNodeStatus node_status;
  node_status.union_.value_ = ATOMIC_LOAD(&union_.value_);
  return node_status.union_.field_.is_dumped_;
}

MdsNodeStatus &MdsNodeStatus::operator=(const MdsNodeStatus &rhs)// ATOMIC
{
  MdsNodeStatus old_node_status, new_node_status;
  do {
    old_node_status.union_.value_ = ATOMIC_LOAD(&union_.value_);
    new_node_status.union_.value_ = ATOMIC_LOAD(&rhs.union_.value_);
  } while (!ATOMIC_BCAS(&union_.value_, old_node_status.union_.value_, new_node_status.union_.value_));
  return *this;
}

MdsNodeStatus::Union::Union() : value_(0) {}

void MdsNodeStatus::advance(TwoPhaseCommitState new_stat)// ATOMIC
{
  MdsNodeStatus old_node_status, new_node_status;
  do {
    old_node_status.union_.value_ = ATOMIC_LOAD(&union_.value_);
    MDS_ASSERT(new_stat >= old_node_status.union_.field_.state_ &&
              new_stat < TwoPhaseCommitState::STATE_END &&
              new_stat > TwoPhaseCommitState::STATE_INIT);
    MDS_ASSERT(STATE_CHECK_ALLOWED_MAP[(int)old_node_status.union_.field_.state_][(int)new_stat] == true);
    new_node_status.union_.value_ = old_node_status.union_.value_;
    new_node_status.union_.field_.state_ = new_stat;
  } while (!ATOMIC_BCAS(&union_.value_, old_node_status.union_.value_, new_node_status.union_.value_));
}

TwoPhaseCommitState MdsNodeStatus::get_state() const// ATOMIC
{
  MdsNodeStatus node_status;
  node_status.union_.value_ = ATOMIC_LOAD(&union_.value_);
  return (TwoPhaseCommitState)node_status.union_.field_.state_;
}

int64_t MdsNodeStatus::to_string(char *buf, const int64_t buf_len) const// ATOMIC
{
  MdsNodeStatus node_status;
  node_status.union_.value_ = ATOMIC_LOAD(&union_.value_);
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "%s|", obj_to_string(node_status.union_.field_.node_type_));
  databuff_printf(buf, buf_len, pos, "%s|", obj_to_string(node_status.union_.field_.writer_type_));
  databuff_printf(buf, buf_len, pos, "%s|", obj_to_string(node_status.union_.field_.state_));
  databuff_printf(buf, buf_len, pos, "%s", to_cstring(node_status.union_.field_.is_dumped_));
  return pos;
}

MdsNode::MdsNode(MdsNodeType node_type,
                 WriterType writer_type,
                 const int64_t writer_id) :
ListNode<MdsNode>(),
status_(node_type, writer_type),
writer_id_(writer_id),
seq_no_(0),
redo_scn_(share::SCN::max_scn()),
end_scn_(share::SCN::max_scn()),
trans_version_(share::SCN::max_scn()),
mds_ctx_(nullptr) {}

void MdsNode::remove_self_if_in_mds_ctx_()
{
  if (OB_NOT_NULL(mds_ctx_)) {
    MDS_TG(1_ms);
    mds_ctx_->remove_node(this);
    MDS_LOG(INFO, "remove mds_node from mds_ctx", K(*this));
    mds_ctx_ = nullptr;
  }
}

MdsNode::~MdsNode() { ListNode<MdsNode>::~ListNode(); }

MdsWriter MdsNode::get_writer_() const
{
  return MdsWriter(status_.union_.field_.writer_type_, writer_id_);
}

bool MdsNode::is_aborted_() const
{
  return status_.get_state() == TwoPhaseCommitState::ON_ABORT;
}

bool MdsNode::is_committed_() const
{
  return status_.get_state() == TwoPhaseCommitState::ON_COMMIT;
}

bool MdsNode::is_decided_() const
{
  TwoPhaseCommitState state = status_.get_state();
  return state == TwoPhaseCommitState::ON_ABORT || state == TwoPhaseCommitState::ON_COMMIT;
}

share::SCN MdsNode::get_commit_version_() const
{
  return status_.get_state() == TwoPhaseCommitState::ON_COMMIT ?
         trans_version_ :
         share::SCN::max_scn();
}

share::SCN MdsNode::get_prepare_version_() const
{
  share::SCN prepare_vresion;
  TwoPhaseCommitState stat = status_.get_state();
  if (stat == TwoPhaseCommitState::STATE_INIT ||
      stat == TwoPhaseCommitState::ON_ABORT) {// not in two-phase-commit, should not see this node
    prepare_vresion = share::SCN::max_scn();
  } else if (stat == TwoPhaseCommitState::BEFORE_PREPARE) {// in two-phase-commit, see if need block
    prepare_vresion = share::SCN::min_scn();
  } else {// otherwise, trans_version is prepare version:
    // 1. either trans_version really is prepare version in BEFORE_PREPARE/ON_PREPARE phase.
    // 2. or trans_version is commit version in ON_COMMIT phase.
    prepare_vresion = trans_version_;
  }
  return prepare_vresion;
}

int MdsNodeInfoForVirtualTable::assign(const MdsNodeInfoForVirtualTable &rhs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(user_key_.assign(rhs.user_key_))) {
    MDS_LOG(WARN, "fail to copy user key", KR(ret));
  } else if (OB_FAIL(user_data_.assign(rhs.user_data_))) {
    MDS_LOG(WARN, "fail to copy user data", KR(ret));
  } else {
    ls_id_ = rhs.ls_id_;
    tablet_id_ = rhs.tablet_id_;
    unit_id_ = rhs.unit_id_;
    version_idx_ = rhs.version_idx_;
    writer_ = rhs.writer_;
    seq_no_ = rhs.seq_no_;
    redo_scn_ = rhs.redo_scn_;
    end_scn_ = rhs.end_scn_;
    trans_version_ = rhs.trans_version_;
    node_type_ = rhs.node_type_;
    state_ = rhs.state_;
    position_ = rhs.position_;
  }
  return ret;
}

}
}
}