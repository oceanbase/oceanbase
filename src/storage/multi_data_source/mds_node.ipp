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

#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_NODE_IPP
#define STORAGE_MULTI_DATA_SOURCE_MDS_NODE_IPP

#include "lib/ob_abort.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/multi_data_source/runtime_utility/list_helper.h"
#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_NODE_H_IPP
#define STORAGE_MULTI_DATA_SOURCE_MDS_NODE_H_IPP
#include "mds_node.h"
#endif

namespace oceanbase
{
namespace storage
{
namespace mds
{
template <typename K, typename V>
UserMdsNode<K, V>::UserMdsNode()
: MdsNode(MdsNodeType::UNKNOWN_NODE,
          WriterType::UNKNOWN_WRITER,
          INVALID_VALUE,
          transaction::ObTxSEQ()),
ListNode<UserMdsNode<K, V>>() { p_mds_row_ = nullptr; }

template <typename K, typename V>
UserMdsNode<K, V>::UserMdsNode(MdsRowBase<K, V> *p_mds_row,
                               MdsNodeType node_type,
                               WriterType writer_type,
                               const int64_t writer_id,
                               const transaction::ObTxSEQ seq_no)
: MdsNode(node_type, writer_type, writer_id, seq_no),
ListNode<UserMdsNode<K, V>>()
{
  static_assert(std::is_default_constructible<V>::value,
                "User Data MUST SUPPORT default construction");
  p_mds_row_ = p_mds_row;
  if (OB_LIKELY(has_valid_link_back_ptr_())) {
    p_mds_row_->p_mds_unit_->p_mds_table_->inc_valid_node_cnt();
  }
}

template <typename K, typename V>
UserMdsNode<K, V>::~UserMdsNode()
{
  remove_self_if_in_mds_ctx_();
  if (OB_UNLIKELY((nullptr != ((ListNode<UserMdsNode<K, V>>*)this)->prev_) ||
                  (nullptr != ((ListNode<UserMdsNode<K, V>>*)this)->next_) ||
                  (nullptr != ((ListNode<MdsNode>*)this)->prev_) ||
                  (nullptr != ((ListNode<MdsNode>*)this)->next_))) {
    ob_abort();
  }
  if (OB_LIKELY(has_valid_link_back_ptr_())) {
    MDS_LOG(INFO, "mds node destructed", K(*this));
    p_mds_row_->p_mds_unit_->p_mds_table_->dec_valid_node_cnt();
    p_mds_row_ = nullptr;
  }
}

template <typename K, typename V>
bool UserMdsNode<K, V>::operator<(const UserMdsNode<K, V> &rhs) const
{
  return redo_scn_ < rhs.redo_scn_;
}

template <typename K, typename V>
bool UserMdsNode<K, V>::operator==(const UserMdsNode<K, V> &rhs) const
{
  return redo_scn_ == rhs.redo_scn_;
}

template <typename K, typename V>
int64_t UserMdsNode<K, V>::to_string(char * buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  MdsTableBase *p_mds_table = nullptr;
  if (p_mds_row_ && p_mds_row_->p_mds_unit_) {
    p_mds_table = p_mds_row_->p_mds_unit_->p_mds_table_;
  }
  databuff_printf(buf, buf_len, pos, "this:0x%lx, ", (unsigned long)this);
  databuff_printf(buf, buf_len, pos, "ls_id:%s, ", p_mds_table ? to_cstring(p_mds_table->ls_id_) : "NULL");
  databuff_printf(buf, buf_len, pos, "tablet_id:%s, ", p_mds_table ? to_cstring(p_mds_table->tablet_id_) : "NULL");
  databuff_printf(buf, buf_len, pos, "writer:%ld, ", writer_id_);
  databuff_printf(buf, buf_len, pos, "seq_no:%s, ", to_cstring(seq_no_));
  databuff_printf(buf, buf_len, pos, "redo_scn:%s, ", obj_to_string(redo_scn_));
  databuff_printf(buf, buf_len, pos, "end_scn:%s, ", obj_to_string(end_scn_));
  databuff_printf(buf, buf_len, pos, "trans_version:%s, ", obj_to_string(trans_version_));
  databuff_printf(buf, buf_len, pos, "status:%s, ", to_cstring(status_));
  databuff_printf(buf, buf_len, pos, "ver_next:0x%lx, ",
  (unsigned long)(static_cast<UserMdsNode*>(static_cast<ListNode<UserMdsNode<K, V>>*>(ListNode<UserMdsNode<K, V>>::next_))));
  databuff_printf(buf, buf_len, pos, "linked:0x%lx, ",
  (unsigned long)(static_cast<UserMdsNode*>(static_cast<ListNode<MdsNode>*>(ListNode<MdsNode>::next_))));
  databuff_printf(buf, buf_len, pos, "mds_ctx:0x%lx, ", (unsigned long)(mds_ctx_));
  databuff_printf(buf, buf_len, pos, "user_data:%s", to_cstring(user_data_));
  return pos;
}

template <typename K, typename V>
MdsNodeType UserMdsNode<K, V>::get_node_type() const
{
  MDS_TG(1_ms);
  CLICK();
  return status_.union_.field_.node_type_;
}

template <typename K, typename V>
bool UserMdsNode<K, V>::try_on_redo(const share::SCN &redo_scn)
{
  MDS_TG(1_ms);
  TryLockGuard lg(this);
  if (lg.lock_succeed()) {
    CLICK();
    on_redo_(redo_scn);
  }
  return lg.lock_succeed();
}

template <typename K, typename V>
void UserMdsNode<K, V>::on_redo_(const share::SCN &redo_scn)
{
  MdsTableBase *p_mds_table = nullptr;
  if (p_mds_row_ && p_mds_row_->p_mds_unit_) {
    p_mds_table = p_mds_row_->p_mds_unit_->p_mds_table_;
  }
  if (is_valid_scn_(redo_scn_)) {
    if (redo_scn != redo_scn_) {
      MDS_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "there has been a valid reso scn!", K(*this), K(redo_scn));
    }
  } else if (!is_valid_scn_(redo_scn)) {
    MDS_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "redo scn is not a valid scn!", K(*this), K(redo_scn));
  } else {
    if (OB_ISNULL(p_mds_table)) {
      MDS_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "pointer is NULL, can not try decline rec_scn", K(*this), K(redo_scn));
    } else {
      p_mds_table->try_decline_rec_scn(redo_scn);
    }
    redo_scn_ = redo_scn;
    on_user_data_redo_(redo_scn);
    report_event_("ON_REDO");
  }
}

template <typename K, typename V>
bool UserMdsNode<K, V>::try_before_prepare()
{
  MDS_TG(1_ms);
  TryLockGuard lg(this);
  if (lg.lock_succeed()) {
    CLICK();
    before_prepare_();
  }
  return lg.lock_succeed();
}

template <typename K, typename V>
void UserMdsNode<K, V>::before_prepare_()
{
  if (status_.union_.field_.state_ == TwoPhaseCommitState::BEFORE_PREPARE) {// reentrant
    MDS_ASSERT(trans_version_.is_min());
  } else {
    status_.advance(TwoPhaseCommitState::BEFORE_PREPARE);
    trans_version_.set_min();// block all read operation
    report_event_("BEFORE_PREPARE");
  }
}

template <typename K, typename V>
bool UserMdsNode<K, V>::try_on_prepare(const share::SCN &prepare_version)
{
  MDS_TG(1_ms);
  TryLockGuard lg(this);
  if (lg.lock_succeed()) {
    CLICK();
    on_prepare_(prepare_version);
  }
  return lg.lock_succeed();
}

template <typename K, typename V>
void UserMdsNode<K, V>::on_prepare_(const share::SCN &prepare_version)
{
  if (status_.union_.field_.state_ == TwoPhaseCommitState::ON_PREPARE) {// reentrant
    if (is_valid_scn_(prepare_version)) {
      MDS_ASSERT(trans_version_ == prepare_version);
    }
  } else {
    status_.advance(TwoPhaseCommitState::ON_PREPARE);
    if (is_valid_scn_(prepare_version)) {
      trans_version_ = prepare_version;
    }
    report_event_("ON_PREPARE");
  }
}

template <typename K, typename V>
bool UserMdsNode<K, V>::try_on_commit(const share::SCN &commit_version,
                                      const share::SCN &commit_scn)
{
  bool lock_success = true;
  MDS_TG(1_ms);
  TryLockGuard lg(this);
  if (lg.lock_succeed()) {
    CLICK();
    on_commit_(commit_version, commit_scn);
  }
  return lg.lock_succeed();
}

template <typename K, typename V>
void UserMdsNode<K, V>::on_commit_(const share::SCN &commit_version,
                                   const share::SCN &commit_scn)
{
  if (!is_valid_scn_(commit_scn) || !is_valid_scn_(commit_scn)) {
    MDS_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "commit version or commit scn is not a valid scn!",
                    K(*this), K(commit_version), K(commit_scn));
  } else {
    if (status_.union_.field_.state_ == TwoPhaseCommitState::ON_COMMIT) {// reentrant
      MDS_ASSERT(trans_version_ == commit_version);
      MDS_ASSERT(end_scn_ == commit_scn);
    } else {
      status_.advance(TwoPhaseCommitState::ON_COMMIT);
      trans_version_ = commit_version;
      end_scn_ = commit_scn;
      if (!redo_scn_.is_max() && end_scn_ < redo_scn_) {
        MDS_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "end scn lower than redo scn", K(*this));
      }
      on_user_data_commit_(commit_version, commit_scn);
      report_event_("ON_COMMIT");
      mds_ctx_ = nullptr;// if this node destroyed, don't access to ctx again
    }
  }
}

template <typename K, typename V>
bool UserMdsNode<K, V>::try_on_abort(const share::SCN &abort_scn)// CAUTIONS: non-reentrant!!!
{
  MDS_TG(1_ms);
  TryLockGuard lg(this);
  if (lg.lock_succeed()) {
    CLICK();
    status_.advance(TwoPhaseCommitState::ON_ABORT);
    end_scn_ = abort_scn;
    on_user_data_abort_(abort_scn);
    MDS_LOG(INFO, "mds node on_abort", K(*this), K(abort_scn));
    if (is_valid_scn_(redo_scn_) && is_valid_scn_(abort_scn)) {
      if (end_scn_ < redo_scn_) {
        MDS_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "end scn lower than redo scn", K(*this), K(abort_scn));
      }
    } else {
      report_event_("ON_ABORT");
    }
    mds_ctx_ = nullptr;// this node will be destroyed in abort callback, if ctx is valid, will try to remove self from ctx, and will deadlock with self thread
    if (OB_NOT_NULL(p_mds_row_)) {
      // this must be last line of this function, caus this node may be deleted inner
      p_mds_row_->node_abort_callback_((ListNodeBase*)(ListNode<UserMdsNode<K, V>>*)(this));
    }
  }
  return lg.lock_succeed();
}

template <typename K, typename T>
bool UserMdsNode<K, T>::try_single_log_commit(const share::SCN &commit_version, const share::SCN &commit_scn)
{
  MDS_TG(1_ms);
  TryLockGuard lg(this);
  if (lg.lock_succeed()) {
    CLICK();
    if (redo_scn_.is_max()) {
      on_redo_(commit_scn);
    }
    before_prepare_();
    on_prepare_(commit_version);
    on_commit_(commit_version, commit_scn);
  }
  return lg.lock_succeed();
}

template <typename K, typename V>
bool UserMdsNode<K, V>::is_valid_scn_(const share::SCN &scn) const
{
  return scn.is_valid() && !scn.is_max() && !scn.is_min();
}

template <typename K, typename V>
bool UserMdsNode<K, V>::has_valid_link_back_ptr_() const
{
  return OB_NOT_NULL(p_mds_row_) &&
         OB_NOT_NULL(p_mds_row_->key_) &&
         OB_NOT_NULL(p_mds_row_->p_mds_unit_) &&
         OB_NOT_NULL(p_mds_row_->p_mds_unit_->p_mds_table_);
}

template <typename K, typename V>
int UserMdsNode<K, V>::fill_virtual_info(MdsNodeInfoForVirtualTable &mds_node_info) const
{
  int ret = OB_SUCCESS;
  constexpr int64_t buffer_size = 2_KB;
  char stack_buffer[buffer_size] = { 0 };
  int64_t pos = 0;
  if (FALSE_IT(databuff_printf(stack_buffer, buffer_size, pos, "%s", to_cstring(user_data_)))) {
  } else if (OB_FAIL(mds_node_info.user_data_.assign(ObString(pos, stack_buffer)))) {
    MDS_LOG(WARN, "fail construct ObStringHolder", K(*this));
  } else {
    if (OB_LIKELY(has_valid_link_back_ptr_())) {
      mds_node_info.ls_id_ = p_mds_row_->p_mds_unit_->p_mds_table_->get_ls_id();
      mds_node_info.tablet_id_ = p_mds_row_->p_mds_unit_->p_mds_table_->get_tablet_id();
    }
    mds_node_info.writer_ = MdsWriter(status_.union_.field_.writer_type_, writer_id_);
    mds_node_info.seq_no_ = seq_no_;
    mds_node_info.redo_scn_ = redo_scn_;
    mds_node_info.end_scn_ = end_scn_;
    mds_node_info.trans_version_ = trans_version_;
    mds_node_info.node_type_ = status_.union_.field_.node_type_;
    mds_node_info.state_ = status_.union_.field_.state_;
    mds_node_info.position_ = NodePosition::MDS_TABLE;
  }
  return ret;
}

template <typename K, typename V>
template <int N>
void UserMdsNode<K, V>::report_event_(const char (&event_str)[N],
                                      const char *file,
                                      const uint32_t line,
                                      const char *function_name) const
{
  int ret = OB_SUCCESS;
  constexpr int64_t buffer_size = 1_KB;
  char stack_buffer[buffer_size] = { 0 };
  observer::MdsEvent event;
  if (OB_UNLIKELY(!has_valid_link_back_ptr_())) {
    // do nothing
  } else if (OB_FAIL(fill_event_(event, event_str, stack_buffer, buffer_size))) {
    MDS_LOG(WARN, "fail fill mds event", K(*this));
  } else {
    observer::MdsEventKey key(MTL_ID(),
                              p_mds_row_->p_mds_unit_->p_mds_table_->ls_id_,
                              p_mds_row_->p_mds_unit_->p_mds_table_->tablet_id_);
    observer::ObMdsEventBuffer::append(key, event, p_mds_row_->p_mds_unit_->p_mds_table_, file, line, function_name);
  }
}

template <typename K, typename V>
template <int N>
int UserMdsNode<K, V>::fill_event_(observer::MdsEvent &event,
                                   const char (&event_str)[N],
                                   char *stack_buffer,
                                   const int64_t buffer_size) const
{
  int ret = OB_SUCCESS;
  MDS_TG(1_ms);
  int64_t pos = 0;
  int64_t last_pos = 0;
  if (FALSE_IT(databuff_printf(stack_buffer, buffer_size, pos, "%s", to_cstring(user_data_)))) {
  } else if (FALSE_IT(event.info_str_.assign_ptr(stack_buffer, pos))) {
  } else if (FALSE_IT(last_pos = pos)) {
  } else if (FALSE_IT(databuff_printf(stack_buffer, buffer_size, pos, "%s", p_mds_row_ ? to_cstring(*p_mds_row_->key_) : "NULL"))) {
  } else if (FALSE_IT(event.key_str_.assign_ptr(&stack_buffer[last_pos], pos - last_pos))) {
  } else if (FALSE_IT(last_pos = pos)) {
  } else {
    event.record_thread_info_();
    event.event_ = event_str;
    if (OB_LIKELY(has_valid_link_back_ptr_())) {
      event.unit_id_ = p_mds_row_->p_mds_unit_->unit_id_;
    }
    event.writer_type_ = status_.union_.field_.writer_type_;
    event.writer_id_ = writer_id_;
    event.seq_no_ = seq_no_;
    event.redo_scn_ = redo_scn_;
    event.end_scn_ = end_scn_;
    event.trans_version_ = trans_version_;
    event.node_type_ = status_.union_.field_.node_type_;
    event.state_ = status_.union_.field_.state_;
  }
  return ret;
}
#undef CONSTRUCT_ROW_LOCK_GUARD
}
}
}
#endif