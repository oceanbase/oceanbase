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
#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_NODE_H
#define STORAGE_MULTI_DATA_SOURCE_MDS_NODE_H

#include "lib/atomic/ob_atomic.h"
#include "lib/list/ob_dlist.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/queue/ob_link.h"
#include "ob_clock_generator.h"
#include "ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "runtime_utility/common_define.h"
#include "runtime_utility/list_helper.h"
#include <atomic>
#include <cstdint>
#include <type_traits>
#include "mds_writer.h"
#include "deps/oblib/src/common/meta_programming/ob_type_traits.h"
#include "runtime_utility/mds_lock.h"
#include "lib/string/ob_string_holder.h"
#include "meta_programming/ob_meta_copy.h"
#include "mds_table_base.h"
#include "runtime_utility/mds_factory.h"

namespace oceanbase
{
namespace unittest
{
class TestMdsUserNode;
}
namespace storage
{
namespace mds
{
class MdsNode;
template <typename K, typename V>
class UserMdsNode;
class MdsDumpNode;
class MdsTableBase;
class MdsNodeBase;
class MdsCtx;

struct MdsNodeInfoForVirtualTable
{
  MdsNodeInfoForVirtualTable()
  : ls_id_(), tablet_id_(), unit_id_(UINT8_MAX), user_key_(), version_idx_(-1), writer_(), seq_no_(-1), redo_scn_(),
  end_scn_(), trans_version_(), node_type_(MdsNodeType::TYPE_END), state_(TwoPhaseCommitState::STATE_END),
  position_(NodePosition::POSITION_END), user_data_() {}
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  int64_t unit_id_;
  common::ObStringHolder user_key_;
  int64_t version_idx_;
  MdsWriter writer_;
  int64_t seq_no_;
  share::SCN redo_scn_;
  share::SCN end_scn_;
  share::SCN trans_version_;
  MdsNodeType node_type_;
  TwoPhaseCommitState state_;
  NodePosition position_;
  common::ObStringHolder user_data_;
  int assign(const MdsNodeInfoForVirtualTable &rhs);
  int64_t to_string(char *buf, const int64_t len) const { return 0; }
};

template <typename K, typename V>
struct MdsUnitBase
{
  MdsUnitBase() : p_mds_table_(nullptr), unit_id_(UINT8_MAX) {}
  ~MdsUnitBase() { p_mds_table_ = nullptr; }
  MdsTableBase *p_mds_table_;
  uint8_t unit_id_;
};

template <typename K, typename V>
struct MdsRowBase
{
  MdsRowBase() : p_mds_unit_(nullptr), key_(nullptr) {}
  virtual ~MdsRowBase() { p_mds_unit_ = nullptr; key_ = nullptr; }
  // if a node aborted, delete it immediately.
  virtual void node_abort_callback_(ListNodeBase *node) = 0;
  MdsUnitBase<K, V> *p_mds_unit_;
  K *key_;
  mutable MdsLock lock_;
};

struct MdsNodeStatus// No need lock, atomic operatrion for all interfaces
{
  OB_UNIS_VERSION(1);
public:
  MdsNodeStatus();
  ~MdsNodeStatus();
  MdsNodeStatus(MdsNodeType node_type, WriterType writer_type);
  MdsNodeStatus(const MdsNodeStatus &rhs);
  MdsNodeStatus &operator=(const MdsNodeStatus &rhs);
  void advance(TwoPhaseCommitState new_stat);
  void set_dumped();
  bool is_dumped() const;
  TwoPhaseCommitState get_state() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  union Union {
    Union();
    struct {
      // won't change
      MdsNodeType node_type_ : 4;
      WriterType writer_type_ : 8;
      // can be changed
      TwoPhaseCommitState state_ : 4;
      bool is_dumped_ : 1;
    } field_;
    uint32_t value_;
  } union_;
};
OB_SERIALIZE_MEMBER_TEMP(inline, MdsNodeStatus, union_.value_);

// define a base NO TEMPLATE class
// so that most method can be defined in cpp file
class MdsNode : public ListNode<MdsNode>
{
  template <typename MdsTableType>
  friend class MdsTableImpl;
  template <typename K, typename V>
  friend class MdsRow;
  template <typename K, typename V>
  friend class UserMdsNode;
  friend class MdsDumpNode;
public:
  MdsNode(MdsNodeType node_type,
          WriterType writer_type,
          const int64_t writer_id);
  virtual ~MdsNode() override;
public:// log sync and two-phase-commit related
  virtual bool try_on_redo(const share::SCN &redo_scn) = 0;
  virtual bool try_before_prepare() = 0;
  virtual bool try_on_prepare(const share::SCN &prepare_version) = 0;
  virtual bool try_on_commit(const share::SCN &commit_version, const share::SCN &commit_scn) = 0;
  virtual bool try_on_abort(const share::SCN &abort_scn) = 0;
  virtual bool try_single_log_commit(const share::SCN &commit_version, const share::SCN &commit_scn) = 0;
  virtual int64_t to_string(char *buf, const int64_t buf_len) const = 0;
  virtual int fill_virtual_info(MdsNodeInfoForVirtualTable &mds_node_indo) const = 0;
public:// node states related
  void remove_self_if_in_mds_ctx_();
  void set_dumped_() { status_.set_dumped(); }
  bool is_dumped_() const { return status_.is_dumped(); }
  MdsWriter get_writer_() const;
  bool is_persisted_() const;
  bool is_aborted_() const;
  bool is_committed_() const;
  bool is_decided_() const;
  share::SCN get_commit_version_() const;
  share::SCN get_prepare_version_() const;
public:
  MdsNodeStatus status_;// include lock state, type state, persisted state and two-phase-commit state
  int64_t writer_id_; // mostly is tx id, and maybe not tx id, depends on writer_type_ in status_;
  int64_t seq_no_;// not used for now
  share::SCN redo_scn_; // log scn of redo
  share::SCN end_scn_; // log scn of commit/abort
  share::SCN trans_version_; // read as prepare version if phase is not COMMIT, or read as commit version
  MdsCtx *mds_ctx_;// need remove node from ctx when forcelly delete node before decided
};

template <typename K, typename V>
class UserMdsNode final : public MdsNode, public ListNode<UserMdsNode<K, V>>// this order can not be changed!!!
{
  template <typename MdsTableType>
  friend class MdsTableImpl;
  template <typename K1, typename V1>
  friend class MdsRow;
  friend class MdsDumpNode;
private:
  struct TryLockGuard {// for RAII
    TryLockGuard(UserMdsNode<K, V> *p_mds_node) : lock_(nullptr) {
      if (OB_NOT_NULL(p_mds_node->p_mds_row_)) {
        bool lock_succ = p_mds_node->p_mds_row_->lock_.try_wrlock();
        if (OB_LIKELY(lock_succ)) {
          lock_ = &p_mds_node->p_mds_row_->lock_;
        }
      } else {
        OCCAM_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "not expected valid mds_node! no row related!", KPC(p_mds_node));
      }
    }
    bool lock_succeed() const {
      return OB_NOT_NULL(lock_);
    }
    ~TryLockGuard() {
      if (OB_NOT_NULL(lock_)) {
        lock_->unlock();
      }
    }
    MdsLock *lock_;
  };
public:
  UserMdsNode();
  UserMdsNode(MdsRowBase<K, V> *p_mds_row,
              MdsNodeType node_type,
              WriterType writer_type,
              const int64_t writer_id);
  ~UserMdsNode();
  bool operator<(const UserMdsNode<K, V> &rhs) const;
  bool operator==(const UserMdsNode<K, V> &rhs) const;
  virtual int64_t to_string(char * buf, const int64_t buf_len) const override;
  template <int N>
  void report_event_(const char (&event)[N],
                     const char *file = __builtin_FILE(),
                     const uint32_t line = __builtin_LINE(),
                     const char *function_name = __builtin_FUNCTION()) const;
  virtual int fill_virtual_info(MdsNodeInfoForVirtualTable &mds_node_indo) const override;
  template <int N>
  int fill_event_(observer::MdsEvent &mds_event, const char (&event)[N], char *buffer, const int64_t len) const;
  MdsNodeType get_node_type() const;
public:// do user action if there has define, in transaction phase point
  bool try_on_redo(const share::SCN &redo_scn) override;
  void on_redo_(const share::SCN &redo_scn);
  bool try_before_prepare() override;
  void before_prepare_();
  bool try_on_prepare(const share::SCN &prepare_version) override;
  void on_prepare_(const share::SCN &prepare_version);
  bool try_on_commit(const share::SCN &commit_version, const share::SCN &commit_scn) override;
  void on_commit_(const share::SCN &commit_version, const share::SCN &commit_scn);
  bool try_on_abort(const share::SCN &abort_scn) override;
  bool try_single_log_commit(const share::SCN &commit_version, const share::SCN &commit_scn) override;
public:// conditional compile
  template <typename DATA_TYPE = V, ENABLE_IF_NOT_HAS_ON_SET(DATA_TYPE)>
  void on_user_data_set_() {}
  template <typename DATA_TYPE = V, ENABLE_IF_HAS_ON_SET(DATA_TYPE)>
  void on_user_data_set_() { user_data_.on_set(); }
  template <typename DATA_TYPE = V, ENABLE_IF_NOT_HAS_ON_REDO(DATA_TYPE)>
  void on_user_data_redo_(const share::SCN redo_scn) {}
  template <typename DATA_TYPE = V, ENABLE_IF_HAS_ON_REDO(DATA_TYPE)>
  void on_user_data_redo_(const share::SCN redo_scn) { user_data_.on_redo(redo_scn); }
  template <typename DATA_TYPE = V, ENABLE_IF_NOT_HAS_ON_COMMIT(DATA_TYPE)>
  void on_user_data_commit_(const share::SCN, const share::SCN) {}
  template <typename DATA_TYPE = V, ENABLE_IF_HAS_ON_COMMIT(DATA_TYPE)>
  void on_user_data_commit_(const share::SCN commit_version, const share::SCN commit_scn) { user_data_.on_commit(commit_version, commit_scn); }
  template <typename DATA_TYPE = V, ENABLE_IF_NOT_HAS_ON_ABORT(DATA_TYPE)>
  void on_user_data_abort_(const share::SCN abort_scn) {}
  template <typename DATA_TYPE = V, ENABLE_IF_HAS_ON_ABORT(DATA_TYPE)>
  void on_user_data_abort_(const share::SCN abort_scn) { user_data_.on_abort(abort_scn); }
public:
  bool is_valid_scn_(const share::SCN &scn) const;
  bool has_valid_link_back_ptr_() const;
  MdsRowBase<K, V> *p_mds_row_;
  V user_data_;
};
}
}
}

#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_NODE_H_IPP
#define STORAGE_MULTI_DATA_SOURCE_MDS_NODE_H_IPP
#include "mds_node.ipp"
#endif

#endif
