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

#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_IMPL_IPP
#define STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_IMPL_IPP

#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "ob_clock_generator.h"
#include "share/ob_errno.h"
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/multi_data_source/mds_table_base.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"
#ifndef STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_IMPL_H_IPP
#define STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_IMPL_H_IPP
#include "mds_table_impl.h"
#endif

namespace oceanbase
{
namespace storage
{
namespace mds
{

template <typename MdsTableType>
struct InitHelper {
  InitHelper(MdsTableBase *p_mds_table) : p_mds_table_(p_mds_table) {}
  template <typename K, typename V>
  struct InnerHelper {
    void operator()(MdsUnit<K, V> &mds_unit, MdsTableBase *p_mds_table) {
      mds_unit.p_mds_table_ = p_mds_table;
      mds_unit.unit_id_ = TupleTypeIdx<MdsTableType, MdsUnit<K, V>>::value;
    }
  };
  template <typename V>
  struct InnerHelper<DummyKey, V> {// this is for single unit logic
    void operator()(MdsUnit<DummyKey, V> &mds_unit, MdsTableBase *p_mds_table) {
      mds_unit.p_mds_table_ = p_mds_table;
      mds_unit.unit_id_ = TupleTypeIdx<MdsTableType, MdsUnit<DummyKey, V>>::value;
    }
  };
  template <typename K, typename V>
  int operator()(MdsUnit<K, V> &mds_unit) {
    InnerHelper<K, V> inner_helper;
    inner_helper(mds_unit, p_mds_table_);
    return OB_SUCCESS;
  }
  MdsTableBase *p_mds_table_;
};
template <typename MdsTableType>
MdsTableImpl<MdsTableType>::MdsTableImpl() : MdsTableBase::MdsTableBase()
{
  InitHelper<MdsTableType> helper(this);
  unit_tuple_.for_each(helper);
}

template <typename MdsTableType>
MdsTableImpl<MdsTableType>::~MdsTableImpl() {
  int ret = OB_SUCCESS;
  if (!is_removed_from_t3m()) {
    if (OB_FAIL(unregister_from_mds_table_mgr())) {
      MDS_LOG(ERROR, "fail to unregister from mds table mgr", K(*this));
      ret = OB_SUCCESS;
    }
  }
  if (OB_FAIL(unregister_from_removed_recorder())) {
    MDS_LOG(ERROR, "fail to unregister from removed_recorder", K(*this));
  }
}

template <typename MdsTableImpl>
struct SetHelper {// find which unit to set at runtime
  SetHelper(MdsTableImpl &mds_table_impl,
            const uint8_t unit_id,
            void *key,
            void *data,
            bool is_rvalue,
            MdsCtx &ctx,
            const int64_t lock_timeout_us)
  : mds_table_impl_(mds_table_impl),
  current_idx_(0),
  unit_id_(unit_id),
  key_(key),
  data_(data),
  is_rvalue_(is_rvalue),
  ctx_(ctx),
  lock_timeout_us_(lock_timeout_us) {}
  template <typename K, typename V>
  struct InnerHelper {// member states of multi unit and single unit is not same,
                      // so need template partial specialization here,
                      // but function template partial specialization is not suppoted,
                      // so defined a inner class here
                      // this is for multi unit logic
    InnerHelper(SetHelper &helper) : helper_(helper) {}
    int operator()(MdsUnit<K, V> &mds_unit) {
      int ret = OB_SUCCESS;
      MdsRLockGuard lg(helper_.mds_table_impl_.lock_);// may create new mds_row, but locks all rows in on_flush phase
      if (helper_.is_rvalue_) {
        ret = mds_unit.set(&helper_.mds_table_impl_,
                           *(reinterpret_cast<K*>(helper_.key_)),
                           std::move(*(reinterpret_cast<V*>(helper_.data_))),
                           helper_.ctx_,
                           helper_.lock_timeout_us_);
      } else {
        ret = mds_unit.set(&helper_.mds_table_impl_,
                           *(reinterpret_cast<K*>(helper_.key_)),
                           *(reinterpret_cast<V*>(helper_.data_)),
                           helper_.ctx_,
                           helper_.lock_timeout_us_);
      }
      return ret;
    }
    SetHelper &helper_;
  };
  template <typename V>
  struct InnerHelper<DummyKey, V> {// this is for single unit logic
    InnerHelper(SetHelper &helper) : helper_(helper) {}
    int operator()(MdsUnit<DummyKey, V> &mds_unit) {
      int ret = OB_SUCCESS;
      if (helper_.is_rvalue_) {
        ret = mds_unit.set(&helper_.mds_table_impl_,
                           std::move(*(reinterpret_cast<V*>(helper_.data_))),
                           helper_.ctx_,
                           helper_.lock_timeout_us_);
      } else {
        ret = mds_unit.set(&helper_.mds_table_impl_,
                           *(reinterpret_cast<V*>(helper_.data_)),
                           helper_.ctx_,
                           helper_.lock_timeout_us_);
      }
      return ret;
    }
    SetHelper &helper_;
  };
  template <typename K, typename V>
  int operator()(MdsUnit<K, V> &mds_unit) {
    int ret = OB_SUCCESS;
    if (current_idx_++ == unit_id_) {
      InnerHelper<K, V> inner_helper(*this);
      if (OB_FAIL(inner_helper(mds_unit))) {
      } else {
        ret = OB_ITER_END;
      }
    }
    return ret;// if ret == OB_ITER_END or other err, interruptted
  }
  MdsTableImpl &mds_table_impl_;
  uint8_t current_idx_;
  const uint8_t unit_id_;
  void *key_;
  void *data_;
  bool is_rvalue_;
  MdsCtx &ctx_;
  const int64_t lock_timeout_us_;
};
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::set(int64_t unit_id,
                                    void *key,
                                    void *data,
                                    bool is_rvalue,
                                    MdsCtx &ctx,
                                    const int64_t lock_timeout_us) {
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);// point write should be fast
  if (MDS_FAIL(advance_state_to(State::WRITTING))) {
    MDS_LOG(ERROR, "mds table switch state failed", KR(ret), K(*this));
  } else {
    SetHelper<MdsTableImpl<MdsTableType>> helper(*this,
                                                 unit_id,
                                                 key,
                                                 data,
                                                 is_rvalue,
                                                 ctx,
                                                 lock_timeout_us);
    if (OB_ITER_END == (ret = unit_tuple_.for_each(helper))) {
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      MDS_LOG(WARN, "fail to set", KR(ret));
    } else {
      ret = OB_OBJ_TYPE_ERROR;
      MDS_LOG(WARN, "not found in tuple", KR(ret));
    }
  }
  return ret;
}

template <typename MdsTableImpl>
struct ReplayHelper {
  ReplayHelper(MdsTableImpl &mds_table_impl,
               int64_t unit_id,
               void *key,
               void *data,
               bool is_rvalue,
               MdsCtx &ctx,
               const share::SCN &scn)
  : mds_table_impl_(mds_table_impl),
  current_idx_(0),
  unit_id_(unit_id),
  key_(key),
  data_(data),
  is_rvalue_(is_rvalue),
  ctx_(ctx),
  scn_(scn) {}
  template <typename K, typename V>
  struct InnerHelper {
    InnerHelper(ReplayHelper &helper) : helper_(helper) {}
    int operator()(MdsUnit<K, V> &mds_unit) {
      int ret = OB_SUCCESS;
      // 1. Mutex with on_flush(replay is out-of-order, may affects calculate rec_scn phase)
      // 2. may create new mds_row, but locks all rows in on_flush phase
      MdsRLockGuard lg(helper_.mds_table_impl_.lock_);
      if (helper_.is_rvalue_) {
        ret = mds_unit.replay(&helper_.mds_table_impl_,
                              *(reinterpret_cast<K*>(helper_.key_)),
                              std::move(*(reinterpret_cast<V*>(helper_.data_))),
                              helper_.ctx_,
                              helper_.scn_);
      } else {
        ret = mds_unit.replay(&helper_.mds_table_impl_,
                              *(reinterpret_cast<K*>(helper_.key_)),
                              *(reinterpret_cast<V*>(helper_.data_)),
                              helper_.ctx_,
                              helper_.scn_);
      }
      return ret;
    }
    ReplayHelper &helper_;
  };
  template <typename V>
  struct InnerHelper<DummyKey, V> {
    InnerHelper(ReplayHelper &helper) : helper_(helper) {}
    int operator()(MdsUnit<DummyKey, V> &mds_unit) {
      int ret = OB_SUCCESS;
      MdsRLockGuard lg(helper_.mds_table_impl_.lock_);// Mutex with on_flush(replay is out-of-order, may affects calculate rec_scn phase)
      if (helper_.is_rvalue_) {
        ret = mds_unit.replay(&helper_.mds_table_impl_,
                              std::move(*(reinterpret_cast<V*>(helper_.data_))),
                              helper_.ctx_,
                              helper_.scn_);
      } else {
        ret = mds_unit.replay(&helper_.mds_table_impl_,
                              *(reinterpret_cast<V*>(helper_.data_)),
                              helper_.ctx_,
                              helper_.scn_);
      }
      return ret;
    }
    ReplayHelper &helper_;
  };
  template <typename K, typename V>
  int operator()(MdsUnit<K, V> &mds_unit) {
    int ret = OB_SUCCESS;
    if (current_idx_++ == unit_id_) {
      InnerHelper<K, V> inner_helper(*this);
      if (OB_FAIL(inner_helper(mds_unit))) {
      } else {
        ret = OB_ITER_END;
      }
    }
    return ret;// if ret == OB_ITER_END or other err, interruptted
  }
  MdsTableImpl &mds_table_impl_;
  uint8_t current_idx_;
  const uint8_t unit_id_;
  void *key_;
  void *data_;
  bool is_rvalue_;
  MdsCtx &ctx_;
  const share::SCN &scn_;
};
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::replay(int64_t unit_id,
                                       void *key,
                                       void *data,
                                       bool is_rvalue,
                                       MdsCtx &ctx,
                                       const share::SCN &scn) {
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);// point write should be fast
  if (MDS_FAIL(advance_state_to(State::WRITTING))) {
    MDS_LOG(ERROR, "mds table switch state failed", KR(ret), K(*this));
  } else {
    ReplayHelper<MdsTableImpl<MdsTableType>> helper(*this,
                                                    unit_id,
                                                    key,
                                                    data,
                                                    is_rvalue,
                                                    ctx,
                                                    scn);
    if (OB_ITER_END == (ret = unit_tuple_.for_each(helper))) {
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      MDS_LOG(WARN, "fail to set", KR(ret));
    } else {
      ret = OB_OBJ_TYPE_ERROR;
      MDS_LOG(WARN, "not found in tuple", KR(ret));
    }
  }
  return ret;
}

template <typename MdsTableImpl>
struct RemoveHelper {
  RemoveHelper(MdsTableImpl &mds_table_impl,
               int64_t unit_id,
               void *key,
               MdsCtx &ctx,
               const int64_t lock_timeout_us)
  : mds_table_impl_(mds_table_impl),
  current_idx_(0),
  unit_id_(unit_id),
  key_(key),
  ctx_(ctx),
  lock_timeout_us_(lock_timeout_us) {}
  template <typename K, typename V>
  struct InnerHelper {
    InnerHelper(RemoveHelper &helper) : helper_(helper) {}
    int operator()(MdsUnit<K, V> &mds_unit) {
      int ret = OB_SUCCESS;
      MdsRLockGuard lg(helper_.mds_table_impl_.lock_);// Mutex with on_flush(replay is out-of-order, may affects calculate rec_scn phase)
      ret = mds_unit.set(&helper_.mds_table_impl_,
                         *(reinterpret_cast<K*>(helper_.key_)),
                         V(),
                         helper_.ctx_,
                         helper_.lock_timeout_us_,
                         true/*is_for_remove*/);
      return ret;
    }
    RemoveHelper &helper_;
  };
  template <typename V>
  struct InnerHelper<DummyKey, V> {
    InnerHelper(RemoveHelper &helper) : helper_(helper) {}
    int operator()(MdsUnit<DummyKey, V> &mds_unit) {
      return OB_OBJ_TYPE_ERROR;
    }
    RemoveHelper &helper_;
  };
  template <typename K, typename V>
  int operator()(MdsUnit<K, V> &mds_unit) {
    int ret = OB_SUCCESS;
    if (current_idx_++ == unit_id_) {
      InnerHelper<K, V> inner_helper(*this);
      if (OB_FAIL(inner_helper(mds_unit))) {
      } else {
        ret = OB_ITER_END;
      }
    }
    return ret;// if ret == OB_ITER_END or other err, interruptted
  }
  MdsTableImpl &mds_table_impl_;
  uint8_t current_idx_;
  const uint8_t unit_id_;
  void *key_;
  MdsCtx &ctx_;
  const int64_t lock_timeout_us_;
};
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::remove(int64_t unit_id,
                                       void *key,
                                       MdsCtx &ctx,
                                       const int64_t lock_timeout_us) {
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);// point write should be fast
  if (MDS_FAIL(advance_state_to(State::WRITTING))) {
    MDS_LOG(ERROR, "mds table switch state failed", KR(ret), K(*this));
  } else {
    RemoveHelper<MdsTableImpl<MdsTableType>> helper(*this,
                                                    unit_id,
                                                    key,
                                                    ctx,
                                                    lock_timeout_us);
    if (OB_ITER_END == (ret = unit_tuple_.for_each(helper))) {
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      MDS_LOG(WARN, "fail to set", KR(ret));
    } else {
      ret = OB_OBJ_TYPE_ERROR;
      MDS_LOG(WARN, "not found in tuple", KR(ret));
    }
  }
  return ret;
}

template <typename MdsTableImpl>
struct ReplayRemoveHelper {
  ReplayRemoveHelper(MdsTableImpl &mds_table_impl,
                     int64_t unit_id,
                     void *key,
                     MdsCtx &ctx,
                     const share::SCN &scn)
  : mds_table_impl_(mds_table_impl),
  current_idx_(0),
  unit_id_(unit_id),
  key_(key),
  ctx_(ctx),
  scn_(scn) {}
  template <typename K, typename V>
  struct InnerHelper {
    InnerHelper(ReplayRemoveHelper &helper) : helper_(helper) {}
    int operator()(MdsUnit<K, V> &mds_unit) {
      int ret = OB_SUCCESS;
      MdsRLockGuard lg(helper_.mds_table_impl_.lock_);// Mutex with on_flush(replay is out-of-order, may affects calculate rec_scn phase)
      ret = mds_unit.replay(&helper_.mds_table_impl_,
                            *(reinterpret_cast<K*>(helper_.key_)),
                            V(),
                            helper_.ctx_,
                            helper_.scn_,
                            true/*is_for_remove*/);
      return ret;
    }
    ReplayRemoveHelper &helper_;
  };
  template <typename V>
  struct InnerHelper<DummyKey, V> {
    InnerHelper(ReplayRemoveHelper &helper) : helper_(helper) {}
    int operator()(MdsUnit<DummyKey, V> &mds_unit) {
      return OB_OBJ_TYPE_ERROR;
    }
    ReplayRemoveHelper &helper_;
  };
  template <typename K, typename V>
  int operator()(MdsUnit<K, V> &mds_unit) {
    int ret = OB_SUCCESS;
    if (current_idx_++ == unit_id_) {
      InnerHelper<K, V> inner_helper(*this);
      if (OB_FAIL(inner_helper(mds_unit))) {
      } else {
        ret = OB_ITER_END;
      }
    }
    return ret;// if ret == OB_ITER_END or other err, interruptted
  }
  MdsTableImpl &mds_table_impl_;
  uint8_t current_idx_;
  const uint8_t unit_id_;
  void *key_;
  MdsCtx &ctx_;
  const share::SCN &scn_;
};
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::replay_remove(int64_t unit_id,
                                              void *key,
                                              MdsCtx &ctx,
                                              const share::SCN &scn) {
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);// point write should be fast
  if (MDS_FAIL(advance_state_to(State::WRITTING))) {
    MDS_LOG(ERROR, "mds table switch state failed", KR(ret), K(*this));
  } else {
    ReplayRemoveHelper<MdsTableImpl<MdsTableType>> helper(*this,
                                                          unit_id,
                                                          key,
                                                          ctx,
                                                          scn);
    if (OB_ITER_END == (ret = unit_tuple_.for_each(helper))) {
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      MDS_LOG(WARN, "fail to set", KR(ret));
    } else {
      ret = OB_OBJ_TYPE_ERROR;
      MDS_LOG(WARN, "not found in tuple", KR(ret));
    }
  }
  return ret;
}

template <typename MdsTableImpl>
struct GetLatestHelper {
  GetLatestHelper(const MdsTableImpl &mds_table_impl,
                  int64_t unit_id,
                  void *key,
                  ObFunction<int(void *)> &op,
                  MdsWriter &writer,// FIXME(zk250686): should not exposed, will be removed later
                  TwoPhaseCommitState &trans_stat,// FIXME(zk250686): should not exposed, will be removed later
                  share::SCN &trans_version,// FIXME(zk250686): should not exposed, will be removed later
                  const int64_t read_seq)
  : mds_table_impl_(mds_table_impl),
  current_idx_(0),
  unit_id_(unit_id),
  key_(key),
  function_(op),
  writer_(writer),
  trans_stat_(trans_stat),
  trans_version_(trans_version),
  read_seq_(read_seq) {}
  struct ReadNodeOp {
    ReadNodeOp(GetLatestHelper &helper)
    : helper_(helper) {}
    template <typename K, typename V>
    int operator()(const UserMdsNode<K, V> &node) {
      helper_.writer_.writer_type_ = node.status_.get_writer_type();
      helper_.writer_.writer_id_ = node.writer_id_;
      helper_.trans_stat_ = node.status_.get_state();
      helper_.trans_version_ = node.trans_version_;
      return helper_.function_(reinterpret_cast<void *>(&const_cast<V&>(node.user_data_)));
    }
    GetLatestHelper &helper_;
  };
  template <typename K, typename V>
  struct InnerHelper {
    InnerHelper(GetLatestHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<K, V> &mds_unit) {
      return mds_unit.get_latest(*reinterpret_cast<K*>(helper_.key_),
                                 ReadNodeOp(helper_));
    }
    GetLatestHelper &helper_;
  };
  template <typename V>
  struct InnerHelper<DummyKey, V> {
    InnerHelper(GetLatestHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<DummyKey, V> &mds_unit) {
      return mds_unit.get_latest(ReadNodeOp(helper_));
    }
    GetLatestHelper &helper_;
  };
  template <typename K, typename V>
  int operator()(const MdsUnit<K, V> &mds_unit) {
    int ret = OB_SUCCESS;
    if (current_idx_++ == unit_id_) {
      InnerHelper<K, V> inner_helper(*this);
      if (OB_FAIL(inner_helper(mds_unit))) {
      } else {
        ret = OB_ITER_END;
      }
    }
    return ret;// if ret == OB_ITER_END or other err, interruptted
  }
  const MdsTableImpl &mds_table_impl_;
  uint8_t current_idx_;
  const uint8_t unit_id_;
  void *key_;
  ObFunction<int(void *)> &function_;
  MdsWriter &writer_;// FIXME(zk250686): should not exposed, will be removed later
  TwoPhaseCommitState &trans_stat_;// FIXME(zk250686): should not exposed, will be removed later
  share::SCN &trans_version_;// FIXME(zk250686): should not exposed, will be removed later
  const int64_t read_seq_;
};
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::get_latest(int64_t unit_id,
                                           void *key,
                                           ObFunction<int(void *)> &op,
                                           MdsWriter &writer,// FIXME(zk250686): should not exposed, will be removed later
                                           TwoPhaseCommitState &trans_stat,// FIXME(zk250686): should not exposed, will be removed later
                                           share::SCN &trans_version,// FIXME(zk250686): should not exposed, will be removed later
                                           const int64_t read_seq) const {
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);// point select should be fast
  if (MDS_FAIL(advance_state_to(State::WRITTING))) {
    MDS_LOG(ERROR, "mds table switch state failed", KR(ret), K(*this));
  } else {
    GetLatestHelper<MdsTableImpl<MdsTableType>> helper(*this,
                                                       unit_id,
                                                       key,
                                                       op,
                                                       writer,
                                                       trans_stat,
                                                       trans_version,
                                                       read_seq);
    if (OB_ITER_END == (ret = unit_tuple_.for_each(helper))) {
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret)) {
        MDS_LOG(WARN, "fail to get_latest", KR(ret));
      }
    } else {
      ret = OB_OBJ_TYPE_ERROR;
      MDS_LOG(WARN, "not found in tuple", KR(ret));
    }
  }
  return ret;
}

// only normal mds table support this method, and only for transfer
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::get_tablet_status_node(ObFunction<int(void *)> &op, const int64_t read_seq) const
{
  return OB_NOT_SUPPORTED;
}
struct GetTabletSetusNodeOpWrapper {
  GetTabletSetusNodeOpWrapper(ObFunction<int(void *)> &op) : op_(op) {}
  int operator()(const UserMdsNode<DummyKey, ObTabletCreateDeleteMdsUserData> &node) {
    return op_((void *)&node);
  }
  ObFunction<int(void *)> &op_;
};
template <>
inline int MdsTableImpl<NormalMdsTable>::get_tablet_status_node(ObFunction<int(void *)> &op, const int64_t read_seq) const
{
  return const_cast<NormalMdsTable &>(unit_tuple_).element<MdsUnit<DummyKey, ObTabletCreateDeleteMdsUserData>>().
         get_latest(GetTabletSetusNodeOpWrapper(op));
}

template <typename MdsTableImpl>
struct GetLatestCommittedHelper {
  GetLatestCommittedHelper(const MdsTableImpl &mds_table_impl,
                           int64_t unit_id,
                           void *key,
                           ObFunction<int(void *)> &op)
  : mds_table_impl_(mds_table_impl),
  current_idx_(0),
  unit_id_(unit_id),
  key_(key),
  function_(op) {}
  struct ReadNodeOp {
    ReadNodeOp(GetLatestCommittedHelper &helper)
    : helper_(helper) {}
    template <typename K, typename V>
    int operator()(const UserMdsNode<K, V> &node) { return helper_.function_(reinterpret_cast<void *>(&const_cast<V&>(node.user_data_))); }
    GetLatestCommittedHelper &helper_;
  };
  template <typename K, typename V>
  struct InnerHelper {
    InnerHelper(GetLatestCommittedHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<K, V> &mds_unit) {
      return mds_unit.get_latest_committed(*reinterpret_cast<K*>(helper_.key_), ReadNodeOp(helper_));
    }
    GetLatestCommittedHelper &helper_;
  };
  template <typename V>
  struct InnerHelper<DummyKey, V> {
    InnerHelper(GetLatestCommittedHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<DummyKey, V> &mds_unit) {
      return mds_unit.get_latest_committed(ReadNodeOp(helper_));
    }
    GetLatestCommittedHelper &helper_;
  };
  template <typename K, typename V>
  int operator()(const MdsUnit<K, V> &mds_unit) {
    int ret = OB_SUCCESS;
    if (current_idx_++ == unit_id_) {
      InnerHelper<K, V> inner_helper(*this);
      if (OB_FAIL(inner_helper(mds_unit))) {
      } else {
        ret = OB_ITER_END;
      }
    }
    return ret;// if ret == OB_ITER_END or other err, interruptted
  }
  const MdsTableImpl &mds_table_impl_;
  uint8_t current_idx_;
  const uint8_t unit_id_;
  void *key_;
  ObFunction<int(void *)> &function_;
};
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::get_latest_committed(int64_t unit_id,
                                                     void *key,
                                                     ObFunction<int(void *)> &op) const
{
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);// point select should be fast
  if (MDS_FAIL(advance_state_to(State::WRITTING))) {
    MDS_LOG(ERROR, "mds table switch state failed", KR(ret), K(*this));
  } else {
    GetLatestCommittedHelper<MdsTableImpl<MdsTableType>> helper(*this,
                                                                unit_id,
                                                                key,
                                                                op);
    if (OB_ITER_END == (ret = unit_tuple_.for_each(helper))) {
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret)) {
        MDS_LOG(WARN, "fail to get_latest_committed", KR(ret));
      }
    } else {
      ret = OB_OBJ_TYPE_ERROR;
      MDS_LOG(WARN, "not found in tuple", KR(ret));
    }
  }
  return ret;
}

template <typename MdsTableImpl>
struct GetSnapshotHelper {
  GetSnapshotHelper(const MdsTableImpl &mds_table_impl,
                    int64_t unit_id,
                    void *key,
                    ObFunction<int(void *)> &op,
                    const share::SCN &snapshot,
                    const int64_t timeout_us)
  : mds_table_impl_(mds_table_impl),
  current_idx_(0),
  unit_id_(unit_id),
  key_(key),
  function_(op),
  snapshot_(snapshot),
  timeout_us_(timeout_us) {}
  struct ReadNodeOp {
    ReadNodeOp(GetSnapshotHelper &helper)
    : helper_(helper) {}
    template <typename K, typename V>
    int operator()(const UserMdsNode<K, V> &node) { return helper_.function_(reinterpret_cast<void *>(&const_cast<V&>(node.user_data_))); }
    GetSnapshotHelper &helper_;
  };
  template <typename K, typename V>
  struct InnerHelper {
    InnerHelper(GetSnapshotHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<K, V> &mds_unit) {
      return mds_unit.get_snapshot(*reinterpret_cast<K*>(helper_.key_),
                                   ReadNodeOp(helper_),
                                   helper_.snapshot_,
                                   helper_.timeout_us_);
    }
    GetSnapshotHelper &helper_;
  };
  template <typename V>
  struct InnerHelper<DummyKey, V> {
    InnerHelper(GetSnapshotHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<DummyKey, V> &mds_unit) {
      return mds_unit.get_snapshot(ReadNodeOp(helper_),
                                   helper_.snapshot_,
                                   helper_.timeout_us_);
    }
    GetSnapshotHelper &helper_;
  };
  template <typename K, typename V>
  int operator()(const MdsUnit<K, V> &mds_unit) {
    int ret = OB_SUCCESS;
    if (current_idx_++ == unit_id_) {
      InnerHelper<K, V> inner_helper(*this);
      if (OB_FAIL(inner_helper(mds_unit))) {
      } else {
        ret = OB_ITER_END;
      }
    }
    return ret;// if ret == OB_ITER_END or other err, interruptted
  }
  const MdsTableImpl &mds_table_impl_;
  uint8_t current_idx_;
  const uint8_t unit_id_;
  void *key_;
  ObFunction<int(void *)> &function_;
  const share::SCN &snapshot_;
  const int64_t timeout_us_;
};
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::get_snapshot(int64_t unit_id,
                                             void *key,
                                             ObFunction<int(void *)> &op,
                                             const share::SCN &snapshot,
                                             const int64_t timeout_us) const {
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);// point select should be fast
  if (MDS_FAIL(advance_state_to(State::WRITTING))) {
    MDS_LOG(ERROR, "mds table switch state failed", KR(ret), K(*this));
  } else {
    GetSnapshotHelper<MdsTableImpl<MdsTableType>> helper(*this,
                                                         unit_id,
                                                         key,
                                                         op,
                                                         snapshot,
                                                         timeout_us);
    if (OB_ITER_END == (ret = unit_tuple_.for_each(helper))) {
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret)) {
        MDS_LOG(WARN, "fail to get_snapshot", KR(ret));
      }
    } else {
      ret = OB_OBJ_TYPE_ERROR;
      MDS_LOG(WARN, "not found in tuple", KR(ret));
    }
  }
  return ret;
}

template <typename MdsTableImpl>
struct GetByWriterHelper {
  GetByWriterHelper(const MdsTableImpl &mds_table_impl,
                    int64_t unit_id,
                    void *key,
                    ObFunction<int(void *)> &op,
                    const MdsWriter &writer,
                    const share::SCN &snapshot,
                    const transaction::ObTxSEQ read_seq,
                    const int64_t timeout_us)
  : mds_table_impl_(mds_table_impl),
  current_idx_(0),
  unit_id_(unit_id),
  key_(key),
  function_(op),
  writer_(writer),
  snapshot_(snapshot),
  read_seq_(read_seq),
  timeout_us_(timeout_us) {}
  struct ReadNodeOp {
    ReadNodeOp(GetByWriterHelper &helper)
    : helper_(helper) {}
    template <typename K, typename V>
    int operator()(const UserMdsNode<K, V> &node) { return helper_.function_(reinterpret_cast<void *>(&const_cast<V&>(node.user_data_))); }
    GetByWriterHelper &helper_;
  };
  template <typename K, typename V>
  struct InnerHelper {
    InnerHelper(GetByWriterHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<K, V> &mds_unit) {
      return mds_unit.get_by_writer(*reinterpret_cast<K*>(helper_.key_),
                                    ReadNodeOp(helper_),
                                    helper_.writer_,
                                    helper_.snapshot_,
                                    helper_.read_seq_,
                                    helper_.timeout_us_);
    }
    GetByWriterHelper &helper_;
  };
  template <typename V>
  struct InnerHelper<DummyKey, V> {
    InnerHelper(GetByWriterHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<DummyKey, V> &mds_unit) {
      return mds_unit.get_by_writer(ReadNodeOp(helper_),
                                    helper_.writer_,
                                    helper_.snapshot_,
                                    helper_.read_seq_,
                                    helper_.timeout_us_);
    }
    GetByWriterHelper &helper_;
  };
  template <typename K, typename V>
  int operator()(const MdsUnit<K, V> &mds_unit) {
    int ret = OB_SUCCESS;
    if (current_idx_++ == unit_id_) {
      InnerHelper<K, V> inner_helper(*this);
      if (OB_FAIL(inner_helper(mds_unit))) {
      } else {
        ret = OB_ITER_END;
      }
    }
    return ret;// if ret == OB_ITER_END or other err, interruptted
  }
  const MdsTableImpl &mds_table_impl_;
  uint8_t current_idx_;
  const uint8_t unit_id_;
  void *key_;
  ObFunction<int(void *)> &function_;
  const MdsWriter &writer_;
  const share::SCN &snapshot_;
  const transaction::ObTxSEQ read_seq_;
  const int64_t timeout_us_;
};
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::get_by_writer(int64_t unit_id,
                                              void *key,
                                              ObFunction<int(void *)> &op,
                                              const MdsWriter &writer,
                                              const share::SCN &snapshot,
                                              const transaction::ObTxSEQ read_seq,
                                              const int64_t timeout_us) const {
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);// point select should be fast
  if (MDS_FAIL(advance_state_to(State::WRITTING))) {
    MDS_LOG(ERROR, "mds table switch state failed", KR(ret), K(*this));
  } else {
    GetByWriterHelper<MdsTableImpl<MdsTableType>> helper(*this,
                                                         unit_id,
                                                         key,
                                                         op,
                                                         writer,
                                                         snapshot,
                                                         read_seq,
                                                         timeout_us);
    if (OB_ITER_END == (ret = unit_tuple_.for_each(helper))) {
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret)) {
        MDS_LOG(WARN, "fail to get_by_writer", KR(ret));
      }
    } else {
      ret = OB_OBJ_TYPE_ERROR;
      MDS_LOG(WARN, "not found in tuple", KR(ret));
    }
  }
  return ret;
}

template <typename MdsTableImpl>
struct IsLockedByOthersHelper {
  IsLockedByOthersHelper(const MdsTableImpl &mds_table_impl,
                         int64_t unit_id,
                         void *key,
                         bool &is_locked,
                         const MdsWriter &self)
  : mds_table_impl_(mds_table_impl),
  current_idx_(0),
  unit_id_(unit_id),
  key_(key),
  is_locked_(is_locked),
  self_(self) {}
  struct ReadNodeOp {
    ReadNodeOp(IsLockedByOthersHelper &helper)
    : helper_(helper) {}
    template <typename K, typename V>
    int operator()(const UserMdsNode<K, V> &node) {
      MDS_ASSERT(node.status_.union_.field_.state_ != TwoPhaseCommitState::ON_ABORT);
      if (node.status_.union_.field_.state_ == TwoPhaseCommitState::ON_COMMIT) {// no lock on decided node
        helper_.is_locked_ = false;
      } else if (node.status_.union_.field_.writer_type_ == helper_.self_.writer_type_ &&
                  node.writer_id_ == helper_.self_.writer_id_) {// lock by myself
        helper_.is_locked_ = false;
      } else {
        helper_.is_locked_ = true;
      }
      return OB_SUCCESS;
    }
    IsLockedByOthersHelper &helper_;
  };
  template <typename K, typename V>
  struct InnerHelper {
    InnerHelper(IsLockedByOthersHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<K, V> &mds_unit) {
      return mds_unit.get_latest(*reinterpret_cast<K*>(helper_.key_), ReadNodeOp(helper_));
    }
    IsLockedByOthersHelper &helper_;
  };
  template <typename V>
  struct InnerHelper<DummyKey, V> {
    InnerHelper(IsLockedByOthersHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<DummyKey, V> &mds_unit) { return mds_unit.get_latest(ReadNodeOp(helper_)); }
    IsLockedByOthersHelper &helper_;
  };
  template <typename K, typename V>
  int operator()(const MdsUnit<K, V> &mds_unit) {
    int ret = OB_SUCCESS;
    if (current_idx_++ == unit_id_) {
      InnerHelper<K, V> inner_helper(*this);
      if (OB_FAIL(inner_helper(mds_unit))) {
      } else {
        ret = OB_ITER_END;
      }
    }
    return ret;// if ret == OB_ITER_END or other err, interruptted
  }
  const MdsTableImpl &mds_table_impl_;
  uint8_t current_idx_;
  const uint8_t unit_id_;
  void *key_;
  bool &is_locked_;
  const MdsWriter &self_;
};
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::is_locked_by_others(int64_t unit_id,
                                                    void *key,
                                                    bool &is_locked,
                                                    const MdsWriter &self) const {
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);// point select should be fast
  if (MDS_FAIL(advance_state_to(State::WRITTING))) {
    MDS_LOG(ERROR, "mds table switch state failed", KR(ret), K(*this));
  } else {
    IsLockedByOthersHelper<MdsTableImpl<MdsTableType>> helper(*this,
                                                              unit_id,
                                                              key,
                                                              is_locked,
                                                              self);
    if (OB_ITER_END == (ret = unit_tuple_.for_each(helper))) {
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret)) {
        MDS_LOG(WARN, "fail to call is_locked_by_others", KR(ret));
      }
    } else {
      ret = OB_OBJ_TYPE_ERROR;
      MDS_LOG(WARN, "not found in tuple", KR(ret));
    }
  }
  return ret;
}

template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::scan_all_nodes_to_dump(ObFunction<int(const MdsDumpKV&)> &for_each_op,
                                                       const int64_t mds_construct_sequence,
                                                       const bool for_flush,
                                                       const ScanRowOrder scan_row_order,
                                                       const ScanNodeOrder scan_node_order) const {
  int ret = OB_SUCCESS;
  MDS_TG(100_ms);// scan could be slow
  if (MDS_FAIL(advance_state_to(State::WRITTING))) {
    MDS_LOG(WARN, "mds table switch state failed", KR(ret), K(*this));
  } else if (MDS_FAIL(const_cast<MdsTableImpl<MdsTableType>*>(this)->scan_all_nodes_to_dump(for_each_op,
                                                                                            mds_construct_sequence,
                                                                                            for_flush,
                                                                                            scan_row_order,
                                                                                            scan_node_order))) {
    MDS_LOG(WARN, "mds table scan_all_nodes_to_dump failed", KR(ret), K(*this));
  }
  return ret;
}

struct RecalculateFlushScnCauseOnlySuppportDumpCommittedNodeOP// FIXME: delete this after support dump uncommitted node
{
  struct ForEachOp {
    ForEachOp(RecalculateFlushScnCauseOnlySuppportDumpCommittedNodeOP &op) : op_(op) {}
    template <typename K, typename V>
    bool operator()(const UserMdsNode<K, V> &mds_node) {
      bool need_break = false;
      if (op_.do_flush_scn_ >= mds_node.redo_scn_ && op_.do_flush_scn_ < mds_node.end_scn_) {
        need_break = true;
        MDS_LOG(DEBUG, "try decline do_flush_scn", K_(op_.do_flush_scn), K(mds_node));
        op_.do_flush_scn_ = std::min(op_.do_flush_scn_, share::SCN::minus(mds_node.redo_scn_, 1));
      } else if (!mds_node.redo_scn_.is_max() && op_.do_flush_scn_ < mds_node.redo_scn_) {
        need_break = true;
      }
      return need_break;
    }
    RecalculateFlushScnCauseOnlySuppportDumpCommittedNodeOP &op_;
  };
  RecalculateFlushScnCauseOnlySuppportDumpCommittedNodeOP(share::SCN &do_flush_scn) : do_flush_scn_(do_flush_scn) {}
  template <typename K, typename V>
  int operator()(const MdsRow<K, V> &row) {
    int ret = OB_SUCCESS;
    // already add lock on unit, but no lock on row
    MDS_TG(5_ms);
    MdsRLockGuard lg(row.lock_);// lock on row
    CLICK();
    row.sorted_list_.for_each_node_from_tail_to_head_until_true(ForEachOp(*this));
    return ret;
  }
  share::SCN &do_flush_scn_;
};
struct CountUnDumpdedNodesBelowDoFlushScn// To filter unnecessary flush operation
{
  struct ForEachOp {
    ForEachOp(CountUnDumpdedNodesBelowDoFlushScn &op) : op_(op) {}
    template <typename K, typename V>
    bool operator()(const UserMdsNode<K, V> &mds_node) {
      bool need_break = false;
      if (check_node_scn_beflow_flush(mds_node, op_.do_flush_scn_)) {
        if (!mds_node.is_dumped_()) {
          op_.total_cnt_++;
        }
      } else {
        need_break = true;
      }
      return need_break;
    }
    CountUnDumpdedNodesBelowDoFlushScn &op_;
  };
  CountUnDumpdedNodesBelowDoFlushScn(int64_t &total_cnt, share::SCN &do_flush_scn)
  : total_cnt_(total_cnt), do_flush_scn_(do_flush_scn) { total_cnt_ = 0; }
  template <typename K, typename V>
  int operator()(const MdsRow<K, V> &row) {
    int ret = OB_SUCCESS;
    // already add lock on unit, but no lock on row
    MDS_TG(5_ms);
    MdsRLockGuard lg(row.lock_);// lock on row
    CLICK();
    row.sorted_list_.for_each_node_from_tail_to_head_until_true(ForEachOp(*this));
    return ret;
  }
  int64_t &total_cnt_;
  const share::SCN &do_flush_scn_;
};
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::calculate_flush_scn_and_need_dumped_nodes_cnt_(share::SCN &do_flush_scn, int64_t &need_dumped_nodes_cnt)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(do_flush_scn), K(need_dumped_nodes_cnt)
  MDS_TG(100_ms);
  int ret = OB_SUCCESS;
  share::SCN last_calculated_do_flush_scn = do_flush_scn;
  RecalculateFlushScnCauseOnlySuppportDumpCommittedNodeOP op1(do_flush_scn);// recalculate flush scn
  do {
    last_calculated_do_flush_scn = do_flush_scn;
    if (MDS_FAIL(for_each_scan_row(FowEachRowAction::CALCUALTE_FLUSH_SCN, op1))) {
      MDS_LOG_FLUSH(WARN, "for each to calculate flush scn failed");
    }
  } while (OB_SUCC(ret) && last_calculated_do_flush_scn != do_flush_scn);// flush scn must not cross any node's [redo_scn, end_scn)
  CountUnDumpdedNodesBelowDoFlushScn op2(need_dumped_nodes_cnt, do_flush_scn);// count nodes need dump
  if (MDS_FAIL(ret)) {
  } else if (MDS_FAIL(for_each_scan_row(FowEachRowAction::COUNT_NODES_BEFLOW_FLUSH_SCN, op2))) {
    MDS_LOG_FLUSH(WARN, "for each to count undumped nodes failed");
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::flush(share::SCN need_advanced_rec_scn_lower_limit, share::SCN max_decided_scn)
{// if rec_scn below this limit, need generate dag and try hard to advance it
  #define PRINT_WRAPPER KR(ret), K(*this), K(need_advanced_rec_scn_lower_limit), K(do_flush_scn), K(max_decided_scn), K(undump_node_cnt)
  MDS_TG(100_ms);
  int ret = OB_SUCCESS;
  share::SCN do_flush_scn = max_decided_scn;// this scn is defined for calculation
  int64_t undump_node_cnt = 0;
  MdsWLockGuard lg(lock_);
  if (!mgr_handle_.get_mds_table_mgr()->can_flush()) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(10LL * 1000LL * 1000LL/*10 seconds*/)) {
      MDS_LOG_FLUSH(INFO, "mds flush is disabled");
    }
  } else if (OB_UNLIKELY(!need_advanced_rec_scn_lower_limit.is_valid() || !max_decided_scn.is_valid() || max_decided_scn.is_max())) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG_FLUSH(WARN, "invalid recycle scn");
  } else if (get_rec_scn().is_max()) {
    MDS_LOG_FLUSH(TRACE, "no need do flush cause rec_scn is MAX already");
  } else if (need_advanced_rec_scn_lower_limit < get_rec_scn()) {// no need dump this mds table to advance rec_scn
    MDS_LOG_FLUSH(TRACE, "no need do flush need_advanced_rec_scn_lower_limit less than rec_scn");
  } else if (MDS_FAIL(calculate_flush_scn_and_need_dumped_nodes_cnt_(do_flush_scn, undump_node_cnt))) {
    MDS_LOG_FLUSH(WARN, "fail to call calculate_flush_scn_and_need_dumped_nodes_cnt_");
  } else if (do_flush_scn < get_rec_scn()) {// no need dump this mds table to advance rec_scn
    MDS_LOG_FLUSH(TRACE, "no need do flush need_advanced_rec_scn_lower_limit less than rec_scn");
  } else {
#ifndef UNITTEST_DEBUG
    if (MDS_FAIL(merge(construct_sequence_, do_flush_scn))) {
      if (OB_EAGAIN == ret) {
        if (REACH_TIME_INTERVAL(100_ms)) {
          MDS_LOG_FLUSH(WARN, "failed to commit merge mds table dag cause already exist");
          ret = OB_SUCCESS;
        } else if (OB_SIZE_OVERFLOW == ret) {// throw out
          MDS_LOG_FLUSH(WARN, "failed to commit merge mds table dag cause queue already full");
        }
      } else {
        MDS_LOG_FLUSH(WARN, "failed to commit merge mds table dag");
      }
    } else {
      if (flushing_scn_.is_valid()) {
        MDS_LOG_FLUSH(WARN, "flushing_scn is valid scn, that means last committed dag not scheduled, and dropped for some unknown reason");
      }
      if (max_aborted_scn_.atomic_load() >= do_flush_scn) {
        MDS_LOG_FLUSH(INFO, "max_aborted_scn is not less than do_flush_scn, that means this time dump maybe just for advance rec_scn");
      }
      flushing_scn_ = do_flush_scn;
      report_flush_event_("DO_FLUSH", flushing_scn_);
      // if commit dag success, there is no guarantee that dag will be executed finally
      debug_info_.last_flush_ts_ = ObClockGenerator::getClock();// record commit dag ts to debug
    }
#else
    flushing_scn_ = do_flush_scn;
#endif
  }
  MDS_LOG_FLUSH(DEBUG, "call flush mds_table");
  return ret;
  #undef PRINT_WRAPPER
}

struct CalculateRecScnOp
{
  CalculateRecScnOp(const share::SCN on_flush_scn)
  : on_flush_scn_(on_flush_scn),
  rec_scn_(share::SCN::max_scn()) {}
  ~CalculateRecScnOp() {
    for (int64_t idx = 0; idx < lock_array_.count(); ++idx) {
      ((MdsLock*)lock_array_[idx])->unlock();
    }
  }
  struct ForEachOp {
    ForEachOp(CalculateRecScnOp &op, share::SCN &closest_bigger_scn) : op_(op), closest_bigger_scn_(closest_bigger_scn) {}
    template <typename K, typename V>
    bool operator()(const UserMdsNode<K, V> &mds_node) {
      bool need_break = false;
      MDS_TG(1_ms);
      if (check_node_scn_beflow_flush(mds_node, op_.on_flush_scn_)) {
        if (!mds_node.is_dumped_()) {
          const_cast<UserMdsNode<K, V> &>(mds_node).set_dumped_();// mark it dumped, so no need dump again
          MDS_LOG_RET(INFO, OB_SUCCESS, "mark mds node dumped", K(mds_node));
        }
        if (mds_node.redo_scn_ <= op_.on_flush_scn_ && mds_node.end_scn_ > op_.on_flush_scn_) {
          MDS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "should not go here cause not support dump uncomitted node yet", K(mds_node));// FIXME: remove this log after support dump uncomitted node
          closest_bigger_scn_ = mds_node.end_scn_;
          need_break = true;// break for_each operation
        }
      } else {
        closest_bigger_scn_ = mds_node.redo_scn_;
        need_break = true;// break for_each operation
      }
      return need_break;
    }
    CalculateRecScnOp &op_;
    share::SCN &closest_bigger_scn_;
  };
  template <typename K, typename V>
  int operator()(const MdsRow<K, V> &row) {
    int ret = OB_SUCCESS;
    MDS_TG(5_ms);
    // add row's lock until calculation done
    if (MDS_FAIL(lock_array_.push_back(nullptr))) {
      MDS_LOG(WARN, "fail to add lock placeholder on low");
    } else {
      row.lock_.wrlock();
      lock_array_[lock_array_.count() - 1] = &row.lock_;
      CLICK();
      share::SCN closest_bigger_scn = share::SCN::max_scn();
      // must scan from tail to head, cause flush may happend in replay phase(replay operation is out of order)
      row.sorted_list_.for_each_node_from_tail_to_head_until_true(ForEachOp(*this, closest_bigger_scn));
      rec_scn_ = std::min(rec_scn_, closest_bigger_scn);
    }
    return ret;
  }
  const share::SCN on_flush_scn_;
  share::SCN rec_scn_;
  common::ObSEArray<void*, 10> lock_array_;
};
template <typename MdsTableType>
void MdsTableImpl<MdsTableType>::on_flush(const share::SCN &flush_scn, const int flush_ret)
{
  MDS_TG(10_ms);
  MdsWLockGuard lg(lock_);
  CLICK();
  on_flush_(flush_scn, flush_ret);
}
template <typename MdsTableType>
void MdsTableImpl<MdsTableType>::calculate_rec_scn_and_advance_to_it_(share::SCN on_flush_scn)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(on_flush_scn)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  bool need_retry = false;
  do {
    need_retry = false;
    CalculateRecScnOp op(on_flush_scn);
    if (MDS_FAIL(for_each_scan_row(FowEachRowAction::CALCULATE_REC_SCN, op))) {// lock all rows failed, retry until lock all rows success
      need_retry = true;
      MDS_LOG_FLUSH(WARN, "fail to do on flush");// record row lock guard may failed, cause lock guard array may meet extended failed cause memory not enough, but retry will make it success
    } else {
      share::SCN new_rec_scn = op.rec_scn_;// next nearby scn
      if (max_aborted_scn_.atomic_load() > on_flush_scn) {// can only be advanced to flush scn cause must consider about aborted scn between flush scn and max_aborted_scn_
        MDS_LOG_FLUSH(INFO, "max_aborted_scn_on_mds_table is greater than flush scn, rec_scn can only be advanced to flush_scn");
        new_rec_scn = share::SCN::scn_inc(on_flush_scn);
      }
      try_advance_rec_scn(new_rec_scn);
      report_on_flush_event_("ON_FLUSH", on_flush_scn);
    }
  } while (need_retry && !FALSE_IT(PAUSE()));// here will release all rows lock
  #undef PRINT_WRAPPER
}
template <typename MdsTableType>
void MdsTableImpl<MdsTableType>::on_flush_(const share::SCN &flush_scn, const int flush_ret)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(flush_scn)
  int ret = flush_ret;
  MDS_TG(10_ms);
  share::SCN max_consequent_callbacked_scn = share::SCN::max_scn();
  if (MDS_FAIL(ret)) {
    MDS_LOG_FLUSH(WARN, "flush failed");
    flushing_scn_.reset();
  } else if (!flushing_scn_.is_valid() || flushing_scn_ != flush_scn) {
    if (rec_scn_ == share::SCN::max_scn()) {
      MDS_LOG_FLUSH(WARN, "maybe meet concurrent reset mds table");
    } else {
      MDS_LOG_FLUSH(WARN, "flush version mismatch!");
    }
  } else {
    flushing_scn_.reset();
    (void) calculate_rec_scn_and_advance_to_it_(flush_scn);
  }
  #undef PRINT_WRAPPER
}

template <typename MdsTableType>
template <typename DUMP_OP,
          typename std::enable_if<OB_TRAIT_IS_FUNCTION_LIKE(DUMP_OP,
                                                            int(const MdsDumpKV &)), bool>::type>
int MdsTableImpl<MdsTableType>::scan_all_nodes_to_dump(DUMP_OP &&for_each_op,
                                                       const int64_t mds_construct_sequence,
                                                       /*false is for transfer to bring mds data from old tablet to new*/
                                                       const bool for_flush,
                                                       const ScanRowOrder scan_row_order,
                                                       const ScanNodeOrder scan_node_order) {
  #define PRINT_WRAPPER KR(ret), K(mds_construct_sequence), K(*this)
  MDS_TG(100_ms);
  int ret = OB_SUCCESS;
  share::SCN flushing_version;
  MdsRLockGuard lg(lock_);
  CLICK();
  if (mds_construct_sequence != 0) {
    if (mds_construct_sequence != ATOMIC_LOAD(&construct_sequence_)) {
      ret = OB_VERSION_NOT_MATCH;
      MDS_LOG_FLUSH(WARN, "construct sequence mismatch");
    }
  }
  if (OB_SUCC(ret)) {
    if (for_flush) {
      if (!flushing_scn_.is_valid()) {
        ret = OB_NO_NEED_MERGE;
        MDS_LOG_FLUSH(WARN, "not in flushing process");
      } else {
        flushing_version = flushing_scn_;
      }
    } else {
      flushing_version = share::SCN::max_scn();
    }
    if (OB_SUCC(ret)) {
      if (MDS_FAIL(for_each_to_dump_node_(std::forward<DUMP_OP>(for_each_op),
                                          flushing_version,
                                          for_flush,
                                          scan_row_order,
                                          scan_node_order))) {
        MDS_LOG_FLUSH(WARN, "for each node to dump failed");
      } else {
        MDS_LOG_FLUSH(TRACE, "for each node to dump success");
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename MdsTableType>
template <typename SCAN_OP>
int MdsTableImpl<MdsTableType>::for_each_scan_row(FowEachRowAction action_type, SCAN_OP &&op)
{// add lock on unit
  ForEachUnitScanRowHelper<SCAN_OP> for_each_op(action_type, op);
  return unit_tuple_.for_each(for_each_op);
}

struct RecycleNodeOp
{
  template<typename K, typename V>
  struct ForEachOp {
    ForEachOp(RecycleNodeOp &op, const MdsRow<K, V> &row) : op_(op), row_(row) {}
    bool operator()(const UserMdsNode<K, V> &mds_node) {
      int ret = OB_SUCCESS;
      bool need_break = false;
      bool need_delete = false;
      MDS_TG(1_ms);
      {
        CLICK();
        MDS_ASSERT(!mds_node.is_aborted_());// should not see aborted node cause cause it is deleted immediatly
        if (mds_node.is_committed_()) {
          if (mds_node.end_scn_ == share::SCN::max_scn()) {// must has an associated valid end LOG to commit/abort
            ret = OB_ERR_UNEXPECTED;
            MDS_LOG(ERROR, "meet a valid decided mds node with max scn", K(mds_node));
          } else if (mds_node.end_scn_ <= op_.recycle_scn_ && mds_node.is_dumped_()) {// safe to destroy mds node
            need_delete = true;
          } else {
            need_break = true;// break for each operation
          }
        }
      }
      if (need_delete) {
        UserMdsNode<K, V> &cast_node = const_cast<UserMdsNode<K, V> &>(mds_node);
        const_cast<MdsRow<K, V> &>(row_).sorted_list_.del((ListNodeBase*)(ListNode<UserMdsNode<K, V>>*)(&cast_node));
        MdsFactory::destroy(&cast_node);
      }
      return need_break || OB_SUCCESS != ret;
    }
    RecycleNodeOp &op_;
    const MdsRow<K, V> &row_;
  };
  RecycleNodeOp(const share::SCN recycle_scn) : recycle_scn_(recycle_scn) {}
  template <typename K, typename V>
  int operator()(const MdsRow<K, V> &row) {
    int ret = OB_SUCCESS;
    // already add lock on unit, but no lock on row
    MDS_TG(5_ms);
    MdsWLockGuard lg(row.lock_);// lock on row
    CLICK();
    // must scan from tail to head, cause recycle may happened in replay phase(replay operation is out of order)
    // skip nodes no need to see(see class MdsRowSkipAndDestroyOp to check definitions)
    row.sorted_list_.for_each_node_from_tail_to_head_until_true(ForEachOp<K, V>(*this, row));
    return ret;
  }
  const share::SCN recycle_scn_;
};
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::try_recycle(const share::SCN recycle_scn)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(recycle_scn), K(do_inner_recycle_scn)
  int ret = OB_SUCCESS;
  MDS_TG(100_ms);
  if (!recycle_scn.is_min()) {
    MdsWLockGuard lg(lock_);
    share::SCN do_inner_recycle_scn = std::min(get_rec_scn(), recycle_scn);
    if (!recycle_scn.is_valid() || recycle_scn.is_max()) {
      ret = OB_INVALID_ARGUMENT;
      MDS_LOG_GC(ERROR, "invalid argument");
    } else if (last_inner_recycled_scn_.is_valid() && last_inner_recycled_scn_ >= do_inner_recycle_scn) {
      // do nothing
    } else {
      RecycleNodeOp op(do_inner_recycle_scn);
      if (OB_FAIL(for_each_scan_row(FowEachRowAction::RECYCLE, op))) {
        MDS_LOG_GC(ERROR, "fail to do recycle");
      } else {
        last_inner_recycled_scn_ = do_inner_recycle_scn;
        report_recycle_event_(do_inner_recycle_scn);

        MDS_LOG_GC(DEBUG, "succeed to recycle mds node");
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

struct ForcelyReleaseNodesRedoScnBelowOp
{
  ForcelyReleaseNodesRedoScnBelowOp(const char *reason, share::SCN redo_below_scn)
  : reason_(reason),
  redo_below_scn_(redo_below_scn) {}
  template <typename K, typename V>
  int operator()(const MdsRow<K, V> &row) {
    #define PRINT_WRAPPER K(cast_node), K_(reason)
    int ret = OB_SUCCESS;
    // already add lock on unit, but no lock on row
    MDS_TG(5_ms);
    MdsWLockGuard lg(row.lock_);// lock on row
    CLICK();
    row.sorted_list_.for_each_node_from_tail_to_head_until_true(
      [this, &row](const UserMdsNode<K, V> &mds_node) {
        UserMdsNode<K, V> &cast_node = const_cast<UserMdsNode<K, V> &>(mds_node);
        if (mds_node.redo_scn_ <= redo_below_scn_) {
          if (!cast_node.is_committed_()) {
            MDS_LOG_GC(INFO, "release uncommitted node");
          } else {
            // below_scn_ comes from mds sstable mds_ckpt_scn, it must not cross any node's [redo, end) scn range
            // before support dump uncommitted nodes, all nodes be scanned must satisfy this rule
            MDS_ASSERT(mds_node.end_scn_ <= redo_below_scn_);
          }
          const_cast<MdsRow<K, V> &>(row).sorted_list_.del((ListNodeBase*)(ListNode<UserMdsNode<K, V>>*)(&cast_node));
          MdsFactory::destroy(&cast_node);
        } else if (redo_below_scn_.is_max()) {// just for defence
          ob_abort();
        }
        return false;
      }
    );
    return ret;
    #undef PRINT_WRAPPER
  }
  const char *reason_;
  share::SCN redo_below_scn_;
};
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::forcely_remove_nodes(const char *reason, share::SCN redo_scn_limit)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(reason), K(redo_scn_limit)
  int ret = OB_SUCCESS;
  MDS_TG(100_ms);
  MdsWLockGuard lg(lock_);
  ForcelyReleaseNodesRedoScnBelowOp op(reason, redo_scn_limit);
  if (OB_FAIL(for_each_scan_row(FowEachRowAction::REMOVE, op))) {
    MDS_LOG_GC(ERROR, "fail to do remove");
  } else {
    debug_info_.last_remove_ts_ = ObClockGenerator::getClock();
    flushing_scn_.reset();
    calculate_rec_scn_and_advance_to_it_(redo_scn_limit);
    if (redo_scn_limit.is_max()) {// reset operation
      last_inner_recycled_scn_ = share::SCN::min_scn();
    } else {// remove operation
      last_inner_recycled_scn_ = std::max(last_inner_recycled_scn_, redo_scn_limit);
    }
    MDS_LOG_GC(INFO, "forcely release mds nodes below");
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename MdsTableType>
inline int MdsTableImpl<MdsTableType>::operate(const ObFunction<int(MdsTableBase &)> &operation)
{
  #define PRINT_WRAPPER KR(ret), K(*this)
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  MdsWLockGuard lg(lock_);
  if (OB_FAIL(operation(*this))) {
    MDS_LOG_NONE(INFO, "fail to apply upper layer operation");
  }
  return ret;
  #undef PRINT_WRAPPER
}

}
}
}
#endif