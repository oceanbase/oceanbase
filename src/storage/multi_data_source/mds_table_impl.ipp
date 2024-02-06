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
#include "storage/multi_data_source/mds_table_base.h"
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
#define MDS_ELEMENT(K, V) const_cast<MdsTableType &>(unit_tuple_).\
                          template element<MdsUnit<K, typename std::decay<V>::type>>()

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
  MDS_LOG(INFO, "mds table destructed", K(*this));
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
      if (helper_.is_rvalue_) {
        ret = helper_.mds_table_impl_.set(*(reinterpret_cast<K*>(helper_.key_)),
                                          std::move(*(reinterpret_cast<V*>(helper_.data_))),
                                          helper_.ctx_,
                                          helper_.lock_timeout_us_);
      } else {
        ret = helper_.mds_table_impl_.set(*(reinterpret_cast<K*>(helper_.key_)),
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
        ret = helper_.mds_table_impl_.set(std::move(*(reinterpret_cast<V*>(helper_.data_))),
                                          helper_.ctx_,
                                          helper_.lock_timeout_us_);
      } else {
        ret = helper_.mds_table_impl_.set(*(reinterpret_cast<V*>(helper_.data_)),
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
      if (helper_.is_rvalue_) {
        ret = helper_.mds_table_impl_.replay(*(reinterpret_cast<K*>(helper_.key_)),
                                             std::move(*(reinterpret_cast<V*>(helper_.data_))),
                                             helper_.ctx_,
                                             helper_.scn_);
      } else {
        ret = helper_.mds_table_impl_.replay(*(reinterpret_cast<K*>(helper_.key_)),
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
      if (helper_.is_rvalue_) {
        ret = helper_.mds_table_impl_.replay(std::move(*(reinterpret_cast<V*>(helper_.data_))),
                                             helper_.ctx_,
                                             helper_.scn_);
      } else {
        ret = helper_.mds_table_impl_.replay(*(reinterpret_cast<V*>(helper_.data_)),
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
      ret = helper_.mds_table_impl_.template remove<K, V>(*(reinterpret_cast<K*>(helper_.key_)),
                                                          helper_.ctx_,
                                                          helper_.lock_timeout_us_);
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
      ret = helper_.mds_table_impl_.template
                    replay_remove<K, V>(*(reinterpret_cast<K*>(helper_.key_)),
                                        helper_.ctx_,
                                        helper_.scn_);
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
                  bool &is_committed,
                  const int64_t read_seq)
  : mds_table_impl_(mds_table_impl),
  current_idx_(0),
  unit_id_(unit_id),
  key_(key),
  function_(op),
  is_committed_(is_committed),
  read_seq_(read_seq) {}
  template <typename K, typename V>
  struct InnerHelper {
    InnerHelper(GetLatestHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<K, V> &mds_unit) {
      return helper_.mds_table_impl_.template get_latest<K, V>(
        *reinterpret_cast<K*>(helper_.key_),
        [this](const V &data) {
          return helper_.function_(reinterpret_cast<void *>(&const_cast<V&>(data)));
        },
        helper_.is_committed_,
        helper_.read_seq_
      );
    }
    GetLatestHelper &helper_;
  };
  template <typename V>
  struct InnerHelper<DummyKey, V> {
    InnerHelper(GetLatestHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<DummyKey, V> &mds_unit) {
      return helper_.mds_table_impl_.template get_latest<V>(
        [this](const V &data) -> int {
          return helper_.function_(reinterpret_cast<void *>(&const_cast<V&>(data)));
        },
        helper_.is_committed_,
        helper_.read_seq_
      );
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
  bool &is_committed_;
  const int64_t read_seq_;
};
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::get_latest(int64_t unit_id,
                                           void *key,
                                           ObFunction<int(void *)> &op,
                                           bool &is_committed,
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
                                                       is_committed,
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

template <typename MdsTableImpl>
struct GetSnapshotHelper {
  GetSnapshotHelper(const MdsTableImpl &mds_table_impl,
                    int64_t unit_id,
                    void *key,
                    ObFunction<int(void *)> &op,
                    const share::SCN &snapshot,
                    const int64_t read_seq,
                    const int64_t timeout_us)
  : mds_table_impl_(mds_table_impl),
  current_idx_(0),
  unit_id_(unit_id),
  key_(key),
  function_(op),
  snapshot_(snapshot),
  read_seq_(read_seq),
  timeout_us_(timeout_us) {}
  template <typename K, typename V>
  struct InnerHelper {
    InnerHelper(GetSnapshotHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<K, V> &mds_unit) {
      return helper_.mds_table_impl_.template get_snapshot<K, V>(
        *reinterpret_cast<K*>(helper_.key_),
        [this](const V &data) {
          return helper_.function_(reinterpret_cast<void *>(&const_cast<V&>(data)));
        },
        helper_.snapshot_,
        helper_.read_seq_,
        helper_.timeout_us_
      );
    }
    GetSnapshotHelper &helper_;
  };
  template <typename V>
  struct InnerHelper<DummyKey, V> {
    InnerHelper(GetSnapshotHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<DummyKey, V> &mds_unit) {
      return helper_.mds_table_impl_.template get_snapshot<V>(
        [this](const V &data) {
          return helper_.function_(reinterpret_cast<void *>(&const_cast<V&>(data)));
        },
        helper_.snapshot_,
        helper_.read_seq_,
        helper_.timeout_us_
      );
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
  const int64_t read_seq_;
  const int64_t timeout_us_;
};
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::get_snapshot(int64_t unit_id,
                                             void *key,
                                             ObFunction<int(void *)> &op,
                                             const share::SCN &snapshot,
                                             const int64_t read_seq,
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
                                                         read_seq,
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
                    const int64_t read_seq,
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
  template <typename K, typename V>
  struct InnerHelper {
    InnerHelper(GetByWriterHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<K, V> &mds_unit) {
      return helper_.mds_table_impl_.template get_by_writer<K, V>(
        *reinterpret_cast<K*>(helper_.key_),
        [this](const V &data) {
          return helper_.function_(reinterpret_cast<void *>(&const_cast<V&>(data)));
        },
        helper_.writer_,
        helper_.snapshot_,
        helper_.read_seq_,
        helper_.timeout_us_
      );
    }
    GetByWriterHelper &helper_;
  };
  template <typename V>
  struct InnerHelper<DummyKey, V> {
    InnerHelper(GetByWriterHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<DummyKey, V> &mds_unit) {
      return helper_.mds_table_impl_.template get_by_writer<V>(
        [this](const V &data) {
          return helper_.function_(reinterpret_cast<void *>(&const_cast<V&>(data)));
        },
        helper_.writer_,
        helper_.snapshot_,
        helper_.read_seq_,
        helper_.timeout_us_
      );
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
  const int64_t read_seq_;
  const int64_t timeout_us_;
};
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::get_by_writer(int64_t unit_id,
                                              void *key,
                                              ObFunction<int(void *)> &op,
                                              const MdsWriter &writer,
                                              const share::SCN &snapshot,
                                              const int64_t read_seq,
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
  template <typename K, typename V>
  struct InnerHelper {
    InnerHelper(IsLockedByOthersHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<K, V> &mds_unit) {
      return helper_.mds_table_impl_.template is_locked_by_others<K, V>(
        *reinterpret_cast<K*>(helper_.key_),
        helper_.is_locked_,
        helper_.self_
      );
    }
    IsLockedByOthersHelper &helper_;
  };
  template <typename V>
  struct InnerHelper<DummyKey, V> {
    InnerHelper(IsLockedByOthersHelper &helper) : helper_(helper) {}
    int operator()(const MdsUnit<DummyKey, V> &mds_unit) {
      return helper_.mds_table_impl_.template is_locked_by_others<V>(
        helper_.is_locked_,
        helper_.self_
      );
    }
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
      MDS_LOG(WARN, "fail to call is_locked_by_others", KR(ret));
    } else {
      ret = OB_OBJ_TYPE_ERROR;
      MDS_LOG(WARN, "not found in tuple", KR(ret));
    }
  }
  return ret;
}

template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::for_each_unit_from_small_key_to_big_from_old_node_to_new_to_dump(
                                ObFunction<int(const MdsDumpKV&)> &for_each_op,
                                const int64_t mds_construct_sequence,
                                const bool for_flush) const {
  int ret = OB_SUCCESS;
  MDS_TG(100_ms);// scan could be slow
  if (MDS_FAIL(advance_state_to(State::WRITTING))) {
    MDS_LOG(WARN, "mds table switch state failed", KR(ret), K(*this));
  } else if (MDS_FAIL(const_cast<MdsTableImpl<MdsTableType>*>(this)->
                      for_each_unit_from_small_key_to_big_from_old_node_to_new_to_dump(
    [for_each_op](const MdsDumpKV& data) {
      return for_each_op(data);
    }, mds_construct_sequence, for_flush
  ))) {
    MDS_LOG(WARN, "mds table switch state failed", KR(ret), K(*this));
  }
  return ret;
}

struct RecalculateFlushScnCauseOnlySuppportDumpCommittedNodeOP// FIXME: delete this after support dump uncommitted node
{
  RecalculateFlushScnCauseOnlySuppportDumpCommittedNodeOP(share::SCN &do_flush_scn) : do_flush_scn_(do_flush_scn) {}
  template <typename K, typename V>
  int operator()(const MdsRow<K, V> &row) {
    int ret = OB_SUCCESS;
    // already add lock on unit, but no lock on row
    MDS_TG(5_ms);
    MdsRLockGuard lg(row.lock_);// lock on row
    CLICK();
    row.sorted_list_.for_each_node_from_tail_to_head_until_true(
      [this](const UserMdsNode<K, V> &mds_node) {
        bool need_break = false;
        if (do_flush_scn_ >= mds_node.redo_scn_ && do_flush_scn_ < mds_node.end_scn_) {
          need_break = true;
          MDS_LOG(DEBUG, "try decline do_flush_scn", K_(do_flush_scn), K(mds_node));
          do_flush_scn_ = std::min(do_flush_scn_, share::SCN::minus(mds_node.redo_scn_, 1));
        } else if (!mds_node.redo_scn_.is_max() && do_flush_scn_ < mds_node.redo_scn_) {
          need_break = true;
        }
        return need_break;
      }
    );
    return ret;
  }
  share::SCN &do_flush_scn_;
};
struct CountUnDumpdedNodesBelowDoFlushScn// To filter unnecessary flush operation
{
  CountUnDumpdedNodesBelowDoFlushScn(int64_t &total_cnt, share::SCN &do_flush_scn)
  : total_cnt_(total_cnt), do_flush_scn_(do_flush_scn) { total_cnt_ = 0; }
  template <typename K, typename V>
  int operator()(const MdsRow<K, V> &row) {
    int ret = OB_SUCCESS;
    // already add lock on unit, but no lock on row
    MDS_TG(5_ms);
    MdsRLockGuard lg(row.lock_);// lock on row
    CLICK();
    row.sorted_list_.for_each_node_from_tail_to_head_until_true(
      [this](const UserMdsNode<K, V> &mds_node) {
        bool need_break = false;
        if (check_node_scn_beflow_flush(mds_node, do_flush_scn_)) {
          if (!mds_node.is_dumped_()) {
            total_cnt_++;
          }
        } else {
          need_break = true;
        }
        return need_break;
      }
    );
    return ret;
  }
  int64_t &total_cnt_;
  const share::SCN &do_flush_scn_;
};
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::calculate_flush_scn_and_need_dumped_nodes_cnt_(share::SCN need_advanced_rec_scn_lower_limit,
                                                                               share::SCN &do_flush_scn,
                                                                               int64_t &need_dumped_nodes_cnt)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(need_advanced_rec_scn_lower_limit), K(do_flush_scn), \
          K(need_dumped_nodes_cnt)
  MDS_TG(100_ms);
  int ret = OB_SUCCESS;
  share::SCN calculated_flush_scn;
  share::SCN ls_max_consequent_callbacked_scn = share::SCN::max_scn();
#ifndef UNITTEST_DEBUG
    if (MDS_FAIL(get_ls_max_consequent_callbacked_scn_(ls_max_consequent_callbacked_scn))) {
      MDS_LOG_FLUSH(WARN, "fail to get ls_max_consequent_callbacked_scn");
    } else if (ls_max_consequent_callbacked_scn.is_max()) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_FLUSH(ERROR, "invalid ls max consequent callbacked scn");
    } else {
      do_flush_scn = ls_max_consequent_callbacked_scn;
    }
#else
    do_flush_scn = need_advanced_rec_scn_lower_limit;
#endif
    if (OB_SUCC(ret)) {
      RecalculateFlushScnCauseOnlySuppportDumpCommittedNodeOP op1(do_flush_scn);// recalculate flush scn
      CountUnDumpdedNodesBelowDoFlushScn op2(need_dumped_nodes_cnt, do_flush_scn);// count nodes need dump
      if (MDS_FAIL(for_each_scan_row(FowEachRowAction::CALCUALTE_FLUSH_SCN, op1))) {
        MDS_LOG_FLUSH(WARN, "for each to calculate flush scn failed");
      } else if (MDS_FAIL(for_each_scan_row(FowEachRowAction::COUNT_NODES_BEFLOW_FLUSH_SCN, op2))) {
        MDS_LOG_FLUSH(WARN, "for each to count undumped nodes failed");
      }
    }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::flush(share::SCN need_advanced_rec_scn_lower_limit)
{// if rec_scn below this limit, need generate dag and try hard to advance it
  #define PRINT_WRAPPER KR(ret), K(*this), K(need_advanced_rec_scn_lower_limit), K(do_flush_scn), K(undump_node_cnt)
  MDS_TG(100_ms);
  int ret = OB_SUCCESS;
  share::SCN do_flush_scn;// this scn is defined for calculation
  int64_t undump_node_cnt = 0;
  MdsWLockGuard lg(lock_);
  if (!need_advanced_rec_scn_lower_limit.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG_FLUSH(WARN, "invalid recycle scn");
  } else if (get_rec_scn().is_max()) {
    MDS_LOG_FLUSH(TRACE, "no need do flush cause rec_scn is MAX already");
  } else if (need_advanced_rec_scn_lower_limit < get_rec_scn()) {// no need dump this mds table to advance rec_scn
    MDS_LOG_FLUSH(TRACE, "no need do flush need_advanced_rec_scn_lower_limit less than rec_scn");
  } else if (MDS_FAIL(calculate_flush_scn_and_need_dumped_nodes_cnt_(need_advanced_rec_scn_lower_limit,
                                                                     do_flush_scn,
                                                                     undump_node_cnt))) {
    MDS_LOG_FLUSH(WARN, "fail to call calculate_flush_scn_and_need_dumped_nodes_cnt_");
  } else if (undump_node_cnt == 0) {// no need do flush actually
    // mds_ckpt_scn on tablet won't be advanced,
    // replay will cost more time after restart process, but saved cpu and io for dump dag
    MDS_LOG_FLUSH(INFO, "no undump nodes below do flush scn, directly advance rec_scn");
    flushing_scn_ = do_flush_scn;// will be resetted in on_flush_()
    on_flush_(do_flush_scn, OB_SUCCESS);
  } else {
#ifndef UNITTEST_DEBUG
    if (MDS_FAIL(merge(construct_sequence_, do_flush_scn))) {
      if (OB_EAGAIN == ret || OB_SIZE_OVERFLOW == ret) {
        if (REACH_TIME_INTERVAL(100_ms)) {
          MDS_LOG_FLUSH(WARN, "failed to commit merge mds table dag cause already exist or queue already full");
          ret = OB_SUCCESS;
        }
      } else {
        MDS_LOG_FLUSH(WARN, "failed to commit merge mds table dag");
      }
    } else {
      if (flushing_scn_.is_valid()) {
        MDS_LOG_FLUSH(WARN, "flushing_scn is valid scn, that means last committed dag not scheduled, and dropped for some unknown reason");
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
      row.sorted_list_.for_each_node_from_tail_to_head_until_true(
        [this, &ret, &closest_bigger_scn](const UserMdsNode<K, V> &mds_node) {
          bool need_break = false;
          MDS_TG(1_ms);
          if (check_node_scn_beflow_flush(mds_node, on_flush_scn_)) {
            if (!mds_node.is_dumped_()) {
              const_cast<UserMdsNode<K, V> &>(mds_node).set_dumped_();// mark it dumped, so no need dump again
              MDS_LOG(INFO, "mark mds node dumped", K(mds_node));
            }
            if (mds_node.redo_scn_ <= on_flush_scn_ && mds_node.end_scn_ > on_flush_scn_) {
              MDS_LOG(ERROR, "should not go here cause not support dump uncomitted node yet", K(mds_node));// FIXME: remove this log after support dump uncomitted node
              closest_bigger_scn = mds_node.end_scn_;
              need_break = true;// break for_each operation
            }
          } else {
            closest_bigger_scn = mds_node.redo_scn_;
            need_break = true;// break for_each operation
          }
          return need_break || OB_FAIL(ret);
        }
      );
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
      MDS_LOG_FLUSH(ERROR, "flush version mismatch!");
    }
  } else {
    flushing_scn_.reset();
    bool need_retry = false;
    do {
      need_retry = false;
      CalculateRecScnOp op(flush_scn);
      if (MDS_FAIL(for_each_scan_row(FowEachRowAction::CALCULATE_REC_SCN, op))) {// lock all rows failed, retry until lock all rows success
        need_retry = true;
        MDS_LOG_FLUSH(WARN, "fail to do on flush");// record row lock guard may failed, cause lock guard array may meet extended failed cause memory not enough, but retry will make it success
      } else {
        share::SCN new_rec_scn = op.rec_scn_;
        try_advance_rec_scn(new_rec_scn);
        report_on_flush_event_("ON_FLUSH", flush_scn);
      }
    } while (need_retry && !FALSE_IT(PAUSE()));// here will release all rows lock
  }
  #undef PRINT_WRAPPER
}

template <typename MdsTableType>
template <typename DUMP_OP,
          typename std::enable_if<OB_TRAIT_IS_FUNCTION_LIKE(DUMP_OP,
                                                            int(const MdsDumpKV &)), bool>::type>
int MdsTableImpl<MdsTableType>::
    for_each_unit_from_small_key_to_big_from_old_node_to_new_to_dump(DUMP_OP &&for_each_op,
      const int64_t mds_construct_sequence,
      const bool for_flush/*false is for transfer to bring mds data from old tablet to new*/) {
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
        ret = OB_ERR_UNEXPECTED;
        MDS_LOG_FLUSH(WARN, "not in flushing process");
      } else {
        flushing_version = flushing_scn_;
      }
    } else {
      flushing_version = share::SCN::max_scn();
    }
    if (OB_SUCC(ret)) {
      if (MDS_FAIL(for_each_to_dump_node_(std::forward<DUMP_OP>(for_each_op), flushing_version, for_flush))) {
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
template <typename T>
int MdsTableImpl<MdsTableType>::set(T &&data, MdsCtx &ctx, const int64_t lock_timeout_us)
{
  uint8_t unit_id = TupleTypeIdx<MdsTableType, MdsUnit<DummyKey, T>>::value;
  return MDS_ELEMENT(DummyKey, T).set(this,
                                      std::forward<T>(data),
                                      ctx,
                                      lock_timeout_us);
}

template <typename MdsTableType>
template <typename T>
int MdsTableImpl<MdsTableType>::replay(T &&data, MdsCtx &ctx, const share::SCN &scn)
{
  MdsRLockGuard lg(lock_);// Mutex with on_flush(replay is out-of-order, may affects calculate rec_scn phase)
  uint8_t unit_id = TupleTypeIdx<MdsTableType, MdsUnit<DummyKey, T>>::value;
  return MDS_ELEMENT(DummyKey, T).replay(this, std::forward<T>(data), ctx, scn);
}

template <typename MdsTableType>
template <typename T,
          typename OP,
          typename std::enable_if<OB_TRAIT_IS_FUNCTION_LIKE(OP, int(const T&)), bool>::type>
int MdsTableImpl<MdsTableType>::get_latest(OP &&read_op,
                                           bool &is_committed,
                                           const int64_t read_seq) const
{
  auto read_op_wrapper = [&read_op, &is_committed](const UserMdsNode<DummyKey, T> &node) -> int {
    is_committed = node.is_committed_();
    return read_op(node.user_data_);
  };
  return MDS_ELEMENT(DummyKey, T).get_latest(read_op_wrapper, read_seq);
}

template <typename MdsTableType>
template <typename T,
          typename OP,
          typename std::enable_if<OB_TRAIT_IS_FUNCTION_LIKE(OP, int(const T&)), bool>::type>
int MdsTableImpl<MdsTableType>::get_snapshot(OP &&read_op,
                                             const share::SCN snapshot,
                                             const int64_t read_seq, const int64_t timeout_us) const
{
  auto read_op_wrapper = [&read_op](const UserMdsNode<DummyKey, T> &node) -> int {
    return read_op(node.user_data_);
  };
  return MDS_ELEMENT(DummyKey, T).get_snapshot(read_op_wrapper,
                                                snapshot,
                                                read_seq,
                                                timeout_us);
}

template <typename MdsTableType>
template <typename T,
          typename OP,
          typename std::enable_if<OB_TRAIT_IS_FUNCTION_LIKE(OP, int(const T&)), bool>::type>
int MdsTableImpl<MdsTableType>::get_by_writer(OP &&read_op,
                                              const MdsWriter &writer,
                                              const share::SCN snapshot,
                                              const int64_t read_seq,
                                              const int64_t timeout_us) const
{
  auto read_op_wrapper = [&read_op](const UserMdsNode<DummyKey, T> &node) -> int {
    return read_op(node.user_data_);
  };
  return MDS_ELEMENT(DummyKey, T).get_by_writer(read_op_wrapper,
                                                writer,
                                                snapshot,
                                                read_seq,
                                                timeout_us);
}

template <typename MdsTableType>
template <typename T>
int MdsTableImpl<MdsTableType>::is_locked_by_others(bool &is_locked, const MdsWriter &self) const
{
  auto read_op_wrapper = [&is_locked, &self](const UserMdsNode<DummyKey, T> &node) -> int {
    MDS_ASSERT(node.status_.union_.field_.state_ != TwoPhaseCommitState::ON_ABORT);
    if (node.status_.union_.field_.state_ == TwoPhaseCommitState::ON_COMMIT) {// no lock on decided node
      is_locked = false;
    } else if (node.status_.union_.field_.writer_type_ == self.writer_type_ &&
                node.writer_id_ == self.writer_id_) {// lock by myself
      is_locked = false;
    } else {
      is_locked = true;
    }
    return OB_SUCCESS;
  };
  return MDS_ELEMENT(DummyKey, T).get_latest(read_op_wrapper, 0);
}

template <typename MdsTableType>
template <typename Key, typename Value>
int MdsTableImpl<MdsTableType>::set(const Key &key,
                                    Value &&data,
                                    MdsCtx &ctx,
                                    const int64_t lock_timeout_us)
{
  MdsRLockGuard lg(lock_);// Mutex with on_flush(this operation may create new mds_row, but locks all rows in on_flush phase)
  uint8_t unit_id = TupleTypeIdx<MdsTableType, MdsUnit<Key, Value>>::value;
  return MDS_ELEMENT(Key, Value).set(this, key, std::forward<Value>(data), ctx, lock_timeout_us);
}

template <typename MdsTableType>
template <typename Key, typename Value>
int MdsTableImpl<MdsTableType>::replay(const Key &key,
                                       Value &&data,
                                       MdsCtx &ctx,
                                       const share::SCN &scn)
{
  MdsRLockGuard lg(lock_);// Mutex with on_flush(replay is out-of-order, may affects calculate rec_scn phase)
  uint8_t unit_id = TupleTypeIdx<MdsTableType, MdsUnit<Key, Value>>::value;
  return MDS_ELEMENT(Key, Value).replay(this, key, std::forward<Value>(data), ctx, scn);
}

template <typename MdsTableType>
template <typename Key, typename Value>
int MdsTableImpl<MdsTableType>::remove(const Key &key,
                                       MdsCtx &ctx,
                                       const int64_t lock_timeout_us)
{
  MdsRLockGuard lg(lock_);// Mutex with on_flush(this operation may create new mds_row, but locks all rows in on_flush phase)
  uint8_t unit_id = TupleTypeIdx<MdsTableType, MdsUnit<Key, Value>>::value;
  return MDS_ELEMENT(Key, Value).set(this, key, Value(), ctx, lock_timeout_us, true);
}

template <typename MdsTableType>
template <typename Key, typename Value>
int MdsTableImpl<MdsTableType>::replay_remove(const Key &key,
                                              MdsCtx &ctx,
                                              const share::SCN &scn)
{
  MdsRLockGuard lg(lock_);// Mutex with on_flush(replay is out-of-order, may affects calculate rec_scn phase)
  uint8_t unit_id = TupleTypeIdx<MdsTableType, MdsUnit<Key, Value>>::value;
  return MDS_ELEMENT(Key, Value).replay(this, key, Value(), ctx, scn, true);
}

template <typename MdsTableType>
template <typename Key, typename Value, typename OP>
int MdsTableImpl<MdsTableType>::get_latest(const Key &key,
                                           OP &&read_op,
                                           bool &is_committed,
                                           const int64_t read_seq) const
{
  auto read_op_wrapper = [&read_op, &is_committed](const UserMdsNode<Key, Value> &node) -> int {
    is_committed = node.is_committed_();
    return read_op(node.user_data_);
  };
  return MDS_ELEMENT(Key, Value).get_latest(key, read_op_wrapper, read_seq);
}

template <typename MdsTableType>
template <typename Key, typename Value, typename OP>
int MdsTableImpl<MdsTableType>::get_snapshot(const Key &key,
                                             OP &&read_op,
                                             const share::SCN snapshot,
                                             const int64_t read_seq,
                                             const int64_t timeout_us) const
{
  auto read_op_wrapper = [&read_op](const UserMdsNode<Key, Value> &node) -> int {
    return read_op(node.user_data_);
  };
  return MDS_ELEMENT(Key, Value).get_snapshot(key,
                                              read_op_wrapper,
                                              snapshot,
                                              read_seq,
                                              timeout_us);
}

template <typename MdsTableType>
template <typename Key, typename Value, typename OP>
int MdsTableImpl<MdsTableType>::get_by_writer(const Key &key,
                                              OP &&read_op,
                                              const MdsWriter &writer,
                                              const share::SCN snapshot,
                                              const int64_t read_seq,
                                              const int64_t timeout_us) const
{
  auto read_op_wrapper = [&read_op](const UserMdsNode<Key, Value> &node) -> int {
    return read_op(node.user_data_);
  };
  return MDS_ELEMENT(Key, Value).get_by_writer(key,
                                               read_op_wrapper,
                                               writer,
                                               snapshot,
                                               read_seq,
                                               timeout_us);
}

template <typename MdsTableType>
template <typename Key, typename Value>
int MdsTableImpl<MdsTableType>::is_locked_by_others(const Key &key,
                                                    bool &is_locked,
                                                    const MdsWriter &self) const
{
  auto read_op_wrapper = [&is_locked, &self](const UserMdsNode<Key, Value> &node) -> int {
    MDS_ASSERT(node.status_.union_.field_.state_ != TwoPhaseCommitState::ON_ABORT);
    if (node.status_.union_.field_.state_ == TwoPhaseCommitState::ON_COMMIT) {
      is_locked = false;
    } else if (node.status_.union_.field_.writer_type_ == self.writer_type_ &&
                node.writer_id_ == self.writer_id_) {// lock by myself
      is_locked = false;
    } else {
      is_locked = true;
    }
    return OB_SUCCESS;
  };
  return MDS_ELEMENT(Key, Value).get_latest(key, read_op_wrapper, 0);
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
    row.sorted_list_.for_each_node_from_tail_to_head_until_true(
      [&ret, this, &row](const UserMdsNode<K, V> &mds_node) {
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
            } else if (mds_node.end_scn_ <= recycle_scn_ && mds_node.is_dumped_()) {// safe to destroy mds node
              need_delete = true;
            } else {
              need_break = true;// break for each operation
            }
          }
        }
        if (need_delete) {
          UserMdsNode<K, V> &cast_node = const_cast<UserMdsNode<K, V> &>(mds_node);
          const_cast<MdsRow<K, V> &>(row).sorted_list_.del((ListNodeBase*)(ListNode<UserMdsNode<K, V>>*)(&cast_node));
          MdsFactory::destroy(&cast_node);
        }
        return need_break || OB_SUCCESS != ret;
      }
    );
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
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

struct ForcelyReleaseAllNodeOp
{
  ForcelyReleaseAllNodeOp(const char *reason) : reason_(reason) {}
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
        if (!cast_node.is_committed_()) {
          MDS_LOG_GC(INFO, "release uncommitted node");
        }
        const_cast<MdsRow<K, V> &>(row).sorted_list_.del((ListNodeBase*)(ListNode<UserMdsNode<K, V>>*)(&cast_node));
        MdsFactory::destroy(&cast_node);
        return false;
      }
    );
    return ret;
    #undef PRINT_WRAPPER
  }
  const char *reason_;
};
template <typename MdsTableType>
int MdsTableImpl<MdsTableType>::forcely_reset_mds_table(const char *reason)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(reason)
  int ret = OB_SUCCESS;
  MDS_TG(100_ms);
  MdsWLockGuard lg(lock_);
  ForcelyReleaseAllNodeOp op(reason);
  if (OB_FAIL(for_each_scan_row(FowEachRowAction::RESET, op))) {
    MDS_LOG_GC(ERROR, "fail to do reset");
  } else {
    debug_info_.last_reset_ts_ = ObClockGenerator::getCurrentTime();
    flushing_scn_.reset();
    last_inner_recycled_scn_ = share::SCN::min_scn();
    rec_scn_ = share::SCN::max_scn();
    ATOMIC_STORE(&total_node_cnt_, 0);
    MDS_LOG_GC(INFO, "forcely release all mds nodes");
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

#undef MDS_ELEMENT
}
}
}
#endif
