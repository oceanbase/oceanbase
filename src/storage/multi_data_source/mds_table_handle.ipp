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

#ifndef STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_HANDLE_IPP
#define STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_HANDLE_IPP

#include "lib/ob_errno.h"
#include "storage/multi_data_source/mds_node.h"
#ifndef STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_HANDLE_H_IPP
#define STORAGE_MULTI_DATE_SOURCE_MDS_TABLE_HANDLE_H_IPP
#include "mds_table_handle.h"
#endif

namespace oceanbase
{
namespace storage
{
namespace mds
{
#define CHECK_MDS_TABLE_INIT() \
do {\
  if (!is_valid()) {\
    ret = OB_NOT_INIT;\
    MDS_LOG(ERROR, "MdsTableHandle is not init yet", KR(ret), K(*this));\
    return ret;\
  }\
} while(0)

template <typename K, typename V>
struct MdsTableHandleHelper {
  template <typename MdsTableType>
  struct InnerInnerHelper {
    template <int IDX>
    static int get_unit_id(uint8_t &mds_unit_id) {
      int ret = OB_SUCCESS;
      using UnitType = typename TupleIdxType<MdsTableType, IDX>::type;
      if (std::is_same<typename UnitType::key_type, typename std::decay<K>::type>::value &&
          std::is_same<typename UnitType::value_type, typename std::decay<V>::type>::value) {
        mds_unit_id = IDX;
      } else {
        ret = get_unit_id<IDX + 1>(mds_unit_id);
      }
      return ret;
    }
    template <>
    static int get_unit_id<MdsTableType::get_element_size()>(uint8_t &mds_unit_id) {
      int ret = OB_OBJ_TYPE_ERROR;
      MDS_LOG(ERROR, "type error, no this KV unit in this MdsTable", KR(ret),
                      K(typeid(MdsTableType).name()), K(typeid(K).name()), K(typeid(V).name()),
                      K(typeid(typename TupleIdxType<MdsTableType, 0>::type::key_type).name()),
                      K(typeid(typename TupleIdxType<MdsTableType, 0>::type::value_type).name()));
      return ret;
    }
  };
  template <int IDX>
  static int get_unit_id(const uint8_t mds_table_id, uint8_t &mds_unit_id) {
    int ret = OB_SUCCESS;
    if (IDX == mds_table_id) {
      ret = InnerInnerHelper<typename TupleIdxType<MdsTableTypeTuple, IDX>::type>::
            template get_unit_id<0>(mds_unit_id);
    } else {
      ret = get_unit_id<IDX + 1>(mds_table_id, mds_unit_id);
    }
    return ret;
  }
  template <>
  static int get_unit_id<MdsTableTypeTuple::get_element_size()>(const uint8_t mds_table_id,
                                                                uint8_t &mds_unit_id) {
    int ret = OB_OBJ_TYPE_ERROR;
    MDS_LOG(ERROR, "type error, no this MdsTable", KR(ret), K(mds_table_id),
                    K(typeid(K).name()), K(typeid(V).name()));
    return ret;
  }
};

inline bool MdsTableHandle::operator==(const MdsTableHandle &rhs) const
{
    return mds_table_id_ == rhs.mds_table_id_ &&
           p_mds_table_base_ == rhs.p_mds_table_base_;
}

inline MdsTableHandle::~MdsTableHandle()
{
  MDS_LOG(DEBUG, "MdsTableHandle destructed", K_(mds_table_id), KPC_(p_mds_table_base_.ctrl_ptr));
  mds_table_id_ = UINT8_MAX;
}

inline int MdsTableHandle::get_tablet_id(common::ObTabletID &tablet_id) const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  if (!p_mds_table_base_.is_valid()) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(WARN, "p_mds_table_base_ is invalid", K(*this));
  } else {
    tablet_id = p_mds_table_base_->get_tablet_id();
  }
  return ret;
}

inline int MdsTableHandle::get_ls_id(share::ObLSID &ls_id) const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  if (!p_mds_table_base_.is_valid()) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(WARN, "p_mds_table_base_ is invalid", K(*this));
  } else {
    ls_id = p_mds_table_base_->get_ls_id();
  }
  return ret;
}

template <typename MdsTableType>
int MdsTableHandle::init(ObIAllocator &allocator,
                         const ObTabletID tablet_id,
                         const share::ObLSID ls_id,
                         ObTabletPointer *pointer,
                         ObMdsTableMgr *p_mgr)
{
  int ret = OB_SUCCESS;
  ObLightSharedPtr<MdsTableImpl<MdsTableType>> p_mds_table;
  if (TupleTypeIdx<MdsTableTypeTuple,
                    MdsTableType>::value == MdsTableTypeTuple::get_element_size()) {
    ret = OB_OBJ_TYPE_ERROR;
    MDS_LOG(WARN, "this MdsTableType not exist", K(typeid(MdsTableType).name()));
  } else if (is_valid()) {
    this->~MdsTableHandle();
    new (this) MdsTableHandle();
    MDS_LOG(WARN, "mds_table has been inited, reset and init again", KP(this), K(lbt()));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(p_mds_table.construct(allocator))) {
      MDS_LOG(WARN, "construct mds table impl failed", KP(this), K(lbt()));
    } else if (OB_FAIL(p_mds_table->init(tablet_id, ls_id, pointer, p_mgr))) {
      MDS_LOG(WARN, "init mds table failed", KR(ret), K(mds_table_id_),
                    K(typeid(MdsTableType).name()));
    } else {
      p_mds_table_base_ = p_mds_table;
      uint8_t tablet_id = TupleTypeIdx<MdsTableTypeTuple, MdsTableType>::value;
      ATOMIC_STORE(&mds_table_id_, tablet_id);
    }
  }
  return ret;
}

template <typename T>
int MdsTableHandle::set(T &&data, MdsCtx &ctx, const int64_t lock_timeout_us)
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  uint8_t unit_id = INT8_MAX;
  ret = MdsTableHandleHelper<DummyKey, T>::template get_unit_id<0>(mds_table_id_, unit_id);
  DummyKey dummy_key;
  if (OB_SUCC(ret)) {
    int64_t converted_timeout = 0;
    if (TLOCAL_MDS_INFO.notify_type_ == transaction::NotifyType::UNKNOWN) { // no restrict
      converted_timeout = lock_timeout_us;
    } else if (TLOCAL_MDS_INFO.notify_type_ == transaction::NotifyType::REGISTER_SUCC) {
      if (lock_timeout_us > 30_s) {// timeout no more than 30s
        MDS_LOG(INFO, "timeout ts mustn't more than 30s in current version",
                      KR(ret), K(unit_id), K(data), K(ctx), K(lock_timeout_us), K(converted_timeout), K(TLOCAL_MDS_INFO));
        converted_timeout = 30_s;
      } else {
        converted_timeout = lock_timeout_us;
      }
    } else {// do mds data maybe hang operation is not allowed in other phase callback
      ret = OB_OP_NOT_ALLOW;// this call may deadlock with other threads and can not be avoided
      MDS_LOG(ERROR, "you mustn't do maybe hung operation in trans callbacks :"
                     " on_redo/before_prepare/on_prepare/on_commit/on_abort",
                     KR(ret), K(unit_id), K(data), K(ctx), K(lock_timeout_us), K(converted_timeout), K(TLOCAL_MDS_INFO));
      MDS_ASSERT(false);// abort in test environment
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(p_mds_table_base_->set(unit_id,
                                        (void*)&dummy_key,
                                        (void*)&data,
                                        std::is_rvalue_reference<decltype(data)>::value,
                                        ctx,
                                        converted_timeout))) {
        MDS_LOG(WARN, "fail to call set", KR(ret), K(unit_id), K(data), K(ctx), K(lock_timeout_us), K(converted_timeout));
      }
    }
  }
  return ret;
}

template <typename T>
int MdsTableHandle::replay(T &&data, MdsCtx &ctx, const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  uint8_t unit_id = INT8_MAX;
  ret = MdsTableHandleHelper<DummyKey, T>::template get_unit_id<0>(mds_table_id_, unit_id);
  DummyKey dummy_key;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(p_mds_table_base_->replay(unit_id,
                                          (void*)&dummy_key,
                                          (void*)&data,
                                          std::is_rvalue_reference<T>::value,
                                          ctx,
                                          scn))) {
      MDS_LOG(WARN, "fail to call replay", KR(ret), K(unit_id), K(data), K(ctx), K(scn));
    }
  }
  return ret;
}

template <typename T, typename OP,
          typename std::enable_if<OB_TRAIT_IS_FUNCTION_LIKE(OP, int(const T&)), bool>::type>
int MdsTableHandle::get_latest(OP &&read_op, bool &is_committed) const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  uint8_t unit_id = INT8_MAX;
  ret = MdsTableHandleHelper<DummyKey, T>::template get_unit_id<0>(mds_table_id_, unit_id);
  DummyKey dummy_key;
  ObFunction<int(void *)> function = [&read_op](void *data) -> int {
    return read_op(*reinterpret_cast<const T*>(data));
  };
  if (OB_SUCC(ret)) {
    if (OB_FAIL(p_mds_table_base_->get_latest(unit_id,
                                              (void*)&dummy_key,
                                              function,
                                              is_committed))) {
      if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret)) {
        MDS_LOG(WARN, "fail to call get_latest", KR(ret), K(unit_id));
      }
    }
  }
  return ret;
}

template <typename OP>
struct GetTabletStatusNodeOpWrapper {
  GetTabletStatusNodeOpWrapper(OP &op) : op_(op) {}
  int operator()(void *tablet_status_node) {
    return op_(*reinterpret_cast<const UserMdsNode<DummyKey, ObTabletCreateDeleteMdsUserData> *>(tablet_status_node));
  }
  OP &op_;
};
template <typename OP,
          typename std::enable_if<OB_TRAIT_IS_FUNCTION_LIKE(OP,
                   int(const UserMdsNode<DummyKey, ObTabletCreateDeleteMdsUserData>&)), bool>::type>
int MdsTableHandle::get_tablet_status_node(OP &&read_op, const int64_t read_seq) const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  ObFunction<int(void *)> function = GetTabletStatusNodeOpWrapper<OP>(read_op);
  if (OB_FAIL(p_mds_table_base_->get_tablet_status_node(function, read_seq))) {
    if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret)) {
      MDS_LOG(WARN, "fail to call get_latest", KR(ret), K(read_seq));
    }
  }
  return ret;
}

template <typename T, typename OP,
          typename std::enable_if<OB_TRAIT_IS_FUNCTION_LIKE(OP, int(const T&)), bool>::type>
int MdsTableHandle::get_snapshot(OP &&read_op,
                                 const share::SCN snapshot,
                                 const int64_t timeout_us) const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  uint8_t unit_id = INT8_MAX;
  ret = MdsTableHandleHelper<DummyKey, T>::template get_unit_id<0>(mds_table_id_, unit_id);
  DummyKey dummy_key;
  ObFunction<int(void *)> function = [&read_op](void *data) -> int {
    return read_op(*reinterpret_cast<const T*>(data));
  };
  if (OB_SUCC(ret)) {
    int64_t converted_timeout = 0;
    if (TLOCAL_MDS_INFO.notify_type_ == transaction::NotifyType::UNKNOWN) { // no restrict
      converted_timeout = timeout_us;
    } else if (TLOCAL_MDS_INFO.notify_type_ == transaction::NotifyType::REGISTER_SUCC) {
      if (timeout_us > 30_s) {// timeout no more than 30s
        MDS_LOG(INFO, "timeout ts mustn't more than 30s in current version", KR(ret), K(unit_id), K(snapshot),
                      K(timeout_us), K(converted_timeout), K(TLOCAL_MDS_INFO));
        converted_timeout = 30_s;
      } else {
        converted_timeout = timeout_us;
      }
    } else {// do mds data maybe hang operation is not allowed in other phase callback
      ret = OB_OP_NOT_ALLOW;// this call may deadlock with other threads and can not be avoided
      MDS_LOG(ERROR, "you mustn't do maybe hung operation in trans callbacks :"
                     " on_redo/before_prepare/on_prepare/on_commit/on_abort", KR(ret), K(unit_id), K(snapshot),
                     K(timeout_us), K(converted_timeout), K(TLOCAL_MDS_INFO));
      MDS_ASSERT(false);// abort in test environment
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(p_mds_table_base_->get_snapshot(unit_id,
                                                  (void*)&dummy_key,
                                                  function,
                                                  snapshot,
                                                  converted_timeout))) {
        if (OB_SNAPSHOT_DISCARDED != ret) {
          MDS_LOG(WARN, "fail to call get_snapshot", KR(ret), K(unit_id), K(snapshot),
                  K(timeout_us), K(converted_timeout));
        }
      }
    }
  }
  return ret;
}

template <typename T, typename OP,
          typename std::enable_if<OB_TRAIT_IS_FUNCTION_LIKE(OP, int(const T&)), bool>::type>
int MdsTableHandle::get_by_writer(OP &&read_op,
                                  const MdsWriter &writer,
                                  const share::SCN snapshot,
                                  const transaction::ObTxSEQ read_seq,
                                  const int64_t timeout_us) const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  uint8_t unit_id = INT8_MAX;
  ret = MdsTableHandleHelper<DummyKey, T>::template get_unit_id<0>(mds_table_id_, unit_id);
  DummyKey dummy_key;
  ObFunction<int(void *)> function = [&read_op](void *data) -> int {
    return read_op(*reinterpret_cast<const T*>(data));
  };
  if (OB_SUCC(ret)) {
    int64_t converted_timeout = 0;
    if (TLOCAL_MDS_INFO.notify_type_ == transaction::NotifyType::UNKNOWN) { // no restrict
      converted_timeout = timeout_us;
    } else if (TLOCAL_MDS_INFO.notify_type_ == transaction::NotifyType::REGISTER_SUCC) {
      if (timeout_us > 30_s) {// timeout no more than 30s
        MDS_LOG(INFO, "timeout ts mustn't more than 30s in current version", KR(ret), K(unit_id), K(writer),
                      K(snapshot), K(read_seq), K(timeout_us), K(converted_timeout), K(TLOCAL_MDS_INFO));
        converted_timeout = 30_s;
      } else {
        converted_timeout = timeout_us;
      }
    } else {// do mds data maybe hang operation is not allowed in other phase callback
      ret = OB_OP_NOT_ALLOW;// this call may deadlock with other threads and can not be avoided
      MDS_LOG(ERROR, "you mustn't do maybe hung operation in trans callbacks :"
                     " on_redo/before_prepare/on_prepare/on_commit/on_abort", KR(ret), K(unit_id), K(writer),
                     K(snapshot), K(read_seq), K(timeout_us), K(converted_timeout), K(TLOCAL_MDS_INFO));
      MDS_ASSERT(false);// abort in test environment
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(p_mds_table_base_->get_by_writer(unit_id,
                                                  (void*)&dummy_key,
                                                  function,
                                                  writer,
                                                  snapshot,
                                                  read_seq,
                                                  converted_timeout))) {
        if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret)) {
          MDS_LOG(WARN, "fail to call get_by_writer", KR(ret), K(unit_id), K(writer),
                  K(snapshot), K(read_seq), K(timeout_us), K(converted_timeout));
        }
      }
    }
  }
  return ret;
}

template <typename T>
int MdsTableHandle::is_locked_by_others(bool &is_locked, const MdsWriter &self) const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  uint8_t unit_id = INT8_MAX;
  ret = MdsTableHandleHelper<DummyKey, T>::template get_unit_id<0>(mds_table_id_, unit_id);
  DummyKey dummy_key;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(p_mds_table_base_->is_locked_by_others(unit_id,
                                                       (void*)&dummy_key,
                                                       is_locked,
                                                       self))) {
      MDS_LOG(WARN, "fail to call is_locked_by_others", KR(ret), K(unit_id), K(is_locked),
              K(self));
    }
  }
  return ret;
}
/**************************************************************************************************/

/******************************Multi Key Unit Access Interface*************************************/
template <typename Key, typename Value>
int MdsTableHandle::set(const Key &key, Value &&data, MdsCtx &ctx, const int64_t lock_timeout_us)
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  uint8_t unit_id = INT8_MAX;
  ret = MdsTableHandleHelper<Key, Value>::template get_unit_id<0>(mds_table_id_, unit_id);
  if (OB_SUCC(ret)) {
    int64_t converted_timeout = 0;
    if (TLOCAL_MDS_INFO.notify_type_ == transaction::NotifyType::UNKNOWN) { // no restrict
      converted_timeout = lock_timeout_us;
    } else if (TLOCAL_MDS_INFO.notify_type_ == transaction::NotifyType::REGISTER_SUCC) {
      if (lock_timeout_us > 30_s) {// timeout no more than 30s
        MDS_LOG(INFO, "timeout ts mustn't more than 30s in current version", KR(ret), K(unit_id), K(key), K(data), K(ctx),
                      K(lock_timeout_us), K(converted_timeout), K(TLOCAL_MDS_INFO));
        converted_timeout = 30_s;
      } else {
        converted_timeout = lock_timeout_us;
      }
    } else {// do mds data maybe hang operation is not allowed in other phase callback
      ret = OB_OP_NOT_ALLOW;// this call may deadlock with other threads and can not be avoided
      MDS_LOG(ERROR, "you mustn't do maybe hung operation in trans callbacks :"
                     " on_redo/before_prepare/on_prepare/on_commit/on_abort", KR(ret), K(unit_id), K(key), K(data),
                     K(ctx), K(lock_timeout_us), K(converted_timeout), K(TLOCAL_MDS_INFO));
      MDS_ASSERT(false);// abort in test environment
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(p_mds_table_base_->set(unit_id,
                                        (void*)&key,
                                        (void*)&data,
                                        std::is_rvalue_reference<Value>::value,
                                        ctx,
                                        converted_timeout))) {
        MDS_LOG(WARN, "fail to call set", KR(ret), K(unit_id), K(key), K(data), K(ctx),
                      K(lock_timeout_us), K(converted_timeout));
      }
    }
  }
  return ret;
}

template <typename Key, typename Value>
int MdsTableHandle::replay(const Key &key, Value &&data, MdsCtx &ctx, const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  uint8_t unit_id = INT8_MAX;
  ret = MdsTableHandleHelper<Key, Value>::template get_unit_id<0>(mds_table_id_, unit_id);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(p_mds_table_base_->replay(unit_id,
                                          (void*)&key,
                                          (void*)&data,
                                          std::is_rvalue_reference<Value>::value,
                                          ctx,
                                          scn))) {
      MDS_LOG(WARN, "fail to call replay", KR(ret), K(unit_id), K(key), K(data), K(ctx), K(scn));
    }
  }
  return ret;
}

template <typename Key, typename Value>
int MdsTableHandle::remove(const Key &key, MdsCtx &ctx, const int64_t lock_timeout_us)
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  uint8_t unit_id = INT8_MAX;
  ret = MdsTableHandleHelper<Key, Value>::template get_unit_id<0>(mds_table_id_, unit_id);
  if (OB_SUCC(ret)) {
    int64_t converted_timeout = 0;
    if (TLOCAL_MDS_INFO.notify_type_ == transaction::NotifyType::UNKNOWN) { // no restrict
      converted_timeout = lock_timeout_us;
    } else if (TLOCAL_MDS_INFO.notify_type_ == transaction::NotifyType::REGISTER_SUCC) {
      if (lock_timeout_us > 30_s) {// timeout no more than 30s
        MDS_LOG(INFO, "timeout ts mustn't more than 30s in current version", KR(ret), K(unit_id), K(key), K(ctx),
                      K(lock_timeout_us), K(converted_timeout), K(TLOCAL_MDS_INFO));
        converted_timeout = 30_s;
      } else {
        converted_timeout = lock_timeout_us;
      }
    } else {// do mds data maybe hang operation is not allowed in other phase callback
      ret = OB_OP_NOT_ALLOW;// this call may deadlock with other threads and can not be avoided
      MDS_LOG(ERROR, "you mustn't do maybe hung operation in trans callbacks :"
                     " on_redo/before_prepare/on_prepare/on_commit/on_abort", KR(ret), K(unit_id), K(key), K(ctx),
                      K(lock_timeout_us), K(converted_timeout), K(TLOCAL_MDS_INFO));
      MDS_ASSERT(false);// abort in test environment
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(p_mds_table_base_->remove(unit_id,
                                            (void*)&key,
                                            ctx,
                                            converted_timeout))) {
        MDS_LOG(WARN, "fail to call remove", KR(ret), K(unit_id), K(key), K(ctx),
                      K(lock_timeout_us), K(converted_timeout));
      }
    }
  }
  return ret;
}

template <typename Key, typename Value>
int MdsTableHandle::replay_remove(const Key &key, MdsCtx &ctx, share::SCN &scn)
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  uint8_t unit_id = INT8_MAX;
  ret = MdsTableHandleHelper<Key, Value>::template get_unit_id<0>(mds_table_id_, unit_id);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(p_mds_table_base_->replay_remove(unit_id,
                                                 (void*)&key,
                                                 ctx,
                                                 scn))) {
      MDS_LOG(WARN, "fail to call replay_remove", KR(ret), K(unit_id), K(key), K(ctx), K(scn));
    }
  }
  return ret;
}

template <typename Key, typename Value, typename OP>
int MdsTableHandle::get_latest(const Key &key,
                               OP &&read_op,
                               bool &is_committed) const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  uint8_t unit_id = INT8_MAX;
  ret = MdsTableHandleHelper<Key, Value>::template get_unit_id<0>(mds_table_id_, unit_id);
  ObFunction<int(void *)> function = [&read_op](void *data) -> int {
    return read_op(*reinterpret_cast<const Value*>(data));
  };
  if (OB_SUCC(ret)) {
    if (OB_FAIL(p_mds_table_base_->get_latest(unit_id,
                                              (void*)&key,
                                              function,
                                              is_committed))) {
      if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret)) {
        MDS_LOG(WARN, "fail to call get_latest", KR(ret), K(unit_id), K(key));
      }
    }
  }
  return ret;
}

template <typename Key, typename Value, typename OP>
int MdsTableHandle::get_snapshot(const Key &key,
                                 OP &&read_op,
                                 const share::SCN snapshot,
                                 const int64_t timeout_us) const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  uint8_t unit_id = INT8_MAX;
  ret = MdsTableHandleHelper<Key, Value>::template get_unit_id<0>(mds_table_id_, unit_id);
  ObFunction<int(void *)> function = [&read_op](void *data) -> int {
    return read_op(*reinterpret_cast<const Value*>(data));
  };
  if (OB_SUCC(ret)) {
    int64_t converted_timeout = 0;
    if (TLOCAL_MDS_INFO.notify_type_ == transaction::NotifyType::UNKNOWN) { // no restrict
      converted_timeout = timeout_us;
    } else if (TLOCAL_MDS_INFO.notify_type_ == transaction::NotifyType::REGISTER_SUCC) {
      if (timeout_us > 30_s) {// timeout no more than 30s
        MDS_LOG(INFO, "timeout ts mustn't more than 30s in current version", KR(ret), K(unit_id), K(key), K(snapshot),
                      K(timeout_us), K(converted_timeout), K(TLOCAL_MDS_INFO));
        converted_timeout = 30_s;
      } else {
        converted_timeout = timeout_us;
      }
    } else {// do mds data maybe hang operation is not allowed in other phase callback
      ret = OB_OP_NOT_ALLOW;// this call may deadlock with other threads and can not be avoided
      MDS_LOG(ERROR, "you mustn't do maybe hung operation in trans callbacks :"
                     " on_redo/before_prepare/on_prepare/on_commit/on_abort", KR(ret), K(unit_id), K(key), K(snapshot),
                     K(timeout_us), K(converted_timeout), K(TLOCAL_MDS_INFO));
      MDS_ASSERT(false);// abort in test environment
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(p_mds_table_base_->get_snapshot(unit_id,
                                                  (void*)&key,
                                                  function,
                                                  snapshot,
                                                  converted_timeout))) {
        if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret)) {
          MDS_LOG(WARN, "fail to call get_snapshot", KR(ret), K(unit_id), K(key), K(snapshot),
                        K(timeout_us), K(converted_timeout));
        }
      }
    }
  }
  return ret;
}

template <typename Key, typename Value, typename OP>
int MdsTableHandle::get_by_writer(const Key &key,
                                  OP &&read_op,
                                  const MdsWriter &writer,
                                  const share::SCN snapshot,
                                  const transaction::ObTxSEQ read_seq,
                                  const int64_t timeout_us) const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  uint8_t unit_id = INT8_MAX;
  ret = MdsTableHandleHelper<Key, Value>::template get_unit_id<0>(mds_table_id_, unit_id);
  ObFunction<int(void *)> function = [&read_op](void *data) -> int {
    return read_op(*reinterpret_cast<const Value*>(data));
  };
  if (OB_SUCC(ret)) {
    int64_t converted_timeout = 0;
    if (TLOCAL_MDS_INFO.notify_type_ == transaction::NotifyType::UNKNOWN) { // no restrict
      converted_timeout = timeout_us;
    } else if (TLOCAL_MDS_INFO.notify_type_ == transaction::NotifyType::REGISTER_SUCC) {
      if (timeout_us > 30_s) {// timeout no more than 30s
        MDS_LOG(INFO, "timeout ts mustn't more than 30s in current version", KR(ret), K(unit_id), K(key), K(writer),
                       K(snapshot), K(read_seq), K(timeout_us), K(converted_timeout), K(TLOCAL_MDS_INFO));
        converted_timeout = 30_s;
      } else {
        converted_timeout = timeout_us;
      }
    } else {// do mds data maybe hang operation is not allowed in other phase callback
      ret = OB_OP_NOT_ALLOW;// this call may deadlock with other threads and can not be avoided
      MDS_LOG(ERROR, "you mustn't do maybe hung operation in trans callbacks :"
                     " on_redo/before_prepare/on_prepare/on_commit/on_abort", KR(ret), K(unit_id), K(key), K(writer),
                     K(snapshot), K(read_seq), K(timeout_us), K(converted_timeout), K(TLOCAL_MDS_INFO));
      MDS_ASSERT(false);// abort in test environment
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(p_mds_table_base_->get_by_writer(unit_id,
                                                  (void*)&key,
                                                  function,
                                                  writer,
                                                  snapshot,
                                                  read_seq,
                                                  converted_timeout))) {
        if (OB_UNLIKELY(OB_SNAPSHOT_DISCARDED != ret)) {
          MDS_LOG(WARN, "fail to call get_by_writer", KR(ret), K(unit_id), K(key), K(writer),
                        K(snapshot), K(read_seq), K(timeout_us), K(converted_timeout));
        }
      }
    }
  }
  return ret;
}

template <typename Key, typename Value>
int MdsTableHandle::is_locked_by_others(const Key &key,
                                        bool &is_locked,
                                        const MdsWriter &self) const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  uint8_t unit_id = INT8_MAX;
  ret = MdsTableHandleHelper<Key, Value>::template get_unit_id<0>(mds_table_id_, unit_id);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(p_mds_table_base_->is_locked_by_others(unit_id,
                                                       (void*)&key,
                                                       is_locked,
                                                       self))) {
      MDS_LOG(WARN, "fail to call is_locked_by_others", KR(ret), K(unit_id), K(key), K(is_locked),
              K(self));
    }
  }
  return ret;
}

template <ScanRowOrder SCAN_ROW_ORDER,
          ScanNodeOrder SCAN_NODE_ORDER,
          typename DUMP_OP,
          typename std::enable_if<OB_TRAIT_IS_FUNCTION_LIKE(DUMP_OP,
                                                            int(const MdsDumpKV &)), bool>::type>
int MdsTableHandle::scan_all_nodes_to_dump(DUMP_OP &&for_each_op,
                                           const int64_t mds_construct_sequence,
                                           const bool for_flush) const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  ObFunction<int(const MdsDumpKV &)> op = [&for_each_op](const MdsDumpKV &kv) -> int {
    return for_each_op(kv);
  };
  if (OB_FAIL(p_mds_table_base_->scan_all_nodes_to_dump(op,
                                                        mds_construct_sequence,
                                                        for_flush,
                                                        SCAN_ROW_ORDER,
                                                        SCAN_NODE_ORDER))) {
    MDS_LOG(WARN, "fail to do for_each dump op", KR(ret), K(*this));
  }
  return ret;
}

inline int MdsTableHandle::flush(share::SCN need_advanced_rec_scn_lower_limit, share::SCN max_decided_scn)
{
  int ret = OB_SUCCESS;
  // return ret;// FIXME: for lixia test, will block CLOG recycle
#ifndef TEST_MDS_TRANSACTION
  CHECK_MDS_TABLE_INIT();
  ret = p_mds_table_base_->flush(need_advanced_rec_scn_lower_limit, max_decided_scn);
#endif
  return ret;
}

inline int MdsTableHandle::is_flushing(bool &is_flushing) const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  is_flushing = p_mds_table_base_->is_flushing();
  return ret;
}

inline void MdsTableHandle::on_flush(const share::SCN &flush_scn, const int flush_ret)
{
  if (!is_valid()) {
    MDS_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "can not do on_flush, cause this handle is invalid", K(*this));
  } else {
    p_mds_table_base_->on_flush(flush_scn, flush_ret);
  }
}

inline int MdsTableHandle::get_rec_scn(share::SCN &rec_scn) const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  rec_scn = p_mds_table_base_->get_rec_scn();
  return ret;
}

inline int MdsTableHandle::get_node_cnt(int64_t &valid_cnt) const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  valid_cnt = p_mds_table_base_->get_node_cnt();
  return ret;
}

inline bool MdsTableHandle::is_valid() const
{
  uint8_t mds_table_id = ATOMIC_LOAD(&mds_table_id_);
  return mds_table_id >= 0 && mds_table_id < MdsTableTypeTuple::get_element_size();
}

inline void MdsTableHandle::reset()
{
  this->~MdsTableHandle();
}

inline int MdsTableHandle::try_recycle(const share::SCN &recycle_scn)// release nodes
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  return p_mds_table_base_->try_recycle(recycle_scn);
}

inline int MdsTableHandle::sync_ref_until_last() const
{
  #define PRINT_WRAPPER KR(ret), K(*this)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (!p_mds_table_base_.is_valid()) {
    ret = OB_NOT_INIT;
    MDS_LOG_GC(ERROR, "this is an invalid handle", K(*this));
  } else if (MDS_FAIL(p_mds_table_base_.sync_until_last())) {
    MDS_LOG_GC(WARN, "fail to sync ref", K(*this));
  }
  return ret;
  #undef PRINT_WRAPPER
}

inline int MdsTableHandle::get_ref_cnt(int64_t &ref_cnt) const
{
  #define PRINT_WRAPPER KR(ret), K(*this)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (!p_mds_table_base_.is_valid()) {
    ret = OB_NOT_INIT;
    MDS_LOG_GC(ERROR, "this is an invalid handle", K(*this));
  } else if (MDS_FAIL(p_mds_table_base_.get_ref_cnt(ref_cnt))) {
    MDS_LOG_GC(WARN, "fail to get ref cnt", K(*this));
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
struct MdsTableUnitConvertHelper {
  template <typename MdsTableType>
  struct InnerInnerHelper {
    template <int IDX>
    static int get_unit(MdsTableBase *p_mds_table, MdsUnit<K, V> *&p_mds_unit) {
      int ret = OB_SUCCESS;
      using UnitType = typename TupleIdxType<MdsTableType, IDX>::type;
      if (std::is_same<typename UnitType::key_type, typename std::decay<K>::type>::value &&
          std::is_same<typename UnitType::value_type, typename std::decay<V>::type>::value) {
        p_mds_unit = (MdsUnit<K, V> *)&(static_cast<MdsTableImpl<MdsTableType> *>(p_mds_table)->unit_tuple().template element<IDX>());
      } else {
        ret = get_unit<IDX + 1>(p_mds_table, p_mds_unit);
      }
      return ret;
    }
    template <>
    static int get_unit<MdsTableType::get_element_size()>(MdsTableBase *p_mds_table,
                                                          MdsUnit<K, V> *&p_mds_unit) {
      int ret = OB_OBJ_TYPE_ERROR;
      MDS_LOG(ERROR, "type error, no this KV unit in this MdsTable", KR(ret),
                      K(typeid(MdsTableType).name()), K(typeid(K).name()), K(typeid(V).name()),
                      K(typeid(typename TupleIdxType<MdsTableType, 0>::type::key_type).name()),
                      K(typeid(typename TupleIdxType<MdsTableType, 0>::type::value_type).name()));
      return ret;
    }
  };
  template <int IDX>
  static int get_unit(const uint8_t mds_table_id,
                      MdsTableBase *p_mds_table,
                      MdsUnit<K, V> *&p_mds_unit) {
    int ret = OB_SUCCESS;
    if (IDX == mds_table_id) {
      ret = InnerInnerHelper<typename TupleIdxType<MdsTableTypeTuple, IDX>::type>::
            template get_unit<0>(p_mds_table, p_mds_unit);
    } else {
      ret = get_unit<IDX + 1>(mds_table_id, p_mds_table, p_mds_unit);
    }
    return ret;
  }
  template <>
  static int get_unit<MdsTableTypeTuple::get_element_size()>(const uint8_t mds_table_id,
                                                             MdsTableBase *p_mds_table,
                                                             MdsUnit<K, V> *&p_mds_unit) {
    int ret = OB_OBJ_TYPE_ERROR;
    MDS_LOG(ERROR, "type error, no this MdsTable", KR(ret), K(mds_table_id),
                    K(typeid(K).name()), K(typeid(V).name()));
    return ret;
  }
};
template <typename UnitKey, typename UnitValue>
int MdsTableHandle::get_mds_unit(MdsUnit<UnitKey, UnitValue> *&p_mds_unit)
{
  int ret = OB_SUCCESS;
  p_mds_unit = nullptr;
  if (!p_mds_table_base_.is_valid()) {
    ret = OB_NOT_INIT;
    MDS_LOG(ERROR, "this is an invalid handle", K(*this));
  } else if (OB_SUCCESS != (ret = (MdsTableUnitConvertHelper<UnitKey, UnitValue>::
                                   template get_unit<0>(mds_table_id_,
                                                        get_mds_table_ptr(),
                                                        p_mds_unit)))) {
    MDS_LOG(WARN, "fail to get mds unit", K(*this));
  }
  return ret;
}

inline int MdsTableHandle::fill_virtual_info(ObIArray<MdsNodeInfoForVirtualTable> &mds_node_info_array) const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  if (!p_mds_table_base_.is_valid()) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(WARN, "p_mds_table_base_ is invalid", K(*this));
  } else if (OB_FAIL(p_mds_table_base_->fill_virtual_info(mds_node_info_array))) {
    MDS_LOG(WARN, "fail to fill virtual info", K(*this));
  }
  return ret;
}

inline int MdsTableHandle::mark_removed_from_t3m(ObTabletPointer *pointer) const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  if (!p_mds_table_base_.is_valid()) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(WARN, "p_mds_table_base_ is invalid", K(*this));
  } else {
    p_mds_table_base_->mark_removed_from_t3m(pointer);
  }
  return ret;
}

inline int MdsTableHandle::mark_switched_to_empty_shell() const
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  if (!p_mds_table_base_.is_valid()) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(WARN, "p_mds_table_base_ is invalid", K(*this));
  } else {
    p_mds_table_base_->mark_switched_to_empty_shell();
  }
  return ret;
}

template <int N>
inline int MdsTableHandle::forcely_reset_mds_table(const char (&reason)[N])
{
  int ret = OB_SUCCESS;
  CHECK_MDS_TABLE_INIT();
  if (!p_mds_table_base_.is_valid()) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(WARN, "p_mds_table_base_ is invalid", K(*this));
  } else {
    p_mds_table_base_->forcely_reset_mds_table(reason);
  }
  return ret;
}

}
}
}
#undef CHECK_MDS_TABLE_INIT
#endif
