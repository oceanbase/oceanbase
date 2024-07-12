#ifndef INCLUDE_OB_TABLET_MDS_PART_IPP
#define INCLUDE_OB_TABLET_MDS_PART_IPP
#include "lib/ob_errno.h"
#include "ob_i_tablet_mds_interface.h"
#include "share/ob_errno.h"
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/multi_data_source/mds_node.h"
#include "storage/tablet/ob_tablet_status.h"
#endif
namespace oceanbase
{
namespace storage
{

/********************************IMPLEMENTATION WITHOUT TEMPLATE***********************************/

inline common::ObTabletID ObITabletMdsInterface::get_tablet_id_() const
{
  return get_tablet_meta_().tablet_id_;
}

inline int ObITabletMdsInterface::check_tablet_status_written(bool &written)
{
  int ret = OB_SUCCESS;
  written = false;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG(WARN, "not inited", K(ret), KPC(this));
  } else if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(ERROR, "tablet pointer is null", K(ret), KPC(this));
  } else {
    written = get_tablet_pointer_()->is_tablet_status_written();
  }
  return ret;
}

/**********************************IMPLEMENTATION WITH TEMPLATE************************************/

template <typename K, typename T>
int ObITabletMdsInterface::read_data_from_mds_sstable(
    common::ObIAllocator &allocator,
    const K &key,
    const share::SCN &snapshot,
    const int64_t timeout_us,
    const common::ObFunction<int(const T&)> &read_op) const
{
  int ret = OB_SUCCESS;

  constexpr uint8_t mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<K, T>>::value;
  const int64_t key_size = key.mds_get_serialize_size();
  common::ObString key_str;
  mds::MdsDumpKV kv;

  if (key_size != 0) {
    char *buffer = static_cast<char *>(allocator.alloc(key_size));
    int64_t pos = 0;
    if (OB_ISNULL(buffer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      MDS_LOG(WARN, "fail to alloc memory", K(ret), K(key_size));
    } else if (OB_FAIL(key.mds_serialize(buffer, key_size, pos))) {
      MDS_LOG(WARN, "fail to serialize key", K(ret));
    } else {
      key_str.assign_ptr(buffer, key_size);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(read_raw_data(allocator, mds_unit_id, key_str, snapshot, timeout_us, kv))) {
    if (OB_ITER_END == ret) {
      MDS_LOG(DEBUG, "read nothing from mds sstable", K(ret));
    } else {
      MDS_LOG(WARN, "fail to read raw data", K(ret));
    }
  } else {
    T data;
    common::meta::MetaSerializer<T> ms(allocator, data);
    const common::ObString &user_data = kv.v_.user_data_;
    int64_t pos = 0;
    if (OB_FAIL(ms.deserialize(user_data.ptr(), user_data.length(), pos))) {
      MDS_LOG(WARN, "fail to deserialize", K(ret));
    } else if (OB_FAIL(read_op(data))) {
      MDS_LOG(WARN, "fail to do read op", K(ret));
    } else {
      MDS_LOG(DEBUG, "succeed to get data from mds sstable", K(ret), K(data));
    }
  }

  return ret;
}

template <>
inline int ObITabletMdsInterface::get_mds_data_from_tablet<mds::DummyKey, ObTabletCreateDeleteMdsUserData>(
    const mds::DummyKey &key,
    const share::SCN &snapshot,
    const int64_t timeout_us,
    const common::ObFunction<int(const ObTabletCreateDeleteMdsUserData&)> &read_op) const
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "mds_reader", ObCtxIds::DEFAULT_CTX_ID));
  const ObTabletCreateDeleteMdsUserData &last_persisted_committed_tablet_status = get_tablet_meta_().last_persisted_committed_tablet_status_;
  MDS_LOG(DEBUG, "print last persisted tablet status", K(last_persisted_committed_tablet_status));
  if (last_persisted_committed_tablet_status.is_valid() &&
        ObTabletStatus::NONE != last_persisted_committed_tablet_status.get_tablet_status()) {
    if (CLICK_FAIL(read_op(last_persisted_committed_tablet_status))) {
      MDS_LOG(WARN, "failed to do read op", K(ret), K(last_persisted_committed_tablet_status));
    }
  } else {
    if (CLICK_FAIL((read_data_from_mds_sstable<mds::DummyKey, ObTabletCreateDeleteMdsUserData>(
        allocator, key, snapshot, timeout_us, read_op)))) {
      if (OB_ITER_END == ret) {
        ret = OB_SNAPSHOT_DISCARDED;
        MDS_LOG(DEBUG, "read nothing from mds sstable", K(ret));
      } else {
        MDS_LOG(WARN, "fail to read data from mds sstable", K(ret));
      }
    } else {
      MDS_LOG(DEBUG, "succeed to read tablet status", K(ret));
    }
  }
  return ret;
}

template <>
inline int ObITabletMdsInterface::get_mds_data_from_tablet<mds::DummyKey, ObTabletBindingMdsUserData>(
    const mds::DummyKey &key,
    const share::SCN &snapshot,
    const int64_t timeout_us,
    const common::ObFunction<int(const ObTabletBindingMdsUserData&)> &read_op) const
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "mds_reader", ObCtxIds::DEFAULT_CTX_ID));

  if (CLICK_FAIL((read_data_from_mds_sstable<mds::DummyKey, ObTabletBindingMdsUserData>(
      allocator, key, snapshot, timeout_us, read_op)))) {
    if (OB_ITER_END == ret) {
      ret = OB_SNAPSHOT_DISCARDED;
      MDS_LOG(DEBUG, "read nothing from mds sstable", K(ret));
    } else {
      MDS_LOG(WARN, "fail to read data from mds sstable", K(ret), K(lbt()));
    }
  } else {
    MDS_LOG(DEBUG, "succeed to read aux tablet info", K(ret));
  }

  return ret;
}

template <>
inline int ObITabletMdsInterface::get_mds_data_from_tablet<mds::DummyKey, share::ObTabletAutoincSeq>(
    const mds::DummyKey &key,
    const share::SCN &snapshot,
    const int64_t timeout_us,
    const common::ObFunction<int(const share::ObTabletAutoincSeq&)> &read_op) const
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "mds_reader", ObCtxIds::DEFAULT_CTX_ID));

  if (OB_UNLIKELY(!snapshot.is_max())) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "invalid args, snapshot is not max scn", K(ret), K(snapshot));
  } else if (CLICK_FAIL((read_data_from_mds_sstable<mds::DummyKey, share::ObTabletAutoincSeq>(
      allocator, key, snapshot, timeout_us, read_op)))) {
    if (OB_ITER_END == ret) {
      ret = OB_SNAPSHOT_DISCARDED;
      MDS_LOG(DEBUG, "read nothing from mds sstable", K(ret));
    } else {
      MDS_LOG(WARN, "fail to read data from mds sstable", K(ret));
    }
  } else {
    MDS_LOG(DEBUG, "succeed to read auto inc seq", K(ret));
  }
  return ret;
}

template <>
inline int ObITabletMdsInterface::get_mds_data_from_tablet<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
    const compaction::ObMediumCompactionInfoKey &key,
    const share::SCN &snapshot,
    const int64_t timeout_us,
    const common::ObFunction<int(const compaction::ObMediumCompactionInfo&)> &read_op) const
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "mds_reader", ObCtxIds::DEFAULT_CTX_ID));

  if (OB_UNLIKELY(!snapshot.is_max())) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "invalid args, snapshot is not max scn", K(ret), K(snapshot));
  } else if (CLICK_FAIL((read_data_from_mds_sstable<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      allocator, key, snapshot, timeout_us, read_op)))) {
    if (OB_ITER_END == ret) {
      ret = OB_SNAPSHOT_DISCARDED;
      MDS_LOG(DEBUG, "read nothing from mds sstable", K(ret));
    } else {
      MDS_LOG(WARN, "fail to read data from mds sstable", K(ret));
    }
  } else {
    MDS_LOG(DEBUG, "succeed to read medium info", K(ret));
  }

  return ret;
}

template <typename T>// general set for dummy key unit
int ObITabletMdsInterface::set(T &&data, mds::MdsCtx &ctx, const int64_t lock_timeout_us)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(data), K(ctx), K(lock_timeout_us)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  mds::MdsTableHandle handle;
  if (MDS_FAIL(get_mds_table_handle_(handle, true))) {
    MDS_LOG_SET(WARN, "failed to get_mds_table");
  } else if (!handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_SET(WARN, "mds cannot be NULL");
  } else if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_SET(WARN, "tablet pointer is null", K(ret), KPC(this));
  } else if (MDS_FAIL(handle.set(std::forward<T>(data), ctx, lock_timeout_us))) {
    MDS_LOG_SET(WARN, "failed to set dummy key unit data");
  } else if (std::is_same<ObTabletCreateDeleteMdsUserData, typename std::decay<T>::type>::value) {
    get_tablet_pointer_()->set_tablet_status_written();
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename T>// general replay for dummy key unit
int ObITabletMdsInterface::replay(T &&data, mds::MdsCtx &ctx, const share::SCN &scn)// called by ObTabletReplayExecutor
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(data), K(ctx), K(scn)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (scn < get_tablet_meta_().mds_checkpoint_scn_) {
    MDS_LOG_SET(TRACE, "no need do replay");
  } else {
    mds::MdsTableHandle handle;
    if (CLICK_FAIL(get_mds_table_handle_(handle, true))) {
      MDS_LOG_SET(WARN, "failed to get_mds_table");
    } else if (!handle.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_SET(WARN, "mds cannot be NULL");
    } else if (OB_ISNULL(get_tablet_pointer_())) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_SET(WARN, "tablet pointer is null", K(ret), KPC(this));
    } else if (CLICK_FAIL(handle.replay(std::forward<T>(data), ctx, scn))) {
      MDS_LOG_SET(WARN, "failed to replay dummy key unit data");
    } else if (std::is_same<ObTabletCreateDeleteMdsUserData, typename std::decay<T>::type>::value) {
      get_tablet_pointer_()->set_tablet_status_written();
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename Key, typename Value>// general set for multi key unit
int ObITabletMdsInterface::set(const Key &key, Value &&data, mds::MdsCtx &ctx, const int64_t lock_timeout_us)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(key), K(data), K(ctx), K(lock_timeout_us)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  mds::MdsTableHandle handle;
  if (CLICK_FAIL(get_mds_table_handle_(handle, true))) {
    MDS_LOG_SET(WARN, "failed to get_mds_table");
  } else if (!handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_SET(WARN, "mds cannot be NULL");
  } else if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_SET(WARN, "tablet pointer is null", K(ret), KPC(this));
  } else if (CLICK_FAIL(handle.set(key, std::forward<Value>(data), ctx, lock_timeout_us))) {
    MDS_LOG_SET(WARN, "failed to set multi key unit data");
  } else {
    get_tablet_pointer_()->set_tablet_status_written();
    MDS_LOG_SET(TRACE, "success to set multi key unit data");
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename Key, typename Value>
int ObITabletMdsInterface::replay(const Key &key,
                                  Value &&mds,
                                  mds::MdsCtx &ctx,
                                  const share::SCN &scn)// called only by ObTabletReplayExecutor
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(key), K(mds), K(ctx), K(scn)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (scn < get_tablet_meta_().mds_checkpoint_scn_) {
    MDS_LOG_SET(TRACE, "no need do replay");
  } else {
    mds::MdsTableHandle handle;
    if (CLICK_FAIL(get_mds_table_handle_(handle, true))) {
      MDS_LOG_SET(WARN, "failed to get_mds_table");
    } else if (!handle.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_SET(WARN, "mds cannot be NULL");
    } else if (OB_ISNULL(get_tablet_pointer_())) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_SET(WARN, "tablet pointer is null", K(ret), KPC(this));
    } else if (CLICK_FAIL(handle.replay(key, std::forward<Value>(mds), ctx, scn))) {
      MDS_LOG_SET(WARN, "failed to replay multi key unit data");
    } else {
      get_tablet_pointer_()->set_tablet_status_written();
      MDS_LOG_SET(TRACE, "success to replay multi key unit data");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename Key, typename Value>// general remove for multi key unit
int ObITabletMdsInterface::remove(const Key &key, mds::MdsCtx &ctx, const int64_t lock_timeout_us)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(key), K(ctx), K(lock_timeout_us)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  mds::MdsTableHandle handle;
  if (CLICK_FAIL(get_mds_table_handle_(handle, true))) {
    MDS_LOG_SET(WARN, "failed to get_mds_table");
  } else if (!handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_SET(WARN, "mds cannot be NULL");
  } else if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_SET(WARN, "tablet pointer is null", K(ret), KPC(this));
  } else if (CLICK_FAIL(handle.remove(key, ctx, lock_timeout_us))) {
    MDS_LOG_SET(WARN, "failed to remove multi key unit data");
  } else if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_SET(WARN, "tablet pointer is null", K(ret), KPC(this));
  } else {
    get_tablet_pointer_()->set_tablet_status_written();
    MDS_LOG_SET(TRACE, "success to remove multi key unit data");
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename Key, typename Value>
int ObITabletMdsInterface::replay_remove(const Key &key, mds::MdsCtx &ctx, const share::SCN &scn)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(key), K(ctx), K(scn)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  mds::MdsTableHandle handle;
  if (CLICK_FAIL(get_mds_table_handle_(handle, true))) {
    MDS_LOG_SET(WARN, "failed to get_mds_table");
  } else if (!handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_SET(WARN, "mds cannot be NULL");
  } else if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_SET(WARN, "tablet pointer is null", K(ret), KPC(this));
  } else if (CLICK() && OB_SUCCESS != (ret = handle.replay_remove<Key, Value>(key, ctx, scn))) {
    MDS_LOG_SET(WARN, "failed to replay remove multi key unit data");
  } else {
    get_tablet_pointer_()->set_tablet_status_written();
    MDS_LOG_SET(TRACE, "success to remove multi key unit data");
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename T>
int ObITabletMdsInterface::is_locked_by_others(bool &is_locked, const mds::MdsWriter &self) const
{// FIXME: need concern about dumped uncommitted data on tablet
  #define PRINT_WRAPPER KR(ret), K(*this), K(is_locked), K(self)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  bool is_online = false;
  do {
    mds::MdsTableHandle handle;
    ObLSSwitchChecker ls_switch_checker;
    if (CLICK_FAIL(get_mds_table_handle_(handle, false))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        MDS_LOG_GET(WARN, "failed to get_mds_table");
      } else {
        MDS_LOG_GET(TRACE, "failed to get_mds_table");
        ret = OB_SUCCESS;
        is_locked = false;
      }
    } else if (!handle.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_GET(WARN, "mds cannot be NULL");
    } else if (OB_ISNULL(get_tablet_pointer_())) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_GET(WARN, "tablet pointer is null", K(ret), KPC(this));
    } else if (MDS_FAIL(ls_switch_checker.check_ls_switch_state(get_tablet_pointer_()->get_ls(), is_online))) {
      MDS_LOG_GET(WARN, "check ls online state failed", K(ret), KPC(this));
    } else if (CLICK_FAIL(handle.is_locked_by_others<T>(is_locked, self))) {
      if (OB_SNAPSHOT_DISCARDED != ret) {
        MDS_LOG_GET(WARN, "failed to check lock unit data");
      } else {
        MDS_LOG_GET(TRACE, "failed to check lock unit data");
        ret = OB_SUCCESS;
        is_locked = false;
      }
    }
    if (OB_SUCC(ret)) {
      if (is_online && MDS_FAIL(ls_switch_checker.double_check_epoch(is_online))) {
        if (!is_online) {
          ret = OB_LS_OFFLINE;
        }
        MDS_LOG_GET(WARN, "failed to double check ls online");
      } else {
        MDS_LOG_GET(TRACE, "success to get is locked by others state");
      }
    }
  } while (ret == OB_VERSION_NOT_MATCH && is_online);
  return ret;
  #undef PRINT_WRAPPER
}

template <typename T, typename OP>
int ObITabletMdsInterface::get_latest(OP &&read_op, bool &is_committed) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(typeid(OP).name())
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  bool is_online = false;
  do {
    mds::MdsTableHandle handle;
    ObLSSwitchChecker ls_switch_checker;
    if (CLICK_FAIL(get_mds_table_handle_(handle, false))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        MDS_LOG_GET(WARN, "failed to get_mds_table");
      } else {
        MDS_LOG_GET(TRACE, "failed to get_mds_table");
      }
    } else if (!handle.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_GET(WARN, "mds cannot be NULL");
    } else if (OB_ISNULL(get_tablet_pointer_())) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_GET(WARN, "tablet pointer is null", K(ret), KPC(this));
    } else if (MDS_FAIL(ls_switch_checker.check_ls_switch_state(get_tablet_pointer_()->get_ls(), is_online))) {
      MDS_LOG_GET(WARN, "check ls online state failed", K(ret), KPC(this));
    } else if (CLICK_FAIL(handle.get_latest<T>(read_op, is_committed))) {
      if (OB_SNAPSHOT_DISCARDED != ret) {
        MDS_LOG_GET(WARN, "failed to get mds data");
      } else {
        MDS_LOG_GET(TRACE, "failed to get mds data");
      }
    }
    if (CLICK_FAIL(ret)) {
      if (OB_ENTRY_NOT_EXIST == ret || OB_SNAPSHOT_DISCARDED == ret) {
        auto func = [&read_op, &is_committed](const T& data) -> int {
          is_committed = true;// FIXME: here need more judge after support dump uncommitted node
          return read_op(data);
        };
        if (CLICK_FAIL((get_mds_data_from_tablet<mds::DummyKey, T>(
            mds::DummyKey(),
            share::SCN::max_scn()/*snapshot*/,
            ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US/*timeout_us*/,
            func)))) {
          if (OB_SNAPSHOT_DISCARDED == ret) {
            ret = OB_EMPTY_RESULT;
            MDS_LOG_GET(DEBUG, "read nothing from mds sstable");
          } else {
            MDS_LOG_GET(WARN, "failed to get latest data from tablet");
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_online && MDS_FAIL(ls_switch_checker.double_check_epoch(is_online))) {
        if (!is_online) {
          ret = OB_LS_OFFLINE;
        }
        MDS_LOG_GET(WARN, "failed to double check ls online");
      } else {
        MDS_LOG_GET(TRACE, "success to get_latest");
      }
    }
  } while (ret == OB_VERSION_NOT_MATCH && is_online);
  return ret;
  #undef PRINT_WRAPPER
}

template <typename T, typename OP>// general get for dummy key unit
int ObITabletMdsInterface::get_snapshot(OP &&read_op,
                                        const share::SCN snapshot,
                                        const int64_t timeout_us) const
{
  return get_snapshot<mds::DummyKey, T>(mds::DummyKey(), read_op, snapshot, timeout_us);
}

template <typename Key, typename Value, typename OP>// general get for multi key unit
int ObITabletMdsInterface::get_snapshot(const Key &key,
                                        OP &&read_op,
                                        const share::SCN snapshot,
                                        const int64_t timeout_us) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(key), K(snapshot), K(typeid(OP).name())
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  bool is_online = false;
  do {
    mds::MdsTableHandle handle;
    ObLSSwitchChecker ls_switch_checker;
    if (CLICK_FAIL(get_mds_table_handle_(handle, false))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        MDS_LOG_GET(WARN, "failed to get_mds_table");
      } else {
        MDS_LOG_GET(TRACE, "failed to get_mds_table");
      }
    } else if (!handle.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_GET(WARN, "mds cannot be NULL");
    } else if (OB_ISNULL(get_tablet_pointer_())) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_GET(WARN, "tablet pointer is null", K(ret), KPC(this));
    } else if (MDS_FAIL(ls_switch_checker.check_ls_switch_state(get_tablet_pointer_()->get_ls(), is_online))) {
      MDS_LOG_GET(WARN, "check ls online state failed", K(ret), KPC(this));
    } else if (CLICK() && OB_SUCCESS != (ret = handle.get_snapshot<Key, Value>(key, read_op, snapshot, timeout_us))) {
      if (OB_SNAPSHOT_DISCARDED != ret) {
        MDS_LOG_GET(WARN, "failed to get mds data");
      } else {
        MDS_LOG_GET(TRACE, "failed to get mds data");
      }
    }
    if (CLICK_FAIL(ret)) {
      if (OB_ENTRY_NOT_EXIST == ret || OB_SNAPSHOT_DISCARDED == ret) {
        auto func = [&read_op](const Value& data) -> int {
          return read_op(data);
        };
        if (CLICK_FAIL((get_mds_data_from_tablet<Key, Value>(key, snapshot, timeout_us, func)))) {
          if (OB_SNAPSHOT_DISCARDED == ret) {
            // read nothing from tablet, maybe this is not an error
            ret = OB_EMPTY_RESULT;
            MDS_LOG_GET(DEBUG, "read nothing from mds sstable");
          } else {
            MDS_LOG_GET(WARN, "failed to get snapshot data from tablet");
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_online && MDS_FAIL(ls_switch_checker.double_check_epoch(is_online))) {
        if (!is_online) {
          ret = OB_LS_OFFLINE;
        }
        MDS_LOG_GET(WARN, "failed to double check ls online");
      } else {
        MDS_LOG_GET(TRACE, "success to get snapshot");
      }
    }
  } while (ret == OB_VERSION_NOT_MATCH && is_online);
  return ret;
  #undef PRINT_WRAPPER
}

template <typename T>
int ObITabletMdsInterface::obj_to_string_holder_(const T &obj, ObStringHolder &holder) const
{
  int ret = OB_SUCCESS;
  constexpr int64_t buffer_size = 1_KB;
  char stack_buffer[buffer_size] = { 0 };
  int64_t pos = 0;
  if (FALSE_IT(databuff_printf(stack_buffer, buffer_size, pos, "%s", to_cstring(obj)))) {// try hard to fill buffer, it's ok if buffer not enough
  } else if (OB_FAIL(holder.assign(ObString(pos, stack_buffer)))) {
    MDS_LOG(WARN, "fail to assign to holder");
  }
  return ret;
}

template <typename T>
int ObITabletMdsInterface::fill_virtual_info_from_mds_sstable(ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(mds_node_info_array), K(typeid(T).name()), KPC(dump_kv)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("vir_mds_reader");
  mds::MdsNodeInfoForVirtualTable *cur_virtual_info = nullptr;
  mds::UserMdsNode<mds::DummyKey, T> user_mds_node;
  common::ObString dummy_key;
  mds::MdsDumpKV kv;
  mds::MdsDumpKV *dump_kv = &kv;
  constexpr uint8_t mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, ObTabletBindingMdsUserData>>::value;


  if (CLICK_FAIL(mds_node_info_array.push_back(mds::MdsNodeInfoForVirtualTable()))) {
    MDS_LOG_GET(WARN, "fail to push_back");
  } else if (FALSE_IT(cur_virtual_info = &mds_node_info_array.at(mds_node_info_array.count() - 1))) {
  } else {
    if (OB_FAIL(read_raw_data(allocator, mds_unit_id, dummy_key, share::SCN::max_scn(),
        ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US/*timeout_us*/, kv))) {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        MDS_LOG(DEBUG, "read nothing from mds sstable", K(ret));
      } else {
        MDS_LOG(WARN, "fail to read raw data", K(ret));
      }
    } else if (CLICK_FAIL(obj_to_string_holder_(dump_kv->k_, cur_virtual_info->user_key_))) {
      MDS_LOG_GET(WARN, "fail to fill string holder");
    } else if (CLICK_FAIL(dump_kv->v_.convert_to_user_mds_node(user_mds_node, get_tablet_meta_().ls_id_, get_tablet_meta_().tablet_id_))) {
      MDS_LOG_GET(WARN, "fail to convert tablet_status_node");
    } else if (CLICK_FAIL(user_mds_node.fill_virtual_info(*cur_virtual_info))) {
      MDS_LOG_GET(WARN, "fail to fill virtual info");
    } else {
      cur_virtual_info->ls_id_ = get_tablet_meta_().ls_id_;
      cur_virtual_info->tablet_id_ = get_tablet_meta_().tablet_id_;
      cur_virtual_info->position_ = mds::NodePosition::DISK;
      cur_virtual_info->unit_id_ = dump_kv->v_.mds_unit_id_;
    }
    if (OB_FAIL(ret)) {
      mds_node_info_array.pop_back();
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    }

    ObTabletObjLoadHelper::free(allocator, dump_kv);
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename T>
int ObITabletMdsInterface::fill_virtual_info_by_obj_(const T &obj,
                                                     const mds::NodePosition position,
                                                     ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(mds_node_info_array)
  MDS_TG(10_ms);
  mds::MdsNodeInfoForVirtualTable *cur_virtual_info = nullptr;
  int ret = OB_SUCCESS;
  if (!obj.is_valid()) {
    MDS_LOG_GET(INFO, "obj is not valid");
  } else if (CLICK_FAIL(mds_node_info_array.push_back(mds::MdsNodeInfoForVirtualTable()))) {
    MDS_LOG_GET(WARN, "fail to push_back");
  } else if (FALSE_IT(cur_virtual_info = &mds_node_info_array.at(mds_node_info_array.count() - 1))) {
  } else {
    if (CLICK_FAIL(obj_to_string_holder_(obj, cur_virtual_info->user_data_))) {
      MDS_LOG_GET(WARN, "fail to fill string holder");
    } else {
      cur_virtual_info->ls_id_ = get_tablet_meta_().ls_id_;
      cur_virtual_info->tablet_id_ = get_tablet_meta_().tablet_id_;
      cur_virtual_info->position_ = position;
      cur_virtual_info->unit_id_ = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, T>>::value;
    }
    if (CLICK_FAIL(ret)) {
      mds_node_info_array.pop_back();
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

inline int ObITabletMdsInterface::fill_virtual_info(ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(mds_node_info_array)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;

  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "mds_reader", ObCtxIds::DEFAULT_CTX_ID));
  share::ObTabletAutoincSeq seq_on_tablet;
  if (CLICK_FAIL((get_mds_data_from_tablet<mds::DummyKey, share::ObTabletAutoincSeq>(
      mds::DummyKey(),
      share::SCN::max_scn()/*snapshot*/,
      ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US/*timeout_us*/,
      [&allocator, &seq_on_tablet](const share::ObTabletAutoincSeq &seq) {
        return seq_on_tablet.assign(allocator, seq);
      })))) {
    if (OB_SNAPSHOT_DISCARDED == ret) {
      ret = OB_SUCCESS;
    }
  } else if (CLICK_FAIL(fill_virtual_info_by_obj_(seq_on_tablet, mds::NodePosition::DISK, mds_node_info_array))) {
    MDS_LOG_GET(WARN, "fail to fill seq from disk");
  } else if (CLICK_FAIL(fill_virtual_info_by_obj_(get_tablet_meta_().last_persisted_committed_tablet_status_, mds::NodePosition::TABLET, mds_node_info_array))) {
    MDS_LOG_GET(WARN, "fail to fill tablet_status_ from cache");
  } else if (CLICK_FAIL(fill_virtual_info_from_mds_sstable<ObTabletCreateDeleteMdsUserData>(mds_node_info_array))) {
    MDS_LOG_GET(WARN, "fail to fill tablet_status_");
  } else if (CLICK_FAIL(fill_virtual_info_from_mds_sstable<ObTabletBindingMdsUserData>(mds_node_info_array))) {
    MDS_LOG_GET(WARN, "fail to fill aux_tablet_info_");
  }

  return ret;
  #undef PRINT_WRAPPER
}

template <typename T, typename OP>
int ObITabletMdsInterface::cross_ls_get_latest(const ObITabletMdsInterface *another, OP &&read_op, bool &is_committed) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL((get_latest<T, OP>(std::forward<OP>(read_op), is_committed)))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "fail to get latest", K(ret));
    }
  }

  if (OB_EMPTY_RESULT == ret && nullptr != another) {
    if (OB_FAIL((another->get_latest<T, OP>(std::forward<OP>(read_op), is_committed)))) {
      if (OB_EMPTY_RESULT == ret) {
        MDS_LOG(WARN, "fail to get latest", K(ret));
      }
    }
  }

  return ret;
}

template <typename Key, typename Value, typename OP>
int ObITabletMdsInterface::cross_ls_get_snapshot(const ObITabletMdsInterface *another,
                                                 const Key &key,
                                                 OP &&read_op,
                                                 const share::SCN snapshot,
                                                 const int64_t timeout_us) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL((get_snapshot<Key, Value>(key, read_op, snapshot, timeout_us)))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "fail to get latest", K(ret));
    }
  }

  if (OB_EMPTY_RESULT == ret && nullptr != another) {
    if (OB_FAIL((another->get_snapshot<Key, Value>(key, read_op, snapshot, timeout_us)))) {
      if (OB_EMPTY_RESULT != ret) {
        MDS_LOG(WARN, "fail to get snapshot", K(ret));
      }
    }
  }

  return ret;
}

/********************************************this is special logic*****************************************************/
inline int ObITabletMdsInterface::check_transfer_in_redo_written(bool &written)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(written)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  bool is_online = false;
  ObTabletCreateDeleteMdsUserData tablet_status;
  share::SCN redo_scn;
  do {
    mds::MdsTableHandle handle;
    ObLSSwitchChecker ls_switch_checker;
    if (CLICK_FAIL(get_mds_table_handle_(handle, false))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        MDS_LOG_GET(WARN, "failed to get_mds_table");
      } else {
        MDS_LOG_GET(TRACE, "failed to get_mds_table");
      }
    } else if (!handle.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_GET(WARN, "mds cannot be NULL");
    } else if (OB_ISNULL(get_tablet_pointer_())) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_GET(WARN, "tablet pointer is null", K(ret), KPC(this));
    } else if (MDS_FAIL(ls_switch_checker.check_ls_switch_state(get_tablet_pointer_()->get_ls(), is_online))) {
      MDS_LOG_GET(WARN, "check ls online state failed", K(ret), KPC(this));
    } else if (CLICK_FAIL(handle.get_tablet_status_node(GetTabletStatusNodeFromMdsTableOp(tablet_status, redo_scn)))) {
      if (OB_SNAPSHOT_DISCARDED != ret) {
        MDS_LOG_GET(WARN, "failed to get mds data");
      } else {
        MDS_LOG_GET(TRACE, "failed to get mds data");
      }
    } else {
      if (tablet_status.get_tablet_status() != ObTabletStatus::TRANSFER_IN) {
        ret = OB_STATE_NOT_MATCH;
      } else if (!redo_scn.is_valid() || redo_scn.is_max()) {
        written = false;
        MDS_LOG_GET(TRACE, "get transfer in status on mds_table, but redo scn is not valid");
      } else {
        written = true;
        MDS_LOG_GET(TRACE, "get transfer in status on mds_table, and redo scn is valid");
      }
    }
    if (CLICK_FAIL(ret)) {
      if (OB_ENTRY_NOT_EXIST == ret || OB_SNAPSHOT_DISCARDED == ret) {
        if (CLICK_FAIL((get_mds_data_from_tablet<mds::DummyKey, ObTabletCreateDeleteMdsUserData>(
            mds::DummyKey(),
            share::SCN::max_scn()/*snapshot*/,
            ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US/*timeout_us*/,
            ReadTabletStatusOp(tablet_status))))) {
          MDS_LOG_GET(WARN, "failed to get latest data from tablet");
        } else if (tablet_status.get_tablet_status() != ObTabletStatus::TRANSFER_IN) {
          ret = OB_STATE_NOT_MATCH;
        } else {
          written = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_online && MDS_FAIL(ls_switch_checker.double_check_epoch(is_online))) {
        if (!is_online) {
          ret = OB_LS_OFFLINE;
        }
        MDS_LOG_GET(WARN, "failed to double check ls online");
      } else {
        MDS_LOG_GET(TRACE, "success to check_transfer_in_redo_written");
      }
    }
  } while (ret == OB_VERSION_NOT_MATCH && is_online);
  return ret;
  #undef PRINT_WRAPPER
}

}
}
