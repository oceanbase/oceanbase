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

inline int ObITabletMdsInterface::check_tablet_status_written(bool &written) const
{
  int ret = OB_SUCCESS;
  written = false;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG(WARN, "not inited", K(ret), KPC(this));
  } else {
    written = get_tablet_pointer_()->is_tablet_status_written();
  }
  return ret;
}

/**********************************IMPLEMENTATION WITH TEMPLATE************************************/

template <typename K, typename V>
int ObITabletMdsInterface::read_data_from_tablet_cache(const K &key,
                                                       const common::ObFunction<int(const V&)> &read_op,
                                                       bool &applied_success) const
{
  static_assert(!std::is_same<typename std::decay<V>::type, ObTabletCreateDeleteMdsUserData>::value,
                "tablet status should read from real cache");
  int ret = OB_SUCCESS;
  applied_success = false;
  return ret;
}

template <>
inline int ObITabletMdsInterface::read_data_from_tablet_cache
           <mds::DummyKey, ObTabletCreateDeleteMdsUserData>(const mds::DummyKey &key,
                                                            const common::ObFunction<int(const ObTabletCreateDeleteMdsUserData&)> &read_op,
                                                            bool &applied_success) const
{
  int ret = OB_SUCCESS;
  const ObTabletCreateDeleteMdsUserData &last_persisted_committed_tablet_status = get_tablet_meta_().last_persisted_committed_tablet_status_;
  if (last_persisted_committed_tablet_status.is_valid() && ObTabletStatus::NONE != last_persisted_committed_tablet_status.get_tablet_status()) {
    applied_success = true;
    if (OB_FAIL(read_op(last_persisted_committed_tablet_status))) {
      MDS_LOG(WARN, "failed to do read op", KR(ret), K(last_persisted_committed_tablet_status));
    }
  } else {
    applied_success = false;
  }
  return ret;
}

template <typename K, typename V>
int ObITabletMdsInterface::read_data_from_cache_or_mds_sstable(common::ObIAllocator &allocator,
                                                               const K &key,
                                                               const share::SCN &snapshot,
                                                               const int64_t timeout_us,
                                                               const common::ObFunction<int(const V&)> &read_op) const
{
  int ret = OB_SUCCESS;
  bool applied_success = false;
  if (OB_SUCCESS != (ret = read_data_from_tablet_cache<K, V>(key, read_op, applied_success))) {
    MDS_LOG(WARN, "failed to read mds data from cache", KR(ret), K(key), K(snapshot), K(timeout_us));
  } else if (!applied_success) {
    if (OB_FAIL(read_data_from_mds_sstable(allocator, key, snapshot, timeout_us, read_op))) {
      if (OB_ITER_END == ret) {
        ret = OB_SNAPSHOT_DISCARDED;
        MDS_LOG(DEBUG, "read nothing from mds sstable", K(ret));
      } else {
        MDS_LOG(WARN, "fail to read data from mds sstable", K(ret));
      }
    }
  }
  return ret;
}

template <typename K, typename T>
int ObITabletMdsInterface::read_data_from_mds_sstable(common::ObIAllocator &allocator,
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

template <typename K, typename V>
inline int ObITabletMdsInterface::get_mds_data_from_tablet(
    const K &key,
    const share::SCN &snapshot,
    const int64_t timeout_us,
    const common::ObFunction<int(const V&)> &read_op) const
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "mds_reader", ObCtxIds::DEFAULT_CTX_ID));

  if (!mds::get_multi_version_flag<K, V>() &&
      !std::is_same<V, ObTabletCreateDeleteMdsUserData>::value &&
      !snapshot.is_max()) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "invalid args, snapshot is not max scn", K(ret), K(snapshot));
  } else if (CLICK_FAIL((read_data_from_cache_or_mds_sstable<K, V>(
      allocator, key, snapshot, timeout_us, read_op)))) {
    if (OB_ITER_END == ret) {
      ret = OB_SNAPSHOT_DISCARDED;
      MDS_LOG(DEBUG, "read nothing from mds sstable", K(ret));
    } else if (OB_EMPTY_RESULT == ret) {
      // skip report warn log
    } else {
      MDS_LOG(WARN, "fail to read data from mds sstable", K(ret));
    }
  } else {
    MDS_LOG(DEBUG, "succeed to read auto inc seq", K(ret));
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
  TabletMdsLockGuard<LockMode::SHARE> guard;
  if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(ERROR, "pointer on tablet should not be null");
  } else {
    get_tablet_pointer_()->get_mds_truncate_lock_guard(guard);
    if (MDS_FAIL(get_mds_table_handle_(handle, true))) {
      MDS_LOG_SET(WARN, "failed to get_mds_table");
    } else if (!handle.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_SET(WARN, "mds cannot be NULL");
    } else if (MDS_FAIL(handle.set(std::forward<T>(data), ctx, lock_timeout_us))) {
      MDS_LOG_SET(WARN, "failed to set dummy key unit data");
    } else if (std::is_same<ObTabletCreateDeleteMdsUserData, typename std::decay<T>::type>::value) {
      get_tablet_pointer_()->set_tablet_status_written();
    }
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
  TabletMdsLockGuard<LockMode::SHARE> guard;
  if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(ERROR, "pointer on tablet should not be null");
  } else {
    get_tablet_pointer_()->get_mds_truncate_lock_guard(guard);
    if (scn < get_tablet_meta_().mds_checkpoint_scn_) {
      MDS_LOG_SET(TRACE, "no need do replay");
    } else {
      mds::MdsTableHandle handle;
      if (CLICK_FAIL(get_mds_table_handle_(handle, true))) {
        MDS_LOG_SET(WARN, "failed to get_mds_table");
      } else if (!handle.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        MDS_LOG_SET(WARN, "mds cannot be NULL");
      } else if (CLICK_FAIL(handle.replay(std::forward<T>(data), ctx, scn))) {
        MDS_LOG_SET(WARN, "failed to replay dummy key unit data");
      } else if (std::is_same<ObTabletCreateDeleteMdsUserData, typename std::decay<T>::type>::value) {
        get_tablet_pointer_()->set_tablet_status_written();
      }
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
  TabletMdsLockGuard<LockMode::SHARE> guard;
  if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(ERROR, "pointer on tablet should not be null");
  } else {
    get_tablet_pointer_()->get_mds_truncate_lock_guard(guard);
    if (CLICK_FAIL(get_mds_table_handle_(handle, true))) {
      MDS_LOG_SET(WARN, "failed to get_mds_table");
    } else if (!handle.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_SET(WARN, "mds cannot be NULL");
    } else if (CLICK_FAIL(handle.set(key, std::forward<Value>(data), ctx, lock_timeout_us))) {
      MDS_LOG_SET(WARN, "failed to set multi key unit data");
    } else {
      get_tablet_pointer_()->set_tablet_status_written();
      MDS_LOG_SET(TRACE, "success to set multi key unit data");
    }
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
  TabletMdsLockGuard<LockMode::SHARE> guard;
  if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(ERROR, "pointer on tablet should not be null");
  } else {
    get_tablet_pointer_()->get_mds_truncate_lock_guard(guard);
    if (scn < get_tablet_meta_().mds_checkpoint_scn_) {
      MDS_LOG_SET(TRACE, "no need do replay");
    } else {
      mds::MdsTableHandle handle;
      if (CLICK_FAIL(get_mds_table_handle_(handle, true))) {
        MDS_LOG_SET(WARN, "failed to get_mds_table");
      } else if (!handle.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        MDS_LOG_SET(WARN, "mds cannot be NULL");
      } else if (CLICK_FAIL(handle.replay(key, std::forward<Value>(mds), ctx, scn))) {
        MDS_LOG_SET(WARN, "failed to replay multi key unit data");
      } else {
        get_tablet_pointer_()->set_tablet_status_written();
        MDS_LOG_SET(TRACE, "success to replay multi key unit data");
      }
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
  TabletMdsLockGuard<LockMode::SHARE> guard;
  if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(ERROR, "pointer on tablet should not be null");
  } else {
    get_tablet_pointer_()->get_mds_truncate_lock_guard(guard);
    if (CLICK_FAIL(get_mds_table_handle_(handle, true))) {
      MDS_LOG_SET(WARN, "failed to get_mds_table");
    } else if (!handle.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_SET(WARN, "mds cannot be NULL");
    } else if (CLICK_FAIL(handle.remove(key, ctx, lock_timeout_us))) {
      MDS_LOG_SET(WARN, "failed to remove multi key unit data");
    } else {
      get_tablet_pointer_()->set_tablet_status_written();
      MDS_LOG_SET(TRACE, "success to remove multi key unit data");
    }
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
  TabletMdsLockGuard<LockMode::SHARE> guard;
  if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(ERROR, "pointer on tablet should not be null");
  } else {
    get_tablet_pointer_()->get_mds_truncate_lock_guard(guard);
    if (CLICK_FAIL(get_mds_table_handle_(handle, true))) {
      MDS_LOG_SET(WARN, "failed to get_mds_table");
    } else if (!handle.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_SET(WARN, "mds cannot be NULL");
    } else if (CLICK() && OB_SUCCESS != (ret = handle.replay_remove<Key, Value>(key, ctx, scn))) {
      MDS_LOG_SET(WARN, "failed to replay remove multi key unit data");
    } else {
      get_tablet_pointer_()->set_tablet_status_written();
      MDS_LOG_SET(TRACE, "success to remove multi key unit data");
    }
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
  TabletMdsLockGuard<LockMode::SHARE> guard;
  if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(ERROR, "pointer on tablet should not be null");
  } else {
    get_tablet_pointer_()->get_mds_truncate_lock_guard(guard);
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
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename T, typename OP, typename std::enable_if<OB_TRAIT_IS_FUNCTION_LIKE(OP, int(const T&)), bool>::type>
int ObITabletMdsInterface::get_latest(OP &&read_op,
                                      mds::MdsWriter &writer,// FIXME(xuwang.txw): should not exposed, will be removed later
                                      mds::TwoPhaseCommitState &trans_stat,// FIXME(xuwang.txw): should not exposed, will be removed later
                                      share::SCN &trans_version,// FIXME(xuwang.txw): should not exposed, will be removed later
                                      const int64_t read_seq) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(typeid(OP).name())
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  bool is_online = false;
  bool is_data_complete = false;
  TabletMdsLockGuard<LockMode::SHARE> guard;
  if (OB_FAIL(check_mds_data_complete_<T>(is_data_complete))) {
    MDS_LOG(WARN, "failed to check data completion");
  } else if (!is_data_complete) {
    ret = OB_EAGAIN;
    MDS_LOG(INFO, "mds_data is not complete, try again later", K(ret), K(get_tablet_meta_().ha_status_));
  } else if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(ERROR, "pointer on tablet should not be null");
  } else {
    get_tablet_pointer_()->get_mds_truncate_lock_guard(guard);
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
      } else if (MDS_FAIL(ls_switch_checker.check_ls_switch_state(get_tablet_pointer_()->get_ls(), is_online))) {
        MDS_LOG_GET(WARN, "check ls online state failed", K(ret), KPC(this));
      } else if (CLICK_FAIL(handle.get_latest<T>(read_op, writer, trans_stat, trans_version, read_seq))) {
        if (OB_SNAPSHOT_DISCARDED != ret) {
          MDS_LOG_GET(WARN, "failed to get mds data");
        } else {
          MDS_LOG_GET(TRACE, "failed to get mds data");
        }
      }
      if (CLICK_FAIL(ret)) {
        if (OB_ENTRY_NOT_EXIST == ret || OB_SNAPSHOT_DISCARDED == ret) {
          auto func = [&read_op, &writer, &trans_stat, &trans_version](const T& data) -> int {
            writer.reset();
            trans_stat = mds::TwoPhaseCommitState::ON_COMMIT;// FIXME: here need more judge after support dump uncommitted node
            trans_version.reset();
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
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename T, typename OP, typename std::enable_if<OB_TRAIT_IS_FUNCTION_LIKE(OP, int(const T&)), bool>::type>
int ObITabletMdsInterface::get_latest_committed(OP &&read_op) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(typeid(OP).name())
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  mds::MdsTableHandle handle;
  ObLSSwitchChecker ls_switch_checker;
  bool is_online = false;
  bool is_data_complete = false;
  TabletMdsLockGuard<LockMode::SHARE> guard;
  if (OB_FAIL(check_mds_data_complete_<T>(is_data_complete))) {
    MDS_LOG(WARN, "failed to check data completion");
  } else if (!is_data_complete) {
    ret = OB_EAGAIN;
    MDS_LOG(INFO, "mds_data is not complete, try again later", K(ret), K(get_tablet_meta_().ha_status_));
  } else if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(ERROR, "pointer on tablet should not be null");
  } else {
    get_tablet_pointer_()->get_mds_truncate_lock_guard(guard);
    do {
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
      } else if (CLICK_FAIL(handle.get_latest_committed<T>(read_op))) {
        if (OB_SNAPSHOT_DISCARDED != ret) {
          MDS_LOG_GET(WARN, "failed to get mds data");
        } else {
          MDS_LOG_GET(TRACE, "failed to get mds data");
        }
      }
      if (CLICK_FAIL(ret)) {
        if (OB_ENTRY_NOT_EXIST == ret || OB_SNAPSHOT_DISCARDED == ret) {
          auto func = [&read_op](const T& data) -> int {
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
              MDS_LOG_GET(WARN, "failed to get latest committed data from tablet");
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (is_online && MDS_FAIL(ls_switch_checker.double_check_epoch(is_online))) {
          MDS_LOG_GET(WARN, "failed to double check ls online");
        } else {
          MDS_LOG_GET(TRACE, "success to get_latest_committed");
        }
      }
    } while (ret == OB_VERSION_NOT_MATCH && is_online);
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename T, typename OP, typename std::enable_if<OB_TRAIT_IS_FUNCTION_LIKE(OP, int(const T&)), bool>::type>
int ObITabletMdsInterface::get_snapshot(OP &&read_op,
                                        const share::SCN snapshot,
                                        const int64_t timeout_us) const
{
  return get_snapshot<mds::DummyKey, T>(mds::DummyKey(), read_op, snapshot, timeout_us);
}

template <typename Key, typename Value, typename OP>
int ObITabletMdsInterface::get_snapshot(const Key &key,
                                        OP &&read_op,
                                        const share::SCN snapshot,
                                        const int64_t timeout_us) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(snapshot), K(typeid(OP).name())
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  bool is_online = false;
  TabletMdsLockGuard<LockMode::SHARE> guard;
  bool is_data_complete = false;
  if (OB_FAIL(check_mds_data_complete_<Value>(is_data_complete))) {
    MDS_LOG(WARN, "failed to check data completion");
  } else if (!is_data_complete) {
    ret = OB_EAGAIN;
    MDS_LOG(INFO, "mds_data is not complete, try again later", K(ret), K(get_tablet_meta_().ha_status_));
  } else if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(ERROR, "pointer on tablet should not be null");
  } else {
    get_tablet_pointer_()->get_mds_truncate_lock_guard(guard);
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
  }
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
  if (FALSE_IT(databuff_printf(stack_buffer, buffer_size, pos, obj))) {// try hard to fill buffer, it's ok if buffer not enough
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
  constexpr uint8_t mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, T>>::value;


  if (CLICK_FAIL(mds_node_info_array.push_back(mds::MdsNodeInfoForVirtualTable()))) {
    MDS_LOG_GET(WARN, "fail to push_back");
  } else if (FALSE_IT(cur_virtual_info = &mds_node_info_array.at(mds_node_info_array.count() - 1))) {
  } else {
    if (OB_FAIL(read_raw_data(allocator, mds_unit_id, dummy_key, share::SCN::max_scn(),
        ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US/*timeout_us*/, kv))) {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        MDS_LOG(TRACE, "read nothing from mds sstable", K(ret));
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
      MDS_LOG_GET(TRACE, "success to fill virtual info");
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
  if (CLICK_FAIL(fill_virtual_info_by_obj_(get_tablet_meta_().last_persisted_committed_tablet_status_,
                                           mds::NodePosition::TABLET,
                                           mds_node_info_array))) {
    MDS_LOG_GET(WARN, "fail to fill tablet_status_ from cache");
  } else if (CLICK_FAIL(fill_virtual_info_from_mds_sstable<ObTabletCreateDeleteMdsUserData>(mds_node_info_array))) {
    MDS_LOG_GET(WARN, "fail to fill tablet_status_");
  } else if (CLICK_FAIL(fill_virtual_info_from_mds_sstable<share::ObTabletAutoincSeq>(mds_node_info_array))) {
    MDS_LOG_GET(WARN, "fail to fill seq from disk");
  } else if (CLICK_FAIL(fill_virtual_info_from_mds_sstable<ObTabletBindingMdsUserData>(mds_node_info_array))) {
    MDS_LOG_GET(WARN, "fail to fill aux_tablet_info_");
  } else if (CLICK_FAIL(fill_virtual_info_from_mds_sstable<ObTabletSplitMdsUserData>(mds_node_info_array))) {
    MDS_LOG_GET(WARN, "fail to fill tablet_split_data_");
  } else {
    MDS_LOG_GET(TRACE, "sucess to fill virtual info");
  }

  return ret;
  #undef PRINT_WRAPPER
}

template <typename T>
int ObITabletMdsInterface::get_latest_committed_data(T &value, ObIAllocator *alloc)
{
  #define PRINT_WRAPPER KR(ret), K(value)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else {
    const ObTabletMeta &tablet_meta = get_tablet_meta_();
    const bool has_transfer_table = tablet_meta.has_transfer_table();
    ObITabletMdsInterface *src = nullptr;
    ObTabletHandle src_tablet_handle;
    if (has_transfer_table) {
      const share::ObLSID &src_ls_id = tablet_meta.transfer_info_.ls_id_;
      const common::ObTabletID &tablet_id = tablet_meta.tablet_id_;
      if (CLICK_FAIL(get_tablet_handle_and_base_ptr(src_ls_id, tablet_id, src_tablet_handle, src))) {
        MDS_LOG(WARN, "fail to get src tablet handle", K(ret), K(src_ls_id), K(tablet_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (CLICK_FAIL((cross_ls_get_latest_committed(
        src,
        value,
        alloc)))) {
      if (OB_EMPTY_RESULT != ret) {
        MDS_LOG_GET(WARN, "fail to cross ls get latest", K(lbt()));
      }
    } else if (!value.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_GET(WARN, "invalid user data", K(lbt()));
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}


template <typename T, typename OP>
int ObITabletMdsInterface::cross_ls_get_latest(
    const ObITabletMdsInterface *another,
    OP &&read_op,
    mds::MdsWriter &writer,
    mds::TwoPhaseCommitState &trans_stat,
    share::SCN &trans_version,
    const int64_t read_seq) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL((get_latest<T, OP>(std::forward<OP>(read_op), writer, trans_stat, trans_version, read_seq)))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "fail to get latest", K(ret));
    }
  }

  if (OB_EMPTY_RESULT == ret && nullptr != another) {
    if (OB_FAIL((another->get_latest<T, OP>(std::forward<OP>(read_op), writer, trans_stat, trans_version, read_seq)))) {
      if (OB_EMPTY_RESULT != ret) {
        MDS_LOG(WARN, "fail to get latest", K(ret));
      }
    }
  }

  return ret;
}

template <typename T>
int ObITabletMdsInterface::cross_ls_get_latest_committed(
    const ObITabletMdsInterface *another,
    T &value,
    ObIAllocator *alloc) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL((get_latest_committed(value, alloc)))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "fail to get latest", K(ret));
    }
  }

  if (OB_EMPTY_RESULT == ret && nullptr != another) {
    if (OB_FAIL((another->get_latest_committed(value, alloc)))) {
      if (OB_EMPTY_RESULT != ret) {
        MDS_LOG(WARN, "fail to get latest", K(ret));
      }
    }
  }

  return ret;
}

template <typename Key, typename Value, typename OP>
int ObITabletMdsInterface::cross_ls_get_snapshot(
    const ObITabletMdsInterface *another,
    const Key &key,
    OP &&read_op,
    const share::SCN snapshot,
    const int64_t timeout_us) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL((get_snapshot<Key, Value>(key, read_op, snapshot, timeout_us)))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "fail to get snapshot", K(ret));
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

template <class T, typename std::enable_if<!OB_TRAIT_IS_SAME_CLASS(T, ObTabletCreateDeleteMdsUserData), bool>::type>
int ObITabletMdsInterface::check_mds_data_complete_(bool &is_complete) const
{
  int ret = OB_SUCCESS;
  // For multi-source data (excluding tablet_status), during the migration of LS,
  // reading is only permitted once the data is complete;
  // otherwise, the data read will be incomplete.
  is_complete = get_tablet_meta_().ha_status_.is_data_status_complete();
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
  TabletMdsLockGuard<LockMode::SHARE> guard;
  if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(ERROR, "pointer on tablet should not be null");
  } else {
    get_tablet_pointer_()->get_mds_truncate_lock_guard(guard);
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
  }
  return ret;
  #undef PRINT_WRAPPER
}
}
}
