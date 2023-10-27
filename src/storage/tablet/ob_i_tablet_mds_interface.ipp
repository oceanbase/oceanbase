#ifndef INCLUDE_OB_TABLET_MDS_PART_IPP
#define INCLUDE_OB_TABLET_MDS_PART_IPP
#include "ob_i_tablet_mds_interface.h"
#endif
namespace oceanbase
{
namespace storage
{

/********************************IMPLEMENTATION WITHOUT TEMPLATE***********************************/

inline common::ObTabletID ObITabletMdsInterface::get_table_id_() const
{
  return get_tablet_meta_().tablet_id_;
}

inline int ObITabletMdsInterface::get_tablet_status(const share::SCN &snapshot,
                                                    ObTabletCreateDeleteMdsUserData &data,
                                                    const int64_t timeout) const
{
  #define PRINT_WRAPPER KR(ret), K(snapshot), K(data), K(timeout), K(*this)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (OB_UNLIKELY(!snapshot.is_max())) {
    ret = OB_NOT_SUPPORTED;
    MDS_LOG_GET(WARN, "only support read latest data currently");
  } else if (CLICK_FAIL(get_snapshot<ObTabletCreateDeleteMdsUserData>(
    [&data](const ObTabletCreateDeleteMdsUserData &user_data) -> int {
      return data.assign(user_data);
    }, snapshot, 0, timeout))) {
    MDS_LOG_GET(WARN, "tablet_status does not exist on neither mds_table nor tablet", K(lbt()));
  } else if (!data.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_GET(WARN, "invalid user data", K(lbt()));
  }
  return ret;
  #undef PRINT_WRAPPER
}

inline int ObITabletMdsInterface::get_latest_tablet_status(ObTabletCreateDeleteMdsUserData &data, bool &is_committed) const
{
  #define PRINT_WRAPPER KR(ret), K(data)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (CLICK_FAIL(get_latest<ObTabletCreateDeleteMdsUserData>(
    [&data](const ObTabletCreateDeleteMdsUserData &user_data) -> int {
      return data.assign(user_data);
    }, is_committed, 0))) {
    MDS_LOG_GET(WARN, "fail to get_latest_tablet_status");
  } else if (!data.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_GET(WARN, "invalid user data", K(lbt()));
  }
  return ret;
  #undef PRINT_WRAPPER
}

inline int ObITabletMdsInterface::get_ddl_data(const share::SCN &snapshot,
                                               ObTabletBindingMdsUserData &data,
                                               const int64_t timeout) const
{
  #define PRINT_WRAPPER KR(ret), K(data), K(snapshot), K(timeout)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (OB_UNLIKELY(!snapshot.is_max())) {
    ret = OB_NOT_SUPPORTED;
    MDS_LOG_GET(WARN, "only support read latest data currently");
  } else if (CLICK_FAIL(get_snapshot<ObTabletBindingMdsUserData>(
      [&data](const ObTabletBindingMdsUserData &user_data) -> int { return data.assign(user_data); },
      snapshot, 0, timeout))) {
    if (OB_EMPTY_RESULT == ret) {
      data.set_default_value(); // use default value
      ret = OB_SUCCESS;
    } else {
      MDS_LOG_GET(WARN, "fail to get ddl data");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

inline int ObITabletMdsInterface::get_autoinc_seq(ObIAllocator &allocator,
                                                  const share::SCN &snapshot,
                                                  share::ObTabletAutoincSeq &data,
                                                  const int64_t timeout) const
{
  #define PRINT_WRAPPER KR(ret), K(data), K(snapshot), K(timeout)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (OB_UNLIKELY(!snapshot.is_max())) {
    ret = OB_NOT_SUPPORTED;
    MDS_LOG_GET(WARN, "only support read latest data currently");
  } else if (CLICK_FAIL(get_snapshot<share::ObTabletAutoincSeq>(
    [&allocator, &data](const share::ObTabletAutoincSeq &user_data) -> int {
      return data.assign(allocator, user_data);
    }, snapshot, 0, timeout))) {
    if (OB_EMPTY_RESULT == ret) {
      data.reset(); // use default value
      ret = OB_SUCCESS;
    } else {
      MDS_LOG_GET(WARN, "fail to get_autoinc_seq");
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}

inline int ObITabletMdsInterface::check_tablet_status_written(bool &written)
{
  int ret = OB_SUCCESS;
  written = false;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG(WARN, "not inited", K(ret), KPC(this));
  } else if (OB_ISNULL(get_tablet_ponter_())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(ERROR, "tablet pointer is null", K(ret), KPC(this));
  } else {
    written = get_tablet_ponter_()->is_tablet_status_written();
  }
  return ret;
}

/**********************************IMPLEMENTATION WITH TEMPLATE************************************/

template <>
inline int ObITabletMdsInterface::get_mds_data_from_tablet<ObTabletCreateDeleteMdsUserData>(
    const common::ObFunction<int(const ObTabletCreateDeleteMdsUserData&)> &read_op) const
{
  #define PRINT_WRAPPER KR(ret), K(data), KPC(kv), K(*this)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData data;
  const mds::MdsDumpKV *kv = nullptr;
  ObArenaAllocator allocator("mds_reader");
  const ObTabletComplexAddr<mds::MdsDumpKV> &tablet_status_addr = get_mds_data_().tablet_status_.committed_kv_;
  const ObTabletCreateDeleteMdsUserData &tablet_status_cache = get_mds_data_().tablet_status_cache_;

  // TODO(@chenqingxiang.cqx): remove read from IO after cache ready
  if (tablet_status_cache.is_valid()) {
    if (CLICK_FAIL(read_op(tablet_status_cache))) {
      MDS_LOG_GET(WARN, "failed to do read op", K(tablet_status_cache));
    }
  } else {
    // for debug perf issue
    if (tablet_status_addr.is_disk_object()) {
      MDS_LOG_GET(ERROR, "tablet status addr is disk, but cache is invalid", K(tablet_status_addr), K(tablet_status_cache));
    }

    if (CLICK_FAIL(ObTabletMdsData::load_mds_dump_kv(allocator, tablet_status_addr, kv))) {
      MDS_LOG_GET(WARN, "failed to load mds dump kv");
    } else if (nullptr == kv) {
      ret = OB_EMPTY_RESULT;
    } else {
      const common::ObString &user_data = kv->v_.user_data_;
      int64_t pos = 0;
      if (user_data.empty()) {
        ret = OB_EMPTY_RESULT;
      } else if (CLICK_FAIL(data.deserialize(user_data.ptr(), user_data.length(), pos))) {
        MDS_LOG_GET(WARN, "failed to deserialize", K(user_data),
                    "user_data_length", user_data.length(),
                    "user_hash:%x", user_data.hash(),
                    "crc_check_number", kv->v_.crc_check_number_);
      } else if (CLICK_FAIL(read_op(data))) {
        MDS_LOG_GET(WARN, "failed to do read op");
      }
    }

    ObTabletMdsData::free_mds_dump_kv(allocator, kv);
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <>
inline int ObITabletMdsInterface::get_mds_data_from_tablet<ObTabletBindingMdsUserData>(
    const common::ObFunction<int(const ObTabletBindingMdsUserData&)> &read_op) const
{
  #define PRINT_WRAPPER KR(ret), K(data), KPC(kv), K(*this)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObTabletBindingMdsUserData data;
  const mds::MdsDumpKV *kv = nullptr;
  ObArenaAllocator allocator("mds_reader");
  const ObTabletComplexAddr<mds::MdsDumpKV> &aux_tablet_info_addr = get_mds_data_().aux_tablet_info_.committed_kv_;
  const ObTabletBindingMdsUserData &aux_tablet_info_cache = get_mds_data_().aux_tablet_info_cache_;

  if (aux_tablet_info_addr.is_memory_object()) {
    if (CLICK_FAIL(read_op(aux_tablet_info_cache))) {
      MDS_LOG_GET(WARN, "failed to read_op");
    }
  } else if (aux_tablet_info_addr.is_none_object()) {
    ret = OB_EMPTY_RESULT;
  } else if (aux_tablet_info_addr.is_disk_object()) {
    if (aux_tablet_info_cache.is_valid()) {
      if (CLICK_FAIL(read_op(aux_tablet_info_cache))) {
        MDS_LOG_GET(WARN, "failed to read_op");
      }
    } else {
      if (CLICK_FAIL(ObTabletMdsData::load_mds_dump_kv(allocator, aux_tablet_info_addr, kv))) {
        MDS_LOG_GET(WARN, "failed to load mds dump kv");
      } else if (nullptr == kv) {
        ret = OB_EMPTY_RESULT;
      } else {
        const common::ObString &user_data = kv->v_.user_data_;
        int64_t pos = 0;
        if (user_data.empty()) {
          ret = OB_EMPTY_RESULT;
        } else if (CLICK_FAIL(data.deserialize(user_data.ptr(), user_data.length(), pos))) {
          MDS_LOG_GET(WARN, "failed to deserialize", K(user_data),
                      "user_data_length", user_data.length(),
                      "user_hash:%x", user_data.hash(),
                      "crc_check_number", kv->v_.crc_check_number_);
        } else if (CLICK_FAIL(read_op(data))) {
          MDS_LOG_GET(WARN, "failed to read_op");
        }
      }

      ObTabletMdsData::free_mds_dump_kv(allocator, kv);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_GET(WARN, "unexpected addr", K(aux_tablet_info_addr));
  }

  return ret;
  #undef PRINT_WRAPPER
}

template <>
inline int ObITabletMdsInterface::get_mds_data_from_tablet<share::ObTabletAutoincSeq>(
    const common::ObFunction<int(const share::ObTabletAutoincSeq&)> &read_op) const
{
  #define PRINT_WRAPPER KR(ret), K(auto_inc_seq_addr), K(*this)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  const ObTabletComplexAddr<share::ObTabletAutoincSeq> &auto_inc_seq_addr = get_mds_data_().auto_inc_seq_;
  const share::ObTabletAutoincSeq *auto_inc_seq = nullptr;
  ObArenaAllocator allocator("mds_reader");

  if (CLICK_FAIL(ObTabletMdsData::load_auto_inc_seq(allocator, auto_inc_seq_addr, auto_inc_seq))) {
    MDS_LOG_GET(WARN, "failed to load auto inc seq");
  } else if (nullptr == auto_inc_seq) {
    ret = OB_EMPTY_RESULT;
  } else if (CLICK_FAIL(read_op(*auto_inc_seq))) {
    MDS_LOG_GET(WARN, "failed to read_op");
  }

  ObTabletMdsData::free_auto_inc_seq(allocator, auto_inc_seq);

  return ret;
  #undef PRINT_WRAPPER
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
  } else if (OB_ISNULL(get_tablet_ponter_())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_SET(WARN, "tablet pointer is null", K(ret), KPC(this));
  } else if (MDS_FAIL(handle.set(std::forward<T>(data), ctx, lock_timeout_us))) {
    MDS_LOG_SET(WARN, "failed to set dummy key unit data");
  } else if (std::is_same<ObTabletCreateDeleteMdsUserData, typename std::decay<T>::type>::value) {
    get_tablet_ponter_()->set_tablet_status_written();
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
    } else if (OB_ISNULL(get_tablet_ponter_())) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_SET(WARN, "tablet pointer is null", K(ret), KPC(this));
    } else if (CLICK_FAIL(handle.replay(std::forward<T>(data), ctx, scn))) {
      MDS_LOG_SET(WARN, "failed to replay dummy key unit data");
    } else if (std::is_same<ObTabletCreateDeleteMdsUserData, typename std::decay<T>::type>::value) {
      get_tablet_ponter_()->set_tablet_status_written();
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
  } else if (OB_ISNULL(get_tablet_ponter_())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_SET(WARN, "tablet pointer is null", K(ret), KPC(this));
  } else if (CLICK_FAIL(handle.set(key, std::forward<Value>(data), ctx, lock_timeout_us))) {
    MDS_LOG_SET(WARN, "failed to set multi key unit data");
  } else {
    get_tablet_ponter_()->set_tablet_status_written();
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
    } else if (OB_ISNULL(get_tablet_ponter_())) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_SET(WARN, "tablet pointer is null", K(ret), KPC(this));
    } else if (CLICK_FAIL(handle.replay(key, std::forward<Value>(mds), ctx, scn))) {
      MDS_LOG_SET(WARN, "failed to replay multi key unit data");
    } else {
      get_tablet_ponter_()->set_tablet_status_written();
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
  ObLSSwitchChecker ls_switch_checker;
  if (CLICK_FAIL(get_mds_table_handle_(handle, true))) {
    MDS_LOG_SET(WARN, "failed to get_mds_table");
  } else if (!handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_SET(WARN, "mds cannot be NULL");
  } else if (OB_ISNULL(get_tablet_ponter_())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_SET(WARN, "tablet pointer is null", K(ret), KPC(this));
  } else if (CLICK_FAIL(handle.remove(key, ctx, lock_timeout_us))) {
    MDS_LOG_SET(WARN, "failed to remove multi key unit data");
  } else if (OB_ISNULL(get_tablet_ponter_())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_SET(WARN, "tablet pointer is null", K(ret), KPC(this));
  } else {
    get_tablet_ponter_()->set_tablet_status_written();
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
  ObLSSwitchChecker ls_switch_checker;
  if (CLICK_FAIL(get_mds_table_handle_(handle, true))) {
    MDS_LOG_SET(WARN, "failed to get_mds_table");
  } else if (!handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_SET(WARN, "mds cannot be NULL");
  } else if (OB_ISNULL(get_tablet_ponter_())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_SET(WARN, "tablet pointer is null", K(ret), KPC(this));
  } else if (CLICK() && OB_SUCCESS != (ret = handle.replay_remove<Key, Value>(key, ctx, scn))) {
    MDS_LOG_SET(WARN, "failed to replay remove multi key unit data");
  } else {
    get_tablet_ponter_()->set_tablet_status_written();
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
  mds::MdsTableHandle handle;
  ObLSSwitchChecker ls_switch_checker;
  bool is_online = false;
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
  } else if (OB_ISNULL(get_tablet_ponter_())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_GET(WARN, "tablet pointer is null", K(ret), KPC(this));
  } else if (MDS_FAIL(ls_switch_checker.check_ls_switch_state(get_tablet_ponter_()->get_ls(), is_online))) {
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
    if (is_online && MDS_FAIL(ls_switch_checker.double_check_epoch())) {
      MDS_LOG_GET(WARN, "failed to double check ls online");
    } else {
      MDS_LOG_GET(TRACE, "success to get is locked by others state");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename T, typename OP>
int ObITabletMdsInterface::get_latest(OP &&read_op, bool &is_committed, const int64_t read_seq) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(read_seq), K(typeid(OP).name())
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  mds::MdsTableHandle handle;
  ObLSSwitchChecker ls_switch_checker;
  bool is_online = false;
  if (CLICK_FAIL(get_mds_table_handle_(handle, false))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      MDS_LOG_GET(WARN, "failed to get_mds_table");
    } else {
      MDS_LOG_GET(TRACE, "failed to get_mds_table");
    }
  } else if (!handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_GET(WARN, "mds cannot be NULL");
  } else if (OB_ISNULL(get_tablet_ponter_())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_GET(WARN, "tablet pointer is null", K(ret), KPC(this));
  } else if (MDS_FAIL(ls_switch_checker.check_ls_switch_state(get_tablet_ponter_()->get_ls(), is_online))) {
    MDS_LOG_GET(WARN, "check ls online state failed", K(ret), KPC(this));
  } else if (CLICK_FAIL(handle.get_latest<T>(read_op, is_committed, read_seq))) {
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
      if (CLICK_FAIL(get_mds_data_from_tablet<T>(func))) {
        MDS_LOG_GET(WARN, "failed to get latest data from tablet");
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (is_online && MDS_FAIL(ls_switch_checker.double_check_epoch())) {
      MDS_LOG_GET(WARN, "failed to double check ls online");
    } else {
      MDS_LOG_GET(TRACE, "success to get_latest");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename T, typename OP>// general get for dummy key unit
int ObITabletMdsInterface::get_snapshot(OP &&read_op,
                                        const share::SCN snapshot,
                                        const int64_t read_seq,
                                        const int64_t timeout_us) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(snapshot), K(read_seq), K(typeid(OP).name())
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  mds::MdsTableHandle handle;
  ObLSSwitchChecker ls_switch_checker;
  bool is_online = false;
  if (CLICK_FAIL(get_mds_table_handle_(handle, false))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      MDS_LOG_GET(WARN, "failed to get_mds_table");
    } else {
      MDS_LOG_GET(TRACE, "failed to get_mds_table");
    }
  } else if (!handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_GET(WARN, "mds cannot be NULL");
  } else if (OB_ISNULL(get_tablet_ponter_())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_GET(WARN, "tablet pointer is null", K(ret), KPC(this));
  } else if (MDS_FAIL(ls_switch_checker.check_ls_switch_state(get_tablet_ponter_()->get_ls(), is_online))) {
    MDS_LOG_GET(WARN, "check ls online state failed", K(ret), KPC(this));
  } else if (CLICK_FAIL(handle.get_snapshot<T>(read_op, snapshot, read_seq, timeout_us))) {
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
      if (CLICK_FAIL(get_mds_data_from_tablet<T>(func))) {
        if (OB_EMPTY_RESULT == ret) {
          // read nothing from tablet, maybe this is not an error
        } else {
          MDS_LOG_GET(WARN, "failed to get snapshot data from tablet");
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (is_online && MDS_FAIL(ls_switch_checker.double_check_epoch())) {
      MDS_LOG_GET(WARN, "failed to double check ls online");
    } else {
      MDS_LOG_GET(TRACE, "success to get_snapshot");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename Key, typename Value, typename OP>// general get for multi key unit
int ObITabletMdsInterface::get_snapshot(const Key &key,
                                        OP &&read_op,
                                        const share::SCN snapshot,
                                        const int64_t read_seq,
                                        const int64_t timeout_us) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(key), K(snapshot), K(read_seq), K(typeid(OP).name())
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  mds::MdsTableHandle handle;
  ObLSSwitchChecker ls_switch_checker;
  bool is_online = false;
  if (CLICK_FAIL(get_mds_table_handle_(handle, false))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      MDS_LOG_GET(WARN, "failed to get_mds_table");
    } else {
      MDS_LOG_GET(TRACE, "failed to get_mds_table");
    }
  } else if (!handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_GET(WARN, "mds cannot be NULL");
  } else if (OB_ISNULL(get_tablet_ponter_())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_GET(WARN, "tablet pointer is null", K(ret), KPC(this));
  } else if (MDS_FAIL(ls_switch_checker.check_ls_switch_state(get_tablet_ponter_()->get_ls(), is_online))) {
    MDS_LOG_GET(WARN, "check ls online state failed", K(ret), KPC(this));
  } else if (CLICK() && OB_SUCCESS != (ret = handle.get_snapshot<Key, Value>(key, read_op, snapshot, read_seq, timeout_us))) {
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
      if (CLICK_FAIL(get_mds_data_from_tablet<Value>(func))) {
        if (OB_EMPTY_RESULT == ret) {
          // read nothing from tablet, maybe this is not an error
        } else {
          MDS_LOG_GET(WARN, "failed to get snapshot data from tablet");
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (is_online && MDS_FAIL(ls_switch_checker.double_check_epoch())) {
      MDS_LOG_GET(WARN, "failed to double check ls online");
    } else {
      MDS_LOG_GET(TRACE, "success to get_snapshot");
    }
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
  if (FALSE_IT(databuff_printf(stack_buffer, buffer_size, pos, "%s", to_cstring(obj)))) {// try hard to fill buffer, it's ok if buffer not enough
  } else if (OB_FAIL(holder.assign(ObString(pos, stack_buffer)))) {
    MDS_LOG(WARN, "fatil to assign to holder");
  }
  return ret;
}

template <typename T>
int ObITabletMdsInterface::fill_virtual_info_by_complex_addr_(const ObTabletComplexAddr<mds::MdsDumpKV> &addr,
                                                              ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(mds_node_info_array), K(typeid(T).name()), KPC(dump_kv)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("vir_mds_reader");
  mds::MdsNodeInfoForVirtualTable *cur_virtual_info = nullptr;
  mds::UserMdsNode<mds::DummyKey, T> user_mds_node;
  const mds::MdsDumpKV *dump_kv = nullptr;

  if (CLICK_FAIL(mds_node_info_array.push_back(mds::MdsNodeInfoForVirtualTable()))) {
    MDS_LOG_GET(WARN, "fail to push_back");
  } else if (FALSE_IT(cur_virtual_info = &mds_node_info_array.at(mds_node_info_array.count() - 1))) {
  } else {
    if (CLICK_FAIL(ObTabletMdsData::load_mds_dump_kv(allocator, addr, dump_kv))) {
      MDS_LOG_GET(WARN, "fatil to read tablet_status_addr");
    } else if (nullptr == dump_kv || (nullptr != dump_kv && !dump_kv->is_valid())) {
      ret = OB_ENTRY_NOT_EXIST;
      MDS_LOG_GET(INFO, "dump kv not exist");
    } else if (CLICK_FAIL(obj_to_string_holder_(dump_kv->k_, cur_virtual_info->user_key_))) {
      MDS_LOG_GET(WARN, "fatil to fill string holder");
    } else if (CLICK_FAIL(dump_kv->v_.convert_to_user_mds_node(user_mds_node, get_tablet_meta_().ls_id_, get_tablet_meta_().tablet_id_))) {
      MDS_LOG_GET(WARN, "fatil to convert tablet_status_node");
    } else if (CLICK_FAIL(user_mds_node.fill_virtual_info(*cur_virtual_info))) {
      MDS_LOG_GET(WARN, "fatil to fill virtual info");
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

    ObTabletMdsData::free_mds_dump_kv(allocator, dump_kv);
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
    MDS_LOG_GET(WARN, "fatil to push_back");
  } else if (FALSE_IT(cur_virtual_info = &mds_node_info_array.at(mds_node_info_array.count() - 1))) {
  } else {
    if (CLICK_FAIL(obj_to_string_holder_(obj, cur_virtual_info->user_data_))) {
      MDS_LOG_GET(WARN, "fatil to fill string holder");
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

  ObArenaAllocator allocator("mds_reader");
  share::ObTabletAutoincSeq seq_on_tablet;
  if (CLICK_FAIL(get_mds_data_from_tablet<share::ObTabletAutoincSeq>([&allocator, &seq_on_tablet](const share::ObTabletAutoincSeq &seq) {
    return seq_on_tablet.assign(allocator, seq);
  }))) {
    MDS_LOG_GET(WARN, "fatil to get seq from disk");
  } else if (CLICK_FAIL(fill_virtual_info_by_obj_(seq_on_tablet, mds::NodePosition::DISK, mds_node_info_array))) {
    MDS_LOG_GET(WARN, "fatil to fill seq from disk");
  } else if (CLICK_FAIL(fill_virtual_info_by_obj_(get_mds_data_().tablet_status_cache_, mds::NodePosition::TABLET, mds_node_info_array))) {
    MDS_LOG_GET(WARN, "fatil to fill tablet_status_ from cache");
  } else if (CLICK_FAIL(fill_virtual_info_by_complex_addr_<ObTabletCreateDeleteMdsUserData>(get_mds_data_().tablet_status_.committed_kv_, mds_node_info_array))) {
    MDS_LOG_GET(WARN, "fatil to fill tablet_status_");
  } else if (CLICK_FAIL(fill_virtual_info_by_complex_addr_<ObTabletBindingMdsUserData>(get_mds_data_().aux_tablet_info_.committed_kv_, mds_node_info_array))) {
    MDS_LOG_GET(WARN, "fatil to fill aux_tablet_info_");
  }

  return ret;
  #undef PRINT_WRAPPER
}

}
}
