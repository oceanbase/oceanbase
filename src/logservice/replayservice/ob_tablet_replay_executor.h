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
#ifndef OCEANBASE_LOGSERVICE_OB_TABLET_REPLAY_EXECUTOR_
#define OCEANBASE_LOGSERVICE_OB_TABLET_REPLAY_EXECUTOR_

#include <type_traits>
#include "lib/utility/ob_macro_utils.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_binding_mds_user_data.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"

namespace oceanbase
{

namespace share
{
class SCN;
class ObLSID;
}

namespace storage
{
namespace mds
{
class MdsCtx;
}
}

namespace logservice
{

// Adaptation method:
class ObTabletReplayExecutor
{
public:
  ObTabletReplayExecutor() : is_inited_(false) {}
  virtual ~ObTabletReplayExecutor() {}

  // replay on one tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_TABLET_NOT_EXIST, the tablet is not exist.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return other error codes, failed to replay.
  int execute(const share::SCN &scn, const share::ObLSID &ls_id, const common::ObTabletID &tablet_id);

  // check restore status before replay
  // @return OB_SUCCESS, need replay.
  // @return OB_EAGAIN, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return other error codes, failed to replay.
  static int replay_check_restore_status(storage::ObTabletHandle &tablet_handle, const bool update_tx_data);


protected:
  // Check if this replay operation will update the tablet status(ObTabletCreateDeleteMdsUserData), for example, the following
  // 6 types of ObTxDataSourceType need return TRUE.
  // 1. CREATE_TABLET
  // 2. REMOVE_TABLET
  // 3. TX_START_TRANSFER_IN
  // 4. TX_START_TRANSFER_OUT
  // 5. TX_FINISH_TRANSFER_OUT
  // 6. TX_FINISH_TRANSFER_IN
  virtual bool is_replay_update_tablet_status_() const = 0;

  // replay to the tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return other error codes, failed to replay.
  virtual int do_replay_(storage::ObTabletHandle &tablet_handle) = 0;

  // The ObTxDataSourceType corresponding to the multi-source data belonging to the member variable of the class ObTabletMdsData can return true
  virtual bool is_replay_update_mds_table_() const = 0;

  // Check restore status before replay.
  // Usually, you need not override this function, just use the default.
  // @return OB_SUCCESS, need replay.
  // @return OB_EAGAIN, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return other error codes, failed to replay.
  virtual int replay_check_restore_status_(storage::ObTabletHandle &tablet_handle);

  // not allowed to pass ObTabletCreateDeleteMdsUserData or ObTabletBindingMdsUserData
  template <typename T,
            typename U = typename std::enable_if<
                                                 !std::is_same<typename std::decay<T>::type, ObTabletCreateDeleteMdsUserData>::value
                                                 && !std::is_same<typename std::decay<T>::type, ObTabletBindingMdsUserData>::value
                                                >::type>
  int replay_to_mds_table_(
      storage::ObTabletHandle &tablet_handle,
      T &&mds,
      storage::mds::MdsCtx &ctx,
      const share::SCN &scn);

  // non template
  int replay_to_mds_table_(
      storage::ObTabletHandle &tablet_handle,
      const ObTabletCreateDeleteMdsUserData &mds,
      storage::mds::MdsCtx &ctx,
      const share::SCN &scn,
      const bool for_old_mds = false);
  int replay_to_mds_table_(
      storage::ObTabletHandle &tablet_handle,
      const ObTabletBindingMdsUserData &mds,
      storage::mds::MdsCtx &ctx,
      const share::SCN &scn,
      const bool for_old_mds = false);
  template <typename K, typename V>
  int replay_to_mds_table_(
      storage::ObTabletHandle &tablet_handle,
      const K &key,
      V &&value,
      storage::mds::MdsCtx &ctx,
      const share::SCN &scn);
  template <typename K, typename V>
  int replay_remove_to_mds_table_(
      storage::ObTabletHandle &tablet_handle,
      const K &key,
      storage::mds::MdsCtx &ctx,
      const share::SCN &scn);

private:
  int check_can_skip_replay_to_mds_(
      const share::SCN &scn,
      storage::ObTabletHandle &tablet_handle,
      bool &can_skip);
  //  The replay of multi-source log modified by ObTabletCreateDeleteMdsUserData needs to be filtered by tablet_change_checkpoint_scn
  int check_can_skip_replay_(
      const storage::ObLSHandle &ls_handle,
      const share::SCN &scn,
      bool &can_skip);

  int replay_get_tablet_(
      const storage::ObLSHandle &ls_handle,
      const common::ObTabletID &tablet_id,
      const share::SCN &scn,
      storage::ObTabletHandle &tablet_handle);

protected:
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObTabletReplayExecutor);
};

template <typename T, typename U>
int ObTabletReplayExecutor::replay_to_mds_table_(
    storage::ObTabletHandle &tablet_handle,
    T &&mds,
    storage::mds::MdsCtx &ctx,
    const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  storage::ObTablet *tablet = tablet_handle.get_obj();
  if (!is_replay_update_mds_table_()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay log do not update mds table, cannot replay to mds table", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "tablet should not be NULL", KR(ret));
  } else if (tablet->is_ls_inner_tablet()) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "inner tablets have no mds table", KR(ret));
  } else if (OB_FAIL(tablet->replay(std::forward<T>(mds), ctx, scn))) {
    CLOG_LOG(WARN, "failed to do tablet replay", KR(ret));
  }
  return ret;
}

template <typename K, typename V>
int ObTabletReplayExecutor::replay_to_mds_table_(
    storage::ObTabletHandle &tablet_handle,
    const K &key,
    V &&value,
    storage::mds::MdsCtx &ctx,
    const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  storage::ObTablet *tablet = tablet_handle.get_obj();
  if (!is_replay_update_mds_table_()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay log do not update mds table, cannot replay to mds table", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "tablet should not be NULL", KR(ret));
  } else if (tablet->is_ls_inner_tablet()) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "inner tablets have no mds table", KR(ret));
  } else if (OB_FAIL(tablet->replay(key, std::forward<V>(value), ctx, scn))) {
    CLOG_LOG(WARN, "failed to do tablet replay", KR(ret));
  }
  return ret;
}

template <typename K, typename V>
int ObTabletReplayExecutor::replay_remove_to_mds_table_(
    storage::ObTabletHandle &tablet_handle,
    const K &key,
    storage::mds::MdsCtx &ctx,
    const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  storage::ObTablet *tablet = tablet_handle.get_obj();
  if (!is_replay_update_mds_table_()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay log do not remove mds table, cannot replay to mds table", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "tablet should not be NULL", KR(ret));
  } else if (tablet->is_ls_inner_tablet()) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "inner tablets have no mds table", KR(ret));
  } else if (OB_SUCCESS != (ret = tablet->replay_remove<K, V>(key, ctx, scn))) {
    CLOG_LOG(WARN, "failed to do tablet replay", KR(ret));
  }
  return ret;
}

} // namespace logservice
} // namespace oceanbase
#endif
