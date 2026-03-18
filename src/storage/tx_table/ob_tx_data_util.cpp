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

#include "ob_tx_data_util.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase {
namespace storage {

// alter system set_tp tp_name = "EN_MIN_INTERVAL_OF_TX_DATA_RECYCLE_SECOND", error_code = 60, frequency = 1;
ERRSIM_POINT_DEF(EN_MIN_INTERVAL_OF_TX_DATA_RECYCLE_SECOND)
bool ObTxDataUtil::tx_data_need_recycle(const share::SCN &new_fill_tx_scn, const SCN &min_filled_tx_scn)
{

  // Inject logic to modify default freeze interval
  int tmp_ret = OB_SUCCESS;
  int64_t min_interval_of_tx_data_recycle_us = ObTxTable::MIN_INTERVAL_OF_TX_DATA_RECYCLE_US;
  if (OB_TMP_FAIL(EN_MIN_INTERVAL_OF_TX_DATA_RECYCLE_SECOND)) {
    min_interval_of_tx_data_recycle_us = abs(tmp_ret) * 1000LL * 1000LL;
  }

  bool bret = false;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!new_fill_tx_scn.is_valid() || !min_filled_tx_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(new_fill_tx_scn), K(min_filled_tx_scn));
  } else if (new_fill_tx_scn < min_filled_tx_scn) {
    // no need recycle tx data
  } else {
    // convert ns to ts
    const int64_t new_fill_tx_ts = new_fill_tx_scn.get_val_for_tx() / 1000;
    const int64_t min_filled_tx_ts = min_filled_tx_scn.get_val_for_tx() / 1000;
    if (new_fill_tx_ts - min_filled_tx_ts > min_interval_of_tx_data_recycle_us * 2) {
      bret = true;
    }
  }
  return bret;
}

bool ObTxDataUtil::tablet_is_normal_status(const ObTablet &tablet)
{
  int tmp_ret = OB_SUCCESS;
  bool is_normal_status = false;
  ObTabletCreateDeleteMdsUserData user_data;
  mds::MdsWriter writer;
  mds::TwoPhaseCommitState trans_stat;
  share::SCN trans_version;
  if (OB_TMP_FAIL(
          tablet.ObITabletMdsInterface::get_latest_tablet_status(user_data, writer, trans_stat, trans_version))) {
    STORAGE_LOG_RET(WARN, 0, "failed to get tablet status", K(tmp_ret), K(tablet));
  } else if (user_data.get_tablet_status() == ObTabletStatus::Status::NORMAL) {
    is_normal_status = true;
  } else {
    STORAGE_LOG_RET(INFO, 0, "tablet status is not normal", K(tablet.get_tablet_id()), K(user_data.get_tablet_status()));
  }
  return is_normal_status;
}

}  // namespace storage
}  // namespace oceanbase