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

#define USING_LOG_PREFIX STORAGE

#include "storage/tablet/ob_tablet_status.h"

#include "lib/lock/ob_thread_cond.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/serialization.h"
#include "share/ob_errno.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_multi_source_data.h"
#include "storage/tx/ob_trans_define.h"
#include "share/scn.h"

namespace oceanbase
{
using namespace share;
namespace storage
{
ObTabletStatus::ObTabletStatus()
  : status_(Status::MAX)
{
}

ObTabletStatus::ObTabletStatus(const Status &status)
  : status_(status)
{
}

int ObTabletStatus::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::encode_i8(buf, len, new_pos, static_cast<int8_t>(status_)))) {
    LOG_WARN("failed to serialize status", K(ret), K(len), K_(status));
  } else {
    pos = new_pos;
  }

  return ret;
}

int ObTabletStatus::deserialize(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::decode_i8(buf, len, new_pos, (int8_t*)(&status_)))) {
    LOG_WARN("failed to deserialize status", K(ret), K(len), K(new_pos));
  } else {
    pos = new_pos;
  }

  return ret;
}

int64_t ObTabletStatus::get_serialize_size() const
{
  return serialization::encoded_length_i8(static_cast<int8_t>(status_));
}

bool ObTabletStatus::is_valid_status(const Status current_status, const Status target_status)
{
  bool b_ret = true;

  switch (current_status) {
    case CREATING:
      if (target_status != NORMAL && target_status != DELETED) {
        b_ret = false;
      }
      break;
    case NORMAL:
      if (target_status != DELETING && target_status != NORMAL) {
        b_ret = false;
      }
      break;
    case DELETING:
      if (target_status != NORMAL && target_status != DELETED
          && target_status != DELETING) {
        b_ret = false;
      }
      break;
    case DELETED:
      break;
    case MAX:
      if (target_status != CREATING && target_status != DELETED) {
        b_ret = false;
      }
      break;
    default:
      b_ret = false;
      break;
  }

  return b_ret;
}

ObTabletStatusChecker::ObTabletStatusChecker(ObTablet &tablet)
  : tablet_(tablet)
{
}

int ObTabletStatusChecker::check(const uint64_t time_us)
{
  int ret = OB_SUCCESS;
  common::ObThreadCond &cond = tablet_.get_cond();
  ObThreadCondGuard guard(cond);

  if (OB_FAIL(do_wait(cond, time_us))) {
    LOG_WARN("failed to do cond wait", K(ret));
  }

  return ret;
}

int ObTabletStatusChecker::wake_up(
    ObTabletTxMultiSourceDataUnit &tx_data,
    const SCN &memtable_scn,
    const bool for_replay,
    const memtable::MemtableRefOp ref_op)
{
  int ret = OB_SUCCESS;
  common::ObThreadCond &cond = tablet_.get_cond();
  ObThreadCondGuard guard(cond);

  if (OB_FAIL(tablet_.set_tablet_final_status(tx_data, memtable_scn, for_replay, ref_op))) {
    LOG_WARN("failed to set tablet status", K(ret), K(tx_data), K(memtable_scn),
        K(for_replay), K(ref_op));
  } else if (OB_FAIL(cond.broadcast())) {
    LOG_WARN("failed to broadcast", K(ret));
  }

  return ret;
}

int ObTabletStatusChecker::do_wait(common::ObThreadCond &cond, const uint64_t time_us)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const share::ObLSID &ls_id = tablet_.tablet_meta_.ls_id_;
  const common::ObTabletID &tablet_id = tablet_.tablet_meta_.tablet_id_;
  bool need_wait = true;
  ObTabletStatus::Status actual_status = ObTabletStatus::MAX;

  if (OB_FAIL(tablet_.get_tablet_status(actual_status))) {
    LOG_WARN("failed to get status", K(ret));
  } else if (ObTabletStatus::NORMAL == actual_status
      || ObTabletStatus::DELETED == actual_status) {
    need_wait = false;
  } else if (ObTabletStatus::CREATING == actual_status
      || ObTabletStatus::DELETING == actual_status
      || ObTabletStatus::MAX == actual_status) {
    need_wait = true;
    // MAX status only occurs when tablet is in creating procedure
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", K(ret), K(tenant_id), K(ls_id), K(tablet_id), K(actual_status));
  }

  if (OB_FAIL(ret)) {
  } else if (!need_wait) {
  } else {
    while (OB_SUCC(ret) && !is_final_status(actual_status)) {
      if (OB_FAIL(tablet_.get_tablet_status(actual_status))) {
        LOG_WARN("failed to get status", K(ret));
      } else if (is_final_status(actual_status)) {
        break;
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(cond.wait_us(time_us))) {
        if (OB_TIMEOUT == ret) {
          LOG_WARN("cond wait timeout", K(ret), K(tenant_id), K(ls_id), K(tablet_id),
              K(time_us), K(actual_status));
        } else {
          LOG_WARN("failed to cond wait", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && ObTabletStatus::DELETED == actual_status) {
    ret = OB_TABLET_NOT_EXIST;
    LOG_WARN("tablet does not exist, may be deleted", K(ret), K(actual_status));
  }

  return ret;
}
} // namespace storage
} // namespace oceanbase
