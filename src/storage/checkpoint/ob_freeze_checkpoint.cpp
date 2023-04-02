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

#include "share/ob_errno.h"
#include "storage/checkpoint/ob_freeze_checkpoint.h"
#include "storage/checkpoint/ob_data_checkpoint.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
namespace checkpoint
{

void ObFreezeCheckpoint::remove_from_data_checkpoint()
{
  if (OUT != location_) {
    ObSpinLockGuard ls_frozen_list_guard(data_checkpoint_->ls_frozen_list_lock_);
    ObSpinLockGuard guard(data_checkpoint_->lock_);
    int ret = OB_SUCCESS;
    if(OB_FAIL(unlink_())) {
      STORAGE_LOG(WARN, "ObFreezeCheckpoint Unlink From DataCheckpoint Failed", K(ret));
    }
  }
}

void ObFreezeCheckpoint::reset()
{
  data_checkpoint_ = nullptr;
}

int ObFreezeCheckpoint::unlink_()
{
  int ret = OB_SUCCESS;
  if (location_ != OUT) {
    switch (location_) {
      case NEW_CREATE:
        if (OB_FAIL(data_checkpoint_->unlink_(this, data_checkpoint_->new_create_list_))) {
          STORAGE_LOG(ERROR, "ObFreezeCheckpoint Unlink From New_Create_List Failed", K(ret));
        }
        break;
      case ACTIVE:
        if (OB_FAIL(data_checkpoint_->unlink_(this, data_checkpoint_->active_list_))) {
          STORAGE_LOG(ERROR, "ObFreezeCheckpoint Unlink From Active_List Failed", K(ret));
        }
        break;
      case LS_FROZEN:
        if (OB_FAIL(data_checkpoint_->unlink_(this, data_checkpoint_->ls_frozen_list_))) {
          STORAGE_LOG(ERROR, "ObFreezeCheckpoint Unlink From Ls_Frozen_List Failed", K(ret));
        }
        break;
      case PREPARE:
        if (OB_FAIL(data_checkpoint_->unlink_(this, data_checkpoint_->prepare_list_))) {
          STORAGE_LOG(ERROR, "ObFreezeCheckpoint Unlink From Prepare_List Failed", K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unknown ObFreezeCheckpoint State", K(location_));
        break;
    }
    location_ = OUT;
  }
  prev_ = NULL;
  next_ = NULL;

  return ret;
}

bool ObFreezeCheckpoint::is_in_prepare_list_of_data_checkpoint()
{
  return location_ == PREPARE;
}

int ObFreezeCheckpoint::add_to_data_checkpoint(ObDataCheckpoint *data_checkpoint)
{
  data_checkpoint_ = data_checkpoint;
  int ret = OB_SUCCESS;
  if (OB_FAIL(data_checkpoint_->add_to_new_create(this))) {
    STORAGE_LOG(ERROR, "add_to_data_checkpoint Failed",
                                      K(ret), K(*this));
  }
  return ret;
}

int ObFreezeCheckpoint::check_can_move_to_active(bool is_ls_freeze)
{
  int ret = OB_SUCCESS;
  if (location_ != ACTIVE) {
    // only when the unit rec_scn is stable that can be moved to ordered_active_list
    if (rec_scn_is_stable()) {
      if (OB_FAIL(move_to_active_(is_ls_freeze))) {
        STORAGE_LOG(ERROR, "transfer to active failed", K(ret));
      }
    }
  }
  return ret;
}

int ObFreezeCheckpoint::move_to_active_(bool is_ls_freeze)
{
  int ret = OB_SUCCESS;
  if (is_ls_freeze) {
    if (OB_FAIL(data_checkpoint_->transfer_from_ls_frozen_to_active_(this))) {
      STORAGE_LOG(ERROR, "can_freeze Failed", K(is_ls_freeze));
    }
  } else if (OB_FAIL(data_checkpoint_->transfer_from_new_create_to_active_(this))) {
    STORAGE_LOG(ERROR, "can_freeze Failed", K(is_ls_freeze));
  }
  return ret;
}

int ObFreezeCheckpoint::finish_freeze()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(data_checkpoint_->lock_);
  if (PREPARE != location_) {
    if (NEW_CREATE == location_ &&
        OB_FAIL(data_checkpoint_->transfer_from_new_create_to_prepare_(this))) {
      STORAGE_LOG(ERROR, "transfer_from_new_create_to_prepare failed", K(ret), K(*this));
    } else if (ACTIVE == location_ &&
               OB_FAIL(data_checkpoint_->transfer_from_active_to_prepare_(this))) {
      STORAGE_LOG(ERROR, "finish_freeze transfer_from_active_to_prepare_ failed", K(ret));
    } else if (LS_FROZEN == location_ &&
               OB_FAIL(data_checkpoint_->transfer_from_ls_frozen_to_prepare_(this))) {
      STORAGE_LOG(ERROR, "finish_freeze transfer_from_ls_frozen_to_prepare_ failed", K(ret));
    }
  }
  return ret;
}

}  // namespace checkpoint
}  // namespace storage
}  // namespace oceanbase
