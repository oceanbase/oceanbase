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
  if (OB_FAIL(data_checkpoint_->unlink_(this))) {
    STORAGE_LOG(ERROR, "failed to unlink", K(ret), KPC(this));
  } else {
    location_ = OUT;
    prev_ = NULL;
    next_ = NULL;
  }

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

int ObFreezeCheckpoint::finish_freeze()
{
  return data_checkpoint_->finish_freeze(this);
}

}  // namespace checkpoint
}  // namespace storage
}  // namespace oceanbase
