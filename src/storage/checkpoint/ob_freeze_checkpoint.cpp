/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_freeze_checkpoint.h"
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
