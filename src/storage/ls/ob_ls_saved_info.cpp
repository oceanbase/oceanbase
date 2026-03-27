/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_ls_saved_info.h"
#include "share/ob_table_range.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

ObLSSavedInfo::ObLSSavedInfo()
  : clog_checkpoint_scn_(share::ObScnRange::MIN_SCN),
    clog_base_lsn_(palf::PALF_INITIAL_LSN_VAL),
    replayable_point_(0),
    tablet_change_checkpoint_scn_(SCN::min_scn())
{
}

void ObLSSavedInfo::reset()
{
  clog_checkpoint_scn_ = share::ObScnRange::MIN_SCN;
  clog_base_lsn_.reset();
  replayable_point_ = 0;
  tablet_change_checkpoint_scn_ = SCN::min_scn();
}

bool ObLSSavedInfo::is_valid() const
{
  return clog_checkpoint_scn_ >= share::ObScnRange::MIN_SCN
      && clog_checkpoint_scn_.is_valid()
      && clog_base_lsn_.is_valid()
      && replayable_point_ >= 0
      && tablet_change_checkpoint_scn_.is_valid();
}

bool ObLSSavedInfo::is_empty() const
{
  return share::ObScnRange::MIN_SCN == clog_checkpoint_scn_
      && palf::PALF_INITIAL_LSN_VAL == clog_base_lsn_
      && 0 == replayable_point_
      && !tablet_change_checkpoint_scn_.is_valid();
}

OB_SERIALIZE_MEMBER(ObLSSavedInfo, clog_checkpoint_scn_, clog_base_lsn_, replayable_point_, tablet_change_checkpoint_scn_);

}
}
