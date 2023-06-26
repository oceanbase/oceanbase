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
#include "ob_ls_transfer_info.h"

using namespace oceanbase;
using namespace share;
using namespace storage;


ObLSTransferInfo::ObLSTransferInfo()
  : ls_id_(TRANSFER_INIT_LS_ID),
    transfer_start_scn_(share::SCN::invalid_scn())
{
}

int ObLSTransferInfo::init(
    const share::ObLSID &ls_id,
    const share::SCN &transfer_start_scn)
{
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid() || !transfer_start_scn.is_valid_and_not_min()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init transfer info get invalid agument", K(ret), K(ls_id), K(transfer_start_scn));
  } else {
    ls_id_ = ls_id;
    transfer_start_scn_ = transfer_start_scn;
  }
  return ret;
}

void ObLSTransferInfo::reset()
{
  ls_id_.reset();
  transfer_start_scn_.reset();
}

bool ObLSTransferInfo::is_valid() const
{
  return ls_id_.is_valid()
      && transfer_start_scn_.is_valid();
}

bool ObLSTransferInfo::already_enable_replay() const
{
  return !is_valid();
}
