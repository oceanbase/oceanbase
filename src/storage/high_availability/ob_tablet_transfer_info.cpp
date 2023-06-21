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
#include "ob_tablet_transfer_info.h"

using namespace oceanbase;
using namespace share;
using namespace storage;


ObTabletTransferInfo::ObTabletTransferInfo()
  : ls_id_(),
    transfer_start_scn_(),
    transfer_seq_(-1),
    has_transfer_table_(false)
{
}

int ObTabletTransferInfo::init()
{
  int ret = OB_SUCCESS;
  ls_id_ = TRANSFER_INIT_LS_ID;
  transfer_start_scn_.set_min();
  transfer_seq_ = 0;
  has_transfer_table_ = false;
  return ret;
}

int ObTabletTransferInfo::init(
    const share::ObLSID &ls_id,
    const share::SCN &transfer_start_scn,
    const int64_t transfer_seq)
{
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid() || !transfer_start_scn.is_valid_and_not_min() || transfer_seq < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init transfer info get invalid agument", K(ret), K(ls_id), K(transfer_start_scn), K(transfer_seq));
  } else {
    ls_id_ = ls_id;
    transfer_start_scn_ = transfer_start_scn;
    transfer_seq_ = transfer_seq;
    has_transfer_table_ = true;
  }
  return ret;
}

void ObTabletTransferInfo::reset()
{
  ls_id_.reset();
  transfer_start_scn_.reset();
  transfer_seq_ = -1;
  has_transfer_table_ = false;
}

bool ObTabletTransferInfo::is_valid() const
{
  return ls_id_.is_valid()
      && transfer_start_scn_.is_valid()
      && transfer_seq_ >= 0;
}

bool ObTabletTransferInfo::has_transfer_table() const
{
  return has_transfer_table_;
}

void ObTabletTransferInfo::reset_transfer_table()
{
  has_transfer_table_ = false;
  //transfer seq, ls id, transfer start scn will not change
}

OB_SERIALIZE_MEMBER(ObTabletTransferInfo, ls_id_, transfer_start_scn_, transfer_seq_, has_transfer_table_);
