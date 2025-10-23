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
#include "src/storage/blocksstable/ob_macro_block_id.h"

using namespace oceanbase;
using namespace share;
using namespace storage;

/*static*/bool ObTabletTransferInfo::is_private_transfer_epoch_valid(const int64_t transfer_epoch)
{
  return transfer_epoch >= 0 && transfer_epoch < blocksstable::MacroBlockId::MAX_TRANSFER_SEQ;
}

ObTabletTransferInfo::ObTabletTransferInfo()
  : ls_id_(),
    transfer_start_scn_(),
    transfer_seq_(-1),
    has_transfer_table_(false),
    unused_is_transfer_out_deleted_(false),
    src_reorganization_scn_(),
    private_transfer_epoch_(-1)
{
}

int ObTabletTransferInfo::init()
{
  int ret = OB_SUCCESS;
  ls_id_ = TRANSFER_INIT_LS_ID;
  transfer_start_scn_.set_min();
  transfer_seq_ = TRANSFER_INIT_SEQ;
  has_transfer_table_ = false;
  unused_is_transfer_out_deleted_ = false;
  src_reorganization_scn_.set_min();
  return ret;
}

int ObTabletTransferInfo::init(
    const share::ObLSID &ls_id,
    const share::SCN &transfer_start_scn,
    const int64_t transfer_seq,
    const int32_t private_transfer_epoch,
    const share::SCN &src_reorganization_scn)
{
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid()
   || !transfer_start_scn.is_valid_and_not_min()
   || transfer_seq < 0
   || !src_reorganization_scn.is_valid()
   || !ObTabletTransferInfo::is_private_transfer_epoch_valid(private_transfer_epoch)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init transfer info get invalid argument",
      K(ret), K(ls_id), K(transfer_start_scn), K(transfer_seq), K(src_reorganization_scn),
      K(private_transfer_epoch));
  } else {
    ls_id_ = ls_id;
    transfer_start_scn_ = transfer_start_scn;
    transfer_seq_ = transfer_seq;
    has_transfer_table_ = true;
    unused_is_transfer_out_deleted_ = false;
    src_reorganization_scn_ = src_reorganization_scn;
    private_transfer_epoch_ = private_transfer_epoch;
  }
  return ret;
}

void ObTabletTransferInfo::reset()
{
  ls_id_.reset();
  transfer_start_scn_.reset();
  transfer_seq_ = -1;
  has_transfer_table_ = false;
  unused_is_transfer_out_deleted_ = false;
  src_reorganization_scn_.reset();
  private_transfer_epoch_ = -1;
}

bool ObTabletTransferInfo::is_valid() const
{
  return ls_id_.is_valid()
      && transfer_start_scn_.is_valid()
      && transfer_seq_ >= 0;
      // won't check src_reorganization_scn_ for compatibility
      // won't check transfer_epoch cause it's inited at dest.
}

bool ObTabletTransferInfo::has_transfer_table() const
{
  return has_transfer_table_;
}

void ObTabletTransferInfo::reset_transfer_table()
{
  has_transfer_table_ = false;
  //transfer seq, ls id, transfer start scn, is transfer out deleted will not change
}

bool ObTabletTransferInfo::is_transfer_out_deleted() const
{
  return unused_is_transfer_out_deleted_;
}

int ObTabletTransferInfo::get_private_transfer_epoch(int32_t &transfer_epoch) const
{
  int ret = OB_SUCCESS;
  transfer_epoch = -1;
  if (-1 == private_transfer_epoch_) {
    // transfer seq(!= -1) for previous version should within [0, 1<<20 - 1)
    if (transfer_seq_ != -1
        && (transfer_seq_ >= static_cast<int64_t>(blocksstable::MacroBlockId::MAX_TRANSFER_SEQ)
        || transfer_seq_ < TRANSFER_INIT_SEQ)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected transfer seq", K(ret), K(transfer_seq_));
    } else {
      transfer_epoch = transfer_seq_;
    }
  } else if (OB_UNLIKELY(!is_private_transfer_epoch_valid(private_transfer_epoch_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected transfer epoch", K(ret), K(private_transfer_epoch_));
  } else {
    transfer_epoch = private_transfer_epoch_;
  }
  return ret;
}

int ObTabletTransferInfo::set_private_transfer_epoch(const int32_t private_transfer_epoch)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_private_transfer_epoch_valid(private_transfer_epoch))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid private transfer epoch", K(ret));
  } else {
    private_transfer_epoch_ = private_transfer_epoch;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTabletTransferInfo, ls_id_, transfer_start_scn_, transfer_seq_, has_transfer_table_,
  unused_is_transfer_out_deleted_, src_reorganization_scn_, private_transfer_epoch_);
