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

#include "ob_trans_split_adapter.h"
#include "ob_trans_msg2.h"
#include "ob_trans_part_ctx.h"
#include "ob_trans_coord_ctx.h"
#include "storage/ob_pg_mgr.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {

using namespace storage;
using namespace common;

namespace transaction {

OB_SERIALIZE_MEMBER(ObTransSplitInfo, src_pk_, dest_pks_);

int ObTransSplitInfo::init(const ObPartitionKey& src_pk, const ObIArray<ObPartitionKey>& dest_pks)
{
  int ret = OB_SUCCESS;
  if (!src_pk.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument");
  } else {
    src_pk_ = src_pk;
    int64_t count = dest_pks.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(dest_pks_.push_back(dest_pks.at(i)))) {
        TRANS_LOG(WARN, "failed to init dest pks", KR(ret));
      }
    }
  }
  return ret;
}

int ObTransSplitInfo::assign(const ObTransSplitInfo& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dest_pks_.assign(other.dest_pks_))) {
    TRANS_LOG(WARN, "dest array assign failed", KR(ret));
  } else {
    src_pk_ = other.src_pk_;
  }
  return ret;
}

int ObTransSplitAdapter::update_participant_list(ObCoordTransCtx* ctx, ObPartitionArray& ctx_participants,
    ObMaskSet& ctx_mask_set, const ObTransSplitInfoArray& split_info_arr, ObPartitionArray& new_participants)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx) || split_info_arr.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument");
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < split_info_arr.count(); ++idx) {
      const ObTransSplitInfo& split_info = split_info_arr.at(idx);
      int64_t dest_count = split_info.get_dest_array().count();
      for (int64_t i = 0; OB_SUCC(ret) && i < dest_count; ++i) {
        ObPartitionKey pk = split_info.get_dest_array().at(i);
        bool found = false;
        for (int64_t j = 0; !found && j < ctx_participants.count(); ++j) {
          if (pk == ctx_participants.at(j)) {
            found = true;
          }
        }
        if (!found) {
          if (OB_FAIL(ctx_participants.push_back(pk))) {
            TRANS_LOG(WARN, "failed to add new participants", KR(ret));
          } else if (OB_FAIL(new_participants.push_back(pk))) {
            TRANS_LOG(WARN, "failed to add new participants", KR(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && !new_participants.empty()) {
      ObPartitionArray mask_participants;
      if (OB_FAIL(ctx_mask_set.get_mask(mask_participants))) {
        TRANS_LOG(WARN, "failed to get masked participants", KR(ret));
      } else {
        ctx_mask_set.reset();
        if (OB_FAIL(ctx_mask_set.init(ctx_participants))) {
          TRANS_LOG(WARN, "failed to init msg mask set", KR(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < mask_participants.count(); ++i) {
          if (OB_FAIL(ctx_mask_set.mask(mask_participants.at(i)))) {
            TRANS_LOG(WARN, "failed to mask participant", KR(ret), K(mask_participants.at(i)));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransSplitAdapter::set_split_info_for_prepare_response(
    const ObPartitionService* partition_service, const ObPartitionKey& pk, ObTrx2PCPrepareResponse& res)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  bool is_split_partition = false;

  if (OB_ISNULL(partition_service) || !res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument");
  } else if (OB_FAIL(partition_service->get_partition(pk, guard))) {
    TRANS_LOG(WARN, "failed to get partition guard", KR(ret));
  } else if (NULL == (partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "failed to get partition", KR(ret));
  } else if (OB_FAIL(partition->check_cur_partition_split(is_split_partition))) {
    TRANS_LOG(WARN, "failed to check partition split", KR(ret));
  } else if (!is_split_partition) {
    // do nothing
  } else {
    ObTransSplitInfo split_info;
    if (OB_FAIL(partition->get_trans_split_info(split_info))) {
      TRANS_LOG(WARN, "get trans split info failed", KR(ret));
    } else if (!split_info.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "invalid trans split info after split started", KR(ret));
    } else if (OB_FAIL(res.set_split_info(split_info))) {
      TRANS_LOG(WARN, "failed to set split info in prepare response", KR(ret));
    }
  }

  return ret;
}

int ObTransSplitAdapter::find_corresponding_split_info(
    const ObPartitionKey& dest_pk, const ObTransSplitInfoArray& split_info_arr, ObTransSplitInfo& split_info)
{
  int ret = OB_SUCCESS;
  if (!dest_pk.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret));
  } else {
    split_info.reset();
    int64_t split_count = split_info_arr.count();
    bool found = false;
    for (int64_t idx = 0; !found && idx < split_count; ++idx) {
      const ObTransSplitInfo& sp_info = split_info_arr.at(idx);
      int64_t dest_count = sp_info.get_dest_array().count();
      for (int64_t i = 0; !found && i < dest_count; ++i) {
        if (dest_pk == sp_info.get_dest_array().at(i)) {
          if (OB_FAIL(split_info.assign(sp_info))) {
            TRANS_LOG(WARN, "failed to assign split info", KR(ret));
          }
          found = true;
        }
      }
    }
  }
  return ret;
}

}  // namespace transaction
}  // namespace oceanbase