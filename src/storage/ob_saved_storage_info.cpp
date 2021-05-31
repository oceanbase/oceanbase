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

#include "ob_saved_storage_info.h"

namespace oceanbase {
namespace storage {
OB_SERIALIZE_MEMBER(ObSavedStorageInfo, version_, epoch_id_, proposal_id_, last_replay_log_id_, last_submit_timestamp_,
    accumulate_checksum_, replica_num_, membership_timestamp_, membership_log_id_, curr_member_list_,
    memstore_version_.version_, publish_version_, schema_version_, frozen_version_.version_, frozen_timestamp_);

int ObSavedStorageInfo::deep_copy(const common::ObBaseStorageInfo& base_storage_info)
{
  return ObBaseStorageInfo::deep_copy(base_storage_info);
}

int ObSavedStorageInfo::deep_copy(const ObSavedStorageInfo& save_storage_info)
{
  int ret = common::OB_SUCCESS;
  if (common::OB_SUCCESS != (ret = ObBaseStorageInfo::deep_copy(save_storage_info))) {
    STORAGE_LOG(WARN, "base storage info copy failed", K(ret));
  } else {
    memstore_version_ = save_storage_info.memstore_version_;
    publish_version_ = save_storage_info.publish_version_;
    schema_version_ = save_storage_info.schema_version_;
    frozen_version_ = save_storage_info.frozen_version_;
    frozen_timestamp_ = save_storage_info.frozen_timestamp_;
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
