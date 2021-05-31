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

#include "storage/ob_data_storage_info.h"

namespace oceanbase {

using namespace common;

namespace storage {

OB_SERIALIZE_MEMBER(ObDataStorageInfo, last_replay_log_id_, publish_version_, schema_version_, for_filter_log_compat_,
    created_by_new_minor_freeze_, last_replay_log_ts_);

void ObDataStorageInfo::reset()
{
  last_replay_log_id_ = 0;
  publish_version_ = 0;
  schema_version_ = 0;
  created_by_new_minor_freeze_ = false;
  last_replay_log_ts_ = 0;
}

void ObDataStorageInfo::reset_for_no_memtable_replica()
{
  // TODO(jingyan): The schema version should be separated from data_info.
  // At present, this schema version is used to judge whether it is can be
  // gc, leave it temporarily.
  last_replay_log_id_ = 0;
  last_replay_log_ts_ = 0;
  publish_version_ = 0;
  //  schema_version_ = 0;
}

}  // namespace storage
}  // namespace oceanbase
