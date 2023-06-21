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

#ifndef OCEANBASE_STORAGE_OB_FREEZE_DEFINE_
#define OCEANBASE_STORAGE_OB_FREEZE_DEFINE_

#include "common/ob_range.h"

namespace oceanbase
{
namespace storage
{

enum ObFreezeCmd
{
  FREEZE_PREPARE_CMD = 0,
  FREEZE_COMMIT_CMD,
  FREEZE_ABORT_CMD,
  MAX_FREEZE_CMD,
};

enum ObFreezeType
{
  INVALID_FREEZE = 0,
  MAJOR_FREEZE,
  MINOR_FREEZE,
  TX_DATA_TABLE_FREEZE,
  MDS_TABLE_FREEZE,
  MAX_FREEZE_TYPE
};

struct ObFrozenStatus
{
  static const int64_t INVALID_FROZEN_VERSION = 0;
  static const int64_t INVALID_TIMESTAMP = common::OB_INVALID_TIMESTAMP;
  static const int64_t INVALID_SCHEMA_VERSION = 0;
  ObFrozenStatus() : frozen_version_(),
                     frozen_timestamp_(INVALID_TIMESTAMP),
                     status_(common::FREEZE_STATUS_MAX),
                     schema_version_(INVALID_SCHEMA_VERSION),
                     cluster_version_(0)
  {}
  ObFrozenStatus(const common::ObVersion &frozen_version,
                 const int64_t frozen_timestamp,
                 const common::ObFreezeStatus &status,
                 const int64_t schema_version,
                 const int64_t cluster_version)
  : frozen_version_(frozen_version.version_),
    frozen_timestamp_(frozen_timestamp),
    status_(status),
    schema_version_(schema_version),
    cluster_version_(cluster_version)
  {}

  void reset()
  {
    frozen_version_.reset();
    frozen_timestamp_ = INVALID_TIMESTAMP;
    status_ = common::FREEZE_STATUS_MAX;
    schema_version_ = INVALID_SCHEMA_VERSION;
    cluster_version_ = 0;
  }
  bool is_valid() const
  {
    return  frozen_version_.is_valid()
        && frozen_timestamp_ > common::OB_INVALID_TIMESTAMP
        && common::FREEZE_STATUS_MAX != status_;
  }
  bool operator ==(const ObFrozenStatus &other) const
  {
    return ((this == &other)
        || (this->frozen_version_ == other.frozen_version_
          && this->frozen_timestamp_ == other.frozen_timestamp_
          && this->status_ == other.status_
          && this->schema_version_ == other.schema_version_
          && this->cluster_version_ == other.cluster_version_));
  }
  TO_STRING_KV(N_FROZEN_VERSION, frozen_version_,
              "frozen_timestamp", frozen_timestamp_,
              "freeze_status", status_,
              N_SCHEMA_VERSION, schema_version_,
              "cluster_version", cluster_version_);

  common::ObVersion frozen_version_;
  int64_t frozen_timestamp_;
  common::ObFreezeStatus status_;
  int64_t schema_version_;
  int64_t cluster_version_;

  OB_UNIS_VERSION(1);
};

inline bool is_cmd_valid(const ObFreezeCmd &cmd)
{
  return cmd >= FREEZE_PREPARE_CMD && cmd < MAX_FREEZE_CMD;
}


}//end namespace storage
}//end namespace oceanbase

#endif //OCEANBASE_OCEANBASE_STORAGE_OB_FREEZE_DEFINE_
