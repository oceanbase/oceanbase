/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/slog/ob_storage_log_struct.h"

namespace oceanbase
{
namespace storage
{
ObStorageLogParam::ObStorageLogParam()
  : cmd_(0),
    data_(nullptr),
    disk_addr_()
{
}

ObStorageLogParam::ObStorageLogParam(const int32_t cmd, ObIBaseStorageLogEntry *data)
  : cmd_(cmd),
    data_(data),
    disk_addr_()
{
}

void ObStorageLogParam::reset()
{
  cmd_ = 0;
  data_ = nullptr;
  disk_addr_.reset();
}

bool ObStorageLogParam::is_valid() const
{
  return cmd_ > 0
      && nullptr != data_
      && data_->is_valid();
}
}//end namespace blocksstable
}//end namespace oceanbase