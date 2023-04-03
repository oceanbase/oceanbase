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