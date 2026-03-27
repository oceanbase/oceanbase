/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_TLOCAL_INFO_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_TLOCAL_INFO_H

#include "storage/tx/ob_multi_data_source.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

struct MdsTLocalInfo
{
  MdsTLocalInfo() : notify_type_(transaction::NotifyType::UNKNOWN) {}
  void reset() { new (this) MdsTLocalInfo(); }
  TO_STRING_KV(K_(notify_type))
  transaction::NotifyType notify_type_;
};

}
}
}
#endif