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