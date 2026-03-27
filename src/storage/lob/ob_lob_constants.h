/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_LOB_CONSTANTS_H_
#define OCEANBASE_STORAGE_OB_LOB_CONSTANTS_H_

namespace oceanbase
{
namespace storage
{

class ObLobConstants
{
public:
  static const int64_t LOB_AUX_TABLE_COUNT = 2; // lob aux table count for each table
  static const int64_t LOB_WITH_OUTROW_CTX_SIZE = sizeof(ObLobCommon) + sizeof(ObLobData) + sizeof(ObLobDataOutRowCtx);
  static const int64_t LOB_OUTROW_FULL_SIZE = ObLobLocatorV2::DISK_LOB_OUTROW_FULL_SIZE;
  static const uint64_t LOB_QUERY_RETRY_MAX = 100L; // 100 times
};


}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_CONSTANTS_H_
