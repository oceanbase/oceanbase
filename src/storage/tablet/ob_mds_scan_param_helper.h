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

#ifndef OCEANBASE_STORAGE_OB_MDS_SCAN_APRAM_HELPER
#define OCEANBASE_STORAGE_OB_MDS_SCAN_APRAM_HELPER

#include <stdint.h>
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObNewRange;
class ObTabletID;
class ObString;
}

namespace share
{
class ObLSID;
class SCN;
}

namespace sql
{
struct ObStoragePushdownFlag;
}

namespace storage
{
class ObTableScanParam;

class ObMdsScanParamHelper
{
public:
  static int build_scan_param(
      common::ObIAllocator &allocator,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const uint64_t table_id,
      const uint8_t mds_unit_id,
      const common::ObString &udf_key,
      const bool is_get,
      const int64_t timeout,
      const share::SCN &snapshot,
      ObTableScanParam &scan_param);
  static int build_medium_info_scan_param(
      common::ObIAllocator &allocator,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      ObTableScanParam &scan_param);
  static int build_key_range(
      common::ObIAllocator &allocator,
      const uint64_t table_id,
      const uint8_t mds_unit_id,
      const common::ObString &udf_key,
      common::ObNewRange &key_range);
  static int build_key_range(
      common::ObIAllocator &allocator,
      const uint64_t table_id,
      const uint8_t mds_unit_id,
      common::ObNewRange &key_range);
private:
  static int build_table_param(
      common::ObIAllocator &allocator,
      const share::schema::ObTableSchema &table_schema,
      const common::ObIArray<uint64_t> &column_ids,
      const sql::ObStoragePushdownFlag &pd_pushdown_flag,
      share::schema::ObTableParam *&table_param);
private:
  static constexpr int64_t MDS_SSTABLE_ROWKEY_CNT = 2;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MDS_SCAN_APRAM_HELPER
