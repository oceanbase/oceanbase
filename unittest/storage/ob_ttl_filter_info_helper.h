/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_UNITTEST_TTL_FILTER_INFO_HELPER
#define OCEANBASE_UNITTEST_TTL_FILTER_INFO_HELPER

#include "storage/mockcontainer/mock_ob_iterator.h"
#define protected public
#define private public

#include "storage/compaction_ttl/ob_ttl_filter_info_array.h"
#include "storage/tablet/ob_tablet.h"
#include "unittest/storage/ob_truncate_info_helper.h"
#include "storage/truncate_info/ob_mds_info_distinct_mgr.h"

#include <stdint.h>

namespace oceanbase
{
using namespace common;
using namespace share;
namespace storage
{
class TTLFilterInfoHelper
{
public:
  enum MockTTLFilterInfoCol : uint8_t
  {
    TRANS_ID = 0,
    COMMIT_VERSION,
    FILTER_TYPE,
    FILTER_VALUE,
    FILTER_COL_IDX,
    COL_MAX
  };

  static void mock_ttl_filter_info(const int64_t trans_id,
                                   const int64_t commit_version,
                                   const int64_t filter_type,
                                   const int64_t filter_value,
                                   const int64_t filter_col_idx,
                                   ObTTLFilterInfo &info)
  {
    info.key_.tx_id_ = trans_id;
    info.commit_version_ = commit_version;
    info.ttl_filter_col_type_ = static_cast<ObTTLFilterInfo::ObTTLFilterColType>(filter_type);
    info.ttl_filter_value_ = filter_value;
    info.ttl_filter_col_idx_ = filter_col_idx;
  }

  static int get_tablet(const ObLSID &ls_id,
                        const ObTabletID &tablet_id,
                        ObTabletHandle &tablet_handle)
  {
    return TruncateInfoHelper::get_tablet(ls_id, tablet_id, tablet_handle);
  }

  static int batch_mock_ttl_filter_info_without_sort(ObArenaAllocator &allocator,
                                                     const char *key_data,
                                                     ObTTLFilterInfoArray &ttl_filter_info_array)
  {
    int ret = OB_SUCCESS;

    ObMockIterator key_iter;
    const ObStoreRow *row = nullptr;
    const ObObj *cells = nullptr;

    if (OB_FAIL(key_iter.from(key_data))) {
      COMMON_LOG(WARN, "failed to init key iterator", KR(ret), K(key_data));
    } else if (OB_FAIL(ttl_filter_info_array.init_for_first_creation(allocator))) {
      COMMON_LOG(WARN, "failed to init ttl filter info array", KR(ret), K(ttl_filter_info_array));
    }

    ObArray<bool> is_negative;

    for (int64_t i = 0; OB_SUCC(ret) && i < key_iter.count(); ++i) {
      if (OB_FAIL(key_iter.get_row(i, row))) {
        COMMON_LOG(WARN, "failed to get row", KR(ret), K(i));
      } else {
        cells = row->row_val_.cells_;
        ObTTLFilterInfo ttl_filter_info;
        if (OB_FAIL(is_negative.push_back(cells[COMMIT_VERSION].get_int() < 0))) {
          COMMON_LOG(WARN, "failed to push back is negative", KR(ret), K(i));
        } else {
          mock_ttl_filter_info(cells[TRANS_ID].get_int(),
                               is_negative.at(i) ? -cells[COMMIT_VERSION].get_int() : cells[COMMIT_VERSION].get_int(),
                               cells[FILTER_TYPE].get_int(),
                               cells[FILTER_VALUE].get_int(),
                               cells[FILTER_COL_IDX].get_int(),
                               ttl_filter_info);
          if (OB_FAIL(ttl_filter_info_array.append_with_deep_copy(ttl_filter_info))) {
            COMMON_LOG(WARN, "failed to append ttl filter info", KR(ret), K(ttl_filter_info));
          }
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < is_negative.count(); ++i) {
      if (is_negative.at(i)) {
        ttl_filter_info_array.at(i)->commit_version_ = -ttl_filter_info_array.at(i)->commit_version_;
      }
    }

    return ret;
  }

  static int read_ttl_filter_info_array(ObArenaAllocator &allocator,
                                        const ObLSID &ls_id,
                                        const ObTabletID &tablet_id,
                                        const ObVersionRange &read_version_range,
                                        ObTTLFilterInfoArray &ttl_filter_info_array)
  {
    int ret = OB_SUCCESS;

    ObTTLFilterInfoDistinctMgr distinct_mgr;
    ObTabletHandle tablet_handle;
    ttl_filter_info_array.reset();

    if (OB_FAIL(TTLFilterInfoHelper::get_tablet(ls_id, tablet_id, tablet_handle))) {
      COMMON_LOG(WARN, "failed to get tablet", KR(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(distinct_mgr.init(allocator, *tablet_handle.get_obj(), nullptr, read_version_range, false/*access*/))) {
      COMMON_LOG(WARN, "failed to init distinct mgr", KR(ret), K(read_version_range));
    } else if (OB_FAIL(ttl_filter_info_array.init_for_first_creation(allocator))) {
      COMMON_LOG(WARN, "failed to init ttl filter info array", KR(ret));
    } else if (OB_FAIL(distinct_mgr.get_distinct_mds_info_array(ttl_filter_info_array))) {
      COMMON_LOG(WARN, "fail to read ttl filter info array", KR(ret), K(read_version_range));
    }

    return ret;
  }
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_UNITTEST_TRUNCATE_INFO_HELPER
