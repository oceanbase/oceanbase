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

#ifndef OCEANBASE_STORAGE_OB_TABLET_COMMON
#define OCEANBASE_STORAGE_OB_TABLET_COMMON

#include <stdint.h>

namespace oceanbase
{
namespace storage
{
class ObTabletCommon final
{
public:
  // DIRECT_GET_COMMITTED_TABLET_TIMEOUT_US: only get NORMAL tablet instantly
  static const int64_t DIRECT_GET_COMMITTED_TABLET_TIMEOUT_US = -1;
  // NO_CHECK_GET_TABLET_TIMEOUT_US: return tablet handle IMMEDIATELY if succeeded, won't check tablet status
  static const int64_t NO_CHECK_GET_TABLET_TIMEOUT_US = 0;
  // DEFAULT_GET_TABLET_TIMEOUT_US: cond wait until tablet status changes to NORMAL/DELETED
  static const int64_t DEFAULT_GET_TABLET_TIMEOUT_US = 10 * 1000 * 1000; // 10s

  static const int64_t DEFAULT_ITERATOR_TABLET_ID_CNT = 128;
  static const int64_t BUCKET_LOCK_BUCKET_CNT = 10243L;
  static const int64_t TABLET_ID_SET_BUCKET_CNT = 10243L;
  static const int64_t DEFAULT_GET_TABLET_DURATION_US = 1 * 1000 * 1000; // 1s

  static const int64_t FINAL_TX_ID = 0;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_COMMON
