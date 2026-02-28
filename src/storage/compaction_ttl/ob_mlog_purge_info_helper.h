//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_TTL_MLOG_CACHE_H_
#define OB_STORAGE_COMPACTION_TTL_MLOG_CACHE_H_
#include "lib/utility/ob_print_utils.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
namespace oceanbase
{
namespace storage
{
class ObMLogPurgeInfoHelper
{
public:
  static int get_mlog_purge_scn(
    const uint64_t mlog_id,
    const int64_t read_snapshot,
    int64_t &last_purge_scn);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_TTL_MLOG_CACHE_H_
