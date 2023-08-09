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

#ifndef OCEANBASE_STORAGE_TABLET_OB_TABLET_DDL_INFO
#define OCEANBASE_STORAGE_TABLET_OB_TABLET_DDL_INFO

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "share/scn.h"
#include "lib/lock/ob_small_spin_lock.h"

namespace oceanbase
{
namespace storage
{
struct ObTabletDDLInfo final
{
public:
  ObTabletDDLInfo();
  ~ObTabletDDLInfo() = default;
  ObTabletDDLInfo &operator =(const ObTabletDDLInfo &other);
public:
  void reset();
  int get(int64_t &schema_version, int64_t &schema_refreshed_ts);
  int update(const int64_t schema_version,
             const share::SCN &scn,
             int64_t &schema_refreshed_ts);
  TO_STRING_KV(K_(ddl_schema_version), K_(ddl_schema_refreshed_ts), K_(schema_version_change_scn));
private:
  int64_t ddl_schema_version_;
  int64_t ddl_schema_refreshed_ts_;
  share::SCN schema_version_change_scn_;
  common::ObByteLock rwlock_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_TABLET_OB_TABLET_DDL_INFO
