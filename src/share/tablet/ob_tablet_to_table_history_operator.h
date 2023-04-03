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

#ifndef OCEANBASE_SHARE_OB_TABLET_TO_TABLE_HISTORY_OPERATOR
#define OCEANBASE_SHARE_OB_TABLET_TO_TABLE_HISTORY_OPERATOR

#include "lib/container/ob_iarray.h"     // ObIArray
#include "common/ob_tablet_id.h"         // ObTabletID
#include "share/tablet/ob_tablet_info.h" // ObTabletTablePair

namespace oceanbase
{
namespace common
{
class ObISQLClient;

} // end nampspace common

namespace share
{
class ObTabletToTableHistoryOperator
{
public:
  ObTabletToTableHistoryOperator() {}
  virtual ~ObTabletToTableHistoryOperator() {}

  static int create_tablet_to_table_history(
             common::ObISQLClient &sql_proxy,
             const uint64_t tenant_id,
             const int64_t schema_version,
             const common::ObIArray<ObTabletTablePair> &pairs);
  static int drop_tablet_to_table_history(
             common::ObISQLClient &sql_proxy,
             const uint64_t tenant_id,
             const int64_t schema_version,
             const common::ObIArray<ObTabletID> &tablet_ids);
};

} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_OB_TABLET_TO_TABLE_HISTORY_OPERATOR
