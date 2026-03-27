/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
