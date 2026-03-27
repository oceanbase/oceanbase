/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_TRANSACTION_OB_TX_STANDBY_CLEANUP_
#define OCEANBASE_TRANSACTION_OB_TX_STANDBY_CLEANUP_

#include "ob_trans_define.h"

namespace oceanbase
{

namespace transaction
{
class ObTxStandbyCleanupTask : public ObTransTask
{
public:
  ObTxStandbyCleanupTask() : ObTransTask(ObTransRetryTaskType::STANDBY_CLEANUP_TASK)
  {}
  ~ObTxStandbyCleanupTask() { destroy(); }
};

} // transaction
} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_TX_STANDBY_CLEANUP_
