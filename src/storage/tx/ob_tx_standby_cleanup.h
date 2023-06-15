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
