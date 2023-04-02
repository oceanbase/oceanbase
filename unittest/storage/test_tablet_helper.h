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

#ifndef OCEANBASE_STORAGE_TEST_TABLET_HELPER
#define OCEANBASE_STORAGE_TEST_TABLET_HELPER

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_rpc_struct.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{
class TestTabletHelper
{
public:
  static int create_tablet(ObLSTabletService &ls_tablet_svr, obrpc::ObBatchCreateTabletArg &arg);
  static int remove_tablet(ObLSTabletService &ls_tablet_svr, obrpc::ObBatchRemoveTabletArg &arg);
};

int TestTabletHelper::create_tablet(ObLSTabletService &ls_tablet_svr, obrpc::ObBatchCreateTabletArg &arg)
{
  int ret = common::OB_SUCCESS;
  transaction::ObMulSourceDataNotifyArg trans_flags;
  trans_flags.tx_id_ = 123;
  trans_flags.scn_ = share::SCN::invalid_scn();
  trans_flags.for_replay_ = false;

  if (OB_FAIL(ls_tablet_svr.on_prepare_create_tablets(arg, trans_flags))) {
    STORAGE_LOG(WARN, "failed to prepare create tablets", K(ret), K(arg));
  } else if (FALSE_IT(trans_flags.scn_ = share::SCN::minus(share::SCN::max_scn(), 100))) {
  } else if (OB_FAIL(ls_tablet_svr.on_redo_create_tablets(arg, trans_flags))) {
    STORAGE_LOG(WARN, "failed to redo create tablets", K(ret), K(arg));
  } else if (FALSE_IT(trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1))) {
  } else if (OB_FAIL(ls_tablet_svr.on_tx_end_create_tablets(arg, trans_flags))) {
    STORAGE_LOG(WARN, "failed to tx end create tablets", K(ret), K(arg));
  } else if (FALSE_IT(trans_flags.scn_ = share::SCN::plus(trans_flags.scn_, 1))) {
  } else if (OB_FAIL(ls_tablet_svr.on_commit_create_tablets(arg, trans_flags))) {
    STORAGE_LOG(WARN, "failed to commit create tablets", K(ret), K(arg));
  }

  return ret;
}

int TestTabletHelper::remove_tablet(ObLSTabletService &ls_tablet_svr, obrpc::ObBatchRemoveTabletArg &arg)
{
  int ret = OB_SUCCESS;
  return ret;
}
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_TEST_TABLET_HELPER
