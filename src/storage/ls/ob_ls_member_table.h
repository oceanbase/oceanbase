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

#ifndef OCEANBASE_STORAGE_OB_LS_MEMBER_TABLE_H
#define OCEANBASE_STORAGE_OB_LS_MEMBER_TABLE_H

#include "share/schema/ob_table_schema.h"
#include "storage/tx/ob_multi_data_source.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
class ObLSMemberMemtableMgr;
class ObLSMemberTransRequest;

class ObLSMemberTable
{
public:
  static int handle_create_tablet_notify(const transaction::NotifyType type,
                                         const char *buf, const int64_t len,
                                         const transaction::ObMulSourceDataNotifyArg & trans_flags);
  static int handle_remove_tablet_notify(const transaction::NotifyType type,
                                         const char *buf, const int64_t len,
                                         const transaction::ObMulSourceDataNotifyArg & trans_flags);
private:
  // Below functions are for multi-source trans support
  static int prepare_create_tablets(const obrpc::ObBatchCreateTabletArg &arg,
                                    const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static int on_redo_create_tablets(const obrpc::ObBatchCreateTabletArg &arg,
                                    const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static int on_commit_create_tablets(const obrpc::ObBatchCreateTabletArg *arg,
                                      const transaction::ObMulSourceDataNotifyArg &trans_flags,
                                      bool commit);
  static int on_tx_end_create_tablets(const obrpc::ObBatchCreateTabletArg &arg,
                                   const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static int prepare_remove_tablets(const obrpc::ObBatchRemoveTabletArg &arg,
                                    const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static int on_redo_remove_tablets(const obrpc::ObBatchRemoveTabletArg &arg,
                                    const transaction::ObMulSourceDataNotifyArg &trans_flags);
  static int on_commit_remove_tablets(const obrpc::ObBatchRemoveTabletArg *arg,
                                      const transaction::ObMulSourceDataNotifyArg &trans_flags,
                                      bool commit);
  static int on_tx_end_remove_tablets(const obrpc::ObBatchRemoveTabletArg &arg,
                                   const transaction::ObMulSourceDataNotifyArg &trans_flags);

};

} // namespace storage
} // namespace oceanbase

#endif /* !OCEANBASE_STORAGE_OB_LS_MEMBER_TABLE_H */
