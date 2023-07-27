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

#ifndef OCEABASE_STORAGE_HA_SRC_PROVIDER_
#define OCEABASE_STORAGE_HA_SRC_PROVIDER_

#include "share/ob_srv_rpc_proxy.h"  // ObPartitionServiceRpcProxy
#include "storage/ob_storage_rpc.h"
#include "ob_storage_ha_struct.h"

namespace oceanbase {
namespace storage {

class ObStorageHASrcProvider {
public:
  ObStorageHASrcProvider();
  virtual ~ObStorageHASrcProvider();
  int init(const uint64_t tenant_id, const ObMigrationOpType::TYPE &type, storage::ObStorageRpc *storage_rpc);
  int choose_ob_src(const share::ObLSID &ls_id, const share::SCN &local_clog_checkpoint_scn,
      ObStorageHASrcInfo &src_info);
  int get_ls_member_list(const uint64_t tenant_id, const share::ObLSID &ls_id,
      common::ObIArray<common::ObAddr> &addr_list);

private:
  int get_ls_leader_(const uint64_t tenant_id, const share::ObLSID &ls_id, common::ObAddr &addr);
  int fetch_ls_member_list_(const uint64_t tenant_id, const share::ObLSID &ls_id, const common::ObAddr &addr,
      common::ObIArray<common::ObAddr> &addr_list);
  int inner_choose_ob_src_(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const share::SCN &local_clog_checkpoint_scn, const common::ObIArray<common::ObAddr> &addr_list,
      common::ObAddr &choosen_src_addr);
  int fetch_ls_meta_info_(const uint64_t tenant_id, const share::ObLSID &ls_id, const common::ObAddr &member_addr,
      obrpc::ObFetchLSMetaInfoResp &ls_meta_info);
  int get_ls_member_list_(const uint64_t tenant_id, const share::ObLSID &ls_id,  const common::ObAddr &leader_addr,
      common::ObIArray<common::ObAddr> &addr_list);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObMigrationOpType::TYPE type_;
  storage::ObStorageRpc *storage_rpc_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageHASrcProvider);
};

}  // namespace storage
}  // namespace oceanbase
#endif
