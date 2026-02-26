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

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_REBUILD_TABLET_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_REBUILD_TABLET_H_

#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace common
{
  template <typename T> class ObIArray;
}

namespace obrpc
{
class ObSrvRpcProxy;
}//end namespace obrpc

namespace rootserver
{
class ObUnitManager;

class ObRootRebuildTablet
{
public:
  ObRootRebuildTablet();
  virtual ~ObRootRebuildTablet();

  int init(obrpc::ObSrvRpcProxy &rpc_proxy,
           ObUnitManager &unit_manager);
  int try_rebuild_tablet(const obrpc::ObRebuildTabletArg &arg) const;
private:
  bool is_server_alive_(const common::ObAddr &server) const;
  int get_tenant_server_list_(uint64_t tenant_id,
                             common::ObIArray<common::ObAddr> &target_server_list) const;
  int check_rebuild_src_and_dest_(
      const obrpc::ObRebuildTabletArg &arg,
      const common::ObIArray<common::ObAddr> &target_server_list) const;
  int check_rebuild_ls_exist_(
      const obrpc::ObRebuildTabletArg &arg) const;
  int do_rebuild_tablet_(
      const obrpc::ObRebuildTabletArg &arg) const;
private:
  bool inited_;
  bool stopped_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  ObUnitManager *unit_manager_;
};

}
}

#endif /* OCEANBASE_ROOTSERVER_OB_ROOT_MINOR_FREEZE_H_ */
