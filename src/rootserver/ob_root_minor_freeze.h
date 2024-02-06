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

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_MINOR_FREEZE_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_MINOR_FREEZE_H_

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

class ObRootMinorFreeze
{
public:
  ObRootMinorFreeze();
  virtual ~ObRootMinorFreeze();

  int init(obrpc::ObSrvRpcProxy &rpc_proxy,
           ObUnitManager &unit_manager);
  void start();
  void stop();
  int destroy();
  int try_minor_freeze(const obrpc::ObRootMinorFreezeArg &arg) const;
private:
  typedef struct MinorFreezeParam
  {
    common::ObAddr server;
    obrpc::ObMinorFreezeArg arg;

    TO_STRING_KV(K(server), K(arg));
  } MinorFreezeParam;

  class ParamsContainer
  {
  public:
    void reset() { params_.reset(); }
    bool is_empty() const { return params_.count() <= 0; }
    const common::ObIArray<MinorFreezeParam> &get_params() const { return params_; }

    int push_back_param(const common::ObAddr &server,
                        const uint64_t tenant_id = 0,
                        share::ObLSID ls_id = share::INVALID_LS,
                        const common::ObTabletID &tablet_id = ObTabletID(ObTabletID::INVALID_TABLET_ID));

    TO_STRING_KV(K_(params));
  private:
    common::ObSEArray<MinorFreezeParam, 32> params_;
  };

  static const int64_t MAX_FREEZE_OP_RETRY_CNT = 5;
  static const int64_t MINOR_FREEZE_TIMEOUT = (1000 * 30 + 1000) * 1000; // copy from major freeze

  int is_server_belongs_to_zone(const common::ObAddr &addr,
                                const common::ObZone &zone,
                                bool &server_in_zone) const;

  int init_params_by_ls_or_tablet(const uint64_t tenant_id,
                                  share::ObLSID ls_id,
                                  const common::ObTabletID &tablet_id,
                                  ParamsContainer &params) const;
  int init_params_by_tenant(const common::ObIArray<uint64_t> &tenant_ids,
                            const common::ObZone &zone,
                            const common::ObIArray<common::ObAddr> &server_list,
                            ParamsContainer &params) const;

  int init_params_by_zone(const common::ObZone &zone,
                          ParamsContainer &params) const;

  int init_params_by_server(const common::ObIArray<common::ObAddr> &server_list,
                            ParamsContainer &params) const;

  int do_minor_freeze(const ParamsContainer &params) const;

  int check_cancel() const;
  bool is_server_alive(const common::ObAddr &server) const;
  int get_tenant_server_list(uint64_t tenant_id,
                             common::ObIArray<common::ObAddr> &target_server_list) const;


  bool inited_;
  bool stopped_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  ObUnitManager *unit_manager_;
};

}
}

#endif /* OCEANBASE_ROOTSERVER_OB_ROOT_MINOR_FREEZE_H_ */
