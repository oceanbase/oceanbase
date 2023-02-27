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

#ifndef OCEANBASE_ROOTSERVER_OB_COMMON_LS_SERVICE_H
#define OCEANBASE_ROOTSERVER_OB_COMMON_LS_SERVICE_H
#include "lib/thread/ob_reentrant_thread.h"//ObRsReentrantThread
#include "logservice/ob_log_base_type.h"
#include "share/scn.h"//SCN
#include "share/ls/ob_ls_status_operator.h"//ObLSStatusOperator
#include "share/ls/ob_ls_operator.h" //ObLSAttr
#include "share/ob_thread_mgr.h" //OBTGDefIDEnum
#include "logservice/palf/palf_iterator.h"          //PalfBufferIterator
#include "share/unit/ob_unit_info.h"//ObUnit::Status
#include "lib/thread/thread_mgr_interface.h"          // TGRunnable
#include "lib/lock/ob_thread_cond.h"//ObThreadCond
#include "rootserver/ob_tenant_thread_helper.h"//ObTenantThreadHelper


namespace oceanbase
{
namespace obrpc
{
class  ObSrvRpcProxy;
}
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
}
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
class ObTenantSchema;
}

}
namespace palf
{
struct PalfBaseInfo;
}
namespace rootserver
{
/*description:
 *COMMON_LS_SERVICE thread: Started on the leader of the meta tenant sys ls
 * 1. adjust sys ls primary zone of meta and user tenant
 * 2. adjust user ls primary zone
 * 3. make ls status from creating to created of __all_ls_status
 * 4. check dropping tenant need drop tenant force;
 * */
class ObCommonLSService : public ObTenantThreadHelper,
                           public logservice::ObICheckpointSubHandler,
                           public logservice::ObIReplaySubHandler
{
public:
  ObCommonLSService():inited_(false), tenant_id_(OB_INVALID_TENANT_ID) {}
  virtual ~ObCommonLSService() {}
  static int mtl_init(ObCommonLSService *&ka);
  int init();
  void destroy();
  virtual void do_work() override;

public:
  virtual share::SCN get_rec_scn() override { return share::SCN::max_scn();}
  virtual int flush(share::SCN &scn) override { return OB_SUCCESS; }
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &scn) override
  {
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }

private:
  int try_update_sys_ls_primary_zone_();
  int try_adjust_user_ls_primary_zone_(const share::schema::ObTenantSchema &tenant_schema);
  int try_update_ls_primary_zone_(
    const share::ObLSPrimaryZoneInfo &primary_zone_info,
    const common::ObZone &new_primary_zone,
    const common::ObSqlString &zone_priority);
  static int set_ls_to_primary_zone(const common::ObIArray<common::ObZone> &primary_zone_array,
                             const share::ObLSPrimaryZoneInfoArray &primary_zone_infos,
                             common::ObIArray<common::ObZone> &ls_primary_zone,
                             common::ObIArray<uint64_t> &count_group_by_zone);
  static int balance_ls_primary_zone(const common::ObIArray<common::ObZone> &primary_zone_array,
                              common::ObIArray<common::ObZone> &ls_primary_zone,
                              common::ObIArray<uint64_t> &count_group_by_zone);
  int adjust_primary_zone_by_ls_group_(const common::ObIArray<common::ObZone> &primary_zone_array,
                                       const share::ObLSPrimaryZoneInfoArray &primary_zone_infos,
                                       const share::schema::ObTenantSchema &tenant_schema);


  // force drop user tenant if tenant is in dropping status
  int try_force_drop_tenant_(
      const share::schema::ObTenantSchema &tenant_schema);
  int try_create_ls_(const share::schema::ObTenantSchema &tenant_schema);
  //restore tenant can not create ls before "CREATE_INIT_LS" status
  int check_can_create_ls_(const share::schema::ObTenantSchema &tenant_schema);
public:
  //restore_service need create init ls too
  static int do_create_user_ls(const share::schema::ObTenantSchema &tenant_schema,
                   const share::ObLSStatusInfo &info,
                   const SCN &create_scn,
                   bool create_with_palf,
                   const palf::PalfBaseInfo &palf_base_info);
private:
  bool inited_;
  uint64_t tenant_id_;

};
}
}


#endif /* !OCEANBASE_ROOTSERVER_OB_COMMON_LS_SERVICE_H */
