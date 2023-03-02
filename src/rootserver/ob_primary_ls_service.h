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

#ifndef OCEANBASE_ROOTSERVER_OB_PRIMARY_LS_SERVICE_H
#define OCEANBASE_ROOTSERVER_OB_PRIMARY_LS_SERVICE_H
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
class ObClusterVersion;
}
namespace share
{
class ObLSTableOperator;
class SCN;
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
 *Log stream management thread: Started on the leader of the system log stream
 * under each tenant. Under the meta tenant, the primary_zone information of
 * each log stream needs to be adjusted. Under the user tenant, log streams need
 * to be created and deleted according to the changes of primary_zone and
 * unit_num. And since the update of __all_ls and __all_ls_status is not atomic,
 * it is also necessary to deal with the mismatch problem caused by this
 * non-atomic. When the tenant is in the deletion state, it is also necessary to
 * advance the state of each log stream to the end.*/
class ObPrimaryLSService : public ObTenantThreadHelper,
                           public logservice::ObICheckpointSubHandler,
                           public logservice::ObIReplaySubHandler 
{
public:
  ObPrimaryLSService():inited_(false), tenant_id_(OB_INVALID_TENANT_ID) {}
  virtual ~ObPrimaryLSService() {}
  static int mtl_init(ObPrimaryLSService *&ka);
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

  int process_user_tenant_thread0_(const share::schema::ObTenantSchema &tenant_schema);
  int process_user_tenant_thread1_(palf::PalfBufferIterator &iterator,
                                  share::SCN &start_scn);
private:
  bool inited_;
  uint64_t tenant_id_;

};
}
}


#endif /* !OCEANBASE_ROOTSERVER_OB_PRIMARY_LS_SERVICE_H */
