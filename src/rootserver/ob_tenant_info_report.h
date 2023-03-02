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

#ifndef OCEANBASE_ROOTSERVER_OB_TENANT_INFO_REPORT_H
#define OCEANBASE_ROOTSERVER_OB_TENANT_INFO_REPORT_H
#include "lib/thread/ob_reentrant_thread.h"//ObRsReentrantThread
#include "logservice/ob_log_base_type.h"
#include "share/scn.h"//SCN
#include "share/ob_thread_mgr.h" //OBTGDefIDEnum
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
class ObLSTableOperator;
class SCN;

}
namespace palf
{
struct PalfBaseInfo;
}
namespace rootserver
{
/*description:
 * report __all_tenant_info
 */
class ObTenantInfoReportor : public ObTenantThreadHelper,
                           public logservice::ObICheckpointSubHandler,
                           public logservice::ObIReplaySubHandler
{
public:
  ObTenantInfoReportor():inited_(false), tenant_id_(OB_INVALID_TENANT_ID) {}
  virtual ~ObTenantInfoReportor() {}
  int init();
  void destroy();
  virtual void do_work() override;
  DEFINE_MTL_FUNC(ObTenantInfoReportor)

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
  int gather_tenant_recovery_stat_();
private:
  bool inited_;
  uint64_t tenant_id_;

};
}
}


#endif /* !OCEANBASE_ROOTSERVER_OB_TENANT_INFO_REPORT_H */
