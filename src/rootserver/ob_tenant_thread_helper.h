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

#ifndef OCEANBASE_ROOTSERVER_OB_TENANT_THREAD_HELPER_H
#define OCEANBASE_ROOTSERVER_OB_TENANT_THREAD_HELPER_H
#include "lib/thread/ob_reentrant_thread.h"//ObRsReentrantThread
#include "logservice/ob_log_base_type.h"
#include "share/ob_thread_mgr.h" //OBTGDefIDEnum
#include "lib/thread/thread_mgr_interface.h"          // TGRunnable
#include "lib/lock/ob_thread_cond.h"//ObThreadCond
#include "common/ob_zone.h"//ObZone


namespace oceanbase
{
namespace obrpc
{
class  ObSrvRpcProxy;
}
namespace common
{
class ObSqlString;
class ObMySQLTransaction;
}
namespace share
{
class ObAllTenantInfo;
class ObLSRecoveryStat;
namespace schema
{
class ObTenantSchema;
}
}
namespace logservice
{
class ObLogHandler;
}

namespace rootserver
{
class ObTenantThreadHelper : public lib::TGRunnable,
  public logservice::ObIRoleChangeSubHandler
{
public:
  ObTenantThreadHelper() : tg_id_(-1), thread_cond_(), is_created_(false), is_first_time_to_start_(true), thread_name_("") {}
  virtual ~ObTenantThreadHelper() {}
  virtual void do_work() = 0;
  virtual void run1() override;
  virtual void destroy();
  int start();
  void stop();
  void wait();
  void mtl_thread_stop();
  void mtl_thread_wait();
  int create(const char* thread_name, int tg_def_id, ObTenantThreadHelper &tenant_thread);
  void idle(const int64_t idle_time_us);
  void wakeup();
public:
  virtual void switch_to_follower_forcedly() override;

  virtual int switch_to_leader() override;
  virtual int switch_to_follower_gracefully() override
  {
    stop();
    return OB_SUCCESS;
  }
  virtual int resume_leader() override
  {
    return OB_SUCCESS;
  }
public:
#define DEFINE_MTL_FUNC(TYPE)\
  static int mtl_init(TYPE *&ka) {\
    int ret = OB_SUCCESS;\
    if (OB_ISNULL(ka)) {\
      ret = OB_ERR_UNEXPECTED;\
    } else if (OB_FAIL(ka->init())) {\
    }\
    return ret;\
  }\
  static void mtl_stop(TYPE *&ka) {\
    if (OB_NOT_NULL(ka)) {\
      ka->mtl_thread_stop();\
    }\
  }\
  static void mtl_wait(TYPE *&ka) {\
    if (OB_NOT_NULL(ka)) {\
      ka->mtl_thread_wait();\
    }\
  }


 static int get_tenant_schema(const uint64_t tenant_id,
                              share::schema::ObTenantSchema &tenant_schema);
 static int get_zone_priority(const ObZone &primary_zone,
                                 const share::schema::ObTenantSchema &tenant_schema,
                                 common::ObSqlString &primary_zone_str);
protected:
 int wait_tenant_schema_and_version_ready_(
     const uint64_t tenant_id, const uint64_t &data_version);
 int wait_tenant_data_version_ready_(
     const uint64_t tenant_id, const uint64_t &data_version);
 int check_can_do_recovery_(const uint64_t tenant_id);
 int tg_id_;
private:
  common::ObThreadCond thread_cond_;
  bool is_created_;
  bool is_first_time_to_start_;
  const char* thread_name_;
};


}
}


#endif /* !OCEANBASE_ROOTSERVER_OB_TENANT_THREAD_HELPER_H */
