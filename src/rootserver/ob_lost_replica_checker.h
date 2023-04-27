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

#ifndef OCEANBASE_ROOTSERVER_OB_LOST_REPLICA_CHECKER_H_
#define OCEANBASE_ROOTSERVER_OB_LOST_REPLICA_CHECKER_H_

#include "share/ob_define.h"
#include "lib/thread/ob_reentrant_thread.h"//block_run
#include "rootserver/ob_rs_reentrant_thread.h"



namespace oceanbase
{
namespace share
{
class ObLSTableOperator;
class ObLSInfo;
class ObLSReplica;
namespace schema
{
class ObMultiVersionSchemaService;
}//end namespace schema
}//end namespace share

namespace rootserver
{

class ObLostReplicaChecker :  public ObRsReentrantThread 
{
public:
  ObLostReplicaChecker();
  virtual ~ObLostReplicaChecker();

  int init(share::ObLSTableOperator &lst_operator, share::schema::ObMultiVersionSchemaService &schema_service);
  int check_lost_replicas();
  virtual void run3() override;
  virtual int blocking_run() {
     BLOCKING_RUN_IMPLEMENT();
  }
  virtual void wakeup();
  virtual void stop();

private:
  int check_lost_replica_by_ls_(const share::ObLSInfo &ls_info);
  // (server lost and not in member list) or (server lost and not in schema)
  int check_lost_replica_(const share::ObLSInfo &ls_info,
                         const share::ObLSReplica &replica,
                         bool &is_lost_replica) const;
  int check_lost_server_(const common::ObAddr &server,
                        bool &is_lost_server) const;
  int check_cancel_();

 private:
  bool inited_;
  common::ObThreadCond cond_;
  share::ObLSTableOperator *lst_operator_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLostReplicaChecker);
};
}//end namespace rootserver
}//end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_LOST_REPLICA_CHECKER_H_
