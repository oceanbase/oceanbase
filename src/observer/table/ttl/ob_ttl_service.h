/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_TABLE_OB_TTL_SERVICE_DEFINE_H_
#define OCEANBASE_OBSERVER_TABLE_OB_TTL_SERVICE_DEFINE_H_
#include "share/ob_ls_id.h"
#include "logservice/ob_log_base_type.h"
#include "share/scn.h"
#include "lib/lock/ob_recursive_mutex.h"
#include "observer/table/ttl/ob_tenant_ttl_manager.h"
namespace oceanbase
{
namespace table
{
class ObTTLService : public logservice::ObIReplaySubHandler,
                     public logservice::ObICheckpointSubHandler,
                     public logservice::ObIRoleChangeSubHandler
{
public:
  ObTTLService()
    : is_inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      tenant_ttl_mgr_(nullptr),
      has_start_(false)
  {}
  virtual ~ObTTLService();
  int init(const uint64_t tenant_id);
  int flush(share::SCN &rec_scn)
  {
    UNUSED(rec_scn);
    return OB_SUCCESS;
  }
  share::SCN get_rec_scn() override { return share::SCN::max_scn(); }

  // for replay, do nothing
  int replay(const void *buffer,
             const int64_t buf_size,
             const palf::LSN &lsn,
             const share::SCN &scn)
  {
    UNUSED(buffer);
    UNUSED(buf_size);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }
  static int mtl_init(ObTTLService *&service);
  // switch leader
  void switch_to_follower_forcedly();
  int switch_to_leader();
  int switch_to_follower_gracefully();
  int resume_leader() { return switch_to_leader(); }
  int launch_ttl_task(const obrpc::ObTTLRequestArg &req);
  uint64_t get_tenant_id() const { return tenant_id_; }
  int start() { return OB_SUCCESS; }
  void stop();
  void wait();
  void destroy();
private:
  int check_inner_stat();
  int alloc_tenant_ttl_mgr();
  void delete_tenant_ttl_mgr();
  void inner_switch_to_follower();
private:
  bool is_inited_;
  int64_t tenant_id_;
  ObTenantTTLManager *tenant_ttl_mgr_;
  bool has_start_;
};
}
}
#endif // OCEANBASE_OBSERVER_TABLE_OB_TTL_SERVICE_DEFINE_H_
