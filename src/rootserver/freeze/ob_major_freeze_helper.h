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

#ifndef OCEANBASE_ROOTSERVER_FREEZE_OB_MAJOR_FREEZE_HELPER_H_
#define OCEANBASE_ROOTSERVER_FREEZE_OB_MAJOR_FREEZE_HELPER_H_
#include "rpc/frame/ob_req_transport.h"
#include "rootserver/freeze/ob_major_freeze_rpc_define.h"
#include "share/scn.h"

namespace oceanbase
{
namespace share
{
class SCN;
class ObSimpleFrozenStatus;
}
namespace rootserver
{
struct ObMajorFreezeParam
{
public:
  ObMajorFreezeParam()
    : freeze_info_array_(), freeze_all_(false), transport_(nullptr)
  {}

  void reset()
  {
    freeze_info_array_.reset();
    freeze_all_ = false;
    transport_ = nullptr;
  }

  bool is_valid() const
  {
    return (nullptr != transport_);
  }

  int add_freeze_info(const uint64_t tenant_id);

  TO_STRING_KV(K_(freeze_info_array), K_(freeze_all), KP_(transport));

  common::ObArray<obrpc::ObSimpleFreezeInfo> freeze_info_array_;
  bool freeze_all_;
  rpc::frame::ObReqTransport *transport_;
};

struct ObTenantAdminMergeParam
{
public:
  ObTenantAdminMergeParam()
    : tenant_array_(), need_all_(false), transport_(nullptr)
  {}

  void reset()
  {
    tenant_array_.reset();
    need_all_ = false;
    transport_ = nullptr;
  }

  bool is_valid() const
  {
    return (nullptr != transport_) && (!tenant_array_.empty() || need_all_);
  }

  TO_STRING_KV(K_(tenant_array), K_(need_all), KP_(transport));

  common::ObArray<uint64_t> tenant_array_;
  bool need_all_;
  rpc::frame::ObReqTransport *transport_;
};

class ObMajorFreezeHelper
{
public:
  ObMajorFreezeHelper() {}
  ~ObMajorFreezeHelper() {}

  // @param, contains some tenant_ids which need to launch major freeze
  // @merge_results, save each tenant's major_freeze result
  static int major_freeze(const ObMajorFreezeParam &param,
                          common::ObIArray<int> &merge_results);

  static int major_freeze(const ObMajorFreezeParam &param);

  static int suspend_merge(const ObTenantAdminMergeParam &param);

  static int resume_merge(const ObTenantAdminMergeParam &param);

  static int clear_merge_error(const ObTenantAdminMergeParam &param);

  static int get_frozen_status(const int64_t tenant_id, 
                               const share::SCN &frozen_scn,
                               share::ObSimpleFrozenStatus &frozen_status);
  static int get_frozen_scn(const int64_t tenant_id, share::SCN &frozen_scn);

private:
  static int get_freeze_info(
      const ObMajorFreezeParam &param, 
      common::ObIArray<obrpc::ObSimpleFreezeInfo> &freeze_info_array);
  static int get_all_tenant_freeze_info(
      common::ObIArray<obrpc::ObSimpleFreezeInfo> &freeze_info_array);
  static int check_tenant_is_restore(const uint64_t tenant_id, bool &is_restore);
  
  static int do_major_freeze(
      const rpc::frame::ObReqTransport &transport, 
      const common::ObIArray<obrpc::ObSimpleFreezeInfo> &freeze_info_array,
      common::ObIArray<int> &merge_results);
  static int do_one_tenant_major_freeze(
      const rpc::frame::ObReqTransport &transport, 
      const obrpc::ObSimpleFreezeInfo &freeze_info);
  
  static int do_tenant_admin_merge(
      const ObTenantAdminMergeParam &param, 
      const obrpc::ObTenantAdminMergeType &admin_type);
  static int do_one_tenant_admin_merge(
      const rpc::frame::ObReqTransport &transport, 
      const uint64_t tenant_id,
      const obrpc::ObTenantAdminMergeType &admin_type);

private:
  static const int64_t MAX_PROCESS_TIME_US = 10 * 1000 * 1000L;
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_FREEZE_OB_MAJOR_FREEZE_HELPER_H_
