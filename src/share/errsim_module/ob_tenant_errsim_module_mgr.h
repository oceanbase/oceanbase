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

#ifndef SRC_SHARE_ERRSIM_MODULE_OB_TENANT_ERRSIM_MODULE_MGR_H_
#define SRC_SHARE_ERRSIM_MODULE_OB_TENANT_ERRSIM_MODULE_MGR_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "common/errsim_module/ob_errsim_module_type.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_bucket_lock.h"

namespace oceanbase
{
namespace share
{

class ObTenantErrsimModuleMgr
{
public:
  typedef ObFixedLengthString<ObErrsimModuleTypeHelper::MAX_TYPE_NAME_LENGTH> ErrsimModuleString;
  typedef ObArray<ObFixedLengthString<ObErrsimModuleTypeHelper::MAX_TYPE_NAME_LENGTH>> ModuleArray;
public:
  ObTenantErrsimModuleMgr();
  virtual ~ObTenantErrsimModuleMgr();
  static int mtl_init(ObTenantErrsimModuleMgr *&errsim_module_mgr);
  int init(const uint64_t tenant_id);
  int build_tenant_moulde(
      const uint64_t tenant_id,
      const int64_t config_version,
      const ModuleArray &module_array,
      const int64_t percentage);
  bool is_errsim_module(
      const ObErrsimModuleType::TYPE &type);
  void destroy();

private:
  typedef hash::ObHashSet<ObErrsimModuleType> ErrsimModuleSet;
  static const int64_t MAX_BUCKET_NUM = 128;
  bool is_inited_;
  uint64_t tenant_id_;
  common::SpinRWLock lock_;
  int64_t config_version_;
  bool is_whole_module_;
  ErrsimModuleSet module_set_;
  int64_t percentage_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantErrsimModuleMgr);
};

} // namespace share
} // namespace oceanbase
#endif
