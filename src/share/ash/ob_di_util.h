/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_DI_UTIL_H_
#define OB_DI_UTIL_H_

#include "lib/stat/ob_diagnostic_info_container.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_di_cache.h"

namespace oceanbase
{
namespace share
{

class ObDiagnosticInfoUtil
{
public:
  static int get_the_diag_info(int64_t session_id, ObDISessionCollect &diag_infos);
  static int get_all_diag_info(ObIArray<std::pair<uint64_t, ObDISessionCollect>> &diag_infos, int64_t tenant_id = OB_SYS_TENANT_ID);
  static int get_the_diag_info(uint64_t tenant_id, ObDiagnoseTenantInfo &diag_info);
  static int get_group_diag_info(uint64_t tenant_id,
      ObArray<std::pair<int64_t, common::ObDiagnoseTenantInfo>> &diag_infos,
      common::ObIAllocator *alloc);
  static int release_diag_info_array(ObArray<ObDiagnosticInfo *> &running_di_array, ObArray<ObDiagnosticInfo *> &global_di_array, int64_t tenant_id);

private:
  ObDiagnosticInfoUtil() = default;
  ~ObDiagnosticInfoUtil() = default;
  DISABLE_COPY_ASSIGN(ObDiagnosticInfoUtil);
};

}  // namespace share
}  // namespace oceanbase

#endif /* OB_DI_UTIL_H_ */