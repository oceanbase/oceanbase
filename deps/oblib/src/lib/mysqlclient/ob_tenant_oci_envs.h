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

#ifndef OBTENANOCIENVS_H
#define OBTENANOCIENVS_H
namespace oceanbase
{
namespace common
{
namespace sqlclient
{
#ifndef OB_BUILD_DBLINK
class ObTenantOciEnvs
{
public:
  static int mtl_new(ObTenantOciEnvs *&tenant_oci_envs) { return OB_SUCCESS; }
  static int mtl_init(ObTenantOciEnvs *&tenant_oci_envs) { return OB_SUCCESS; }
  static void mtl_destroy(ObTenantOciEnvs *&tenant_oci_envs) { }
};
class ObTenantDblinkKeeper
{
public:
  static int mtl_new(ObTenantDblinkKeeper *&dblink_keeper) { return OB_SUCCESS; }
  static int mtl_init(ObTenantDblinkKeeper *&dblink_keeper) { return OB_SUCCESS; }
  static void mtl_destroy(ObTenantDblinkKeeper *&dblink_keeper) { }
};
#endif

} //sqlclient
} // namespace common
} // namespace oceanbase
#endif //OBTENANOCIENVS_H