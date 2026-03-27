/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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