/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_CLUSTER_ROLE_H_
#define OCEANBASE_COMMON_OB_CLUSTER_ROLE_H_
namespace oceanbase
{
namespace common
{
enum ObClusterRole
{
  INVALID_CLUSTER_ROLE = 0,
  PRIMARY_CLUSTER = 1,
  STANDBY_CLUSTER = 2,
};
}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_CLUSTER_ROLE_H_
