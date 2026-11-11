/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_SCHEMA_OB_PASSWORD_EXPIRE_OPTION_H_
#define OCEANBASE_SHARE_SCHEMA_OB_PASSWORD_EXPIRE_OPTION_H_

#ifdef __cplusplus
namespace oceanbase
{
namespace share
{
namespace schema
{
#endif

enum ObPasswordExpireOption
{
  PASSWORD_EXPIRE_NONE = -3,
  PASSWORD_EXPIRE_NOW = -2,
  PASSWORD_EXPIRE_DEFAULT = -1,
  PASSWORD_EXPIRE_NEVER = 0,
  PASSWORD_EXPIRE_INTERVAL = 1
};

#ifdef __cplusplus
} // namespace schema
} // namespace share
} // namespace oceanbase
#endif

#endif // OCEANBASE_SHARE_SCHEMA_OB_PASSWORD_EXPIRE_OPTION_H_
