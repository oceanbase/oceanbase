/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_SQL_CONFIG_PROVIDER_H
#define _OB_SQL_CONFIG_PROVIDER_H 1

namespace oceanbase
{
namespace sql
{
class ObSQLConfigProvider
{
public:
  ObSQLConfigProvider() {};
  virtual ~ObSQLConfigProvider() {};

  virtual bool is_read_only() const = 0;
  virtual int64_t get_nlj_cache_limit() const = 0;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSQLConfigProvider);
};
} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SQL_CONFIG_PROVIDER_H */
