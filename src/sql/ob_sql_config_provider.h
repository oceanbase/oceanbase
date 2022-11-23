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
