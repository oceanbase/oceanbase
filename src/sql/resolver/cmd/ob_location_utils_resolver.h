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

#ifndef _OB_LOCATION_UTILS_RESOLVER_H
#define _OB_LOCATION_UTILS_RESOLVER_H
#include "sql/resolver/cmd/ob_cmd_resolver.h"

namespace oceanbase
{
namespace sql
{
// resolver for both SET NAMES and SET CHARSET
class ObLocationUtilsResolver : public ObCMDResolver
{
public:
  static const int64_t LOCATION_NAME = 0;
  static const int64_t SUB_PATH = 1;
  static const int64_t PATTERN = 2;
  explicit ObLocationUtilsResolver(ObResolverParams &params);
  virtual ~ObLocationUtilsResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObLocationUtilsResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif // _OB_LOCATION_UTILS_RESOLVER_H
