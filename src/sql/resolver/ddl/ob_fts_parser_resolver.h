/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_FTS_PARSER_RESOLVER_
#define OCEANBASE_FTS_PARSER_RESOLVER_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "storage/fts/ob_fts_parser_property.h"
#include "storage/fts/ob_fts_plugin_helper.h"

namespace oceanbase
{
namespace sql
{
class ObFTParserResolverHelper final
{
public:
  ObFTParserResolverHelper() = default;
  ~ObFTParserResolverHelper() = default;

  static int resolve_parser_properties(
      const ParseNode &parse_tree,
      common::ObIAllocator &allocator,
      common::ObString &parser_property);

private:
  static int resolve_fts_index_parser_properties(const ParseNode *node,
                                                 storage::ObFTParserJsonProps &property);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_FTS_PARSER_RESOLVER_ */
