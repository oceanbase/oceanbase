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

#ifndef _OB_CREATE_EVENT_RESOLVER_H
#define _OB_CREATE_EVENT_RESOLVER_H 1

#include "sql/resolver/cmd/ob_cmd_resolver.h"
#include "sql/resolver/cmd/ob_create_event_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObCreateEventResolver: public ObCMDResolver
{
public:
  explicit ObCreateEventResolver(ObResolverParams &params);
  virtual ~ObCreateEventResolver() = default;

  virtual int resolve(const ParseNode &parse_tree);
private:
  static const int OB_EVENT_NAME_MAX_LENGTH = 128;
  static const int OB_EVENT_REPEAT_MAX_LENGTH = 128;
  static const int OB_EVENT_SQL_MAX_LENGTH = 16 * 1024;
  static const int OB_EVENT_COMMENT_MAX_LENGTH = 4096;
  static const int OB_EVENT_BODY_MAX_LENGTH = 64 * 1024;
  static const int OB_EVEX_MAX_INTERVAL_VALUE = 1000000000L;
  int resolve_create_event_(const ParseNode *create_event_node);
  int get_time_us_(const ParseNode *time_node, int64_t &time_us);
  int get_time_us_from_sql_(const char *sql, int64_t &time_us);
  int get_repeat_interval_(const ParseNode *repeat_num_node, const ParseNode *repeat_type_node, char *repeat_interval_str, int64_t &repeat_ts);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateEventResolver);
};
}//namespace sql
}//namespace oceanbase
#endif // _OB_CREATE_EVENT_RESOLVER_H
