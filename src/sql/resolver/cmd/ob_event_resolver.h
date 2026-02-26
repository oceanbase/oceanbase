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

#ifndef _OB_EVENT_RESOLVER_H
#define _OB_EVENT_RESOLVER_H 1

#include "sql/resolver/cmd/ob_cmd_resolver.h"
#include "sql/resolver/cmd/ob_event_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObEventResolver: public ObCMDResolver
{
public:
  explicit ObEventResolver(ObResolverParams &params);
  virtual ~ObEventResolver() = default;

  virtual int resolve(const ParseNode &parse_tree);
private:
  static const int OB_EVENT_DEFINER_MAX_LEN = common::OB_MAX_USER_NAME_LENGTH + common::OB_MAX_HOST_NAME_LENGTH + 2;
  static const int OB_EVENT_NAME_MAX_LEN = 128;
  static const int OB_EVENT_REPEAT_MAX_LEN = 128;
  static const int OB_EVENT_SQL_MAX_LEN = 16 * 1024;
  static const int OB_EVENT_COMMENT_MAX_LEN = 4096;
  static const int OB_EVENT_BODY_MAX_LEN = 64 * 1024;
  static const int OB_EVENT_INTERVAL_MAX_VALUE = 1000000000L;

  ObItemType stmt_type_;

  int resolve_create_event_stmt(const ParseNode &parse_node, ObEventInfo &event_info);
  int resolve_alter_event_stmt(const ParseNode &parse_node, ObEventInfo &event_info);
  int resolve_drop_event_stmt(const ParseNode &parse_node, ObEventInfo &event_info);
  int resolve_event_definer(const ParseNode *parse_node, ObEventInfo &event_info);
  int resolve_event_exist(const ParseNode *parse_node, ObEventInfo &event_info);
  int resolve_event_name(const ParseNode *parse_node, ObEventInfo &event_info);
  int resolve_event_schedule(const ParseNode *parse_node, ObEventInfo &event_info);
  int resolve_event_preserve(const ParseNode *parse_node, ObEventInfo &event_info);
  int resolve_event_enable(const ParseNode *parse_node, ObEventInfo &event_info);
  int resolve_event_comment(const ParseNode *parse_node, ObEventInfo &event_info);
  int resolve_event_body(const ParseNode *parse_node, ObEventInfo &event_info);
  int resolve_schedule_and_comple(const ParseNode *parse_node, ObEventInfo &event_info);
  int resolve_event_rename(const ParseNode *parse_node, ObEventInfo &event_info);

  int get_event_exec_env(ObEventInfo &event_info);
  int get_event_time_node_value(const ParseNode *parse_node, int64_t &time_us);
  int get_repeat_interval(const ParseNode *repeat_num_node, const ParseNode *repeat_type_node, char *repeat_interval_str, int64_t &max_run_duration);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObEventResolver);
};
}//namespace sql
}//namespace oceanbase
#endif // _OB_EVENT_RESOLVER_H
