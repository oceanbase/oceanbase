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

#ifndef _OBMP_UTILS_H_
#define _OBMP_UTILS_H_
#include <stdint.h>

namespace oceanbase {
namespace obmysql {
class OMPKOK;
}
namespace share {
class ObFeedbackRerouteInfo;
}
namespace sql {
class ObSQLSessionInfo;
}
namespace common {
class ObTimeZoneInfo;
class ObString;
class ObIAllocator;
class ObObj;
}  // namespace common
namespace observer {
class ObMPUtils {
public:
  static int add_changed_session_info(obmysql::OMPKOK& ok_pkt, sql::ObSQLSessionInfo& session);
  static int add_session_info_on_connect(obmysql::OMPKOK& okp, sql::ObSQLSessionInfo& session);
  static int add_client_feedback(obmysql::OMPKOK& ok_pkt, sql::ObSQLSessionInfo& session);
  static int add_client_reroute_info(
      obmysql::OMPKOK& pk_pkt, sql::ObSQLSessionInfo& session, share::ObFeedbackRerouteInfo& reroute_info);
  static int add_nls_format(obmysql::OMPKOK& pk_pkt, sql::ObSQLSessionInfo& session, const bool only_changed = false);

private:
  static int get_plain_str_literal(
      common::ObIAllocator& allocator, const common::ObObj& obj, common::ObString& value_str);

  static int get_user_sql_literal(common::ObIAllocator& allocator, const common::ObObj& obj,
      common::ObString& value_str, const common::ObTimeZoneInfo* tz_info);
  static int get_literal_print_length(const common::ObObj& obj, bool is_plain, int64_t& len);
};
}  // end of namespace observer
}  // end of namespace oceanbase

#endif /* _OBMP_UTILS_H_ */
