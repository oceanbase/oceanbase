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

#ifndef OCEANBASE_OBSERVER_MYSQL_QUERY_DRIVER_
#define OCEANBASE_OBSERVER_MYSQL_QUERY_DRIVER_

#include "share/ob_define.h"
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_string.h"

namespace oceanbase {

namespace sql {
class ObSqlCtx;
class ObSQLSessionInfo;
class ObResultSet;
}  // namespace sql

namespace observer {

class ObIMPPacketSender;
class ObGlobalContext;
class ObMySQLResultSet;
class ObQueryRetryCtrl;
class ObQueryDriver {
public:
  ObQueryDriver(const ObGlobalContext& gctx, const sql::ObSqlCtx& ctx, sql::ObSQLSessionInfo& session,
      ObQueryRetryCtrl& retry_ctrl, ObIMPPacketSender& sender)
      : gctx_(gctx), ctx_(ctx), session_(session), retry_ctrl_(retry_ctrl), sender_(sender)
  {}
  virtual ~ObQueryDriver()
  {}

  virtual int response_result(ObMySQLResultSet& result) = 0;
  virtual int response_query_header(
      sql::ObResultSet& result, bool has_nore_result = false, bool need_set_ps_out = false);
  int convert_string_charset(const common::ObString& in_str, const common::ObCollationType in_cs_type,
      const common::ObCollationType out_cs_type, char* buf, int32_t buf_len, uint32_t& result_len);
  int convert_string_value_charset(common::ObObj& value, sql::ObResultSet& result);
  int convert_string_value_charset(
      common::ObObj& value, common::ObCharsetType charset_type, common::ObIAllocator& allocator);
  int convert_lob_locator_to_longtext(common::ObObj& value, sql::ObResultSet& result);
  int convert_lob_value_charset(common::ObObj& value, sql::ObResultSet& result);

private:
  int convert_field_charset(common::ObIAllocator& allocator, const common::ObCollationType& from_collation,
      const common::ObCollationType& dest_collation, const common::ObString& from_string,
      common::ObString& dest_string);
  int like_match(const char* str, int64_t length_str, int64_t i, const char* pattern, int64_t length_pat, int64_t j,
      bool& is_match);

protected:
  /* variables */
  const ObGlobalContext& gctx_;
  const sql::ObSqlCtx& ctx_;
  sql::ObSQLSessionInfo& session_;
  ObQueryRetryCtrl& retry_ctrl_;
  ObIMPPacketSender& sender_;
  /* const */
  /* disallow copy & assign */
  DISALLOW_COPY_AND_ASSIGN(ObQueryDriver);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* OCEANBASE_OBSERVER_MYSQL_QUERY_DRIVER_ */
//// end of header file
