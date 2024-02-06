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
#include "deps/oblib/src/common/ob_field.h"

namespace oceanbase
{

namespace sql
{
struct ObSqlCtx;
class ObSQLSessionInfo;
class ObResultSet;
}


namespace observer
{

class ObIMPPacketSender;
struct ObGlobalContext;
class ObMySQLResultSet;
class ObQueryRetryCtrl;
class ObQueryDriver
{
public:
  static const int64_t RESET_CONVERT_CHARSET_ALLOCATOR_EVERY_X_ROWS = 32;
public:
  ObQueryDriver(const ObGlobalContext &gctx,
                const sql::ObSqlCtx &ctx,
                sql::ObSQLSessionInfo &session,
                ObQueryRetryCtrl &retry_ctrl,
                ObIMPPacketSender &sender,
                bool is_prexecute = false)
    : gctx_(gctx),
      ctx_(ctx),
      session_(session),
      retry_ctrl_(retry_ctrl),
      sender_(sender),
      is_prexecute_(is_prexecute)
  {
  }
  virtual ~ObQueryDriver()
  {
  }

  virtual int response_result(ObMySQLResultSet &result) = 0;
  virtual int response_query_header(sql::ObResultSet &result,
                            bool has_more_result,
                            bool need_set_ps_out_flag,
                            bool need_flush_buffer = false);
  virtual int response_query_result(sql::ObResultSet &result,
                                    bool is_ps_protocol,
                                    bool has_more_result,
                                    bool &can_retry,
                                    int64_t fetch_limit  = common::OB_INVALID_COUNT);
  int response_query_header(const ColumnsFieldIArray &fields,
                                    bool has_more_result = false,
                                    bool need_set_ps_out = false,
                                    bool ps_cursor_execute = false,
                                    sql::ObResultSet *result = NULL);
  int convert_string_value_charset(common::ObObj& value, sql::ObResultSet &result);
  int convert_string_value_charset(common::ObObj& value, 
                                   common::ObCharsetType charset_type, 
                                   common::ObIAllocator &allocator);
  int convert_lob_locator_to_longtext(common::ObObj& value, sql::ObResultSet &result);
  int process_lob_locator_results(common::ObObj& value, sql::ObResultSet &result);
  int process_sql_udt_results(common::ObObj& value, sql::ObResultSet &result);
  int convert_lob_value_charset(common::ObObj& value, sql::ObResultSet &result);
  int convert_text_value_charset(common::ObObj& value, sql::ObResultSet &result);
  static int convert_lob_locator_to_longtext(common::ObObj& value, 
                                             bool is_use_lob_locator, 
                                             common::ObIAllocator *allocator);
  static int process_lob_locator_results(common::ObObj& value,
                                         bool is_use_lob_locator,
                                         bool is_support_outrow_locator_v2,
                                         common::ObIAllocator *allocator,
                                         const sql::ObSQLSessionInfo *session_info);

  static int process_sql_udt_results(common::ObObj& value,
                                     common::ObIAllocator *allocator,
                                     sql::ObSQLSessionInfo *session_info);

  static int convert_string_charset(const common::ObString &in_str, 
                                    const common::ObCollationType in_cs_type,
                                    const common::ObCollationType out_cs_type, 
                                    char *buf, int32_t buf_len, uint32_t &result_len);
  static int convert_lob_value_charset(common::ObObj& value, 
                                       common::ObCharsetType charset_type, 
                                       common::ObIAllocator &allocator);
  static int convert_text_value_charset(ObObj& value,
                                        ObCharsetType charset_type,
                                        ObIAllocator &allocator,
                                        const sql::ObSQLSessionInfo *session_info);
private:
  int convert_field_charset(common::ObIAllocator& allocator,
      const common::ObCollationType& from_collation,
      const common::ObCollationType& dest_collation,
      const common::ObString &from_string,
      common::ObString &dest_string);
  int like_match(const char* str, int64_t length_str, int64_t i,
                 const char* pattern, int64_t length_pat, int64_t j,
                 bool &is_match);
  int is_com_filed_list_match_wildcard_str(sql::ObResultSet &result,
                                           const common::ObCollationType &from_collation,
                                           const common::ObString &from_string,
                                           bool &is_not_match);

protected:
  /* variables */
  const ObGlobalContext &gctx_;
  const sql::ObSqlCtx &ctx_;
  sql::ObSQLSessionInfo &session_;
  ObQueryRetryCtrl &retry_ctrl_;
  ObIMPPacketSender &sender_;
  bool is_prexecute_;
  /* const */
  /* disallow copy & assign */
  DISALLOW_COPY_AND_ASSIGN(ObQueryDriver);
};


}
}
#endif /* OCEANBASE_OBSERVER_MYSQL_QUERY_DRIVER_ */
//// end of header file
