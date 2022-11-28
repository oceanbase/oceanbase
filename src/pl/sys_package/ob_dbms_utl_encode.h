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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_UTL_ENCODE_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_UTL_ENCODE_H_

#include "sql/engine/ob_exec_context.h"
#include "lib/charset/ob_charset.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSUtlEncode
{
public:
  struct UUEncodeTypeFlag {
    UUEncodeTypeFlag(const int64_t type);
    bool has_head_;
    bool has_text_;
    bool has_end_;
  };
  static int nls_id_name_by_charset(const ObCharsetType &charset, ObString &nls_id_name);
  static int base64_encode(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int base64_decode(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int quoted_printable_encode(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int quoted_printable_decode(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int mimeheader_encode(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int mimeheader_decode(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int text_encode(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int text_decode(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int uu_encode(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int uu_decode(
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  //按照charset 为quoted_prinatble编码结果增加首尾和换行符, org_charset保留了用户输入字段的原始大小写
  static int formalize_for_mime_quoted_printable(const uint8_t *input, const int64_t input_len,
                                                 const ObString &org_charset, uint8_t *output,
                                                 const int64_t output_len, int64_t &pos);
  static int formalize_for_text_quoted_printable(const uint8_t *input,
                                                 const int64_t input_len,
                                                 uint8_t *output,
                                                 const int64_t output_len,
                                                 int64_t &pos);
  //按照charset 为base64编码结果增加首尾和换行符
  static int formalize_for_mime_base64(const uint8_t *input, const int64_t input_len,
                                       const ObString &org_charset, uint8_t *output,
                                       const int64_t output_len, int64_t &pos,
                                       const bool is_utf16 = false);
  static int formalize_for_text_base64(const uint8_t *input, const int64_t input_len,
                                       uint8_t *output,const int64_t output_len, int64_t &pos);
  static int formalize_for_uuencode(const UUEncodeTypeFlag &encode_type, const ObString & filename,
                                    const ObString &permission, const uint8_t *input,
                                    const int64_t input_len, const int64_t padding, uint8_t *output,
                                    const int64_t output_len, int64_t &pos);
  static int add_header(const ObString &org_charset, const int64_t encoding,
                        const int64_t output_len,  uint8_t *output, int64_t &pos);
  static int add_text_for_base64(const uint8_t *input, const int64_t input_len,
                                 const int64_t capacity, const int64_t output_len,
                                 uint8_t *output, int64_t &input_pos, int64_t &output_pos,
                                 bool &is_end);
  static int add_text_for_quoted_printable(const uint8_t *input, const int64_t input_len,
                                        const int64_t encode_charset_len, const int64_t output_len,
                                        uint8_t *output, int64_t &input_pos, int64_t &output_pos,
                                        bool &is_end, bool &is_first_line);
  static int add_tail(uint8_t *output, const int64_t &output_len,
                      int64_t &pos, const bool is_end, const int64_t encoding);
  static int add_crlf(uint8_t *output, const int64_t &output_len, int64_t &pos, const bool is_end);

  static int add_text_for_quoted_printable_text(const uint8_t *input, const int64_t input_len,
                                        int64_t output_len, uint8_t *output, int64_t &input_pos,
                                        int64_t &output_pos, bool &is_end);
  static int add_head_for_uuencode(const ObString &filename, const ObString &permission,
                                   uint8_t *output, const int64_t output_len, int64_t &pos);
private:
  static bool check_output_buffer_is_enough(const int64_t input_len, const int64_t output_len,
                                            const int64_t head_len, const int64_t tail_len,
                                            const int64_t encoding, const bool is_utf16 = false);
  static bool check_output_buffer_is_enough_for_text(const int64_t input_len,
                                                     const int64_t output_len,
                                                     const int64_t tail_len,
                                                     const int64_t encoding);
  static int process_encoded_string_from_mime(const uint8_t *input, const int64_t input_len,
                                              int64_t &encoding, ObString &charset_name,
                                              uint8_t *output, int64_t &output_len);
  static int process_encoded_string_from_text(const uint8_t *input, const int64_t input_len,
                                              uint8_t *output, int64_t &output_len,
                                              const int64_t encoding);
  static int process_encoded_string_from_uu(const uint8_t *input, const int64_t input_len,
                                            uint8_t *output, int64_t &output_len);
  const static uint8_t ESCAPE_CHAR = 61;
  const static uint8_t TAB = 9;
  const static uint8_t SPACE = 32;
  const static uint8_t QUESTIONMARK = 63;
  const static uint8_t UUEND = 95;

  static const int64_t NCHAR_PER_BASE64 = 4;
  static const int64_t NCHAR_PER_BASE64_GROUP = 3;
  //对于编码内容， QUOTED_PRINTABLE会进行软换行(区别于编码之前文本中固有的换行符)
  static const int64_t NUM_OF_CHAR_FOR_SOFT_BREAK = 3;
  //QUOTED_PRINTABLE 添加软换行的最大行长
  static const int64_t NUM_OF_CHAR_PER_LINE_QUOTED_PRINTABLE = 76;
  //QUOTED_PRINTABLE 至多将文本膨胀3倍
  static const int64_t EXPANSION_FACTOR_QUOTE_PRINTABLE = 3;
  static const int64_t BASE64_ENCODE = 1;
  static const int64_t QUOTED_PRINTABLE_ENCODE = 2;
  static const int64_t MIME_HEADER_ENCODE = 3;
  static const int64_t TEXT_ENCODE = 4;
  //MIME HEADER同样会添加软换行
  static const int64_t MAX_NUM_OF_CHAR_FOR_MIME_BASE64 = 63;
  //MIME HEADER的行长规则比较复杂 https://yuque.antfin-inc.com/ob/sql/nfnzce
  static const int64_t MAX_NUM_OF_CHAR_FOR_MIME_QUOTED_PRINTABLE_FIRST_LINE = 74;
  static const int64_t MAX_NUM_OF_CHAR_FOR_MIME_QUOTED_PRINTABLE = 75;
  static const int64_t MAX_NUM_OF_CHAR_FOR_MIME_QUOTED_PRINTABLE_EQUAL = 77;
  static const int64_t MAX_NUM_OF_CHAR_FOR_MIME_HEADER = 17; //=?ZHS32GB18030?Q?
  static const int64_t MAX_NUM_OF_CHAR_FOR_MIME_TAIL = 4; //?=CRLF
  static const int64_t MAX_NUM_OF_CHAR_FOR_TEXT_BASE64 = 64;
  static const int64_t MAX_NUM_OF_CHAR_FOR_TEXT_QUOTED_PRINTABLE = 75;
  static const int64_t UUENCODE_TYPE_COMPLETE = 1;
  static const int64_t UUENCODE_TYPE_WITHOUT_END = 2;
  static const int64_t UUENCODE_TYPE_ONLY_TEXT = 3;
  static const int64_t UUENCODE_TYPE_WITHOUT_HEAD = 4;
  static const int64_t UUENCODE_MAX_HEAD_LEN = 74;
  static const int64_t UUENCODE_TEXT_CAPACITY = 76;
  static const int64_t MAX_NLS_CHARSET_ID_LEN = 20; //for ZHS16CGB231280FIXED
};

} // end of pl
} // end of oceanbase

#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_UTL_ENCODE_H_ */