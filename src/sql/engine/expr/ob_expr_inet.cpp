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

#define USING_LOG_PREFIX SQL_ENG
#include <string.h>
#include "sql/parser/ob_item_type.h"
#include "sql/engine/expr/ob_expr_inet.h"
#include "sql/engine/ob_exec_context.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
bool ObExprInetCommon::str_to_ipv4(int len, const char *str, in_addr *ipv4addr){
  bool is_ipv4 = true;
  if (7 > len || 15 < len) {
    is_ipv4 = false;
  } else {
    unsigned char *byte_addr = (unsigned char *)ipv4addr;
    int dotcnt = 0, numcnt = 0;
    int num = 0, byte = 0;
    char c;
    for (int i = 0; i < len && *(i + str) && is_ipv4; ++i) {
      c = *(i + str);
      num = c - '0';
      if ('.' == c) {
        if (255 < byte|| 0 == numcnt || 3 < numcnt) {
          is_ipv4 = false;
        } else if (i == len - 1 || !*(i + str + 1)) {
          is_ipv4 = false;
        } else {
          byte_addr[dotcnt] = (unsigned char) byte;
        }
        dotcnt ++;
        if (3 < dotcnt) {
          is_ipv4 = false;
        } else {
        }
        byte = 0;
        numcnt = 0;
      } else if (9 >= num && 0 <= num) {
        byte = byte * 10 + num;
        numcnt ++;
      } else {
        is_ipv4 = false;
      }
    }
    if (255 < byte || 3 != dotcnt) {
      is_ipv4 = false;
    } else {
      byte_addr[3] = (unsigned char) byte;
    }
  }
  return is_ipv4;
}

bool ObExprInetCommon::str_to_ipv6(int len, const char *str, in6_addr *ipv6addr){
  bool is_ipv6 = true;
  //Ipv6 length of mysql support: 2~39
  if(2 > len || 45 < len) {
    is_ipv6 = false;
  } else {
    char *dst = (char *)ipv6addr;
    char c;
    int group = 0, num = 0;
    int numcnt = 0;
    int group_start = 0;
    char *gap_ptr = nullptr;
    char *ipv6addr_end= (char*)ipv6addr + 16;
    memset(ipv6addr, 0, 16);
    int i = 0;
    if (':' == str[0]) {
      if (':' == str[1]) {
        i = 1;
      } else {
        is_ipv6 = false;
      }
    } else {
    }
    for (; i < len && str[i] && is_ipv6; ++i) {
      c = str[i];
      if (':' == c) {
        group_start = i + 1;
        if (0 == numcnt) {
          if (nullptr == gap_ptr) {
            gap_ptr = dst;
          } else {
            is_ipv6 = false;
          }
          continue;
        } else if (i >= len || !str[i+1]) {
          is_ipv6 = false;
        } else if (dst + 2 > ipv6addr_end) {
          is_ipv6 = false;
        } else {
          dst[0] = (unsigned char) (group >> 8) & 0xff;
          dst[1] = (unsigned char) group & 0xff;
          dst += 2;
          group = 0;
          numcnt = 0;
        }
      } else if ('.' == c) {
        if (dst + 4 > ipv6addr_end){
          is_ipv6 = false;
        } else if (!str_to_ipv4(len - group_start, str + group_start, (in_addr *)dst)) {
          is_ipv6 = false;
        } else {
          numcnt = 0;
          dst += 4;
        }
        break;
      } else {
        numcnt++;
        if (4 < numcnt) {
          is_ipv6 = false;
        } else {
          if ('0' <= c && '9' >= c){
            num = c - '0';
          } else if ('a' <= c && 'f' >= c){
            num = c - 'a' + 10;
          } else if ('A' <= c && 'F' >= c){
            num = c - 'A' + 10;
          } else {
            is_ipv6 = false;
          }
          group = (group << 4) | num;
        }
      }
    }
    if( 0 != numcnt && is_ipv6) {
      if (dst + 2 > ipv6addr_end) {
          is_ipv6 = false;
        } else {
          dst[0] = (unsigned char) (group >> 8) & 0xff;
          dst[1] = (unsigned char) group & 0xff;
          dst += 2;
        }
    }
    if (nullptr != gap_ptr && is_ipv6) {
      if (dst == ipv6addr_end) {
        is_ipv6 = false;
      } else {
        int to_move = dst - gap_ptr;
        for (int i = 1; i <= to_move; ++i) {
          *(ipv6addr_end - i) = *(gap_ptr + to_move - i);
          *(gap_ptr + to_move - i) = 0;
        }
        dst = ipv6addr_end;
      }
    }
    if (dst != ipv6addr_end){
      is_ipv6 = false;
    }
  }
  return is_ipv6;
}

void ObExprInetCommon::ip_to_str(int type, const void *ip, void *result){
  if (type == AF_INET) {
    const unsigned char *ipv4_groups = (const unsigned char *)ip;
    sprintf((char*)result, "%d.%d.%d.%d", ipv4_groups[0], ipv4_groups[1], ipv4_groups[2], ipv4_groups[3]);
  } else if (type == AF_INET6) {
    int gap = -1, gap_len = -1;
    int rg = -1, rg_len = -1;
    const uint16_t *ipv6_binary =  (const uint16_t *)ip;
    uint16_t ipv6_groups[8];
    for (int i= 0; i < 8; ++i){
      ipv6_groups[i] = (ipv6_binary[i] << 8) | ((ipv6_binary[i] >> 8) & 0xff);
    }
    for (int i = 0; i < 8; ++i){
      if (0 == ipv6_groups[i]) {
        if (-1 == rg_len) {
          rg_len = 1;
          rg = i;
        } else {
          rg_len++;
        }
      } else {
        if (rg_len > gap_len){
          gap = rg;
          gap_len = rg_len;
        } else {
        }
        rg_len = -1;
      }
    }
    if (rg_len > gap_len){
      gap = rg;
      gap_len = rg_len;
    } else {
    }
    char *p = (char *)result;
    for (int i = 0; i < 8; ++i){
      if (gap == i){
        if (0 == i) {
          *p = ':';
          ++p;
        }else{
        }
        *p = ':';
        ++p;
        i += gap_len - 1;
      } else if (6 == i && 0 == gap &&
             (6 == gap_len ||                           // IPv4-compatible
              (5 == gap_len && 0xffff == ipv6_groups[5]) // IPv4-mapped
             ))
      {
        const unsigned char *ipv4_groups = (const unsigned char *)ip + 12;
        p += sprintf(p, "%d.%d.%d.%d", ipv4_groups[0], ipv4_groups[1], ipv4_groups[2], ipv4_groups[3]);
        break;
      } else {
        p += sprintf(p, "%x", ipv6_groups[i]);
        if (i != 7) {
          *p= ':';
          ++p;
        }
      }
    }
    *p = '\0';
  } else {
  }
}
ObExprInetAton::ObExprInetAton(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_INETATON, N_INETATON, 1, NOT_ROW_DIMENSION)
{}

ObExprInetAton::~ObExprInetAton()
{}

int ObExprInetAton::calc(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  if (text.is_null()) {
    result.set_null();
  } else {
    ObString m_text = text.get_string();
    if (OB_FAIL(ob_inet_aton(result, m_text))) {
      LOG_WARN("fail to convert ip to int", K(ret), K(m_text));
    } else {
    }
  }
  if (OB_FAIL(ret)) {
    uint64_t cast_mode = 0;
    ObSQLSessionInfo* session = expr_ctx.my_session_;
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else {
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
      if (CM_IS_WARN_ON_FAIL(cast_mode)) {
        ret = OB_SUCCESS;
        result.set_null();
      } else {
      }
    }
  } else {
  }
  UNUSED(expr_ctx);
  return ret;
}

int ObExprInetAton::calc_result1(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (OB_FAIL(calc(result, text, expr_ctx))) {
    LOG_WARN("fail to calc", K(ret), K(text));
  } else {
  }
  return ret;
}

template <typename T>
int ObExprInetAton::ob_inet_aton(T& result, const ObString& text)
{
  int ret = OB_SUCCESS;
  if (0 == text.length()) {
    LOG_WARN("ip format invalid", K(text));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int len = text.length();
    char c;
    int cnt = 0;
    unsigned long value = 0;
    int byte = 0;
    for (int i = 0; OB_SUCC(ret) && i < len; ++i) {
      c = text.ptr()[i];
      int num = c - '0';
      if (0 <= num && 9 >= num) {
        byte = byte * 10 + num;
        if (255 < byte) {
          ret = OB_INVALID_ARGUMENT;
        } else {
        }
      } else if ('.' == c) {
        cnt++;
        value = (value << 8) + byte;
        byte = 0;
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (3 < cnt || '.' == c) {
      LOG_WARN("ip format invalid", K(cnt));
      ret = OB_INVALID_ARGUMENT;
    } else {
      if (2 == cnt) {
        value <<= 8;
      } else if (1 == cnt) {
        value <<= 16;
      } else {
      }
      value = (value << 8) + byte;
      result.set_int(value);
    }
  }
  return ret;
}

int ObExprInetAton::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inet_aton expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of inet_aton expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprInetAton::calc_inet_aton;
  }
  return ret;
}

int ObExprInetAton::calc_inet_aton(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("inet_aton expr eval param value failed", K(ret));
  } else {
    ObDatum& text = expr.locate_param_datum(ctx, 0);
    if(text.is_null()){
      expr_datum.set_null();
    } else {
      ObString m_text = text.get_string();
      if (OB_FAIL(ob_inet_aton(expr_datum, m_text))) {
        LOG_WARN("fail to convert ip to int", K(ret), K(m_text));
      } else {
      } 
    }
    if (OB_FAIL(ret)) {
      uint64_t cast_mode = 0;
      ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is NULL", K(ret));
      } else {
        ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
        if (CM_IS_WARN_ON_FAIL(cast_mode)) {
          ret = OB_SUCCESS;
          expr_datum.set_null();
        } else {
        }
      }
    } else {
    }
  }
  return ret;
}

ObExprInet6Ntoa::ObExprInet6Ntoa(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_INET6NTOA, N_INET6NTOA, 1)
{}

ObExprInet6Ntoa::~ObExprInet6Ntoa()
{}

int ObExprInet6Ntoa::calc(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx, common::ObExprStringBuf& string_buf)
{
  int ret = OB_SUCCESS;
  if (text.is_null()) {
    result.set_null();
  } else if (!text.is_varbinary()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObString num_val = text.get_varbinary();
    if (OB_FAIL(inet6_ntoa(result, num_val, string_buf))) {
      LOG_WARN("fail to convert num 2 ip6", K(ret), K(num_val));
    }
  }
  if (OB_FAIL(ret)) {
    uint64_t cast_mode = 0;
    ObSQLSessionInfo* session = expr_ctx.my_session_;
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else {
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
      if (CM_IS_WARN_ON_FAIL(cast_mode)) {
        ret = OB_SUCCESS;
        result.set_null();
      } else {
      }
    }
  } else {
  }
  return ret;
}

int ObExprInet6Ntoa::calc_result1(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (OB_FAIL(calc(result, text, expr_ctx, *expr_ctx.calc_buf_))) {
    LOG_WARN("fail to calc", K(ret), K(text));
  } else if (OB_LIKELY(!result.is_null())) {
    result.set_collation_type(result_type_.get_collation_type());
    result.set_collation_level(result_type_.get_collation_level());
  } else {
  }
  return ret;
}

int ObExprInet6Ntoa::inet6_ntoa(ObObj& result, const ObString& num_val, ObExprStringBuf& string_buf)
{
  int ret = OB_SUCCESS;
  ObString str_result;
  char* buf = static_cast<char*>(string_buf.alloc(MAX_IP_ADDR_LENGTH));
  if (OB_ISNULL(buf)) {
    result.set_null();
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(ret));
  } else if (4 != num_val.length() && 16 != num_val.length()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (4 == num_val.length()) {
      ObExprInetCommon::ip_to_str(AF_INET, num_val.ptr(), buf);
    } else {
      ObExprInetCommon::ip_to_str(AF_INET6, num_val.ptr(), buf);
    }
    str_result.assign_ptr(buf, static_cast<ObString::obstr_size_t>(strlen(buf)));
    result.set_varchar(str_result);
  }
  return ret;
}

int ObExprInet6Ntoa::inet6_ntoa(ObDatum& result, const ObString& num_val)
{
  int ret = OB_SUCCESS;
  ObString str_result = result.get_string();
  char* buf = str_result.ptr();
  if (4 != num_val.length() && 16 != num_val.length()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (4 == num_val.length()) {
      ObExprInetCommon::ip_to_str(AF_INET, num_val.ptr(), buf);
    } else {
      ObExprInetCommon::ip_to_str(AF_INET6, num_val.ptr(), buf);
    }
    str_result.assign_ptr(buf, static_cast<ObString::obstr_size_t>(strlen(buf)));
    result.set_string(str_result);
  }
  return ret;
}

int ObExprInet6Ntoa::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (1 != rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inet6_ntoa expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of inet6_ntoa expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprInet6Ntoa::calc_inet6_ntoa;
  }
  return ret;
}

int ObExprInet6Ntoa::calc_inet6_ntoa(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("inet6_ntoa expr eval param value failed", K(ret));
  } else {
    ObDatum& text = expr.locate_param_datum(ctx, 0);
    if (text.is_null()) {
      expr_datum.set_null();
    } else if (!ob_is_varbinary_type(expr.args_[0]->datum_meta_.type_,expr.args_[0]->datum_meta_.cs_type_)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      CK(expr.res_buf_len_ >= MAX_IP_ADDR_LENGTH);
      ObString num_val = text.get_string();
      if (OB_FAIL(inet6_ntoa(expr_datum, num_val))) {
        LOG_WARN("fail to convert binary 2 ip6", K(ret), K(num_val));
      } else {
      }
    }
    if (OB_FAIL(ret)) {
      uint64_t cast_mode = 0;
      ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is NULL", K(ret));
      } else {
        ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
        if (CM_IS_WARN_ON_FAIL(cast_mode)) {
          ret = OB_SUCCESS;
          expr_datum.set_null();
        } else {
        }
      }
    } else {
    }
  }
  return ret;
}

ObExprInet6Aton::ObExprInet6Aton(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_INET6ATON, N_INET6ATON, 1, NOT_ROW_DIMENSION)
{}

ObExprInet6Aton::~ObExprInet6Aton()
{}

int ObExprInet6Aton::calc(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  if (text.is_null()) {
    result.set_null();
  } else if (!text.is_varchar()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObString m_text = text.get_string();
    if (OB_FAIL(inet6_aton(result, m_text, *expr_ctx.calc_buf_))) {
      LOG_WARN("fail to convert ip to num", K(ret), K(m_text));
    } else {
    }
  }
  if (OB_FAIL(ret)) {
    uint64_t cast_mode = 0;
    ObSQLSessionInfo* session = expr_ctx.my_session_;
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else {
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
      if (CM_IS_WARN_ON_FAIL(cast_mode)) {
        ret = OB_SUCCESS;
        result.set_null();
      } else {
      }
    }
  } else {
  }
  UNUSED(expr_ctx);
  return ret;
}

int ObExprInet6Aton::calc_result1(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (OB_FAIL(calc(result, text, expr_ctx))) {
    LOG_WARN("fail to calc", K(ret), K(text));
  } else {
  }
  return ret;
}

int ObExprInet6Aton::inet6_aton(common::ObDatum& result, const ObString& text){
  int ret = OB_SUCCESS;
  char buf[MAX_IP_ADDR_LENGTH];
  if (45 < text.length()) {
    LOG_WARN("ip format invalid", K(text));
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(buf, text.ptr(), text.length());
    int len = text.length();
    buf[len] = '\0';
    ObString str_result = result.get_string();
    char *result_buf = str_result.ptr();
    bool success = ObExprInetCommon::str_to_ipv6(len, buf, (in6_addr *)result_buf);
    if (success) {
      str_result.assign_ptr(result_buf, 16);
      result.set_string(str_result);
    } else {
      success = ObExprInetCommon::str_to_ipv4(len, buf, (in_addr *)result_buf);
      if (success) {
        str_result.assign_ptr(result_buf, 4);
        result.set_string(str_result);
      } else {
        LOG_WARN("ip format invalid", K(text));
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  return ret;  
}

int ObExprInet6Aton::inet6_aton(ObObj& result, const ObString& text, common::ObExprStringBuf& string_buf)
{
  int ret = OB_SUCCESS;
  char buf[MAX_IP_ADDR_LENGTH];
  if (45 < text.length()) {
    LOG_WARN("ip format invalid", K(text));
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(buf, text.ptr(), text.length());
    int len = text.length();
    buf[len] = '\0';
    ObString str_result;
    char* result_buf = static_cast<char*>(string_buf.alloc(16));
    bool success = ObExprInetCommon::str_to_ipv6(len, buf, (in6_addr*)result_buf);
    if (success){
      str_result.assign_ptr(result_buf, 16);
      result.set_varbinary(str_result);
    } else {
      success = ObExprInetCommon::str_to_ipv4(len, buf, (in_addr*)result_buf);
      if (success) {
        str_result.assign_ptr(result_buf, 4);
        result.set_varbinary(str_result);
      } else {
        LOG_WARN("ip format invalid", K(text));
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  return ret;
}

int ObExprInet6Aton::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (1 != rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inet6_aton expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of inet6_aton expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprInet6Aton::calc_inet6_aton;
  }
  return ret;
}

int ObExprInet6Aton::calc_inet6_aton(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
  } else {
    ObDatum& text = expr.locate_param_datum(ctx, 0);
    ObString m_text = text.get_string();
    if (text.is_null()) {
      expr_datum.set_null();
    } else if (OB_FAIL(inet6_aton(expr_datum, m_text))) {
      LOG_WARN("fail to convert ipv6 to num", K(ret), K(m_text));
    } else {
    }
    if (OB_FAIL(ret)) {
      uint64_t cast_mode = 0;
      ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is NULL", K(ret));
      } else {
        ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
        if (CM_IS_WARN_ON_FAIL(cast_mode)) {
          ret = OB_SUCCESS;
          expr_datum.set_null();
        } else {
        }
      }
    } else {
    }
  }
  return ret;
}

ObExprIsIpv4::ObExprIsIpv4(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_IS_IPV4, N_IS_IPV4, 1, NOT_ROW_DIMENSION)
{}

ObExprIsIpv4::~ObExprIsIpv4()
{}

int ObExprIsIpv4::calc(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  if (text.is_null()) {
    result.set_int(0);
  } else {
    ObString m_text = text.get_string();
    ret = is_ipv4(result, m_text);
  }
  UNUSED(expr_ctx);
  return ret;
}

int ObExprIsIpv4::calc_result1(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (OB_FAIL(calc(result, text, expr_ctx))) {
    LOG_WARN("fail to calc", K(ret), K(text));
  } else {
  }
  return ret;
}

template <typename T>
int ObExprIsIpv4::is_ipv4(T& result, const ObString& text)
{
  char buf[16];
  int ipv4_ret = 1;
  if (15 < text.length()) {
    LOG_WARN("ip format invalid", K(text));
    ipv4_ret = 0;
  } else {
    MEMCPY(buf, text.ptr(), text.length());
    int len = text.length();
    buf[len] = '\0';
    struct in_addr addr;
    if (!ObExprInetCommon::str_to_ipv4(len, buf, &addr)) {
      ipv4_ret = 0;
    } else {
    }
  }
  result.set_int(ipv4_ret);
  return OB_SUCCESS;
}

int ObExprIsIpv4::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (1 != rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is_ipv4 expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of is_ipv4 expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprIsIpv4::calc_is_ipv4;
  }
  return ret;
}

int ObExprIsIpv4::calc_is_ipv4(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("is_ipv4 expr eval param value failed", K(ret));
  } else {
    ObDatum& text = expr.locate_param_datum(ctx, 0);
    ObString m_text = text.get_string();
    if (text.is_null()) {
      expr_datum.set_int(0);
    } else {
      ret = is_ipv4(expr_datum, m_text);
    }
  }
  return ret;
}

ObExprIsIpv4Mapped::ObExprIsIpv4Mapped(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_IS_IPV4_MAPPED, N_IS_IPV4_MAPPED, 1, NOT_ROW_DIMENSION)
{}

ObExprIsIpv4Mapped::~ObExprIsIpv4Mapped()
{}

int ObExprIsIpv4Mapped::calc(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  if (text.is_null()) {
    result.set_int(0);
  } else if (!text.is_varbinary()) {
    result.set_int(0);
  } else {
    ObString m_text = text.get_varbinary();
    ret = is_ipv4_mapped(result, m_text);
  }
  UNUSED(expr_ctx);
  return ret;
}

int ObExprIsIpv4Mapped::calc_result1(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (OB_FAIL(calc(result, text, expr_ctx))) {
    LOG_WARN("fail to calc", K(ret), K(text));
  } else {
  }
  return ret;
}

int ObExprIsIpv4Mapped::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (1 != rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is_ipv4_mapped expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of is_ipv4_mapped expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprIsIpv4Mapped::calc_is_ipv4_mapped;
  }
  return ret;
}

int ObExprIsIpv4Mapped::calc_is_ipv4_mapped(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("is_ipv4_mapped expr eval param value failed", K(ret));
  } else {
    ObDatum& text = expr.locate_param_datum(ctx, 0);
    if (text.is_null()) {
      expr_datum.set_int(0);
    } else if (!ob_is_varbinary_type(expr.args_[0]->datum_meta_.type_,expr.args_[0]->datum_meta_.cs_type_)) {
      expr_datum.set_int(0);
    } else {
      ObString m_text = text.get_string();
      ret = is_ipv4_mapped(expr_datum, m_text);
    }
  }
  return ret;
}

template <typename T>
int ObExprIsIpv4Mapped::is_ipv4_mapped(T& result, const ObString& num_val)
{
  char buf[16];
  int ipv4_ret = 1;
  if (16 != num_val.length()) {
    LOG_WARN("ip format invalid", K(num_val));
    ipv4_ret = 0;
  } else {
    MEMCPY(buf, num_val.ptr(), num_val.length());
    ipv4_ret = IN6_IS_ADDR_V4MAPPED((struct in6_addr *)buf);
  }
  result.set_int(ipv4_ret);
  return OB_SUCCESS;
}

ObExprIsIpv4Compat::ObExprIsIpv4Compat(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_IS_IPV4_COMPAT, N_IS_IPV4_COMPAT, 1, NOT_ROW_DIMENSION)
{}

ObExprIsIpv4Compat::~ObExprIsIpv4Compat()
{}

int ObExprIsIpv4Compat::calc(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  if (text.is_null()) {
    result.set_int(0);
  } else if (!text.is_varbinary()) {
    result.set_int(0);
  } else {
    ObString m_text = text.get_varbinary();
    ret = is_ipv4_compat(result, m_text);
  }
  UNUSED(expr_ctx);
  return ret;
}

int ObExprIsIpv4Compat::calc_result1(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (OB_FAIL(calc(result, text, expr_ctx))) {
    LOG_WARN("fail to calc", K(ret), K(text));
  } else {
  }
  return ret;
}

int ObExprIsIpv4Compat::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (1 != rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is_ipv4_compat expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of is_ipv4_compat expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprIsIpv4Compat::calc_is_ipv4_compat;
  }
  return ret;
}

int ObExprIsIpv4Compat::calc_is_ipv4_compat(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("is_ipv4_compat expr eval param value failed", K(ret));
  } else {
    ObDatum& text = expr.locate_param_datum(ctx, 0);
    if (text.is_null()) {
      expr_datum.set_int(0);
    } else if (!ob_is_varbinary_type(expr.args_[0]->datum_meta_.type_,expr.args_[0]->datum_meta_.cs_type_)) {
      expr_datum.set_int(0);
    } else {
      ObString m_text = text.get_string();
      ret = is_ipv4_compat(expr_datum, m_text);
    }
  }
  return ret;
}

template <typename T>
int ObExprIsIpv4Compat::is_ipv4_compat(T& result, const ObString& num_val)
{
  char buf[16];
  int ipv4_ret = 1;
  if (16 != num_val.length()) {
    LOG_WARN("ip format invalid", K(num_val));
    ipv4_ret = 0;
  } else {
    MEMCPY(buf, num_val.ptr(), num_val.length());
    ipv4_ret = IN6_IS_ADDR_V4COMPAT((struct in6_addr *)buf);
  }
  result.set_int(ipv4_ret);
  return OB_SUCCESS;
}

ObExprIsIpv6::ObExprIsIpv6(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_IS_IPV6, N_IS_IPV6, 1, NOT_ROW_DIMENSION)
{}

ObExprIsIpv6::~ObExprIsIpv6()
{}

int ObExprIsIpv6::calc(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  if (text.is_null()) {
    result.set_int(0);
  } else if (!text.is_varchar()) {
    result.set_int(0);
  } else {
    ObString m_text = text.get_string();
    ret = is_ipv6(result, m_text);
  }
  UNUSED(expr_ctx);
  return ret;
}

int ObExprIsIpv6::calc_result1(ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (OB_FAIL(calc(result, text, expr_ctx))) {
    LOG_WARN("fail to calc", K(ret), K(text));
  } else {
  }
  return ret;
}

int ObExprIsIpv6::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (1 != rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is_ipv6 expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of is_ipv6 expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprIsIpv6::calc_is_ipv6;
  }
  return ret;
}

int ObExprIsIpv6::calc_is_ipv6(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("is_ipv6 expr eval param value failed", K(ret));
  } else if (ObVarcharType != expr.args_[0]->datum_meta_.type_) {
    expr_datum.set_int(0);
  } else {
    ObDatum& text = expr.locate_param_datum(ctx, 0);
    ObString m_text = text.get_string();
    if (text.is_null()) {
      expr_datum.set_int(0);
    } else {
      ret = is_ipv6(expr_datum, m_text);
    }
  }
  return ret;
}

template <typename T>
int ObExprIsIpv6::is_ipv6(T& result, const ObString& text)
{
  char buf[MAX_IP_ADDR_LENGTH];
  int ipv6_ret = 1;
  if (MAX_IP_ADDR_LENGTH - 1 < text.length()) {
    LOG_WARN("ip format invalid", K(text));
    ipv6_ret = 0;
  } else {
    MEMCPY(buf, text.ptr(), text.length());
    buf[text.length()] = '\0';
    in6_addr addr;
    if (!ObExprInetCommon::str_to_ipv6(text.length(), buf, &addr)) {
      ipv6_ret = 0;
    } else {
    }
  }
  result.set_int(ipv6_ret);
  return OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase
