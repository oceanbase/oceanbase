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
#include "sql/engine/expr/ob_expr_inet.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
int ObExprInetCommon::str_to_ipv4(int len, const char *str, bool& is_ip_format_invalid, in_addr* ipv4addr)
{
  is_ip_format_invalid = false;
  int ret = OB_SUCCESS;
  //Shortest IPv4 address："x.x.x.x"，length:7
  if (7 > len || INET_ADDRSTRLEN - 1 < len) {
    is_ip_format_invalid = true;
    LOG_WARN("ip format invalid, too short or too long", K(len));
  } else if (OB_UNLIKELY(OB_UNLIKELY(OB_ISNULL(str) || OB_ISNULL(ipv4addr)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ip_str or ipv4addr is null", K(ret), K(str), K(ipv4addr));
  } else {
    unsigned char byte_addr[4] = {0};
    int dotcnt = 0, numcnt = 0;
    int byte = 0;
    char c;
    for (int i = 0; !is_ip_format_invalid && i < len && *(i + str); ++i) {
      c = *(i + str);
      if ('.' == c) {
        if (255 < byte) {
          is_ip_format_invalid = true;
          LOG_WARN("ip format invalid", K(byte));
        } else if (0 == numcnt || 3 < numcnt) {
          is_ip_format_invalid = true;
          LOG_WARN("ip format invalid, group num count invalid", K(numcnt), K(dotcnt));
        } else if (i == len - 1 || !*(i + str + 1)) {
          is_ip_format_invalid = true;
          LOG_WARN("ip format invalid, end with '.'", K(i));
        } else {
          byte_addr[dotcnt] = (unsigned char) byte;
        }
        dotcnt ++;
        if (3 < dotcnt) {
          is_ip_format_invalid = true;
          LOG_WARN("ip format invalid, too much '.'", K(dotcnt), K(i));
        } else {
        }
        byte = 0;
        numcnt = 0;
      } else if ('9' >= c && '0' <= c) {
        byte = byte * 10 + c - '0';
        numcnt ++;
      } else {
        is_ip_format_invalid = true;
        LOG_WARN("ip format invalid, invalid number", K(c), K(i));
      }
    }
    if (255 < byte) {
      is_ip_format_invalid = true;
      LOG_WARN("ip format invalid", K(byte));
    } else if (3 != dotcnt) {
      is_ip_format_invalid = true;
      LOG_WARN("ip format invalid, number of '.' is not 3", K(dotcnt));
    } else if ('.' == c) { // IP number can't end on '.'
      is_ip_format_invalid = true;
      LOG_WARN("ip format invalid, end with '.'");
    } else {
      byte_addr[3] = (unsigned char) byte;
    }
    MEMCPY((unsigned char *) ipv4addr, byte_addr, sizeof(in_addr));
  }
  return ret;
}

int ObExprInetCommon::str_to_ipv6(int len, const char *str, bool& is_ip_format_invalid, in6_addr* ipv6addr)
{
  int ret = OB_SUCCESS;
  is_ip_format_invalid = false;
  //Ipv6 length of mysql support: 2~39
  //Shortest IPv6 address："::"，length:2
  if (2 > len || INET6_ADDRSTRLEN - 1 < len) {
    is_ip_format_invalid = true;
    LOG_WARN("ip format invalid, too short or too long", K(len));
  } else if (OB_UNLIKELY(OB_ISNULL(str) || OB_ISNULL(ipv6addr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ip_str or ipv6addr is null", K(ret), K(str), K(ipv6addr));
  } else {
    int dst_index = 0;
    char c;
    int group = 0, num = 0;
    int numcnt = 0;
    int group_start = 0;
    int gap_index = -1;
    int i = 0;
    uint8_t ip_addr[sizeof(in6_addr)];
    MEMSET(ip_addr, 0, sizeof(in6_addr));
    if (':' == str[0]) {
      if (':' == str[1]) {
        i = 1;
      } else { //cannot start with ":x"
        is_ip_format_invalid = true;
        LOG_WARN("ip format invalid, start with :x", K(str[1]));
      }
    } else {
    }
    for (; !is_ip_format_invalid && i < len && str[i]; ++i) {
      c = str[i];
      if (':' == c) {
        group_start = i + 1;
        if (0 == numcnt) {
          if (-1 == gap_index) {
            gap_index = dst_index;
          } else { //number of "::" greater than 1
            is_ip_format_invalid = true;
            LOG_WARN("ip format invalid, too much '::'", K(gap_index), K(i));
          }
        } else if (i >= len || !str[i+1]) { // cannot end with "x:"
          is_ip_format_invalid = true;
          LOG_WARN("ip format invalid, end with x:", K(i));
        } else if (dst_index + 2 > sizeof(in6_addr)) {
          is_ip_format_invalid = true;
          LOG_WARN("ip format invalid", K(i), K(dst_index));
        } else {
          ip_addr[dst_index] = (unsigned char) (group >> 8) & 0xff;
          ip_addr[dst_index + 1] = (unsigned char) group & 0xff;
          dst_index += 2;
          group = 0;
          numcnt = 0;
        }
      } else if ('.' == c) {
        if (dst_index + sizeof(in_addr) > sizeof(in6_addr)) {
          is_ip_format_invalid = true;
          LOG_WARN("ip format invalid, no room for ipv4", K(dst_index), K(i));
        } else if (OB_FAIL(str_to_ipv4(len - group_start, str + group_start, is_ip_format_invalid, (in_addr *)(ip_addr + dst_index)))) {
          LOG_WARN("fail to excute str_to_ipv4", K(ret));
        } else if (is_ip_format_invalid) {
          LOG_WARN("ipv4 format invalid", K(group_start));
        } else {
          numcnt = 0;
          dst_index += 4;
        }
        break;
      } else {
        numcnt++;
        if (4 < numcnt) {
          is_ip_format_invalid = true;
          LOG_WARN("ip format invalid, too many numbers in one group", K(numcnt), K(i));
        } else {
          if ('0' <= c && '9' >= c) {
            num = c - '0';
          } else if ('a' <= c && 'f' >= c) {
            num = c - 'a' + 10;
          } else if ('A' <= c && 'F' >= c) {
            num = c - 'A' + 10;
          } else {
            is_ip_format_invalid = true;
            LOG_WARN("ip format invalid, invaild number", K(c), K(i));
          }
          group = (group << 4) | num;
        }
      }
    }
    if (!is_ip_format_invalid) {
      if (0 != numcnt) { //add last group: ":xxxx"
        if (dst_index + 2 > sizeof(in6_addr)) {
          is_ip_format_invalid = true;
          LOG_WARN("ip format invalid, too many groups", K(dst_index), K(i));
        } else {
          ip_addr[dst_index] = (unsigned char) (group >> 8) & 0xff;
          ip_addr[dst_index+1] = (unsigned char) group & 0xff;
          dst_index += 2;
        }
      } else {
      }
      if (-1 != gap_index) {
        if (dst_index == sizeof(in6_addr)) {
          is_ip_format_invalid = true;
          LOG_WARN("ip format invalid, no room for ::", K(gap_index));
        } else {
          int to_move = dst_index - gap_index;
          for (int i = 1; i <= to_move; ++i) {
            ip_addr[sizeof(in6_addr) - i] = ip_addr[gap_index + to_move - i];
            ip_addr[gap_index + to_move - i] = 0;
          }
        }
      } else if (dst_index != sizeof(in6_addr)) {
        is_ip_format_invalid = true;
        LOG_WARN("ip format invalid", K(dst_index),K(sizeof(in6_addr)));
      } else {
      }
      MEMCPY(ipv6addr, ip_addr, sizeof(in6_addr));
    }
  }
  return ret;
}

int ObExprInetCommon::ip_to_str(ObString& ip_binary, bool& is_ip_format_invalid, ObString& ip_str)
{
  is_ip_format_invalid = false;
  int ret = OB_SUCCESS;
  const char *ip = ip_binary.ptr();
  char * result_ptr = ip_str.ptr();
  char result[common::MAX_IP_ADDR_LENGTH] = {0};
  if (OB_UNLIKELY(OB_ISNULL(ip) || OB_ISNULL(result_ptr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ip or result is null", K(ret), K(ip), K(result));    
  }else if (sizeof(in_addr) == ip_binary.length()) {
    const unsigned char *ipv4_groups = (const unsigned char *) ip;
    sprintf((char *)result_ptr, "%d.%d.%d.%d", ipv4_groups[0], ipv4_groups[1], ipv4_groups[2], ipv4_groups[3]);
    ip_str.assign(result_ptr, static_cast<ObString::obstr_size_t>(strlen(result_ptr)));
  } else if (sizeof(in6_addr) == ip_binary.length()) {
    int gap = -1, gap_len = -1;
    int rg = -1, rg_len = -1;
    const uint16_t *ipv6_binary = (const uint16_t *) ip;
    uint16_t ipv6_groups[8];
    for (int i= 0; i < 8; ++i) {
      ipv6_groups[i] = (ipv6_binary[i] << 8) | ((ipv6_binary[i] >> 8) & 0xff);
    }
    //find longest gap
    for (int i = 0; i < 8; ++i) {
      if (0 == ipv6_groups[i]) {
        if (-1 == rg_len) {
          rg_len = 1;
          rg = i;
        } else {
          rg_len++;
        }
      } else {
        if (rg_len > gap_len) {
          gap = rg;
          gap_len = rg_len;
        } else {
        }
        rg_len = -1;
      }
    }
    if (rg_len > gap_len) {
      gap = rg;
      gap_len = rg_len;
    } else {
    }
    char *p = &result[0];
    for (int i = 0; i < 8; ++i) {
      if (gap == i) {
        if (0 == i) {
          *p = ':';
          ++p;
        } else {
        }
        *p = ':';
        ++p;
        i += gap_len - 1;  //skip gap
      } else if (6 == i && 0 == gap &&
             (6 == gap_len ||                           // IPv4-compatible
              (5 == gap_len && 0xffff == ipv6_groups[5]) // IPv4-mapped
             ))
      {
        const unsigned char *ipv4_groups = (const unsigned char *) ip + 12;
        p += sprintf(p, "%d.%d.%d.%d", ipv4_groups[0], ipv4_groups[1], ipv4_groups[2], ipv4_groups[3]);
        break;
      } else {
        p += sprintf(p, "%x", ipv6_groups[i]);
        if (i != 7) {
          *p = ':';
          ++p;
        }
      }
    }
    *p = '\0';
    int len = static_cast<ObString::obstr_size_t>(strlen(result));
    MEMCPY(result_ptr, result, len);
    ip_str.assign(result_ptr, len);
  } else {
    is_ip_format_invalid = true;
    LOG_WARN("invalid ip length", K(ip_binary.length()));
  }
  return ret;
}

ObExprInetAton::ObExprInetAton(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_INETATON, N_INETATON, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprInetAton::~ObExprInetAton()
{}

template <typename T>
int ObExprInetAton::ob_inet_aton(T& result, const ObString& text, bool& is_ip_format_invalid)
{
  int ret = OB_SUCCESS;
  is_ip_format_invalid = false;
  if (text.empty()) {
    is_ip_format_invalid = true;
    LOG_WARN("ip length is zero");
  } else {
    int len = text.length();
    char c;
    int cnt = 0;
    unsigned long value = 0;
    int byte = 0;
    for (int i = 0; !is_ip_format_invalid && i < len; ++i) {
      c = text.ptr()[i];
      if ('0' <= c && '9' >= c) {
        byte = byte * 10 + c - '0';
        if (255 < byte) {
          is_ip_format_invalid = true;
          LOG_WARN("ip format invalid", K(byte), K(text));
        } else {
        }
      } else if ('.' == c) {
        cnt++;
        value = (value << 8) + byte;
        byte = 0;
      } else {
        is_ip_format_invalid = true;
        LOG_WARN("ip format invalid", K(c), K(text));
      }
    }
    if (is_ip_format_invalid) {
    } else if (3 < cnt || '.' == c) { // IP number can't end on '.', '.' count <= 3
      is_ip_format_invalid = true;
      LOG_WARN("ip format invalid", K(text), K(cnt), K(c));
    } else {
      /*
      Attempt to support short forms of IP-addresses.
      Examples:
        127     -> 0.0.0.127
        127.255 -> 127.0.0.255
        127.256 -> NULL
        127.2.1 -> 127.2.0.1
      */
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
  } else if (OB_UNLIKELY(OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]))) {
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
    if (text.is_null()) {
      expr_datum.set_null();
    } else {
      ObString m_text = text.get_string();
      bool is_ip_format_invalid = false;
      if (OB_FAIL(ob_inet_aton(expr_datum, m_text, is_ip_format_invalid))) {
        LOG_WARN("fail to excute ob_inet_aton", K(ret));
      }else if (is_ip_format_invalid) {
        uint64_t cast_mode = 0;
        ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
        if (OB_UNLIKELY(OB_ISNULL(session))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("session is NULL", K(ret));
        } else {
          ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
          if (CM_IS_WARN_ON_FAIL(cast_mode)) { //support no strict sql_mode
            expr_datum.set_null();
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("fail to convert ip to int", K(ret), K(m_text));
          }
        }
      }
    }
  }
  return ret;
}

ObExprInet6Ntoa::ObExprInet6Ntoa(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_INET6NTOA, N_INET6NTOA, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprInet6Ntoa::~ObExprInet6Ntoa()
{}

inline int ObExprInet6Ntoa::calc_result_type1(
    ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(text);
  type.set_varchar();
  type.set_length(common::MAX_IP_ADDR_LENGTH);
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  const sql::ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_UNLIKELY(OB_ISNULL(session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null",K(ret));
  } else {
    type.set_collation_type(get_default_collation_type(type.get_type(), *type_ctx.get_session()));
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
  } else if (OB_UNLIKELY(OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]))) {
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
    } else {
      char * buf = NULL;
      CK(expr.res_buf_len_ >= MAX_IP_ADDR_LENGTH);
      if (OB_FAIL(ret)) {
        LOG_WARN("result buf size greater than MAX_IP_ADDR_LENGTH", K(ret));
      } else if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, MAX_IP_ADDR_LENGTH))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(MAX_IP_ADDR_LENGTH));
      } else {
        bool is_ip_format_invalid = false;
        ObString num_val = text.get_string();
        ObString ip_str(MAX_IP_ADDR_LENGTH, 0, buf);
        if (!ob_is_varbinary_type(expr.args_[0]->datum_meta_.type_,expr.args_[0]->datum_meta_.cs_type_)) {
          is_ip_format_invalid = true;
          LOG_WARN("ip format invalid", K(ret), K(text));
        } else if (OB_FAIL(ObExprInetCommon::ip_to_str(num_val, is_ip_format_invalid, ip_str))) {
          LOG_WARN("fail to excute ip_to_str", K(ret));
        } else if (!is_ip_format_invalid) {
          expr_datum.set_string(ip_str);
        }
        if (OB_SUCC(ret) && is_ip_format_invalid) {
          uint64_t cast_mode = 0;
          ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
          if (OB_UNLIKELY(OB_ISNULL(session))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("session is NULL", K(ret));
          } else {
            ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
            if (CM_IS_WARN_ON_FAIL(cast_mode)) {
              expr_datum.set_null(); //support no strict sql_mode
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("ip format invalid", K(ret),K(text));
            }
          }
        }
      }
    }
  }
  return ret;
}

ObExprInet6Aton::ObExprInet6Aton(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_INET6ATON, N_INET6ATON, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprInet6Aton::~ObExprInet6Aton()
{}

int ObExprInet6Aton::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (1 != rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inet6_aton expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_UNLIKELY(OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]))) {
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
    LOG_WARN("inet6_ntoa expr eval param value failed", K(ret));
  } else {
    char * buf = NULL;
    CK(expr.res_buf_len_ >= sizeof(in6_addr));
    if (OB_FAIL(ret)) {
      LOG_WARN("result buf size greater than sizeof(in6_addr)", K(ret));
    } else if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, sizeof(in6_addr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(sizeof(in6_addr)));
    } else {
      ObDatum& text = expr.locate_param_datum(ctx, 0);
      ObString m_text = text.get_string();
      bool is_ip_format_invalid = false;
      if (text.is_null()) {
        expr_datum.set_null();
      } else {
        ObString str_result(sizeof(in6_addr), 0, buf);
        if (OB_FAIL(inet6_aton(m_text, is_ip_format_invalid, str_result))) {
          LOG_WARN("fail to excute inet6_aton", K(ret));
        } else if (is_ip_format_invalid) {
          uint64_t cast_mode = 0;
          ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
          if (OB_ISNULL(session)) {
            LOG_WARN("session is NULL", K(ret));
          } else {
            ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
            if (CM_IS_WARN_ON_FAIL(cast_mode)) {
              expr_datum.set_null(); //support no strict mode
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("ip format invalid", K(ret));
            }
          }
        } else {
          expr_datum.set_string(str_result);
        }
      }
    }
  }
  return ret;
}

int ObExprInet6Aton::inet6_aton(const ObString& ip, bool& is_ip_format_invalid, ObString& str_result)
{
  is_ip_format_invalid = false;
  int ret = OB_SUCCESS;
  char buf[MAX_IP_ADDR_LENGTH];
  if (INET6_ADDRSTRLEN - 1 < ip.length()) {
    is_ip_format_invalid = true;
    LOG_WARN("ip format invalid", K(ip));
  } else {
    char *result_buf = str_result.ptr();
    if (OB_UNLIKELY(OB_ISNULL(result_buf))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result_buf is null", K(ret));
    } else {
      MEMCPY(buf, ip.ptr(), ip.length());
      buf[ip.length()] = '\0';
      if (OB_FAIL(ObExprInetCommon::str_to_ipv4(ip.length(), buf, is_ip_format_invalid, (in_addr *)result_buf))) {
        LOG_WARN("fail to excute str_to_ipv4", K(ret));
      }else if (is_ip_format_invalid) {
        if (OB_FAIL(ObExprInetCommon::str_to_ipv6(ip.length(), buf, is_ip_format_invalid, (in6_addr *)result_buf))) {
          LOG_WARN("fail to excute str_to_ipv6", K(ret));
        } else if (is_ip_format_invalid) {
          LOG_WARN("ip format invalid", K(ip));
        } else {
          str_result.assign(result_buf, static_cast<int32_t>(sizeof(in6_addr)));
        }
      } else {
        str_result.assign(result_buf, static_cast<int32_t>(sizeof(in_addr)));
      }
    }
  }
  return ret;
}

ObExprIsIpv4::ObExprIsIpv4(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_IS_IPV4, N_IS_IPV4, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprIsIpv4::~ObExprIsIpv4()
{}

template <typename T>
int ObExprIsIpv4::is_ipv4(T& result, const ObString& text)
{
  char buf[16];
  int ipv4_ret = 1;
  int ret = OB_SUCCESS;
  if (INET_ADDRSTRLEN - 1 < text.length()) {
    ipv4_ret = 0;
  } else {
    MEMCPY(buf, text.ptr(), text.length());
    int len = text.length();
    buf[len] = '\0';
    struct in_addr addr;
    bool is_ip_invalid;
    if (OB_FAIL(ObExprInetCommon::str_to_ipv4(len, buf, is_ip_invalid, &addr))) {
      LOG_WARN("fail to excute str_to_ipv4");
    } else {
      ipv4_ret = is_ip_invalid ? 0 : 1;
    }
  }
  result.set_int(ipv4_ret);
  return ret;
}

int ObExprIsIpv4::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (1 != rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is_ipv4 expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_UNLIKELY(OB_UNLIKELY(OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])))) {
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
    } else if (OB_FAIL(is_ipv4(expr_datum, m_text))) {
      LOG_WARN("fail to excute is_ipv4", K(ret));
    } else {
    }
  }
  return ret;
}

ObExprIsIpv4Mapped::ObExprIsIpv4Mapped(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_IS_IPV4_MAPPED, N_IS_IPV4_MAPPED, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprIsIpv4Mapped::~ObExprIsIpv4Mapped()
{}

int ObExprIsIpv4Mapped::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (1 != rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is_ipv4_mapped expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_UNLIKELY(OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]))) {
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
      is_ipv4_mapped(expr_datum, m_text);
    }
  }
  return ret;
}

template <typename T>
void ObExprIsIpv4Mapped::is_ipv4_mapped(T& result, const ObString& num_val)
{
  char buf[16];
  int ipv4_ret = 1;
  if (sizeof(in6_addr) != num_val.length()) { //length of binary ipv6 addr is 16
    ipv4_ret = 0;
  } else {
    MEMCPY(buf, num_val.ptr(), num_val.length());
    ipv4_ret = IN6_IS_ADDR_V4MAPPED((struct in6_addr *) buf);
  }
  result.set_int(ipv4_ret);
}

ObExprIsIpv4Compat::ObExprIsIpv4Compat(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_IS_IPV4_COMPAT, N_IS_IPV4_COMPAT, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprIsIpv4Compat::~ObExprIsIpv4Compat()
{}

int ObExprIsIpv4Compat::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (1 != rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is_ipv4_compat expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_UNLIKELY(OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]))) {
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
    } else if (!ob_is_varbinary_type(expr.args_[0]->datum_meta_.type_, expr.args_[0]->datum_meta_.cs_type_)) {
      expr_datum.set_int(0);
    } else {
      ObString m_text = text.get_string();
      is_ipv4_compat(expr_datum, m_text);
    }
  }
  return ret;
}

template <typename T>
void ObExprIsIpv4Compat::is_ipv4_compat(T& result, const ObString& num_val)
{
  char buf[16];
  int ipv4_ret = 1;
  if (sizeof(in6_addr) != num_val.length()) { //length of binary ipv6 addr is 16
    ipv4_ret = 0;
  } else {
    MEMCPY(buf, num_val.ptr(), num_val.length());
    ipv4_ret = IN6_IS_ADDR_V4COMPAT((struct in6_addr *) buf);
  }
  result.set_int(ipv4_ret);
}

ObExprIsIpv6::ObExprIsIpv6(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_IS_IPV6, N_IS_IPV6, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprIsIpv6::~ObExprIsIpv6()
{}

int ObExprIsIpv6::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (1 != rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is_ipv6 expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_UNLIKELY(OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]))) {
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
  } else {
    ObDatum& text = expr.locate_param_datum(ctx, 0);
    ObString m_text = text.get_string();
    if (text.is_null()) {
      expr_datum.set_int(0);
    } else if (OB_FAIL(is_ipv6(expr_datum, m_text))) {
      LOG_WARN("fail to excute is_ipv6", K(ret));
    }
  }
  return ret;
}

template <typename T>
int ObExprIsIpv6::is_ipv6(T& result, const ObString& text)
{
  char buf[MAX_IP_ADDR_LENGTH];
  int ipv6_ret = 1;
  int ret = OB_SUCCESS;
  if (MAX_IP_ADDR_LENGTH - 1 < text.length()) {
    ipv6_ret = 0;
  } else {
    MEMCPY(buf, text.ptr(), text.length());
    buf[text.length()] = '\0';
    in6_addr addr;
    bool is_ip_invaild;
    if (OB_FAIL(ObExprInetCommon::str_to_ipv6(text.length(), buf, is_ip_invaild, &addr))) {
      LOG_WARN("fail to excute str_to_ipv6", K(ret));
    } else {
      ipv6_ret = is_ip_invaild ? 0 : 1;
    }
  }
  result.set_int(ipv6_ret);
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
