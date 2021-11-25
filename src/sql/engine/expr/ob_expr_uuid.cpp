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
#include <stdlib.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>
#include "sql/engine/expr/ob_expr_uuid.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase {
namespace sql {

int ObBigEndian::put_uint16(unsigned char* b, uint16_t v)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(b)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. pointer is null", K(ret), K(b));
  } else {
    b[0] = static_cast<unsigned char>(v >> 8);
    b[1] = static_cast<unsigned char>(v);
  }
  return ret;
}
int ObBigEndian::put_uint32(unsigned char* b, uint32_t v)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(b)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. pointer is null", K(ret), K(b));
  } else {
    b[0] = static_cast<unsigned char>(v >> 24);
    b[1] = static_cast<unsigned char>(v >> 16);
    b[2] = static_cast<unsigned char>(v >> 8);
    b[3] = static_cast<unsigned char>(v);
  }
  return ret;
}

ObUUIDNode::ObUUIDNode()
{
  is_inited_ = false;
  (void)init();
}

/*
 * get and set mac address
 *
 */
int ObUUIDNode::init()
{
  int ret = OB_SUCCESS;
  struct ifreq ifr;
  struct ifconf ifc;
  char buf[1024];
  is_inited_ = false;
  int sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
  if (sock == -1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("socket failed", K(ret), K(errno));
  } else {
    ifc.ifc_len = sizeof(buf);
    ifc.ifc_buf = buf;
    if (ioctl(sock, SIOCGIFCONF, &ifc) < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error. ioctl failed", K(ret), K(errno));
    } else {
      ifreq* it = ifc.ifc_req;
      ifreq* end = it + (ifc.ifc_len / sizeof(ifreq));
      bool mac_addr_found = false;
      for (; it != end && OB_SUCC(ret); ++it) {
        if (OB_ISNULL(it) || OB_ISNULL(it->ifr_name) || OB_ISNULL(ifr.ifr_name)) {
          continue;
        } else {
          strncpy(ifr.ifr_name, it->ifr_name, IFNAMSIZ - 1);
          if (ioctl(sock, SIOCGIFFLAGS, &ifr) == 0) {
            if (!(ifr.ifr_flags & IFF_LOOPBACK)) {
              if (ioctl(sock, SIOCGIFHWADDR, &ifr) == 0) {
                MEMCPY(mac_addr_, ifr.ifr_hwaddr.sa_data, 6);
                mac_addr_found = true;
                break;
              }
            }
          }
        }
      }  // end for
      if (OB_FAIL(ret)) {
      } else if (!mac_addr_found) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error. can not get mac address", K(ret), K(errno));
      } else {
        is_inited_ = true;
      }
    }
    close(sock);
  }
  return ret;
}

int ObUUIDTime::get_time(uint64_t& time, uint16_t& seq)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, ObLatchIds::DEFAULT_MUTEX);
  uint64_t now = 0;
  if (OB_FAIL(time_now(now))) {
    LOG_WARN("get time failed", K(ret));
  } else {
    if (clock_seq_ == 0) {
      reset_clock_seq();
    }
    now = (now / uint64_t(100)) + G1582NS100;
    if (now <= lasttime_) {
      clock_seq_ = static_cast<uint16_t>(((clock_seq_ + 1) & 0x3fff) | 0x8000);
    }
    lasttime_ = now;
    time = now;
    seq = clock_seq_;
  }
  return ret;
}

int ObUUIDTime::time_now(uint64_t& now)
{
  int ret = OB_SUCCESS;
  struct timespec ts;
  int tmpret = clock_gettime(CLOCK_REALTIME, &ts);
  if (tmpret != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get clock time failed", K(ret), K(errno));
  } else {
    now = uint64_t(ts.tv_sec) * uint64_t(1000000000) + uint64_t(ts.tv_nsec);
  }
  return ret;
}

void ObUUIDTime::reset_clock_seq()
{
  uint8_t b[2] = {0, 0};
  srandom(static_cast<unsigned int>(time(NULL)));
  b[0] = static_cast<uint8_t>(random());
  b[1] = static_cast<uint8_t>(random());
  uint16_t seq = static_cast<uint16_t>(uint16_t(b[0] << 8) | uint16_t(b[1]));
  clock_seq_ = static_cast<uint16_t>((seq & 0x3fff) | 0x8000);
}

ObExprUuid::ObExprUuid(ObIAllocator& alloc) : ObFuncExprOperator(alloc, T_FUN_SYS_UUID, N_UUID, 0, NOT_ROW_DIMENSION)
{}

ObExprUuid::ObExprUuid(
    ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num, int32_t dimension)
    : ObFuncExprOperator(alloc, type, name, param_num, dimension)
{}

ObExprUuid::~ObExprUuid()
{}

int ObExprUuid::calc_result0(ObObj& result, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  char* buffer = NULL;
  unsigned char scratch[ObExprUuid::LENGTH_UUID];
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret), K(expr_ctx.calc_buf_));
  } else if (OB_UNLIKELY(NULL == (buffer = static_cast<char*>(expr_ctx.calc_buf_->alloc(ObExprUuid::LENGTH_UUID))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory failed", K(ret));
  } else if (OB_FAIL(calc(scratch))) {
    LOG_WARN("calc failed", K(ret));
  } else {
    // to string
    char* start = buffer;
    for (int64_t i = 0; i < 16; ++i) {
      if (4 == i || 6 == i || 8 == i || 10 == i) {
        sprintf(buffer++, "-");
      }
      sprintf(buffer, "%02x", scratch[i]);
      buffer += 2;
    }
    result.set_varchar(start, ObExprUuid::LENGTH_UUID);
    result.set_collation(result_type_);
  }
  return ret;
}

int ObExprUuid::init()
{
  // will be inited once when and only when server starts
  // so, please do not worry too much about the contention even we use ob_malloc here.
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  mem_attr.label_ = ObModIds::OB_SQL_EXPR;
  uuid_node = static_cast<ObUUIDNode*>(ob_malloc(sizeof(ObUUIDNode), mem_attr));
  if (OB_UNLIKELY(NULL == uuid_node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory failed", K(ret));
  } else if (OB_FAIL(uuid_node->init())) {
    LOG_WARN("init uuid node failed", K(ret));
    ob_free(uuid_node);  // very important ! do not forget this !
    uuid_node = NULL;    // very important ! do not forget this !
  }
  return ret;
}

int ObExprUuid::calc(unsigned char* scratch)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scratch)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. pointer is null", K(ret), K(scratch));
  } else if (OB_ISNULL(uuid_node) || OB_UNLIKELY(!(uuid_node->is_inited_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. uuid node is not inited", K(ret), K(uuid_node));
  } else {
    unsigned char* mac_addr = uuid_node->mac_addr_;
    uint64_t time = 0;
    uint16_t seq = 0;
    if (OB_FAIL(ObUUIDTime::get_time(time, seq))) {
      LOG_WARN("get time failed", K(ret));
    } else {
      uint32_t time_low = static_cast<uint32_t>(time);
      uint16_t time_mid = static_cast<uint16_t>(time >> 32);
      uint16_t time_hi = static_cast<uint16_t>((time >> 48) & 0x0fff);
      time_hi |= 0x1000;
      /*faint. really ugly. 4 if(ob_fail)s here, not efficient at all*/
      /*may I omit the tests for NULL on scratch ? */
      if (OB_FAIL(ObBigEndian::put_uint32(scratch, time_low))) {
        LOG_WARN("put uint32 failed", K(ret));
      } else if (OB_FAIL(ObBigEndian::put_uint16(scratch + 4, time_mid))) {
        LOG_WARN("put uint16 failed", K(ret));
      } else if (OB_FAIL(ObBigEndian::put_uint16(scratch + 6, time_hi))) {
        LOG_WARN("put uint16 failed", K(ret));
      } else if (OB_FAIL(ObBigEndian::put_uint16(scratch + 8, seq))) {
        LOG_WARN("put uint16 failed", K(ret));
      } else {
        MEMCPY(scratch + 10, mac_addr, 6);
      }
    }
  }
  return ret;
}

uint64_t ObUUIDTime::lasttime_ = 0;
uint16_t ObUUIDTime::clock_seq_ = 0;
ObLatch ObUUIDTime::lock_;

ObUUIDNode* ObExprUuid::uuid_node = NULL;

int ObExprUuid::eval_uuid(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  unsigned char scratch[ObExprUuid::LENGTH_UUID] = {0};
  if (OB_FAIL(calc(scratch))) {
    LOG_WARN("calc failed", K(ret));
  } else {
    char* buf = expr.get_str_res_mem(ctx, LENGTH_UUID);
    int64_t pos = 0;
    if (OB_ISNULL(buf)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "buff is null", K(ret));
    } else {
      // to string
      const char* HEXCHARS = "0123456789abcdef";
      for (int64_t i = 0; pos < LENGTH_UUID && i < 16; ++i) {
        if (4 == i || 6 == i || 8 == i || 10 == i) {
          buf[pos++] = '-';
        }
        buf[pos++] = HEXCHARS[scratch[i] >> 4 & 0xF];
        buf[pos++] = HEXCHARS[scratch[i] & 0xF];
      }
      if (OB_SUCC(ret)) {
        expr_datum.set_string(buf, LENGTH_UUID);
      }
    }
  }
  return ret;
}

int ObExprUuid::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprUuid::eval_uuid;
  return OB_SUCCESS;
}

ObExprSysGuid::ObExprSysGuid(ObIAllocator& alloc) : ObExprUuid(alloc, T_FUN_SYS_GUID, N_SYS_GUID, 0, NOT_ROW_DIMENSION)
{}

ObExprSysGuid::~ObExprSysGuid()
{}

int ObExprSysGuid::calc_result0(ObObj& result, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  unsigned char* buffer = NULL;
  CK(OB_NOT_NULL(expr_ctx.calc_buf_));
  if (OB_SUCC(ret) &&
      OB_ISNULL(buffer = static_cast<unsigned char*>(expr_ctx.calc_buf_->alloc(ObExprUuid::LENGTH_UUID)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for sys guid failed", K(ret));
  }
  OZ(calc(buffer));
  OX(result.set_raw(reinterpret_cast<char*>(buffer), ObExprSysGuid::LENGTH_SYS_GUID));
  OX(result.set_collation(result_type_));
  return ret;
}

int ObExprSysGuid::eval_sys_guid(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  unsigned char* buf = reinterpret_cast<unsigned char*>(expr.get_str_res_mem(ctx, LENGTH_UUID));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("buff is null", K(ret));
  } else if (OB_FAIL(calc(buf))) {
    LOG_WARN("calc uuid failed", K(ret));
  } else {
    expr_datum.set_string(reinterpret_cast<char*>(buf), LENGTH_SYS_GUID);
  }
  return ret;
}

ObExprUuidToBin::ObExprUuidToBin(ObIAllocator& alloc) : ObFuncExprOperator(alloc, T_FUN_SYS_UUID_TO_BIN, N_UUID_TO_BIN, ONE_OR_TWO, NOT_ROW_DIMENSION)
{}

ObExprUuidToBin::ObExprUuidToBin(
    ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num, int32_t dimension)
    : ObFuncExprOperator(alloc, type, name, param_num, dimension)
{}

ObExprUuidToBin::~ObExprUuidToBin()
{}

int ObExprUuidToBin::hexchar_to_int(char c) {
  if (c <= '9' && c >= '0') {
    return c - '0';
  }
  c |= 32;
  if (c <= 'f' && c >= 'a') {
    return c - 'a' + 10;
  }
  return -1;
}

int ObExprUuidToBin::uuid_to_bin(ObString& str) {
  int ret = OB_SUCCESS;

  char uuid[ObExprUuidToBin::LENGTH_UUID_NO_DASH];
  char* pos = str.ptr();
  if (str.length() == ObExprUuidToBin::LENGTH_UUID_NO_DASH) {
    memcpy(uuid, pos, ObExprUuidToBin::LENGTH_UUID_NO_DASH);
  } else if (str.length() == ObExprUuidToBin::LENGTH_UUID) {
    if ('-' != *(pos + 8) || '-' != *(pos + 13) || '-' != *(pos + 18) || '-' != *(pos + 23)) {
      ret = OB_ERR_INCORRECT_VALUE_FOR_FUNCTION;
    } else {
      memcpy(uuid, pos, 8);
      memcpy(uuid + 8, pos + 9, 4);
      memcpy(uuid + 12, pos + 14, 4);
      memcpy(uuid + 16, pos + 19, 4);
      memcpy(uuid + 20, pos + 24, 12);
    }
  } else if (str.length() == ObExprUuidToBin::LENGTH_UUID_WITH_BRACES) {
    if ('{' != *pos || '-' != *(pos + 9) || '-' != *(pos + 14) || '-' != *(pos + 19) || '-' != *(pos + 24) || '}' != *(pos + 37)) {
      ret = OB_ERR_INCORRECT_VALUE_FOR_FUNCTION;
    } else {
      memcpy(uuid, pos + 1, 8);
      memcpy(uuid + 8, pos + 10, 4);
      memcpy(uuid + 12, pos + 15, 4);
      memcpy(uuid + 16, pos + 20, 4);
      memcpy(uuid + 20, pos + 25, 12);
    }
  } else {
    ret = OB_ERR_INCORRECT_VALUE_FOR_FUNCTION;
  }

  if (OB_SUCC(ret)) {
    int j = 0;
    char bin[ObExprUuidToBin::LENGTH_BIN_UUID];
    for (int i = 0; i < ObExprUuidToBin::LENGTH_UUID_NO_DASH; i += 2) {
      int high = hexchar_to_int(uuid[i]);
      int low = hexchar_to_int(uuid[i + 1]);

      if (high < 0 || low < 0) {
        ret = OB_ERR_INCORRECT_VALUE_FOR_FUNCTION;
        break;
      }
      bin[j++] = (high << 4) + low;
    }
    memcpy(pos, bin, ObExprUuidToBin::LENGTH_BIN_UUID);
  }
  return ret;
}

int ObExprUuidToBin::calc_result_typeN(ObExprResType &type,
                                  ObExprResType *types_array,
                                  int64_t param_num,
                                  ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != param_num && 2 != param_num)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid argument count of funtion uuid_to_bin", K(ret));
  } else {
    types_array[0].set_calc_type(ObCharType);
    if (2 == param_num) {
      types_array[1].set_calc_type(ObTinyIntType);
    }

    type.set_hex_string();
    type.set_result_flag(OB_MYSQL_BINARY_FLAG);
    type.set_length(ObExprUuidToBin::LENGTH_BIN_UUID);
  }
  return ret;
}


int ObExprUuidToBin::eval_uuid_to_bin(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (expr.locate_param_datum(ctx, 0).is_null()) {
    expr_datum.set_null();
  } else {
    bool swap_flag = false;
    if (2 == expr.arg_cnt_) {
      if (!expr.locate_param_datum(ctx, 1).is_null() && expr.locate_param_datum(ctx, 1).get_int() != 0) {
        swap_flag = true;
      }
    }

    ObString str = expr.locate_param_datum(ctx, 0).get_string();
    ret = uuid_to_bin(str);
    if (OB_FAIL(ret)) {
       ObString data_type("string");
       ObString func_name("uuid_to_bin");
       LOG_USER_ERROR(OB_ERR_INCORRECT_VALUE_FOR_FUNCTION,
                data_type.length(), data_type.ptr(),
                str.length(), str.ptr(),
                func_name.length(), func_name.ptr());
    } else {
      const char* bin = str.ptr();
      char* buf = expr.get_str_res_mem(ctx, ObExprUuidToBin::LENGTH_BIN_UUID);
      if (swap_flag) {
        memcpy(buf, bin + 6, 2);
        memcpy(buf + 2, bin + 4, 2);
        memcpy(buf + 4, bin, 4);
        memcpy(buf + 8, bin + 8, 8);
      } else {
        memcpy(buf, bin, ObExprUuidToBin::LENGTH_BIN_UUID);
      }
      expr_datum.set_string(buf, ObExprUuidToBin::LENGTH_BIN_UUID);
    }
  }

  return ret;
}

int ObExprUuidToBin::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_ && 2 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = eval_uuid_to_bin;
  }
  return ret;
}

ObExprBinToUuid::ObExprBinToUuid(ObIAllocator& alloc) : ObExprUuidToBin(alloc, T_FUN_SYS_BIN_TO_UUID, N_BIN_TO_UUID, ONE_OR_TWO, NOT_ROW_DIMENSION)
{}

ObExprBinToUuid::ObExprBinToUuid(
    ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num, int32_t dimension)
    : ObExprUuidToBin(alloc, type, name, param_num, dimension)
{}

ObExprBinToUuid::~ObExprBinToUuid()
{}

int ObExprBinToUuid::calc_result_typeN(ObExprResType &type,
                                  ObExprResType *types_array,
                                  int64_t param_num,
                                  ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != param_num && 2 != param_num)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid argument count of funtion bin_to_uuid", K(ret));
  } else {
    types_array[0].set_hex_string();
    types_array[0].set_result_flag(OB_MYSQL_BINARY_FLAG);
    if (2 == param_num) {
      types_array[1].set_calc_type(ObTinyIntType);
    }

    type.set_varchar();
    type.set_collation_level(common::CS_LEVEL_IMPLICIT);
    type.set_collation_type(common::ObCharset::get_default_collation(common::ObCharset::get_default_charset()));
    type.set_length(ObExprBinToUuid::LENGTH_UUID);
  }
  return ret;
}


int ObExprBinToUuid::eval_bin_to_uuid(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (expr.locate_param_datum(ctx, 0).is_null()) {
    expr_datum.set_null();
  } else {
    bool swap_flag = false;
    if (2 == expr.arg_cnt_) {
      if (!expr.locate_param_datum(ctx, 1).is_null() && expr.locate_param_datum(ctx, 1).get_int() != 0) {
        swap_flag = true;
      }
    }

    ObString bin = expr.locate_param_datum(ctx, 0).get_string();
    if (bin.length() != ObExprBinToUuid::LENGTH_BIN_UUID) {
      ret = OB_ERR_INCORRECT_VALUE_FOR_FUNCTION;
    } else {
      int j = 0;
      char uuid[ObExprBinToUuid::LENGTH_UUID];
      for (int i = 0; i < ObExprBinToUuid::LENGTH_BIN_UUID; i++) {
        uuid[j++] = hexValues[(bin[i] >> 4) & 0xf];
        uuid[j++] = hexValues[bin[i] & 0xf];
        if(j == 8 || j == 13 || j == 18 || j == 23) {
          uuid[j++] = '-';
        }
      }
      char* buf = expr.get_str_res_mem(ctx, ObExprBinToUuid::LENGTH_UUID);
      if (swap_flag) {
        memcpy(buf, uuid + 9, 4);
        memcpy(buf + 4, uuid + 14, 4);
        memcpy(buf + 8, uuid + 8, 1);
        memcpy(buf + 9, uuid + 4, 4);
        memcpy(buf + 13, uuid + 13, 1);
        memcpy(buf + 14, uuid, 4);
        memcpy(buf + 18, uuid + 18, 14);
      } else {
        memcpy(buf, uuid, ObExprBinToUuid::LENGTH_UUID);
      }
      expr_datum.set_string(buf, ObExprBinToUuid::LENGTH_UUID);
    }

    if (OB_FAIL(ret)) {
       ObString data_type("string");
       ObString func_name("bin_to_uuid");
       LOG_USER_ERROR(OB_ERR_INCORRECT_VALUE_FOR_FUNCTION,
                data_type.length(), data_type.ptr(),
                bin.length(), bin.ptr(),
                func_name.length(), func_name.ptr());
    }
  }

  return ret;
}

int ObExprBinToUuid::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_ && 2 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = eval_bin_to_uuid;
  }
  return ret;
}


ObExprIsUuid::ObExprIsUuid(ObIAllocator& alloc) : ObExprUuidToBin(alloc, T_FUN_SYS_IS_UUID, N_IS_UUID, 1, NOT_ROW_DIMENSION)
{}

ObExprIsUuid::ObExprIsUuid(
    ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num, int32_t dimension)
    : ObExprUuidToBin(alloc, type, name, param_num, dimension)
{}

ObExprIsUuid::~ObExprIsUuid()
{}

int ObExprIsUuid::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const {
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type1.set_calc_type(ObCharType);
  type.set_tinyint();
  return ret;
}

int ObExprIsUuid::eval_is_uuid(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (expr.locate_param_datum(ctx, 0).is_null()) {
    expr_datum.set_null();
  } else {
    ObString str = expr.locate_param_datum(ctx, 0).get_string();
    ret = uuid_to_bin(str);
    if (OB_FAIL(ret)) {
      ret = OB_SUCCESS;
      expr_datum.set_int(0);
    } else {
      expr_datum.set_int(1);
    }
  }
  return ret;
}

int ObExprIsUuid::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = eval_is_uuid;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
