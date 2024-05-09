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

#define USING_LOG_PREFIX  SQL_ENG
#include <string.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>
#include "sql/engine/expr/ob_expr_uuid.h"
#include "sql/engine/ob_exec_context.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

int ObBigEndian::put_uint16(unsigned char *b, uint16_t v)
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
int ObBigEndian::put_uint32(unsigned char *b, uint32_t v)
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
      ifreq *it = ifc.ifc_req;
      ifreq *end = it + (ifc.ifc_len / sizeof(ifreq));
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
      }//end for
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


int ObUUIDTime::get_time(uint64_t &time, uint16_t &seq)
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

int ObUUIDTime::time_now(uint64_t &now)
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

int UuidCommon::bin2uuid(char *result, const uchar *src) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result) || OB_ISNULL(src)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(src), K(result));
  } else {
    int64_t pos = 0;
    const char *HEXCHARS = "0123456789abcdef";
    for (int64_t i = 0; pos < UuidCommon::LENGTH_UUID && i < 16; ++i) {
      if (4 == i || 6 == i || 8 == i || 10 == i) {
        result[pos++] = '-';
      }
      result[pos++] = HEXCHARS[src[i] >> 4 & 0xF];
      result[pos++] = HEXCHARS[src[i] & 0xF];
    }
  }
  return ret;
}

int UuidCommon::read_section(char *res, bool &is_valid, const char *src, int64_t len) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(res) || OB_ISNULL(src)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(res), K(src));
  } else {
    int cnt = 0;
    is_valid = true;
    while(is_valid && cnt < len) {
      if (!isxdigit(src[cnt * 2]) || !isxdigit(src[cnt * 2 + 1])) {
        is_valid = false;
      } else {
        char hi = common::get_xdigit(src[cnt * 2]);
        char lo = common::get_xdigit(src[cnt * 2 + 1]);
        res[cnt] = static_cast<uchar> ((hi << 4) + lo);
        cnt ++;
      }
    }
  }
  return ret;
}

//convert uuid to bin, is_valid is set to false if src is null or not a valid string-format UUID.
int UuidCommon::uuid2bin(char *result, bool &is_valid, const char *src, int64_t len) {
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(src), K(result));
  } else if (OB_ISNULL(src)) {
    is_valid = false;
  } else {
    char bin_buf[UuidCommon::BYTE_LENGTH + 1];
    switch (len) {
      case UuidCommon::LENGTH_UUID - 4:{
        if (OB_FAIL(UuidCommon::read_section(result, is_valid, src, UuidCommon::BYTE_LENGTH))) {
          LOG_WARN("fail to read section", K(ret));
        }
      }
      break;
      case UuidCommon::LENGTH_UUID + 2: {
        if ('{' == src[0] && '}' == src[len - 1]) {
          src++;
        } else {
          is_valid = false;
        }
      }
      case UuidCommon::LENGTH_UUID: {
        int cnt = 0;
        for (int i = 0; OB_SUCC(ret) && i < 5 && is_valid; ++i) {
          if (OB_FAIL(UuidCommon::read_section(result + cnt, is_valid, src + cnt * 2 + i, UuidCommon::bytes_per_section[i]))) {
            LOG_WARN("fail to read section", K(ret), K(i));
          } else {
            cnt += UuidCommon::bytes_per_section[i];
          }
          if (i < 4 && src[cnt * 2 + i] != '-') {
            is_valid = false;
          }
        }
      }
      break;
      default: {
        is_valid = false;
      }
    }
  }
  return ret;
}

ObExprUuid::ObExprUuid(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_UUID, N_UUID, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprUuid::ObExprUuid(
  ObIAllocator &alloc,
  ObExprOperatorType type,
  const char *name,
  int32_t param_num,
  int32_t dimension)
  : ObFuncExprOperator(alloc, type, name, param_num, NOT_VALID_FOR_GENERATED_COL, dimension)
{
}

ObExprUuid::~ObExprUuid()
{
}

int ObExprUuid::init()
{
  //will be inited once when and only when server starts
  //so, please do not worry too much about the contention even we use ob_malloc here.
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  mem_attr.label_ = ObModIds::OB_SQL_EXPR;
  uuid_node = static_cast<ObUUIDNode*>(ob_malloc(sizeof(ObUUIDNode), mem_attr));
  if (OB_UNLIKELY(NULL == uuid_node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory failed", K(ret));
  } else if (OB_FAIL(uuid_node->init())) {
    LOG_WARN("init uuid node failed", K(ret));
    ob_free(uuid_node);//very important ! do not forget this !
    uuid_node = NULL;//very important ! do not forget this !
  }
  return ret;
}

int ObExprUuid::calc(unsigned char *scratch)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scratch)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. pointer is null", K(ret), K(scratch));
  } else if (OB_ISNULL(uuid_node) || OB_UNLIKELY(!(uuid_node->is_inited_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error. uuid node is not inited", K(ret), K(uuid_node));
  } else {
    unsigned char *mac_addr = uuid_node->mac_addr_;
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

ObUUIDNode *ObExprUuid::uuid_node = NULL;

int ObExprUuid::eval_uuid(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  unsigned char scratch[UuidCommon::LENGTH_UUID] = {0};
  if (OB_FAIL(calc(scratch))) {
    LOG_WARN("calc failed", K(ret));
  } else {
    char *buf = expr.get_str_res_mem(ctx, UuidCommon::LENGTH_UUID);
    int64_t pos = 0;
    if (OB_ISNULL(buf)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "buff is null", K(ret));
    } else if (OB_FAIL(UuidCommon::bin2uuid(buf, scratch))) {
      LOG_WARN("fail to convert strach to uuid", K(ret), K(buf), K(scratch));
    } else {
      expr_datum.set_string(buf, UuidCommon::LENGTH_UUID);
    }
  }
  return ret;
}

int ObExprUuid::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprUuid::eval_uuid;
  return OB_SUCCESS;
}

ObExprSysGuid::ObExprSysGuid(ObIAllocator &alloc)
  : ObExprUuid(alloc, T_FUN_SYS_GUID, N_SYS_GUID, 0, NOT_ROW_DIMENSION)
{
}

ObExprSysGuid::~ObExprSysGuid()
{
}

int ObExprSysGuid::eval_sys_guid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  unsigned char *buf = reinterpret_cast<unsigned char *>(expr.get_str_res_mem(ctx, UuidCommon::LENGTH_UUID));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("buff is null", K(ret));
  } else if (OB_FAIL(calc(buf))) {
    LOG_WARN("calc uuid failed", K(ret));
  } else {
    expr_datum.set_string(reinterpret_cast<char *>(buf), LENGTH_SYS_GUID);
  }
  return ret;
}

int ObExprUuid::gen_server_uuid(char *server_uuid, const int64_t uuid_len)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator calc_buf("ServerUuid");
  unsigned char scratch[UuidCommon::LENGTH_UUID] = {0};
  bool need_reset = false;
  if (OB_ISNULL(uuid_node)) {
    need_reset = true;
    if (OB_ISNULL(uuid_node = static_cast<ObUUIDNode*>(calc_buf.alloc(sizeof(ObUUIDNode))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FAIL(uuid_node->init())) {
      LOG_WARN("failed to init", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_ISNULL(server_uuid) || OB_UNLIKELY(uuid_len != UuidCommon::LENGTH_UUID)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(server_uuid), K(uuid_len));
  } else if (OB_FAIL(calc(scratch))) {
    LOG_WARN("failed to calc", K(ret));
  } else if (OB_FAIL(UuidCommon::bin2uuid(server_uuid, scratch))) {
    LOG_WARN("fail to convert strach to server_uuid", K(ret), K(server_uuid), K(scratch));
  } else {/*do nothing*/}

  if (need_reset) {
    uuid_node = NULL;
  }
  return ret;
}

ObExprUuid2bin::ObExprUuid2bin(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_UUID2BIN, N_UUID_TO_BIN, ONE_OR_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprUuid2bin::~ObExprUuid2bin()
{
}


int ObExprUuid2bin::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *types,
                                      int64_t type_num,
                                      common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_UNLIKELY(NULL == types || type_num <= 0 || type_num > 2)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("Invalid argument.", K(ret), K(types), K(type_num));
  } else {
    types[0].set_calc_type(common::ObVarcharType);
    if (2 == type_num) {
      types[1].set_calc_type(types[1].get_type());
    }
  }
  type.set_varbinary();
  type.set_length(UuidCommon::BYTE_LENGTH);
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  return ret;
}

int ObExprUuid2bin::cg_expr(ObExprCGCtx &op_cg_ctx,
                          const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_ && 2 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else if (raw_expr.get_param_count() == 2) {
    const ObRawExpr *flag_expr = raw_expr.get_param_expr(1);
    if (OB_ISNULL(flag_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(rt_expr.args_[1]));
    } else {
      ObObjType flag_expr_type = flag_expr->get_result_type().get_type();
      if (OB_UNLIKELY(ObNullType != flag_expr_type &&
                  !ob_is_integer_type(flag_expr_type))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("swap flag expr must return integer", K(ret), K(flag_expr_type));
      }
    }
  }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprUuid2bin::uuid2bin;
    // Only implement vectorization when parameter 0 is batch and parameter 1 is constant or null
    if (OB_ISNULL(rt_expr.args_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(rt_expr.args_[0]));
    } else if (rt_expr.args_[0]->is_batch_result()) {
      if (rt_expr.arg_cnt_ != 2 || !rt_expr.args_[1]->is_batch_result()) {
        rt_expr.eval_batch_func_ = ObExprUuid2bin::uuid2bin_batch;
      } else {}
    } else {}
  }
  return ret;
}

int ObExprUuid2bin::uuid2bin(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *text = nullptr;
  ObDatum *swap_flag = nullptr;
  bool need_swap = false;
  bool is_null = false;
  if (OB_FAIL(expr.args_[0]->eval(ctx, text))) {
    LOG_WARN("eval arg 0 failed", K(ret));
  } else if (OB_ISNULL(text)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(text));
  } else if (text->is_null()) {
    expr_datum.set_null();
    is_null = true;
  } else if (expr.arg_cnt_ == 2) {
    if (OB_ISNULL(expr.args_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(expr.args_[1]));
    } else if (OB_FAIL(expr.args_[1]->eval(ctx, swap_flag))) {
      LOG_WARN("eval arg 1 failed", K(ret));
    } else if (OB_ISNULL(swap_flag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(swap_flag));
    } else if (swap_flag->is_null()) {
      need_swap = false;
    } else {
      need_swap = swap_flag->get_bool();
    }
  } else {
    need_swap = false;
  }

  if (OB_SUCC(ret) && !is_null) {
    ObString uuid_text = text->get_string();
    char *res_buf = expr.get_str_res_mem(ctx, UuidCommon::BYTE_LENGTH);
    const char *text_ptr = uuid_text.ptr();
    bool is_valid = true;

    if (OB_ISNULL(res_buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(res_buf));
    } else if (OB_FAIL(UuidCommon::uuid2bin(res_buf, is_valid, text_ptr, uuid_text.length()))) {
      LOG_WARN("fail to parse uuid to bin", K(ret));
    } else if (!is_valid) {
      ret = OB_ERR_INCORRECT_VALUE_FOR_FUNCTION;
      ObString string_type_str("string");
      ObString func_str(N_UUID_TO_BIN);
      LOG_USER_ERROR(OB_ERR_INCORRECT_VALUE_FOR_FUNCTION,
                      string_type_str.length(), string_type_str.ptr(),
                      uuid_text.length(), uuid_text.ptr(),
                      func_str.length(), func_str.ptr());
      LOG_WARN("incorrect string value for function uuid_to_bin", K(ret), K(uuid_text));
    } else if (need_swap) { //check if need swap
      char swap_buf[8];
      MEMCPY(swap_buf, res_buf + 6, 2);
      MEMCPY(swap_buf + 2, res_buf + 4, 2);
      MEMCPY(swap_buf + 4, res_buf, 4);
      MEMCPY(res_buf, swap_buf, 8);
    } else {
    }
    if (OB_SUCC(ret)) {
      expr_datum.set_string(res_buf, UuidCommon::BYTE_LENGTH);
    }
  }
  return ret;
}


int ObExprUuid2bin::uuid2bin_batch(const ObExpr &expr,
                                   ObEvalCtx &ctx,
                                   const ObBitVector &skip,
                                   const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  bool need_swap = false;
  if (OB_ISNULL(expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(expr.args_[0]));
  } else if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval arg 0 failed", K(ret));
  } else if (expr.arg_cnt_ == 2) {
    ObDatum *swap_flag = nullptr;
    if (OB_ISNULL(expr.args_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(expr.args_[1]));
    } else if (OB_FAIL(expr.args_[1]->eval(ctx, swap_flag))) {
      LOG_WARN("eval arg 1 failed", K(ret));
    } else if (OB_ISNULL(swap_flag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(swap_flag));
    } else if (swap_flag->is_null()) {
      need_swap = false;
    } else {
      need_swap = swap_flag->get_bool();
    }
  } else {
    need_swap = false;
  }

  if (OB_SUCC(ret)) {
    ObDatum *res_datum = expr.locate_batch_datums(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }
      ObDatum &text= expr.args_[0]->locate_expr_datum(ctx, i);
      if (text.is_null()) {
        res_datum[i].set_null();
      } else {
        ObString uuid_text = text.get_string();
        char *res_buf = expr.get_str_res_mem(ctx, UuidCommon::BYTE_LENGTH, i);
        const char *text_ptr = uuid_text.ptr();
        bool is_valid = true;
        if (OB_ISNULL(res_buf)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(res_buf));
        } else if (OB_FAIL(UuidCommon::uuid2bin(res_buf, is_valid, text_ptr, uuid_text.length()))) {
          LOG_WARN("fail to parse uuid to bin", K(ret));
        } else if (!is_valid) {
          ret = OB_ERR_INCORRECT_VALUE_FOR_FUNCTION;
          ObString string_type_str("string");
          ObString func_str(N_UUID_TO_BIN);
          LOG_USER_ERROR(OB_ERR_INCORRECT_VALUE_FOR_FUNCTION,
                        string_type_str.length(), string_type_str.ptr(),
                        uuid_text.length(), uuid_text.ptr(),
                        func_str.length(), func_str.ptr());
          LOG_WARN("incorrect string value for function uuid_to_bin", K(ret), K(uuid_text));
        } else if (need_swap) {
          char swap_buf[8];
          MEMCPY(swap_buf, res_buf + 6, 2);
          MEMCPY(swap_buf + 2, res_buf + 4, 2);
          MEMCPY(swap_buf + 4, res_buf, 4);
          MEMCPY(res_buf, swap_buf, 8);
        }
        if (OB_SUCC(ret)) {
          res_datum[i].set_string(res_buf, UuidCommon::BYTE_LENGTH);
        }
      }
      eval_flags.set(i);
    }
  }
  return ret;
}

ObExprIsUuid::ObExprIsUuid(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_IS_UUID, N_IS_UUID, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprIsUuid::~ObExprIsUuid()
{
}

int ObExprIsUuid::cg_expr(ObExprCGCtx &op_cg_ctx,
                          const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = ObExprIsUuid::is_uuid;
    rt_expr.eval_batch_func_ = ObExprIsUuid::is_uuid_batch;
  }
  return ret;
}


int ObExprIsUuid::is_uuid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *text = nullptr;
  if (OB_ISNULL(expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(expr.args_[0]));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, text))) {
    LOG_WARN("eval arg 0 failed", K(ret));
  } else if (OB_ISNULL(text)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(text));
  } else if (text->is_null()) {
    expr_datum.set_null();
  } else {
    ObString uuid_text = text->get_string();
    const char *text_ptr = uuid_text.ptr();
    char result_buf[UuidCommon::BYTE_LENGTH];
    bool is_valid = true;
    if (OB_ISNULL(text_ptr)) {
      expr_datum.set_bool(false);
    } else if (OB_FAIL(UuidCommon::uuid2bin(result_buf, is_valid, text_ptr, uuid_text.length()))) {
      LOG_WARN("fail to parse uuid to bin", K(ret));
    } else {
      expr_datum.set_bool(is_valid);
    }
  }
  return ret;
}

int ObExprIsUuid::is_uuid_batch(const ObExpr &expr,
                                ObEvalCtx &ctx,
                                const ObBitVector &skip,
                                const int64_t batch_size) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(expr.args_[0]));
  } else if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval arg 0 failed", K(ret));
  } else {
    ObDatum *res_datum = expr.locate_batch_datums(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }
      ObDatum &text= expr.args_[0]->locate_expr_datum(ctx, i);
      if (text.is_null()) {
        res_datum[i].set_null();
      } else {
        ObString uuid_text = text.get_string();
        const char *text_ptr = uuid_text.ptr();
        char result_buf[UuidCommon::BYTE_LENGTH];
        bool is_valid = true;
        if (OB_FAIL(UuidCommon::uuid2bin(result_buf, is_valid, text_ptr, uuid_text.length()))) {
          LOG_WARN("fail to parse uuid to bin", K(ret));
        } else {
          res_datum[i].set_bool(is_valid);
        }
      }
      eval_flags.set(i);
    }
  }
  return ret;
}

ObExprBin2uuid::ObExprBin2uuid(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_BIN2UUID, N_BIN_TO_UUID, ONE_OR_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprBin2uuid::~ObExprBin2uuid()
{
}

int ObExprBin2uuid::calc_result_typeN(ObExprResType &type,
                                      ObExprResType *types,
                                      int64_t type_num,
                                      common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_UNLIKELY(NULL == types || type_num <= 0 || type_num > 2)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("Invalid argument.", K(ret), K(types), K(type_num));
  } else {
    types[0].set_calc_type(common::ObVarcharType);
    types[0].set_calc_collation_type(CS_TYPE_BINARY);
    if (2 == type_num) {
      types[1].set_calc_type(types[1].get_type());
    }
  }
  type.set_varchar();
  type.set_collation_level(common::CS_LEVEL_IMPLICIT);
  type.set_collation_type(common::ObCharset::get_default_collation(common::ObCharset::get_default_charset()));
  type.set_length(UuidCommon::LENGTH_UUID);
  return ret;
}

int ObExprBin2uuid::cg_expr(ObExprCGCtx &op_cg_ctx,
                          const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_ && 2 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else if (raw_expr.get_param_count() == 2) {
    const ObRawExpr *flag_expr = raw_expr.get_param_expr(1);
    if (OB_ISNULL(flag_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(rt_expr.args_[1]));
    } else {
      ObObjType flag_expr_type = flag_expr->get_result_type().get_type();
      if (OB_UNLIKELY(ObNullType != flag_expr_type &&
                  !ob_is_integer_type(flag_expr_type))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("swap flag expr must return integer", K(ret), K(flag_expr_type));
      }
    }
  }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = ObExprBin2uuid::bin2uuid;
    // Only implement vectorization when parameter 0 is batch and parameter 1 is constant or null
    if (OB_ISNULL(rt_expr.args_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(rt_expr.args_[0]));
    } else if (rt_expr.args_[0]->is_batch_result()) {
      if (rt_expr.arg_cnt_ == 2) {
        if (OB_ISNULL(rt_expr.args_[1])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(rt_expr.args_[0]));
        } else if (!rt_expr.args_[1]->is_batch_result()) {
          rt_expr.eval_batch_func_ = ObExprBin2uuid::bin2uuid_batch;
        }
      } else {
        rt_expr.eval_batch_func_ = ObExprBin2uuid::bin2uuid_batch;
      }
    } else {
    }
  }
  return ret;
}

int ObExprBin2uuid::bin2uuid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *text = nullptr;
  ObDatum *swap_flag = nullptr;
  bool need_swap = false;
  bool is_null = false;
  if (OB_ISNULL(expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(expr.args_[0]));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, text))) {
    LOG_WARN("eval arg 0 failed", K(ret));
  } else if (OB_ISNULL(text)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(text));
  } else if (text->is_null()) {
    expr_datum.set_null();
    is_null = true;
  } else if (expr.arg_cnt_ == 2) {
    if (OB_ISNULL(expr.args_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(expr.args_[0]));
    } else if (OB_FAIL(expr.args_[1]->eval(ctx, swap_flag))) {
      LOG_WARN("eval arg 1 failed", K(ret));
    } else if (OB_ISNULL(swap_flag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(swap_flag));
    } else if (swap_flag->is_null()) {
      need_swap = false;
    } else {
      need_swap = swap_flag->get_bool();
    }
  } else {
    need_swap = false;
  }

  if (OB_SUCC(ret) && !is_null) {
    ObString bin_text = text->get_string();
    char *res_buf = expr.get_str_res_mem(ctx, UuidCommon::LENGTH_UUID);
    const char *bin_text_ptr = bin_text.ptr();
    uchar new_bin_text[UuidCommon::BYTE_LENGTH];
    if (OB_ISNULL(res_buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(res_buf));
    } else if (OB_ISNULL(bin_text_ptr) || UuidCommon::BYTE_LENGTH != bin_text.length()) {
      ObString string_type_str("binary");
      ObString func_str(N_BIN_TO_UUID);
      int text_len = std::min(bin_text.length(), UuidCommon::BYTE_LENGTH * 2);
      SMART_VAR(char[UuidCommon::BYTE_LENGTH * 4], hex_buf) {
        int64_t pos = 0;
        if (OB_SUCC(common::hex_print(bin_text.ptr(), text_len, hex_buf, text_len*2, pos))) {
          ret = OB_ERR_INCORRECT_VALUE_FOR_FUNCTION;
          LOG_USER_ERROR(OB_ERR_INCORRECT_VALUE_FOR_FUNCTION,
                        string_type_str.length(), string_type_str.ptr(),
                        text_len*2, hex_buf,
                        func_str.length(), func_str.ptr());
          LOG_WARN("incorrect string value for function bin_to_uuid", K(ret), K(bin_text));
        } else {
          LOG_WARN("fail to print incorrect hex string value", K(ret), K(bin_text));
        }
      }
    } else  if (need_swap) {
      // The first 4 bytes are restored to "time-low".
      MEMCPY(new_bin_text, bin_text_ptr + 4, 4);
      // Bytes starting with 4th will be restored to "time-mid".
      MEMCPY(new_bin_text + 4, bin_text_ptr + 2, 2);
      // Bytes starting with 6th will be restored to "time-high".
      MEMCPY(new_bin_text + 6, bin_text_ptr, 2);
      // The last 8 bytes were not changed so we just copy them.
      MEMCPY(new_bin_text + 8, bin_text_ptr + 8, 8);
    } else {
      MEMCPY(new_bin_text, bin_text_ptr, UuidCommon::BYTE_LENGTH);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(UuidCommon::bin2uuid(res_buf, new_bin_text))) {
        LOG_WARN("fail to parse uuid to bin", K(ret));
      } else {
        expr_datum.set_string(res_buf, UuidCommon::LENGTH_UUID);
      }
    }
  }
  return ret;
}

int ObExprBin2uuid::bin2uuid_batch(const ObExpr &expr,
                                   ObEvalCtx &ctx,
                                   const ObBitVector &skip,
                                   const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  bool need_swap = false;
  if (OB_ISNULL(expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(expr.args_[0]));
  } else if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval arg 0 failed", K(ret));
  } else if (expr.arg_cnt_ == 2) {
    ObDatum *swap_flag = nullptr;
    if (OB_ISNULL(expr.args_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null", K(expr.args_[1]));
    } else if (OB_FAIL(expr.args_[1]->eval(ctx, swap_flag))) {
      LOG_WARN("eval arg 1 failed", K(ret));
    } else if (OB_ISNULL(swap_flag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null", K(swap_flag));
    } else if (swap_flag->is_null()) {
      need_swap = false;
    } else {
      need_swap = swap_flag->get_bool();
    }
  } else {
    need_swap = false;
  }

  if (OB_SUCC(ret)) {
    ObDatum *res_datum = expr.locate_batch_datums(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }
      ObDatum &text = expr.args_[0]->locate_expr_datum(ctx, i);
      if (text.is_null()) {
        res_datum[i].set_null();
      } else {
        ObString bin_text = text.get_string();
        char *res_buf = expr.get_str_res_mem(ctx, UuidCommon::LENGTH_UUID, i);
        const char *bin_text_ptr = bin_text.ptr();
        uchar new_bin_text[UuidCommon::BYTE_LENGTH];
        if (OB_ISNULL(res_buf)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(bin_text_ptr), K(res_buf));
        } else if (OB_ISNULL(bin_text_ptr) || UuidCommon::BYTE_LENGTH != bin_text.length()) {
          ObString string_type_str("binary");
          ObString func_str(N_BIN_TO_UUID);
          int text_len = std::min(bin_text.length(), UuidCommon::BYTE_LENGTH * 2);
          SMART_VAR(char[UuidCommon::BYTE_LENGTH * 4], hex_buf) {
            int64_t pos = 0;
            if (OB_SUCC(common::hex_print(bin_text.ptr(), text_len, hex_buf, text_len*2, pos))) {
              ret = OB_ERR_INCORRECT_VALUE_FOR_FUNCTION;
              LOG_USER_ERROR(OB_ERR_INCORRECT_VALUE_FOR_FUNCTION,
                            string_type_str.length(), string_type_str.ptr(),
                            text_len*2, hex_buf,
                            func_str.length(), func_str.ptr());
              LOG_WARN("incorrect string value for function bin_to_uuid", K(ret), K(bin_text));
            } else {
              LOG_WARN("fail to print incorrect hex string value", K(ret), K(bin_text));
            }
          }
        } else  if (need_swap) {
          // The first 4 bytes are restored to "time-low".
          MEMCPY(new_bin_text, bin_text_ptr + 4, 4);
          // Bytes starting with 4th will be restored to "time-mid".
          MEMCPY(new_bin_text + 4, bin_text_ptr + 2, 2);
          // Bytes starting with 6th will be restored to "time-high".
          MEMCPY(new_bin_text + 6, bin_text_ptr, 2);
          // The last 8 bytes were not changed so we just copy them.
          MEMCPY(new_bin_text + 8, bin_text_ptr + 8, 8);
        } else {
          MEMCPY(new_bin_text, bin_text_ptr, UuidCommon::BYTE_LENGTH);
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(UuidCommon::bin2uuid(res_buf, new_bin_text))) {
            LOG_WARN("fail to parse uuid to bin", K(ret));
          } else {
            res_datum[i].set_string(res_buf, UuidCommon::LENGTH_UUID);
          }
        }
      }
      eval_flags.set(i);
    }
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
