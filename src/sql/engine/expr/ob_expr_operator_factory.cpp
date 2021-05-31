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
#include "sql/engine/expr/ob_expr_operator_factory.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_substring_index.h"
#include "sql/engine/expr/ob_expr_strcmp.h"
#include "sql/engine/expr/ob_expr_add.h"
#include "sql/engine/expr/ob_expr_assign.h"
#include "sql/engine/expr/ob_expr_database.h"
#include "sql/engine/expr/ob_expr_and.h"
#include "sql/engine/expr/ob_expr_sin.h"
#include "sql/engine/expr/ob_expr_cos.h"
#include "sql/engine/expr/ob_expr_tan.h"
#include "sql/engine/expr/ob_expr_asin.h"
#include "sql/engine/expr/ob_expr_acos.h"
#include "sql/engine/expr/ob_expr_atan.h"
#include "sql/engine/expr/ob_expr_atan2.h"
#include "sql/engine/expr/ob_expr_arg_case.h"
#include "sql/engine/expr/ob_expr_between.h"
#include "sql/engine/expr/ob_expr_bit_and.h"
#include "sql/engine/expr/ob_expr_bit_xor.h"
#include "sql/engine/expr/ob_expr_bit_or.h"
#include "sql/engine/expr/ob_expr_bit_neg.h"
#include "sql/engine/expr/ob_expr_bit_left_shift.h"
#include "sql/engine/expr/ob_expr_bit_right_shift.h"
#include "sql/engine/expr/ob_expr_case.h"
#include "sql/engine/expr/ob_expr_oracle_decode.h"
#include "sql/engine/expr/ob_expr_oracle_trunc.h"
#include "sql/engine/expr/ob_expr_fun_values.h"
#include "sql/engine/expr/ob_expr_fun_default.h"
#include "sql/engine/expr/ob_expr_cast.h"
#include "sql/engine/expr/ob_expr_to_type.h"
#include "sql/engine/expr/ob_expr_convert.h"
#include "sql/engine/expr/ob_expr_coalesce.h"
#include "sql/engine/expr/ob_expr_current_user.h"
#include "sql/engine/expr/ob_expr_nvl.h"
#include "sql/engine/expr/ob_expr_concat.h"
#include "sql/engine/expr/ob_expr_concat_ws.h"
#include "sql/engine/expr/ob_expr_div.h"
#include "sql/engine/expr/ob_expr_effective_tenant.h"
#include "sql/engine/expr/ob_expr_effective_tenant_id.h"
#include "sql/engine/expr/ob_expr_equal.h"
#include "sql/engine/expr/ob_expr_from_unix_time.h"
#include "sql/engine/expr/ob_expr_null_safe_equal.h"
#include "sql/engine/expr/ob_expr_get_user_var.h"
#include "sql/engine/expr/ob_expr_greater_equal.h"
#include "sql/engine/expr/ob_expr_greater_than.h"
#include "sql/engine/expr/ob_expr_greatest.h"
#include "sql/engine/expr/ob_expr_agg_param_list.h"
#include "sql/engine/expr/ob_expr_is_serving_tenant.h"
#include "sql/engine/expr/ob_expr_hex.h"
#include "sql/engine/expr/ob_expr_in.h"
#include "sql/engine/expr/ob_expr_not_in.h"
#include "sql/engine/expr/ob_expr_int2ip.h"
#include "sql/engine/expr/ob_expr_ip2int.h"
#include "sql/engine/expr/ob_expr_last_exec_id.h"
#include "sql/engine/expr/ob_expr_last_trace_id.h"
#include "sql/engine/expr/ob_expr_is.h"
#include "sql/engine/expr/ob_expr_least.h"
#include "sql/engine/expr/ob_expr_length.h"
#include "sql/engine/expr/ob_expr_less_equal.h"
#include "sql/engine/expr/ob_expr_less_than.h"
#include "sql/engine/expr/ob_expr_like.h"
#include "sql/engine/expr/ob_expr_lower.h"
#include "sql/engine/expr/ob_expr_minus.h"
#include "sql/engine/expr/ob_expr_mod.h"
#include "sql/engine/expr/ob_expr_int_div.h"
#include "sql/engine/expr/ob_expr_mul.h"
#include "sql/engine/expr/ob_expr_neg.h"
#include "sql/engine/expr/ob_expr_prior.h"
#include "sql/engine/expr/ob_expr_abs.h"
#include "sql/engine/expr/ob_expr_uuid.h"
#include "sql/engine/expr/ob_expr_not_between.h"
#include "sql/engine/expr/ob_expr_not_equal.h"
#include "sql/engine/expr/ob_expr_not.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_or.h"
#include "sql/engine/expr/ob_expr_xor.h"
#include "sql/engine/expr/ob_expr_sqrt.h"
#include "sql/engine/expr/ob_expr_log2.h"
#include "sql/engine/expr/ob_expr_log10.h"
#include "sql/engine/expr/ob_expr_regexp.h"
#include "sql/engine/expr/ob_expr_regexp_substr.h"
#include "sql/engine/expr/ob_expr_regexp_instr.h"
#include "sql/engine/expr/ob_expr_regexp_replace.h"
#include "sql/engine/expr/ob_expr_regexp_count.h"
#include "sql/engine/expr/ob_expr_regexp_like.h"
#include "sql/engine/expr/ob_expr_substr.h"
#include "sql/engine/expr/ob_expr_initcap.h"
#include "sql/engine/expr/ob_expr_mid.h"
#include "sql/engine/expr/ob_expr_substrb.h"
#include "sql/engine/expr/ob_expr_insert.h"
#include "sql/engine/expr/ob_expr_sinh.h"
#include "sql/engine/expr/ob_expr_cosh.h"
#include "sql/engine/expr/ob_expr_tanh.h"
#include "sql/engine/expr/ob_expr_trim.h"
#include "sql/engine/expr/ob_expr_inner_trim.h"
#include "sql/engine/expr/ob_expr_unhex.h"
#include "sql/engine/expr/ob_expr_upper.h"
#include "sql/engine/expr/ob_expr_user.h"
#include "sql/engine/expr/ob_expr_version.h"
#include "sql/engine/expr/ob_expr_connection_id.h"
#include "sql/engine/expr/ob_expr_sys_view_bigint_param.h"
#include "sql/engine/expr/ob_expr_date.h"
#include "sql/engine/expr/ob_expr_date_add.h"
#include "sql/engine/expr/ob_expr_date_diff.h"
#include "sql/engine/expr/ob_expr_timestamp_diff.h"
#include "sql/engine/expr/ob_expr_time_diff.h"
#include "sql/engine/expr/ob_expr_period_diff.h"
#include "sql/engine/expr/ob_expr_unix_timestamp.h"
#include "sql/engine/expr/ob_expr_maketime.h"
#include "sql/engine/expr/ob_expr_extract.h"
#include "sql/engine/expr/ob_expr_to_days.h"
#include "sql/engine/expr/ob_expr_day_of_func.h"
#include "sql/engine/expr/ob_expr_from_days.h"
#include "sql/engine/expr/ob_expr_pad.h"
#include "sql/engine/expr/ob_expr_position.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/expr/ob_expr_date_format.h"
#include "sql/engine/expr/ob_expr_str_to_date.h"
#include "sql/engine/expr/ob_expr_cur_time.h"
#include "sql/engine/expr/ob_expr_time.h"
#include "sql/engine/expr/ob_expr_time_to_usec.h"
#include "sql/engine/expr/ob_expr_usec_to_time.h"
#include "sql/engine/expr/ob_expr_func_round.h"
#include "sql/engine/expr/ob_expr_func_ceil.h"
#include "sql/engine/expr/ob_expr_func_dump.h"
#include "sql/engine/expr/ob_expr_func_sleep.h"
#include "sql/engine/expr/ob_expr_merging_frozen_time.h"
#include "sql/engine/expr/ob_expr_remainder.h"
#include "sql/engine/expr/ob_expr_repeat.h"
#include "sql/engine/expr/ob_expr_replace.h"
#include "sql/engine/expr/ob_expr_translate.h"
#include "sql/engine/expr/ob_expr_func_part_hash.h"
#include "sql/engine/expr/ob_expr_func_partition_key.h"
#include "sql/engine/expr/ob_expr_func_addr_to_part_id.h"
#include "sql/engine/expr/ob_expr_lnnvl.h"
#include "sql/engine/expr/ob_expr_autoinc_nextval.h"
#include "sql/engine/expr/ob_expr_last_insert_id.h"
#include "sql/engine/expr/ob_expr_instr.h"
#include "sql/engine/expr/ob_expr_instrb.h"
#include "sql/engine/expr/ob_expr_locate.h"
#include "sql/engine/expr/ob_expr_subquery_ref.h"
#include "sql/engine/expr/ob_expr_exists.h"
#include "sql/engine/expr/ob_expr_not_exists.h"
#include "sql/engine/expr/ob_expr_collation.h"
#include "sql/engine/expr/ob_expr_subquery_equal.h"
#include "sql/engine/expr/ob_expr_subquery_not_equal.h"
#include "sql/engine/expr/ob_expr_subquery_greater_equal.h"
#include "sql/engine/expr/ob_expr_subquery_greater_than.h"
#include "sql/engine/expr/ob_expr_subquery_less_equal.h"
#include "sql/engine/expr/ob_expr_subquery_less_than.h"
#include "sql/engine/expr/ob_expr_sys_privilege_check.h"
#include "sql/engine/expr/ob_expr_reverse.h"
#include "sql/engine/expr/ob_expr_right.h"
#include "sql/engine/expr/ob_expr_md5.h"
#include "sql/engine/expr/ob_expr_lrpad.h"
#include "sql/engine/expr/ob_expr_conv.h"
#include "sql/engine/expr/ob_expr_sign.h"
#include "sql/engine/expr/ob_expr_pow.h"
#include "sql/engine/expr/ob_expr_found_rows.h"
#include "sql/engine/expr/ob_expr_row_count.h"
#include "sql/engine/expr/ob_expr_char_length.h"
#include "sql/engine/expr/ob_expr_ifnull.h"
#include "sql/engine/expr/ob_expr_quote.h"
#include "sql/engine/expr/ob_expr_field.h"
#include "sql/engine/expr/ob_expr_timestamp_nvl.h"
#include "sql/engine/expr/ob_expr_nullif.h"
#include "sql/engine/expr/ob_expr_subquery_ns_equal.h"
#include "sql/engine/expr/ob_expr_host_ip.h"
#include "sql/engine/expr/ob_expr_rpc_port.h"
#include "sql/engine/expr/ob_expr_mysql_port.h"
#include "sql/engine/expr/ob_expr_char.h"
#include "sql/engine/expr/ob_expr_get_sys_var.h"
#include "sql/engine/expr/ob_expr_elt.h"
#include "sql/engine/expr/ob_expr_part_id.h"
#include "sql/engine/expr/ob_expr_timestamp_add.h"
#include "sql/engine/expr/ob_expr_des_hex_str.h"
#include "sql/engine/expr/ob_expr_word_segment.h"
#include "sql/engine/expr/ob_expr_ascii.h"
#include "sql/engine/expr/ob_expr_truncate.h"
#include "sql/engine/expr/ob_expr_bit_count.h"
#include "sql/engine/expr/ob_expr_make_set.h"
#include "sql/engine/expr/ob_expr_find_in_set.h"
#include "sql/engine/expr/ob_expr_estimate_ndv.h"
#include "sql/engine/expr/ob_expr_left.h"
#include "sql/engine/expr/ob_expr_space.h"
#include "sql/engine/expr/ob_expr_random.h"
#include "sql/engine/expr/ob_expr_obj_access.h"
#include "sql/engine/expr/ob_expr_rownum.h"
#include "sql/engine/expr/ob_expr_type_to_str.h"
#include "sql/engine/expr/ob_expr_connect_by_root.h"
#include "sql/engine/expr/ob_expr_sys_connect_by_path.h"
#include "sql/engine/expr/ob_expr_sys_op_opnsize.h"
#include "sql/engine/expr/ob_expr_shadow_uk_project.h"
#include "sql/engine/expr/ob_expr_interval.h"
#include "sql/engine/expr/ob_expr_week_of_func.h"
#include "sql/engine/expr/ob_expr_userenv.h"
#include "sql/engine/expr/ob_expr_sys_context.h"
#include "sql/engine/expr/ob_expr_dll_udf.h"
#include "sql/engine/expr/ob_expr_uid.h"
#include "sql/engine/expr/ob_expr_timestamp.h"
#include "sql/engine/expr/ob_expr_oracle_to_char.h"
#include "sql/engine/expr/ob_expr_to_clob.h"
#include "sql/engine/expr/ob_expr_seq_nextval.h"
#include "sql/engine/expr/ob_expr_to_blob.h"
#include "sql/engine/expr/ob_expr_to_number.h"
#include "sql/engine/expr/ob_expr_spm.h"
#include "sql/engine/expr/ob_expr_width_bucket.h"
#include "sql/engine/expr/ob_expr_lengthb.h"
#include "sql/engine/expr/ob_expr_hextoraw.h"
#include "sql/engine/expr/ob_expr_rawtohex.h"
#include "sql/engine/expr/ob_expr_utl_raw.h"
#include "sql/engine/expr/ob_expr_utl_inaddr.h"
#include "sql/engine/expr/ob_expr_dbms_lob.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_chr.h"
#include "sql/engine/expr/ob_expr_timezone.h"
#include "sql/engine/expr/ob_expr_sys_extract_utc.h"
#include "sql/engine/expr/ob_expr_tz_offset.h"
#include "sql/engine/expr/ob_expr_from_tz.h"
#include "sql/engine/expr/ob_expr_to_interval.h"
#include "sql/engine/expr/ob_expr_nvl2_oracle.h"
#include "sql/engine/expr/ob_expr_vsize.h"
#include "sql/engine/expr/ob_expr_orahash.h"
#include "sql/engine/expr/ob_expr_power.h"
#include "sql/engine/expr/ob_expr_exp.h"
#include "sql/engine/expr/ob_expr_ln.h"
#include "sql/engine/expr/ob_expr_log.h"
#include "sql/engine/expr/ob_expr_oracle_nullif.h"
#include "sql/engine/expr/ob_expr_bool.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/engine/expr/ob_expr_part_id_pseudo_column.h"
#include "sql/engine/expr/ob_expr_to_single_byte.h"
#include "sql/engine/expr/ob_expr_to_multi_byte.h"
#include "sql/engine/expr/ob_expr_stmt_id.h"
#include "sql/engine/expr/ob_expr_obversion.h"
#include "sql/engine/expr/ob_expr_utl_i18n.h"
#include "sql/engine/expr/ob_expr_remove_const.h"
#include "sql/engine/expr/ob_expr_calc_urowid.h"
#include "sql/engine/expr/ob_expr_cardinality.h"
#include "sql/engine/expr/ob_expr_coll_pred.h"
#include "sql/engine/expr/ob_expr_user_can_access_obj.h"
#include "sql/engine/expr/ob_expr_empty_lob.h"
#include "sql/engine/expr/ob_expr_radians.h"
#include "sql/engine/expr/ob_expr_to_outfile_row.h"
#include "sql/engine/expr/ob_expr_format.h"
#include "sql/engine/expr/ob_expr_quarter.h"
#include "sql/engine/expr/ob_expr_bit_length.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace sql {
static AllocFunc OP_ALLOC[T_MAX_OP];
static AllocFunc OP_ALLOC_ORCL[T_MAX_OP];

#define REG_OP(OpClass)                                                \
  do {                                                                 \
    OpClass op(alloc);                                                 \
    if (OB_UNLIKELY(i >= EXPR_OP_NUM)) {                               \
      LOG_ERROR("out of the max expr");                                \
    } else {                                                           \
      NAME_TYPES[i].name_ = op.get_name();                             \
      NAME_TYPES[i].type_ = op.get_type();                             \
      OP_ALLOC[op.get_type()] = ObExprOperatorFactory::alloc<OpClass>; \
      i++;                                                             \
    }                                                                  \
  } while (0)

// You can use this macro when you want to develop two expressions
// with exactly the same function (for example, two expressions mid and substr)
// OriOp is an existing expression.
// Now I want to develop NewOp and the functions of the two are exactly the same.
// Use this macro to avoid duplication of code
// But OriOp is required to be registered first
#define REG_SAME_OP(OriOpType, NewOpType, NewOpName, idx_mysql)             \
  do {                                                                      \
    if (OB_UNLIKELY((idx_mysql) >= EXPR_OP_NUM)) {                          \
      LOG_ERROR("out of the max expr");                                     \
    } else if (OB_ISNULL(OP_ALLOC[OriOpType])) {                            \
      LOG_ERROR("OriOp is not registered yet", K(OriOpType), K(NewOpType)); \
    } else {                                                                \
      NAME_TYPES[(idx_mysql)].name_ = NewOpName;                            \
      NAME_TYPES[(idx_mysql)].type_ = NewOpType;                            \
      OP_ALLOC[NewOpType] = OP_ALLOC[OriOpType];                            \
      (idx_mysql)++;                                                        \
    }                                                                       \
  } while (0)

#define REG_OP_ORCL(OpClass)                                                \
  do {                                                                      \
    OpClass op(alloc);                                                      \
    if (OB_UNLIKELY(j >= EXPR_OP_NUM)) {                                    \
      LOG_ERROR("out of the max expr");                                     \
    } else {                                                                \
      NAME_TYPES_ORCL[j].name_ = op.get_name();                             \
      NAME_TYPES_ORCL[j].type_ = op.get_type();                             \
      OP_ALLOC_ORCL[op.get_type()] = ObExprOperatorFactory::alloc<OpClass>; \
      j++;                                                                  \
    }                                                                       \
  } while (0)

// Expressions used to register the same function in Oracle mode
#define REG_SAME_OP_ORCL(OriOpType, NewOpType, NewOpName, idx_oracle)       \
  do {                                                                      \
    if (OB_UNLIKELY((idx_oracle) >= EXPR_OP_NUM)) {                         \
      LOG_ERROR("out of the max expr");                                     \
    } else if (OB_ISNULL(OP_ALLOC_ORCL[OriOpType])) {                       \
      LOG_ERROR("OriOp is not registered yet", K(OriOpType), K(NewOpType)); \
    } else {                                                                \
      NAME_TYPES_ORCL[(idx_oracle)].name_ = NewOpName;                      \
      NAME_TYPES_ORCL[(idx_oracle)].type_ = NewOpType;                      \
      OP_ALLOC_ORCL[NewOpType] = OP_ALLOC_ORCL[OriOpType];                  \
      (idx_oracle)++;                                                       \
    }                                                                       \
  } while (0)

ObExprOperatorFactory::NameType ObExprOperatorFactory::NAME_TYPES[EXPR_OP_NUM] = {};
ObExprOperatorFactory::NameType ObExprOperatorFactory::NAME_TYPES_ORCL[EXPR_OP_NUM] = {};

char* ObExprOperatorFactory::str_toupper(char* buff)
{
  if (OB_LIKELY(NULL != buff)) {
    char* ptr = buff;
    unsigned char ch = *ptr;
    while ('\0' != *ptr) {
      ch = *ptr;
      if (ch >= 'a' && ch <= 'z') {
        ch = (unsigned char)(ch - ('a' - 'A'));
      } else if (ch >= 0x80 && islower(ch)) {
        ch = (unsigned char)toupper(ch);
      }
      *ptr = ch;
      ptr++;
    }
  }
  return buff;
}

ObExprOperatorType ObExprOperatorFactory::get_type_by_name(const ObString& name)
{
  ObExprOperatorType type = T_INVALID;
  if (share::is_oracle_mode()) {
    char name_buf[OB_MAX_FUNC_EXPR_LENGTH];
    ObString func_name(N_ORA_DECODE);
    if (name.case_compare("decode") != 0) {
      func_name.assign_ptr(name.ptr(), name.length());
    }
    for (uint32_t i = 0; i < ARRAYSIZEOF(NAME_TYPES_ORCL); i++) {
      if (NAME_TYPES_ORCL[i].type_ <= T_MIN_OP || NAME_TYPES_ORCL[i].type_ >= T_MAX_OP) {
        break;
      }
      // Convert the original name of op to uppercase for matching
      MEMSET(name_buf, 0, OB_MAX_FUNC_EXPR_LENGTH);
      strcpy(name_buf, NAME_TYPES_ORCL[i].name_);
      str_toupper(name_buf);
      // Temporarily insensitive matching, update to strncmp after the case mechanism is added
      if (static_cast<int32_t>(strlen(NAME_TYPES_ORCL[i].name_)) == func_name.length() &&
          strncasecmp(name_buf, func_name.ptr(), func_name.length()) == 0) {
        type = NAME_TYPES_ORCL[i].type_;
        break;
      }
    }
  } else {
    for (uint32_t i = 0; i < ARRAYSIZEOF(NAME_TYPES); i++) {
      if (NAME_TYPES[i].type_ <= T_MIN_OP || NAME_TYPES[i].type_ >= T_MAX_OP) {
        break;
      }
      if (static_cast<int32_t>(strlen(NAME_TYPES[i].name_)) == name.length() &&
          strncasecmp(NAME_TYPES[i].name_, name.ptr(), name.length()) == 0) {
        type = NAME_TYPES[i].type_;
        break;
      }
    }
  }
  return type;
}

void ObExprOperatorFactory::register_expr_operators()
{
  memset(NAME_TYPES, 0, sizeof(NAME_TYPES));
  memset(NAME_TYPES_ORCL, 0, sizeof(NAME_TYPES_ORCL));
  ObArenaAllocator alloc;
  int64_t i = 0;
  int64_t j = 0;
  /*
  --REG_OP is used for mysql tenant registration,
    REG_OP_ORCL is used for oracle tenant system function registration
  --If the same function is to be used both under mysql tenant and oracle,
    and compatibility has been achieved
  --Please use REG_OP() and REG_OP_ORCL() to register separately. for the format,
    please register in the oracle system function centralized area at the end of the function
  */
  REG_OP(ObExprAdd);
  REG_OP(ObExprAggAdd);
  REG_OP(ObExprAnd);
  REG_OP(ObExprArgCase);
  REG_OP(ObExprAssign);
  REG_OP(ObExprBetween);
  REG_OP(ObExprBitAnd);
  REG_OP(ObExprCase);
  REG_OP(ObExprCast);
  REG_OP(ObExprTimeStampAdd);
  REG_OP(ObExprToType);
  REG_OP(ObExprChar);
  REG_OP(ObExprConvert);
  REG_OP(ObExprCoalesce);
  REG_OP(ObExprNvl);
  REG_OP(ObExprConcat);
  REG_OP(ObExprCurrentUser);
  REG_OP(ObExprYear);
  REG_OP(ObExprOracleDecode);
  REG_OP(ObExprOracleTrunc);
  REG_OP(ObExprDiv);
  REG_OP(ObExprAggDiv);
  REG_OP(ObExprEffectiveTenant);
  REG_OP(ObExprEffectiveTenantId);
  REG_OP(ObExprEqual);
  REG_OP(ObExprNullSafeEqual);
  REG_OP(ObExprGetUserVar);
  REG_OP(ObExprGreaterEqual);
  REG_OP(ObExprGreaterThan);
  REG_OP(ObExprGreatestMySQL);
  REG_OP(ObExprGreatestMySQLInner);
  REG_OP(ObExprHex);
  REG_OP(ObExprIn);
  REG_OP(ObExprNotIn);
  REG_OP(ObExprInt2ip);
  REG_OP(ObExprIp2int);
  REG_OP(ObExprInsert);
  REG_OP(ObExprIs);
  REG_OP(ObExprIsNot);
  REG_OP(ObExprLeastMySQL);
  REG_OP(ObExprLeastMySQLInner);
  REG_OP(ObExprLength);
  REG_OP(ObExprLessEqual);
  REG_OP(ObExprLessThan);
  REG_OP(ObExprLike);
  REG_OP(ObExprLower);
  REG_OP(ObExprMinus);
  REG_OP(ObExprAggMinus);
  REG_OP(ObExprMod);
  REG_OP(ObExprMd5);
  REG_OP(ObExprTime);
  REG_OP(ObExprHour);
  REG_OP(ObExprRpad);
  REG_OP(ObExprLpad);
  REG_OP(ObExprColumnConv);
  REG_OP(ObExprFunValues);
  REG_OP(ObExprFunDefault);
  REG_OP(ObExprIntDiv);
  REG_OP(ObExprMul);
  REG_OP(ObExprAggMul);
  REG_OP(ObExprAbs);
  REG_OP(ObExprUuid);
  REG_OP(ObExprNeg);
  REG_OP(ObExprPrior);
  REG_OP(ObExprFromUnixTime);
  REG_OP(ObExprNotBetween);
  REG_OP(ObExprNotEqual);
  REG_OP(ObExprNot);
  REG_OP(ObExprOr);
  REG_OP(ObExprXor);
  REG_OP(ObExprRegexp);
  REG_OP(ObExprRegexpSubstr);
  REG_OP(ObExprRegexpInstr);
  REG_OP(ObExprRegexpReplace);
  REG_OP(ObExprRegexpLike);
  REG_OP(ObExprSleep);
  REG_OP(ObExprStrcmp);
  REG_OP(ObExprSubstr);
  REG_OP(ObExprSubstringIndex);
  REG_OP(ObExprMid);
  REG_OP(ObExprSysViewBigintParam);
  REG_OP(ObExprInnerTrim);
  REG_OP(ObExprTrim);
  REG_OP(ObExprLtrim);
  REG_OP(ObExprSpace);
  REG_OP(ObExprRtrim);
  REG_OP(ObExprUnhex);
  REG_OP(ObExprUpper);
  REG_OP(ObExprConv);
  REG_OP(ObExprUser);
  REG_OP(ObExprDate);
  REG_OP(ObExprMonth);
  REG_OP(ObExprMonthName);
  REG_OP(ObExprDateAdd);
  REG_OP(ObExprDateSub);
  REG_OP(ObExprSubtime);
  REG_OP(ObExprDateDiff);
  REG_OP(ObExprTimeStampDiff);
  REG_OP(ObExprTimeDiff);
  REG_OP(ObExprPeriodDiff);
  REG_OP(ObExprUnixTimestamp);
  REG_OP(ObExprMakeTime);
  REG_OP(ObExprExtract);
  REG_OP(ObExprToDays);
  REG_OP(ObExprPosition);
  REG_OP(ObExprFromDays);
  REG_OP(ObExprDateFormat);
  REG_OP(ObExprStrToDate);
  REG_OP(ObExprCurDate);
  REG_OP(ObExprCurTime);
  REG_OP(ObExprSysdate);
  REG_OP(ObExprCurTimestamp);
  REG_OP(ObExprUtcTimestamp);
  REG_OP(ObExprTimeToUsec);
  REG_OP(ObExprUsecToTime);
  REG_OP(ObExprMergingFrozenTime);
  REG_OP(ObExprFuncRound);
  REG_OP(ObExprFuncFloor);
  REG_OP(ObExprFuncCeil);
  REG_OP(ObExprFuncCeiling);
  REG_OP(ObExprFuncDump);
  REG_OP(ObExprRepeat);
  REG_OP(ObExprReplace);
  REG_OP(ObExprFuncPartOldHash);
  REG_OP(ObExprFuncPartHash);
  REG_OP(ObExprFuncPartOldKey);
  REG_OP(ObExprFuncPartKey);
  REG_OP(ObExprFuncPartNewKey);
  REG_OP(ObExprFuncAddrToPartId);
  REG_OP(ObExprDatabase);
  REG_OP(ObExprAutoincNextval);
  REG_OP(ObExprLastInsertID);
  REG_OP(ObExprInstr);
  REG_OP(ObExprFuncLnnvl);
  REG_OP(ObExprLocate);
  REG_OP(ObExprVersion);
  REG_OP(ObExprObVersion);
  REG_OP(ObExprConnectionId);
  REG_OP(ObExprCharset);
  REG_OP(ObExprCollation);
  REG_OP(ObExprCoercibility);
  REG_OP(ObExprSetCollation);
  REG_OP(ObExprReverse);
  REG_OP(ObExprRight);
  REG_OP(ObExprSign);
  REG_OP(ObExprBitXor);
  REG_OP(ObExprSqrt);
  REG_OP(ObExprLog2);
  REG_OP(ObExprLog10);
  REG_OP(ObExprPow);
  REG_OP(ObExprRowCount);
  REG_OP(ObExprFoundRows);
  REG_OP(ObExprAggParamList);
  REG_OP(ObExprIsServingTenant);
  REG_OP(ObExprSysPrivilegeCheck);
  REG_OP(ObExprField);
  REG_OP(ObExprElt);
  REG_OP(ObExprNullif);
  REG_OP(ObExprTimestampNvl);
  REG_OP(ObExprDesHexStr);
  REG_OP(ObExprAscii);
  REG_OP(ObExprOrd);
  REG_OP(ObExprBitCount);
  REG_OP(ObExprFindInSet);
  REG_OP(ObExprLeft);
  REG_OP(ObExprRandom);
  REG_OP(ObExprMakeSet);
  REG_OP(ObExprEstimateNdv);
  REG_OP(ObExprSysOpOpnsize);
  REG_OP(ObExprDayOfMonth);
  REG_OP(ObExprDayOfWeek);
  REG_OP(ObExprDayOfYear);
  REG_OP(ObExprSecond);
  REG_OP(ObExprMinute);
  REG_OP(ObExprMicrosecond);
  REG_OP(ObExprToSeconds);
  REG_OP(ObExprTimeToSec);
  REG_OP(ObExprSecToTime);
  REG_OP(ObExprInterval);
  REG_OP(ObExprTruncate);
  REG_OP(ObExprDllUdf);
  REG_OP(ObExprExp);
  /* subquery comparison experator */
  REG_OP(ObExprSubQueryRef);
  REG_OP(ObExprSubQueryEqual);
  REG_OP(ObExprSubQueryNotEqual);
  REG_OP(ObExprSubQueryNSEqual);
  REG_OP(ObExprSubQueryGreaterEqual);
  REG_OP(ObExprSubQueryGreaterThan);
  REG_OP(ObExprSubQueryLessEqual);
  REG_OP(ObExprSubQueryLessThan);
  REG_OP(ObExprRemoveConst);
  REG_OP(ObExprExists);
  REG_OP(ObExprNotExists);
  REG_OP(ObExprCharLength);
  REG_OP(ObExprBitAnd);
  REG_OP(ObExprBitOr);
  REG_OP(ObExprBitNeg);
  REG_OP(ObExprBitLeftShift);
  REG_OP(ObExprBitLength);
  REG_OP(ObExprBitRightShift);
  REG_OP(ObExprIfNull);
  REG_OP(ObExprConcatWs);
  REG_OP(ObExprCmpMeta);
  REG_OP(ObExprQuote);
  REG_OP(ObExprPad);
  REG_OP(ObExprHostIP);
  REG_OP(ObExprRpcPort);
  REG_OP(ObExprMySQLPort);
  REG_OP(ObExprGetSysVar);
  REG_OP(ObExprPartId);
  REG_OP(ObExprLastTraceId);
  REG_OP(ObExprLastExecId);
  REG_OP(ObExprWordSegment);
  REG_OP(ObExprObjAccess);
  REG_OP(ObExprEnumToStr);
  REG_OP(ObExprSetToStr);
  REG_OP(ObExprEnumToInnerType);
  REG_OP(ObExprSetToInnerType);
  REG_OP(ObExprConnectByRoot);
  REG_OP(ObExprShadowUKProject);
  REG_OP(ObExprWeekOfYear);
  REG_OP(ObExprWeekDay);
  REG_OP(ObExprYearWeek);
  REG_OP(ObExprWeek);
  REG_OP(ObExprQuarter);
  REG_OP(ObExprSeqNextval);
  REG_OP(ObExprSpmLoadPlans);
  REG_OP(ObExprSpmAlterBaseline);
  REG_OP(ObExprSpmDropBaseline);
  REG_OP(ObExprBool);
  REG_OP(ObExprSin);
  REG_OP(ObExprTan);
  REG_OP(ObExprCalcPartitionId);
  REG_OP(ObExprPartIdPseudoColumn);
  REG_OP(ObExprStmtId);
  REG_OP(ObExprRadians);
  REG_OP(ObExprAtan);
  REG_OP(ObExprAtan2);
  REG_OP(ObExprToOutfileRow);
  REG_OP(ObExprFormat);
  // register oracle system function
  REG_OP_ORCL(ObExprSysConnectByPath);
  REG_OP_ORCL(ObExprTimestampNvl);
  REG_OP_ORCL(ObExprOracleToDate);
  REG_OP_ORCL(ObExprOracleToChar);
  REG_OP_ORCL(ObExprToClob);
  REG_OP_ORCL(ObExprToBlob);
  REG_OP_ORCL(ObExprToTimestamp);
  REG_OP_ORCL(ObExprToTimestampTZ);
  REG_OP_ORCL(ObExprSysdate);
  REG_OP_ORCL(ObExprFuncPartHash);
  REG_OP_ORCL(ObExprFuncPartOldHash);
  REG_OP_ORCL(ObExprOracleDecode);
  REG_OP_ORCL(ObExprUid);
  REG_OP_ORCL(ObExprReplace);
  REG_OP_ORCL(ObExprTranslate);
  REG_OP_ORCL(ObExprLength);
  REG_OP_ORCL(ObExprLengthb);
  REG_OP_ORCL(ObExprEffectiveTenantId);  // this id will be removed in oracle tenant in the future
  REG_OP_ORCL(ObExprRowNum);
  REG_OP_ORCL(ObExprTrunc);
  REG_OP_ORCL(ObExprUser);
  REG_OP_ORCL(ObExprCast);
  REG_OP_ORCL(ObExprMod);
  REG_OP_ORCL(ObExprRemainder);
  REG_OP_ORCL(ObExprAbs);
  REG_OP_ORCL(ObExprConcat);
  REG_OP_ORCL(ObExprToNumber);
  REG_OP_ORCL(ObExprOracleRpad);
  REG_OP_ORCL(ObExprOracleLpad);
  REG_OP_ORCL(ObExprOracleInstr);
  REG_OP_ORCL(ObExprInstrb);
  REG_OP_ORCL(ObExprSubstr);
  REG_OP_ORCL(ObExprInitcap);
  REG_OP_ORCL(ObExprSubstrb);
  REG_OP_ORCL(ObExprFuncRound);
  REG_OP_ORCL(ObExprCoalesce);
  REG_OP_ORCL(ObExprUserEnv);
  REG_OP_ORCL(ObExprSysContext);
  REG_OP_ORCL(ObExprLower);
  REG_OP_ORCL(ObExprUpper);
  REG_OP_ORCL(ObExprInnerTrim);
  REG_OP_ORCL(ObExprTrim);
  REG_OP_ORCL(ObExprSinh);
  REG_OP_ORCL(ObExprCosh);
  REG_OP_ORCL(ObExprTanh);
  REG_OP_ORCL(ObExprLtrim);
  REG_OP_ORCL(ObExprRtrim);
  REG_OP_ORCL(ObExprOracleNvl);
  REG_OP_ORCL(ObExprFuncCeil);
  REG_OP_ORCL(ObExprFuncFloor);
  REG_OP_ORCL(ObExprAsin);
  REG_OP_ORCL(ObExprAcos);
  REG_OP_ORCL(ObExprAtan);
  REG_OP_ORCL(ObExprAtan2);
  REG_OP_ORCL(ObExprSign);
  REG_OP_ORCL(ObExprSysTimestamp);
  REG_OP_ORCL(ObExprLocalTimestamp);
  REG_OP_ORCL(ObExprSetCollation);
  REG_OP_ORCL(ObExprWidthBucket);
  REG_OP_ORCL(ObExprChr);
  REG_OP_ORCL(ObExprExtract);
  REG_OP_ORCL(ObExprSqrt);
  REG_OP_ORCL(ObExprNlsLower);
  REG_OP_ORCL(ObExprNlsUpper);
  // some internally used expressions
  REG_OP_ORCL(ObExprAdd);
  REG_OP_ORCL(ObExprAggAdd);
  REG_OP_ORCL(ObExprEqual);
  REG_OP_ORCL(ObExprLessEqual);
  REG_OP_ORCL(ObExprNeg);
  REG_OP_ORCL(ObExprLessThan);
  REG_OP_ORCL(ObExprGreaterThan);
  REG_OP_ORCL(ObExprNullSafeEqual);
  REG_OP_ORCL(ObExprGreaterEqual);
  REG_OP_ORCL(ObExprSubQueryRef);
  REG_OP_ORCL(ObExprSubQueryEqual);
  REG_OP_ORCL(ObExprSubQueryNotEqual);
  REG_OP_ORCL(ObExprSubQueryNSEqual);
  REG_OP_ORCL(ObExprSubQueryGreaterEqual);
  REG_OP_ORCL(ObExprSubQueryGreaterThan);
  REG_OP_ORCL(ObExprSubQueryLessEqual);
  REG_OP_ORCL(ObExprSubQueryLessThan);
  REG_OP_ORCL(ObExprRemoveConst);
  REG_OP_ORCL(ObExprIs);
  REG_OP_ORCL(ObExprIsNot);
  REG_OP_ORCL(ObExprBetween);
  REG_OP_ORCL(ObExprNotBetween);
  REG_OP_ORCL(ObExprLike);
  REG_OP_ORCL(ObExprRegexpSubstr);
  REG_OP_ORCL(ObExprRegexpInstr);
  REG_OP_ORCL(ObExprRegexpReplace);
  REG_OP_ORCL(ObExprRegexpCount);
  REG_OP_ORCL(ObExprRegexpLike);
  REG_OP_ORCL(ObExprNot);
  REG_OP_ORCL(ObExprAnd);
  REG_OP_ORCL(ObExprOr);
  REG_OP_ORCL(ObExprIn);
  REG_OP_ORCL(ObExprNotIn);
  REG_OP_ORCL(ObExprArgCase);
  REG_OP_ORCL(ObExprCase);
  REG_OP_ORCL(ObExprQuote);
  REG_OP_ORCL(ObExprConv);
  REG_OP_ORCL(ObExprAssign);
  REG_OP_ORCL(ObExprGetUserVar);
  REG_OP_ORCL(ObExprGetSysVar);
  REG_OP_ORCL(ObExprBitOr);
  REG_OP_ORCL(ObExprBitXor);
  REG_OP_ORCL(ObExprBitAnd);
  REG_OP_ORCL(ObExprBitOr);
  REG_OP_ORCL(ObExprBitNeg);
  REG_OP_ORCL(ObExprBitLeftShift);
  REG_OP_ORCL(ObExprBitRightShift);
  REG_OP_ORCL(ObExprAggParamList);
  REG_OP_ORCL(ObExprPrior);
  REG_OP_ORCL(ObExprObjAccess);
  REG_OP_ORCL(ObExprConnectByRoot);
  REG_OP_ORCL(ObExprShadowUKProject);
  REG_OP_ORCL(ObExprXor);
  REG_OP_ORCL(ObExprAutoincNextval);
  REG_OP_ORCL(ObExprColumnConv);
  REG_OP_ORCL(ObExprFunValues);
  REG_OP_ORCL(ObExprPartId);
  REG_OP_ORCL(ObExprSeqNextval);
  REG_OP_ORCL(ObExprToType);
  REG_OP_ORCL(ObExprNotEqual);
  REG_OP_ORCL(ObExprMinus);
  REG_OP_ORCL(ObExprAggMinus);
  REG_OP_ORCL(ObExprPosition);
  REG_OP_ORCL(ObExprMul);
  REG_OP_ORCL(ObExprAggMul);
  REG_OP_ORCL(ObExprDiv);
  REG_OP_ORCL(ObExprAggDiv);
  REG_OP_ORCL(ObExprFuncLnnvl);
  REG_OP_ORCL(ObExprCurDate);
  REG_OP_ORCL(ObExprPad);
  REG_OP_ORCL(ObExprExists);
  REG_OP_ORCL(ObExprNotExists);
  REG_OP_ORCL(ObExprCurTimestamp);
  REG_OP_ORCL(ObExprFunDefault);
  REG_OP_ORCL(ObExprAscii);
  REG_OP_ORCL(ObExprNvl2Oracle);
  REG_OP_ORCL(ObExprToBinaryFloat);
  REG_OP_ORCL(ObExprToBinaryDouble);
  REG_OP_ORCL(ObExprOracleNullif);
  REG_OP_ORCL(ObExprStmtId);

  // for SPM
  REG_OP_ORCL(ObExprSpmLoadPlans);
  REG_OP_ORCL(ObExprSpmAlterBaseline);
  REG_OP_ORCL(ObExprSpmDropBaseline);
  // for SPM end
  REG_OP_ORCL(ObExprOracleLeast);
  REG_OP_ORCL(ObExprOracleGreatest);
  REG_OP_ORCL(ObExprHostIP);
  REG_OP_ORCL(ObExprRpcPort);
  REG_OP_ORCL(ObExprIsServingTenant);
  REG_OP_ORCL(ObExprBitAndOra);
  REG_OP_ORCL(ObExprHextoraw);
  REG_OP_ORCL(ObExprRawtohex);
  REG_OP_ORCL(ObExprUtlRawCastToRaw);
  REG_OP_ORCL(ObExprUtlRawCastToVarchar2);
  REG_OP_ORCL(ObExprUtlRawLength);
  REG_OP_ORCL(ObExprUtlRawBitAnd);
  REG_OP_ORCL(ObExprUtlRawBitOr);
  REG_OP_ORCL(ObExprUtlRawBitXor);
  REG_OP_ORCL(ObExprUtlRawBitComplement);
  REG_OP_ORCL(ObExprUtlRawReverse);
  REG_OP_ORCL(ObExprUtlRawCopies);
  REG_OP_ORCL(ObExprUtlRawCompare);
  REG_OP_ORCL(ObExprUtlRawSubstr);
  REG_OP_ORCL(ObExprUtlRawConcat);
  REG_OP_ORCL(ObExprDateAdd);
  REG_OP_ORCL(ObExprDateSub);
  REG_OP_ORCL(ObExprFunDefault);
  REG_OP_ORCL(ObExprSessiontimezone);
  REG_OP_ORCL(ObExprDbtimezone);
  REG_OP_ORCL(ObExprSysExtractUtc);
  REG_OP_ORCL(ObExprTzOffset);
  REG_OP_ORCL(ObExprFromTz);
  REG_OP_ORCL(ObExprDbmsLobGetLength);
  REG_OP_ORCL(ObExprDbmsLobAppend);
  REG_OP_ORCL(ObExprDbmsLobRead);
  REG_OP_ORCL(ObExprDbmsLobConvertToBlob);
  REG_OP_ORCL(ObExprDbmsLobCastClobToBlob);
  REG_OP_ORCL(ObExprDbmsLobConvertClobCharset);
  REG_OP_ORCL(ObExprAddMonths);
  REG_OP_ORCL(ObExprLastDay);
  REG_OP_ORCL(ObExprNextDay);
  REG_OP_ORCL(ObExprMonthsBetween);
  REG_OP_ORCL(ObExprToYMInterval);
  REG_OP_ORCL(ObExprToDSInterval);
  REG_OP_ORCL(ObExprNumToYMInterval);
  REG_OP_ORCL(ObExprNumToDSInterval);
  REG_OP_ORCL(ObExprSin);
  REG_OP_ORCL(ObExprCos);
  REG_OP_ORCL(ObExprTan);
  REG_OP_ORCL(ObExprVsize);
  REG_OP_ORCL(ObExprOrahash);
  REG_OP_ORCL(ObExprPower);
  REG_OP_ORCL(ObExprExp);
  REG_OP_ORCL(ObExprLn);
  REG_OP_ORCL(ObExprLog);

  REG_SAME_OP_ORCL(T_FUN_SYS_LENGTH, T_FUN_SYS_LENGTHC, "lengthc", j);
  REG_SAME_OP_ORCL(T_FUN_SYS_SUBSTR, T_FUN_SYS_SUBSTRC, "substrc", j);
  REG_SAME_OP_ORCL(T_FUN_SYS_INSTR, T_FUN_SYS_INSTRC, "instrc", j);
  REG_OP_ORCL(ObExprFuncDump);
  REG_OP_ORCL(ObExprCalcPartitionId);
  // SYS_GUID
  REG_OP_ORCL(ObExprSysGuid);
  REG_OP_ORCL(ObExprPartIdPseudoColumn);
  REG_OP_ORCL(ObExprToSingleByte);
  REG_OP_ORCL(ObExprToMultiByte);
  REG_OP_ORCL(ObExprUtlI18nStringToRaw);
  REG_OP_ORCL(ObExprUtlI18nRawToChar);
  REG_OP_ORCL(ObExprUtlInaddrGetHostAddr);
  REG_OP_ORCL(ObExprUtlInaddrGetHostName);
  REG_OP_ORCL(ObExprOracleToNChar);
  // URowID
  REG_OP_ORCL(ObExprCalcURowID);
  REG_OP_ORCL(ObExprCardinality);
  REG_OP_ORCL(ObExprCollPred);
  // Priv
  REG_OP_ORCL(ObExprUserCanAccessObj);
  REG_OP_ORCL(ObExprEmptyClob);
  REG_OP_ORCL(ObExprEmptyBlob);
  REG_OP_ORCL(ObExprToOutfileRow);
  REG_OP_ORCL(ObExprCharset);
  REG_OP_ORCL(ObExprCollation);
  REG_OP_ORCL(ObExprCoercibility);
}

bool ObExprOperatorFactory::is_expr_op_type_valid(ObExprOperatorType type)
{
  bool bret = false;
  if (type > T_REF_COLUMN && type < T_MAX_OP) {
    bret = true;
  }
  return bret;
}

int ObExprOperatorFactory::alloc(ObExprOperatorType type, ObExprOperator*& expr_op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_expr_op_type_valid(type))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(type));
  } else if (OB_ISNULL(share::is_oracle_mode() ? OP_ALLOC_ORCL[type] : OP_ALLOC[type])) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpectd expr item type", K(ret), K(type));
  } else if (OB_FAIL(
                 share::is_oracle_mode() ? OP_ALLOC_ORCL[type](alloc_, expr_op) : OP_ALLOC[type](alloc_, expr_op))) {
    OB_LOG(WARN, "fail to alloc expr_op", K(ret), K(type));
  } else if (OB_ISNULL(expr_op)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(ERROR, "fail to alloc expr_op", K(ret), K(type));
  } else if (NULL != next_expr_id_) {
    expr_op->set_id((*next_expr_id_)++);
  }
  return ret;
}

int ObExprOperatorFactory::alloc_fast_expr(ObExprOperatorType type, ObFastExprOperator*& fast_op)
{
#define BEGIN_ALLOC switch (type) {
#define END_ALLOC                               \
  default:                                      \
    ret = OB_ERR_UNEXPECTED;                    \
    LOG_WARN("invalid operator type", K(type)); \
    break;                                      \
    }
#define ALLOC_FAST_EXPR(op_type, OpClass)               \
  case op_type: {                                       \
    void* ptr = alloc_.alloc(sizeof(OpClass));          \
    if (OB_ISNULL(ptr)) {                               \
      ret = OB_ALLOCATE_MEMORY_FAILED;                  \
      LOG_WARN("allocate operator failed", K(op_type)); \
    } else {                                            \
      fast_op = new (ptr) OpClass(alloc_);              \
    }                                                   \
    break;                                              \
  }

  int ret = OB_SUCCESS;
  BEGIN_ALLOC
  ALLOC_FAST_EXPR(T_FUN_COLUMN_CONV, ObFastColumnConvExpr);
  END_ALLOC
  return ret;
#undef BEGIN_ALLOC
#undef END_ALLOC
#undef ALLOC_FAST_EXPR
}

// int ObExprOperatorFactory::free(ObExprOperator *&expr_op)
//{
//  int ret = OB_SUCCESS;
//  if (OB_ISNULL(expr_op)) {
//    ret = OB_INVALID_ARGUMENT;
//    OB_LOG(WARN, "invalid argument", K(ret), K(expr_op));
//  } else {
//    expr_op->~ObExprOperator();
//    alloc_.free(expr_op);
//    expr_op = NULL;
//  }
//  return ret;
//}

template <typename ClassT>
int ObExprOperatorFactory::alloc(common::ObIAllocator& alloc, ObExprOperator*& expr_op)
{
  int ret = common::OB_SUCCESS;
  void* buf = NULL;
  if (OB_ISNULL(buf = alloc.alloc(sizeof(ClassT)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(ERROR, "fail to alloc expr_operator", K(ret));
  } else {
    expr_op = new (buf) ClassT(alloc);
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
