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

#include "ob_expr_eval_functions.h"
#include "sql/engine/ob_serializable_function.h"
#include "ob_expr_add.h"
#include "ob_expr_bit_count.h"
#include "ob_expr_bit_neg.h"
#include "ob_expr_column_conv.h"
#include "ob_expr_concat.h"
#include "ob_expr_connection_id.h"
#include "ob_expr_regexp_count.h"
#include "ob_expr_conv.h"
#include "ob_expr_current_user.h"
#include "ob_expr_cur_time.h"
#include "ob_expr_database.h"
#include "ob_expr_date.h"
#include "ob_expr_date_diff.h"
#include "ob_expr_day_of_func.h"
#include "ob_expr_div.h"
#include "ob_expr_effective_tenant.h"
#include "ob_expr_effective_tenant_id.h"
#include "ob_expr_exists.h"
#include "ob_expr_extract.h"
#include "ob_expr_found_rows.h"
#include "ob_expr_from_unix_time.h"
#include "ob_expr_func_partition_key.h"
#include "ob_expr_greatest.h"
#include "ob_expr_host_ip.h"
#include "ob_expr_trim.h"
#include "ob_expr_insert.h"
#include "ob_expr_int2ip.h"
#include "ob_expr_int_div.h"
#include "ob_expr_ip2int.h"
#include "ob_expr_is.h"
#include "ob_expr_last_exec_id.h"
#include "ob_expr_last_trace_id.h"
#include "ob_expr_least.h"
#include "ob_expr_length.h"
#include "ob_expr_like.h"
#include "ob_expr_lower.h"
#include "ob_expr_md5.h"
#include "ob_expr_mid.h"
#include "ob_expr_minus.h"
#include "ob_expr_mod.h"
#include "ob_expr_mul.h"
#include "ob_expr_mysql_port.h"
#include "ob_expr_neg.h"
#include "ob_expr_not.h"
#include "ob_expr_not_exists.h"
#include "ob_expr_null_safe_equal.h"
#include "ob_expr_nvl2_oracle.h"
#include "ob_expr_nvl.h"
#include "ob_expr_operator.h"
#include "ob_expr_pow.h"
#include "ob_expr_regexp_count.h"
#include "ob_expr_regexp.h"
#include "ob_expr_regexp_instr.h"
#include "ob_expr_regexp_like.h"
#include "ob_expr_regexp_replace.h"
#include "ob_expr_regexp_substr.h"
#include "ob_expr_repeat.h"
#include "ob_expr_replace.h"
#include "ob_expr_func_dump.h"
#include "ob_expr_func_part_hash.h"
#include "ob_expr_func_addr_to_part_id.h"
#include "ob_expr_autoinc_nextval.h"
#include "ob_expr_is_serving_tenant.h"
#include "ob_expr_sys_privilege_check.h"
#include "ob_expr_field.h"
#include "ob_expr_elt.h"
#include "ob_expr_des_hex_str.h"
#include "ob_expr_lnnvl.h"
#include "ob_expr_row_count.h"
#include "ob_expr_rownum.h"
#include "ob_expr_rpc_port.h"
#include "ob_expr_space.h"
#include "ob_expr_subquery_ref.h"
#include "ob_expr_substr.h"
#include "ob_expr_substring_index.h"
#include "ob_expr_time.h"
#include "ob_expr_timestamp.h"
#include "ob_expr_timezone.h"
#include "ob_expr_trim.h"
#include "ob_expr_uid.h"
#include "ob_expr_unhex.h"
#include "ob_expr_user.h"
#include "ob_expr_uuid.h"
#include "ob_expr_version.h"
#include "ob_expr_xor.h"
#include "ob_expr_estimate_ndv.h"
#include "ob_expr_find_in_set.h"
#include "ob_expr_get_sys_var.h"
#include "ob_expr_to_number.h"
#include "ob_expr_hextoraw.h"
#include "ob_expr_rawtohex.h"
#include "ob_expr_chr.h"
#include "ob_expr_prior.h"
#include "ob_expr_seq_nextval.h"
#include "ob_expr_ifnull.h"
#include "ob_expr_lengthb.h"
#include "ob_expr_ascii.h"
#include "ob_expr_instr.h"
#include "ob_expr_calc_partition_id.h"
#include "ob_expr_instrb.h"
#include "ob_expr_reverse.h"
#include "ob_expr_concat_ws.h"
#include "ob_expr_make_set.h"
#include "ob_expr_interval.h"
#include "ob_expr_sys_op_opnsize.h"
#include "ob_expr_quote.h"
#include "ob_expr_date_add.h"
#include "ob_expr_date_format.h"
#include "ob_expr_from_days.h"
#include "ob_expr_period_diff.h"
#include "ob_expr_time_diff.h"
#include "ob_expr_timestamp_nvl.h"
#include "ob_expr_to_interval.h"
#include "ob_expr_week_of_func.h"
#include "ob_expr_in.h"
#include "ob_expr_fun_default.h"
#include "ob_expr_substrb.h"
#include "ob_expr_remainder.h"
#include "ob_expr_random.h"
#include "ob_expr_width_bucket.h"
#include "ob_expr_sys_extract_utc.h"
#include "ob_expr_to_clob.h"
#include "ob_expr_userenv.h"
#include "ob_expr_vsize.h"
#include "ob_expr_lrpad.h"
#include "ob_expr_pad.h"
#include "ob_expr_fun_values.h"
#include "ob_expr_connect_by_root.h"
#include "ob_expr_oracle_to_char.h"
#include "ob_expr_part_id.h"
#include "ob_expr_hex.h"
#include "ob_expr_shadow_uk_project.h"
#include "ob_expr_char_length.h"
#include "ob_expr_unix_timestamp.h"
#include "ob_expr_case.h"
#include "ob_expr_oracle_decode.h"
#include "ob_expr_remove_const.h"
#include "ob_expr_func_sleep.h"
#include "ob_expr_sys_context.h"
#include "ob_expr_timestamp_diff.h"
#include "ob_expr_from_tz.h"
#include "ob_expr_tz_offset.h"
#include "ob_expr_orahash.h"
#include "ob_expr_get_user_var.h"
#include "ob_expr_util.h"
#include "ob_expr_convert.h"
#include "ob_expr_type_to_str.h"
#include "ob_expr_date_format.h"
#include "ob_expr_calc_urowid.h"
#include "ob_expr_sys_connect_by_path.h"
#include "ob_expr_last_insert_id.h"
#include "ob_expr_part_id_pseudo_column.h"
#include "ob_expr_nullif.h"
#include "ob_expr_oracle_nullif.h"
#include "ob_expr_user_can_access_obj.h"
#include "ob_expr_empty_lob.h"
#include "ob_expr_radians.h"
#include "ob_expr_maketime.h"
#include "ob_expr_to_blob.h"
#include "ob_expr_to_outfile_row.h"
#include "ob_expr_format.h"
#include "ob_expr_quarter.h"
#include "ob_expr_bit_length.h"

namespace oceanbase {
using namespace common;
namespace sql {

extern int cast_eval_arg(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int anytype_to_varchar_char_explicit(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int anytype_anytype_explicit(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_acos_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_and_exprN(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_asin_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_assign_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_atan2_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_atan_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_between_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_bool_expr_for_integer_type(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_bool_expr_for_float_type(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_bool_expr_for_double_type(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_bool_expr_for_other_type(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_char_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_coalesce_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_cos_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_cosh_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_exp_expr_double(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_exp_expr_number(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_ceil_floor(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_round_expr_datetime1(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_round_expr_datetime2(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_round_expr_numeric2(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_round_expr_numeric1(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_initcap_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_left_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_ln_expr_mysql(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_ln_expr_oracle_double(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_ln_expr_oracle_number(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_log10_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_log2_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_log_expr_double(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_log_expr_number(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_not_between_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_or_exprN(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_power_expr_oracle(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_right_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_sign_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_sin_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_sinh_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_sqrt_expr_mysql(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_sqrt_expr_oracle_double(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_sqrt_expr_oracle_number(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_str_to_date_expr_date(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_str_to_date_expr_time(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_str_to_date_expr_datetime(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_tan_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_tanh_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_timestampadd_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_time_to_usec_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_todays_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_to_temporal_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_translate_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_usec_to_time_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_charset_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_collation_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_coercibility_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_set_collation_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_cmp_meta_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_trunc_expr_datetime(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_trunc_expr_numeric(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_truncate_expr(const ObExpr&, ObEvalCtx&, ObDatum&);
extern int calc_reverse_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum);
extern int calc_instrb_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum);
extern int calc_convert_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum);
extern int calc_translate_using_expr(const ObExpr&, ObEvalCtx&, ObDatum&);

// append only, can not delete, set to NULL for mark delete
static ObExpr::EvalFunc g_expr_eval_functions[] = {
    cast_eval_arg,                                     /* 0 */
    anytype_to_varchar_char_explicit,                  /* 1 */
    anytype_anytype_explicit,                          /* 2 */
    calc_acos_expr,                                    /* 3 */
    ObExprAdd::add_datetime_datetime,                  /* 4 */
    ObExprAdd::add_datetime_intervalds,                /* 5 */
    ObExprAdd::add_datetime_intervalym,                /* 6 */
    ObExprAdd::add_datetime_number,                    /* 7 */
    ObExprAdd::add_double_double,                      /* 8 */
    ObExprAdd::add_float_float,                        /* 9 */
    ObExprAdd::add_intervalds_datetime,                /* 10 */
    ObExprAdd::add_intervalds_intervalds,              /* 11 */
    ObExprAdd::add_intervalds_timestamp_tiny,          /* 12 */
    ObExprAdd::add_intervalds_timestamptz,             /* 13 */
    ObExprAdd::add_intervalym_datetime,                /* 14 */
    ObExprAdd::add_intervalym_intervalym,              /* 15 */
    ObExprAdd::add_intervalym_timestampltz,            /* 16 */
    ObExprAdd::add_intervalym_timestampnano,           /* 17 */
    ObExprAdd::add_intervalym_timestamptz,             /* 18 */
    ObExprAdd::add_int_int,                            /* 19 */
    ObExprAdd::add_int_uint,                           /* 20 */
    ObExprAdd::add_number_datetime,                    /* 21 */
    ObExprAdd::add_number_number,                      /* 22 */
    ObExprAdd::add_timestampltz_intervalym,            /* 23 */
    ObExprAdd::add_timestampnano_intervalym,           /* 24 */
    ObExprAdd::add_timestamp_tiny_intervalds,          /* 25 */
    ObExprAdd::add_timestamptz_intervalds,             /* 26 */
    ObExprAdd::add_timestamptz_intervalym,             /* 27 */
    ObExprAdd::add_uint_int,                           /* 28 */
    ObExprAdd::add_uint_uint,                          /* 29 */
    calc_and_exprN,                                    /* 30 */
    calc_asin_expr,                                    /* 31 */
    calc_assign_expr,                                  /* 32 */
    calc_atan2_expr,                                   /* 33 */
    calc_atan_expr,                                    /* 34 */
    calc_between_expr,                                 /* 35 */
    ObExprBitCount::calc_bitcount_expr,                /* 36 */
    ObExprBitNeg::calc_bitneg_expr,                    /* 37 */
    calc_bool_expr_for_integer_type,                   /* 38 */
    calc_bool_expr_for_float_type,                     /* 39 */
    calc_bool_expr_for_double_type,                    /* 40 */
    calc_bool_expr_for_other_type,                     /* 41 */
    calc_char_expr,                                    /* 42 */
    calc_coalesce_expr,                                /* 43 */
    ObExprColumnConv::column_convert,                  /* 44 */
    ObExprConcat::eval_concat,                         /* 45 */
    ObExprConnectionId::eval_connection_id,            /* 46 */
    ObExprConv::eval_conv,                             /* 47 */
    calc_cos_expr,                                     /* 48 */
    calc_cosh_expr,                                    /* 49 */
    ObExprCurrentUser::eval_current_user,              /* 50 */
    ObExprUtcTimestamp::eval_utc_timestamp,            /* 51 */
    ObExprCurTimestamp::eval_cur_timestamp,            /* 52 */
    ObExprSysdate::eval_sysdate,                       /* 53 */
    ObExprCurDate::eval_cur_date,                      /* 54 */
    ObExprCurTime::eval_cur_time,                      /* 55 */
    ObExprDatabase::eval_database,                     /* 56 */
    ObExprDate::eval_date,                             /* 57 */
    ObExprDateDiff::eval_date_diff,                    /* 58 */
    ObExprMonthsBetween::eval_months_between,          /* 59 */
    ObExprToSeconds::calc_toseconds,                   /* 60 */
    ObExprSecToTime::calc_sectotime,                   /* 61 */
    ObExprTimeToSec::calc_timetosec,                   /* 62 */
    ObExprSubtime::subtime_datetime,                   /* 63 */
    ObExprSubtime::subtime_varchar,                    /* 64 */
    ObExprDiv::div_float,                              /* 65 */
    ObExprDiv::div_double,                             /* 66 */
    ObExprDiv::div_number,                             /* 67 */
    ObExprDiv::div_intervalym_number,                  /* 68 */
    ObExprDiv::div_intervalds_number,                  /* 69 */
    ObExprEffectiveTenant::eval_effective_tenant,      /* 70 */
    ObExprEffectiveTenantId::eval_effective_tenant_id, /* 71 */
    ObExprExists::exists_eval,                         /* 72 */
    calc_exp_expr_double,                              /* 73 */
    calc_exp_expr_number,                              /* 74 */
    ObExprExtract::calc_extract_oracle,                /* 75 */
    ObExprExtract::calc_extract_mysql,                 /* 76 */
    ObExprFoundRows::eval_found_rows,                  /* 77 */
    ObExprFromUnixTime::eval_one_temporal_fromtime,    /* 78 */
    ObExprFromUnixTime::eval_one_param_fromtime,       /* 79 */
    ObExprFromUnixTime::eval_fromtime_normal,          /* 80 */
    ObExprFromUnixTime::eval_fromtime_special,         /* 81 */
    calc_ceil_floor,                                   /* 82 */
    ObExprFuncPartKey::calc_partition_key,             /* 83 */
    calc_round_expr_datetime1,                         /* 84 */
    calc_round_expr_datetime2,                         /* 85 */
    calc_round_expr_numeric2,                          /* 86 */
    calc_round_expr_numeric1,                          /* 87 */
    ObExprBaseGreatest::calc_greatest,                 /* 88 */
    ObExprHostIP::eval_host_ip,                        /* 89 */
    calc_initcap_expr,                                 /* 90 */
    ObExprTrim::eval_trim,                             /* 91 */
    ObExprInsert::calc_expr_insert,                    /* 92 */
    ObExprInt2ip::int2ip_varchar,                      /* 93 */
    ObExprIntDiv::div_int_int,                         /* 94 */
    ObExprIntDiv::div_int_uint,                        /* 95 */
    ObExprIntDiv::div_uint_int,                        /* 96 */
    ObExprIntDiv::div_uint_uint,                       /* 97 */
    ObExprIntDiv::div_number,                          /* 98 */
    ObExprIp2int::ip2int_varchar,                      /* 99 */
    ObExprIs::calc_is_date_int_null,                   /* 100 */
    ObExprIs::calc_is_null,                            /* 101 */
    ObExprIs::int_is_true,                             /* 102 */
    ObExprIs::int_is_false,                            /* 103 */
    ObExprIs::float_is_true,                           /* 104 */
    ObExprIs::float_is_false,                          /* 105 */
    ObExprIs::double_is_true,                          /* 106 */
    ObExprIs::double_is_false,                         /* 107 */
    ObExprIs::number_is_true,                          /* 108 */
    ObExprIs::number_is_false,                         /* 109 */
    ObExprIsNot::calc_is_not_null,                     /* 110 */
    ObExprIsNot::int_is_not_true,                      /* 111 */
    ObExprIsNot::int_is_not_false,                     /* 112 */
    ObExprIsNot::float_is_not_true,                    /* 113 */
    ObExprIsNot::float_is_not_false,                   /* 114 */
    ObExprIsNot::double_is_not_true,                   /* 115 */
    ObExprIsNot::double_is_not_false,                  /* 116 */
    ObExprIsNot::number_is_not_true,                   /* 117 */
    ObExprIsNot::number_is_not_false,                  /* 118 */
    ObExprLastExecId::eval_last_exec_id,               /* 119 */
    ObExprLastTraceId::eval_last_trace_id,             /* 120 */
    ObExprBaseLeast::calc_least,                       /* 121 */
    calc_left_expr,                                    /* 122 */
    ObExprLength::calc_null,                           /* 123 */
    ObExprLength::calc_oracle_mode,                    /* 124 */
    ObExprLength::calc_mysql_mode,                     /* 125 */
    ObExprLike::like_varchar,                          /* 126 */
    calc_ln_expr_mysql,                                /* 127 */
    calc_ln_expr_oracle_double,                        /* 128 */
    calc_ln_expr_oracle_number,                        /* 129 */
    calc_log10_expr,                                   /* 130 */
    calc_log2_expr,                                    /* 131 */
    calc_log_expr_double,                              /* 132 */
    calc_log_expr_number,                              /* 133 */
    ObExprLower::calc_lower,                           /* 134 */
    ObExprUpper::calc_upper,                           /* 135 */
    ObExprMd5::calc_md5,                               /* 136 */
    ObExprMinus::minus_datetime_datetime,              /* 137 */
    ObExprMinus::minus_datetime_datetime_oracle,       /* 138 */
    ObExprMinus::minus_datetime_intervalds,            /* 139 */
    ObExprMinus::minus_datetime_intervalym,            /* 140 */
    ObExprMinus::minus_datetime_number,                /* 141 */
    ObExprMinus::minus_double_double,                  /* 142 */
    ObExprMinus::minus_float_float,                    /* 143 */
    ObExprMinus::minus_intervalds_intervalds,          /* 144 */
    ObExprMinus::minus_intervalym_intervalym,          /* 145 */
    ObExprMinus::minus_int_int,                        /* 146 */
    ObExprMinus::minus_int_uint,                       /* 147 */
    ObExprMinus::minus_number_number,                  /* 148 */
    ObExprMinus::minus_timestampltz_intervalym,        /* 149 */
    ObExprMinus::minus_timestampnano_intervalym,       /* 150 */
    ObExprMinus::minus_timestamp_timestamp,            /* 151 */
    ObExprMinus::minus_timestamp_tiny_intervalds,      /* 152 */
    ObExprMinus::minus_timestamptz_intervalds,         /* 153 */
    ObExprMinus::minus_timestamptz_intervalym,         /* 154 */
    ObExprMinus::minus_uint_int,                       /* 155 */
    ObExprMinus::minus_uint_uint,                      /* 156 */
    ObExprMod::mod_double,                             /* 157 */
    ObExprMod::mod_float,                              /* 158 */
    ObExprMod::mod_int_int,                            /* 159 */
    ObExprMod::mod_int_uint,                           /* 160 */
    ObExprMod::mod_number,                             /* 161 */
    ObExprMod::mod_uint_int,                           /* 162 */
    ObExprMod::mod_uint_uint,                          /* 163 */
    ObExprMul::mul_double,                             /* 164 */
    ObExprMul::mul_float,                              /* 165 */
    ObExprMul::mul_intervalds_number,                  /* 166 */
    ObExprMul::mul_intervalym_number,                  /* 167 */
    ObExprMul::mul_int_int,                            /* 168 */
    ObExprMul::mul_int_uint,                           /* 169 */
    ObExprMul::mul_number,                             /* 170 */
    ObExprMul::mul_number_intervalds,                  /* 171 */
    ObExprMul::mul_number_intervalym,                  /* 172 */
    ObExprMul::mul_uint_int,                           /* 173 */
    ObExprMul::mul_uint_uint,                          /* 174 */
    ObExprMySQLPort::eval_mysql_port,                  /* 175 */
    NULL,                                        // ObExprNeg::eval_tinyint is deleted                         /* 176 */
    calc_not_between_expr,                       /* 177 */
    ObExprNot::eval_not,                         /* 178 */
    ObExprNotExists::not_exists_eval,            /* 179 */
    ObExprNullSafeEqual::ns_equal_eval,          /* 180 */
    ObExprNullSafeEqual::row_ns_equal_eval,      /* 181 */
    ObExprNvlUtil::calc_nvl_expr,                /* 182 */
    ObExprNvlUtil::calc_nvl_expr2,               /* 183 */
    ObSubQueryRelationalExpr::subquery_cmp_eval, /* 184 */
    ObBitwiseExprOperator::calc_result2_oracle,  /* 185 */
    ObBitwiseExprOperator::calc_result2_mysql,   /* 186 */
    ObRelationalExprOperator::row_eval,          /* 187 */
    calc_or_exprN,                               /* 188 */
    ObExprPow::calc_pow_expr,                    /* 189 */
    calc_power_expr_oracle,                      /* 190 */
    ObExprRegexp::eval_regexp,                   /* 191 */
    ObExprRegexpCount::eval_regexp_count,        /* 192 */
    ObExprRegexpInstr::eval_regexp_instr,        /* 193 */
    ObExprRegexpLike::eval_regexp_like,          /* 194 */
    ObExprRegexpReplace::eval_regexp_replace,    /* 195 */
    ObExprRegexpSubstr::eval_regexp_substr,      /* 196 */
    ObExprRepeat::eval_repeat,                   /* 197 */
    ObExprReplace::eval_replace,                 /* 198 */
    ObExprFuncDump::eval_dump,                   /* 199 */
    ObExprFuncPartOldKey::eval_part_old_key,     /* 200 */
    ObExprFuncPartHash::eval_part_hash,          /* 201 */
    ObExprFuncAddrToPartId::eval_addr_to_part_id,      /* 202 */
    ObExprAutoincNextval::eval_nextval,                /* 203 */
    ObExprFuncLnnvl::eval_lnnvl,                       /* 204 */
    ObExprIsServingTenant::eval_is_serving_tenant,     /* 205 */
    ObExprSysPrivilegeCheck::eval_sys_privilege_check, /* 206 */
    ObExprField::eval_field,                           /* 207 */
    ObExprElt::eval_elt,                               /* 208 */
    ObExprDesHexStr::eval_des_hex_str,                 /* 209 */
    calc_right_expr,                                   /* 210 */
    ObExprRowCount::eval_row_count,                    /* 211 */
    ObExprRowNum::rownum_eval,                         /* 212 */
    ObExprRpcPort::eval_rpc_port,                      /* 213 */
    NULL,                                       // ObExprCot::calc_cot_expr,                                   /* 214 */
    calc_sign_expr,                             /* 215 */
    calc_sin_expr,                              /* 216 */
    calc_sinh_expr,                             /* 217 */
    ObExprSpace::eval_space,                    /* 218 */
    calc_sqrt_expr_mysql,                       /* 219 */
    calc_sqrt_expr_oracle_double,               /* 220 */
    calc_sqrt_expr_oracle_number,               /* 221 */
    calc_str_to_date_expr_time,                 /* 222 */
    calc_str_to_date_expr_date,                 /* 223 */
    calc_str_to_date_expr_datetime,             /* 224 */
    ObExprSubQueryRef::expr_eval,               /* 225 */
    ObExprSubstr::eval_substr,                  /* 226 */
    ObExprSubstringIndex::eval_substring_index, /* 227 */
    calc_tan_expr,                              /* 228 */
    calc_tanh_expr,                             /* 229 */
    ObExprDayOfMonth::calc_dayofmonth,          /* 230 */
    ObExprDayOfWeek::calc_dayofweek,            /* 231 */
    ObExprDayOfYear::calc_dayofyear,            /* 232 */
    ObExprHour::calc_hour,                      /* 233 */
    ObExprMicrosecond::calc_microsecond,        /* 234 */
    ObExprMinute::calc_minute,                  /* 235 */
    ObExprMonth::calc_month,                    /* 236 */
    ObExprSecond::calc_second,                  /* 237 */
    ObExprTime::calc_time,                      /* 238 */
    ObExprYear::calc_year,                      /* 239 */
    calc_timestampadd_expr,                     /* 240 */
    ObExprLocalTimestamp::eval_localtimestamp,  /* 241 */
    ObExprSysTimestamp::eval_systimestamp,      /* 242 */
    calc_time_to_usec_expr,                     /* 243 */
    ObExprDbtimezone::eval_db_timezone,         /* 244 */
    ObExprSessiontimezone::eval_session_timezone,      /* 245 */
    calc_todays_expr,                                  /* 246 */
    calc_to_temporal_expr,                             /* 247 */
    calc_translate_expr,                               /* 248 */
    ObExprTrim::eval_trim,                             /* 249 */
    ObExprUid::eval_uid,                               /* 250 */
    ObExprUnhex::eval_unhex,                           /* 251 */
    calc_usec_to_time_expr,                            /* 252 */
    ObExprUser::eval_user,                             /* 253 */
    ObExprUuid::eval_uuid,                             /* 254 */
    ObExprSysGuid::eval_sys_guid,                      /* 255 */
    ObExprVersion::eval_version,                       /* 256 */
    ObExprXor::eval_xor,                               /* 257 */
    calc_charset_expr,                                 /* 258 */
    calc_collation_expr,                               /* 259 */
    calc_coercibility_expr,                            /* 260 */
    calc_set_collation_expr,                           /* 261 */
    calc_cmp_meta_expr,                                /* 262 */
    calc_trunc_expr_datetime,                          /* 263 */
    calc_trunc_expr_numeric,                           /* 264 */
    calc_truncate_expr,                                /* 265 */
    ObExprEstimateNdv::calc_estimate_ndv_expr,         /* 266 */
    ObExprFindInSet::calc_find_in_set_expr,            /* 267 */
    ObExprGetSysVar::calc_get_sys_val_expr,            /* 268 */
    ObExprToNumber::calc_tonumber_expr,                /* 269 */
    ObExprToBinaryFloat::calc_to_binaryfloat_expr,     /* 270 */
    ObExprToBinaryDouble::calc_to_binarydouble_expr,   /* 271 */
    ObExprHextoraw::calc_hextoraw_expr,                /* 272 */
    ObExprRawtohex::calc_rawtohex_expr,                /* 273 */
    ObExprChr::calc_chr_expr,                          /* 274 */
    ObExprIfNull::calc_ifnull_expr,                    /* 275 */
    ObExprLengthb::calc_lengthb_expr,                  /* 276 */
    ObExprAscii::calc_ascii_expr,                      /* 277 */
    ObExprOrd::calc_ord_expr,                          /* 278 */
    ObExprInstr::calc_mysql_instr_expr,                /* 279 */
    ObExprOracleInstr::calc_oracle_instr_expr,         /* 280 */
    ObLocationExprOperator::calc_location_expr,        /* 281 */
    ObExprCalcPartitionId::calc_no_partition_location, /* 282 */
    ObExprCalcPartitionId::calc_partition_level_one,   /* 283 */
    ObExprCalcPartitionId::calc_partition_level_two,   /* 284 */
    ObExprPrior::calc_prior_expr,                      /* 285 */
    ObExprSeqNextval::calc_sequence_nextval,           /* 286 */
    calc_reverse_expr,                                 /* 287 */
    calc_instrb_expr,                                  /* 288 */
    ObExprConcatWs::calc_concat_ws_expr,               /* 289 */
    ObExprMakeSet::calc_make_set_expr,                 /* 290 */
    ObExprInterval::calc_interval_expr,                /* 291 */
    ObExprSysOpOpnsize::calc_sys_op_opnsize_expr,      /* 292 */
    ObExprQuote::calc_quote_expr,                      /* 293 */
    ObExprDateAdd::calc_date_add,                      /* 294 */
    ObExprDateSub::calc_date_sub,                      /* 295 */
    ObExprAddMonths::calc_add_months,                  /* 296 */
    ObExprLastDay::calc_last_day,                      /* 297 */
    ObExprNextDay::calc_next_day,                      /* 298 */
    ObExprFromDays::calc_fromdays,                     /* 299 */
    ObExprPeriodDiff::calc_perioddiff,                 /* 300 */
    ObExprTimeDiff::calc_timediff,                     /* 301 */
    ObExprTimestampNvl::calc_timestampnvl,             /* 302 */
    ObExprToYMInterval::calc_to_yminterval,            /* 303 */
    ObExprToDSInterval::calc_to_dsinterval,            /* 304 */
    ObExprNumToYMInterval::calc_num_to_yminterval,     /* 305 */
    ObExprNumToDSInterval::calc_num_to_dsinterval,     /* 306 */
    ObExprWeekOfYear::calc_weekofyear,                 /* 307 */
    ObExprWeekDay::calc_weekday,                       /* 308 */
    ObExprYearWeek::calc_yearweek,                     /* 309 */
    ObExprWeek::calc_week,                             /* 310 */
    ObExprInOrNotIn::eval_in_with_row,                 /* 311 */
    ObExprInOrNotIn::eval_in_without_row,              /* 312 */
    ObExprInOrNotIn::eval_in_with_row_fallback,        /* 313 */
    ObExprInOrNotIn::eval_in_without_row_fallback,     /* 314 */
    ObExprInOrNotIn::eval_in_with_subquery,            /* 315 */
    ObExprFunDefault::calc_default_expr,               /* 316 */
    ObExprSubstrb::calc_substrb_expr,                  /* 317 */
    ObExprRemainder::calc_remainder_expr,              /* 318 */
    ObExprRandom::calc_random_expr_const_seed,         /* 319 */
    ObExprRandom::calc_random_expr_nonconst_seed,      /* 320 */
    ObExprWidthBucket::calc_width_bucket_expr,         /* 321 */
    ObExprSysExtractUtc::calc_sys_extract_utc,         /* 322 */
    ObExprToClob::calc_to_clob_expr,                   /* 323 */
    ObExprUserEnv::calc_user_env_expr,                 /* 324 */
    ObExprVsize::calc_vsize_expr,                      /* 325 */
    ObExprOracleLpad::calc_oracle_lpad_expr,           /* 326 */
    ObExprOracleRpad::calc_oracle_rpad_expr,           /* 327 */
    ObExprLpad::calc_mysql_lpad_expr,                  /* 328 */
    ObExprRpad::calc_mysql_rpad_expr,                  /* 329 */
    ObExprPad::calc_pad_expr,                          /* 330 */
    ObExprFunValues::eval_values,                      /* 331 */
    ObExprConnectByRoot::eval_connect_by_root,         /* 332 */
    ObExprOracleToChar::eval_oracle_to_char,           /* 333 */
    ObExprPartId::eval_part_id,                        /* 334 */
    ObExprHex::eval_hex,                               /* 335 */
    ObExprShadowUKProject::shadow_uk_project,          /* 336 */
    ObExprCharLength::eval_char_length,                /* 337 */
    ObExprUnixTimestamp::eval_unix_timestamp,          /* 338 */
    NULL,                                     // ObExprAesDecrypt::eval_aes_decrypt,                         /* 339 */
    NULL,                                     // ObExprAesEncrypt::eval_aes_encrypt,                         /* 340 */
    ObExprCase::calc_case_expr,               /* 341 */
    ObExprOracleDecode::eval_decode,          /* 342 */
    ObExprRemoveConst::eval_remove_const,     /* 343 */
    ObExprSleep::eval_sleep,                  /* 344 */
    ObExprSysContext::eval_sys_context,       /* 345 */
    ObExprTimeStampDiff::eval_timestamp_diff, /* 347 */
    ObExprFromTz::eval_from_tz,               /* 348 */
    ObExprTzOffset::eval_tz_offset,           /* 349 */
    ObExprOrahash::eval_orahash,              /* 350 */
    ObExprGetUserVar::eval_get_user_var,      /* 351 */
    ObExprUtil::eval_generated_column,        /* 352 */
    ObExprCalcPartitionId::calc_opt_route_hash_one,   /* 353 */
    calc_convert_expr,                                /* 354 */
    ObExprSetToStr::calc_to_str_expr,                 /* 355 */
    ObExprEnumToStr::calc_to_str_expr,                /* 356 */
    ObExprSetToInnerType::calc_to_inner_expr,         /* 357 */
    ObExprEnumToInnerType::calc_to_inner_expr,        /* 358 */
    ObExprDateFormat::calc_date_format_invalid,       /* 359 */
    ObExprDateFormat::calc_date_format,               /* 360 */
    ObExprCalcURowID::calc_urowid,                    /* 361 */
    ObExprFuncPartOldHash::eval_old_part_hash,        /* 362 */
    ObExprFuncPartNewKey::calc_new_partition_key,     /* 363 */
    ObExprUtil::eval_stack_overflow_check,            /* 364 */
    ObExprSysConnectByPath::calc_sys_path,            /* 365 */
    ObExprLastInsertID::eval_last_insert_id,          /* 366 */
    ObExprPartIdPseudoColumn::eval_part_id,           /* 367 */
    ObExprNullif::eval_nullif,                        /* 368 */
    ObExprOracleNullif::eval_nullif,                  /* 369 */
    ObExprUserCanAccessObj::eval_user_can_access_obj, /* 370 */
    ObExprEmptyClob::eval_empty_clob,                 /* 371 */
    ObExprEmptyBlob::eval_empty_blob,                 /* 372 */
    ObExprRadians::calc_radians_expr,                 /* 373 */
    ObExprMakeTime::eval_maketime,                    /* 374 */
    ObExprMonthName::calc_month_name,                 /* 375 */
    ObExprToBlob::eval_to_blob,                       /* 376 */
    ObExprNlsLower::calc_lower,                       /* 378 */
    ObExprNlsUpper::calc_upper,                       /* 379 */
    ObExprToOutfileRow::to_outfile_str,               /* 380 */
    NULL,                                     // ObExprIs::calc_is_infinite,                                /* 381 */
    NULL,                                     // ObExprIs::calc_is_nan,                                     /* 382 */
    NULL,                                     // ObExprIsNot::calc_is_not_infinite,                         /* 383 */
    NULL,                                     // ObExprIsNot::calc_is_not_nan,                              /* 384 */
    ObExprOracleNullif::eval_nullif_not_null, /* 385 */
    NULL,                                     // ObExprNaNvl::eval_nanvl,                                   /* 386 */
    ObExprFormat::calc_format_expr,           /* 387 */
    calc_translate_using_expr,                /* 388 */
    ObExprQuarter::calc_quater,               /* 389 */
    ObExprBitLength::calc_bit_length          /* 390 */
};

REG_SER_FUNC_ARRAY(OB_SFA_SQL_EXPR_EVAL, g_expr_eval_functions, ARRAYSIZEOF(g_expr_eval_functions));

}  // end namespace sql
}  // end namespace oceanbase
