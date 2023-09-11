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
#include "ob_expr_current_user_priv.h"
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
#include "ob_expr_export_set.h"
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
#include "ob_expr_inet.h"
#include "ob_expr_is.h"
#include "ob_expr_last_exec_id.h"
#include "ob_expr_last_trace_id.h"
#include "ob_expr_least.h"
#include "ob_expr_length.h"
#include "ob_expr_like.h"
#include "ob_expr_lower.h"
#include "ob_expr_md5.h"
#include "ob_expr_crc32.h"
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
#include "ob_expr_rand.h"
#include "ob_expr_randstr.h"
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
#include "ob_expr_aes_encrypt.h"
#include "ob_expr_case.h"
#include "ob_expr_oracle_decode.h"
#include "ob_expr_remove_const.h"
#include "ob_expr_wrapper_inner.h"
#include "ob_expr_func_sleep.h"
#include "ob_expr_errno.h"
#include "ob_expr_get_package_var.h"
#include "ob_expr_sys_context.h"
#include "ob_expr_timestamp_diff.h"
#include "ob_expr_from_tz.h"
#include "ob_expr_tz_offset.h"
#include "ob_expr_orahash.h"
#include "ob_expr_get_user_var.h"
#include "ob_expr_util.h"
#include "ob_expr_cot.h"
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
#include "ob_expr_pi.h"
#include "ob_expr_maketime.h"
#include "ob_expr_makedate.h"
#include "ob_expr_to_blob.h"
#include "ob_expr_join_filter.h"
#include "ob_expr_to_outfile_row.h"
#include "ob_expr_format.h"
#include "ob_expr_and.h"
#include "ob_expr_or.h"
#include "ob_expr_quarter.h"
#include "ob_expr_bit_length.h"
#include "ob_expr_unistr.h"
#include "ob_expr_at_time_zone.h"
#include "ob_expr_to_single_byte.h"
#include "ob_expr_to_multi_byte.h"
#include "ob_expr_time_format.h"
#include "ob_expr_dll_udf.h"
#include "ob_expr_collection_construct.h"
#include "ob_expr_obj_access.h"
#include "ob_expr_pl_associative_index.h"
#include "ob_expr_udf.h"
#include "ob_expr_object_construct.h"
#include "ob_expr_pl_get_cursor_attr.h"
#include "ob_expr_pl_integer_checker.h"
#include "ob_expr_get_subprogram_var.h"
#include "ob_expr_pl_sqlcode_sqlerrm.h"
#include "ob_expr_multiset.h"
#include "ob_expr_cardinality.h"
#include "ob_expr_coll_pred.h"
#include "ob_expr_stmt_id.h"
#include "ob_expr_pl_seq_nextval.h"
#include "ob_expr_output_pack.h"
#include "ob_expr_returning_lob.h"
#include "ob_expr_set.h"
#include "ob_expr_obversion.h"
#include "ob_expr_ols_funcs.h"
#include "ob_expr_plsql_variable.h"
#include "ob_expr_degrees.h"
#include "ob_expr_any_value.h"
#include "ob_expr_uuid_short.h"
#include "ob_expr_func_round.h"
#include "ob_expr_validate_password_strength.h"
#include "ob_expr_soundex.h"
#include "ob_expr_rowid_to_char.h"
#include "ob_expr_rowid_to_nchar.h"
#include "ob_expr_char_to_rowid.h"
#include "ob_expr_benchmark.h"
#include "ob_expr_weight_string.h"
#include "ob_expr_convert_tz.h"
#include "ob_expr_dml_event.h"
#include "ob_expr_to_base64.h"
#include "ob_expr_from_base64.h"
#include "ob_expr_random_bytes.h"
#include "ob_pl_expr_subquery.h"
#include "ob_expr_encode_sortkey.h"
#include "ob_expr_hash.h"
#include "ob_expr_nlssort.h"
#include "ob_expr_json_object.h"
#include "ob_expr_json_extract.h"
#include "ob_expr_json_contains.h"
#include "ob_expr_json_contains_path.h"
#include "ob_expr_json_depth.h"
#include "ob_expr_json_keys.h"
#include "ob_expr_json_search.h"
#include "ob_expr_json_array.h"
#include "ob_expr_json_quote.h"
#include "ob_expr_json_unquote.h"
#include "ob_expr_json_overlaps.h"
#include "ob_expr_json_valid.h"
#include "ob_expr_json_remove.h"
#include "ob_expr_json_array_append.h"
#include "ob_expr_json_array_insert.h"
#include "ob_expr_json_value.h"
#include "ob_expr_json_replace.h"
#include "ob_expr_json_type.h"
#include "ob_expr_json_length.h"
#include "ob_expr_json_insert.h"
#include "ob_expr_json_storage_size.h"
#include "ob_expr_json_storage_free.h"
#include "ob_expr_json_set.h"
#include "ob_expr_json_merge_preserve.h"
#include "ob_expr_json_merge.h"
#include "ob_expr_json_merge_patch.h"
#include "ob_expr_json_pretty.h"
#include "ob_expr_json_member_of.h"
#include "ob_expr_json_equal.h"
#include "ob_expr_sha.h"
#include "ob_expr_compress.h"
#include "ob_expr_statement_digest.h"
#include "ob_expr_is_json.h"
#include "ob_expr_json_query.h"
#include "ob_expr_json_exists.h"
#include "ob_expr_treat.h"
#include "ob_expr_point.h"
#include "ob_expr_spatial_collection.h"
#include "ob_expr_st_geomfromtext.h"
#include "ob_expr_st_area.h"
#include "ob_expr_st_intersects.h"
#include "ob_expr_st_x.h"
#include "ob_expr_st_transform.h"
#include "ob_expr_priv_st_transform.h"
#include "ob_expr_st_covers.h"
#include "ob_expr_st_bestsrid.h"
#include "ob_expr_st_astext.h"
#include "ob_expr_st_buffer.h"
#include "ob_expr_spatial_cellid.h"
#include "ob_expr_spatial_mbr.h"
#include "ob_expr_st_geomfromewkb.h"
#include "ob_expr_st_geomfromwkb.h"
#include "ob_expr_st_geomfromewkt.h"
#include "ob_expr_st_asewkt.h"
#include "ob_expr_st_srid.h"
#include "ob_expr_st_distance.h"
#include "ob_expr_st_geometryfromtext.h"
#include "ob_expr_priv_st_setsrid.h"
#include "ob_expr_priv_st_point.h"
#include "ob_expr_priv_st_geogfromtext.h"
#include "ob_expr_priv_st_geographyfromtext.h"
#include "ob_expr_st_isvalid.h"
#include "ob_expr_st_dwithin.h"
#include "ob_expr_st_aswkb.h"
#include "ob_expr_st_distance_sphere.h"
#include "ob_expr_st_contains.h"
#include "ob_expr_st_within.h"
#include "ob_expr_priv_st_asewkb.h"
#include "ob_expr_name_const.h"
#include "ob_expr_format_bytes.h"
#include "ob_expr_format_pico_time.h"
#include "ob_expr_encrypt.h"
#include "ob_expr_coalesce.h"
#include "ob_expr_cast.h"
#include "ob_expr_current_scn.h"
#include "ob_expr_icu_version.h"
#include "ob_expr_sql_mode_convert.h"
#include "ob_expr_priv_xml_binary.h"
#include "ob_expr_sys_makexml.h"
#include "ob_expr_priv_xml_binary.h"
#include "ob_expr_xmlparse.h"
#include "ob_expr_xml_element.h"
#include "ob_expr_xml_attributes.h"
#include "ob_expr_extract_value.h"
#include "ob_expr_extract_xml.h"
#include "ob_expr_xml_serialize.h"
#include "ob_expr_xmlcast.h"
#include "ob_expr_update_xml.h"
#include "ob_expr_generator_func.h"
#include "ob_expr_random.h"
#include "ob_expr_randstr.h"
#include "ob_expr_zipf.h"
#include "ob_expr_normal.h"
#include "ob_expr_uniform.h"
#include "ob_expr_prefix_pattern.h"
#include "ob_expr_initcap.h"
#include "ob_expr_temp_table_ssid.h"
#include "ob_expr_align_date4cmp.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

//
// this file is for function serialization
// Without maps defined here, you can not get correct function ptr
// when serialize between different observer versions
//
extern int cast_eval_arg(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int anytype_to_varchar_char_explicit(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int anytype_anytype_explicit(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_acos_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_and_exprN(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_asin_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_assign_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_atan2_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_atan_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_between_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_bool_expr_for_integer_type(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_bool_expr_for_float_type(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_bool_expr_for_double_type(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_bool_expr_for_other_type(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_char_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_coalesce_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_cos_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_cosh_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_exp_expr_double(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_exp_expr_number(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_ceil_floor(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_round_expr_datetime1(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_round_expr_datetime2(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_round_expr_numeric2(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_round_expr_numeric1(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_initcap_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_left_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_ln_expr_mysql(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_ln_expr_oracle_double(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_ln_expr_oracle_number(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_log10_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_log2_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_log_expr_double(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_log_expr_number(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_not_between_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_or_exprN(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_power_expr_oracle(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_right_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_sign_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_sin_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_sinh_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_sqrt_expr_mysql(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_sqrt_expr_oracle_double(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_sqrt_expr_oracle_number(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_str_to_date_expr_date(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_str_to_date_expr_time(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_str_to_date_expr_datetime(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_tan_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_tanh_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_timestampadd_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_time_to_usec_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_todays_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_to_temporal_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_translate_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_usec_to_time_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_charset_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_collation_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_coercibility_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_set_collation_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_cmp_meta_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_trunc_expr_datetime(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_trunc_expr_numeric(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_truncate_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_reverse_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
extern int calc_instrb_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
extern int calc_convert_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
extern int calc_translate_using_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int eval_question_mark_func(EVAL_FUNC_ARG_DECL);
extern int cast_eval_arg_batch(const ObExpr &, ObEvalCtx &, const ObBitVector &, const int64_t);
extern int eval_batch_ceil_floor(const ObExpr &, ObEvalCtx &, const ObBitVector &, const int64_t);
extern int eval_assign_question_mark_func(EVAL_FUNC_ARG_DECL);
extern int calc_timestamp_to_scn_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_scn_to_timestamp_expr(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int calc_sqrt_expr_mysql_in_batch(const ObExpr &, ObEvalCtx &, const ObBitVector &, const int64_t);
extern int calc_sqrt_expr_oracle_double_in_batch(const ObExpr &, ObEvalCtx &, const ObBitVector &, const int64_t);
extern int calc_sqrt_expr_oracle_number_in_batch(const ObExpr &, ObEvalCtx &, const ObBitVector &, const int64_t);

// append only, can not delete, set to NULL for mark delete
static ObExpr::EvalFunc g_expr_eval_functions[] = {
  cast_eval_arg,                                                      /* 0 */
  anytype_to_varchar_char_explicit,                                   /* 1 */
  anytype_anytype_explicit,                                           /* 2 */
  calc_acos_expr,                                                     /* 3 */
  ObExprAdd::add_datetime_datetime,                                   /* 4 */
  ObExprAdd::add_datetime_intervalds,                                 /* 5 */
  ObExprAdd::add_datetime_intervalym,                                 /* 6 */
  ObExprAdd::add_datetime_number,                                     /* 7 */
  ObExprAdd::add_double_double,                                       /* 8 */
  ObExprAdd::add_float_float,                                         /* 9 */
  ObExprAdd::add_intervalds_datetime,                                 /* 10 */
  ObExprAdd::add_intervalds_intervalds,                               /* 11 */
  ObExprAdd::add_intervalds_timestamp_tiny,                           /* 12 */
  ObExprAdd::add_intervalds_timestamptz,                              /* 13 */
  ObExprAdd::add_intervalym_datetime,                                 /* 14 */
  ObExprAdd::add_intervalym_intervalym,                               /* 15 */
  ObExprAdd::add_intervalym_timestampltz,                             /* 16 */
  ObExprAdd::add_intervalym_timestampnano,                            /* 17 */
  ObExprAdd::add_intervalym_timestamptz,                              /* 18 */
  ObExprAdd::add_int_int,                                             /* 19 */
  ObExprAdd::add_int_uint,                                            /* 20 */
  ObExprAdd::add_number_datetime,                                     /* 21 */
  ObExprAdd::add_number_number,                                       /* 22 */
  ObExprAdd::add_timestampltz_intervalym,                             /* 23 */
  ObExprAdd::add_timestampnano_intervalym,                            /* 24 */
  ObExprAdd::add_timestamp_tiny_intervalds,                           /* 25 */
  ObExprAdd::add_timestamptz_intervalds,                              /* 26 */
  ObExprAdd::add_timestamptz_intervalym,                              /* 27 */
  ObExprAdd::add_uint_int,                                            /* 28 */
  ObExprAdd::add_uint_uint,                                           /* 29 */
  calc_and_exprN,                                                     /* 30 */
  calc_asin_expr,                                                     /* 31 */
  calc_assign_expr,                                                   /* 32 */
  calc_atan2_expr,                                                    /* 33 */
  calc_atan_expr,                                                     /* 34 */
  calc_between_expr,                                                  /* 35 */
  ObExprBitCount::calc_bitcount_expr,                                 /* 36 */
  ObExprBitNeg::calc_bitneg_expr,                                     /* 37 */
  calc_bool_expr_for_integer_type,                                    /* 38 */
  calc_bool_expr_for_float_type,                                      /* 39 */
  calc_bool_expr_for_double_type,                                     /* 40 */
  calc_bool_expr_for_other_type,                                      /* 41 */
  calc_char_expr,                                                     /* 42 */
  calc_coalesce_expr,                                                 /* 43 */
  ObExprColumnConv::column_convert,                                   /* 44 */
  ObExprConcat::eval_concat,                                          /* 45 */
  ObExprConnectionId::eval_connection_id,                             /* 46 */
  ObExprConv::eval_conv,                                              /* 47 */
  calc_cos_expr,                                                      /* 48 */
  calc_cosh_expr,                                                     /* 49 */
  ObExprCurrentUser::eval_current_user,                               /* 50 */
  ObExprUtcTimestamp::eval_utc_timestamp,                             /* 51 */
  ObExprCurTimestamp::eval_cur_timestamp,                             /* 52 */
  ObExprSysdate::eval_sysdate,                                        /* 53 */
  ObExprCurDate::eval_cur_date,                                       /* 54 */
  ObExprCurTime::eval_cur_time,                                       /* 55 */
  ObExprDatabase::eval_database,                                      /* 56 */
  ObExprDate::eval_date,                                              /* 57 */
  ObExprDateDiff::eval_date_diff,                                     /* 58 */
  ObExprMonthsBetween::eval_months_between,                           /* 59 */
  ObExprToSeconds::calc_toseconds,                                    /* 60 */
  ObExprSecToTime::calc_sectotime,                                    /* 61 */
  ObExprTimeToSec::calc_timetosec,                                    /* 62 */
  ObExprSubtime::subaddtime_datetime,                                    /* 63 */
  ObExprSubtime::subaddtime_varchar,                                     /* 64 */
  ObExprDiv::div_float,                                               /* 65 */
  ObExprDiv::div_double,                                              /* 66 */
  ObExprDiv::div_number,                                              /* 67 */
  ObExprDiv::div_intervalym_number,                                   /* 68 */
  ObExprDiv::div_intervalds_number,                                   /* 69 */
  ObExprEffectiveTenant::eval_effective_tenant,                       /* 70 */
  ObExprEffectiveTenantId::eval_effective_tenant_id,                  /* 71 */
  ObExprExists::exists_eval,                                          /* 72 */
  calc_exp_expr_double,                                               /* 73 */
  calc_exp_expr_number,                                               /* 74 */
  ObExprExtract::calc_extract_oracle,                                 /* 75 */
  ObExprExtract::calc_extract_mysql,                                  /* 76 */
  ObExprFoundRows::eval_found_rows,                                   /* 77 */
  ObExprFromUnixTime::eval_one_temporal_fromtime,                     /* 78 */
  ObExprFromUnixTime::eval_one_param_fromtime,                        /* 79 */
  ObExprFromUnixTime::eval_fromtime_normal,                           /* 80 */
  ObExprFromUnixTime::eval_fromtime_special,                          /* 81 */
  calc_ceil_floor,                                                    /* 82 */
  ObExprFuncPartKey::calc_partition_key,                              /* 83 */
  calc_round_expr_datetime1,                                          /* 84 */
  calc_round_expr_datetime2,                                          /* 85 */
  calc_round_expr_numeric2,                                           /* 86 */
  calc_round_expr_numeric1,                                           /* 87 */
  ObExprGreatest::calc_greatest,                                      /* 88 */
  ObExprHostIP::eval_host_ip,                                         /* 89 */
  calc_initcap_expr,                                                  /* 90 */
  ObExprTrim::eval_trim,                                              /* 91 */
  ObExprInsert::calc_expr_insert,                                     /* 92 */
  ObExprInt2ip::int2ip_varchar,                                       /* 93 */
  ObExprIntDiv::div_int_int,                                          /* 94 */
  ObExprIntDiv::div_int_uint,                                         /* 95 */
  ObExprIntDiv::div_uint_int,                                         /* 96 */
  ObExprIntDiv::div_uint_uint,                                        /* 97 */
  ObExprIntDiv::div_number,                                           /* 98 */
  ObExprIp2int::ip2int_varchar,                                       /* 99 */
  ObExprIs::calc_is_date_int_null,                                    /* 100 */
  ObExprIs::calc_is_null,                                             /* 101 */
  ObExprIs::int_is_true,                                              /* 102 */
  ObExprIs::int_is_false,                                             /* 103 */
  ObExprIs::float_is_true,                                            /* 104 */
  ObExprIs::float_is_false,                                           /* 105 */
  ObExprIs::double_is_true,                                           /* 106 */
  ObExprIs::double_is_false,                                          /* 107 */
  ObExprIs::number_is_true,                                           /* 108 */
  ObExprIs::number_is_false,                                          /* 109 */
  ObExprIsNot::calc_is_not_null,                                      /* 110 */
  ObExprIsNot::int_is_not_true,                                       /* 111 */
  ObExprIsNot::int_is_not_false,                                      /* 112 */
  ObExprIsNot::float_is_not_true,                                     /* 113 */
  ObExprIsNot::float_is_not_false,                                    /* 114 */
  ObExprIsNot::double_is_not_true,                                    /* 115 */
  ObExprIsNot::double_is_not_false,                                   /* 116 */
  ObExprIsNot::number_is_not_true,                                    /* 117 */
  ObExprIsNot::number_is_not_false,                                   /* 118 */
  ObExprLastExecId::eval_last_exec_id,                                /* 119 */
  ObExprLastTraceId::eval_last_trace_id,                              /* 120 */
  ObExprLeast::calc_least,                                            /* 121 */
  calc_left_expr,                                                     /* 122 */
  ObExprLength::calc_null,                                            /* 123 */
  ObExprLength::calc_oracle_mode,                                     /* 124 */
  ObExprLength::calc_mysql_mode,                                      /* 125 */
  ObExprLike::like_varchar,                                           /* 126 */
  calc_ln_expr_mysql,                                                 /* 127 */
  calc_ln_expr_oracle_double,                                         /* 128 */
  calc_ln_expr_oracle_number,                                         /* 129 */
  calc_log10_expr,                                                    /* 130 */
  calc_log2_expr,                                                     /* 131 */
  calc_log_expr_double,                                               /* 132 */
  calc_log_expr_number,                                               /* 133 */
  ObExprLower::calc_lower,                                            /* 134 */
  ObExprUpper::calc_upper,                                            /* 135 */
  ObExprMd5::calc_md5,                                                /* 136 */
  ObExprMinus::minus_datetime_datetime,                               /* 137 */
  ObExprMinus::minus_datetime_datetime_oracle,                        /* 138 */
  ObExprMinus::minus_datetime_intervalds,                             /* 139 */
  ObExprMinus::minus_datetime_intervalym,                             /* 140 */
  ObExprMinus::minus_datetime_number,                                 /* 141 */
  ObExprMinus::minus_double_double,                                   /* 142 */
  ObExprMinus::minus_float_float,                                     /* 143 */
  ObExprMinus::minus_intervalds_intervalds,                           /* 144 */
  ObExprMinus::minus_intervalym_intervalym,                           /* 145 */
  ObExprMinus::minus_int_int,                                         /* 146 */
  ObExprMinus::minus_int_uint,                                        /* 147 */
  ObExprMinus::minus_number_number,                                   /* 148 */
  ObExprMinus::minus_timestampltz_intervalym,                         /* 149 */
  ObExprMinus::minus_timestampnano_intervalym,                        /* 150 */
  ObExprMinus::minus_timestamp_timestamp,                             /* 151 */
  ObExprMinus::minus_timestamp_tiny_intervalds,                       /* 152 */
  ObExprMinus::minus_timestamptz_intervalds,                          /* 153 */
  ObExprMinus::minus_timestamptz_intervalym,                          /* 154 */
  ObExprMinus::minus_uint_int,                                        /* 155 */
  ObExprMinus::minus_uint_uint,                                       /* 156 */
  ObExprMod::mod_double,                                              /* 157 */
  ObExprMod::mod_float,                                               /* 158 */
  ObExprMod::mod_int_int,                                             /* 159 */
  ObExprMod::mod_int_uint,                                            /* 160 */
  ObExprMod::mod_number,                                              /* 161 */
  ObExprMod::mod_uint_int,                                            /* 162 */
  ObExprMod::mod_uint_uint,                                           /* 163 */
  ObExprMul::mul_double,                                              /* 164 */
  ObExprMul::mul_float,                                               /* 165 */
  ObExprMul::mul_intervalds_number,                                   /* 166 */
  ObExprMul::mul_intervalym_number,                                   /* 167 */
  ObExprMul::mul_int_int,                                             /* 168 */
  ObExprMul::mul_int_uint,                                            /* 169 */
  ObExprMul::mul_number,                                              /* 170 */
  ObExprMul::mul_number_intervalds,                                   /* 171 */
  ObExprMul::mul_number_intervalym,                                   /* 172 */
  ObExprMul::mul_uint_int,                                            /* 173 */
  ObExprMul::mul_uint_uint,                                           /* 174 */
  ObExprMySQLPort::eval_mysql_port,                                   /* 175 */
  NULL, // ObExprNeg::eval_tinyint is deleted                         /* 176 */
  calc_not_between_expr,                                              /* 177 */
  ObExprNot::eval_not,                                                /* 178 */
  ObExprNotExists::not_exists_eval,                                   /* 179 */
  ObExprNullSafeEqual::ns_equal_eval,                                 /* 180 */
  ObExprNullSafeEqual::row_ns_equal_eval,                             /* 181 */
  ObExprNvlUtil::calc_nvl_expr,                                       /* 182 */
  ObExprNvlUtil::calc_nvl_expr2,                                      /* 183 */
  ObSubQueryRelationalExpr::subquery_cmp_eval,                        /* 184 */
  ObBitwiseExprOperator::calc_result2_oracle,                         /* 185 */
  ObBitwiseExprOperator::calc_result2_mysql,                          /* 186 */
  ObRelationalExprOperator::row_eval,                                 /* 187 */
  calc_or_exprN,                                                      /* 188 */
  ObExprPow::calc_pow_expr,                                           /* 189 */
  calc_power_expr_oracle,                                             /* 190 */
  ObExprRegexp::eval_regexp,                                          /* 191 */
  ObExprRegexpCount::eval_regexp_count,                               /* 192 */
  ObExprRegexpInstr::eval_regexp_instr,                               /* 193 */
  ObExprRegexpLike::eval_regexp_like,                                 /* 194 */
  ObExprRegexpReplace::eval_regexp_replace,                           /* 195 */
  ObExprRegexpSubstr::eval_regexp_substr,                             /* 196 */
  ObExprRepeat::eval_repeat,                                          /* 197 */
  ObExprReplace::eval_replace,                                        /* 198 */
  ObExprFuncDump::eval_dump,                                          /* 199 */
  NULL,//ObExprFuncPartOldKey::eval_part_old_key is deleted           /* 200 */
  ObExprFuncPartHash::eval_part_hash,                                 /* 201 */
  NULL,//ObExprFuncAddrToPartId::eval_addr_to_part_id is deleted      /* 202 */
  ObExprAutoincNextval::eval_nextval,                                 /* 203 */
  ObExprFuncLnnvl::eval_lnnvl,                                        /* 204 */
  ObExprIsServingTenant::eval_is_serving_tenant,                      /* 205 */
  ObExprSysPrivilegeCheck::eval_sys_privilege_check,                  /* 206 */
  ObExprField::eval_field,                                            /* 207 */
  ObExprElt::eval_elt,                                                /* 208 */
  ObExprDesHexStr::eval_des_hex_str,                                  /* 209 */
  calc_right_expr,                                                    /* 210 */
  ObExprRowCount::eval_row_count,                                     /* 211 */
  ObExprRowNum::rownum_eval,                                          /* 212 */
  ObExprRpcPort::eval_rpc_port,                                       /* 213 */
  ObExprCot::calc_cot_expr,                                           /* 214 */
  calc_sign_expr,                                                     /* 215 */
  calc_sin_expr,                                                      /* 216 */
  calc_sinh_expr,                                                     /* 217 */
  ObExprSpace::eval_space,                                            /* 218 */
  calc_sqrt_expr_mysql,                                               /* 219 */
  calc_sqrt_expr_oracle_double,                                       /* 220 */
  calc_sqrt_expr_oracle_number,                                       /* 221 */
  calc_str_to_date_expr_time,                                         /* 222 */
  calc_str_to_date_expr_date,                                         /* 223 */
  calc_str_to_date_expr_datetime,                                     /* 224 */
  ObExprSubQueryRef::expr_eval,                                       /* 225 */
  ObExprSubstr::eval_substr,                                          /* 226 */
  ObExprSubstringIndex::eval_substring_index,                         /* 227 */
  calc_tan_expr,                                                      /* 228 */
  calc_tanh_expr,                                                     /* 229 */
  ObExprDayOfMonth::calc_dayofmonth,                                  /* 230 */
  ObExprDayOfWeek::calc_dayofweek,                                    /* 231 */
  ObExprDayOfYear::calc_dayofyear,                                    /* 232 */
  ObExprHour::calc_hour,                                              /* 233 */
  ObExprMicrosecond::calc_microsecond,                                /* 234 */
  ObExprMinute::calc_minute,                                          /* 235 */
  ObExprMonth::calc_month,                                            /* 236 */
  ObExprSecond::calc_second,                                          /* 237 */
  ObExprTime::calc_time,                                              /* 238 */
  ObExprYear::calc_year,                                              /* 239 */
  calc_timestampadd_expr,                                             /* 240 */
  ObExprLocalTimestamp::eval_localtimestamp,                          /* 241 */
  ObExprSysTimestamp::eval_systimestamp,                              /* 242 */
  calc_time_to_usec_expr,                                             /* 243 */
  ObExprDbtimezone::eval_db_timezone,                                 /* 244 */
  ObExprSessiontimezone::eval_session_timezone,                       /* 245 */
  calc_todays_expr,                                                   /* 246 */
  calc_to_temporal_expr,                                              /* 247 */
  calc_translate_expr,                                                /* 248 */
  ObExprTrim::eval_trim,                                              /* 249 */
  ObExprUid::eval_uid,                                                /* 250 */
  ObExprUnhex::eval_unhex,                                            /* 251 */
  calc_usec_to_time_expr,                                             /* 252 */
  ObExprUser::eval_user,                                              /* 253 */
  ObExprUuid::eval_uuid,                                              /* 254 */
  ObExprSysGuid::eval_sys_guid,                                       /* 255 */
  ObExprVersion::eval_version,                                        /* 256 */
  ObExprXor::eval_xor,                                                /* 257 */
  calc_charset_expr,                                                  /* 258 */
  calc_collation_expr,                                                /* 259 */
  calc_coercibility_expr,                                             /* 260 */
  calc_set_collation_expr,                                            /* 261 */
  calc_cmp_meta_expr,                                                 /* 262 */
  calc_trunc_expr_datetime,                                           /* 263 */
  calc_trunc_expr_numeric,                                            /* 264 */
  calc_truncate_expr,                                                 /* 265 */
  ObExprEstimateNdv::calc_estimate_ndv_expr,                          /* 266 */
  ObExprFindInSet::calc_find_in_set_expr,                             /* 267 */
  ObExprGetSysVar::calc_get_sys_val_expr,                             /* 268 */
  ObExprToNumber::calc_tonumber_expr,                                 /* 269 */
  ObExprToBinaryFloat::calc_to_binaryfloat_expr,                      /* 270 */
  ObExprToBinaryDouble::calc_to_binarydouble_expr,                    /* 271 */
  ObExprHextoraw::calc_hextoraw_expr,                                 /* 272 */
  ObExprRawtohex::calc_rawtohex_expr,                                 /* 273 */
  ObExprChr::calc_chr_expr,                                           /* 274 */
  ObExprIfNull::calc_ifnull_expr,                                     /* 275 */
  ObExprLengthb::calc_lengthb_expr,                                   /* 276 */
  ObExprAscii::calc_ascii_expr,                                       /* 277 */
  ObExprOrd::calc_ord_expr,                                           /* 278 */
  ObExprInstr::calc_mysql_instr_expr,                                 /* 279 */
  ObExprOracleInstr::calc_oracle_instr_expr,                          /* 280 */
  ObLocationExprOperator::calc_location_expr,                         /* 281 */
  ObExprCalcPartitionBase::calc_no_partition_location,                /* 282 */
  ObExprCalcPartitionBase::calc_partition_level_one,                  /* 283 */
  ObExprCalcPartitionBase::calc_partition_level_two,                  /* 284 */
  ObExprPrior::calc_prior_expr,                                       /* 285 */
  ObExprSeqNextval::calc_sequence_nextval,                            /* 286 */
  calc_reverse_expr,                                                  /* 287 */
  calc_instrb_expr,                                                   /* 288 */
  ObExprConcatWs::calc_concat_ws_expr,                                /* 289 */
  ObExprMakeSet::calc_make_set_expr,                                  /* 290 */
  ObExprInterval::calc_interval_expr,                                 /* 291 */
  ObExprSysOpOpnsize::calc_sys_op_opnsize_expr,                       /* 292 */
  ObExprQuote::calc_quote_expr,                                       /* 293 */
  ObExprDateAdd::calc_date_add,                                       /* 294 */
  ObExprDateSub::calc_date_sub,                                       /* 295 */
  ObExprAddMonths::calc_add_months,                                   /* 296 */
  ObExprLastDay::calc_last_day,                                       /* 297 */
  ObExprNextDay::calc_next_day,                                       /* 298 */
  ObExprFromDays::calc_fromdays,                                      /* 299 */
  ObExprPeriodDiff::calc_perioddiff,                                  /* 300 */
  ObExprTimeDiff::calc_timediff,                                      /* 301 */
  ObExprTimestampNvl::calc_timestampnvl,                              /* 302 */
  ObExprToYMInterval::calc_to_yminterval,                             /* 303 */
  ObExprToDSInterval::calc_to_dsinterval,                             /* 304 */
  ObExprNumToYMInterval::calc_num_to_yminterval,                      /* 305 */
  ObExprNumToDSInterval::calc_num_to_dsinterval,                      /* 306 */
  ObExprWeekOfYear::calc_weekofyear,                                  /* 307 */
  ObExprWeekDay::calc_weekday,                                        /* 308 */
  ObExprYearWeek::calc_yearweek,                                      /* 309 */
  ObExprWeek::calc_week,                                              /* 310 */
  ObExprInOrNotIn::eval_in_with_row,                                  /* 311 */
  ObExprInOrNotIn::eval_in_without_row,                               /* 312 */
  ObExprInOrNotIn::eval_in_with_row_fallback,                         /* 313 */
  ObExprInOrNotIn::eval_in_without_row_fallback,                      /* 314 */
  ObExprInOrNotIn::eval_in_with_subquery,                             /* 315 */
  ObExprFunDefault::calc_default_expr,                                /* 316 */
  ObExprSubstrb::calc_substrb_expr,                                   /* 317 */
  ObExprRemainder::calc_remainder_expr,                               /* 318 */
  ObExprRand::calc_random_expr_const_seed,                            /* 319 */
  ObExprRand::calc_random_expr_nonconst_seed,                         /* 320 */
  ObExprWidthBucket::calc_width_bucket_expr,                          /* 321 */
  ObExprSysExtractUtc::calc_sys_extract_utc,                          /* 322 */
  ObExprToClob::calc_to_clob_expr,                                    /* 323 */
  ObExprUserEnv::calc_user_env_expr,                                  /* 324 */
  ObExprVsize::calc_vsize_expr,                                       /* 325 */
  ObExprOracleLpad::calc_oracle_lpad_expr,                            /* 326 */
  ObExprOracleRpad::calc_oracle_rpad_expr,                            /* 327 */
  ObExprLpad::calc_mysql_lpad_expr,                                   /* 328 */
  ObExprRpad::calc_mysql_rpad_expr,                                   /* 329 */
  ObExprPad::calc_pad_expr,                                           /* 330 */
  ObExprFunValues::eval_values,                                       /* 331 */
  ObExprConnectByRoot::eval_connect_by_root,                          /* 332 */
  ObExprOracleToChar::eval_oracle_to_char,                            /* 333 */
  ObExprPartId::eval_part_id,                                         /* 334 */
  ObExprHex::eval_hex,                                                /* 335 */
  ObExprShadowUKProject::shadow_uk_project,                           /* 336 */
  ObExprCharLength::eval_char_length,                                 /* 337 */
  ObExprUnixTimestamp::eval_unix_timestamp,                           /* 338 */
  ObExprAesDecrypt::eval_aes_decrypt,                                 /* 339 */
  ObExprAesEncrypt::eval_aes_encrypt,                                 /* 340 */
  ObExprCase::calc_case_expr,                                         /* 341 */
  ObExprOracleDecode::eval_decode,                                    /* 342 */
  ObExprRemoveConst::eval_remove_const,                               /* 343 */
  ObExprSleep::eval_sleep,                                            /* 344 */
  ObExprSysContext::eval_sys_context,                                 /* 345 */
  ObExprGetPackageVar::eval_get_package_var,                          /* 346 */
  ObExprTimeStampDiff::eval_timestamp_diff,                           /* 347 */
  ObExprFromTz::eval_from_tz,                                         /* 348 */
  ObExprTzOffset::eval_tz_offset,                                     /* 349 */
  ObExprOrahash::eval_orahash,                                        /* 350 */
  ObExprGetUserVar::eval_get_user_var,                                /* 351 */
  NULL, //ObExprUtil::eval_generated_column,                          /* 352 */
  NULL, //ObExprCalcPartitionBase::calc_opt_route_hash_one            /* 353 */
  calc_convert_expr,                                                  /* 354 */
  ObExprSetToStr::calc_to_str_expr,                                   /* 355 */
  ObExprEnumToStr::calc_to_str_expr,                                  /* 356 */
  ObExprSetToInnerType::calc_to_inner_expr,                           /* 357 */
  ObExprEnumToInnerType::calc_to_inner_expr,                          /* 358 */
  ObExprDateFormat::calc_date_format_invalid,                         /* 359 */
  ObExprDateFormat::calc_date_format,                                 /* 360 */
  ObExprCalcURowID::calc_urowid,                                      /* 361 */
  NULL,//ObExprFuncPartOldHash::eval_old_part_hash is deleted         /* 362 */
  NULL,//ObExprFuncPartNewKey::calc_new_partition_key is deleted      /* 363 */
  ObExprUtil::eval_stack_overflow_check,                              /* 364 */
  ObExprSysConnectByPath::calc_sys_path,                              /* 365 */
  ObExprLastInsertID::eval_last_insert_id,                            /* 366 */
  ObExprPartIdPseudoColumn::eval_part_id,                             /* 367 */
  ObExprNullif::eval_nullif,                                          /* 368 */
  ObExprOracleNullif::eval_nullif,                                    /* 369 */
  ObExprUserCanAccessObj::eval_user_can_access_obj,                   /* 370 */
  ObExprEmptyClob::eval_empty_clob,                                   /* 371 */
  ObExprEmptyBlob::eval_empty_blob,                                   /* 372 */
  ObExprRadians::calc_radians_expr,                                   /* 373 */
  ObExprMakeTime::eval_maketime,                                      /* 374 */
  ObExprMonthName::calc_month_name,                                   /* 375 */
  ObExprToBlob::eval_to_blob,                                         /* 376 */
  ObExprJoinFilter::eval_bloom_filter,                                /* 377 */
  ObExprNlsLower::calc_lower,                                         /* 378 */
  ObExprNlsUpper::calc_upper,                                         /* 379 */
  ObExprToOutfileRow::to_outfile_str,                                 /* 380 */
  ObExprIs::calc_is_infinite,                                         /* 381 */
  ObExprIs::calc_is_nan,                                              /* 382 */
  ObExprIsNot::calc_is_not_infinite,                                  /* 383 */
  ObExprIsNot::calc_is_not_nan,                                       /* 384 */
  ObExprOracleNullif::eval_nullif_not_null,                           /* 385 */
  ObExprNaNvl::eval_nanvl,                                            /* 386 */
  ObExprFormat::calc_format_expr,                                     /* 387 */
  calc_translate_using_expr,                                          /* 388 */
  ObExprQuarter::calc_quater,                                         /* 389 */
  ObExprBitLength::calc_bit_length,                                   /* 390 */
  ObExprConvertOracle::calc_convert_oracle_expr,                      /* 391 */
  ObExprUnistr::calc_unistr_expr,                                     /* 392 */
  ObExprAsciistr::calc_asciistr_expr,                                 /* 393 */
  ObExprAtTimeZone::eval_at_time_zone,                                /* 394 */
  ObExprAtLocal::eval_at_local,                                       /* 395 */
  ObExprToSingleByte::calc_to_single_byte,                            /* 396 */
  ObExprToMultiByte::calc_to_multi_byte,                              /* 397 */
  ObExprDllUdf::eval_dll_udf,                                         /* 398 */
  ObExprRawtonhex::calc_rawtonhex_expr,                               /* 399 */
  ObExprPi::eval_pi,                                                  /* 400 */
  ObExprOutputPack::eval_output_pack,                                 /* 401 */
  ObExprReturningLob::eval_lob,                                       /* 402 */
  eval_question_mark_func,                                            /* 403 */
  ObExprUtcTime::eval_utc_time,                                       /* 404 */
  ObExprUtcDate::eval_utc_date,                                       /* 405 */
  ObExprGetFormat::calc_get_format,                                   /* 406 */
  ObExprCollectionConstruct::eval_collection_construct,               /* 407 */
  ObExprObjAccess::eval_obj_access,                                   /* 408 */
  ObExprTimeFormat::calc_time_format,                                 /* 409 */
  ObExprMakedate::calc_makedate,                                      /* 410 */
  ObExprPeriodAdd::calc_periodadd,                                    /* 411 */
  ObExprPLAssocIndex::eval_assoc_idx,                                 /* 412 */
  ObExprUDF::eval_udf,                                                /* 413 */
  ObExprObjectConstruct::eval_object_construct,                       /* 414 */
  ObRelationalExprOperator::eval_pl_udt_compare,                      /* 415 */
  ObExprInOrNotIn::eval_pl_udt_in,                                    /* 416 */
  ObExprPLGetCursorAttr::calc_pl_get_cursor_attr,                     /* 417 */
  ObExprPLIntegerChecker::calc_pl_integer_checker,                    /* 418 */
  ObExprGetSubprogramVar::calc_get_subprogram_var,                    /* 419 */
  ObExprPLSQLCodeSQLErrm::eval_pl_sql_code_errm,                      /* 420 */
  ObExprMultiSet::eval_multiset,                                      /* 421 */
  ObExprCardinality::eval_card,                                       /* 422 */
  ObExprCollPred::eval_coll_pred,                                     /* 423 */
  ObExprStmtId::eval_stmt_id,                                         /* 424 */
  NULL,//ObExprWordSegment::eval_word_segment is deleted              /* 425 */
  ObExprPLSeqNextval::eval_pl_seq_next_val,                           /* 426 */
  ObExprSet::calc_set,                                                /* 427 */
  ObExprWrapperInner::eval_wrapper_inner,                             /* 428 */
  ObExprObVersion::eval_version,                                      /* 429 */
  ObExprOLSLabelCmpLE::eval_cmple,                                    /* 430 */
  ObExprOLSLabelCheck::eval_label_check,                              /* 431 */
  ObExprOLSCharToLabel::eval_char_to_label,                           /* 432 */
  ObExprOLSLabelToChar::eval_label_to_char,                           /* 433 */
  ObExprPLSQLVariable::eval_plsql_variable,                           /* 434 */
  ObExprDegrees::calc_degrees_expr,                                   /* 435 */
  ObExprAnyValue::eval_any_value,                                     /* 436 */
  ObExprIs::calc_collection_is_null,                                  /* 437 */
  ObExprIsNot::calc_collection_is_not_null,                           /* 438 */
  ObExprOLSSessionRowLabel::eval_row_label,                           /* 439 */
  ObExprOLSSessionLabel::eval_label,                                  /* 440 */
  ObExprTimestamp::calc_timestamp1,                                   /* 441 */
  ObExprTimestamp::calc_timestamp2,                                   /* 442 */
  ObExprValidatePasswordStrength::eval_password_strength,             /* 443 */
  ObExprSoundex::eval_soundex,                                        /* 444 */
  ObExprRowIDToChar::eval_rowid_to_char,                              /* 445 */
  ObExprRowIDToNChar::eval_rowid_to_nchar,                            /* 446 */
  ObExprCharToRowID::eval_char_to_rowid,                              /* 447 */
  ObExprUuidShort::eval_uuid_short,                                   /* 448 */
  ObExprBenchmark::eval_benchmark,                                    /* 449 */
  ObExprExportSet::eval_export_set,                                   /* 450 */
  ObExprInet6Aton::calc_inet6_aton,                                   /* 451 */
  ObExprIsIpv4::calc_is_ipv4,                                         /* 452 */
  ObExprIsIpv6::calc_is_ipv6,                                         /* 453 */
  ObExprIsIpv4Mapped::calc_is_ipv4_mapped,                            /* 454 */
  ObExprIsIpv4Compat::calc_is_ipv4_compat,                            /* 455 */
  ObExprInetAton::calc_inet_aton,                                     /* 456 */
  ObExprInet6Ntoa::calc_inet6_ntoa,                                   /* 457 */
  ObExprWeightString::eval_weight_string,                             /* 458 */
  ObExprConvertTZ::eval_convert_tz,                                   /* 459 */
  ObExprCrc32::calc_crc32_expr,                                       /* 460 */
  ObExprDmlEvent::calc_dml_event,                                     /* 461 */
  ObExprToBase64::eval_to_base64,                                     /* 462 */
  ObExprFromBase64::eval_from_base64,                                 /* 463 */
  ObExprRandomBytes::generate_random_bytes,                           /* 464 */
  ObExprOpSubQueryInPl::eval_subquery,                                /* 465 */
  ObExprEncodeSortkey::eval_encode_sortkey,                           /* 466 */
  ObExprNLSSort::eval_nlssort,                                        /* 467 */
  eval_assign_question_mark_func,                                     /* 468 */
  ObExprEncodeSortkey::eval_encode_sortkey,                           /* 469 */
  ObExprJsonObject::eval_json_object,                                 /* 470 */
  ObExprJsonExtract::eval_json_extract,                               /* 471 */
  ObExprJsonContains::eval_json_contains,                             /* 472 */
  ObExprJsonContainsPath::eval_json_contains_path,                    /* 473 */
  ObExprJsonDepth::eval_json_depth,                                   /* 474 */
  ObExprJsonKeys::eval_json_keys,                                     /* 475 */
  ObExprJsonArray::eval_json_array,                                   /* 476 */
  ObExprJsonQuote::eval_json_quote,                                   /* 477 */
  ObExprJsonUnquote::eval_json_unquote,                               /* 478 */
  ObExprJsonOverlaps::eval_json_overlaps,                             /* 479 */
  ObExprJsonRemove::eval_json_remove,                                 /* 480 */
  ObExprJsonSearch::eval_json_search,                                 /* 481 */
  ObExprJsonValid::eval_json_valid,                                   /* 482 */
  ObExprJsonArrayAppend::eval_json_array_append,                      /* 483 */
  ObExprJsonArrayInsert::eval_json_array_insert,                      /* 484 */
  ObExprJsonReplace::eval_json_replace,                               /* 485 */
  ObExprJsonType::eval_json_type,                                     /* 486 */
  ObExprJsonLength::eval_json_length,                                 /* 487 */
  ObExprJsonInsert::eval_json_insert,                                 /* 488 */
  ObExprJsonStorageSize::eval_json_storage_size,                      /* 489 */
  ObExprJsonStorageFree::eval_json_storage_free,                      /* 490 */
  ObExprJsonMergePreserve::eval_json_merge_preserve,                  /* 491 */
  ObExprJsonMerge::eval_json_merge_preserve,                          /* 492 */
  ObExprJsonMergePatch::eval_json_merge_patch,                        /* 493 */
  ObExprJsonPretty::eval_json_pretty,                                 /* 494 */
  ObExprJsonSet::eval_json_set,                                       /* 495 */
  ObExprJsonValue::eval_json_value,                                   /* 496 */
  ObExprJsonMemberOf::eval_json_member_of,                            /* 497 */
  ObExprJsonExtract::eval_json_extract_null,                          /* 498 */
  ObExprSha::eval_sha,                                                /* 499 */
  ObExprSha2::eval_sha2,                                              /* 500 */
  ObExprCompress::eval_compress,                                      /* 501 */
  ObExprUncompress::eval_uncompress,                                  /* 502 */
  ObExprUncompressedLength::eval_uncompressed_length,                 /* 503 */
  ObExprStatementDigest::eval_statement_digest,                       /* 504 */
  ObExprStatementDigestText::eval_statement_digest_text,              /* 505 */
  ObExprHash::calc_hash_value_expr,                                   /* 506 */
  calc_timestamp_to_scn_expr,                                         /* 507 */
  calc_scn_to_timestamp_expr,                                         /* 508 */
#if defined(ENABLE_DEBUG_LOG) || !defined(NDEBUG)
  ObExprErrno::eval_errno,                                            /* 509 */
#else
  NULL,                                                               /* 509 */
#endif
  ObExprDayName::calc_dayname,                                        /* 510 */
  ObExprNullif::eval_nullif_enumset,                                  /* 511 */
  ObExprSTIntersects::eval_st_intersects,                             /* 512 */
  ObExprSTX::eval_st_x,                                               /* 513 */
  ObExprSTY::eval_st_y,                                               /* 514 */
  ObExprSTLatitude::eval_st_latitude,                                 /* 515 */
  ObExprSTLongitude::eval_st_longitude,                               /* 516 */
  ObExprSTTransform::eval_st_transform,                               /* 517 */
  ObExprPoint::eval_point,                                            /* 518 */
  ObExprLineString::eval_linestring,                                  /* 519 */
  ObExprMultiPoint::eval_multipoint,                                  /* 520 */
  ObExprMultiLineString::eval_multilinestring,                        /* 521 */
  ObExprPolygon::eval_polygon,                                        /* 522 */
  ObExprMultiPolygon::eval_multipolygon,                              /* 523 */
  ObExprGeomCollection::eval_geomcollection,                          /* 524 */
  ObExprPrivSTCovers::eval_st_covers,                                 /* 525 */
  ObExprPrivSTBestsrid::eval_st_bestsrid,                             /* 526 */
  ObExprSTAsText::eval_st_astext,                                     /* 527 */
  ObExprSTAsWkt::eval_st_astext,                                      /* 528 */
  ObExprSTBufferStrategy::eval_st_buffer_strategy,                    /* 529 */
  ObExprSTBuffer::eval_st_buffer,                                     /* 530 */
  ObExprSpatialCellid::eval_spatial_cellid,                           /* 531 */
  ObExprSpatialMbr::eval_spatial_mbr,                                 /* 532 */
  ObExprPrivSTGeomFromEWKB::eval_st_geomfromewkb,                     /* 533 */
  ObExprSTGeomFromWKB::eval_st_geomfromwkb,                           /* 534 */
  ObExprSTGeometryFromWKB::eval_st_geometryfromwkb,                   /* 535 */
  ObExprPrivSTGeomFromEwkt::eval_st_geomfromewkt,                     /* 536 */
  ObExprPrivSTAsEwkt::eval_priv_st_asewkt,                            /* 537 */
  ObExprGeometryCollection::eval_geometrycollection,                  /* 538 */
  ObExprSTSRID::eval_st_srid,                                         /* 539 */
  ObExprSTDistance::eval_st_distance,                                 /* 540 */
  ObExprPrivSTSetSRID::eval_priv_st_setsrid,                          /* 541 */
  ObExprSTGeometryFromText::eval_st_geometryfromtext,                 /* 542 */
  ObExprPrivSTPoint::eval_priv_st_point,                              /* 543 */
  ObExprPrivSTGeogFromText::eval_priv_st_geogfromtext,                /* 544 */
  ObExprPrivSTGeographyFromText::eval_priv_st_geographyfromtext,      /* 545 */
  ObExprSTIsValid::eval_st_isvalid,                                   /* 546 */
  ObExprPrivSTBuffer::eval_priv_st_buffer,                            /* 547 */
  ObExprSTAsWkb::eval_st_aswkb,                                       /* 548 */
  ObExprStPrivAsEwkb::eval_priv_st_as_ewkb,                           /* 549 */
  ObExprSTAsBinary::eval_st_asbinary,                                 /* 550 */
  ObExprSTDistanceSphere::eval_st_distance_sphere,                    /* 551 */
  ObExprPrivSTDWithin::eval_st_dwithin,                               /* 552 */
  ObExprSTContains::eval_st_contains,                                 /* 553 */
  ObExprSTWithin::eval_st_within,                                     /* 554 */
  ObExprPrivSTTransform::eval_priv_st_transform,                      /* 555 */
  ObExprSTGeomFromText::eval_st_geomfromtext,                         /* 556 */
  ObExprSTArea::eval_st_area,                                         /* 557 */
  ObExprCurrentUserPriv::eval_current_user_priv,                      /* 558 */
  ObExprSqlModeConvert::sql_mode_convert,                             /* 559 */
  ObExprJsonValue::eval_ora_json_value,                               /* 560 */
  ObExprIsJson::eval_is_json,                                         /* 561 */
  ObExprJsonEqual::eval_json_equal,                                   /* 562 */
  ObExprJsonQuery::eval_json_query,                                   /* 563 */
  ObExprJsonMergePatch::eval_ora_json_merge_patch,                    /* 564 */
  ObExprJsonExists::eval_json_exists,                                 /* 565 */
  ObExprJsonArray::eval_ora_json_array,                               /* 566 */
  ObExprJsonObject::eval_ora_json_object,                             /* 567 */
  ObExprTreat::eval_treat,                                            /* 568 */
  ObExprUuid2bin::uuid2bin,                                           /* 569 */
  ObExprIsUuid::is_uuid,                                              /* 570 */
  ObExprBin2uuid::bin2uuid,                                           /* 571 */
  ObExprNameConst::eval_name_const,                                   /* 572 */
  ObExprFormatBytes::eval_format_bytes,                               /* 573 */
  ObExprFormatPicoTime::eval_format_pico_time,                        /* 574 */
  ObExprDesEncrypt::eval_des_encrypt_with_key,                        /* 575 */
  ObExprDesEncrypt::eval_des_encrypt_with_default,                    /* 576 */
  ObExprDesDecrypt::eval_des_decrypt,                                 /* 577 */
  ObExprEncrypt::eval_encrypt,                                        /* 578 */
  ObExprEncode::eval_encode,                                          /* 579 */
  ObExprDecode::eval_decode,                                          /* 580 */
  ObExprICUVersion::eval_version,                                     /* 581 */
  ObExprCast::eval_cast_multiset,                                     /* 582 */
  ObExprGeneratorFunc::eval_next_value,                               /* 583 */
  ObExprZipf::eval_next_value,                                        /* 584 */
  ObExprNormal::eval_next_value,                                      /* 585 */
  ObExprUniform::eval_next_int_value,                                 /* 586 */
  ObExprUniform::eval_next_real_value,                                /* 587 */
  ObExprUniform::eval_next_number_value,                              /* 588 */
  ObExprRandom::calc_random_expr_const_seed,                          /* 589 */
  ObExprRandom::calc_random_expr_nonconst_seed,                       /* 590 */
  ObExprRandstr::calc_random_str,                                     /* 591 */
  ObExprNlsInitCap::calc_nls_initcap_expr,                            /* 592 */
  ObExprPrefixPattern::eval_prefix_pattern,                           /* 593 */
  ObExprSysMakeXML::eval_sys_makexml,                                 /* 594 */
  ObExprPrivXmlBinary::eval_priv_xml_binary,                          /* 595 */
  ObExprXmlparse::eval_xmlparse,                                      /* 596 */
  ObExprXmlElement::eval_xml_element,                                 /* 597 */
  ObExprXmlAttributes::eval_xml_attributes,                           /* 598 */
  ObExprExtractValue::eval_extract_value,                             /* 599 */
  ObExprExtractXml::eval_extract_xml,                                 /* 600 */
  ObExprXmlSerialize::eval_xml_serialize,                             /* 601 */
  ObExprXmlcast::eval_xmlcast,                                        /* 602 */
  ObExprUpdateXml::eval_update_xml,                                   /* 603 */
  ObExprJoinFilter::eval_range_filter,                                /* 604 */
  ObExprJoinFilter::eval_in_filter,                                   /* 605 */
  ObExprCurrentScn::eval_current_scn,                                 /* 606 */
  ObExprTempTableSSID::calc_temp_table_ssid,                          /* 607 */
  ObExprAlignDate4Cmp::eval_align_date4cmp,                            /* 608 */
};

static ObExpr::EvalBatchFunc g_expr_eval_batch_functions[] = {
  expr_default_eval_batch_func,                                       /* 0 */
  ObExprUtil::eval_batch_stack_overflow_check,                        /* 1 */
  ObExprAdd::add_datetime_datetime_batch,                             /* 2 */
  ObExprAdd::add_datetime_intervalds_batch,                           /* 3 */
  ObExprAdd::add_datetime_intervalym_batch,                           /* 4 */
  ObExprAdd::add_datetime_number_batch,                               /* 5 */
  ObExprAdd::add_double_double_batch,                                 /* 6 */
  ObExprAdd::add_float_float_batch,                                   /* 7 */
  ObExprAdd::add_intervalds_datetime_batch,                           /* 8 */
  ObExprAdd::add_intervalds_intervalds_batch,                         /* 9 */
  ObExprAdd::add_intervalds_timestamp_tiny_batch,                     /* 10 */
  ObExprAdd::add_intervalds_timestamptz_batch,                        /* 11 */
  ObExprAdd::add_intervalym_datetime_batch,                           /* 12 */
  ObExprAdd::add_intervalym_intervalym_batch,                         /* 13 */
  ObExprAdd::add_intervalym_timestampltz_batch,                       /* 14 */
  ObExprAdd::add_intervalym_timestampnano_batch,                      /* 15 */
  ObExprAdd::add_intervalym_timestamptz_batch,                        /* 16 */
  ObExprAdd::add_int_int_batch,                                       /* 17 */
  ObExprAdd::add_int_uint_batch,                                      /* 18 */
  ObExprAdd::add_number_datetime_batch,                               /* 19 */
  ObExprAdd::add_number_number_batch,                                 /* 20 */
  ObExprAdd::add_timestampltz_intervalym_batch,                       /* 21 */
  ObExprAdd::add_timestampnano_intervalym_batch,                      /* 22 */
  ObExprAdd::add_timestamp_tiny_intervalds_batch,                     /* 23 */
  ObExprAdd::add_timestamptz_intervalds_batch,                        /* 24 */
  ObExprAdd::add_timestamptz_intervalym_batch,                        /* 25 */
  ObExprAdd::add_uint_int_batch,                                      /* 26 */
  ObExprAdd::add_uint_uint_batch,                                     /* 27 */
  ObExprMinus::minus_datetime_datetime_batch,                         /* 28 */
  ObExprMinus::minus_datetime_datetime_oracle_batch,                  /* 29 */
  ObExprMinus::minus_datetime_intervalds_batch,                       /* 30 */
  ObExprMinus::minus_datetime_intervalym_batch,                       /* 31 */
  ObExprMinus::minus_datetime_number_batch,                           /* 32 */
  ObExprMinus::minus_double_double_batch,                             /* 33 */
  ObExprMinus::minus_float_float_batch,                               /* 34 */
  ObExprMinus::minus_intervalds_intervalds_batch,                     /* 35 */
  ObExprMinus::minus_intervalym_intervalym_batch,                     /* 36 */
  ObExprMinus::minus_int_int_batch,                                   /* 37 */
  ObExprMinus::minus_int_uint_batch,                                  /* 38 */
  ObExprMinus::minus_number_number_batch,                             /* 39 */
  ObExprMinus::minus_timestampltz_intervalym_batch,                   /* 40 */
  ObExprMinus::minus_timestampnano_intervalym_batch,                  /* 41 */
  ObExprMinus::minus_timestamp_timestamp_batch,                       /* 42 */
  ObExprMinus::minus_timestamp_tiny_intervalds_batch,                 /* 43 */
  ObExprMinus::minus_timestamptz_intervalds_batch,                    /* 44 */
  ObExprMinus::minus_timestamptz_intervalym_batch,                    /* 45 */
  ObExprMinus::minus_uint_int_batch,                                  /* 46 */
  ObExprMinus::minus_uint_uint_batch,                                 /* 47 */
  ObExprMul::mul_double_batch,                                        /* 48 */
  ObExprMul::mul_float_batch,                                         /* 49 */
  ObExprMul::mul_intervalds_number_batch,                             /* 50 */
  ObExprMul::mul_intervalym_number_batch,                             /* 51 */
  ObExprMul::mul_int_int_batch,                                       /* 52 */
  ObExprMul::mul_int_uint_batch,                                      /* 53 */
  ObExprMul::mul_number_batch,                                        /* 54 */
  ObExprMul::mul_number_intervalds_batch,                             /* 55 */
  ObExprMul::mul_number_intervalym_batch,                             /* 56 */
  ObExprMul::mul_uint_int_batch,                                      /* 57 */
  ObExprMul::mul_uint_uint_batch,                                     /* 58 */
  ObExprDiv::div_float_batch,                                         /* 59 */
  ObExprDiv::div_double_batch,                                        /* 60 */
  ObExprDiv::div_number_batch,                                        /* 61 */
  ObExprDiv::div_intervalym_number_batch,                             /* 62 */
  ObExprDiv::div_intervalds_number_batch,                             /* 63 */
  ObExprMakeTime::eval_batch_maketime,                                /* 64 */
  ObExprAnd::eval_and_batch_exprN,                                    /* 65 */
  ObExprOr::eval_or_batch_exprN,                                      /* 66 */
  ObExprFuncPartKey::calc_partition_key_batch,                        /* 67 */
  NULL,//ObExprFuncPartNewKey::calc_new_partition_key_batch is deleted/* 68 */
  ObExprInOrNotIn::eval_batch_in_without_row_fallback,                /* 69 */
  ObExprInOrNotIn::eval_batch_in_without_row,                         /* 70 */
  ObExprLike::eval_like_expr_batch_only_text_vectorized,              /* 71 */
  ObExprCase::eval_case_batch,                                        /* 72 */
  ObExprSubstr::eval_substr_batch,                                    /* 73 */
  ObExprJoinFilter::eval_bloom_filter_batch,                          /* 74 */
  ObExprToNumber::calc_tonumber_expr_batch,                           /* 75 */
  ObExprToCharCommon::eval_oracle_to_char_batch,                      /* 76 */
  ObExprExtract::calc_extract_oracle_batch,                           /* 77 */
  ObExprExtract::calc_extract_mysql_batch,                            /* 78 */
  cast_eval_arg_batch,                                                /* 79 */
  ObExprOutputPack::eval_output_pack_batch,                           /* 80 */
  eval_batch_ceil_floor,                                              /* 81 */
  ObExprFuncRound::calc_round_expr_numeric1_batch,                    /* 82 */
  ObExprFuncRound::calc_round_expr_numeric2_batch,                    /* 83 */
  ObExprFuncRound::calc_round_expr_datetime1_batch,                   /* 84 */
  ObExprFuncRound::calc_round_expr_datetime2_batch,                   /* 85 */
  ObExprNot::eval_not_batch,                                          /* 86 */
  NULL,    // ObExprCalcPartitionBase::calc_opt_route_hash_one_vec,   /* 87 */
  ObExprBenchmark::eval_benchmark_batch,                              /* 88 */
  ObExprToBase64::eval_to_base64_batch,                               /* 89 */
  ObExprFromBase64::eval_from_base64_batch,                           /* 90 */
  ObExprEncodeSortkey::eval_encode_sortkey_batch,                     /* 91 */
  ObExprHash::calc_hash_value_expr_batch,                             /* 92 */
  ObExprSubstringIndex::eval_substring_index_batch,                   /* 93 */
  ObExprInstrb::calc_instrb_expr_batch,                               /* 94 */
  ObExprNaNvl::eval_nanvl_batch,                                      /* 95 */
  ObExprNvlUtil::calc_nvl_expr_batch,                                 /* 96 */
  ObExprNvl2Oracle::calc_nvl2_oracle_expr_batch,                      /* 97 */
  ObExprUuid2bin::uuid2bin_batch,                                     /* 98 */
  ObExprIsUuid::is_uuid_batch,                                        /* 99 */
  ObExprBin2uuid::bin2uuid_batch,                                     /* 100 */
  ObExprFormatBytes::eval_format_bytes_batch,                         /* 101 */
  ObExprFormatPicoTime::eval_format_pico_time_batch,                  /* 102 */
  ObExprDesEncrypt::eval_des_encrypt_batch_with_default,              /* 103 */
  ObExprDesEncrypt::eval_des_encrypt_batch_with_key,                  /* 104 */
  ObExprDesDecrypt::eval_des_decrypt_batch,                           /* 105 */
  ObExprEncrypt::eval_encrypt_batch,                                  /* 106 */
  ObExprEncode::eval_encode_batch,                                    /* 107 */
  ObExprDecode::eval_decode_batch,                                    /* 108 */
  ObExprCoalesce::calc_batch_coalesce_expr,                           /* 109 */
  ObExprIsNot::calc_batch_is_not_null,                                /* 110 */
  ObExprNlsInitCap::calc_nls_initcap_batch,                           /* 111 */
  ObExprJoinFilter::eval_range_filter_batch,                          /* 112 */
  ObExprJoinFilter::eval_in_filter_batch,                             /* 113 */
  calc_sqrt_expr_mysql_in_batch,                                      /* 114 */
  calc_sqrt_expr_oracle_double_in_batch,                              /* 115 */
  calc_sqrt_expr_oracle_number_in_batch                               /* 116 */
};

REG_SER_FUNC_ARRAY(OB_SFA_SQL_EXPR_EVAL,
                   g_expr_eval_functions,
                   ARRAYSIZEOF(g_expr_eval_functions));

REG_SER_FUNC_ARRAY(OB_SFA_SQL_EXPR_EVAL_BATCH,
                   g_expr_eval_batch_functions,
                   ARRAYSIZEOF(g_expr_eval_batch_functions));

} // end namespace sql
} // end namespace oceanbase
