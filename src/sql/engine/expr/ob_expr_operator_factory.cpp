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
#include "sql/engine/expr/ob_expr_cot.h"
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
#include "sql/engine/expr/ob_expr_current_user_priv.h"
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
#include "sql/engine/expr/ob_expr_inet.h"
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
#include "sql/engine/expr/ob_expr_mid.h"
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
#include "sql/engine/expr/ob_expr_makedate.h"
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
#include "sql/engine/expr/ob_expr_export_set.h"
#include "sql/engine/expr/ob_expr_replace.h"
#include "sql/engine/expr/ob_expr_translate.h"
#include "sql/engine/expr/ob_expr_func_part_hash.h"
#include "sql/engine/expr/ob_expr_func_partition_key.h"
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
#include "sql/engine/expr/ob_expr_crc32.h"
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
#include "sql/engine/expr/ob_expr_ascii.h"
#include "sql/engine/expr/ob_expr_truncate.h"
#include "sql/engine/expr/ob_expr_bit_count.h"
#include "sql/engine/expr/ob_expr_make_set.h"
#include "sql/engine/expr/ob_expr_find_in_set.h"
#include "sql/engine/expr/ob_expr_estimate_ndv.h"
#include "sql/engine/expr/ob_expr_left.h"
#include "sql/engine/expr/ob_expr_space.h"
#include "sql/engine/expr/ob_expr_rand.h"
#include "sql/engine/expr/ob_expr_randstr.h"
#include "sql/engine/expr/ob_expr_random.h"
#include "sql/engine/expr/ob_expr_generator_func.h"
#include "sql/engine/expr/ob_expr_zipf.h"
#include "sql/engine/expr/ob_expr_normal.h"
#include "sql/engine/expr/ob_expr_uniform.h"
#include "sql/engine/expr/ob_expr_obj_access.h"
#include "sql/engine/expr/ob_expr_rownum.h"
#include "sql/engine/expr/ob_expr_type_to_str.h"
#include "sql/engine/expr/ob_expr_connect_by_root.h"
#include "sql/engine/expr/ob_expr_sys_connect_by_path.h"
#include "sql/engine/expr/ob_expr_sys_op_opnsize.h"
#include "sql/engine/expr/ob_expr_get_package_var.h"
#include "sql/engine/expr/ob_expr_get_subprogram_var.h"
#include "sql/engine/expr/ob_expr_shadow_uk_project.h"
#include "sql/engine/expr/ob_expr_time_format.h"
#include "sql/engine/expr/ob_expr_udf.h"
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
#include "sql/engine/expr/ob_expr_width_bucket.h"
#include "sql/engine/expr/ob_expr_lengthb.h"
#include "sql/engine/expr/ob_expr_hextoraw.h"
#include "sql/engine/expr/ob_expr_rawtohex.h"
#include "sql/engine/expr/ob_expr_utl_inaddr.h"
#include "sql/engine/expr/ob_expr_ols_funcs.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_pl_integer_checker.h"
#include "sql/engine/expr/ob_expr_pl_get_cursor_attr.h"
#include "sql/engine/expr/ob_expr_pl_sqlcode_sqlerrm.h"
#include "sql/engine/expr/ob_expr_plsql_variable.h"
#include "sql/engine/expr/ob_expr_pl_associative_index.h"
#include "sql/engine/expr/ob_expr_chr.h"
#include "sql/engine/expr/ob_expr_aes_encrypt.h"
#include "sql/engine/expr/ob_expr_timezone.h"
#include "sql/engine/expr/ob_expr_sys_extract_utc.h"
#include "sql/engine/expr/ob_expr_tz_offset.h"
#include "sql/engine/expr/ob_expr_from_tz.h"
#include "sql/engine/expr/ob_expr_collection_construct.h"
#include "sql/engine/expr/ob_expr_to_interval.h"
#include "sql/engine/expr/ob_expr_nvl2_oracle.h"
#include "sql/engine/expr/ob_expr_vsize.h"
#include "sql/engine/expr/ob_expr_orahash.h"
#include "sql/engine/expr/ob_expr_object_construct.h"
#include "sql/engine/expr/ob_expr_power.h"
#include "sql/engine/expr/ob_expr_exp.h"
#include "sql/engine/expr/ob_expr_ln.h"
#include "sql/engine/expr/ob_expr_log.h"
#include "sql/engine/expr/ob_expr_pl_seq_nextval.h"
#include "sql/engine/expr/ob_expr_oracle_nullif.h"
#include "sql/engine/expr/ob_expr_bool.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/engine/expr/ob_expr_part_id_pseudo_column.h"
#include "sql/engine/expr/ob_expr_to_single_byte.h"
#include "sql/engine/expr/ob_expr_to_multi_byte.h"
#include "sql/engine/expr/ob_expr_multiset.h"
#include "sql/engine/expr/ob_expr_stmt_id.h"
#include "sql/engine/expr/ob_expr_obversion.h"
#include "sql/engine/expr/ob_expr_utl_i18n.h"
#include "sql/engine/expr/ob_expr_dbms_crypto.h"
#include "sql/engine/expr/ob_expr_remove_const.h"
#include "sql/engine/expr/ob_expr_wrapper_inner.h"
#include "sql/engine/expr/ob_expr_calc_urowid.h"
#include "sql/engine/expr/ob_expr_set.h"
#include "sql/engine/expr/ob_expr_cardinality.h"
#include "sql/engine/expr/ob_expr_coll_pred.h"
#include "sql/engine/expr/ob_expr_user_can_access_obj.h"
#include "sql/engine/expr/ob_expr_empty_lob.h"
#include "sql/engine/expr/ob_expr_radians.h"
#include "sql/engine/expr/ob_expr_pi.h"
#include "sql/engine/expr/ob_expr_join_filter.h"
#include "sql/engine/expr/ob_expr_to_outfile_row.h"
#include "sql/engine/expr/ob_expr_format.h"
#include "sql/engine/expr/ob_expr_quarter.h"
#include "sql/engine/expr/ob_expr_bit_length.h"
#include "sql/engine/expr/ob_expr_unistr.h"
#include "sql/engine/expr/ob_expr_at_time_zone.h"
#include "sql/engine/expr/ob_expr_rowid_to_char.h"
#include "sql/engine/expr/ob_expr_rowid_to_nchar.h"
#include "sql/engine/expr/ob_expr_char_to_rowid.h"
#include "sql/engine/expr/ob_expr_soundex.h"
#include "sql/engine/expr/ob_expr_output_pack.h"
#include "sql/engine/expr/ob_expr_returning_lob.h"
#include "sql/engine/expr/ob_expr_degrees.h"
#include "sql/engine/expr/ob_expr_any_value.h"
#include "sql/engine/expr/ob_expr_validate_password_strength.h"
#include "sql/engine/expr/ob_expr_uuid_short.h"
#include "sql/engine/expr/ob_expr_benchmark.h"
#include "sql/engine/expr/ob_expr_weight_string.h"
#include "sql/engine/expr/ob_expr_convert_tz.h"
#include "sql/engine/expr/ob_expr_dml_event.h"
#include "sql/engine/expr/ob_expr_to_base64.h"
#include "sql/engine/expr/ob_expr_from_base64.h"
#include "sql/engine/expr/ob_expr_random_bytes.h"
#include "sql/engine/expr/ob_pl_expr_subquery.h"
#include "sql/engine/expr/ob_expr_encode_sortkey.h"
#include "sql/engine/expr/ob_expr_hash.h"
#include "sql/engine/expr/ob_expr_nlssort.h"
#include "sql/engine/expr/ob_expr_json_object.h"
#include "sql/engine/expr/ob_expr_json_extract.h"
#include "sql/engine/expr/ob_expr_json_contains.h"
#include "sql/engine/expr/ob_expr_json_contains_path.h"
#include "sql/engine/expr/ob_expr_json_depth.h"
#include "sql/engine/expr/ob_expr_json_keys.h"
#include "sql/engine/expr/ob_expr_json_search.h"
#include "sql/engine/expr/ob_expr_json_unquote.h"
#include "sql/engine/expr/ob_expr_json_quote.h"
#include "sql/engine/expr/ob_expr_json_array.h"
#include "sql/engine/expr/ob_expr_json_overlaps.h"
#include "sql/engine/expr/ob_expr_json_valid.h"
#include "sql/engine/expr/ob_expr_json_remove.h"
#include "sql/engine/expr/ob_expr_json_array_append.h"
#include "sql/engine/expr/ob_expr_json_array_insert.h"
#include "sql/engine/expr/ob_expr_json_value.h"
#include "sql/engine/expr/ob_expr_json_replace.h"
#include "sql/engine/expr/ob_expr_json_type.h"
#include "sql/engine/expr/ob_expr_json_length.h"
#include "sql/engine/expr/ob_expr_json_insert.h"
#include "sql/engine/expr/ob_expr_json_storage_size.h"
#include "sql/engine/expr/ob_expr_json_storage_free.h"
#include "sql/engine/expr/ob_expr_json_set.h"
#include "sql/engine/expr/ob_expr_json_merge_preserve.h"
#include "sql/engine/expr/ob_expr_json_merge.h"
#include "sql/engine/expr/ob_expr_json_merge_patch.h"
#include "sql/engine/expr/ob_expr_json_pretty.h"
#include "sql/engine/expr/ob_expr_json_member_of.h"
#include "sql/engine/expr/ob_expr_is_json.h"
#include "sql/engine/expr/ob_expr_json_equal.h"
#include "sql/engine/expr/ob_expr_sha.h"
#include "sql/engine/expr/ob_expr_compress.h"
#include "sql/engine/expr/ob_expr_statement_digest.h"
#include "sql/engine/expr/ob_expr_timestamp_to_scn.h"
#include "sql/engine/expr/ob_expr_scn_to_timestamp.h"
#include "sql/engine/expr/ob_expr_errno.h"
#include "sql/engine/expr/ob_expr_json_query.h"
#include "sql/engine/expr/ob_expr_json_exists.h"
#include "sql/engine/expr/ob_expr_treat.h"
#include "sql/engine/expr/ob_expr_point.h"
#include "sql/engine/expr/ob_expr_spatial_collection.h"
#include "sql/engine/expr/ob_expr_st_geomfromtext.h"
#include "sql/engine/expr/ob_expr_st_area.h"
#include "sql/engine/expr/ob_expr_st_intersects.h"
#include "sql/engine/expr/ob_expr_st_x.h"
#include "sql/engine/expr/ob_expr_st_transform.h"
#include "sql/engine/expr/ob_expr_priv_st_transform.h"
#include "sql/engine/expr/ob_expr_st_covers.h"
#include "sql/engine/expr/ob_expr_st_bestsrid.h"
#include "sql/engine/expr/ob_expr_st_astext.h"
#include "sql/engine/expr/ob_expr_st_buffer.h"
#include "sql/engine/expr/ob_expr_spatial_cellid.h"
#include "sql/engine/expr/ob_expr_spatial_mbr.h"
#include "sql/engine/expr/ob_expr_st_geomfromewkb.h"
#include "sql/engine/expr/ob_expr_st_geomfromwkb.h"
#include "sql/engine/expr/ob_expr_st_geomfromewkt.h"
#include "sql/engine/expr/ob_expr_priv_st_geogfromtext.h"
#include "sql/engine/expr/ob_expr_priv_st_geographyfromtext.h"
#include "sql/engine/expr/ob_expr_st_asewkt.h"
#include "sql/engine/expr/ob_expr_st_srid.h"
#include "sql/engine/expr/ob_expr_st_distance.h"
#include "sql/engine/expr/ob_expr_st_geometryfromtext.h"
#include "sql/engine/expr/ob_expr_priv_st_setsrid.h"
#include "sql/engine/expr/ob_expr_priv_st_point.h"
#include "sql/engine/expr/ob_expr_st_isvalid.h"
#include "sql/engine/expr/ob_expr_st_dwithin.h"
#include "sql/engine/expr/ob_expr_st_aswkb.h"
#include "sql/engine/expr/ob_expr_st_distance_sphere.h"
#include "sql/engine/expr/ob_expr_st_contains.h"
#include "sql/engine/expr/ob_expr_st_within.h"
#include "sql/engine/expr/ob_expr_priv_st_asewkb.h"
#include "sql/engine/expr/ob_expr_current_scn.h"
#include "sql/engine/expr/ob_expr_name_const.h"
#include "sql/engine/expr/ob_expr_format_bytes.h"
#include "sql/engine/expr/ob_expr_format_pico_time.h"
#include "sql/engine/expr/ob_expr_encrypt.h"
#include "sql/engine/expr/ob_expr_icu_version.h"
#include "sql/engine/expr/ob_expr_sql_mode_convert.h"
#include "sql/engine/expr/ob_expr_prefix_pattern.h"
#include "sql/engine/expr/ob_expr_priv_xml_binary.h"
#include "sql/engine/expr/ob_expr_sys_makexml.h"
#include "sql/engine/expr/ob_expr_priv_xml_binary.h"
#include "sql/engine/expr/ob_expr_xmlparse.h"
#include "sql/engine/expr/ob_expr_xml_element.h"
#include "sql/engine/expr/ob_expr_xml_attributes.h"
#include "sql/engine/expr/ob_expr_extract_value.h"
#include "sql/engine/expr/ob_expr_extract_xml.h"
#include "sql/engine/expr/ob_expr_xml_serialize.h"
#include "sql/engine/expr/ob_expr_xmlcast.h"
#include "sql/engine/expr/ob_expr_update_xml.h"
#include "sql/engine/expr/ob_expr_temp_table_ssid.h"
#include "sql/engine/expr/ob_expr_align_date4cmp.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
static AllocFunc OP_ALLOC[T_MAX_OP];
static AllocFunc OP_ALLOC_ORCL[T_MAX_OP];

#define REG_OP(OpClass)                             \
  do {                                              \
    [&]() {                                         \
      OpClass op(alloc);                            \
      if (OB_UNLIKELY(i >= EXPR_OP_NUM)) {          \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "out of the max expr");           \
      } else {                                      \
        NAME_TYPES[i].name_ = op.get_name();        \
        NAME_TYPES[i].type_ = op.get_type();        \
        NAME_TYPES[i].is_internal_ = op.is_internal_for_mysql(); \
        OP_ALLOC[op.get_type()] = ObExprOperatorFactory::alloc<OpClass>; \
        i++;                                        \
      }                                             \
    }();                                            \
  } while(0)

// 当要开发两个功能完全一致的表达式时（例如mid和substr两个表达式）可以使用这个宏
// OriOp是已有的表达式，现在想要开发NewOp且二者功能完全一致，使用该宏就可以避免重复代码
// 但是要求OriOp已经先注册了
#define REG_SAME_OP(OriOpType, NewOpType, NewOpName, idx_mysql)        \
  do {                                                                 \
    [&]() {                                                            \
      if (OB_UNLIKELY((idx_mysql) >= EXPR_OP_NUM)) {                   \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "out of the max expr");                              \
      } else if (OB_ISNULL(OP_ALLOC[OriOpType])) {                     \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "OriOp is not registered yet", K(OriOpType), K(NewOpType)); \
      } else {                                                         \
        NAME_TYPES[(idx_mysql)].name_ = NewOpName;                     \
        NAME_TYPES[(idx_mysql)].type_ = NewOpType;                     \
        OP_ALLOC[NewOpType] = OP_ALLOC[OriOpType];                     \
        (idx_mysql)++;                                                 \
      }                                                                \
    }();                                                               \
  } while(0)

#define REG_OP_ORCL(OpClass)                        \
  do {                                              \
    [&]() {                                         \
      OpClass op(alloc);                            \
      if (OB_UNLIKELY(j >= EXPR_OP_NUM)) {          \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "out of the max expr");           \
      } else {                                      \
        NAME_TYPES_ORCL[j].name_ = op.get_name();   \
        NAME_TYPES_ORCL[j].type_ = op.get_type();   \
        NAME_TYPES_ORCL[j].is_internal_ = op.is_internal_for_oracle();\
        OP_ALLOC_ORCL[op.get_type()] = ObExprOperatorFactory::alloc<OpClass>; \
        j++;                                        \
      }                                             \
    }();                                            \
  } while(0)

// 用于Oracle模式下注册相同功能的表达式
#define REG_SAME_OP_ORCL(OriOpType, NewOpType, NewOpName, idx_oracle)      \
  do {                                                                     \
    [&]() {                                                                \
      if (OB_UNLIKELY((idx_oracle) >= EXPR_OP_NUM)) {                      \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "out of the max expr");                                  \
      } else if (OB_ISNULL(OP_ALLOC_ORCL[OriOpType])) {                    \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "OriOp is not registered yet", K(OriOpType), K(NewOpType)); \
      } else {                                                             \
        NAME_TYPES_ORCL[(idx_oracle)].name_ = NewOpName;                   \
        NAME_TYPES_ORCL[(idx_oracle)].type_ = NewOpType;                   \
        OP_ALLOC_ORCL[NewOpType] = OP_ALLOC_ORCL[OriOpType];               \
        (idx_oracle)++;                                                    \
      }                                                                    \
    }();                                                                   \
  } while(0)

ObExprOperatorFactory::NameType ObExprOperatorFactory::NAME_TYPES[EXPR_OP_NUM] = { };
ObExprOperatorFactory::NameType ObExprOperatorFactory::NAME_TYPES_ORCL[EXPR_OP_NUM] = { };

char *ObExprOperatorFactory::str_toupper(char *buff)
{
  if (OB_LIKELY(NULL != buff)) {
    char *ptr = buff;
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

ObExprOperatorType ObExprOperatorFactory::get_type_by_name(const ObString &name)
{
  ObExprOperatorType type = T_INVALID;
  ObString real_func_name;
  get_function_alias_name(name, real_func_name);
  if (real_func_name.empty()) {
    real_func_name.assign_ptr(name.ptr(), name.length());
  }
  if (lib::is_oracle_mode()) {
    char name_buf[OB_MAX_FUNC_EXPR_LENGTH];
    ObString func_name(N_ORA_DECODE);
    if (real_func_name.case_compare("decode") != 0) {
      func_name.assign_ptr(real_func_name.ptr(), real_func_name.length());
    }
    for (uint32_t i = 0; i < ARRAYSIZEOF(NAME_TYPES_ORCL); i++) {
      if (NAME_TYPES_ORCL[i].type_ <= T_MIN_OP || NAME_TYPES_ORCL[i].type_ >= T_MAX_OP) {
        break;
      }
      //将op原有的名字转为大写进行匹配
      MEMSET(name_buf, 0, OB_MAX_FUNC_EXPR_LENGTH);
      strncpy(name_buf, NAME_TYPES_ORCL[i].name_, OB_MAX_FUNC_EXPR_LENGTH);
      str_toupper(name_buf);
      //暂时不敏感匹配，等大小写机制提上去后更新为strncmp
      if (static_cast<int32_t>(strlen(NAME_TYPES_ORCL[i].name_)) == func_name.length()
          && strncasecmp(name_buf, func_name.ptr(), func_name.length()) == 0) {
        type = NAME_TYPES_ORCL[i].type_;
        break;
      }
    }
  } else {
    for (uint32_t i = 0; i < ARRAYSIZEOF(NAME_TYPES); i++) {
      if (NAME_TYPES[i].type_ <= T_MIN_OP || NAME_TYPES[i].type_ >= T_MAX_OP) {
        break;
      }
      if (static_cast<int32_t>(strlen(NAME_TYPES[i].name_)) == real_func_name.length()
          && strncasecmp(NAME_TYPES[i].name_, real_func_name.ptr(), real_func_name.length()) == 0) {
        type = NAME_TYPES[i].type_;
        break;
      }
    }
  }
  return type;
}

void ObExprOperatorFactory::get_internal_info_by_name(const ObString &name, bool &exist, bool &is_internal)
{
  exist = false;
  ObString real_func_name;
  get_function_alias_name(name, real_func_name);
  if (real_func_name.empty()) {
    real_func_name.assign_ptr(name.ptr(), name.length());
  }
  if (lib::is_oracle_mode()) {
    char name_buf[OB_MAX_FUNC_EXPR_LENGTH];
    ObString func_name(N_ORA_DECODE);
    if (real_func_name.case_compare("decode") != 0) {
      func_name.assign_ptr(real_func_name.ptr(), real_func_name.length());
    }
    for (uint32_t i = 0; i < ARRAYSIZEOF(NAME_TYPES_ORCL); i++) {
      if (NAME_TYPES_ORCL[i].type_ <= T_MIN_OP || NAME_TYPES_ORCL[i].type_ >= T_MAX_OP) {
        break;
      }
      //将op原有的名字转为大写进行匹配
      MEMSET(name_buf, 0, OB_MAX_FUNC_EXPR_LENGTH);
      strncpy(name_buf, NAME_TYPES_ORCL[i].name_, OB_MAX_FUNC_EXPR_LENGTH);
      str_toupper(name_buf);
      //暂时不敏感匹配，等大小写机制提上去后更新为strncmp
      if (static_cast<int32_t>(strlen(NAME_TYPES_ORCL[i].name_)) == func_name.length()
          && strncasecmp(name_buf, func_name.ptr(), func_name.length()) == 0) {
        exist = true;
        is_internal = NAME_TYPES_ORCL[i].is_internal_;
        break;
      }
    }
  } else {
    for (uint32_t i = 0; i < ARRAYSIZEOF(NAME_TYPES); i++) {
      if (NAME_TYPES[i].type_ <= T_MIN_OP || NAME_TYPES[i].type_ >= T_MAX_OP) {
        break;
      }
      if (static_cast<int32_t>(strlen(NAME_TYPES[i].name_)) == real_func_name.length()
          && strncasecmp(NAME_TYPES[i].name_, real_func_name.ptr(), real_func_name.length()) == 0) {
        exist = true;
        is_internal = NAME_TYPES[i].is_internal_;
        break;
      }
    }
  }
}


void ObExprOperatorFactory::register_expr_operators()
{
  memset(NAME_TYPES, 0, sizeof(NAME_TYPES));
  memset(NAME_TYPES_ORCL, 0, sizeof(NAME_TYPES_ORCL));
  ObArenaAllocator alloc;
  int64_t i = 0;
  int64_t j = 0;
  /*
  --REG_OP用于mysql租户注册，REG_OP_ORCL用于oracle租户系统函数注册
  --如果同一函数既要在mysql租户下使用也需在oracle使用，且已实现兼容
  --请使用REG_OP()以及REG_OP_ORCL()分别注册
  为了格式，请在函数末尾oracle系统函数集中区域注册
  */
  [&]() {
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
    REG_OP(ObExprCurrentUserPriv);
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
    REG_OP(ObExprGreatest);
    REG_OP(ObExprHex);
    REG_OP(ObExprIn);
    REG_OP(ObExprNotIn);
    REG_OP(ObExprInt2ip);
    REG_OP(ObExprIp2int);
    REG_OP(ObExprInetAton);
    REG_OP(ObExprInet6Ntoa);
    REG_OP(ObExprInet6Aton);
    REG_OP(ObExprIsIpv4);
    REG_OP(ObExprIsIpv6);
    REG_OP(ObExprIsIpv4Mapped);
    REG_OP(ObExprIsIpv4Compat);
    REG_OP(ObExprInsert);
    REG_OP(ObExprIs);
    REG_OP(ObExprIsNot);
    REG_OP(ObExprLeast);
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
    REG_OP(ObExprMid);
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
    REG_OP(ObExprSoundex);
    REG_OP(ObExprDateAdd);
    REG_OP(ObExprDateSub);
    REG_OP(ObExprSubtime);
    REG_OP(ObExprAddtime);
    REG_OP(ObExprDateDiff);
    REG_OP(ObExprTimeStampDiff);
    REG_OP(ObExprTimeDiff);
    REG_OP(ObExprPeriodDiff);
    REG_OP(ObExprPeriodAdd);
    REG_OP(ObExprUnixTimestamp);
    REG_OP(ObExprMakeTime);
    REG_OP(ObExprMakedate);
    REG_OP(ObExprExtract);
    REG_OP(ObExprToDays);
    REG_OP(ObExprPosition);
    REG_OP(ObExprFromDays);
    REG_OP(ObExprDateFormat);
    REG_OP(ObExprGetFormat);
    REG_OP(ObExprStrToDate);
    REG_OP(ObExprCurDate);
    REG_OP(ObExprCurTime);
    REG_OP(ObExprSysdate);
    REG_OP(ObExprCurTimestamp);
    REG_OP(ObExprUtcTimestamp);
    REG_OP(ObExprUtcTime);
    REG_OP(ObExprUtcDate);
    REG_OP(ObExprTimeToUsec);
    REG_OP(ObExprUsecToTime);
    REG_OP(ObExprMergingFrozenTime);
    REG_OP(ObExprFuncRound);
    REG_OP(ObExprFuncFloor);
    REG_OP(ObExprFuncCeil);
    REG_OP(ObExprFuncCeiling);
    REG_OP(ObExprFuncDump);
    REG_OP(ObExprRepeat);
    REG_OP(ObExprExportSet);
    REG_OP(ObExprReplace);
    REG_OP(ObExprFuncPartHash);
    REG_OP(ObExprFuncPartKey);
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
    REG_OP(ObExprConvertTZ);
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
    REG_OP(ObExprRand);
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
    REG_OP(ObExprAnyValue);
    REG_OP(ObExprUuidShort);
    REG_OP(ObExprRandomBytes);
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
    REG_OP(ObExprObjAccess);
    REG_OP(ObExprEnumToStr);
    REG_OP(ObExprSetToStr);
    REG_OP(ObExprEnumToInnerType);
    REG_OP(ObExprSetToInnerType);
    REG_OP(ObExprConnectByRoot);
    REG_OP(ObExprGetPackageVar);
    REG_OP(ObExprGetSubprogramVar);
    REG_OP(ObExprShadowUKProject);
    REG_OP(ObExprUDF);
    REG_OP(ObExprWeekOfYear);
    REG_OP(ObExprWeekDay);
    REG_OP(ObExprYearWeek);
    REG_OP(ObExprWeek);
    REG_OP(ObExprQuarter);
    REG_OP(ObExprSeqNextval);
    REG_OP(ObExprAesDecrypt);
    REG_OP(ObExprAesEncrypt);
    REG_OP(ObExprBool);
    REG_OP(ObExprSin);
    REG_OP(ObExprCos);
    REG_OP(ObExprTan);
    REG_OP(ObExprCot);
    REG_OP(ObExprCalcPartitionId);
    REG_OP(ObExprCalcTabletId);
    REG_OP(ObExprCalcPartitionTabletId);
    REG_OP(ObExprPartIdPseudoColumn);
    REG_OP(ObExprStmtId);
    REG_OP(ObExprRadians);
    REG_OP(ObExprJoinFilter);
    REG_OP(ObExprAsin);
    REG_OP(ObExprAcos);
    REG_OP(ObExprAtan);
    REG_OP(ObExprAtan2);
    REG_OP(ObExprToOutfileRow);
    REG_OP(ObExprFormat);
    REG_OP(ObExprLastDay);
    REG_OP(ObExprPi);
    REG_OP(ObExprLog);
    REG_OP(ObExprTimeFormat);
    REG_OP(ObExprTimestamp);
    REG_OP(ObExprOutputPack);
    REG_OP(ObExprWrapperInner);
    REG_OP(ObExprDegrees);
    REG_OP(ObExprValidatePasswordStrength);
    REG_OP(ObExprDay);
    REG_OP(ObExprBenchmark);
    REG_OP(ObExprWeightString);
    REG_OP(ObExprCrc32);
    REG_OP(ObExprToBase64);
    REG_OP(ObExprFromBase64);
    REG_OP(ObExprOpSubQueryInPl);
    REG_OP(ObExprEncodeSortkey);
    REG_OP(ObExprHash);
    REG_OP(ObExprJsonObject);
    REG_OP(ObExprJsonExtract);
    REG_OP(ObExprJsonContains);
    REG_OP(ObExprJsonContainsPath);
    REG_OP(ObExprJsonDepth);
    REG_OP(ObExprJsonKeys);
    REG_OP(ObExprJsonQuote);
    REG_OP(ObExprJsonUnquote);
    REG_OP(ObExprJsonArray);
    REG_OP(ObExprJsonOverlaps);
    REG_OP(ObExprJsonRemove);
    REG_OP(ObExprJsonSearch);
    REG_OP(ObExprJsonValid);
    REG_OP(ObExprJsonArrayAppend);
    REG_OP(ObExprJsonArrayInsert);
    REG_OP(ObExprJsonValue);
    REG_OP(ObExprJsonReplace);
    REG_OP(ObExprJsonType);
    REG_OP(ObExprJsonLength);
    REG_OP(ObExprJsonInsert);
    REG_OP(ObExprJsonStorageSize);
    REG_OP(ObExprJsonStorageFree);
    REG_OP(ObExprJsonSet);
    REG_OP(ObExprJsonMergePreserve);
    REG_OP(ObExprJsonMerge);
    REG_OP(ObExprJsonMergePatch);
    REG_OP(ObExprJsonPretty);
    REG_OP(ObExprJsonMemberOf);
    REG_OP(ObExprSha);
    REG_SAME_OP(T_FUN_SYS_SHA ,T_FUN_SYS_SHA, N_SHA1, i);
    REG_OP(ObExprSha2);
    REG_OP(ObExprCompress);
    REG_OP(ObExprUncompress);
    REG_OP(ObExprUncompressedLength);
    REG_OP(ObExprStatementDigest);
    REG_OP(ObExprStatementDigestText);
    REG_OP(ObExprTimestampToScn);
    REG_OP(ObExprScnToTimestamp);
    REG_OP(ObExprSqlModeConvert);
#if  defined(ENABLE_DEBUG_LOG) || !defined(NDEBUG)
    // convert input value into an OceanBase error number and throw out as exception
    REG_OP(ObExprErrno);
#endif
    REG_OP(ObExprPoint);
    REG_OP(ObExprLineString);
    REG_OP(ObExprMultiPoint);
    REG_OP(ObExprMultiLineString);
    REG_OP(ObExprPolygon);
    REG_OP(ObExprMultiPolygon);
    REG_OP(ObExprGeomCollection);
    REG_OP(ObExprGeometryCollection);
    REG_OP(ObExprSTGeomFromText);
    REG_OP(ObExprSTArea);
    REG_OP(ObExprSTIntersects);
    REG_OP(ObExprSTX);
    REG_OP(ObExprSTY);
    REG_OP(ObExprSTLatitude);
    REG_OP(ObExprSTLongitude);
    REG_OP(ObExprSTTransform);
    REG_OP(ObExprPrivSTTransform);
    REG_OP(ObExprPrivSTCovers);
    REG_OP(ObExprPrivSTBestsrid);
    REG_OP(ObExprSTAsText);
    REG_OP(ObExprSTAsWkt);
    REG_OP(ObExprSTBufferStrategy);
    REG_OP(ObExprSTBuffer);
    REG_OP(ObExprSpatialCellid);
    REG_OP(ObExprSpatialMbr);
    REG_OP(ObExprPrivSTGeomFromEWKB);
    REG_OP(ObExprSTGeomFromWKB);
    REG_OP(ObExprSTGeometryFromWKB);
    REG_OP(ObExprPrivSTGeomFromEwkt);
    REG_OP(ObExprPrivSTAsEwkt);
    REG_OP(ObExprSTSRID);
    REG_OP(ObExprSTDistance);
    REG_OP(ObExprPrivSTGeogFromText);
    REG_OP(ObExprPrivSTGeographyFromText);
    REG_OP(ObExprPrivSTSetSRID);
    REG_OP(ObExprSTGeometryFromText);
    REG_OP(ObExprPrivSTPoint);
    REG_OP(ObExprSTIsValid);
    REG_OP(ObExprPrivSTBuffer);
    REG_OP(ObExprPrivSTDWithin);
    REG_OP(ObExprSTAsWkb);
    REG_OP(ObExprStPrivAsEwkb);
    REG_OP(ObExprSTAsBinary);
    REG_OP(ObExprSTDistanceSphere);
    REG_OP(ObExprSTContains);
    REG_OP(ObExprSTWithin);
    REG_OP(ObExprFormatBytes);
    REG_OP(ObExprFormatPicoTime);
    REG_OP(ObExprUuid2bin);
    REG_OP(ObExprIsUuid);
    REG_OP(ObExprBin2uuid);
    REG_OP(ObExprNameConst);
    REG_OP(ObExprDayName);
    REG_OP(ObExprDesDecrypt);
    REG_OP(ObExprDesEncrypt);
    REG_OP(ObExprEncrypt);
    REG_OP(ObExprCurrentScn);
    REG_OP(ObExprEncode);
    REG_OP(ObExprDecode);
    REG_OP(ObExprICUVersion);
    REG_OP(ObExprGeneratorFunc);
    REG_OP(ObExprZipf);
    REG_OP(ObExprNormal);
    REG_OP(ObExprUniform);
    REG_OP(ObExprRandom);
    REG_OP(ObExprRandstr);
    REG_OP(ObExprPrefixPattern);
    REG_OP(ObExprAlignDate4Cmp);
  }();
// 注册oracle系统函数
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
  REG_OP_ORCL(ObExprOracleDecode);
  REG_OP_ORCL(ObExprUid);
  REG_OP_ORCL(ObExprReplace);
  REG_OP_ORCL(ObExprTranslate);
  REG_OP_ORCL(ObExprLength);
  REG_OP_ORCL(ObExprLengthb);
  REG_OP_ORCL(ObExprEffectiveTenantId);   //未来在oracle租户下将要去除该id
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
  REG_OP_ORCL(ObExprSoundex);
  REG_OP_ORCL(ObExprLocalTimestamp);
  REG_OP_ORCL(ObExprSetCollation);
  REG_OP_ORCL(ObExprWidthBucket);
  REG_OP_ORCL(ObExprChr);
  REG_OP_ORCL(ObExprExtract);
  REG_OP_ORCL(ObExprSqrt);
  REG_OP_ORCL(ObExprNlsLower);
  REG_OP_ORCL(ObExprNlsUpper);
  REG_OP_ORCL(ObExprEstimateNdv);
  REG_OP_ORCL(ObExprAtTimeZone);
  REG_OP_ORCL(ObExprAtLocal);
  REG_OP_ORCL(ObExprTimestampToScn);
  REG_OP_ORCL(ObExprScnToTimestamp);
  REG_OP_ORCL(ObExprNlsInitCap);
  //部分内部使用的表达式
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
  REG_OP_ORCL(ObExprGetPackageVar);
  REG_OP_ORCL(ObExprGetSubprogramVar);
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
  REG_OP_ORCL(ObExprUDF);
  REG_OP_ORCL(ObExprAscii);
  REG_OP_ORCL(ObExprNvl2Oracle);
  REG_OP_ORCL(ObExprToBinaryFloat);
  REG_OP_ORCL(ObExprToBinaryDouble);
  REG_OP_ORCL(ObExprOracleNullif);
  REG_OP_ORCL(ObExprStmtId);
  REG_OP_ORCL(ObExprNaNvl);
  REG_OP_ORCL(ObExprOutputPack);
  REG_OP_ORCL(ObExprWrapperInner);
  REG_OP_ORCL(ObExprReturningLob);
  REG_OP_ORCL(ObExprDmlEvent);
  REG_OP_ORCL(ObExprLeast);
  REG_OP_ORCL(ObExprGreatest);
  REG_OP_ORCL(ObExprHostIP);
  REG_OP_ORCL(ObExprRpcPort);
  REG_OP_ORCL(ObExprIsServingTenant);
  REG_OP_ORCL(ObExprBitAndOra);
  REG_OP_ORCL(ObExprHextoraw);
  REG_OP_ORCL(ObExprRawtohex);
  REG_OP_ORCL(ObExprRawtonhex);
  REG_OP_ORCL(ObExprDateAdd);
  REG_OP_ORCL(ObExprDateSub);
  REG_OP_ORCL(ObExprFunDefault);
  REG_OP_ORCL(ObExprPLIntegerChecker);
  REG_OP_ORCL(ObExprPLGetCursorAttr);
  REG_OP_ORCL(ObExprPLSQLCodeSQLErrm);
  REG_OP_ORCL(ObExprPLSQLVariable);
  REG_OP_ORCL(ObExprPLAssocIndex);
  REG_OP_ORCL(ObExprCollectionConstruct);
  REG_OP_ORCL(ObExprObjectConstruct);
  REG_OP_ORCL(ObExprSessiontimezone);
  REG_OP_ORCL(ObExprDbtimezone);
  REG_OP_ORCL(ObExprSysExtractUtc);
  REG_OP_ORCL(ObExprTzOffset);
  REG_OP_ORCL(ObExprFromTz);
  //label security
  REG_OP_ORCL(ObExprOLSPolicyCreate);
  REG_OP_ORCL(ObExprOLSPolicyAlter);
  REG_OP_ORCL(ObExprOLSPolicyDrop);
  REG_OP_ORCL(ObExprOLSPolicyDisable);
  REG_OP_ORCL(ObExprOLSPolicyEnable);
  REG_OP_ORCL(ObExprOLSLevelCreate);
  REG_OP_ORCL(ObExprOLSLevelAlter);
  REG_OP_ORCL(ObExprOLSLevelDrop);
  REG_OP_ORCL(ObExprOLSLabelCreate);
  REG_OP_ORCL(ObExprOLSLabelAlter);
  REG_OP_ORCL(ObExprOLSLabelDrop);
  REG_OP_ORCL(ObExprOLSTablePolicyApply);
  REG_OP_ORCL(ObExprOLSTablePolicyRemove);
  REG_OP_ORCL(ObExprOLSTablePolicyDisable);
  REG_OP_ORCL(ObExprOLSTablePolicyEnable);
  REG_OP_ORCL(ObExprOLSUserSetLevels);
  REG_OP_ORCL(ObExprOLSSessionSetLabel);
  REG_OP_ORCL(ObExprOLSSessionSetRowLabel);
  REG_OP_ORCL(ObExprOLSSessionRestoreDefaultLabels);
  REG_OP_ORCL(ObExprOLSSessionLabel);
  REG_OP_ORCL(ObExprOLSSessionRowLabel);
  REG_OP_ORCL(ObExprOLSLabelCmpLE);
  REG_OP_ORCL(ObExprOLSLabelCheck);
  REG_OP_ORCL(ObExprOLSCharToLabel);
  REG_OP_ORCL(ObExprOLSLabelToChar);
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
  REG_OP_ORCL(ObExprPLSeqNextval);

  REG_SAME_OP_ORCL(T_FUN_SYS_LENGTH, T_FUN_SYS_LENGTHC, "lengthc", j);
  REG_SAME_OP_ORCL(T_FUN_SYS_SUBSTR, T_FUN_SYS_SUBSTRC, "substrc", j);
  REG_SAME_OP_ORCL(T_FUN_SYS_INSTR,  T_FUN_SYS_INSTRC,  "instrc",  j);
  REG_OP_ORCL(ObExprFuncDump);
  REG_OP_ORCL(ObExprCalcPartitionId);
  REG_OP_ORCL(ObExprCalcTabletId);
  REG_OP_ORCL(ObExprCalcPartitionTabletId);
  //SYS_GUID
  REG_OP_ORCL(ObExprSysGuid);
  REG_OP_ORCL(ObExprPartIdPseudoColumn);
  REG_OP_ORCL(ObExprToSingleByte);
  REG_OP_ORCL(ObExprToMultiByte);
  REG_OP_ORCL(ObExprMultiSet);
  REG_OP_ORCL(ObExprUtlI18nStringToRaw);
  REG_OP_ORCL(ObExprUtlI18nRawToChar);
  REG_OP_ORCL(ObExprUtlInaddrGetHostAddr);
  REG_OP_ORCL(ObExprUtlInaddrGetHostName);
  REG_OP_ORCL(ObExprDbmsCryptoEncrypt);
  REG_OP_ORCL(ObExprDbmsCryptoDecrypt);
  REG_OP_ORCL(ObExprOracleToNChar);

  // URowID
  REG_OP_ORCL(ObExprCalcURowID);
  REG_OP_ORCL(ObExprSet);
  REG_OP_ORCL(ObExprCardinality);
  REG_OP_ORCL(ObExprCollPred);
  // Priv
  REG_OP_ORCL(ObExprUserCanAccessObj);
  REG_OP_ORCL(ObExprEmptyClob);
  REG_OP_ORCL(ObExprEmptyBlob);
  REG_OP_ORCL(ObExprJoinFilter);
  REG_OP_ORCL(ObExprToOutfileRow);
  REG_OP_ORCL(ObExprCharset);
  REG_OP_ORCL(ObExprCollation);
  REG_OP_ORCL(ObExprCoercibility);
  REG_OP_ORCL(ObExprConvertOracle);
  REG_OP_ORCL(ObExprUnistr);
  REG_OP_ORCL(ObExprAsciistr);
  REG_OP_ORCL(ObExprSysOpOpnsize);
  REG_OP_ORCL(ObExprRowIDToChar);
  REG_OP_ORCL(ObExprRowIDToNChar);
  REG_OP_ORCL(ObExprCharToRowID);
  REG_OP_ORCL(ObExprLastTraceId);
  REG_OP_ORCL(ObExprReverse);
  REG_OP_ORCL(ObExprEncodeSortkey);
  REG_OP_ORCL(ObExprHash);
  REG_OP_ORCL(ObExprNLSSort);
  REG_OP_ORCL(ObExprObVersion);
#if  defined(ENABLE_DEBUG_LOG) || !defined(NDEBUG)
  REG_OP_ORCL(ObExprErrno);
#endif
  REG_OP_ORCL(ObExprJsonValue);
  REG_OP_ORCL(ObExprIsJson);
  REG_OP_ORCL(ObExprJsonEqual);
  REG_OP_ORCL(ObExprJsonQuery);
  REG_OP_ORCL(ObExprJsonMergePatch);
  REG_OP_ORCL(ObExprJsonExists);
  REG_OP_ORCL(ObExprJsonArray);
  REG_OP_ORCL(ObExprJsonObject);
  REG_OP_ORCL(ObExprCurrentScn);
  REG_OP_ORCL(ObExprTreat);
  REG_OP_ORCL(ObExprGeneratorFunc);
  REG_OP_ORCL(ObExprZipf);
  REG_OP_ORCL(ObExprNormal);
  REG_OP_ORCL(ObExprUniform);
  REG_OP_ORCL(ObExprRandom);
  REG_OP_ORCL(ObExprRandstr);
  REG_OP_ORCL(ObExprPrefixPattern);
  REG_OP_ORCL(ObExprPrivXmlBinary);
  REG_OP_ORCL(ObExprSysMakeXML);
  REG_OP_ORCL(ObExprPrivXmlBinary);
  REG_OP_ORCL(ObExprXmlparse);
  REG_OP_ORCL(ObExprXmlElement);
  REG_OP_ORCL(ObExprXmlAttributes);
  REG_OP_ORCL(ObExprExtractValue);
  REG_OP_ORCL(ObExprExtractXml);
  REG_OP_ORCL(ObExprXmlSerialize);
  REG_OP_ORCL(ObExprXmlcast);
  REG_OP_ORCL(ObExprUpdateXml);
  REG_OP_ORCL(ObExprTempTableSSID);
}

bool ObExprOperatorFactory::is_expr_op_type_valid(ObExprOperatorType type)
{
  bool bret = false;
  if (type > T_REF_COLUMN && type < T_MAX_OP) {
    bret = true;
  }
  return bret;
}

int ObExprOperatorFactory::alloc(ObExprOperatorType type, ObExprOperator *&expr_op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_expr_op_type_valid(type))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(type));
  } else if (OB_ISNULL(lib::is_oracle_mode() ?
             OP_ALLOC_ORCL[type] :
             OP_ALLOC[type])) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpectd expr item type", K(ret), K(type));
  } else if (OB_FAIL(lib::is_oracle_mode() ?
             OP_ALLOC_ORCL[type](alloc_, expr_op) :
             OP_ALLOC[type](alloc_, expr_op))) {
    OB_LOG(WARN, "fail to alloc expr_op", K(ret), K(type));
  } else if (OB_ISNULL(expr_op)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(ERROR, "fail to alloc expr_op", K(ret), K(type));
  } else if (NULL != next_expr_id_) {
    expr_op->set_id((*next_expr_id_)++);
  }
  return ret;
}

int ObExprOperatorFactory::alloc_fast_expr(ObExprOperatorType type, ObFastExprOperator *&fast_op)
{
#define BEGIN_ALLOC \
  switch (type) {
#define END_ALLOC \
    default: \
      ret = OB_ERR_UNEXPECTED; \
      LOG_WARN("invalid operator type", K(type)); \
      break; \
  }
#define ALLOC_FAST_EXPR(op_type, OpClass) \
  case op_type : { \
    void *ptr = alloc_.alloc(sizeof(OpClass)); \
    if (OB_ISNULL(ptr)) { \
      ret = OB_ALLOCATE_MEMORY_FAILED; \
      LOG_WARN("allocate operator failed", K(op_type)); \
    } else { \
      fast_op = new(ptr) OpClass(alloc_); \
    } \
    break; \
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

//int ObExprOperatorFactory::free(ObExprOperator *&expr_op)
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
int ObExprOperatorFactory::alloc(common::ObIAllocator &alloc, ObExprOperator *&expr_op)
{
  int ret = common::OB_SUCCESS;
  void *buf = NULL;
  if (OB_ISNULL(buf = alloc.alloc(sizeof(ClassT)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(ERROR, "fail to alloc expr_operator", K(ret));
  } else {
    expr_op = new(buf) ClassT(alloc);
  }
  return ret;
}


void ObExprOperatorFactory::get_function_alias_name(const ObString &origin_name, ObString &alias_name) {
  if (is_mysql_mode()) {
    //for synonyms in mysql mode
    if (0 == origin_name.case_compare("bin")) {
      // bin(N) is equivalent to CONV(N,10,2)
      alias_name = ObString::make_string(N_CONV);
    } else if (0 == origin_name.case_compare("oct")) {
      // oct(N) is equivalent to CONV(N,10,8)
      alias_name = ObString::make_string(N_CONV);
    } else if (0 == origin_name.case_compare("lcase")) {
      // lcase is synonym for lower
      alias_name = ObString::make_string(N_LOWER);
    } else if (0 == origin_name.case_compare("ucase")) {
      // ucase is synonym for upper
      alias_name = ObString::make_string(N_UPPER);
    } else if (!lib::is_oracle_mode() && 0 == origin_name.case_compare("power")) {
      // don't alias "power" to "pow" in oracle mode, because oracle has no
      // "pow" function.
      alias_name = ObString::make_string(N_POW);
    } else if (0 == origin_name.case_compare("ws")) {
      // ws is synonym for word_segment
      alias_name = ObString::make_string(N_WORD_SEGMENT);
    } else if (0 == origin_name.case_compare("inet_ntoa")) {
      // inet_ntoa is synonym for int2ip
      alias_name = ObString::make_string(N_INT2IP);
    } else if (0 == origin_name.case_compare("octet_length")) {
      // octet_length is synonym for length
      alias_name = ObString::make_string(N_LENGTH);
    } else if (0 == origin_name.case_compare("character_length")) {
      // character_length is synonym for char_length
      alias_name = ObString::make_string(N_CHAR_LENGTH);
    } else if (0 == origin_name.case_compare("area")) {
      // area is synonym for st_area
      alias_name = ObString::make_string(N_ST_AREA);
    } else {
      //do nothing
    }
  } else {
    //for synonyms in oracle mode
  }
}

} //end sql
} //end oceanbase

