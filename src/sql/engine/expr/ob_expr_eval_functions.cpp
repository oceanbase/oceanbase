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
#include "ob_expr_tokenize.h"
#include "ob_expr_insert.h"
#include "ob_expr_int2ip.h"
#include "ob_expr_int_div.h"
#include "ob_expr_ip2int.h"
#include "ob_expr_inet.h"
#include "ob_expr_is.h"
#include "ob_expr_last_exec_id.h"
#include "ob_expr_last_trace_id.h"
#include "ob_expr_length.h"
#include "ob_expr_like.h"
#include "ob_expr_lower.h"
#include "ob_expr_md5.h"
#include "ob_expr_crc32.h"
#include "src/sql/engine/expr/ob_expr_substr.h"
#include "ob_expr_minus.h"
#include "ob_expr_mod.h"
#include "ob_expr_mul.h"
#include "ob_expr_mysql_port.h"
#include "ob_expr_not.h"
#include "ob_expr_not_exists.h"
#include "ob_expr_null_safe_equal.h"
#include "ob_expr_nvl2_oracle.h"
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
#include "ob_expr_substring_index.h"
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
#include "ob_expr_instrb.h"
#include "ob_expr_concat_ws.h"
#include "ob_expr_make_set.h"
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
#include "ob_expr_part_id.h"
#include "ob_expr_hex.h"
#include "ob_expr_shadow_uk_project.h"
#include "ob_expr_char_length.h"
#include "ob_expr_unix_timestamp.h"
#include "ob_expr_symmetric_encrypt.h"
#include "ob_expr_case.h"
#include "ob_expr_oracle_decode.h"
#include "ob_expr_remove_const.h"
#include "ob_expr_wrapper_inner.h"
#include "ob_expr_func_sleep.h"
#include "ob_expr_errno.h"
#include "ob_expr_get_package_var.h"
#include "ob_expr_sys_context.h"
#include "ob_expr_timestamp_diff.h"
#include "ob_expr_timestamp_add.h"
#include "ob_expr_from_tz.h"
#include "ob_expr_tz_offset.h"
#include "ob_expr_orahash.h"
#include "ob_expr_get_user_var.h"
#include "ob_expr_cot.h"
#include "ob_expr_convert.h"
#include "ob_expr_type_to_str.h"
#include "ob_expr_date_format.h"
#include "ob_expr_calc_urowid.h"
#include "ob_expr_sys_connect_by_path.h"
#include "ob_expr_last_insert_id.h"
#include "ob_expr_part_id_pseudo_column.h"
#include "ob_expr_oracle_nullif.h"
#include "ob_expr_user_can_access_obj.h"
#include "ob_expr_empty_lob.h"
#include "ob_expr_radians.h"
#include "ob_expr_pi.h"
#include "ob_expr_maketime.h"
#include "ob_expr_makedate.h"
#include "ob_expr_to_blob.h"
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
#include "ob_expr_udf/ob_expr_udf.h"
#include "ob_expr_object_construct.h"
#include "ob_expr_pl_get_cursor_attr.h"
#include "ob_expr_pl_integer_checker.h"
#include "ob_expr_get_subprogram_var.h"
#include "ob_expr_pl_sqlcode_sqlerrm.h"
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
#include "ob_expr_json_schema_valid.h"
#include "ob_expr_json_schema_validation_report.h"
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
#include "ob_expr_json_append.h"
#include "ob_expr_json_array_insert.h"
#include "ob_expr_json_value.h"
#include "ob_expr_json_replace.h"
#include "ob_expr_json_type.h"
#include "ob_expr_json_length.h"
#include "ob_expr_json_insert.h"
#include "ob_expr_json_storage_size.h"
#include "ob_expr_json_storage_free.h"
#include "ob_expr_json_set.h"
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
#include "ob_expr_st_distance.h"
#include "ob_expr_st_geometryfromtext.h"
#include "ob_expr_priv_st_setsrid.h"
#include "ob_expr_priv_st_point.h"
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
#include "ob_expr_existsnode_xml.h"
#include "ob_expr_xml_serialize.h"
#include "ob_expr_xmlcast.h"
#include "ob_expr_update_xml.h"
#include "ob_expr_insert_child_xml.h"
#include "ob_expr_xml_delete_xml.h"
#include "ob_expr_xml_sequence.h"
#include "ob_expr_xml_concat.h"
#include "ob_expr_xml_forest.h"
#include "ob_expr_generator_func.h"
#include "ob_expr_random.h"
#include "ob_expr_randstr.h"
#include "ob_expr_startup_mode.h"
#include "ob_expr_zipf.h"
#include "ob_expr_normal.h"
#include "ob_expr_uniform.h"
#include "ob_expr_prefix_pattern.h"
#include "ob_expr_initcap.h"
#include "ob_expr_sin.h"
#include "ob_expr_temp_table_ssid.h"
#include "ob_expr_between.h"
#include "ob_expr_align_date4cmp.h"
#include "ob_expr_word_count.h"
#include "ob_expr_word_segment.h"
#include "ob_expr_doc_id.h"
#include "ob_expr_doc_length.h"
#include "ob_expr_bm25.h"
#include "ob_expr_lock_func.h"
#include "ob_expr_extract_cert_expired_time.h"
#include "ob_expr_transaction_id.h"
#include "ob_expr_inner_row_cmp_val.h"
#include "ob_expr_last_refresh_scn.h"
#include "ob_expr_sql_udt_construct.h"
#include "ob_expr_priv_attribute_access.h"
#include "ob_expr_temp_table_ssid.h"
#include "ob_expr_priv_st_numinteriorrings.h"
#include "ob_expr_priv_st_iscollection.h"
#include "ob_expr_priv_st_equals.h"
#include "ob_expr_priv_st_touches.h"
#include "ob_expr_align_date4cmp.h"
#include "ob_expr_priv_st_makeenvelope.h"
#include "ob_expr_priv_st_clipbybox2d.h"
#include "ob_expr_priv_st_pointonsurface.h"
#include "ob_expr_priv_st_geometrytype.h"
#include "ob_expr_st_crosses.h"
#include "ob_expr_st_overlaps.h"
#include "ob_expr_st_union.h"
#include "ob_expr_st_length.h"
#include "ob_expr_st_difference.h"
#include "ob_expr_st_asgeojson.h"
#include "ob_expr_st_centroid.h"
#include "ob_expr_st_symdifference.h"
#include "ob_expr_priv_st_asmvtgeom.h"
#include "ob_expr_priv_st_makevalid.h"
#include "ob_expr_array.h"
#include "ob_expr_vec_vector.h"
#include "ob_expr_vec_key.h"
#include "ob_expr_vec_scn.h"
#include "ob_expr_vec_vid.h"
#include "ob_expr_vec_data.h"
#include "ob_expr_vec_type.h"
#include "ob_expr_spiv_dim.h"
#include "ob_expr_spiv_value.h"
#include "ob_expr_vector.h"
#include "ob_expr_func_ceil.h"
#include "ob_expr_topn_filter.h"
#include "ob_expr_sdo_relate.h"
#include "ob_expr_gtid.h"
#include "ob_expr_inner_table_option_printer.h"
#include "ob_expr_password.h"
#include "ob_expr_decode_trace_id.h"
#include "ob_expr_rb_build_empty.h"
#include "ob_expr_rb_is_empty.h"
#include "ob_expr_rb_build_varbinary.h"
#include "ob_expr_rb_to_varbinary.h"
#include "ob_expr_rb_cardinality.h"
#include "ob_expr_rb_calc_cardinality.h"
#include "ob_expr_rb_calc.h"
#include "ob_expr_rb_to_string.h"
#include "ob_expr_rb_from_string.h"
#include "ob_expr_rb_select.h"
#include "ob_expr_rb_build.h"
#include "ob_expr_array_contains.h"
#include "ob_expr_array_to_string.h"
#include "ob_expr_string_to_array.h"
#include "ob_expr_array_append.h"
#include "ob_expr_array_concat.h"
#include "ob_expr_array_difference.h"
#include "ob_expr_array_max.h"
#include "ob_expr_array_avg.h"
#include "ob_expr_array_compact.h"
#include "ob_expr_array_sort.h"
#include "ob_expr_array_sortby.h"
#include "ob_expr_array_filter.h"
#include "ob_expr_element_at.h"
#include "ob_expr_array_cardinality.h"
#include "ob_expr_audit_log_func.h"
#include "ob_expr_can_access_trigger.h"
#include "ob_expr_enhanced_aes_encrypt.h"
#include "ob_expr_split_part.h"
#include "ob_expr_inner_decode_like.h"
#include "ob_expr_inner_double_to_int.h"
#include "ob_expr_inner_decimal_to_year.h"
#include "ob_expr_array_overlaps.h"
#include "ob_expr_array_contains_all.h"
#include "ob_expr_array_distinct.h"
#include "ob_expr_array_remove.h"
#include "ob_expr_array_map.h"
#include "ob_expr_array_range.h"
#include "ob_expr_calc_odps_size.h"
#include "ob_expr_array_first.h"
#include "ob_expr_mysql_proc_info.h"
#include "ob_expr_get_mysql_routine_parameter_type_str.h"
#include "ob_expr_keyvalue.h"
#include "ob_expr_url_codec.h"
#include "ob_expr_ora_login_user.h"
#include "ob_expr_priv_st_geohash.h"
#include "ob_expr_priv_st_makepoint.h"
#include "ob_expr_to_pinyin.h"
#include "ob_expr_demote_cast.h"
#include "ob_expr_array_sum.h"
#include "ob_expr_array_length.h"
#include "ob_expr_array_position.h"
#include "ob_expr_array_slice.h"
#include "ob_expr_vec_ivf_center_id.h"
#include "ob_expr_vec_ivf_center_vector.h"
#include "ob_expr_vec_ivf_flat_data_vector.h"
#include "ob_expr_vec_ivf_meta_id.h"
#include "ob_expr_vec_ivf_meta_vector.h"
#include "ob_expr_vec_ivf_sq8_data_vector.h"
#include "ob_expr_vec_ivf_pq_center_id.h"
#include "ob_expr_vec_ivf_pq_center_ids.h"
#include "ob_expr_vec_ivf_pq_center_vector.h"
#include "ob_expr_bool.h"
#include "ob_expr_not_between.h"
#include "ob_expr_inner_info_cols_printer.h"
#include "ob_expr_array_except.h"
#include "ob_expr_array_intersect.h"
#include "ob_expr_array_union.h"
#include "ob_expr_map.h"
#include "ob_expr_rb_to_array.h"
#include "ob_expr_rb_contains.h"
#include "ob_expr_map_keys.h"
#include "ob_expr_current_catalog.h"
#include "ob_expr_check_catalog_access.h"
#include "ob_expr_check_location_access.h"
#include "ob_expr_tmp_file_open.h"
#include "ob_expr_tmp_file_write.h"
#include "ob_expr_tmp_file_read.h"
#include "ob_expr_tmp_file_close.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_complete.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_embed.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_rerank.h"
#include "ob_expr_local_dynamic_filter.h"
#include "ob_expr_format_profile.h"
#include "ob_expr_bucket.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_prompt.h"
#include "ob_expr_vector_similarity.h"

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
extern int calc_bool_expr_for_decint_type(const ObExpr &, ObEvalCtx &, ObDatum &);

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
extern int eval_questionmark_decint2nmb(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int eval_questionmark_nmb2decint_eqcast(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int eval_questionmark_decint2decint_eqcast(const ObExpr &, ObEvalCtx &, ObDatum &);
extern int eval_questionmark_decint2decint_normalcast(const ObExpr &, ObEvalCtx &, ObDatum &);

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
  ObExprAlignDate4Cmp::eval_align_date4cmp,                           /* 608 */
  ObExprJsonObjectStar::eval_ora_json_object_star,                    /* 609 */
  calc_bool_expr_for_decint_type,                                     /* 610 */
  ObExprIs::decimal_int_is_true,                                      /* 611 */
  ObExprIs::decimal_int_is_false,                                     /* 612 */
  ObExprIsNot::decimal_int_is_not_true,                               /* 613 */
  ObExprIsNot::decimal_int_is_not_false,                              /* 614 */
  ObExprInnerIsTrue::int_is_true_start,                               /* 615 */
  ObExprInnerIsTrue::int_is_true_end,                                 /* 616 */
  ObExprInnerIsTrue::float_is_true_start,                             /* 617 */
  ObExprInnerIsTrue::float_is_true_end,                               /* 618 */
  ObExprInnerIsTrue::double_is_true_start,                            /* 619 */
  ObExprInnerIsTrue::double_is_true_end,                              /* 620 */
  ObExprInnerIsTrue::number_is_true_start,                            /* 621 */
  ObExprInnerIsTrue::number_is_true_end,                              /* 622 */
  ObExprInnerDecodeLike::eval_inner_decode_like,                      /* 623 */
  ObExprJsonSchemaValid::eval_json_schema_valid,                      /* 624 */
  ObExprJsonSchemaValidationReport::eval_json_schema_validation_report, /* 625 */
  ObExprInsertChildXml::eval_insert_child_xml,                        /* 626 */
  ObExprDeleteXml::eval_delete_xml,                                   /* 627 */
  ObExprExtractValue::eval_mysql_extract_value,                       /* 628 */
  ObExprUpdateXml::eval_mysql_update_xml,                             /* 629 */
  ObExprXmlSequence::eval_xml_sequence,                               /* 630 */
  ObExprJsonAppend::eval_json_array_append,                           /* 631 */
  NULL, //unused                                                      /* 632 */
  ObExprUdtConstruct::eval_udt_construct,                             /* 633 */
  ObExprUDTAttributeAccess::eval_attr_access,                         /* 634 */
  ObExprPrivSTNumInteriorRings::eval_priv_st_numinteriorrings,        /* 635 */
  ObExprPrivSTIsCollection::eval_priv_st_iscollection,                /* 636 */
  ObExprPrivSTEquals::eval_priv_st_equals,                            /* 637 */
  ObExprPrivSTTouches::eval_priv_st_touches,                          /* 638 */
  ObExprPrivSTMakeEnvelope::eval_priv_st_makeenvelope,                /* 639 */
  ObExprPrivSTClipByBox2D::eval_priv_st_clipbybox2d,                  /* 640 */
  ObExprPrivSTPointOnSurface::eval_priv_st_pointonsurface,            /* 641 */
  ObExprPrivSTGeometryType::eval_priv_st_geometrytype,                /* 642 */
  ObExprSTCrosses::eval_st_crosses,                                   /* 643 */
  ObExprSTOverlaps::eval_st_overlaps,                                 /* 644 */
  ObExprSTUnion::eval_st_union,                                       /* 645 */
  ObExprSTLength::eval_st_length,                                     /* 646 */
  ObExprSTDifference::eval_st_difference,                             /* 647 */
  ObExprSTAsGeoJson::eval_st_asgeojson,                               /* 648 */
  ObExprSTCentroid::eval_st_centroid,                                 /* 649 */
  ObExprSTSymDifference::eval_st_symdifference,                       /* 650 */
  ObExprPrivSTAsMVTGeom::eval_priv_st_asmvtgeom,                      /* 651 */
  ObExprPrivSTMakeValid::eval_priv_st_makevalid,                      /* 652 */
  ObExprAuditLogSetFilter::eval_set_filter,                           /* 653 */
  ObExprAuditLogRemoveFilter::eval_remove_filter,                     /* 654 */
  ObExprAuditLogSetUser::eval_set_user,                               /* 655 */
  ObExprAuditLogRemoveUser::eval_remove_user,                         /* 656 */
  eval_questionmark_decint2nmb,                                       /* 657 */
  eval_questionmark_nmb2decint_eqcast,                                /* 658 */
  eval_questionmark_decint2decint_eqcast,                             /* 659 */
  eval_questionmark_decint2decint_normalcast,                         /* 660 */
  ObExprExtractExpiredTime::eval_extract_cert_expired_time,           /* 661 */
  ObExprXmlConcat::eval_xml_concat,                                   /* 662 */
  ObExprXmlForest::eval_xml_forest,                                   /* 663 */
  ObExprExistsNodeXml::eval_existsnode_xml,                           /* 664 */
  ObExprPassword::eval_password,                                      /* 665 */
  ObExprDocID::generate_doc_id,                                       /* 666 */
  ObExprWordSegment::generate_fulltext_column,                        /* 667 */
  ObExprWordCount::generate_word_count,                               /* 668 */
  ObExprBM25::eval_bm25_relevance_expr,                               /* 669 */
  ObExprTransactionId::eval_transaction_id,                           /* 670 */
  ObExprInnerTableOptionPrinter::eval_inner_table_option_printer,     /* 671 */
  ObExprInnerTableSequenceGetter::eval_inner_table_sequence_getter,   /* 672 */
  ObExprDecodeTraceId::calc_decode_trace_id_expr,                     /* 673 */
  ObExprInnerRowCmpVal::eval_inner_row_cmp_val,                       /* 674 */
  ObExprIs::json_is_true,                                             /* 675 */
  ObExprIs::json_is_false,                                            /* 676 */
  ObExprCurrentRole::eval_current_role,                               /* 677 */
  ObExprMod::mod_decimalint,                                          /* 678 */
  ObExprPrivSTGeoHash::eval_priv_st_geohash,                          /* 679 */
  ObExprPrivSTMakePoint::eval_priv_st_makepoint,                      /* 680 */
  ObExprGetLock::get_lock,                                            /* 681 */
  ObExprIsFreeLock::is_free_lock,                                     /* 682 */
  ObExprIsUsedLock::is_used_lock,                                     /* 683 */
  ObExprReleaseLock::release_lock,                                    /* 684 */
  ObExprReleaseAllLocks::release_all_locks,                           /* 685 */
  ObExprGTIDSubset::eval_subset,                                      /* 686 */
  ObExprGTIDSubtract::eval_subtract,                                  /* 687 */
  ObExprWaitForExecutedGTIDSet::eval_wait_for_executed_gtid_set,      /* 688 */
  ObExprWaitUntilSQLThreadAfterGTIDs::eval_wait_until_sql_thread_after_gtids, /* 689 */
  ObExprLastRefreshScn::eval_last_refresh_scn,                        /* 690 */
  ObExprDocLength::generate_doc_length,                               /* 691 */
  ObExprTopNFilter::eval_topn_filter,                                 /* 692 */
  ObExprIsEnabledRole::eval_is_enabled_role,                          /* 693 */
  ObExprCanAccessTrigger::can_access_trigger,                         /* 694 */
  ObRelationalExprOperator::eval_min_max_compare,                     /* 695 */
  ObRelationalExprOperator::min_max_row_eval,                         /* 696 */
  ObExprRbBuildEmpty::eval_rb_build_empty,                            /* 697 */
  ObExprRbIsEmpty::eval_rb_is_empty,                                  /* 698 */
  ObExprRbBuildVarbinary::eval_rb_build_varbinary,                    /* 699 */
  ObExprRbToVarbinary::eval_rb_to_varbinary,                          /* 700 */
  ObExprRbCardinality::eval_rb_cardinality,                           /* 701 */
  ObExprRbAndCardinality::eval_rb_and_cardinality,                    /* 702 */
  ObExprRbOrCardinality::eval_rb_or_cardinality,                      /* 703 */
  ObExprRbXorCardinality::eval_rb_xor_cardinality,                    /* 704 */
  ObExprRbAndnotCardinality::eval_rb_andnot_cardinality,              /* 705 */
  ObExprRbAndNull2emptyCardinality::eval_rb_and_null2empty_cardinality, /* 706 */
  ObExprRbOrNull2emptyCardinality::eval_rb_or_null2empty_cardinality, /* 707 */
  ObExprRbAndnotNull2emptyCardinality::eval_rb_andnot_null2empty_cardinality, /* 708 */
  ObExprRbAnd::eval_rb_and,                                           /* 709 */
  ObExprRbOr::eval_rb_or,                                             /* 710 */
  ObExprRbXor::eval_rb_xor,                                           /* 711 */
  ObExprRbAndnot::eval_rb_andnot,                                     /* 712 */
  ObExprRbAndNull2empty::eval_rb_and_null2empty,                      /* 713 */
  ObExprRbOrNull2empty::eval_rb_or_null2empty,                        /* 714 */
  ObExprRbAndnotNull2empty::eval_rb_andnot_null2empty,                /* 715 */
  ObExprSdoRelate::eval_sdo_relate,                                   /* 716 */
  ObExprRbToString::eval_rb_to_string,                                /* 717 */
  ObExprRbFromString::eval_rb_from_string,                            /* 718 */
  NULL, // ObExprRbIterate::eval_rb_iterate,                          /* 719 */
  ObExprArray::eval_array,                                            /* 720 */
  ObExprVectorL1Distance::calc_l1_distance,                           /* 721 */
  ObExprVectorL2Distance::calc_l2_distance,                           /* 722 */
  ObExprVectorCosineDistance::calc_cosine_distance,                   /* 723 */
  ObExprVectorIPDistance::calc_inner_product,                         /* 724 */
  ObExprVectorDims::calc_dims,                                        /* 725 */
  ObExprVectorNorm::calc_norm,                                        /* 726 */
  ObExprVectorDistance::calc_distance,                                /* 727 */
  ObExprInnerDoubleToInt::eval_inner_double_to_int,                   /* 728 */
  ObExprInnerDecimalToYear::eval_inner_decimal_to_year,               /* 729 */
  ObExprSm3::eval_sm3,                                                /* 730 */
  ObExprSm4Encrypt::eval_sm4_encrypt,                                 /* 731 */
  ObExprSm4Decrypt::eval_sm4_decrypt,                                 /* 732 */
  NULL, // ObExprAdd::add_vec_vec,                                    /* 733 */
  NULL, // ObExprMinus::minus_vec_vec,                                /* 734 */
  ObExprMul::mul_vec_vec,                                             /* 735 */
  ObExprDiv::div_vec,                                                 /* 736 */
  ObExprVecKey::generate_vec_key,                                     /* 737 */
  ObExprVecScn::generate_vec_scn,                                     /* 738 */
  ObExprVecVid::generate_vec_id,                                      /* 739 */
  ObExprVecData::generate_vec_data,                                   /* 740 */
  ObExprVecType::generate_vec_type,                                   /* 741 */
  ObExprVecVector::generate_vec_vector,                               /* 742 */
  ObExprRegexp::eval_hs_regexp,                                       /* 743 */
  ObExprRegexpCount::eval_hs_regexp_count,                            /* 744 */
  ObExprRegexpInstr::eval_hs_regexp_instr,                            /* 745 */
  ObExprRegexpLike::eval_hs_regexp_like,                              /* 746 */
  ObExprRegexpReplace::eval_hs_regexp_replace,                        /* 747 */
  ObExprRegexpSubstr::eval_hs_regexp_substr,                          /* 748 */
  ObExprColumnConv::column_convert_fast,                              /* 749 */
  ObExprArrayContains::eval_array_contains_int64_t,                   /* 750 */
  ObExprArrayContains::eval_array_contains_float,                     /* 751 */
  ObExprArrayContains::eval_array_contains_double,                    /* 752 */
  ObExprArrayContains::eval_array_contains_ObString,                  /* 753 */
  ObExprArrayContains::eval_array_contains_array,                     /* 754 */
  ObExprSplitPart::calc_split_part_expr,                              /* 755 */
  ObExprRbSelect::eval_rb_select,                                     /* 756 */
  ObExprVectorNegativeIPDistance::calc_negative_inner_product,        /* 757 */
  ObExprTokenize::eval_tokenize,                                      /* 758 */
  ObExprEnhancedAesEncrypt::eval_aes_encrypt,                         /* 759 */
  ObExprEnhancedAesDecrypt::eval_aes_decrypt,                         /* 760 */
  ObExprMysqlProcInfo::eval_mysql_proc_info,                          /* 761 */
  ObExprArrayOverlaps::eval_array_overlaps,                           /* 762 */
  ObExprArrayContainsAll::eval_array_contains_all,                    /* 763 */
  ObExprInnerIsTrue::decimal_int_is_true_start,                       /* 764 */
  ObExprInnerIsTrue::decimal_int_is_true_end,                         /* 765 */
  ObExprInnerIsTrue::json_is_true_start,                              /* 766 */
  ObExprInnerIsTrue::json_is_true_end,                                /* 767 */
  ObExprGetMySQLRoutineParameterTypeStr::get_mysql_routine_parameter_type_str, /* 768 */
  ObExprArrayDistinct::eval_array_distinct,                           /* 769 */
  ObExprArrayRemove::eval_array_remove_int64_t,                       /* 770 */
  ObExprArrayRemove::eval_array_remove_float,                         /* 771 */
  ObExprArrayRemove::eval_array_remove_double,                        /* 772 */
  ObExprArrayRemove::eval_array_remove_ObString,                      /* 773 */
  ObExprArrayRemove::eval_array_remove_array,                         /* 774 */
  ObExprArrayMap::eval_array_map,                                     /* 775 */
  ObExprOraLoginUser::eval_ora_login_user,                            /* 776 */
  ObExprArrayToString::eval_array_to_string,                          /* 777 */
  ObExprStringToArray::eval_string_to_array,                          /* 778 */
  ObExprArrayAppend::eval_array_append,                               /* 779 */
  ObExprElementAt::eval_element_at,                                   /* 780 */
  ObExprArrayCardinality::eval_array_cardinality,                     /* 781 */
  ObExprRbBuild::eval_rb_build,                                       /* 782 */
  ObExprArrayPrepend::eval_array_prepend,                             /* 783 */
  ObExprArrayConcat::eval_array_concat,                               /* 784 */
  ObExprArrayDifference::eval_array_difference,                       /* 785 */
  ObExprArrayFirst::eval_array_first,                                 /* 786 */
  NULL, // ObExprCalcPartitionName::get_partition_name,               /* 787 */
  NULL, // ObExprCalcSubPartitionName::get_sub_partition_name,        /* 788 */
  NULL, // ObExprCalcPartitionIdx::get_partition_idx,                 /* 789 */
  NULL, // ObExprCalcSubPartitionIdx::get_sub_partition_idx,          /* 790 */
  ObExprCalcOdpsSize::calc_odps_size,                                 /* 791 */
  ObExprVecIVFCenterID::calc_center_id,                               /* 792 */
  ObExprVecIVFCenterVector::generate_center_vector,                   /* 793 */
  ObExprVecIVFFlatDataVector::generate_data_vector,                   /* 794 */
  ObExprVecIVFSQ8DataVector::generate_data_vector,                    /* 795 */
  ObExprVecIVFMetaID::generate_meta_id,                               /* 796 */
  ObExprVecIVFMetaVector::generate_meta_vector,                       /* 797 */
  ObExprVecIVFPQCenterId::generate_pq_center_id,                      /* 798 */
  ObExprVecIVFPQCenterIds::calc_pq_center_ids,                        /* 799 */
  ObExprArrayMax::eval_array_max,                                     /* 800 */
  ObExprArrayMin::eval_array_min,                                     /* 801 */
  ObExprArrayAvg::eval_array_avg,                                     /* 802 */
  ObExprArraySum::eval_array_sum,                                     /* 803 */
  ObExprArrayCompact::eval_array_compact,                             /* 804 */
  ObExprArraySort::eval_array_sort,                                   /* 805 */
  ObExprKeyValue::calc_key_value_expr,                                /* 806 */
  ObExprToChar::eval_to_char,                                         /* 807 */
  ObExprToPinyin::eval_to_pinyin,                                     /* 808 */
  ObExprArraySlice::eval_array_slice,                                 /* 809 */
  ObExprArraySortby::eval_array_sortby,                               /* 810 */
  ObExprArrayFilter::eval_array_filter,                               /* 811 */
  ObExprArrayLength::eval_array_length,                               /* 812 */
  ObExprArrayRange::eval_array_range,                                 /* 813 */ // FARM COMPAT WHITELIST
  ObExprArrayPosition::eval_array_position,                           /* 814 */
  ObExprURLEncode::eval_url_encode,                                   /* 815 */
  ObExprURLDecode::eval_url_decode,                                   /* 816 */
  ObExprVecIVFPQCenterVector::generate_pq_center_vector,              /* 817 */
  ObExprDemoteCast::eval_demoted_val,                                 /* 818 */
  ObExprRangePlacement::eval_range_placement,                         /* 819 */
  ObExprInnerTypeToEnumSet::eval_inner_type_to_enumset,               /* 820 */
  ObExprIsNot::json_is_not_false,                                     /* 821 */
  ObExprIsNot::json_is_not_true,                                      /* 822 */
  ObExprArrayExcept::eval_array_except,                               /* 823 */
  ObExprArrayIntersect::eval_array_intersect,                         /* 824 */
  ObExprArrayUnion::eval_array_union,                                 /* 825 */
  NULL, // ObExprArrayReplace::eval_array_replace,                    /* 826 */
  NULL, // ObExprArrayPopfront::eval_array_popfront,                  /* 827 */
  NULL, // ObExprCurrentCatalog::eval_current_catalog,                /* 828 */
  ObExprInnerInfoColsColumnDefPrinter::eval_column_def,               /* 829 */
  ObExprInnerInfoColsCharLenPrinter::eval_column_char_len,            /* 830 */
  ObExprInnerInfoColsCharNamePrinter::eval_column_char_name,          /* 831 */
  ObExprInnerInfoColsCollNamePrinter::eval_column_collation_name,     /* 832 */
  ObExprInnerInfoColsPrivPrinter::eval_column_priv,                   /* 833 */
  ObExprInnerInfoColsExtraPrinter::eval_column_extra,                 /* 834 */
  ObExprInnerInfoColsDataTypePrinter::eval_column_data_type,          /* 835 */
  ObExprInnerInfoColsColumnTypePrinter::eval_column_column_type,      /* 836 */
  NULL, // ObExprRbOrCardinalityAggregate::eval_rb_or_cardinality_aggregate, /* 837 */
  NULL, // ObExprRbAndCardinalityAggregate::eval_rb_and_cardinality_aggregate, /* 838 */
  ObExprRbToArray::eval_rb_to_array,                                   /* 839 */
  ObExprRbContains::eval_rb_contains,                                  /* 840 */
  ObExprCurrentCatalog::eval_current_catalog,                          /* 841 */
  ObExprCheckCatalogAccess::eval_check_catalog_access,                 /* 842 */
  ObExprMap::eval_map,                                                 /* 843 */
  ObExprSpivValue::generate_spiv_value,                                /* 844 */
  ObExprMapKeys::eval_map_keys,                                        /* 845 */
  ObExprMapValues::eval_map_values,                                    /* 846 */
  ObExprSpivDim::generate_spiv_dim,                                    /* 847 */
  ObExprInnerInfoColsColumnKeyPrinter::eval_column_column_key,         /* 848 */
  ObExprCheckLocationAccess::eval_check_location_access,               /* 849 */
  ObExprUDF::eval_external_udf,                                        /* 850 */
  ObExprStartUpMode::eval_startup_mode,                                /* 851 */
  ObExprVectorL2Squared::calc_l2_squared,                              /* 852 */
#if defined(ENABLE_DEBUG_LOG) || !defined(NDEBUG)
  ObExprTmpFileOpen::eval_tmp_file_open,                              /* 853 */
  ObExprTmpFileClose::eval_tmp_file_close,                            /* 854 */
  ObExprTmpFileWrite::eval_tmp_file_write,                            /* 855 */
  ObExprTmpFileRead::eval_tmp_file_read,                              /* 856 */
#else
  NULL,// ObExprTmpFileOpen::eval_tmp_file_open,                      /* 853 */
  NULL,// ObExprTmpFileClose::eval_tmp_file_close,                    /* 854 */
  NULL,// ObExprTmpFileWrite::eval_tmp_file_write,                    /* 855 */
  NULL,// ObExprTmpFileRead::eval_tmp_file_read,                      /* 856 */
#endif
  ObExprAIComplete::eval_ai_complete,                                 /* 857 */
  ObExprAIEmbed::eval_ai_embed,                                       /* 858 */
  ObExprAIRerank::eval_ai_rerank,                                     /* 859 */
  NULL,//ObExprMd5ConcatWs::calc_md5_concat_ws_expr                   /* 860 */
  ObExprUDF::eval_mysql_udtf,                                         /* 861 */
  NULL,//ObExprHiddenClusteringKey::eval_hidden_clustering_key,       /* 862 */
  ObExprFormatProfile::format_profile,                                /* 863 */
  ObExprLocalDynamicFilter::eval_local_dynamic_filter,                /* 864 */
  NULL, // ObExprVecChunk::generate_vec_chunk,                        /* 865 */
  NULL, // ObExprEmbeddedVec::generate_embedded_vec,                  /* 866 */
  NULL, // ObExprSemanticDistance::calc_semantic_distance,            /* 867 */
  ObExprBucket::calc_bucket_expr,                                     /* 868 */
  NULL, // ObExprSemanticVectorDistance::calc_semantic_vector_distance, /* 869 */
  ObExprAIPrompt::eval_ai_prompt,                                     /* 870 */
  ObExprVectorL2Similarity::calc_l2_similarity,                       /* 871 */
  ObExprVectorCosineSimilarity::calc_cosine_similarity,               /* 872 */
  ObExprVectorIPSimilarity::calc_ip_similarity,                       /* 873 */
  ObExprVectorSimilarity::calc_similarity,                            /* 874 */
  NULL, // ObExprVecVisible::generate_vec_visible                     /* 875 */
  NULL, // ObExprArrayContains::eval_array_contains_int32_t           /* 876 */
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
  calc_sqrt_expr_oracle_number_in_batch,                              /* 116 */
  ObBatchCast::explicit_batch_cast<ObDecimalIntTC, ObDecimalIntTC>,   /* 117 */
  ObBatchCast::implicit_batch_cast<ObDecimalIntTC, ObDecimalIntTC>,   /* 118 */
  ObBatchCast::explicit_batch_cast<ObIntTC, ObDecimalIntTC>,          /* 119 */
  ObBatchCast::implicit_batch_cast<ObIntTC, ObDecimalIntTC>,          /* 120 */
  ObBatchCast::explicit_batch_cast<ObUIntTC, ObDecimalIntTC>,         /* 121 */
  ObBatchCast::implicit_batch_cast<ObUIntTC, ObDecimalIntTC>,         /* 122 */
  ObBatchCast::explicit_batch_cast<ObDecimalIntTC, ObIntTC>,          /* 123 */
  ObBatchCast::implicit_batch_cast<ObDecimalIntTC, ObIntTC>,          /* 124 */
  ObBatchCast::explicit_batch_cast<ObDecimalIntTC, ObUIntTC>,         /* 125 */
  ObBatchCast::implicit_batch_cast<ObDecimalIntTC, ObUIntTC>,         /* 126 */
  ObBatchCast::explicit_batch_cast<ObDecimalIntTC, ObNumberTC>,       /* 127 */
  ObBatchCast::implicit_batch_cast<ObDecimalIntTC, ObNumberTC>,       /* 128 */
  ObExprDecodeTraceId::calc_decode_trace_id_expr_batch,               /* 129 */
  ObExprTopNFilter::eval_topn_filter_batch,                           /* 130 */
  ObRelationalExprOperator::eval_batch_min_max_compare,               /* 131 */
  ObExprBM25::eval_batch_bm25_relevance_expr,                  /* 132 */
  NULL,// ObExprAdd::add_vec_vec_batch,                               /* 133 */
  NULL,// ObExprMinus::minus_vec_vec_batch,                           /* 134 */
  ObExprMul::mul_vec_vec_batch,                                       /* 135 */
  ObExprDiv::div_vec_batch,                                           /* 136 */
  ObExprColumnConv::column_convert_batch,                             /* 137 */
  ObExprColumnConv::column_convert_batch_fast,                        /* 138 */
  ObExprArrayContains::eval_array_contains_batch_int64_t,             /* 139 */
  ObExprArrayContains::eval_array_contains_batch_float,               /* 140 */
  ObExprArrayContains::eval_array_contains_batch_double,              /* 141 */
  ObExprArrayContains::eval_array_contains_batch_ObString,            /* 142 */
  ObExprArrayContains::eval_array_contains_array_batch,               /* 143 */
  ObExprArrayOverlaps::eval_array_overlaps_batch,                     /* 144 */
  ObExprArrayContainsAll::eval_array_contains_all_batch,              /* 145 */
  ObExprArrayDistinct::eval_array_distinct_batch,                     /* 146 */
  ObExprArrayRemove::eval_array_remove_batch_int64_t,                 /* 147 */
  ObExprArrayRemove::eval_array_remove_batch_float,                   /* 148 */
  ObExprArrayRemove::eval_array_remove_batch_double,                  /* 149 */
  ObExprArrayRemove::eval_array_remove_batch_ObString,                /* 150 */
  ObExprArrayRemove::eval_array_remove_array_batch,                   /* 151 */
  ObExprArrayToString::eval_array_to_string_batch,                    /* 152 */
  ObExprStringToArray::eval_string_to_array_batch,                    /* 153 */
  ObExprArrayAppend::eval_array_append_batch,                         /* 154 */
  ObExprElementAt::eval_element_at_batch,                             /* 155 */
  ObExprArrayCardinality::eval_array_cardinality_batch,               /* 156 */
  ObExprArrayPrepend::eval_array_prepend_batch,                       /* 157 */
  ObExprArrayConcat::eval_array_concat_batch,                         /* 158 */
  ObExprArrayDifference::eval_array_difference_batch,                 /* 159 */
  ObExprArrayMax::eval_array_max_batch,                               /* 160 */
  ObExprArrayMin::eval_array_min_batch,                               /* 161 */
  ObExprArrayAvg::eval_array_avg_batch,                               /* 162 */
  ObExprArraySum::eval_array_sum_batch,                               /* 163 */
  ObExprArrayCompact::eval_array_compact_batch,                       /* 164 */
  ObExprArraySort::eval_array_sort_batch,                             /* 165 */
  ObExprToPinyin::eval_to_pinyin_batch,                               /* 166 */
  ObExprArraySlice::eval_array_slice_batch,                           /* 167 */
  ObExprArrayLength::eval_array_length_batch,                         /* 168 */
  NULL,// ObExprRange::eval_range_batch,                              /* 169 */
  ObExprArrayPosition::eval_array_position_batch,                     /* 170*/
  ObExprURLEncode::eval_url_encode_batch,                             /* 171 */
  ObExprURLDecode::eval_url_decode_batch,                             /* 172 */
  ObExprArrayExcept::eval_array_except_batch,                         /* 173 */
  ObExprArrayIntersect::eval_array_intersect_batch,                   /* 174 */
  ObExprArrayUnion::eval_array_union_batch,                           /* 175 */
  NULL, // ObExprArrayReplace::eval_array_replace_batch,              /* 176 */
  NULL, // ObExprArrayPopfront::eval_array_popfront_batch,            /* 177 */
  ObExprUDF::eval_udf_batch,                                          /* 178 */
  ObExprLocalDynamicFilter::eval_local_dynamic_filter_batch,          /* 179 */
  NULL, // ObExprArrayContains::eval_array_contains_batch_int32_t     /* 180 */
};

static ObExpr::EvalVectorFunc g_expr_eval_vector_functions[] = {
  expr_default_eval_vector_func,                                /* 0 */
  ObExprSin::eval_double_sin_vector,                            /* 1 */
  ObExprSin::eval_number_sin_vector,                            /* 2 */
  ObExprFuncPartKey::calc_partition_key_vector,                 /* 3 */
  ObExprAdd::add_int_int_vector,                                /* 4 */
  ObExprAdd::add_int_uint_vector,                               /* 5 */
  ObExprAdd::add_uint_int_vector,                               /* 6 */
  ObExprAdd::add_uint_uint_vector,                              /* 7 */
  ObExprAdd::add_float_float_vector,                            /* 8 */
  ObExprAdd::add_double_double_vector,                          /* 9 */
  ObExprAdd::add_decimalint32_oracle_vector,                    /* 10 */
  ObExprAdd::add_decimalint64_oracle_vector,                    /* 11 */
  ObExprAdd::add_decimalint128_oracle_vector,                   /* 12 */
  ObExprAdd::add_number_number_vector,                          /* 13 */
  ObExprAdd::add_decimalint32_vector,                           /* 14 */
  ObExprAdd::add_decimalint64_vector,                           /* 15 */
  ObExprAdd::add_decimalint128_vector,                          /* 16 */
  ObExprAdd::add_decimalint256_vector,                          /* 17 */
  ObExprAdd::add_decimalint512_vector,                          /* 18 */
  ObExprAdd::add_decimalint512_with_check_vector,               /* 19 */
  ObExprMinus::minus_int_int_vector,                            /* 20 */
  ObExprMinus::minus_int_uint_vector,                           /* 21 */
  ObExprMinus::minus_uint_uint_vector,                          /* 22 */
  ObExprMinus::minus_uint_int_vector,                           /* 23 */
  ObExprMinus::minus_float_float_vector,                        /* 24 */
  ObExprMinus::minus_double_double_vector,                      /* 25 */
  ObExprMinus::minus_number_number_vector,                      /* 26 */
  ObExprMinus::minus_decimalint32_vector,                       /* 27 */
  ObExprMinus::minus_decimalint64_vector,                       /* 28 */
  ObExprMinus::minus_decimalint128_vector,                      /* 29 */
  ObExprMinus::minus_decimalint256_vector,                      /* 30 */
  ObExprMinus::minus_decimalint512_vector,                      /* 31 */
  ObExprMinus::minus_decimalint512_with_check_vector,           /* 32 */
  ObExprMinus::minus_decimalint32_oracle_vector,                /* 33 */
  ObExprMinus::minus_decimalint64_oracle_vector,                /* 34 */
  ObExprMinus::minus_decimalint128_oracle_vector,               /* 35 */
  ObExprMul::mul_int_int_vector,                                /* 36 */
  ObExprMul::mul_int_uint_vector,                               /* 37 */
  ObExprMul::mul_uint_int_vector,                               /* 38 */
  ObExprMul::mul_uint_uint_vector,                              /* 39 */
  ObExprMul::mul_float_vector,                                  /* 40 */
  ObExprMul::mul_double_vector,                                 /* 41 */
  ObExprMul::mul_number_vector,                                 /* 42 */
  ObExprMul::mul_decimalint32_int32_int32_vector,               /* 43 */
  ObExprMul::mul_decimalint64_int32_int32_vector,               /* 44 */
  ObExprMul::mul_decimalint64_int32_int64_vector,               /* 45 */
  ObExprMul::mul_decimalint64_int64_int32_vector,               /* 46 */
  ObExprMul::mul_decimalint128_int32_int64_vector,              /* 47 */
  ObExprMul::mul_decimalint128_int64_int32_vector,              /* 48 */
  ObExprMul::mul_decimalint128_int32_int128_vector,             /* 49 */
  ObExprMul::mul_decimalint128_int128_int32_vector,             /* 50 */
  ObExprMul::mul_decimalint128_int64_int64_vector,              /* 51 */
  ObExprMul::mul_decimalint128_int64_int128_vector,             /* 52 */
  ObExprMul::mul_decimalint128_int128_int64_vector,             /* 53 */
  ObExprMul::mul_decimalint128_int128_int128_vector,            /* 54 */
  ObExprMul::mul_decimalint256_int32_int128_vector,             /* 55 */
  ObExprMul::mul_decimalint256_int128_int32_vector,             /* 56 */
  ObExprMul::mul_decimalint256_int32_int256_vector,             /* 57 */
  ObExprMul::mul_decimalint256_int256_int32_vector,             /* 58 */
  ObExprMul::mul_decimalint256_int64_int128_vector,             /* 59 */
  ObExprMul::mul_decimalint256_int128_int64_vector,             /* 60 */
  ObExprMul::mul_decimalint256_int64_int256_vector,             /* 61 */
  ObExprMul::mul_decimalint256_int256_int64_vector,             /* 62 */
  ObExprMul::mul_decimalint256_int128_int128_vector,            /* 63 */
  ObExprMul::mul_decimalint256_int128_int256_vector,            /* 64 */
  ObExprMul::mul_decimalint256_int256_int128_vector,            /* 65 */
  ObExprMul::mul_decimalint512_int512_int512_vector,            /* 66 */
  ObExprMul::mul_decimalint512_with_check_vector,               /* 67 */
  ObExprMul::mul_decimalint64_round_vector,                     /* 68 */
  ObExprMul::mul_decimalint128_round_vector,                    /* 69 */
  ObExprMul::mul_decimalint256_round_vector,                    /* 70 */
  ObExprMul::mul_decimalint512_round_vector,                    /* 71 */
  ObExprMul::mul_decimalint512_round_with_check_vector,         /* 72 */
  ObExprMul::mul_decimalint32_int32_int32_oracle_vector,        /* 73 */
  ObExprMul::mul_decimalint64_int32_int32_oracle_vector,        /* 74 */
  ObExprMul::mul_decimalint64_int32_int64_oracle_vector,        /* 75 */
  ObExprMul::mul_decimalint64_int64_int32_oracle_vector,        /* 76 */
  ObExprMul::mul_decimalint128_int32_int64_oracle_vector,       /* 77 */
  ObExprMul::mul_decimalint128_int64_int32_oracle_vector,       /* 78 */
  ObExprMul::mul_decimalint128_int32_int128_oracle_vector,      /* 79 */
  ObExprMul::mul_decimalint128_int128_int32_oracle_vector,      /* 80 */
  ObExprMul::mul_decimalint128_int64_int64_oracle_vector,       /* 81 */
  ObExprMul::mul_decimalint128_int64_int128_oracle_vector,      /* 82 */
  ObExprMul::mul_decimalint128_int128_int64_oracle_vector,      /* 83 */
  ObExprMul::mul_decimalint128_int128_int128_oracle_vector,     /* 84 */
  ObExprDiv::div_float_vector,                                  /* 85 */
  ObExprDiv::div_double_vector,                                 /* 86 */
  ObExprDiv::div_number_vector,                                 /* 87 */
  ObExprAnd::eval_and_vector,                                   /* 88 */
  ObExprOr::eval_or_vector,                                     /* 89 */
  ObExprJoinFilter::eval_bloom_filter_vector,                   /* 90 */
  ObExprJoinFilter::eval_range_filter_vector,                   /* 91 */
  ObExprJoinFilter::eval_in_filter_vector,                      /* 92 */
  ObExprCalcPartitionBase::calc_partition_level_one_vector,     /* 93 */
  ObExprBetween::eval_between_vector,                           /* 94 */
  ObExprLength::calc_mysql_length_vector,                       /* 95 */
  ObExprJoinFilter::eval_in_filter_vector,                      /* 96 */
  ObExprSubstr::eval_substr_vector,                             /* 97 */
  ObExprLower::eval_lower_vector,                               /* 98 */
  ObExprUpper::eval_upper_vector,                               /* 99 */
  ObExprCase::eval_case_vector,                                 /* 100 */
  ObExprFuncRound::calc_round_expr_numeric1_vector,             /* 101 */
  ObExprFuncRound::calc_round_expr_numeric2_vector,             /* 102 */
  ObExprFuncRound::calc_round_expr_datetime1_vector,            /* 103 */
  ObExprFuncRound::calc_round_expr_datetime2_vector,            /* 104 */
  ObExprLike::eval_like_expr_vector_only_text_vectorized,       /* 105 */
  ObExprExtract::calc_extract_oracle_vector,                    /* 106 */
  ObExprExtract::calc_extract_mysql_vector,                     /* 107 */
  ObExprRegexpReplace::eval_regexp_replace_vector,              /* 108 */
  ObExprInOrNotIn::eval_vector_in_without_row_fallback,         /* 109 */
  ObExprInOrNotIn::eval_vector_in_without_row,                  /* 110 */
  NULL,//ObExprDecodeTraceId::calc_decode_trace_id_expr_vector  /* 111 */
  ObExprTopNFilter::eval_topn_filter_vector,                    /* 112 */
  ObRelationalExprOperator::eval_vector_min_max_compare,        /* 113 */
  ObExprCeilFloor::calc_ceil_floor_vector,                      /* 114 */
  ObExprRepeat::eval_repeat_vector,                             /* 115 */
  ObExprRegexpReplace::eval_hs_regexp_replace_vector,           /* 116 */
  ObExprArrayContains::eval_array_contains_vector_int64_t,      /* 117 */
  ObExprArrayContains::eval_array_contains_vector_float,        /* 118 */
  ObExprArrayContains::eval_array_contains_vector_double,       /* 119 */
  ObExprArrayContains::eval_array_contains_vector_ObString,     /* 120 */
  ObExprArrayContains::eval_array_contains_array_vector,        /* 121 */
  ObExprCalcPartitionBase::fast_calc_partition_level_one_vector,/* 122 */
  ObExprTrim::eval_trim_vector,                                 /* 123 */
  NULL, // ObExprEncodeSortkey::eval_encode_sortkey_vector      /* 124 */
  ObExprArrayOverlaps::eval_array_overlaps_vector,              /* 125 */
  ObExprArrayContainsAll::eval_array_contains_all_vector,       /* 126 */
  ObBitwiseExprOperator::calc_bitwise_result2_mysql_vector,     /* 127 */
  ObBitwiseExprOperator::calc_bitwise_result2_oracle_vector,    /* 128 */
  ObExprDiv::decint_div_mysql_vec_fn<int32_t, int32_t>,         /* 129 */
  ObExprDiv::decint_div_mysql_vec_fn<int64_t, int32_t>,         /* 130 */
  ObExprDiv::decint_div_mysql_vec_fn<int64_t, int64_t>,         /* 131 */
  ObExprDiv::decint_div_mysql_vec_fn<int128_t, int32_t>,        /* 132 */
  ObExprDiv::decint_div_mysql_vec_fn<int128_t, int64_t>,        /* 133 */
  ObExprDiv::decint_div_mysql_vec_fn<int128_t, int128_t>,       /* 134 */
  ObExprDiv::decint_div_mysql_vec_fn<int256_t, int32_t>,        /* 135 */
  ObExprDiv::decint_div_mysql_vec_fn<int256_t, int64_t>,        /* 136 */
  ObExprDiv::decint_div_mysql_vec_fn<int256_t, int128_t>,       /* 137 */
  ObExprDiv::decint_div_mysql_vec_fn<int256_t, int256_t>,       /* 138 */
  ObExprDiv::decint_div_mysql_vec_fn<int512_t, int32_t>,        /* 139 */
  ObExprDiv::decint_div_mysql_vec_fn<int512_t, int64_t>,        /* 140 */
  ObExprDiv::decint_div_mysql_vec_fn<int512_t, int128_t>,       /* 141 */
  ObExprDiv::decint_div_mysql_vec_fn<int512_t, int256_t>,       /* 142 */
  ObExprDiv::decint_div_mysql_vec_fn<int512_t, int512_t>,       /* 143 */
  ObExprArrayRemove::eval_array_remove_vector_int64_t,          /* 144 */
  ObExprArrayRemove::eval_array_remove_vector_float,            /* 145 */
  ObExprArrayRemove::eval_array_remove_vector_double,           /* 146 */
  ObExprArrayRemove::eval_array_remove_vector_ObString,         /* 147 */
  ObExprArrayRemove::eval_array_remove_array_vector,            /* 148 */
  ObExprArrayDistinct::eval_array_distinct_vector,              /* 149 */
  ObExprDateFormat::calc_date_format_vector,                    /* 150 */
  ObExprYear::calc_year_vector,                                 /* 151 */
  ObExprMonth::calc_month_vector,                               /* 152 */
  ObExprMonthName::calc_month_name_vector,                      /* 153 */
  ObExprHour::calc_hour_vector,                                 /* 154 */
  ObExprMinute::calc_minute_vector,                             /* 155 */
  ObExprDayOfYear::calc_dayofyear_vector,                       /* 156 */
  ObExprDayOfMonth::calc_dayofmonth_vector,                     /* 157 */
  ObExprDayOfWeek::calc_dayofweek_vector,                       /* 158 */
  ObExprDayName::calc_dayname_vector,                           /* 159 */
  ObExprWeek::calc_week_vector,                                 /* 160 */
  ObExprWeekOfYear::calc_weekofyear_vector,                     /* 161 */
  ObExprDate::eval_date_vector,                                 /* 162 */
  ObExprDateDiff::eval_date_diff_vector,                        /* 163 */
  ObExprDateAdd::calc_date_add_vector,                          /* 164 */
  ObExprDateSub::calc_date_sub_vector,                          /* 165 */
  ObExprFromDays::calc_fromdays_vector,                         /* 166 */
  ObExprTimeStampDiff::eval_timestamp_diff_vector,              /* 167 */
  ObExprTimeStampAdd::calc_timestamp_add_vector,                /* 168 */
  ObExprArrayToString::eval_array_to_string_vector,             /* 169 */
  ObExprStringToArray::eval_string_to_array_vector,             /* 170 */
  ObExprArrayAppend::eval_array_append_vector,                  /* 171 */
  ObExprElementAt::eval_element_at_vector,                      /* 172 */
  ObExprArrayCardinality::eval_array_cardinality_vector,        /* 173 */
  ObExprArrayPrepend::eval_array_prepend_vector,                /* 174 */
  ObExprArrayConcat::eval_array_concat_vector,                  /* 175 */
  ObExprArrayDifference::eval_array_difference_vector,          /* 176 */
  ObExprArrayMax::eval_array_max_vector,                        /* 177 */
  ObExprArrayMin::eval_array_min_vector,                        /* 178 */
  ObExprArrayAvg::eval_array_avg_vector,                        /* 179 */
  ObExprArraySum::eval_array_sum_vector,                        /* 180 */
  ObExprArrayCompact::eval_array_compact_vector,                 /* 181 */
  ObExprArraySort::eval_array_sort_vector,                       /* 182 */
  ObExprSplitPart::calc_split_part_expr_vec,                             /* 183 */
  ObExprKeyValue::calc_key_value_expr_vector,                            /* 184 */
  ObExprLength::calc_oracle_length_vector,                               /* 185 */
  ObExprToChar::eval_to_char_vector,                                     /* 186 */
  ObExprArrayPosition::eval_array_position_vector,                       /* 187 */
  ObExprArraySlice::eval_array_slice_vector,                             /* 188 */
  ObExprArrayLength::eval_array_length_vector,                           /* 189 */
  NULL, // ObExprRange::eval_range_vector,                               /* 190 */
  ObExprURLEncode::eval_url_encode_vector,                      /* 191 */
  ObExprURLDecode::eval_url_decode_vector,                      /* 192 */
  ObExprIs::calc_vector_is_null,                                /* 193 */
  ObExprIs::calc_vector_is_true,                                /* 194 */
  ObExprIs::calc_vector_is_false,                               /* 195 */
  ObExprIsNot::calc_vector_is_not_null,                         /* 196 */
  ObExprIsNot::calc_vector_is_not_true,                         /* 197 */
  ObExprIsNot::calc_vector_is_not_false,                        /* 198 */
  ObExprBool::calc_vector_bool_expr,                            /* 199 */
  ObExprNotBetween::eval_not_between_vector,                    /* 200 */
  ObExprNot::eval_not_vector,                                   /* 201 */
  ObExprArrayExcept::eval_array_except_vector,                  /* 202 */
  ObExprArrayIntersect::eval_array_intersect_vector,            /* 203 */
  ObExprArrayUnion::eval_array_union_vector,                    /* 204 */
  NULL, // ObExprArrayReplace::eval_array_replace_vector,                /* 205 */
  NULL, // ObExprArrayPopfront::eval_array_popfront_vector,              /* 206 */
  ObExprColumnConv::column_convert_vector,                               /* 207 */
  ObExprColumnConv::column_convert_vector_fast,                          /* 208 */
  NULL, // ObExprConcat::eval_concat_vector,                             /* 209 */
  NULL, // ObExprLpad::calc_mysql_lpad_expr_vector,                      /* 210 */
  NULL, // ObExprRpad::calc_mysql_rpad_expr_vector,                      /* 211 */
  NULL, // ObExprOracleLpad::calc_oracle_lpad_expr_vector,               /* 212 */
  NULL, // ObExprOracleRpad::calc_oracle_rpad_expr_vector,               /* 213 */
  ObExprFindInSet::calc_find_in_set_vector,                              /* 214 */
  NULL, // ObExprSubstringIndex::eval_substring_index_vector,            /* 215 */
  NULL, // ObExprConcatWs::calc_concat_ws_expr_vector,                   /* 216 */
  ObExprMapKeys::eval_map_keys_vector,                                   /* 217 */
  ObExprMapValues::eval_map_values_vector,                               /* 218 */
  ObExprRbToArray::eval_rb_to_array_vector,                              /* 219 */
  ObExprRbContains::eval_rb_contains_vector,                             /* 220 */
  ObExprUDF::eval_udf_vector,                                            /* 221 */
  ObExprUDF::eval_external_udf_vector,                                   /* 222 */
  NULL, // ObExprInstr::calc_mysql_instr_expr_vector,                             /* 223 */
  NULL, // ObExprOracleInstr::calc_oracle_instr_expr_vector,                      /* 224 */
  NULL, // ObLocationExprOperator::calc_location_expr_vector,                     /* 225 */
  ObExprConvertTZ::calc_convert_tz_vector,                               /* 226 */
  ObExprMul::mul_decimalint64_int64_int64_vector,                        /* 227 */
  ObExprHash::calc_hash_value_expr_vector,                               /* 228 */
  ObExprAIComplete::eval_ai_complete_vector,                             /* 229 */
  ObExprAIEmbed::eval_ai_embed_vector,                                   /* 230 */
  NULL, // ObExprAIRerank::eval_ai_rerank_vector,                               /* 231 */
  NULL, // ObExprMd5ConcatWs::calc_md5_concat_ws_vector                         /* 232 */
  NULL, // ObExprHiddenClusteringKey::eval_vector_hidden_clustering_key,         /* 233 */
  ObExprToPinyin::eval_to_pinyin_vector,                                 /* 234 */
  ObExprMul::mul_decint32_decint32_int32_vector,                         /* 235 */
  ObExprMul::mul_decint32_int32_decint32_vector,                         /* 236 */
  ObExprMul::mul_decint64_decint32_int32_vector,                         /* 237 */
  ObExprMul::mul_decint64_int32_decint32_vector,                         /* 238 */
  ObExprMul::mul_decint64_decint64_int32_vector,                         /* 239 */
  ObExprMul::mul_decint64_int32_decint64_vector,                         /* 240 */
  ObExprMul::mul_decint128_decint64_int32_vector,                        /* 241 */
  ObExprMul::mul_decint128_int32_decint64_vector,                        /* 242 */
  ObExprMul::mul_decint256_int32_decint128_vector,                       /* 243 */
  ObExprMul::mul_decint256_decint128_int32_vector,                       /* 244 */
  ObExprMul::mul_decint128_decint128_int32_vector,                       /* 245 */
  ObExprMul::mul_decint128_int32_decint128_vector,                       /* 246 */
  ObExprMul::mul_decint256_decint256_int32_vector,                       /* 247 */
  ObExprMul::mul_decint256_int32_decint256_vector,                       /* 248 */
  NULL, // ObExprReplace::eval_replace_vector,                           /* 249 */
  NULL, // ObExprBitCount::calc_bitcount_expr_vector,                    /* 250 */
  NULL, // ObExprBitNeg::calc_bitneg_expr_vector,                        /* 251 */
  NULL, // ObExprBitLength::calc_bit_length_vector,                      /* 252 */
  NULL, // ObBitwiseExprOperator::calc_bitwise_result2_mysql_vector<ObBitwiseExprOperator::BIT_AND>,          /* 253 */
  NULL, // ObBitwiseExprOperator::calc_bitwise_result2_mysql_vector<ObBitwiseExprOperator::BIT_OR>,           /* 254 */
  NULL, // ObBitwiseExprOperator::calc_bitwise_result2_mysql_vector<ObBitwiseExprOperator::BIT_XOR>,          /* 255 */
  NULL, // ObBitwiseExprOperator::calc_bitwise_result2_mysql_vector<ObBitwiseExprOperator::BIT_LEFT_SHIFT>,          /* 256 */
  NULL, // ObBitwiseExprOperator::calc_bitwise_result2_mysql_vector<ObBitwiseExprOperator::BIT_RIGHT_SHIFT>,          /* 257 */
  NULL, // ObExprCrc32::calc_crc32_expr_vector,                          /* 258 */
  NULL, // ObExprFromBase64::eval_from_base64_vector                     /* 259 */
  NULL, // ObExprArrayContains::eval_array_contains_vector_int32_t,      /* 260 */
  NULL, // ObExprSecond::calc_second_vector,                             /* 261 */
  NULL, // ObExprMicrosecond::calc_microsecond_vector,                   /* 262 */
  NULL, // ObExprSecToTime::calc_sectotime_vector,                       /* 263 */
  NULL, // ObExprUsecToTime::calc_usec_to_time_vector,                   /* 264 */
  NULL, // ObExprTimestamp::calc_timestamp1_vector,                      /* 265 */
  NULL, // ObExprWeekDay::calc_weekday_vector,                           /* 266 */
  NULL, // ObExprTimeDiff::calc_timediff_vector,                         /* 267 */
};

REG_SER_FUNC_ARRAY(OB_SFA_SQL_EXPR_EVAL,
                   g_expr_eval_functions,
                   ARRAYSIZEOF(g_expr_eval_functions));

REG_SER_FUNC_ARRAY(OB_SFA_SQL_EXPR_EVAL_BATCH,
                   g_expr_eval_batch_functions,
                   ARRAYSIZEOF(g_expr_eval_batch_functions));

REG_SER_FUNC_ARRAY(OB_SFA_SQL_EXPR_EVAL_VECTOR,
                   g_expr_eval_vector_functions,
                   ARRAYSIZEOF(g_expr_eval_vector_functions));

static ObExpr::EvalFunc g_decimal_int_eval_functions[] = {
  ObExprAdd::add_decimalint32,
  ObExprAdd::add_decimalint64,
  ObExprAdd::add_decimalint128,
  ObExprAdd::add_decimalint256,
  ObExprAdd::add_decimalint512,
  ObExprAdd::add_decimalint512_with_check,
  ObExprMinus::minus_decimalint32,
  ObExprMinus::minus_decimalint64,
  ObExprMinus::minus_decimalint128,
  ObExprMinus::minus_decimalint256,
  ObExprMinus::minus_decimalint512,
  ObExprMinus::minus_decimalint512_with_check,
  ObExprMul::mul_decimalint32_int32_int32,
  ObExprMul::mul_decimalint64_int32_int32,
  ObExprMul::mul_decimalint64_int32_int64,
  ObExprMul::mul_decimalint64_int64_int32,
  ObExprMul::mul_decimalint128_int32_int64,
  ObExprMul::mul_decimalint128_int64_int32,
  ObExprMul::mul_decimalint128_int32_int128,
  ObExprMul::mul_decimalint128_int128_int32,
  ObExprMul::mul_decimalint128_int64_int64,
  ObExprMul::mul_decimalint128_int64_int128,
  ObExprMul::mul_decimalint128_int128_int64,
  ObExprMul::mul_decimalint128_int128_int128,
  ObExprMul::mul_decimalint256_int32_int128,
  ObExprMul::mul_decimalint256_int128_int32,
  ObExprMul::mul_decimalint256_int32_int256,
  ObExprMul::mul_decimalint256_int256_int32,
  ObExprMul::mul_decimalint256_int64_int128,
  ObExprMul::mul_decimalint256_int128_int64,
  ObExprMul::mul_decimalint256_int64_int256,
  ObExprMul::mul_decimalint256_int256_int64,
  ObExprMul::mul_decimalint256_int128_int128,
  ObExprMul::mul_decimalint256_int128_int256,
  ObExprMul::mul_decimalint256_int256_int128,
  ObExprMul::mul_decimalint512_int512_int512,
  ObExprMul::mul_decimalint512_with_check,
  ObExprMul::mul_decimalint64_round,
  ObExprMul::mul_decimalint128_round,
  ObExprMul::mul_decimalint256_round,
  ObExprMul::mul_decimalint512_round,
  ObExprMul::mul_decimalint512_round_with_check,
  ObExprDiv::div_decimalint_32_32,
  ObExprDiv::div_decimalint_32_64,
  ObExprDiv::div_decimalint_32_128,
  ObExprDiv::div_decimalint_32_256,
  ObExprDiv::div_decimalint_32_512,
  ObExprDiv::div_decimalint_64_32,
  ObExprDiv::div_decimalint_64_64,
  ObExprDiv::div_decimalint_64_128,
  ObExprDiv::div_decimalint_64_256,
  ObExprDiv::div_decimalint_64_512,
  ObExprDiv::div_decimalint_128_32,
  ObExprDiv::div_decimalint_128_64,
  ObExprDiv::div_decimalint_128_128,
  ObExprDiv::div_decimalint_128_256,
  ObExprDiv::div_decimalint_128_512,
  ObExprDiv::div_decimalint_256_32,
  ObExprDiv::div_decimalint_256_64,
  ObExprDiv::div_decimalint_256_128,
  ObExprDiv::div_decimalint_256_256,
  ObExprDiv::div_decimalint_256_512,
  ObExprDiv::div_decimalint_512_32,
  ObExprDiv::div_decimalint_512_64,
  ObExprDiv::div_decimalint_512_128,
  ObExprDiv::div_decimalint_512_256,
  ObExprDiv::div_decimalint_512_512,
  ObExprDiv::div_decimalint_512_32_with_check,
  ObExprDiv::div_decimalint_512_64_with_check,
  ObExprDiv::div_decimalint_512_128_with_check,
  ObExprDiv::div_decimalint_512_256_with_check,
  ObExprDiv::div_decimalint_512_512_with_check,
  ObExprAdd::add_decimalint32_oracle,
  ObExprAdd::add_decimalint64_oracle,
  ObExprAdd::add_decimalint128_oracle,
  ObExprMinus::minus_decimalint32_oracle,
  ObExprMinus::minus_decimalint64_oracle,
  ObExprMinus::minus_decimalint128_oracle,
  ObExprMul::mul_decimalint32_int32_int32_oracle,
  ObExprMul::mul_decimalint64_int32_int32_oracle,
  ObExprMul::mul_decimalint64_int32_int64_oracle,
  ObExprMul::mul_decimalint64_int64_int32_oracle,
  ObExprMul::mul_decimalint128_int32_int64_oracle,
  ObExprMul::mul_decimalint128_int64_int32_oracle,
  ObExprMul::mul_decimalint128_int32_int128_oracle,
  ObExprMul::mul_decimalint128_int128_int32_oracle,
  ObExprMul::mul_decimalint128_int64_int64_oracle,
  ObExprMul::mul_decimalint128_int64_int128_oracle,
  ObExprMul::mul_decimalint128_int128_int64_oracle,
  ObExprMul::mul_decimalint128_int128_int128_oracle,
  // div functions of decimal int types in mysql mode
  ObExprDiv::decint_div_mysql_fn<int32_t, int32_t>,
  ObExprDiv::decint_div_mysql_fn<int64_t, int32_t>,
  ObExprDiv::decint_div_mysql_fn<int64_t, int64_t>,
  ObExprDiv::decint_div_mysql_fn<int128_t, int32_t>,
  ObExprDiv::decint_div_mysql_fn<int128_t, int64_t>,
  ObExprDiv::decint_div_mysql_fn<int128_t, int128_t>,
  ObExprDiv::decint_div_mysql_fn<int256_t, int32_t>,
  ObExprDiv::decint_div_mysql_fn<int256_t, int64_t>,
  ObExprDiv::decint_div_mysql_fn<int256_t, int128_t>,
  ObExprDiv::decint_div_mysql_fn<int256_t, int256_t>,
  ObExprDiv::decint_div_mysql_fn<int512_t, int32_t>,
  ObExprDiv::decint_div_mysql_fn<int512_t, int64_t>,
  ObExprDiv::decint_div_mysql_fn<int512_t, int128_t>,
  ObExprDiv::decint_div_mysql_fn<int512_t, int256_t>,
  ObExprDiv::decint_div_mysql_fn<int512_t, int512_t>,
  ObExprMul::mul_decimalint64_int64_int64,
};

static ObExpr::EvalBatchFunc g_decimal_int_eval_batch_functions[] = {
  ObExprAdd::add_decimalint32_batch,
  ObExprAdd::add_decimalint64_batch,
  ObExprAdd::add_decimalint128_batch,
  ObExprAdd::add_decimalint256_batch,
  ObExprAdd::add_decimalint512_batch,
  ObExprAdd::add_decimalint512_with_check_batch,
  ObExprMinus::minus_decimalint32_batch,
  ObExprMinus::minus_decimalint64_batch,
  ObExprMinus::minus_decimalint128_batch,
  ObExprMinus::minus_decimalint256_batch,
  ObExprMinus::minus_decimalint512_batch,
  ObExprMinus::minus_decimalint512_with_check_batch,
  ObExprMul::mul_decimalint32_int32_int32_batch,
  ObExprMul::mul_decimalint64_int32_int32_batch,
  ObExprMul::mul_decimalint64_int32_int64_batch,
  ObExprMul::mul_decimalint64_int64_int32_batch,
  ObExprMul::mul_decimalint128_int32_int64_batch,
  ObExprMul::mul_decimalint128_int64_int32_batch,
  ObExprMul::mul_decimalint128_int32_int128_batch,
  ObExprMul::mul_decimalint128_int128_int32_batch,
  ObExprMul::mul_decimalint128_int64_int64_batch,
  ObExprMul::mul_decimalint128_int64_int128_batch,
  ObExprMul::mul_decimalint128_int128_int64_batch,
  ObExprMul::mul_decimalint128_int128_int128_batch,
  ObExprMul::mul_decimalint256_int32_int128_batch,
  ObExprMul::mul_decimalint256_int128_int32_batch,
  ObExprMul::mul_decimalint256_int32_int256_batch,
  ObExprMul::mul_decimalint256_int256_int32_batch,
  ObExprMul::mul_decimalint256_int64_int128_batch,
  ObExprMul::mul_decimalint256_int128_int64_batch,
  ObExprMul::mul_decimalint256_int64_int256_batch,
  ObExprMul::mul_decimalint256_int256_int64_batch,
  ObExprMul::mul_decimalint256_int128_int128_batch,
  ObExprMul::mul_decimalint256_int128_int256_batch,
  ObExprMul::mul_decimalint256_int256_int128_batch,
  ObExprMul::mul_decimalint512_int512_int512_batch,
  ObExprMul::mul_decimalint512_with_check_batch,
  ObExprMul::mul_decimalint64_round_batch,
  ObExprMul::mul_decimalint128_round_batch,
  ObExprMul::mul_decimalint256_round_batch,
  ObExprMul::mul_decimalint512_round_batch,
  ObExprMul::mul_decimalint512_round_with_check_batch,
  ObExprDiv::div_decimalint_32_32_batch,
  ObExprDiv::div_decimalint_32_32_batch,
  ObExprDiv::div_decimalint_32_64_batch,
  ObExprDiv::div_decimalint_32_128_batch,
  ObExprDiv::div_decimalint_32_256_batch,
  ObExprDiv::div_decimalint_32_512_batch,
  ObExprDiv::div_decimalint_64_32_batch,
  ObExprDiv::div_decimalint_64_64_batch,
  ObExprDiv::div_decimalint_64_128_batch,
  ObExprDiv::div_decimalint_64_256_batch,
  ObExprDiv::div_decimalint_64_512_batch,
  ObExprDiv::div_decimalint_128_32_batch,
  ObExprDiv::div_decimalint_128_64_batch,
  ObExprDiv::div_decimalint_128_128_batch,
  ObExprDiv::div_decimalint_128_256_batch,
  ObExprDiv::div_decimalint_128_512_batch,
  ObExprDiv::div_decimalint_256_32_batch,
  ObExprDiv::div_decimalint_256_64_batch,
  ObExprDiv::div_decimalint_256_128_batch,
  ObExprDiv::div_decimalint_256_256_batch,
  ObExprDiv::div_decimalint_256_512_batch,
  ObExprDiv::div_decimalint_512_32_batch,
  ObExprDiv::div_decimalint_512_64_batch,
  ObExprDiv::div_decimalint_512_128_batch,
  ObExprDiv::div_decimalint_512_256_batch,
  ObExprDiv::div_decimalint_512_512_batch,
  ObExprDiv::div_decimalint_512_32_with_check_batch,
  ObExprDiv::div_decimalint_512_64_with_check_batch,
  ObExprDiv::div_decimalint_512_128_with_check_batch,
  ObExprDiv::div_decimalint_512_256_with_check_batch,
  ObExprDiv::div_decimalint_512_512_with_check_batch,
  ObExprAdd::add_decimalint32_oracle_batch,
  ObExprAdd::add_decimalint64_oracle_batch,
  ObExprAdd::add_decimalint128_oracle_batch,
  ObExprMinus::minus_decimalint32_oracle_batch,
  ObExprMinus::minus_decimalint64_oracle_batch,
  ObExprMinus::minus_decimalint128_oracle_batch,
  ObExprMul::mul_decimalint32_int32_int32_oracle_batch,
  ObExprMul::mul_decimalint64_int32_int32_oracle_batch,
  ObExprMul::mul_decimalint64_int32_int64_oracle_batch,
  ObExprMul::mul_decimalint64_int64_int32_oracle_batch,
  ObExprMul::mul_decimalint128_int32_int64_oracle_batch,
  ObExprMul::mul_decimalint128_int64_int32_oracle_batch,
  ObExprMul::mul_decimalint128_int32_int128_oracle_batch,
  ObExprMul::mul_decimalint128_int128_int32_oracle_batch,
  ObExprMul::mul_decimalint128_int64_int64_oracle_batch,
  ObExprMul::mul_decimalint128_int64_int128_oracle_batch,
  ObExprMul::mul_decimalint128_int128_int64_oracle_batch,
  ObExprMul::mul_decimalint128_int128_int128_oracle_batch,
  // div functions of decimal int types in mysql mode
  ObExprDiv::decint_div_mysql_batch_fn<int32_t, int32_t>,
  ObExprDiv::decint_div_mysql_batch_fn<int64_t, int32_t>,
  ObExprDiv::decint_div_mysql_batch_fn<int64_t, int64_t>,
  ObExprDiv::decint_div_mysql_batch_fn<int128_t, int32_t>,
  ObExprDiv::decint_div_mysql_batch_fn<int128_t, int64_t>,
  ObExprDiv::decint_div_mysql_batch_fn<int128_t, int128_t>,
  ObExprDiv::decint_div_mysql_batch_fn<int256_t, int32_t>,
  ObExprDiv::decint_div_mysql_batch_fn<int256_t, int64_t>,
  ObExprDiv::decint_div_mysql_batch_fn<int256_t, int128_t>,
  ObExprDiv::decint_div_mysql_batch_fn<int256_t, int256_t>,
  ObExprDiv::decint_div_mysql_batch_fn<int512_t, int32_t>,
  ObExprDiv::decint_div_mysql_batch_fn<int512_t, int64_t>,
  ObExprDiv::decint_div_mysql_batch_fn<int512_t, int128_t>,
  ObExprDiv::decint_div_mysql_batch_fn<int512_t, int256_t>,
  ObExprDiv::decint_div_mysql_batch_fn<int512_t, int512_t>,
  ObExprMul::mul_decimalint64_int64_int64_batch,
};

REG_SER_FUNC_ARRAY(OB_SFA_DECIMAL_INT_EXPR_EVAL,
                   g_decimal_int_eval_functions,
                   ARRAYSIZEOF(g_decimal_int_eval_functions));

REG_SER_FUNC_ARRAY(OB_SFA_DECIMAL_INT_EXPR_EVAL_BATCH,
                   g_decimal_int_eval_batch_functions,
                   ARRAYSIZEOF(g_decimal_int_eval_batch_functions));

static ObExpr::EvalFunc g_collection_eval_functions[] = {
  ObExprAdd::add_collection_collection_int8_t,
  ObExprAdd::add_collection_collection_int16_t,
  ObExprAdd::add_collection_collection_int32_t,
  ObExprAdd::add_collection_collection_int64_t,
  ObExprAdd::add_collection_collection_float,
  ObExprAdd::add_collection_collection_double,
  ObExprMinus::minus_collection_collection_int8_t,
  ObExprMinus::minus_collection_collection_int16_t,
  ObExprMinus::minus_collection_collection_int32_t,
  ObExprMinus::minus_collection_collection_int64_t,
  ObExprMinus::minus_collection_collection_float,
  ObExprMinus::minus_collection_collection_double,
  ObExprAdd::add_collection_collection_uint64_t,
  ObExprMinus::minus_collection_collection_uint64_t,
};

static ObExpr::EvalBatchFunc g_collection_eval_batch_functions[] = {
  ObExprAdd::add_collection_collection_int8_t_batch,
  ObExprAdd::add_collection_collection_int16_t_batch,
  ObExprAdd::add_collection_collection_int32_t_batch,
  ObExprAdd::add_collection_collection_int64_t_batch,
  ObExprAdd::add_collection_collection_float_batch,
  ObExprAdd::add_collection_collection_double_batch,
  ObExprMinus::minus_collection_collection_int8_t_batch,
  ObExprMinus::minus_collection_collection_int16_t_batch,
  ObExprMinus::minus_collection_collection_int32_t_batch,
  ObExprMinus::minus_collection_collection_int64_t_batch,
  ObExprMinus::minus_collection_collection_float_batch,
  ObExprMinus::minus_collection_collection_double_batch,
  ObExprAdd::add_collection_collection_uint64_t_batch,
  ObExprMinus::minus_collection_collection_uint64_t_batch,
};

static ObExpr::EvalVectorFunc g_collection_expr_eval_vector_functions[] = {
  ObExprAdd::add_collection_collection_int8_t_vector,
  ObExprAdd::add_collection_collection_int16_t_vector,
  ObExprAdd::add_collection_collection_int32_t_vector,
  ObExprAdd::add_collection_collection_int64_t_vector,
  ObExprAdd::add_collection_collection_float_vector,
  ObExprAdd::add_collection_collection_double_vector,
  ObExprMinus::minus_collection_collection_int8_t_vector,
  ObExprMinus::minus_collection_collection_int16_t_vector,
  ObExprMinus::minus_collection_collection_int32_t_vector,
  ObExprMinus::minus_collection_collection_int64_t_vector,
  ObExprMinus::minus_collection_collection_float_vector,
  ObExprMinus::minus_collection_collection_double_vector,
  ObExprAdd::add_collection_collection_uint64_t_vector,
  ObExprMinus::minus_collection_collection_uint64_t_vector,
};

REG_SER_FUNC_ARRAY(OB_SFA_COLLECTION_EXPR_EVAL,
                   g_collection_eval_functions,
                   ARRAYSIZEOF(g_collection_eval_functions));

REG_SER_FUNC_ARRAY(OB_SFA_COLLECTION_EXPR_EVAL_BATCH,
                   g_collection_eval_batch_functions,
                   ARRAYSIZEOF(g_collection_eval_batch_functions));

REG_SER_FUNC_ARRAY(OB_SFA_COLLECTION_EXPR_EVAL_VEC,
                   g_collection_expr_eval_vector_functions,
                   ARRAYSIZEOF(g_collection_expr_eval_vector_functions));

} // end namespace sql
} // end namespace oceanbase
