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
#include "ob_expr_operator_factory.h"
#include "sql/engine/expr/ob_expr_substring_index.h"
#include "sql/engine/expr/ob_expr_strcmp.h"
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
#include "sql/engine/expr/ob_expr_bm25.h"
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
#include "sql/engine/expr/ob_expr_password.h"
#include "sql/engine/expr/ob_expr_int2ip.h"
#include "sql/engine/expr/ob_expr_ip2int.h"
#include "sql/engine/expr/ob_expr_inet.h"
#include "sql/engine/expr/ob_expr_last_exec_id.h"
#include "sql/engine/expr/ob_expr_last_trace_id.h"
#include "sql/engine/expr/ob_expr_is.h"
#include "sql/engine/expr/ob_expr_is_nan.h"
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
#include "sql/engine/expr/ob_expr_user.h"
#include "sql/engine/expr/ob_expr_version.h"
#include "sql/engine/expr/ob_expr_connection_id.h"
#include "sql/engine/expr/ob_expr_sys_view_bigint_param.h"
#include "sql/engine/expr/ob_expr_date.h"
#include "sql/engine/expr/ob_expr_date_add.h"
#include "sql/engine/expr/ob_expr_date_add_ck.h"
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
#include "sql/engine/expr/ob_expr_doc_id.h"
#include "sql/engine/expr/ob_expr_doc_length.h"
#include "sql/engine/expr/ob_expr_word_segment.h"
#include "sql/engine/expr/ob_expr_word_count.h"
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
#include "sql/engine/expr/ob_expr_udf/ob_expr_udf.h"
#include "sql/engine/expr/ob_expr_week_of_func.h"
#include "sql/engine/expr/ob_expr_userenv.h"
#include "sql/engine/expr/ob_expr_sys_context.h"
#include "sql/engine/expr/ob_expr_dll_udf.h"
#include "sql/engine/expr/ob_expr_uid.h"
#include "sql/engine/expr/ob_expr_timestamp.h"
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
#include "sql/engine/expr/ob_expr_pl_integer_checker.h"
#include "sql/engine/expr/ob_expr_pl_get_cursor_attr.h"
#include "sql/engine/expr/ob_expr_pl_sqlcode_sqlerrm.h"
#include "sql/engine/expr/ob_expr_plsql_variable.h"
#include "sql/engine/expr/ob_expr_pl_associative_index.h"
#include "sql/engine/expr/ob_expr_chr.h"
#include "sql/engine/expr/ob_expr_symmetric_encrypt.h"
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
#include "sql/engine/expr/ob_expr_part_id_pseudo_column.h"
#include "sql/engine/expr/ob_expr_to_single_byte.h"
#include "sql/engine/expr/ob_expr_to_multi_byte.h"
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
#include "sql/engine/expr/ob_expr_json_schema_valid.h"
#include "sql/engine/expr/ob_expr_json_schema_validation_report.h"
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
#include "sql/engine/expr/ob_expr_json_append.h"
#include "sql/engine/expr/ob_expr_json_array_insert.h"
#include "sql/engine/expr/ob_expr_json_value.h"
#include "sql/engine/expr/ob_expr_json_replace.h"
#include "sql/engine/expr/ob_expr_json_type.h"
#include "sql/engine/expr/ob_expr_json_length.h"
#include "sql/engine/expr/ob_expr_json_insert.h"
#include "sql/engine/expr/ob_expr_json_storage_size.h"
#include "sql/engine/expr/ob_expr_json_storage_free.h"
#include "sql/engine/expr/ob_expr_json_set.h"
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
#include "sql/engine/expr/ob_expr_sdo_relate.h"
#include "sql/engine/expr/ob_expr_st_geomfromewkb.h"
#include "sql/engine/expr/ob_expr_st_geomfromwkb.h"
#include "sql/engine/expr/ob_expr_st_geomfromewkt.h"
#include "sql/engine/expr/ob_expr_priv_st_geographyfromtext.h"
#include "sql/engine/expr/ob_expr_st_asewkt.h"
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
#include "sql/engine/expr/ob_expr_xml_forest.h"
#include "sql/engine/expr/ob_expr_xml_concat.h"
#include "sql/engine/expr/ob_expr_xml_attributes.h"
#include "sql/engine/expr/ob_expr_extract_value.h"
#include "sql/engine/expr/ob_expr_extract_xml.h"
#include "sql/engine/expr/ob_expr_existsnode_xml.h"
#include "sql/engine/expr/ob_expr_xml_serialize.h"
#include "sql/engine/expr/ob_expr_xmlcast.h"
#include "sql/engine/expr/ob_expr_update_xml.h"
#include "sql/engine/expr/ob_expr_insert_child_xml.h"
#include "sql/engine/expr/ob_expr_xml_delete_xml.h"
#include "sql/engine/expr/ob_expr_xml_sequence.h"
#include "sql/engine/expr/ob_expr_sql_udt_construct.h"
#include "sql/engine/expr/ob_expr_priv_attribute_access.h"
#include "sql/engine/expr/ob_expr_temp_table_ssid.h"
#include "sql/engine/expr/ob_expr_priv_st_numinteriorrings.h"
#include "sql/engine/expr/ob_expr_priv_st_iscollection.h"
#include "sql/engine/expr/ob_expr_priv_st_equals.h"
#include "sql/engine/expr/ob_expr_priv_st_touches.h"
#include "sql/engine/expr/ob_expr_align_date4cmp.h"
#include "sql/engine/expr/ob_expr_extract_cert_expired_time.h"
#include "sql/engine/expr/ob_expr_transaction_id.h"
#include "sql/engine/expr/ob_expr_inner_row_cmp_val.h"
#include "sql/engine/expr/ob_expr_last_refresh_scn.h"
#include "sql/engine/expr/ob_expr_priv_st_makeenvelope.h"
#include "sql/engine/expr/ob_expr_priv_st_clipbybox2d.h"
#include "sql/engine/expr/ob_expr_priv_st_pointonsurface.h"
#include "sql/engine/expr/ob_expr_priv_st_geometrytype.h"
#include "sql/engine/expr/ob_expr_st_crosses.h"
#include "sql/engine/expr/ob_expr_st_overlaps.h"
#include "sql/engine/expr/ob_expr_st_union.h"
#include "sql/engine/expr/ob_expr_st_length.h"
#include "sql/engine/expr/ob_expr_st_difference.h"
#include "sql/engine/expr/ob_expr_st_asgeojson.h"
#include "sql/engine/expr/ob_expr_st_centroid.h"
#include "sql/engine/expr/ob_expr_st_symdifference.h"
#include "sql/engine/expr/ob_expr_priv_st_asmvtgeom.h"
#include "sql/engine/expr/ob_expr_priv_st_makevalid.h"
#include "sql/engine/expr/ob_expr_gtid.h"
#include "sql/engine/expr/ob_expr_array.h"
#include "sql/engine/expr/ob_expr_vec_ivf_center_id.h"
#include "sql/engine/expr/ob_expr_vec_ivf_center_vector.h"
#include "sql/engine/expr/ob_expr_vec_ivf_flat_data_vector.h"
#include "sql/engine/expr/ob_expr_vec_ivf_sq8_data_vector.h"
#include "sql/engine/expr/ob_expr_vec_ivf_meta_id.h"
#include "sql/engine/expr/ob_expr_vec_ivf_meta_vector.h"
#include "sql/engine/expr/ob_expr_vec_ivf_pq_center_id.h"
#include "sql/engine/expr/ob_expr_vec_ivf_pq_center_ids.h"
#include "sql/engine/expr/ob_expr_vec_ivf_pq_center_vector.h"
#include "sql/engine/expr/ob_expr_vec_vid.h"
#include "sql/engine/expr/ob_expr_vec_type.h"
#include "sql/engine/expr/ob_expr_vec_vector.h"
#include "sql/engine/expr/ob_expr_vec_scn.h"
#include "sql/engine/expr/ob_expr_vec_key.h"
#include "sql/engine/expr/ob_expr_vec_data.h"
#include "sql/engine/expr/ob_expr_vec_visible.h"
#include "sql/engine/expr/ob_expr_spiv_dim.h"
#include "sql/engine/expr/ob_expr_spiv_value.h"
#include "sql/engine/expr/ob_expr_vector.h"
#include "sql/engine/expr/ob_expr_semantic_distance.h"
#include "sql/engine/expr/ob_expr_vec_chunk.h"
#include "sql/engine/expr/ob_expr_embedded_vec.h"
#include "sql/engine/expr/ob_expr_inner_table_option_printer.h"
#include "sql/engine/expr/ob_expr_rb_build_empty.h"
#include "sql/engine/expr/ob_expr_rb_is_empty.h"
#include "sql/engine/expr/ob_expr_rb_build_varbinary.h"
#include "sql/engine/expr/ob_expr_rb_to_varbinary.h"
#include "sql/engine/expr/ob_expr_rb_cardinality.h"
#include "sql/engine/expr/ob_expr_rb_calc_cardinality.h"
#include "sql/engine/expr/ob_expr_rb_calc.h"
#include "sql/engine/expr/ob_expr_rb_to_string.h"
#include "sql/engine/expr/ob_expr_startup_mode.h"
#include "sql/engine/expr/ob_expr_rb_from_string.h"
#include "sql/engine/expr/ob_expr_rb_select.h"
#include "sql/engine/expr/ob_expr_rb_build.h"
#include "sql/engine/expr/ob_expr_array_contains.h"
#include "sql/engine/expr/ob_expr_array_to_string.h"
#include "sql/engine/expr/ob_expr_string_to_array.h"
#include "sql/engine/expr/ob_expr_array_append.h"
#include "sql/engine/expr/ob_expr_array_concat.h"
#include "sql/engine/expr/ob_expr_array_difference.h"
#include "sql/engine/expr/ob_expr_array_max.h"
#include "sql/engine/expr/ob_expr_array_avg.h"
#include "sql/engine/expr/ob_expr_array_compact.h"
#include "sql/engine/expr/ob_expr_array_sort.h"
#include "sql/engine/expr/ob_expr_array_sortby.h"
#include "sql/engine/expr/ob_expr_array_filter.h"
#include "sql/engine/expr/ob_expr_element_at.h"
#include "sql/engine/expr/ob_expr_array_cardinality.h"
#include "sql/engine/expr/ob_expr_tokenize.h"
#include "sql/engine/expr/ob_expr_lock_func.h"
#include "sql/engine/expr/ob_expr_decode_trace_id.h"
#include "sql/engine/expr/ob_expr_topn_filter.h"
#include "sql/engine/expr/ob_expr_get_path.h"
#include "sql/engine/expr/ob_expr_transaction_id.h"
#include "sql/engine/expr/ob_expr_audit_log_func.h"
#include "sql/engine/expr/ob_expr_can_access_trigger.h"
#include "sql/engine/expr/ob_expr_enhanced_aes_encrypt.h"
#include "sql/engine/expr/ob_expr_split_part.h"
#include "sql/engine/expr/ob_expr_inner_decode_like.h"
#include "sql/engine/expr/ob_expr_inner_double_to_int.h"
#include "sql/engine/expr/ob_expr_inner_decimal_to_year.h"
#include "sql/engine/expr/ob_expr_array_overlaps.h"
#include "sql/engine/expr/ob_expr_array_contains_all.h"
#include "sql/engine/expr/ob_expr_array_distinct.h"
#include "sql/engine/expr/ob_expr_array_remove.h"
#include "sql/engine/expr/ob_expr_array_map.h"
#include "sql/engine/expr/ob_expr_array_range.h"
#include "sql/engine/expr/ob_expr_calc_odps_size.h"
#include "sql/engine/expr/ob_expr_array_first.h"
#include "sql/engine/expr/ob_expr_mysql_proc_info.h"
#include "sql/engine/expr/ob_expr_get_mysql_routine_parameter_type_str.h"
#include "sql/engine/expr/ob_expr_ora_login_user.h"
#include "sql/engine/expr/ob_expr_priv_st_geohash.h"
#include "sql/engine/expr/ob_expr_priv_st_makepoint.h"
#include "sql/engine/expr/ob_expr_to_pinyin.h"
#include "sql/engine/expr/ob_expr_url_codec.h"
#include "sql/engine/expr/ob_expr_keyvalue.h"
#include "sql/engine/expr/ob_expr_demote_cast.h"
#include "sql/engine/expr/ob_expr_array_sum.h"
#include "sql/engine/expr/ob_expr_array_length.h"
#include "sql/engine/expr/ob_expr_array_position.h"
#include "sql/engine/expr/ob_expr_array_slice.h"
#include "sql/engine/expr/ob_expr_inner_info_cols_printer.h"
#include "sql/engine/expr/ob_expr_array_except.h"
#include "sql/engine/expr/ob_expr_array_intersect.h"
#include "sql/engine/expr/ob_expr_array_union.h"
#include "sql/engine/expr/ob_expr_map.h"
#include "sql/engine/expr/ob_expr_rb_to_array.h"
#include "sql/engine/expr/ob_expr_rb_contains.h"
#include "sql/engine/expr/ob_expr_map_keys.h"
#include "sql/engine/expr/ob_expr_current_catalog.h"
#include "sql/engine/expr/ob_expr_check_catalog_access.h"
#include "sql/engine/expr/ob_expr_check_location_access.h"
#include "sql/engine/expr/ob_expr_tmp_file_open.h"
#include "sql/engine/expr/ob_expr_tmp_file_close.h"
#include "sql/engine/expr/ob_expr_tmp_file_write.h"
#include "sql/engine/expr/ob_expr_tmp_file_read.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_complete.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_embed.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_rerank.h"
#include "sql/engine/expr/ob_expr_local_dynamic_filter.h"
#include "sql/engine/expr/ob_expr_semantic_distance.h"
#include "sql/engine/expr/ob_expr_bucket.h"
#include "sql/engine/expr/ob_expr_void.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_prompt.h"
#include "sql/engine/expr/ob_expr_vector_similarity.h"
#include "sql/engine/expr/ob_expr_edit_distance.h"
#include "sql/engine/expr/ob_expr_md5_concat_ws.h"
#include "sql/engine/expr/ob_expr_collect_file_list.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_load_file.h"


#include "sql/engine/expr/ob_expr_lock_func.h"
#include "sql/engine/expr/ob_expr_format_profile.h"
#include "sql/engine/expr/ob_expr_max_pt.h"
#include "sql/engine/expr/ob_expr_date_trunc.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
static AllocFunc OP_ALLOC[T_MAX_OP];
static AllocFunc OP_ALLOC_ORCL[T_MAX_OP];


/* Some expressions do not have any short-circuit logic in their calculation logic.
 * They are guaranteed to calculate all their parameters before performing their own calculations.
 * For example, all unary functions, like, left/right and other multi-ary functions.
 * On the contrary, functions with short-circuit logic, when hitting the short circuit,
 * some of their parameters are not used and cannot be calculated,
 * such as Add, case when, nullif, etc.
 * we called 1st functions EAGER_EVALUATION, another are SHORT_CIRCUIT_EVALUATION*/

/* If you are not sure which order to use, choose SHORT_CIRCUIT_EVALUATION ！！！*/
enum ExprEvalOrder {
  SHORT_CIRCUIT_EVALUATION = 0,
  EAGER_EVALUATION = 1,
  INVALID_ORDER = 2
};
static ExprEvalOrder OP_EVAL_ORDERS[T_MAX_OP];
static ExprEvalOrder OP_EVAL_ORDERS_ORCL[T_MAX_OP];
#define REG_OP(OpClass, EvalOrder)        \
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
        OP_EVAL_ORDERS[op.get_type()] = EvalOrder; \
        i++;                                        \
      }                                             \
    }();                                            \
  } while(0)

// 当要开发两个功能完全一致的表达式时（例如mid和substr两个表达式）可以使用这个宏
// OriOp是已有的表达式，现在想要开发NewOp且二者功能完全一致，使用该宏就可以避免重复代码
// 但是要求OriOp已经先注册了
#define REG_SAME_OP(OriOpType, NewOpType, NewOpName, idx_mysql, is_internal)        \
  do {                                                                 \
    [&]() {                                                            \
      if (OB_UNLIKELY((idx_mysql) >= EXPR_OP_NUM)) {                   \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "out of the max expr");                              \
      } else if (OB_ISNULL(OP_ALLOC[OriOpType])) {                     \
        LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "OriOp is not registered yet", K(OriOpType), K(NewOpType)); \
      } else {                                                         \
        NAME_TYPES[(idx_mysql)].name_ = NewOpName;                     \
        NAME_TYPES[(idx_mysql)].type_ = NewOpType;                     \
        NAME_TYPES[(idx_mysql)].is_internal_ = is_internal;            \
        OP_ALLOC[NewOpType] = OP_ALLOC[OriOpType];                     \
        OP_EVAL_ORDERS[NewOpType] = OP_EVAL_ORDERS[OriOpType];         \
        (idx_mysql)++;                                                 \
      }                                                                \
    }();                                                               \
  } while(0)

#define REG_OP_ORCL(OpClass, EvalOrder)                        \
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
        OP_EVAL_ORDERS_ORCL[op.get_type()] = EvalOrder; \
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
        OP_EVAL_ORDERS_ORCL[NewOpType] = OP_EVAL_ORDERS_ORCL[OriOpType]; \
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
    REG_OP(ObExprAdd, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprAggAdd, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprAnd, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArgCase, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprAssign, EAGER_EVALUATION);
    REG_OP(ObExprBetween, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprBitAnd, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprCase, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprCast, EAGER_EVALUATION);
    REG_OP(ObExprTimeStampAdd, EAGER_EVALUATION);
    REG_OP(ObExprToType, EAGER_EVALUATION);
    REG_OP(ObExprChar, EAGER_EVALUATION);
    REG_OP(ObExprToChar, EAGER_EVALUATION);
    REG_OP(ObExprConvert, EAGER_EVALUATION);
    REG_OP(ObExprCoalesce, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprNvl, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprConcat, EAGER_EVALUATION);
    REG_OP(ObExprCurrentUser, EAGER_EVALUATION);
    REG_OP(ObExprCurrentUserPriv, EAGER_EVALUATION);
    REG_OP(ObExprYear, EAGER_EVALUATION);
    REG_OP(ObExprOracleDecode, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprOracleTrunc, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprDiv, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprAggDiv, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprEffectiveTenant, EAGER_EVALUATION);
    REG_OP(ObExprEffectiveTenantId, EAGER_EVALUATION);
    REG_OP(ObExprEqual, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprNullSafeEqual, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprGetUserVar, EAGER_EVALUATION);
    REG_OP(ObExprGreaterEqual, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprGreaterThan, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprGreatest, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprHex, EAGER_EVALUATION);
    REG_OP(ObExprPassword, EAGER_EVALUATION);
    REG_OP(ObExprIn, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprNotIn, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprInt2ip, EAGER_EVALUATION);
    REG_OP(ObExprIp2int, EAGER_EVALUATION);
    REG_OP(ObExprInetAton, EAGER_EVALUATION);
    REG_OP(ObExprInet6Ntoa, EAGER_EVALUATION);
    REG_OP(ObExprInet6Aton, EAGER_EVALUATION);
    REG_OP(ObExprIsIpv4, EAGER_EVALUATION);
    REG_OP(ObExprIsIpv6, EAGER_EVALUATION);
    REG_OP(ObExprIsIpv4Mapped, EAGER_EVALUATION);
    REG_OP(ObExprIsIpv4Compat, EAGER_EVALUATION);
    REG_OP(ObExprInsert, EAGER_EVALUATION);
    REG_OP(ObExprIs, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprIsNot, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprLeast, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprLength, EAGER_EVALUATION);
    REG_OP(ObExprEditDistance, EAGER_EVALUATION);
    REG_OP(ObExprEditDistanceUTF8, EAGER_EVALUATION);
    REG_OP(ObExprLessEqual, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprLessThan, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprLike, EAGER_EVALUATION);
    REG_OP(ObExprLower, EAGER_EVALUATION);
    REG_OP(ObExprMinus, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprAggMinus, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprMod, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprMd5, EAGER_EVALUATION);
    REG_OP(ObExprMd5ConcatWs, EAGER_EVALUATION);
    REG_OP(ObExprTime, EAGER_EVALUATION);
    REG_OP(ObExprHour, EAGER_EVALUATION);
    REG_OP(ObExprRpad, EAGER_EVALUATION);
    REG_OP(ObExprLpad, EAGER_EVALUATION);
    REG_OP(ObExprColumnConv, EAGER_EVALUATION);
    REG_OP(ObExprFunValues, EAGER_EVALUATION);
    REG_OP(ObExprFunDefault, EAGER_EVALUATION);
    REG_OP(ObExprIntDiv, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprMul, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprAggMul, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprAbs, EAGER_EVALUATION);
    REG_OP(ObExprUuid, EAGER_EVALUATION);
    REG_OP(ObExprNeg, EAGER_EVALUATION);
    REG_OP(ObExprPrior, EAGER_EVALUATION);
    REG_OP(ObExprFromUnixTime, EAGER_EVALUATION);
    REG_OP(ObExprNotBetween, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprNotEqual, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprNot, EAGER_EVALUATION);
    REG_OP(ObExprOr, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprXor, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRegexp, EAGER_EVALUATION);
    REG_OP(ObExprRegexpSubstr, EAGER_EVALUATION);
    REG_OP(ObExprRegexpInstr, EAGER_EVALUATION);
    REG_OP(ObExprRegexpReplace, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRegexpLike, EAGER_EVALUATION);
    REG_OP(ObExprSleep, EAGER_EVALUATION);
    REG_OP(ObExprStrcmp, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSubstr, EAGER_EVALUATION);
    REG_OP(ObExprMid, EAGER_EVALUATION);
    REG_OP(ObExprSubstringIndex, EAGER_EVALUATION);
    REG_OP(ObExprMid, EAGER_EVALUATION);
    REG_OP(ObExprSysViewBigintParam, EAGER_EVALUATION);
    REG_OP(ObExprInnerTrim, EAGER_EVALUATION);
    REG_OP(ObExprTrim, EAGER_EVALUATION);
    REG_OP(ObExprLtrim, EAGER_EVALUATION);
    REG_OP(ObExprSpace, EAGER_EVALUATION);
    REG_OP(ObExprRtrim, EAGER_EVALUATION);
    REG_OP(ObExprUnhex, EAGER_EVALUATION);
    REG_OP(ObExprUpper, EAGER_EVALUATION);
    REG_OP(ObExprConv, EAGER_EVALUATION);
    REG_OP(ObExprUser, EAGER_EVALUATION);
    REG_OP(ObExprDate, EAGER_EVALUATION);
    REG_OP(ObExprMonth, EAGER_EVALUATION);
    REG_OP(ObExprMonthName, EAGER_EVALUATION);
    REG_OP(ObExprSoundex, EAGER_EVALUATION);
    REG_OP(ObExprDateAdd, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprMonthsAdd, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprDateSub, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprDateAddClickhouse, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprDateSubClickhouse, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSubtime, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprAddtime, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprDateDiff, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprTimeStampDiff, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprTimeDiff, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPeriodDiff, EAGER_EVALUATION);
    REG_OP(ObExprPeriodAdd, EAGER_EVALUATION);
    REG_OP(ObExprUnixTimestamp, EAGER_EVALUATION);
    REG_OP(ObExprMakeTime, EAGER_EVALUATION);
    REG_OP(ObExprMakedate, EAGER_EVALUATION);
    REG_OP(ObExprExtract, EAGER_EVALUATION);
    REG_OP(ObExprToDays, EAGER_EVALUATION);
    REG_OP(ObExprPosition, EAGER_EVALUATION);
    REG_OP(ObExprFromDays, EAGER_EVALUATION);
    REG_OP(ObExprDateFormat, EAGER_EVALUATION);
    REG_OP(ObExprGetFormat, EAGER_EVALUATION);
    REG_OP(ObExprStrToDate, EAGER_EVALUATION);
    REG_OP(ObExprCurDate, EAGER_EVALUATION);
    REG_OP(ObExprCurTime, EAGER_EVALUATION);
    REG_OP(ObExprSysdate, EAGER_EVALUATION);
    REG_OP(ObExprCurTimestamp, EAGER_EVALUATION);
    REG_OP(ObExprUtcTimestamp, EAGER_EVALUATION);
    REG_OP(ObExprUtcTime, EAGER_EVALUATION);
    REG_OP(ObExprUtcDate, EAGER_EVALUATION);
    REG_OP(ObExprTimeToUsec, EAGER_EVALUATION);
    REG_OP(ObExprUsecToTime, EAGER_EVALUATION);
    REG_OP(ObExprMergingFrozenTime, EAGER_EVALUATION);
    REG_OP(ObExprFuncRound, EAGER_EVALUATION);
    REG_OP(ObExprFuncFloor, EAGER_EVALUATION);
    REG_OP(ObExprFuncCeil, EAGER_EVALUATION);
    REG_OP(ObExprFuncCeiling, EAGER_EVALUATION);
    REG_OP(ObExprFuncDump, EAGER_EVALUATION);
    REG_OP(ObExprRepeat, EAGER_EVALUATION);
    REG_OP(ObExprExportSet, EAGER_EVALUATION);
    REG_OP(ObExprReplace, EAGER_EVALUATION);
    REG_OP(ObExprFuncPartHash, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprFuncPartKey, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprDatabase, EAGER_EVALUATION);
    REG_OP(ObExprAutoincNextval, EAGER_EVALUATION);
    REG_OP(ObExprLastInsertID, EAGER_EVALUATION);
    REG_OP(ObExprInstr, EAGER_EVALUATION);
    REG_OP(ObExprFuncLnnvl, EAGER_EVALUATION);
    REG_OP(ObExprLocate, EAGER_EVALUATION);
    REG_OP(ObExprVersion, EAGER_EVALUATION);
    REG_OP(ObExprObVersion, EAGER_EVALUATION);
    REG_OP(ObExprConnectionId, EAGER_EVALUATION);
    REG_OP(ObExprCharset, EAGER_EVALUATION);
    REG_OP(ObExprCollation, EAGER_EVALUATION);
    REG_OP(ObExprCoercibility, EAGER_EVALUATION);
    REG_OP(ObExprConvertTZ, EAGER_EVALUATION);
    REG_OP(ObExprSetCollation, EAGER_EVALUATION);
    REG_OP(ObExprReverse, EAGER_EVALUATION);
    REG_OP(ObExprRight, EAGER_EVALUATION);
    REG_OP(ObExprSign, EAGER_EVALUATION);
    REG_OP(ObExprBitXor, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSqrt, EAGER_EVALUATION);
    REG_OP(ObExprLog2, EAGER_EVALUATION);
    REG_OP(ObExprLog10, EAGER_EVALUATION);
    REG_OP(ObExprPow, EAGER_EVALUATION);
    REG_OP(ObExprRowCount, EAGER_EVALUATION);
    REG_OP(ObExprFoundRows, EAGER_EVALUATION);
    REG_OP(ObExprAggParamList, EAGER_EVALUATION);
    REG_OP(ObExprIsServingTenant, EAGER_EVALUATION);
    REG_OP(ObExprSysPrivilegeCheck, EAGER_EVALUATION);
    REG_OP(ObExprField, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprElt, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprNullif, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprTimestampNvl, EAGER_EVALUATION);
    REG_OP(ObExprDesHexStr, EAGER_EVALUATION);
    REG_OP(ObExprAscii, EAGER_EVALUATION);
    REG_OP(ObExprOrd, EAGER_EVALUATION);
    REG_OP(ObExprBitCount, EAGER_EVALUATION);
    REG_OP(ObExprFindInSet, EAGER_EVALUATION);
    REG_OP(ObExprLeft, EAGER_EVALUATION);
    REG_OP(ObExprRand, EAGER_EVALUATION);
    REG_OP(ObExprMakeSet, EAGER_EVALUATION);
    REG_OP(ObExprEstimateNdv, EAGER_EVALUATION);
    REG_OP(ObExprSysOpOpnsize, EAGER_EVALUATION);
    REG_OP(ObExprDayOfMonth, EAGER_EVALUATION);
    REG_OP(ObExprDayOfWeek, EAGER_EVALUATION);
    REG_OP(ObExprDayOfYear, EAGER_EVALUATION);
    REG_OP(ObExprSecond, EAGER_EVALUATION);
    REG_OP(ObExprMinute, EAGER_EVALUATION);
    REG_OP(ObExprMicrosecond, EAGER_EVALUATION);
    REG_OP(ObExprToSeconds, EAGER_EVALUATION);
    REG_OP(ObExprTimeToSec, EAGER_EVALUATION);
    REG_OP(ObExprSecToTime, EAGER_EVALUATION);
    REG_OP(ObExprInterval, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprTruncate, EAGER_EVALUATION);
    REG_OP(ObExprDllUdf, EAGER_EVALUATION);
    REG_OP(ObExprExp, EAGER_EVALUATION);
    REG_OP(ObExprAnyValue, EAGER_EVALUATION);
    REG_OP(ObExprUuidShort, EAGER_EVALUATION);
    REG_OP(ObExprRandomBytes, EAGER_EVALUATION);
    /* subquery comparison experator */
    REG_OP(ObExprSubQueryRef, EAGER_EVALUATION);
    REG_OP(ObExprSubQueryEqual, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSubQueryNotEqual, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSubQueryNSEqual, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSubQueryGreaterEqual, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSubQueryGreaterThan, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSubQueryLessEqual, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSubQueryLessThan, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRemoveConst, EAGER_EVALUATION);
    REG_OP(ObExprExists, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprNotExists, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprCharLength, EAGER_EVALUATION);
    REG_OP(ObExprBitOr, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprBitNeg, EAGER_EVALUATION);
    REG_OP(ObExprBitLeftShift, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprBitLength, EAGER_EVALUATION);
    REG_OP(ObExprBitRightShift, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprIfNull, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprConcatWs, EAGER_EVALUATION);
    REG_OP(ObExprCmpMeta, EAGER_EVALUATION);
    REG_OP(ObExprQuote, EAGER_EVALUATION);
    REG_OP(ObExprPad, EAGER_EVALUATION);
    REG_OP(ObExprHostIP, EAGER_EVALUATION);
    REG_OP(ObExprRpcPort, EAGER_EVALUATION);
    REG_OP(ObExprMySQLPort, EAGER_EVALUATION);
    REG_OP(ObExprGetSysVar, EAGER_EVALUATION);
    REG_OP(ObExprPartId, EAGER_EVALUATION);
    REG_OP(ObExprLastTraceId, EAGER_EVALUATION);
    REG_OP(ObExprLastExecId, EAGER_EVALUATION);
    REG_OP(ObExprDocID, EAGER_EVALUATION);
    REG_OP(ObExprDocLength, EAGER_EVALUATION);
    REG_OP(ObExprWordSegment, EAGER_EVALUATION);
    REG_OP(ObExprWordCount, EAGER_EVALUATION);
    REG_OP(ObExprObjAccess, EAGER_EVALUATION);
    REG_OP(ObExprEnumToStr, EAGER_EVALUATION);
    REG_OP(ObExprSetToStr, EAGER_EVALUATION);
    REG_OP(ObExprEnumToInnerType, EAGER_EVALUATION);
    REG_OP(ObExprSetToInnerType, EAGER_EVALUATION);
    REG_OP(ObExprConnectByRoot, EAGER_EVALUATION);
    REG_OP(ObExprGetPackageVar, EAGER_EVALUATION);
    REG_OP(ObExprGetSubprogramVar, EAGER_EVALUATION);
    REG_OP(ObExprShadowUKProject, EAGER_EVALUATION);
    REG_OP(ObExprUDF, EAGER_EVALUATION);
    REG_OP(ObExprWeekOfYear, EAGER_EVALUATION);
    REG_OP(ObExprWeekDay, EAGER_EVALUATION);
    REG_OP(ObExprYearWeek, EAGER_EVALUATION);
    REG_OP(ObExprWeek, EAGER_EVALUATION);
    REG_OP(ObExprQuarter, EAGER_EVALUATION);
    REG_OP(ObExprSeqNextval, EAGER_EVALUATION);
    REG_OP(ObExprAesDecrypt, EAGER_EVALUATION);
    REG_OP(ObExprAesEncrypt, EAGER_EVALUATION);
    REG_OP(ObExprBool, EAGER_EVALUATION);
    REG_OP(ObExprSin, EAGER_EVALUATION);
    REG_OP(ObExprCos, EAGER_EVALUATION);
    REG_OP(ObExprTan, EAGER_EVALUATION);
    REG_OP(ObExprCot, EAGER_EVALUATION);
    REG_OP(ObExprCalcPartitionId, EAGER_EVALUATION);
    REG_OP(ObExprCalcTabletId, EAGER_EVALUATION);
    REG_OP(ObExprCalcPartitionTabletId, EAGER_EVALUATION);
    REG_OP(ObExprPartIdPseudoColumn, EAGER_EVALUATION);
    REG_OP(ObExprStmtId, EAGER_EVALUATION);
    REG_OP(ObExprRadians, EAGER_EVALUATION);
    REG_OP(ObExprJoinFilter, EAGER_EVALUATION);
    REG_OP(ObExprAsin, EAGER_EVALUATION);
    REG_OP(ObExprAcos, EAGER_EVALUATION);
    REG_OP(ObExprAtan, EAGER_EVALUATION);
    REG_OP(ObExprAtan2, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprToOutfileRow, EAGER_EVALUATION);
    REG_OP(ObExprFormat, EAGER_EVALUATION);
    REG_OP(ObExprLastDay, EAGER_EVALUATION);
    REG_OP(ObExprPi, EAGER_EVALUATION);
    REG_OP(ObExprLog, EAGER_EVALUATION);
    REG_OP(ObExprTimeFormat, EAGER_EVALUATION);
    REG_OP(ObExprTimestamp, EAGER_EVALUATION);
    REG_OP(ObExprOutputPack, EAGER_EVALUATION);
    REG_OP(ObExprWrapperInner, EAGER_EVALUATION);
    REG_OP(ObExprDegrees, EAGER_EVALUATION);
    REG_OP(ObExprValidatePasswordStrength, EAGER_EVALUATION);
    REG_OP(ObExprDay, EAGER_EVALUATION);
    REG_OP(ObExprBenchmark, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprWeightString, EAGER_EVALUATION);
    REG_OP(ObExprCrc32, EAGER_EVALUATION);
  #if defined(ENABLE_DEBUG_LOG) || !defined(NDEBUG)
    REG_OP(ObExprTmpFileOpen, EAGER_EVALUATION);
    REG_OP(ObExprTmpFileClose, EAGER_EVALUATION);
    REG_OP(ObExprTmpFileWrite, EAGER_EVALUATION);
    REG_OP(ObExprTmpFileRead, EAGER_EVALUATION);
  #endif
    REG_OP(ObExprToBase64, EAGER_EVALUATION);
    REG_OP(ObExprFromBase64, EAGER_EVALUATION);
    REG_OP(ObExprOpSubQueryInPl, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprEncodeSortkey, EAGER_EVALUATION);
    REG_OP(ObExprHash, EAGER_EVALUATION);
    REG_OP(ObExprJsonObject, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonExtract, EAGER_EVALUATION);
    REG_OP(ObExprJsonSchemaValid, EAGER_EVALUATION);
    REG_OP(ObExprJsonSchemaValidationReport, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonContains, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonContainsPath, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonDepth, EAGER_EVALUATION);
    REG_OP(ObExprJsonKeys, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonQuote, EAGER_EVALUATION);
    REG_OP(ObExprJsonUnquote, EAGER_EVALUATION);
    REG_OP(ObExprJsonArray, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonOverlaps, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonRemove, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonSearch, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonValid, EAGER_EVALUATION);
    REG_OP(ObExprJsonArrayAppend, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonAppend, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonArrayInsert, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonValue, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonReplace, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonType, EAGER_EVALUATION);
    REG_OP(ObExprJsonLength, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonInsert, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonStorageSize, EAGER_EVALUATION);
    REG_OP(ObExprJsonStorageFree, EAGER_EVALUATION);
    REG_OP(ObExprJsonSet, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonMergePreserve, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonMerge, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonMergePatch, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonPretty, EAGER_EVALUATION);
    REG_OP(ObExprJsonMemberOf, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprExtractValue, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprUpdateXml, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSha, EAGER_EVALUATION);
    REG_SAME_OP(T_FUN_SYS_SHA ,T_FUN_SYS_SHA, N_SHA1, i, false);
    REG_OP(ObExprSha2, EAGER_EVALUATION);
    REG_OP(ObExprCompress, EAGER_EVALUATION);
    REG_OP(ObExprUncompress, EAGER_EVALUATION);
    REG_OP(ObExprUncompressedLength, EAGER_EVALUATION);
    REG_OP(ObExprStatementDigest, EAGER_EVALUATION);
    REG_OP(ObExprStatementDigestText, EAGER_EVALUATION);
    REG_OP(ObExprTimestampToScn, EAGER_EVALUATION);
    REG_OP(ObExprScnToTimestamp, EAGER_EVALUATION);
    REG_OP(ObExprSqlModeConvert, EAGER_EVALUATION);
    REG_OP(ObExprCanAccessTrigger, EAGER_EVALUATION);
    REG_OP(ObExprMysqlProcInfo, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprInnerTypeToEnumSet, SHORT_CIRCUIT_EVALUATION);
#if  defined(ENABLE_DEBUG_LOG) || !defined(NDEBUG)
    // convert input value into an OceanBase error number and throw out as exception
    REG_OP(ObExprErrno, EAGER_EVALUATION);
#endif
    REG_OP(ObExprPoint, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprLineString, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprMultiPoint, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprMultiLineString, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPolygon, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprMultiPolygon, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprGeomCollection, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprGeometryCollection, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTGeomFromText, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTArea, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTIntersects, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTX, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTY, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTLatitude, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTLongitude, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTTransform, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTTransform, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTCovers, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTBestsrid, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTAsText, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTAsWkt, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTBufferStrategy, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTBuffer, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSpatialCellid, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSpatialMbr, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTGeomFromEWKB, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTGeomFromWKB, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTGeometryFromWKB, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTGeomFromEwkt, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTAsEwkt, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTSRID, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTDistance, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTGeogFromText, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTGeographyFromText, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTSetSRID, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTGeometryFromText, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTPoint, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTIsValid, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTBuffer, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTDWithin, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTAsWkb, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprStPrivAsEwkb, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTAsBinary, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTDistanceSphere, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTContains, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTWithin, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprFormatBytes, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprFormatPicoTime, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprUuid2bin, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprIsUuid, EAGER_EVALUATION);
    REG_OP(ObExprBin2uuid, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprNameConst, EAGER_EVALUATION);
    REG_OP(ObExprDayName, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprDesDecrypt, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprDesEncrypt, EAGER_EVALUATION);
    REG_OP(ObExprEncrypt, EAGER_EVALUATION);
    REG_OP(ObExprCurrentScn, EAGER_EVALUATION);
    REG_OP(ObExprEncode, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprDecode, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprICUVersion, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprGeneratorFunc, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprZipf, EAGER_EVALUATION);
    REG_OP(ObExprNormal, EAGER_EVALUATION);
    REG_OP(ObExprUniform, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRandom, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRandstr, EAGER_EVALUATION);
    REG_OP(ObExprPrefixPattern, EAGER_EVALUATION);
    REG_OP(ObExprPrivSTNumInteriorRings, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTIsCollection, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTEquals, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTTouches, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprAlignDate4Cmp, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprJsonQuery, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprBM25, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprGetLock, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprIsFreeLock, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprIsUsedLock, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprReleaseLock, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprReleaseAllLocks, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprExtractExpiredTime, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprTransactionId, EAGER_EVALUATION);
    REG_OP(ObExprInnerRowCmpVal, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprLastRefreshScn, EAGER_EVALUATION);
    REG_OP(ObExprTopNFilter, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTMakeEnvelope, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTClipByBox2D, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTPointOnSurface, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTGeometryType, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTCrosses, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTOverlaps, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTUnion, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTLength, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTDifference, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTAsGeoJson, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTCentroid, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSTSymDifference, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTAsMVTGeom, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTMakeValid, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTGeoHash, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprPrivSTMakePoint, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprCurrentRole, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArray, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprDemoteCast, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRangePlacement, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprMap, SHORT_CIRCUIT_EVALUATION);
    /* vector index */
    REG_OP(ObExprVecIVFCenterID, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVecIVFCenterVector, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVecIVFFlatDataVector, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVecIVFSQ8DataVector, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVecIVFMetaID, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVecIVFMetaVector, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVecIVFPQCenterId, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVecIVFPQCenterIds, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVecIVFPQCenterVector, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVecVid, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVecType, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVecVector, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVecScn, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVecKey, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVecData, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVecChunk, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprEmbeddedVec, EAGER_EVALUATION);
    REG_OP(ObExprVecVisible, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSpivDim, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSpivValue, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVectorL2Distance, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVectorCosineDistance, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVectorIPDistance, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVectorNegativeIPDistance, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVectorL1Distance, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVectorDims, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVectorNorm, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVectorDistance, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSemanticDistance, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSemanticVectorDistance, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVectorL2Similarity, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVectorCosineSimilarity, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVectorIPSimilarity, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVectorSimilarity, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprInnerTableOptionPrinter, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprInnerTableSequenceGetter, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRbBuildEmpty, EAGER_EVALUATION);
    REG_OP(ObExprRbIsEmpty, EAGER_EVALUATION);
    REG_OP(ObExprRbBuildVarbinary, EAGER_EVALUATION);
    REG_OP(ObExprRbToVarbinary, EAGER_EVALUATION);
    REG_OP(ObExprRbCardinality, EAGER_EVALUATION);
    REG_OP(ObExprRbAndCardinality, EAGER_EVALUATION);
    REG_OP(ObExprRbOrCardinality, EAGER_EVALUATION);
    REG_OP(ObExprRbXorCardinality, EAGER_EVALUATION);
    REG_OP(ObExprRbAndnotCardinality, EAGER_EVALUATION);
    REG_OP(ObExprRbAndNull2emptyCardinality, EAGER_EVALUATION);
    REG_OP(ObExprRbOrNull2emptyCardinality, EAGER_EVALUATION);
    REG_OP(ObExprRbAndnotNull2emptyCardinality, EAGER_EVALUATION);
    REG_OP(ObExprRbAnd, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRbOr, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRbXor, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRbAndnot, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRbAndNull2empty, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRbOrNull2empty, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRbAndnotNull2empty, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRbToString, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRbFromString, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRbSelect, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRbBuild, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRbToArray, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprRbContains, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprGetPath, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprGTIDSubset, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprGTIDSubtract, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprWaitForExecutedGTIDSet, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprWaitUntilSQLThreadAfterGTIDs, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayContains, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayToString, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprStringToArray, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayAppend, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayLength, EAGER_EVALUATION);
    REG_OP(ObExprArrayPrepend, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayConcat, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayDifference, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayCompact, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArraySort, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArraySortby, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayFilter, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprElementAt, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayCardinality, EAGER_EVALUATION);
    REG_OP(ObExprArrayMax, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayMin, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayAvg, EAGER_EVALUATION);
    REG_OP(ObExprArrayFirst, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprDecodeTraceId, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprAuditLogSetFilter, EAGER_EVALUATION);
    REG_OP(ObExprAuditLogRemoveFilter, EAGER_EVALUATION);
    REG_OP(ObExprAuditLogSetUser, EAGER_EVALUATION);
    REG_OP(ObExprAuditLogRemoveUser, EAGER_EVALUATION);
    REG_OP(ObExprIsEnabledRole, EAGER_EVALUATION);
    REG_OP(ObExprSm3, EAGER_EVALUATION);
    REG_OP(ObExprSm4Encrypt, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSm4Decrypt, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprEnhancedAesEncrypt, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprEnhancedAesDecrypt, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprSplitPart, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprInnerIsTrue, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprInnerDecodeLike, EAGER_EVALUATION);
    REG_OP(ObExprInnerDoubleToInt, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprInnerDecimalToYear, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprTokenize, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayOverlaps, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayContainsAll, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayDistinct, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayRemove, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayMap, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArraySum, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayPosition, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArraySlice, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayRange, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayExcept, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayIntersect, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprArrayUnion, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprGetMySQLRoutineParameterTypeStr, EAGER_EVALUATION);
    REG_OP(ObExprCalcOdpsSize, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprToPinyin, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprURLEncode, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprURLDecode, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprKeyValue, EAGER_EVALUATION);
    REG_OP(ObExprMapKeys, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprMapValues, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprInnerInfoColsColumnDefPrinter, EAGER_EVALUATION);
    REG_OP(ObExprInnerInfoColsCharLenPrinter, EAGER_EVALUATION);
    REG_OP(ObExprInnerInfoColsCharNamePrinter, EAGER_EVALUATION);
    REG_OP(ObExprInnerInfoColsCollNamePrinter, EAGER_EVALUATION);
    REG_OP(ObExprInnerInfoColsPrivPrinter, EAGER_EVALUATION);
    REG_OP(ObExprInnerInfoColsExtraPrinter, EAGER_EVALUATION);
    REG_OP(ObExprInnerInfoColsDataTypePrinter, EAGER_EVALUATION);
    REG_OP(ObExprInnerInfoColsColumnTypePrinter, EAGER_EVALUATION);
    REG_OP(ObExprCurrentCatalog, EAGER_EVALUATION);
    REG_OP(ObExprCheckCatalogAccess, EAGER_EVALUATION);
    REG_OP(ObExprInnerInfoColsColumnKeyPrinter, EAGER_EVALUATION);
    REG_OP(ObExprCheckLocationAccess, EAGER_EVALUATION);
    REG_OP(ObExprStartUpMode, EAGER_EVALUATION);
    REG_OP(ObExprAIComplete, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprAIEmbed, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprAIRerank, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprLocalDynamicFilter, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprFormatProfile, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprBucket, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprVectorL2Squared, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprAIPrompt, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprMaxPt, EAGER_EVALUATION);
    REG_OP(ObExprDateTrunc, EAGER_EVALUATION);
    REG_OP(ObExprToDate, SHORT_CIRCUIT_EVALUATION);
    REG_SAME_OP(T_FUN_SYS_DATE_FORMAT, T_FUN_SYS_FORMAT_DATE_TIME, N_FORMAT_DATE_TIME, i, true);
    REG_SAME_OP(T_FUN_SYS_UNIX_TIMESTAMP, T_FUN_SYS_TO_UNIX_TIMESTAMP, N_TO_UNIX_TIMESTAMP, i, true);
    REG_OP(ObExprIsNan, SHORT_CIRCUIT_EVALUATION);
    REG_OP(ObExprCollectFileList, EAGER_EVALUATION);
    REG_OP(ObExprVoid, EAGER_EVALUATION);
    REG_OP(ObExprLoadFile, SHORT_CIRCUIT_EVALUATION);
  }();
// 注册oracle系统函数
  REG_OP_ORCL(ObExprSysConnectByPath, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprTimestampNvl, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprOracleToDate, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprToChar, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprToClob, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprToBlob, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprToTimestamp, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprToTimestampTZ, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprSysdate, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprFuncPartHash, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOracleDecode, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprUid, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprReplace, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprTranslate, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprLength, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprLengthb, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprEffectiveTenantId, EAGER_EVALUATION);   //未来在oracle租户下将要去除该id
  REG_OP_ORCL(ObExprRowNum, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprTrunc, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprUser, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprCast, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprMod, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprRemainder, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprAbs, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprConcat, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprToNumber, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprOracleRpad, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprOracleLpad, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprOracleInstr, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprInstrb, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprSubstr, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprInitcap, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprSubstrb, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprFuncRound, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprCoalesce, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprUserEnv, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprSysContext, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprLower, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprUpper, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprInnerTrim, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprTrim, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprSinh, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprCosh, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprTanh, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprLtrim, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprRtrim, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprOracleNvl, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprFuncCeil, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprFuncFloor, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprAsin, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprAcos, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprAtan, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprAtan2, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprSign, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprSysTimestamp, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprSoundex, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprLocalTimestamp, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprSetCollation, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprWidthBucket, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprChr, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprExtract, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprSqrt, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprNlsLower, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprNlsUpper, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprEstimateNdv, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprAtTimeZone, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprAtLocal, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprTimestampToScn, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprScnToTimestamp, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprNlsInitCap, SHORT_CIRCUIT_EVALUATION);
  //部分内部使用的表达式
  REG_OP_ORCL(ObExprAdd, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprAggAdd, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprEqual, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprLessEqual, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprNeg, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprLessThan, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprGreaterThan, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprNullSafeEqual, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprGreaterEqual, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprSubQueryRef, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprSubQueryEqual, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprSubQueryNotEqual, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprSubQueryNSEqual, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprSubQueryGreaterEqual, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprSubQueryGreaterThan, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprSubQueryLessEqual, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprSubQueryLessThan, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprRemoveConst, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprIs, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprIsNot, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprBetween, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprNotBetween, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprLike, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprRegexpSubstr, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprRegexpInstr, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprRegexpReplace, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprRegexpCount, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprRegexpLike, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprNot, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprAnd, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOr, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprIn, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprNotIn, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprArgCase, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprCase, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprQuote, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprConv, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprAssign, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprGetUserVar, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprGetSysVar, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprBitOr, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprBitXor, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprBitAnd, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprBitNeg, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprBitLeftShift, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprBitRightShift, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprAggParamList, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprPrior, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprObjAccess, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprConnectByRoot, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprGetPackageVar, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprGetSubprogramVar, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprShadowUKProject, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprXor, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprAutoincNextval, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprColumnConv, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprFunValues, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprPartId, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprSeqNextval, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprToType, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprNotEqual, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprMinus, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprAggMinus, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprPosition, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprMul, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprAggMul, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprDiv, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprAggDiv, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprFuncLnnvl, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprCurDate, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprPad, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprExists, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprNotExists, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprCurTimestamp, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprFunDefault, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprUDF, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprAscii, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprNvl2Oracle, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprToBinaryFloat, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprToBinaryDouble, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprOracleNullif, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprStmtId, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprNaNvl, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOutputPack, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprWrapperInner, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprReturningLob, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprDmlEvent, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprLeast, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprGreatest, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprHostIP, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprRpcPort, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprIsServingTenant, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprBitAndOra, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprHextoraw, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprRawtohex, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprRawtonhex, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprDateAdd, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprDateSub, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprPLIntegerChecker, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprPLGetCursorAttr, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprPLSQLCodeSQLErrm, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprPLSQLVariable, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprPLAssocIndex, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprCollectionConstruct, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprObjectConstruct, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprSessiontimezone, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprDbtimezone, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprSysExtractUtc, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprTzOffset, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprFromTz, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprSpatialCellid, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprSpatialMbr, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprToPinyin, SHORT_CIRCUIT_EVALUATION);
  //label security
  REG_OP_ORCL(ObExprOLSPolicyCreate, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSPolicyAlter, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSPolicyDrop, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSPolicyDisable, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSPolicyEnable, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSLevelCreate, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSLevelAlter, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSLevelDrop, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSLabelCreate, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSLabelAlter, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSLabelDrop, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSTablePolicyApply, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSTablePolicyRemove, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSTablePolicyDisable, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSTablePolicyEnable, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSUserSetLevels, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSSessionSetLabel, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSSessionSetRowLabel, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSSessionRestoreDefaultLabels, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSSessionLabel, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSSessionRowLabel, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSLabelCmpLE, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSLabelCheck, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSCharToLabel, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOLSLabelToChar, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprAddMonths, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprLastDay, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprNextDay, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprMonthsBetween, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprToYMInterval, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprToDSInterval, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprNumToYMInterval, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprNumToDSInterval, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprSin, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprCos, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprTan, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprVsize, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprOrahash, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprPower, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprExp, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprLn, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprLog, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprPLSeqNextval, SHORT_CIRCUIT_EVALUATION);

  REG_SAME_OP_ORCL(T_FUN_SYS_LENGTH, T_FUN_SYS_LENGTHC, "lengthc", j);
  REG_SAME_OP_ORCL(T_FUN_SYS_SUBSTR, T_FUN_SYS_SUBSTRC, "substrc", j);
  REG_SAME_OP_ORCL(T_FUN_SYS_INSTR,  T_FUN_SYS_INSTRC,  "instrc",  j);
  REG_OP_ORCL(ObExprFuncDump, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprCalcPartitionId, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprCalcTabletId, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprCalcPartitionTabletId, EAGER_EVALUATION);
  //SYS_GUID
  REG_OP_ORCL(ObExprSysGuid, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprPartIdPseudoColumn, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprToSingleByte, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprToMultiByte, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprMultiSet, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprUtlI18nStringToRaw, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprUtlI18nRawToChar, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprUtlInaddrGetHostAddr, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprUtlInaddrGetHostName, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprDbmsCryptoEncrypt, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprDbmsCryptoDecrypt, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprOracleToNChar, EAGER_EVALUATION);

  // URowID
  REG_OP_ORCL(ObExprCalcURowID, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprSet, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprCardinality, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprCollPred, EAGER_EVALUATION);
  // Priv
  REG_OP_ORCL(ObExprUserCanAccessObj, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprEmptyClob, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprEmptyBlob, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprJoinFilter, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprToOutfileRow, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprCharset, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprCollation, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprCoercibility, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprConvertOracle, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprUnistr, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprAsciistr, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprSysOpOpnsize, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprRowIDToChar, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprRowIDToNChar, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprCharToRowID, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprLastTraceId, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprReverse, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprEncodeSortkey, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprHash, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprNLSSort, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprObVersion, EAGER_EVALUATION);
#if  defined(ENABLE_DEBUG_LOG) || !defined(NDEBUG)
  REG_OP_ORCL(ObExprErrno, EAGER_EVALUATION);
#endif
  REG_OP_ORCL(ObExprJsonValue, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprIsJson, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprJsonEqual, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprJsonQuery, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprJsonMergePatch, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprJsonExists, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprJsonArray, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprJsonObject, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprCurrentScn, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprTreat, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprGeneratorFunc, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprZipf, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprNormal, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprUniform, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprRandom, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprRandstr, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprPrefixPattern, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprPrivXmlBinary, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprSysMakeXML, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprPrivXmlBinary, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprXmlparse, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprXmlElement, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprXmlConcat, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprXmlForest, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprXmlAttributes, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprExtractValue, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprExtractXml, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprExistsNodeXml, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprXmlSerialize, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprXmlcast, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprUpdateXml, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprInsertChildXml, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprDeleteXml, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprXmlSequence, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprUdtConstruct, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprUDTAttributeAccess, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprTempTableSSID, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprJsonObjectStar, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprTransactionId, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprOraLoginUser, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprInnerRowCmpVal, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprLastRefreshScn, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprTopNFilter, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprInnerTableOptionPrinter, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprInnerTableSequenceGetter, SHORT_CIRCUIT_EVALUATION);
  // REG_OP_ORCL(ObExprTopNFilter, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprSdoRelate, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprGetPath, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprDecodeTraceId, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprSplitPart, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprInnerIsTrue, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprInnerDecodeLike, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprInnerDoubleToInt, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprCalcOdpsSize, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprKeyValue, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprCurrentCatalog, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprCheckCatalogAccess, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprStartUpMode, SHORT_CIRCUIT_EVALUATION);
#if defined(ENABLE_DEBUG_LOG) || !defined(NDEBUG)
  REG_OP_ORCL(ObExprTmpFileOpen, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprTmpFileClose, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprTmpFileWrite, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprTmpFileRead, EAGER_EVALUATION);
#endif
  REG_OP_ORCL(ObExprLocalDynamicFilter, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprFormatProfile, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprCheckLocationAccess, EAGER_EVALUATION);
  REG_OP_ORCL(ObExprEnhancedAesEncrypt, SHORT_CIRCUIT_EVALUATION);
  REG_OP_ORCL(ObExprMaxPt, EAGER_EVALUATION);
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
  } else if (FALSE_IT(expr_op->set_eval_order(lib::is_mysql_mode() ?
                                              OP_EVAL_ORDERS[type]
                                              : OP_EVAL_ORDERS_ORCL[type]))) {
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
    } else if (0 == origin_name.case_compare("VEC_IVF_CENTER_ID")) {
      alias_name = ObString::make_string(N_VEC_IVF_CENTER_ID);
    } else if (0 == origin_name.case_compare("VEC_IVF_CENTER_VECTOR")) {
      alias_name = ObString::make_string(N_VEC_IVF_CENTER_VECTOR);
    } else if (0 == origin_name.case_compare("VEC_IVF_SQ8_DATA_VECTOR")) {
      alias_name = ObString::make_string(N_VEC_IVF_SQ8_DATA_VECTOR);
    } else if (0 == origin_name.case_compare("VEC_IVF_FLAT_DATA_VECTOR")) {
      alias_name = ObString::make_string(N_VEC_IVF_FLAT_DATA_VECTOR);
    } else if (0 == origin_name.case_compare("VEC_IVF_META_ID")) {
      alias_name = ObString::make_string(N_VEC_IVF_META_ID);
    } else if (0 == origin_name.case_compare("VEC_IVF_META_VECTOR")) {
      alias_name = ObString::make_string(N_VEC_IVF_META_VECTOR);
    } else if (0 == origin_name.case_compare("VEC_IVF_PQ_CENTER_ID")) {
      alias_name = ObString::make_string(N_VEC_IVF_PQ_CENTER_ID);
    } else if (0 == origin_name.case_compare("VEC_IVF_PQ_CENTER_IDS")) {
      alias_name = ObString::make_string(N_VEC_IVF_PQ_CENTER_IDS);
    } else if (0 == origin_name.case_compare("VEC_IVF_PQ_CENTER_VECTOR")) {
      alias_name = ObString::make_string(N_VEC_IVF_PQ_CENTER_VECTOR);
    } else if (0 == origin_name.case_compare("VEC_VID")) {
      alias_name = ObString::make_string(N_VEC_VID);
    } else if (0 == origin_name.case_compare("VEC_TYPE")) {
      alias_name = ObString::make_string(N_VEC_TYPE);
    } else if (0 == origin_name.case_compare("VEC_VECTOR")) {
      alias_name = ObString::make_string(N_VEC_VECTOR);
    } else if (0 == origin_name.case_compare("EMBEDDED_VEC")) {
      alias_name = ObString::make_string(N_EMBEDDED_VEC);
    } else if (0 == origin_name.case_compare("VEC_SCN")) {
      alias_name = ObString::make_string(N_VEC_SCN);
    } else if (0 == origin_name.case_compare("VEC_KEY")) {
      alias_name = ObString::make_string(N_VEC_KEY);
    } else if (0 == origin_name.case_compare("VEC_DATA")) {
      alias_name = ObString::make_string(N_VEC_DATA);
    } else if (0 == origin_name.case_compare("VEC_CHUNK")) {
      alias_name = ObString::make_string(N_VEC_CHUNK);
    } else if (0 == origin_name.case_compare("VEC_VISIBLE")) {
      alias_name = ObString::make_string(N_VEC_VISIBLE);
    } else if (0 == origin_name.case_compare("SPIV_DIM")) {
      alias_name = ObString::make_string(N_SPIV_DIM);
    } else if (0 == origin_name.case_compare("SPIV_VALUE")) {
      alias_name = ObString::make_string(N_SPIV_VALUE);
    } else if (0 == origin_name.case_compare("DOC_ID")) {
      alias_name = ObString::make_string(N_DOC_ID);
    } else if (0 == origin_name.case_compare("ws")) {
      // ws is synonym for word_segment
      alias_name = ObString::make_string(N_WORD_SEGMENT);
    } else if (0 == origin_name.case_compare("WORD_COUNT")) {
      alias_name = ObString::make_string(N_WORD_COUNT);
    } else if (0 == origin_name.case_compare("DOC_LENGTH")) {
      alias_name = ObString::make_string(N_DOC_LENGTH);
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
    } else if (0 == origin_name.case_compare("centroid")) {
      // centroid is synonym for st_centroid
      alias_name = ObString::make_string(N_ST_CENTROID);
    } else if (0 == origin_name.case_compare("semantic_distance")) {
      alias_name = ObString::make_string(N_SEMANTIC_DISTANCE);
    } else {
      //do nothing
    }
  } else {
    //for synonyms in oracle mode
  }
}

} //end sql
} //end oceanbase

