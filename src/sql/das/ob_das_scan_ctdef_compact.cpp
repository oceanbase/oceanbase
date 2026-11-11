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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_das_scan_ctdef_compact.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/engine/ob_bit_vector.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_ser_compact.h"
#include "sql/ob_sql_context.h"
#include "sql/optimizer/ob_route_policy.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_cluster_version.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/ob_ser_compact_bindings.h"

namespace oceanbase
{
namespace sql
{
namespace das_compact
{

// File-local helpers live in this uniquely-named namespace instead of an
// anonymous one: this repository builds as unity blobs where anonymous
// namespaces of concatenated TUs could collide.
namespace scan_ctdef_codec
{

// Groups preserve the byte order of the existing compact codec. External
// members remain interleaved with constant members; regrouping them would
// change the wire even if the final size stayed equal.
#define OB_SCAN_CTDEF_COMPACT_PREFIX_FIELDS(ACTION)                                                                    \
  ACTION(ref_table_id_)                                                                                                \
  ACTION(access_column_ids_)                                                                                           \
  ACTION(schema_version_)

#define OB_SCAN_CTDEF_COMPACT_POST_TABLE_PARAM_FIELDS(ACTION)                                                          \
  ACTION(pd_expr_spec_)                                                                                                \
  ACTION(aggregate_column_ids_)                                                                                        \
  ACTION(group_id_expr_)                                                                                               \
  ACTION(result_output_)                                                                                               \
  ACTION(is_get_)                                                                                                      \
  ACTION(is_external_table_)

#define OB_SCAN_CTDEF_COMPACT_EXTERNAL_HEAD_FIELDS(ACTION)                                                             \
  ACTION(external_file_location_)                                                                                      \
  ACTION(external_file_access_info_)                                                                                   \
  ACTION(external_files_)                                                                                              \
  ACTION(external_file_format_str_)

#define OB_SCAN_CTDEF_COMPACT_MIDDLE_FIELDS(ACTION)                                                                    \
  ACTION(trans_info_expr_)                                                                                             \
  ACTION(group_by_column_ids_)                                                                                         \
  ACTION(ir_scan_type_)                                                                                                \
  ACTION(rowkey_exprs_)                                                                                                \
  ACTION(table_scan_opt_)                                                                                              \
  ACTION(doc_id_idx_)                                                                                                  \
  ACTION(vec_vid_idx_)                                                                                                 \
  ACTION(multivalue_idx_)                                                                                              \
  ACTION(multivalue_type_)                                                                                             \
  ACTION(index_merge_idx_)                                                                                             \
  ACTION(flags_)

#define OB_SCAN_CTDEF_COMPACT_SUFFIX_FIELDS(ACTION)                                                                    \
  ACTION(push_down_topn_)                                                                                              \
  ACTION(cache_aware_row_num_)

#define OB_SCAN_CTDEF_COMPACT_RANGE_FIELDS(ACTION)                                                                     \
  ACTION(domain_id_idxs_)                                                                                              \
  ACTION(domain_types_)                                                                                                \
  ACTION(domain_tids_)                                                                                                 \
  ACTION(pre_query_range_)                                                                                             \
  ACTION(pre_range_graph_)

#define OB_SCAN_CTDEF_FIELD_ENCODE(FIELD) OB_UNIS_ENCODE(ctdef.FIELD);
#define OB_SCAN_CTDEF_FIELD_DECODE(FIELD) OB_UNIS_DECODE(ctdef.FIELD);
#define OB_SCAN_CTDEF_FIELD_ADD_LEN(FIELD) OB_UNIS_ADD_LEN(ctdef.FIELD);

// external_object_ctx_ is produced (and consumed) for external tables and for
// virtual tables, so it keeps its own condition instead of joining the block.
inline bool has_external_object_ctx(const ObDASScanCtDef &ctdef)
{
  return ctdef.is_external_table_ || ctdef.table_param_.get_table_type() == share::schema::VIRTUAL_TABLE;
}

int encode_table_param_descriptor(const ObDASScanCtDef &ctdef, char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<bool> &output_sel_mask = ctdef.table_param_.get_output_sel_mask();
  const int32_t mask_bytes = static_cast<int32_t>(ObTinyBitVector::memory_size(output_sel_mask.count()));
  // bit0: plan_enable_rich_format_; bit1 (M6): is_safe_filter_with_di_; bit2: CG read mode.
  // computed by CG after convert() from the raw filter exprs, not replayable
  // on the runner, so it travels in the descriptor
  const int8_t desc_flags = static_cast<int8_t>((ctdef.table_param_.plan_enable_rich_format() ? 0x1 : 0x0)
                                                | (ctdef.table_param_.is_safe_filter_with_di() ? 0x2 : 0x0)
                                                | (ctdef.table_param_.get_read_info().is_oracle_mode() ? 0x4 : 0x0));
  if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, desc_flags))) {
    LOG_WARN("encode ScanCtDef table-param descriptor flags failed", K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, mask_bytes))) {
    LOG_WARN("encode ScanCtDef table-param mask size failed", K(ret), K(mask_bytes));
  } else if (OB_UNLIKELY(mask_bytes > buf_len - pos)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("ScanCtDef table-param mask buffer is too small", K(ret), K(mask_bytes), K(pos), K(buf_len));
  } else {
    ObTinyBitVector *mask = reinterpret_cast<ObTinyBitVector *>(buf + pos);
    mask->init(output_sel_mask.count());
    for (int64_t i = 0; i < output_sel_mask.count(); ++i) {
      if (output_sel_mask.at(i)) { mask->set(i); }
    }
    pos += mask_bytes;
  }
  return ret;
}

int64_t table_param_descriptor_size(const ObDASScanCtDef &ctdef)
{
  const int32_t mask_bytes =
    static_cast<int32_t>(ObTinyBitVector::memory_size(ctdef.table_param_.get_output_sel_mask().count()));
  return serialization::encoded_length_i8(0) + serialization::encoded_length_vi32(mask_bytes)
         + mask_bytes * serialization::encoded_length_i8(0);
}

int decode_table_param_descriptor(const char *buf, const int64_t data_len, int64_t &pos, bool &plan_enable_rich_format,
                                  bool &is_safe_filter_with_di, bool &table_read_is_oracle_mode,
                                  const char *&output_sel_mask, int64_t &mask_bytes)
{
  int ret = OB_SUCCESS;
  int8_t desc_flags = 0;
  int32_t decoded_mask_bytes = 0;
  if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, &desc_flags))) {
    LOG_WARN("decode ScanCtDef table-param descriptor flags failed", K(ret));
  } else if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, &decoded_mask_bytes))) {
    LOG_WARN("decode ScanCtDef table-param mask size failed", K(ret));
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(decoded_mask_bytes < 0 || decoded_mask_bytes > data_len - pos)) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("invalid ScanCtDef table-param mask length", K(ret), K(decoded_mask_bytes), K(pos), K(data_len));
  } else if (OB_SUCC(ret)) {
    plan_enable_rich_format = 0 != (desc_flags & 0x1);
    is_safe_filter_with_di = 0 != (desc_flags & 0x2);
    table_read_is_oracle_mode = 0 != (desc_flags & 0x4);
    output_sel_mask = buf + pos;
    mask_bytes = decoded_mask_bytes;
    pos += decoded_mask_bytes;
  }
  return ret;
}

// Fetch the table schema at exactly schema_version for the rebuild fast path
// (design doc compact serialize opt): try the versioned guard first; if the requested
// version is newer than the local latest (runner refresh lags behind the
// control end), refresh once from RS and retry. Any failure degrades to
// OB_DAS_CTDEF_REBUILD_MISS (compact serialize opt): the control end then resends the task
// with the ctdef encoded TP_INLINE, invisible to the statement. Never rebuild
// with a wrong-version schema.
// NOTE the observer-side baseline window may silently bump a too-old requested
// version, so the caller MUST re-check table_schema->get_schema_version().
int fetch_schema_at_version(const uint64_t table_id, const int64_t schema_version,
                            const share::schema::ObTableSchema *&table_schema,
                            share::schema::ObSchemaGetterGuard &versioned_guard, common::ObKVCacheHandle &schema_handle)
{
  int ret = OB_SUCCESS;
  int errsim_no = OB_E(EventTable::EN_DAS_REBUILD_TABLE_PARAM) OB_SUCCESS;
  table_schema = nullptr;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is required to rebuild ScanCtDef table param", K(ret));
  } else {
    if (OB_SCHEMA_EAGAIN == errsim_no) {
      ret = OB_SCHEMA_EAGAIN;
    } else {
      ret = GCTX.schema_service_->get_tenant_schema_guard(MTL_ID(), versioned_guard, schema_version);
    }
    if (OB_FAIL(ret)) {
      if (OB_SCHEMA_EAGAIN != ret) {
        LOG_WARN("get versioned schema guard for ScanCtDef table param rebuild failed", K(ret), K(table_id),
                 K(schema_version));
      } else {
        // requested version is newer than the local latest: pull once and retry
        ret = OB_SUCCESS;
        common::ObSEArray<uint64_t, 1> tenant_ids;
        if (OB_FAIL(tenant_ids.push_back(MTL_ID()))) {
          LOG_WARN("push back tenant id failed", K(ret));
        } else if (OB_FAIL(GCTX.schema_service_->refresh_and_add_schema(tenant_ids))) {
          LOG_WARN("refresh schema for ScanCtDef table param rebuild failed", K(ret), K(table_id), K(schema_version));
        } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(MTL_ID(), versioned_guard, schema_version))) {
          LOG_WARN("get versioned schema guard after refresh failed", K(ret), K(table_id), K(schema_version));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // Version-explicit fetch straight from the schema cache (lazy mode,
    // mgr == nullptr): the KV cache is keyed with the schema version, which
    // is the canonical precise lookup (same mechanism CDC's historical reads
    // use). ObSchemaGetterGuard::get_table_schema cannot be used here: its
    // version-blind 3-arg form resolves the latest version from the schema
    // mgr, not the requested one.
    share::schema::ObRefreshSchemaStatus schema_status;
    schema_status.tenant_id_ = MTL_ID();
    const share::schema::ObSchema *base_schema = nullptr;
    if (OB_ERR_UNEXPECTED == errsim_no) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = GCTX.schema_service_->get_schema(nullptr, schema_status, share::schema::TABLE_SCHEMA, table_id,
                                            schema_version, schema_handle, base_schema);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("get table schema at version failed", K(ret), K(table_id), K(schema_version));
    } else {
      table_schema = static_cast<const share::schema::ObTableSchema *>(base_schema);
    }
  }
  if (OB_SUCC(ret) && (OB_ISNULL(table_schema) || table_schema->get_schema_version() != schema_version)) {
    // version recycled out of the observer-side window or silently bumped:
    // no precise schema available, degrade to the force-inline resend (compact serialize opt)
    ret = OB_DAS_CTDEF_REBUILD_MISS;
    LOG_INFO("precise schema version unavailable for ScanCtDef table param rebuild", K(ret), K(table_id),
             K(schema_version), KPC(table_schema));
  }
  if (OB_FAIL(ret) && OB_DAS_CTDEF_REBUILD_MISS != ret) {
    LOG_WARN("fetch schema at version failed, degrade to force-inline resend", K(ret), K(table_id), K(schema_version));
    ret = OB_DAS_CTDEF_REBUILD_MISS;
  }
  return ret;
}

} // namespace scan_ctdef_codec

bool can_rebuild_table_param(const ObDASScanCtDef &ctdef)
{
  const share::schema::ObTableParam &table_param = ctdef.table_param_;
  return OB_INVALID_ID != ctdef.ref_table_id_ && 0 <= ctdef.schema_version_ && !ctdef.is_external_table_
         && ctdef.aggregate_column_ids_.empty() && ctdef.group_by_column_ids_.empty()
         && ctdef.aggregate_param_props_.empty() && nullptr == ctdef.group_id_expr_
         && ObTSCIRScanType::OB_NOT_A_SPEC_SCAN == ctdef.ir_scan_type_ && !ctdef.is_index_merge_
         && !table_param.is_fts_index() && !table_param.is_spatial_index() && !table_param.is_multivalue_index()
         && !table_param.is_vec_index() && !table_param.is_mlog_table() && !table_param.is_enable_semistruct_encoding()
         && !table_param.is_column_replica_table() && share::schema::VIRTUAL_TABLE != table_param.get_table_type()
         && table_param.get_output_sel_mask().count() <= static_cast<int64_t>(INT32_MAX) * 8
         && ctdef.access_column_ids_.count() == table_param.get_output_sel_mask().count();
}

int rebuild_table_param_for_scan_ctdef(ObDASScanCtDef &ctdef, const bool plan_enable_rich_format,
                                       const bool is_safe_filter_with_di, const bool table_read_is_oracle_mode,
                                       const char *output_sel_mask, const int64_t mask_bytes, ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  int errsim_no = OB_E(EventTable::EN_DAS_REBUILD_TABLE_PARAM) OB_SUCCESS;
  ObSqlCtx *sql_ctx = exec_ctx.get_sql_ctx();
  share::schema::ObSchemaGetterGuard local_schema_guard;
  // holds the KV cache handle of a precisely-versioned schema (M5 path);
  // must outlive table_param.convert() below
  share::schema::ObSchemaGetterGuard versioned_schema_guard;
  // keeps the precisely-versioned schema cache entry alive (M5 path);
  // must outlive table_param.convert() below
  common::ObKVCacheHandle versioned_schema_handle;
  share::schema::ObSchemaGetterGuard *schema_guard = OB_ISNULL(sql_ctx) ? nullptr : sql_ctx->schema_guard_;
  const share::schema::ObTableSchema *table_schema = nullptr;
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  int64_t route_policy = 0;
  bool query_cs_replica = false;
  if (OB_ISNULL(schema_guard)) {
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is required to rebuild ScanCtDef table param", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(MTL_ID(), local_schema_guard))) {
      LOG_WARN("get tenant schema guard for ScanCtDef table param rebuild failed", K(ret));
    } else {
      schema_guard = &local_schema_guard;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_guard->get_table_schema(MTL_ID(), ctdef.ref_table_id_, table_schema))) {
      LOG_WARN("get table schema for ScanCtDef table param rebuild failed", K(ret), K(ctdef.ref_table_id_));
    } else if (OB_NOT_NULL(table_schema) && table_schema->get_schema_version() == ctdef.schema_version_
               && OB_SUCCESS == errsim_no) {
      // fast path: the latest schema is exactly the CG-time version
    } else {
      // M5: version mismatch (DDL in flight or runner refresh lag). Fetch the
      // schema at exactly ctdef.schema_version_ from the multi-version window
      // (refreshing once if the runner lags); only degrade to statement-level
      // retry when the precise version is genuinely unavailable.
      if (OB_FAIL(scan_ctdef_codec::fetch_schema_at_version(ctdef.ref_table_id_, ctdef.schema_version_, table_schema,
                                                            versioned_schema_guard, versioned_schema_handle))) {
        LOG_WARN("table schema changed and precise version unavailable, retry statement", K(ret),
                 K(ctdef.ref_table_id_), K(ctdef.schema_version_), KP(table_schema));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(session) && OB_FAIL(session->get_sys_variable(SYS_VAR_OB_ROUTE_POLICY, route_policy))) {
      LOG_WARN("get route policy for ScanCtDef table param rebuild failed", K(ret));
    } else {
      query_cs_replica = ObRoutePolicyType::COLUMN_STORE_ONLY == route_policy;
    }
  }

  if (OB_SUCC(ret)) {
    share::schema::ObTableParam &table_param = ctdef.table_param_;
    table_param.reset();
    table_param.set_is_spatial_index(table_schema->is_spatial_index());
    table_param.set_is_fts_index(table_schema->is_fts_index());
    table_param.set_is_multivalue_index(table_schema->is_multivalue_index_aux());
    table_param.set_is_vec_index(table_schema->is_vec_index());
    table_param.set_is_enable_semistruct_encoding(
      table_schema->get_semistruct_encoding_type().is_enable_semistruct_encoding());
    table_param.set_is_partition_table(table_schema->is_partitioned_table());
    table_param.set_is_mlog_table(table_schema->is_mlog_table());
    table_param.set_plan_enable_rich_format(plan_enable_rich_format);
    table_param.get_enable_lob_locator_v2() = GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_1_0_0;
    // The original Oracle virtual-table identity is no longer present in ref_table_id_.
    if (OB_UNLIKELY(table_read_is_oracle_mode && !lib::is_oracle_mode())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Oracle table read mode requires an Oracle execution context", K(ret), K(ctdef.ref_table_id_));
    } else if (OB_FAIL(table_param.convert(*table_schema, ctdef.access_column_ids_,
                                           ctdef.pd_expr_spec_.pd_storage_flag_, nullptr,
                                           !table_read_is_oracle_mode /*force_mysql_mode*/, query_cs_replica))) {
      LOG_WARN("convert table schema for ScanCtDef table param rebuild failed", K(ret), K(ctdef.ref_table_id_),
               K(ctdef.access_column_ids_));
    } else if (OB_FAIL(table_param.assign_output_sel_mask_from_bitmap(ctdef.access_column_ids_.count(), output_sel_mask,
                                                                      mask_bytes))) {
      LOG_WARN("restore ScanCtDef output selection mask failed", K(ret), K(ctdef.ref_table_id_), K(mask_bytes));
    } else {
      // M6: is_safe_filter_with_di_ is computed by CG after convert() from the
      // raw filter exprs (not replayable on the runner); restore the CG value
      // carried in the descriptor.
      table_param.set_is_safe_filter_with_di(is_safe_filter_with_di);
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObDASScanCtDefCompact)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("compact ScanCtDef is not bound", K(ret));
  } else {
    using namespace scan_ctdef_codec;
    serialization::SerCompactEncodingGuard compact_guard(true);
    const ObDASScanCtDef &ctdef = *ctdef_;
    const bool force_inline_table_param = force_inline_table_param_;
    const bool rebuild_table_param = !force_inline_table_param && can_rebuild_table_param(ctdef);
    const bool is_external = ctdef.is_external_table_;
    OB_SCAN_CTDEF_COMPACT_PREFIX_FIELDS(OB_SCAN_CTDEF_FIELD_ENCODE)
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, rebuild_table_param ? TP_REBUILD : TP_INLINE))) {
      LOG_WARN("encode ScanCtDef table-param mode failed", K(ret), K(rebuild_table_param));
    }
    if (OB_SUCC(ret) && rebuild_table_param) {
      if (OB_FAIL(encode_table_param_descriptor(ctdef, buf, buf_len, pos))) {
        LOG_WARN("encode ScanCtDef table-param descriptor failed", K(ret));
      }
    } else if (OB_SUCC(ret)) {
      OB_UNIS_ENCODE(ctdef.table_param_);
    }
    OB_SCAN_CTDEF_COMPACT_POST_TABLE_PARAM_FIELDS(OB_SCAN_CTDEF_FIELD_ENCODE)
    if (is_external) { OB_SCAN_CTDEF_COMPACT_EXTERNAL_HEAD_FIELDS(OB_SCAN_CTDEF_FIELD_ENCODE) }
    OB_SCAN_CTDEF_COMPACT_MIDDLE_FIELDS(OB_SCAN_CTDEF_FIELD_ENCODE)
    if (is_external) { OB_UNIS_ENCODE(ctdef.partition_infos_); }
    OB_SCAN_CTDEF_COMPACT_RANGE_FIELDS(OB_SCAN_CTDEF_FIELD_ENCODE)
    if (is_external) { OB_UNIS_ENCODE(ctdef.external_file_pattern_); }
    if (has_external_object_ctx(ctdef)) { OB_UNIS_ENCODE(ctdef.external_object_ctx_); }
    if (is_external) { OB_UNIS_ENCODE(ctdef.external_pushdown_filters_); }
    if (!rebuild_table_param) { OB_UNIS_ENCODE(ctdef.aggregate_param_props_); }
    if (is_external) { OB_UNIS_ENCODE(ctdef.lake_table_format_); }
    OB_SCAN_CTDEF_COMPACT_SUFFIX_FIELDS(OB_SCAN_CTDEF_FIELD_ENCODE)
    if (is_external) { OB_UNIS_ENCODE(ctdef.external_file_pattern_type_); }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDASScanCtDefCompact)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("compact ScanCtDef is not bound", K(ret));
  } else {
    using namespace scan_ctdef_codec;
    serialization::CompactExprPtrGuard compact_guard(true);
    ObDASScanCtDef &ctdef = *ctdef_;
    OB_SCAN_CTDEF_COMPACT_PREFIX_FIELDS(OB_SCAN_CTDEF_FIELD_DECODE)
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, &table_param_mode_))) {
      LOG_WARN("decode ScanCtDef table-param mode failed", K(ret));
    }
    if (OB_SUCC(ret) && TP_INLINE == table_param_mode_) {
      OB_UNIS_DECODE(ctdef.table_param_);
    } else if (OB_SUCC(ret) && TP_REBUILD == table_param_mode_) {
      if (OB_FAIL(decode_table_param_descriptor(buf, data_len, pos, plan_enable_rich_format_, is_safe_filter_with_di_,
                                                table_read_is_oracle_mode_, output_sel_mask_, mask_bytes_))) {
        LOG_WARN("decode ScanCtDef table-param descriptor failed", K(ret));
      }
    } else if (OB_SUCC(ret)) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("invalid ScanCtDef table-param mode", K(ret), K(table_param_mode_));
    }
    OB_SCAN_CTDEF_COMPACT_POST_TABLE_PARAM_FIELDS(OB_SCAN_CTDEF_FIELD_DECODE)
    const bool is_external = ctdef.is_external_table_;
    if (OB_SUCC(ret) && is_external) { OB_SCAN_CTDEF_COMPACT_EXTERNAL_HEAD_FIELDS(OB_SCAN_CTDEF_FIELD_DECODE) }
    OB_SCAN_CTDEF_COMPACT_MIDDLE_FIELDS(OB_SCAN_CTDEF_FIELD_DECODE)
    if (OB_SUCC(ret) && is_external) { OB_UNIS_DECODE(ctdef.partition_infos_); }
    OB_SCAN_CTDEF_COMPACT_RANGE_FIELDS(OB_SCAN_CTDEF_FIELD_DECODE)
    if (OB_SUCC(ret) && is_external) { OB_UNIS_DECODE(ctdef.external_file_pattern_); }
    if (OB_SUCC(ret) && has_external_object_ctx(ctdef)) { OB_UNIS_DECODE(ctdef.external_object_ctx_); }
    if (OB_SUCC(ret) && is_external) { OB_UNIS_DECODE(ctdef.external_pushdown_filters_); }
    if (TP_INLINE == table_param_mode_) { OB_UNIS_DECODE(ctdef.aggregate_param_props_); }
    if (OB_SUCC(ret) && is_external) { OB_UNIS_DECODE(ctdef.lake_table_format_); }
    OB_SCAN_CTDEF_COMPACT_SUFFIX_FIELDS(OB_SCAN_CTDEF_FIELD_DECODE)
    if (OB_SUCC(ret) && is_external) { OB_UNIS_DECODE(ctdef.external_file_pattern_type_); }
    if (OB_SUCC(ret) && pos != data_len) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("compact ScanCtDef body length mismatch", K(ret), K(pos), K(data_len));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASScanCtDefCompact)
{
  int64_t len = 0;
  if (OB_ISNULL(ctdef_)) {
  } else {
    using namespace scan_ctdef_codec;
    serialization::SerCompactEncodingGuard compact_guard(true);
    const ObDASScanCtDef &ctdef = *ctdef_;
    const bool force_inline_table_param = force_inline_table_param_;
    const bool rebuild_table_param = !force_inline_table_param && can_rebuild_table_param(ctdef);
    const bool is_external = ctdef.is_external_table_;
    OB_SCAN_CTDEF_COMPACT_PREFIX_FIELDS(OB_SCAN_CTDEF_FIELD_ADD_LEN)
    len += serialization::encoded_length_i8(rebuild_table_param ? TP_REBUILD : TP_INLINE);
    if (rebuild_table_param) {
      len += table_param_descriptor_size(ctdef);
    } else {
      OB_UNIS_ADD_LEN(ctdef.table_param_);
    }
    OB_SCAN_CTDEF_COMPACT_POST_TABLE_PARAM_FIELDS(OB_SCAN_CTDEF_FIELD_ADD_LEN)
    if (is_external) { OB_SCAN_CTDEF_COMPACT_EXTERNAL_HEAD_FIELDS(OB_SCAN_CTDEF_FIELD_ADD_LEN) }
    OB_SCAN_CTDEF_COMPACT_MIDDLE_FIELDS(OB_SCAN_CTDEF_FIELD_ADD_LEN)
    if (is_external) { OB_UNIS_ADD_LEN(ctdef.partition_infos_); }
    OB_SCAN_CTDEF_COMPACT_RANGE_FIELDS(OB_SCAN_CTDEF_FIELD_ADD_LEN)
    if (is_external) { OB_UNIS_ADD_LEN(ctdef.external_file_pattern_); }
    if (has_external_object_ctx(ctdef)) { OB_UNIS_ADD_LEN(ctdef.external_object_ctx_); }
    if (is_external) { OB_UNIS_ADD_LEN(ctdef.external_pushdown_filters_); }
    if (!rebuild_table_param) { OB_UNIS_ADD_LEN(ctdef.aggregate_param_props_); }
    if (is_external) { OB_UNIS_ADD_LEN(ctdef.lake_table_format_); }
    OB_SCAN_CTDEF_COMPACT_SUFFIX_FIELDS(OB_SCAN_CTDEF_FIELD_ADD_LEN)
    if (is_external) { OB_UNIS_ADD_LEN(ctdef.external_file_pattern_type_); }
  }
  return len;
}

int ObDASScanCtDefCompact::replay_to(ObDASScanCtDef &dst, ObScanCtDefReplayCtx &ctx) const
{
  int ret = OB_SUCCESS;
  if (&dst != ctdef_) {
    ret = OB_INVALID_ARGUMENT;
  } else if (TP_REBUILD == table_param_mode_) {
    if (OB_ISNULL(ctx.exec_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ScanCtDef table-param rebuild requires execution context", K(ret));
    } else if (OB_FAIL(rebuild_table_param_for_scan_ctdef(dst, plan_enable_rich_format_, is_safe_filter_with_di_,
                                                          table_read_is_oracle_mode_, output_sel_mask_, mask_bytes_,
                                                          *ctx.exec_ctx_))) {
      LOG_WARN("rebuild ScanCtDef table param failed", K(ret), K(dst.ref_table_id_));
    }
  }
  return ret;
}

int serialize_scan_ctdef(const ObDASScanCtDef &ctdef, char *buf, const int64_t buf_len, int64_t &pos,
                         const bool force_inline_table_param)
{
  int ret = OB_SUCCESS;
  ObDASScanCtDefCompact compact(ctdef, force_inline_table_param);
  OB_UNIS_ENCODE(compact);
  return ret;
}

int deserialize_scan_ctdef(ObDASScanCtDef &ctdef, const char *buf, const int64_t data_len, int64_t &pos,
                           const ObScanCtDefDecodeCtx *ctx, SerCompactState *ser_compact_state)
{
  int ret = OB_SUCCESS;
  const SerCompactState state = OB_NOT_NULL(ser_compact_state) ? *ser_compact_state : SerCompactState::UNKNOWN;
  ObDASScanCtDefCompact compact(ctdef);
  DEF_UNIS_VERSION_DECODE_REC(record, ctdef, compact);
  OB_UNIS_DECODE(record);
  if (OB_SUCC(ret) && !record.decoded()) {
    ret = OB_DESERIALIZE_ERROR;
  } else if (OB_SUCC(ret)) {
    const SerCompactState decoded_state = record.version() == ObDASScanCtDefCompact::unis_declared_version() ?
                                            SerCompactState::V4 :
                                            SerCompactState::LEGACY;
    if (SerCompactState::UNKNOWN != state && state != decoded_state) {
      ret = OB_NOT_SUPPORTED;
    } else if (SerCompactState::V4 == decoded_state) {
      ObScanCtDefReplayCtx replay_ctx{OB_NOT_NULL(ctx) ? ctx->exec_ctx_ : nullptr};
      ret = compact.replay_to(ctdef, replay_ctx);
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(ser_compact_state)) {
      *ser_compact_state = decoded_state;
    }
  }
  return ret;
}

int64_t scan_ctdef_size(const ObDASScanCtDef &ctdef, const bool force_inline_table_param)
{
  ObDASScanCtDefCompact compact(ctdef, force_inline_table_param);
  return serialization::encoded_length(compact);
}

#undef OB_SCAN_CTDEF_FIELD_ENCODE
#undef OB_SCAN_CTDEF_FIELD_DECODE
#undef OB_SCAN_CTDEF_FIELD_ADD_LEN

#undef OB_SCAN_CTDEF_COMPACT_PREFIX_FIELDS
#undef OB_SCAN_CTDEF_COMPACT_POST_TABLE_PARAM_FIELDS
#undef OB_SCAN_CTDEF_COMPACT_EXTERNAL_HEAD_FIELDS
#undef OB_SCAN_CTDEF_COMPACT_MIDDLE_FIELDS
#undef OB_SCAN_CTDEF_COMPACT_SUFFIX_FIELDS
#undef OB_SCAN_CTDEF_COMPACT_RANGE_FIELDS

} // namespace das_compact
} // namespace sql
} // namespace oceanbase
