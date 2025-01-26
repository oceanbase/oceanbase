/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_CONTEXT_H_
#define OCEANBASE_OBSERVER_OB_TABLE_CONTEXT_H_

#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/engine/dml/ob_dml_ctx_define.h"
#include "sql/das/ob_das_scan_op.h" // for ObDASScanRtDef
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "share/table/ob_table.h"
#include "ob_table_session_pool.h"
#include "ob_table_schema_cache.h"
#include "ob_table_audit.h"
#include "redis/ob_redis_ttl.h"
#include "sql/das/ob_das_attach_define.h"
#include "fts/ob_table_fts_context.h"
namespace oceanbase
{
namespace table
{
struct ObTableIndexInfo
{
  typedef common::ObSEArray<common::ObTableID, 4, common::ModulePageAllocator, true> TableIDArray;
  ObTableIndexInfo()
      : data_table_id_(common::OB_INVALID_ID),
        index_table_id_(common::OB_INVALID_ID),
        index_schema_(nullptr),
        is_primary_index_(false),
        loc_meta_(nullptr),
        lookup_part_id_expr_(nullptr),
        old_part_id_expr_(nullptr),
        new_part_id_expr_(nullptr),
        related_index_ids_()
  {}
  TO_STRING_KV(K_(data_table_id),
              K_(index_table_id),
              K_(index_schema),
              K_(is_primary_index),
              K_(loc_meta),
              K_(lookup_part_id_expr),
              K_(old_part_id_expr),
              K_(new_part_id_expr),
              K_(related_index_ids));
  void reset()
  {
    data_table_id_ = common::OB_INVALID_ID;
    index_table_id_ = common::OB_INVALID_ID;
    index_schema_ = nullptr;
    is_primary_index_ = false;
    loc_meta_ = nullptr;
    lookup_part_id_expr_ = nullptr;
    old_part_id_expr_ = nullptr;
    new_part_id_expr_ = nullptr;
    related_index_ids_.reset();
  }

  uint64_t data_table_id_;
  // when is primary index, index_table_id == data_table_id
  uint64_t index_table_id_;
  const share::schema::ObTableSchema *index_schema_;
  bool is_primary_index_;
  // loc_meta is inited in init_das_context
  ObDASTableLocMeta *loc_meta_;
  // only use for global index lookup data table
  sql::ObRawExpr *lookup_part_id_expr_;
  sql::ObRawExpr *old_part_id_expr_;
  sql::ObRawExpr *new_part_id_expr_;
  // only primary table has related_index_ids, which contains local index table ids.
  TableIDArray related_index_ids_;
};

struct ObTableColumnItem
{
  ObTableColumnItem()
  : column_info_(nullptr),
    expr_(nullptr),
    raw_expr_(nullptr)
  {}

  ObTableColumnItem(const ObTableColumnInfo *column_info)
    : column_info_(column_info),
      expr_(nullptr),
      raw_expr_(nullptr)
  {}

  TO_STRING_KV(KPC_(column_info),
               KPC_(raw_expr));

  const ObTableColumnInfo *column_info_;
  sql::ObColumnRefRawExpr *expr_; // todo: need to add comment
  sql::ObRawExpr *raw_expr_; // column ref expr or calculate expr
  common::ObSEArray<sql::ObRawExpr*, 8, common::ModulePageAllocator, true> dependant_exprs_;
};

struct ObTableAssignment : public sql::ObAssignment
{
  ObTableAssignment()
      : sql::ObAssignment(),
        column_info_(nullptr),
        column_item_(nullptr),
        is_inc_or_append_(false),
        generated_expr_str_(),
        delta_expr_(nullptr),
        is_assigned_(false)
  {}
  ObTableAssignment(ObTableColumnInfo *col_info)
      : sql::ObAssignment(),
        column_info_(col_info),
        column_item_(nullptr),
        is_inc_or_append_(false),
        delta_expr_(nullptr),
        is_assigned_(false)
  {}
  TO_STRING_KV("ObAssignment", static_cast<const sql::ObAssignment &>(*this),
               KPC_(column_info),
               K_(is_inc_or_append),
               KPC_(delta_expr),
               K_(assign_value),
               K_(is_assigned));
  const ObTableColumnInfo *column_info_;
  // only use for plan cache mismatch in CG stage
  ObTableColumnItem *column_item_;
  bool is_inc_or_append_; // for append/increment
  ObString generated_expr_str_; // for append/increment
  sql::ObColumnRefRawExpr *delta_expr_; // for append/increment
  common::ObObj assign_value_;
  // did user assign specific value or not,
  // e.g. virtual generated column will be added into assignment internally
  //     when its dependent column is assigned by user but its is_assigned_ will false
  bool is_assigned_;
};

enum ObTableExecutorType
{
  TABLE_API_EXEC_INVALID = 0,
  TABLE_API_EXEC_SCAN = 1,
  TABLE_API_EXEC_INSERT = 2,
  TABLE_API_EXEC_DELETE = 3,
  TABLE_API_EXEC_UPDATE = 4,
  TABLE_API_EXEC_INSERT_UP = 5,
  TABLE_API_EXEC_REPLACE = 6,
  TABLE_API_EXEC_LOCK = 7,
  TABLE_API_EXEC_TTL = 8,
  // append new executor type here
  TABLE_API_EXEC_MAX
};

enum ObTableIncAppendStage
{
  TABLE_INCR_APPEND_INVALID = 0,
  TABLE_INCR_APPEND_UPDATE = 1,
  TABLE_INCR_APPEND_INSERT = 2
};

// 1.用于存放整个process过程中需要的通用上下文信息
// 2.在try_process()中进行初始化
class ObTableCtx
{
public:
  explicit ObTableCtx(common::ObIAllocator &allocator)
      : allocator_(allocator),
        ctx_allocator_("ObTableCtx", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        expr_info_(nullptr),
        phy_plan_ctx_(allocator_),
        sql_ctx_(),
        exec_ctx_(allocator_),
        expr_factory_(allocator_),
        all_exprs_(false),
        agg_cell_proj_(allocator_),
        is_count_all_(false),
        has_auto_inc_(false),
        has_global_index_(false),
        has_local_index_(false),
        is_global_index_scan_(false),
        need_dist_das_(false),
        is_redis_ttl_table_(false),
        redis_ttl_ctx_(nullptr)
  {
    // common
    is_init_ = false;
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    database_id_ = common::OB_INVALID_ID;
    ref_table_id_ = common::OB_INVALID_ID;
    index_table_id_ = common::OB_INVALID_ID;
    tablet_id_ = ObTabletID::INVALID_TABLET_ID;
    index_tablet_id_ = ObTabletID::INVALID_TABLET_ID;
    ls_id_ = share::ObLSID::INVALID_LS_ID;
    timeout_ts_ = 0;
    sess_guard_ = nullptr;
    schema_guard_ = nullptr;
    table_schema_ = nullptr;
    simple_table_schema_ = nullptr;
    schema_cache_guard_ = nullptr;
    flags_.value_ = 0;
    // scan
    is_scan_ = false;
    is_index_scan_ = false;
    is_index_back_ = false;
    is_weak_read_ = false;
    is_get_ = false;
    read_latest_ = true;
    index_schema_ = nullptr;
    limit_ = -1;
    offset_ = 0;
    tenant_schema_version_ = -1;
    is_for_update_ = false;
    operation_type_ = ObTableOperationType::Type::INVALID;
    is_for_insertup_ = false;
    entity_type_ = ObTableEntityType::ET_DYNAMIC;
    entity_ = nullptr;
    ops_ = nullptr;
    batch_tablet_ids_ = nullptr;
    inc_append_stage_ = ObTableIncAppendStage::TABLE_INCR_APPEND_INVALID;
    return_affected_entity_ = false;
    return_rowkey_ = false;
    cur_cluster_version_ = GET_MIN_CLUSTER_VERSION();
    is_ttl_table_ = false;
    is_skip_scan_ = false;
    is_client_set_put_ = false;
    has_generated_column_ = false;
    has_lob_column_ = false;
    is_tablegroup_req_ = false;
    binlog_row_image_type_ = ObBinlogRowImage::FULL;
    is_full_table_scan_ = false;
    column_items_.set_attr(ObMemAttr(MTL_ID(), "KvColItm"));
    assigns_.set_attr(ObMemAttr(MTL_ID(), "KvAssigns"));
    select_exprs_.set_attr(ObMemAttr(MTL_ID(), "KvSelExprs"));
    rowkey_exprs_.set_attr(ObMemAttr(MTL_ID(), "KvRowExprs"));
    index_exprs_.set_attr(ObMemAttr(MTL_ID(), "KvIdxExprs"));
    filter_exprs_.set_attr(ObMemAttr(MTL_ID(), "KvFilExprs"));
    pushdown_aggr_exprs_.set_attr(ObMemAttr(MTL_ID(), "KvAggrExprs"));
    select_col_ids_.set_attr(ObMemAttr(MTL_ID(), "KvSelColIds"));
    query_col_ids_.set_attr(ObMemAttr(MTL_ID(), "KvQryColIds"));
    query_col_names_.set_attr(ObMemAttr(MTL_ID(), "KvQryColNams"));
    index_col_ids_.set_attr(ObMemAttr(MTL_ID(), "KvIdxColIds"));
    table_index_info_.set_attr(ObMemAttr(MTL_ID(), "KvIdxInfos"));
    key_ranges_.set_attr(ObMemAttr(MTL_ID(), "KvRanges"));
    credential_ = nullptr;
    audit_ctx_ = nullptr;
    has_fts_index_ = false;
    fts_ctx_ = nullptr;
  }

  void reset()
  {
    // common
    is_init_ = false;
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    database_id_ = common::OB_INVALID_ID;
    ref_table_id_ = common::OB_INVALID_ID;
    index_table_id_ = common::OB_INVALID_ID;
    tablet_id_ = ObTabletID::INVALID_TABLET_ID;
    index_tablet_id_ = ObTabletID::INVALID_TABLET_ID;
    ls_id_ = share::ObLSID::INVALID_LS_ID;
    timeout_ts_ = 0;
    sess_guard_ = nullptr;
    schema_guard_ = nullptr;
    table_schema_ = nullptr;
    simple_table_schema_ = nullptr;
    schema_cache_guard_ = nullptr;
    flags_.value_ = 0;
    has_auto_inc_ = false;
    has_global_index_ = false;
    has_local_index_ = false;
    is_global_index_scan_ = false;
    credential_ = nullptr;
    audit_ctx_ = nullptr;
    // scan
    is_scan_ = false;
    is_index_scan_ = false;
    is_index_back_ = false;
    is_weak_read_ = false;
    is_get_ = false;
    read_latest_ = true;
    index_schema_ = nullptr;
    limit_ = -1;
    offset_ = 0;
    tenant_schema_version_ = -1;
    is_for_update_ = false;
    operation_type_ = ObTableOperationType::Type::INVALID;
    is_for_insertup_ = false;
    entity_type_ = ObTableEntityType::ET_DYNAMIC;
    entity_ = nullptr;
    ops_ = nullptr;
    batch_tablet_ids_ = nullptr;
    inc_append_stage_ = ObTableIncAppendStage::TABLE_INCR_APPEND_INVALID;
    return_affected_entity_ = false;
    return_rowkey_ = false;
    cur_cluster_version_ = GET_MIN_CLUSTER_VERSION();
    is_ttl_table_ = false;
    is_skip_scan_ = false;
    is_client_set_put_ = false;
    has_generated_column_ = false;
    has_lob_column_ = false;
    is_tablegroup_req_ = false;
    binlog_row_image_type_ = ObBinlogRowImage::FULL;
    is_full_table_scan_ = false;
    // others
    agg_cell_proj_.reset();
    is_count_all_ = false;
    all_exprs_.reuse();
    exec_ctx_.get_das_ctx().clear_all_location_info();
    expr_info_ = nullptr;
    column_items_.reset();
    assigns_.reset();
    select_exprs_.reset();
    rowkey_exprs_.reset();
    index_exprs_.reset();
    filter_exprs_.reset();
    pushdown_aggr_exprs_.reset();
    query_col_ids_.reset();
    query_col_names_.reset();
    index_col_ids_.reset();
    table_index_info_.reset();
    key_ranges_.reset();
    phy_plan_ctx_.get_autoinc_params().reset();
    need_dist_das_ = false;
    is_redis_ttl_table_ = false;
    redis_ttl_ctx_ = nullptr;
    has_fts_index_ = false;
    if (OB_NOT_NULL(fts_ctx_)) {
      fts_ctx_->reset();
    }
  }

  virtual ~ObTableCtx()
  {}
  TO_STRING_KV(K_(is_init),
               K_(tenant_id),
               K_(database_id),
               K_(table_name),
               K_(ref_table_id),
               K_(index_table_id),
               K_(tablet_id),
               K_(index_tablet_id),
               K_(ls_id),
               K_(tenant_schema_version),
               K_(column_items),
               K_(assigns),
               K_(has_global_index),
               K_(is_global_index_scan),
               K(flags_.value_),
               // scan to string
               K_(is_scan),
               K_(is_index_scan),
               K_(is_index_back),
               K_(is_weak_read),
               K_(is_get),
               K_(read_latest),
               K_(limit),
               K_(offset),
               K_(query_col_names),
               K_(select_col_ids),
               // update to string
               K_(is_for_update),
               // insert up to string
               K_(is_for_insertup),
               K_(entity_type),
               K_(cur_cluster_version),
               K_(is_ttl_table),
               K_(is_skip_scan),
               K_(is_client_set_put),
               K_(binlog_row_image_type),
               K_(need_dist_das),
               KPC_(credential),
               K_(is_multi_tablet_get),
               K_(has_fts_index),
               K_(fts_ctx));
public:
  //////////////////////////////////////// getter ////////////////////////////////////////////////
  // for common
  OB_INLINE common::ObIAllocator& get_allocator() { return allocator_; }
  OB_INLINE common::ObIAllocator& get_allocator() const { return allocator_; }
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE common::ObTableID &get_table_id() { return index_table_id_; }
  OB_INLINE common::ObTableID get_ref_table_id() const { return ref_table_id_; }
  OB_INLINE common::ObTableID get_index_table_id() const { return index_table_id_; }
  OB_INLINE common::ObTabletID get_tablet_id() const { return tablet_id_; }
  OB_INLINE common::ObTabletID get_index_tablet_id() const { return index_tablet_id_; }
  OB_INLINE common::ObString &get_table_name() { return table_name_; }
  OB_INLINE const share::ObLSID& get_ls_id() const { return ls_id_; }
  OB_INLINE int64_t get_timeout_ts() const { return timeout_ts_; }
  // may be null, need judge table_schema_ is not null when use!
  OB_INLINE const share::schema::ObTableSchema* get_table_schema() const { return table_schema_; }
  OB_INLINE const share::schema::ObSimpleTableSchemaV2* get_simple_table_schema() const { return simple_table_schema_; }
  OB_INLINE share::schema::ObSchemaGetterGuard* get_schema_guard() const { return schema_guard_; }
  OB_INLINE ObKvSchemaCacheGuard* get_schema_cache_guard() const { return schema_cache_guard_; }
  OB_INLINE sql::ObExprFrameInfo* get_expr_frame_info() { return expr_info_; }
  OB_INLINE sql::ObExecContext& get_exec_ctx() { return exec_ctx_; }
  OB_INLINE sql::ObRawExprFactory& get_expr_factory() { return expr_factory_; }
  OB_INLINE sql::ObRawExprUniqueSet& get_all_exprs() { return all_exprs_; }
  OB_INLINE ObIArray<sql::ObRawExpr *>& get_all_exprs_array() {
    return const_cast<ObIArray<ObRawExpr *> &>(all_exprs_.get_expr_array());
  }
  OB_INLINE ObTableApiSessGuard* get_sess_guard() const { return sess_guard_; }
  OB_INLINE sql::ObSQLSessionInfo& get_session_info()
  {
    return sess_guard_->get_sess_info();
  }
  OB_INLINE const sql::ObSQLSessionInfo& get_session_info() const
  {
    return sess_guard_->get_sess_info();
  }
  OB_INLINE const common::ObString get_tenant_name() const
  {
    return sess_guard_->get_tenant_name();
  }
  OB_INLINE const common::ObString get_user_name() const
  {
    return sess_guard_->get_user_name();
  }
  OB_INLINE const common::ObString get_database_name() const
  {
    return sess_guard_->get_database_name();
  }
  OB_INLINE int64_t get_tenant_schema_version() const { return tenant_schema_version_; }
  OB_INLINE ObTableOperationType::Type get_opertion_type() const { return operation_type_; }
  OB_INLINE bool is_init() const { return is_init_; }
  OB_INLINE const ObIArray<ObTableColumnItem>& get_column_items() const { return column_items_; }
  OB_INLINE ObIArray<ObTableColumnItem>& get_column_items() { return column_items_; }
  OB_INLINE const ObIArray<ObTableColumnInfo *>& get_column_info_array() const { return schema_cache_guard_->get_column_info_array(); }
  OB_INLINE const ObIArray<ObTableAssignment>& get_assignments() const { return assigns_; }
  OB_INLINE ObIArray<ObTableAssignment>& get_assignments() { return assigns_; }
  OB_INLINE ObIArray<ObTableIndexInfo>& get_table_index_info() { return table_index_info_; }
  OB_INLINE const ObIArray<ObTableIndexInfo>& get_table_index_info() const { return table_index_info_; }
  OB_INLINE const ObTableApiCredential* get_credential() const { return credential_; }
  OB_INLINE ObTableAuditCtx* get_audit_ctx() { return audit_ctx_; }

  // for scan
  OB_INLINE bool is_scan() const { return is_scan_; }
  OB_INLINE bool is_index_scan() const { return is_index_scan_; }
  OB_INLINE bool is_weak_read() const { return is_weak_read_; }
  OB_INLINE bool is_index_back() const { return is_index_back_; }
  OB_INLINE bool is_get() const { return is_get_; }
  OB_INLINE bool is_read_latest() const { return read_latest_; }
  OB_INLINE common::ObQueryFlag::ScanOrder get_scan_order() const { return scan_order_; }
  OB_INLINE ObIArray<sql::ObRawExpr *>& get_filter_exprs() { return filter_exprs_; }
  OB_INLINE const ObIArray<sql::ObRawExpr *>& get_filter_exprs() const { return filter_exprs_; }
  OB_INLINE ObIArray<sql::ObAggFunRawExpr *>& get_pushdown_aggr_exprs() { return pushdown_aggr_exprs_; }
  OB_INLINE const ObIArray<sql::ObAggFunRawExpr *>& get_pushdown_aggr_exprs() const { return pushdown_aggr_exprs_; }
  OB_INLINE const ObIArray<sql::ObColumnRefRawExpr *>& get_select_exprs() const { return select_exprs_; }
  OB_INLINE const ObIArray<sql::ObRawExpr *>& get_rowkey_exprs() const { return rowkey_exprs_; }
  OB_INLINE const ObIArray<sql::ObRawExpr *>& get_index_exprs() const { return index_exprs_; }
  OB_INLINE const share::schema::ObTableSchema* get_index_schema() const { return index_schema_; }
  OB_INLINE int64_t get_limit() const { return limit_; }
  OB_INLINE int64_t get_offset() const { return offset_; }
  OB_INLINE const common::ObIArray<common::ObNewRange>& get_key_ranges() const { return key_ranges_; }
  OB_INLINE common::ObIArray<common::ObNewRange>& get_key_ranges() { return key_ranges_; }
  OB_INLINE const common::ObIArray<uint64_t>& get_select_col_ids() const { return select_col_ids_; }
  OB_INLINE const common::ObIArray<uint64_t>& get_query_col_ids() const { return query_col_ids_; }
  OB_INLINE const common::ObIArray<common::ObString>& get_query_col_names() const { return query_col_names_; }
  OB_INLINE bool is_total_quantity_log() const { return binlog_row_image_type_ == ObBinlogRowImage::FULL; }
  OB_INLINE bool is_full_table_scan() const { return is_full_table_scan_; }
  // for update
  OB_INLINE bool is_for_update() const { return is_for_update_; }
  OB_INLINE bool is_inc_or_append() const
  {
    return ObTableOperationType::Type::APPEND == operation_type_
      || ObTableOperationType::Type::INCREMENT == operation_type_;
  }
  OB_INLINE bool is_inc() const
  {
    return ObTableOperationType::Type::INCREMENT == operation_type_;
  }
  OB_INLINE bool is_append() const
  {
    return ObTableOperationType::Type::APPEND == operation_type_;
  }
  OB_INLINE bool is_dml() const
  {
    return ObTableOperationType::Type::GET != operation_type_ && !is_scan_;
  }
  OB_INLINE bool need_full_rowkey_op() const
  {
    return ObTableOperationType::Type::DEL == operation_type_
      || ObTableOperationType::Type::UPDATE == operation_type_
      || ObTableOperationType::Type::GET == operation_type_;
  }
  // for dml
  OB_INLINE ObTableIndexInfo& get_primary_index_info() { return table_index_info_.at(0); }
  OB_INLINE const ObTableIndexInfo& get_primary_index_info() const { return table_index_info_.at(0); }
  OB_INLINE const ObIArray<common::ObTableID>& get_related_index_ids() const { return get_primary_index_info().related_index_ids_; }
  OB_INLINE bool is_for_insertup() const { return is_for_insertup_; }
  OB_INLINE const ObITableEntity* get_entity() const { return entity_; }
  OB_INLINE ObTableEntityType get_entity_type() const { return entity_type_; }
  OB_INLINE bool is_htable() const { return ObTableEntityType::ET_HKV == entity_type_; }
  OB_INLINE bool is_insert() const
  {
    return ObTableOperationType::Type::INSERT == operation_type_;
  }
  // for htable
  OB_INLINE const common::ObIArray<table::ObTableOperation>* get_batch_operation() const { return ops_; }
  OB_INLINE bool is_tablegroup_req() const { return is_tablegroup_req_; }
  OB_INLINE void set_is_tablegroup_req(const bool is_tablegroup_req) { is_tablegroup_req_ = is_tablegroup_req; }
  // for increment/append
  OB_INLINE bool return_affected_entity() const { return return_affected_entity_;}
  OB_INLINE bool return_rowkey() const { return return_rowkey_;}
  OB_INLINE uint64_t get_cur_cluster_version() const { return cur_cluster_version_;}
  OB_INLINE bool has_generated_column() const { return has_generated_column_; }
  // for aggregate
  OB_INLINE const common::ObIArray<uint64_t> &get_agg_projs() const { return agg_cell_proj_; }
  OB_INLINE bool is_count_all() const { return is_count_all_; }
  OB_INLINE ObPhysicalPlanCtx *get_physical_plan_ctx() { return exec_ctx_.get_physical_plan_ctx(); }
  OB_INLINE bool has_auto_inc() { return has_auto_inc_; }
  // for global index
  OB_INLINE bool has_global_index() { return has_global_index_; }
  OB_INLINE bool is_global_index_scan() const { return is_global_index_scan_; }
  OB_INLINE bool is_global_index_back() const { return is_global_index_scan_ && is_index_back_;}
  OB_INLINE bool need_dist_das() const { return need_dist_das_ || has_global_index_ || (is_global_index_scan_ && is_index_back_); }
  OB_INLINE bool is_redis_ttl_table() const { return is_redis_ttl_table_; }
  OB_INLINE ObRedisTTLCtx *redis_ttl_ctx() const { return redis_ttl_ctx_; }
  // for local index
  OB_INLINE bool has_local_index() { return has_local_index_; }
  OB_INLINE bool has_secondary_index() { return has_local_index_ || has_global_index_; }
  // for put
  OB_INLINE bool is_client_use_put() const { return is_client_set_put_; }
  // lob column
  OB_INLINE bool has_lob_column() const { return has_lob_column_; }
  OB_INLINE ObTableIncAppendStage get_inc_append_stage() const { return inc_append_stage_; }
  OB_INLINE bool is_inc_append_update() const { return is_inc_or_append() && inc_append_stage_ == ObTableIncAppendStage::TABLE_INCR_APPEND_UPDATE; }
  OB_INLINE bool is_inc_append_insert() const { return is_inc_or_append() && inc_append_stage_ == ObTableIncAppendStage::TABLE_INCR_APPEND_INSERT; }
  OB_INLINE const common::ObIArray<common::ObTabletID>* get_batch_tablet_ids() const { return batch_tablet_ids_; }
  OB_INLINE bool is_multi_tablet_get() const
  {
    return OB_NOT_NULL(batch_tablet_ids_) && batch_tablet_ids_->count() > 1;
  }
  OB_INLINE bool is_local_index_scan() const { return is_index_scan_ && !is_global_index_scan_; }
  OB_INLINE bool need_related_table_id() const { return (is_dml() && has_local_index_) || is_tsc_with_doc_id() || is_local_index_scan(); }
  OB_INLINE bool has_fts_index() const { return has_fts_index_; }
  OB_INLINE ObTableFtsCtx* get_fts_ctx() { return fts_ctx_; }
  OB_INLINE const ObTableFtsCtx* get_fts_ctx() const { return fts_ctx_; }
  OB_INLINE bool is_text_retrieval_scan() const{ return fts_ctx_ != nullptr && fts_ctx_->is_text_retrieval_scan(); }
  OB_INLINE bool is_tsc_with_doc_id() const { return fts_ctx_ != nullptr && fts_ctx_->is_tsc_with_doc_id(); }
  //////////////////////////////////////// setter ////////////////////////////////////////////////
  // for common
  OB_INLINE void set_init_flag(bool is_init) { is_init_ = is_init; }
  OB_INLINE void set_expr_info(ObExprFrameInfo *expr_info) { expr_info_ = expr_info; }
  OB_INLINE void set_credential(table::ObTableApiCredential *credential) { credential_ = credential; }
  OB_INLINE void set_sess_guard(ObTableApiSessGuard *sess_guard) { sess_guard_ = sess_guard; }
  OB_INLINE void set_schema_guard(ObSchemaGetterGuard *schema_guard) { schema_guard_ = schema_guard; }
  OB_INLINE void set_table_schema(const ObTableSchema *table_schema) { table_schema_ = table_schema; }
  OB_INLINE void set_simple_table_schema(const ObSimpleTableSchemaV2 *simple_table_schema) { simple_table_schema_ = simple_table_schema; }
  OB_INLINE void set_schema_cache_guard(ObKvSchemaCacheGuard *schema_cache_guard) { schema_cache_guard_ = schema_cache_guard; }
  OB_INLINE void set_ls_id(share::ObLSID &ls_id) { ls_id_ = ls_id; }
  OB_INLINE void set_audit_ctx(ObTableAuditCtx *ctx) { audit_ctx_ = ctx; }
  OB_INLINE void set_tablet_id(common::ObTabletID &tablet_id) { tablet_id_ = tablet_id; }
  OB_INLINE void set_index_tablet_id(common::ObTabletID &tablet_id) { index_tablet_id_ = tablet_id; }
  // for scan
  OB_INLINE void set_scan(const bool &is_scan) { is_scan_ = is_scan; }
  OB_INLINE void set_scan_order(const common::ObQueryFlag::ScanOrder scan_order) {  scan_order_ = scan_order; }
  OB_INLINE void set_limit(const int64_t &limit) { limit_ = limit; }
  OB_INLINE void set_offset(const int64_t &offset) { offset_ = offset; }
  OB_INLINE void set_read_latest(bool read_latest) { read_latest_ = read_latest; }
  // for dml
  OB_INLINE void set_entity(const ObITableEntity *entity) { entity_ = entity; }
  OB_INLINE void set_entity_type(const ObTableEntityType &type) { entity_type_ = type; }
  OB_INLINE void set_operation_type(const ObTableOperationType::Type op_type) { operation_type_ = op_type; }
  // for htable
  OB_INLINE void set_batch_operation(const ObIArray<table::ObTableOperation> *ops) { ops_ = ops; }
  // for multi tablets batch
  OB_INLINE void set_batch_tablet_ids(const ObIArray<common::ObTabletID> *tablet_ids) { batch_tablet_ids_ = tablet_ids; }
  // for auto inc
  OB_INLINE bool need_auto_inc_expr()
  {
    // delete/update/get/scan操作只需要生成列引用表达式
    return has_auto_inc_
        && operation_type_ != ObTableOperationType::DEL
        && operation_type_ != ObTableOperationType::UPDATE
        && operation_type_ != ObTableOperationType::GET
        && operation_type_ != ObTableOperationType::SCAN;
  }
  // for delete
  OB_INLINE void set_skip_scan(bool skip_scan) { is_skip_scan_ = skip_scan; }
  OB_INLINE bool is_skip_scan() { return is_skip_scan_; }
  // for put
  OB_INLINE void set_client_use_put(bool is_client_use_put) { is_client_set_put_ = is_client_use_put; }
  // for global index
  OB_INLINE bool need_new_calc_tablet_id_expr()
  {
    return operation_type_ == ObTableOperationType::UPDATE ||
           operation_type_ == ObTableOperationType::INSERT_OR_UPDATE ||
           (operation_type_ == ObTableOperationType::INSERT && is_ttl_table_) ||
           is_inc_append_update() || (is_inc_append_insert() && is_ttl_table_);
  }
  OB_INLINE bool need_lookup_calc_tablet_id_expr()
  {
    return operation_type_ == ObTableOperationType::INSERT_OR_UPDATE ||
           operation_type_ == ObTableOperationType::REPLACE ||
           (operation_type_ == ObTableOperationType::INSERT && is_ttl_table_) ||
           (operation_type_ == ObTableOperationType::SCAN && is_global_index_back()) ||
           is_inc_append_update() || (is_inc_append_insert() && is_ttl_table_);
  }
  OB_INLINE void set_inc_append_stage(ObTableIncAppendStage inc_append_stage)
  {
    inc_append_stage_ = inc_append_stage;
  }
  OB_INLINE void set_need_dist_das(bool need_dist_das) { need_dist_das_ = need_dist_das; }
  OB_INLINE void set_is_redis_ttl_table(bool is_redis_ttl_table) { is_redis_ttl_table_ = is_redis_ttl_table; }
  OB_INLINE void set_redis_ttl_ctx(ObRedisTTLCtx *redis_ttl_ctx) { redis_ttl_ctx_ = redis_ttl_ctx; }
  OB_INLINE bool add_redis_meta_range() {
      return is_redis_ttl_table_
             && OB_NOT_NULL(redis_ttl_ctx_)
             && OB_ISNULL(redis_ttl_ctx_->get_meta())
             && redis_ttl_ctx_->get_model() != ObRedisModel::STRING
             && operation_type_ != ObTableOperationType::DEL
             && operation_type_ != ObTableOperationType::UPDATE; }
  void set_ttl_definition(const common::ObString &ttl_definition)
  {
    ttl_definition_ = ttl_definition;
  }
  OB_INLINE const common::ObString &get_ttl_definition() const
  {
    return ttl_definition_;
  }
public:
  // 基于 table name 初始化common部分(不包括expr_info_, exec_ctx_)
  int init_common(ObTableApiCredential &credential,
                  const common::ObTabletID &arg_tablet_id,
                  const int64_t &timeout_ts);
  // 初始化 insert 相关
  int init_insert();
  // init put
  int init_put(bool allow_insup = false);
  // 初始化scan相关(不包括表达分类)
  int init_scan(const ObTableQuery &query,
                const bool &is_wead_read,
                const uint64_t arg_table_id);
  // 初始化update相关
  int init_update();
  // 初始化delete相关
  int init_delete();
  // 初始化replace相关
  int init_replace();
  // 初始化insert_up相关
  int init_insert_up(bool is_client_set_put);
  // 初始化get相关
  int init_get();
  // 初始化increment相关
  int init_increment(bool return_affected_entity, bool return_rowkey);
  // 初始化append相关
  int init_append(bool return_affected_entity, bool return_rowkey);
  // 分类扫描相关表达式
  int classify_scan_exprs();
  // 初始化exec_ctx_和exec_ctx_.das_ctx_
  int init_exec_ctx();
  // init exec_ctx_.my_session_.tx_desc_
  int init_trans(transaction::ObTxDesc *trans_desc,
                 const transaction::ObTxReadSnapshot &tx_snapshot);
  int init_das_context(ObDASCtx &das_ctx);
  int init_related_tablet_map(ObDASCtx &das_ctx);
  void init_physical_plan_ctx(int64_t timeout_ts, int64_t tenant_schema_version);
  // 更新全局自增值
  int update_auto_inc_value();
  // init table context for ttl operation
  bool is_ttl_table() const { return is_ttl_table_; }

  void set_is_ttl_table(bool is_ttl_table) { is_ttl_table_ = is_ttl_table; }
  int init_ttl_delete(const ObIArray<ObNewRange> &scan_range);
  int get_column_item_by_column_id(uint64_t column_id, const ObTableColumnItem *&item) const;
  int get_column_item_by_expr(sql::ObRawExpr *raw_expr, const ObTableColumnItem *&item) const;
  int get_column_item_by_expr(sql::ObColumnRefRawExpr *expr, const ObTableColumnItem *&item) const;
  int get_expr_from_column_items(const common::ObString &col_name, sql::ObRawExpr *&expr) const;
  int get_expr_from_column_items(const common::ObString &col_name, sql::ObColumnRefRawExpr *&expr) const;
  int get_expr_from_assignments(const common::ObString &col_name, sql::ObRawExpr *&expr) const;
  int check_insert_up_can_use_put(bool &use_put);
  int get_assignment_by_column_id(uint64_t column_id, const ObTableAssignment *&assign) const;
  int cons_column_items_for_cg();
  // only for genarate spec or exprs, to generate full table_schema
  int generate_table_schema_for_cg();
  // for common
  int get_tablet_by_rowkey(const common::ObRowkey &rowkey,
                           common::ObTabletID &tablet_id);
  int init_insert_when_inc_append();
  // only use for fts scan cg stage
  int prepare_text_retrieval_scan();
public:
  // convert lob的allocator需要保证obj写入表达式后才能析构
  static int convert_lob(common::ObIAllocator &allocator, ObObj &obj);
  // read lob的allocator需要保证obj序列化到rpc buffer后才能析构
  static int read_real_lob(common::ObIAllocator &allocator, ObObj &obj);
  int adjust_entity();
public:
  // for column store replica query
  int check_is_cs_replica_query(bool &is_cs_replica_query) const;


  static int check_insert_up_can_use_put(ObKvSchemaCacheGuard &schema_cache_guard,
                                         const ObITableEntity *entity,
                                         bool is_client_set_put,
                                         bool is_htable,
                                         bool is_full_binlog_image,
                                         bool &use_put);
private:
  // for scan
  int generate_column_infos(common::ObIArray<const ObTableColumnInfo*> &columns_infos);
  int init_index_info(const common::ObString &index_name, const uint64_t arg_table_id);
  int generate_key_range(const ObIArray<ObString> &scan_ranges_columns, const common::ObIArray<common::ObNewRange> &scan_ranges);
  int init_scan_index_info();
  int init_primary_index_info();
  // for dml
  int init_dml_related_tid(ObIArray<common::ObTableID> &related_tids);
  int init_dml_index_info();
  int check_if_can_skip_update_index(const share::schema::ObTableSchema *index_schema, bool &is_exist);
  // for update
  int init_assignments(const ObTableEntity &entity);
  int add_generated_column_assignment();
  // Init size of aggregation project array.
  //
  // @param [in]  size      The agg size
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int init_agg_cell_proj(int64_t size);
  // Add schema cell idx to aggregation project array.
  //
  // @param [in]  cell_idx      The schema cell idx.
  // @param [in]  column_name   The schema cell column name.
  // @return Returns OB_SUCCESS on success, error code otherwise.
  int add_aggregate_proj(int64_t cell_idx, const common::ObString &column_name, const ObIArray<ObTableAggregation> &aggregations);

  int add_auto_inc_param();

private:
  int init_schema_info_from_cache();
  int adjust_column_type(const ObTableColumnInfo &column_info, ObObj &ob);
  int adjust_rowkey();
  int adjust_properties();
  // 获取索引表的tablet_id
  int get_related_tablet_id(const share::schema::ObTableSchema &index_schema,
                            common::ObTabletID &related_tablet_id);


  // 初始化 table schema 之后的 common 部分
  int inner_init_common(const common::ObTabletID &arg_tablet_id,
                        const common::ObString &table_name,
                        const int64_t &timeout_ts);
  // for fulltext index
  int init_fts_schema();
  int generate_fts_search_range(const ObTableQuery &query);
private:
  bool is_init_;
  common::ObIAllocator &allocator_; // processor allocator
  common::ObArenaAllocator ctx_allocator_;
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString table_name_;
  common::ObTableID ref_table_id_;
  common::ObTableID index_table_id_;
  common::ObTabletID tablet_id_;
  common::ObTabletID index_tablet_id_;
  share::ObLSID ls_id_;
  int64_t timeout_ts_;
  ObTableApiSessGuard *sess_guard_;
  const share::schema::ObTableSchema *table_schema_;
  const share::schema::ObSimpleTableSchemaV2 *simple_table_schema_;
  share::schema::ObSchemaGetterGuard *schema_guard_;
  ObKvSchemaCacheGuard *schema_cache_guard_;
  int64_t tenant_schema_version_;
  sql::ObExprFrameInfo *expr_info_;
  sql::ObPhysicalPlanCtx phy_plan_ctx_;
  sql::ObSqlCtx sql_ctx_;
  sql::ObExecContext exec_ctx_;
  sql::ObRawExprFactory expr_factory_;
  sql::ObRawExprUniqueSet all_exprs_;
  // column items only construct in CG stage when plan cache is mismatch
  common::ObSEArray<ObTableColumnItem, 32> column_items_;
  common::ObSEArray<ObTableAssignment, 16> assigns_;
  // for scan
  bool is_scan_;
  bool is_index_scan_;
  bool is_index_back_;
  bool is_weak_read_;
  bool is_get_;
  bool read_latest_; // default true, false in single get and multi get
  common::ObQueryFlag::ScanOrder scan_order_;
  common::ObSEArray<sql::ObColumnRefRawExpr*, 32> select_exprs_;
  common::ObSEArray<sql::ObRawExpr*, 16> rowkey_exprs_;
  common::ObSEArray<sql::ObRawExpr*, 16> index_exprs_;
  common::ObSEArray<sql::ObRawExpr*, 8> filter_exprs_;
  common::ObSEArray<sql::ObAggFunRawExpr*, 8> pushdown_aggr_exprs_;
  common::ObSEArray<uint64_t, 32> select_col_ids_; // 基于schema序的select column id
  common::ObSEArray<uint64_t, 32> query_col_ids_; // 用户查询的select column id
  common::ObSEArray<common::ObString, 32> query_col_names_; // 用户查询的select column name，引用的是schema上的列名
  common::ObSEArray<uint64_t, 16> index_col_ids_;
  common::ObSEArray<ObTableIndexInfo, 4> table_index_info_; // 用于记录主表和全局索引表信息
  const share::schema::ObTableSchema *index_schema_;
  int64_t offset_;
  int64_t limit_;
  common::ObSEArray<common::ObNewRange, 16> key_ranges_;
  // for update
  bool is_for_update_;
  ObTableOperationType::Type operation_type_;
  // agg cell index in schema
  common::ObFixedArray<uint64_t, common::ObIAllocator> agg_cell_proj_;
  bool is_count_all_;
  // for auto inc
  bool has_auto_inc_;
  // for increment/append
  ObTableIncAppendStage inc_append_stage_;
  bool return_affected_entity_;
  bool return_rowkey_;
  // for multi tablets batch
  const ObIArray<common::ObTabletID> *batch_tablet_ids_;
  // for dml
  bool is_for_insertup_;
  ObTableEntityType entity_type_;
  const ObITableEntity *entity_;
  // for htable
  const ObIArray<table::ObTableOperation> *ops_;
  // for lob adapt
  uint64_t cur_cluster_version_;
  // for ttl table
  bool is_ttl_table_;
  ObString ttl_definition_;
  // for delete skip scan
  bool is_skip_scan_;
  // for put
  bool is_client_set_put_;
  int64_t binlog_row_image_type_;
  // for audit
  bool is_full_table_scan_;
  // for global index
  bool has_global_index_;
  bool has_local_index_;
  bool is_global_index_scan_;
  // from schema_cache
  ObTableSchemaFlags flags_;
  bool has_generated_column_;
  bool is_tablegroup_req_; // is table name a tablegroup name
  bool has_lob_column_;
  bool need_dist_das_; // used for init das_ref
  ObTableApiCredential *credential_;
  ObTableAuditCtx *audit_ctx_;
  bool is_redis_ttl_table_; // redis ttl table and other ttl table behave differently
  ObRedisTTLCtx *redis_ttl_ctx_;
  bool is_multi_tablet_get_;
  // for fulltext index
  bool has_fts_index_;
  ObTableFtsCtx *fts_ctx_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableCtx);
};

struct ObTableDmlBaseCtDef
{
public:
  virtual ~ObTableDmlBaseCtDef() = default;
  VIRTUAL_TO_STRING_KV(K_(column_ids),
                       K_(old_row),
                       K_(new_row),
                       KPC_(old_part_id_expr),
                       KPC_(new_part_id_expr));

  UIntFixedArray column_ids_;
  ExprFixedArray old_row_;
  ExprFixedArray new_row_;
  // for global index
  sql::ObExpr *old_part_id_expr_;
  sql::ObExpr *new_part_id_expr_;
protected:
  ObTableDmlBaseCtDef(common::ObIAllocator &alloc)
      : column_ids_(alloc),
        old_row_(alloc),
        new_row_(alloc),
        old_part_id_expr_(nullptr),
        new_part_id_expr_(nullptr)
  {
  }
};

struct ObTableApiScanCtDef
{
public:
  ObTableApiScanCtDef(common::ObIAllocator &allocator)
      : scan_ctdef_(allocator),
        lookup_ctdef_(nullptr),
        lookup_loc_meta_(nullptr),
        output_exprs_(allocator),
        filter_exprs_(allocator),
        calc_part_id_expr_(nullptr),
        global_index_rowkey_exprs_(allocator),
        attach_spec_(allocator, &scan_ctdef_),
        search_text_(nullptr),
        topn_limit_expr_(nullptr),
        topn_offset_expr_(nullptr),
        allocator_(allocator)
  {
  }
  TO_STRING_KV(K_(scan_ctdef),
               KPC_(lookup_ctdef),
               KPC_(lookup_loc_meta),
               K_(attach_spec));
public:
  // find which is the lookup ctdef in ctdef tree
  const ObDASScanCtDef *get_lookup_ctdef() const;
public:
  sql::ObDASScanCtDef scan_ctdef_;
  sql::ObDASScanCtDef *lookup_ctdef_;
  sql::ObDASTableLocMeta *lookup_loc_meta_;
  ExprFixedArray output_exprs_;
  ExprFixedArray filter_exprs_;
  // Begin for Global Index Lookup
  ObExpr *calc_part_id_expr_;
  ExprFixedArray global_index_rowkey_exprs_;
  // end for Global Index Lookup
  // begin for fts query
  ObDASAttachSpec attach_spec_;
  ObExpr *search_text_;
  // for topK search, disable
  ObExpr *topn_limit_expr_;
  ObExpr *topn_offset_expr_;
  // end for fts query
  common::ObIAllocator &allocator_;
};

struct ObTableApiScanRtDef
{
  ObTableApiScanRtDef(common::ObIAllocator &allocator)
      : scan_rtdef_(),
        lookup_rtdef_(nullptr)
  {
  }
  TO_STRING_KV(K_(scan_rtdef),
               KPC_(lookup_rtdef));
  sql::ObDASScanRtDef scan_rtdef_;
  sql::ObDASScanRtDef *lookup_rtdef_;
  sql::ObDASAttachRtInfo *attach_rtinfo_;
};

struct ObTableDmlBaseRtDef
{
  virtual ~ObTableDmlBaseRtDef() = default;
  VIRTUAL_TO_STRING_KV(K_(cur_row_num));
  int64_t cur_row_num_;
protected:
  ObTableDmlBaseRtDef()
      : cur_row_num_(0)
  {
  }
};

struct ObTableInsCtDef : ObTableDmlBaseCtDef
{
public:
  ObTableInsCtDef(common::ObIAllocator &alloc)
      : ObTableDmlBaseCtDef(alloc),
        das_ctdef_(alloc),
        related_ctdefs_(alloc),
        column_infos_(alloc),
        alloc_(alloc)
  {
  }
  TO_STRING_KV(K_(das_ctdef),
               K_(related_ctdefs));
  ObDASInsCtDef das_ctdef_;
  sql::DASInsCtDefArray related_ctdefs_;
  ColContentFixedArray column_infos_;
  common::ObIAllocator &alloc_;
};

struct ObTableInsRtDef : ObTableDmlBaseRtDef
{
  ObTableInsRtDef()
      : ObTableDmlBaseRtDef(),
        das_rtdef_(),
        related_rtdefs_()
  {
  }
  TO_STRING_KV(K_(das_rtdef),
               K_(related_rtdefs));
  ObDASInsRtDef das_rtdef_;
  sql::DASInsRtDefArray related_rtdefs_;
};

struct ObTableUpdCtDef : ObTableDmlBaseCtDef
{
public:
  ObTableUpdCtDef(common::ObIAllocator &alloc)
      : ObTableDmlBaseCtDef(alloc),
        full_row_(alloc),
        delta_row_(alloc),
        das_ctdef_(alloc),
        assign_columns_(alloc),
        related_ctdefs_(alloc),
        ddel_ctdef_(nullptr),
        dins_ctdef_(nullptr),
        related_del_ctdefs_(alloc),
        related_ins_ctdefs_(alloc),
        alloc_(alloc)
  {
  }
  TO_STRING_KV(K_(full_row),
               K_(delta_row),
               K_(das_ctdef),
               K_(assign_columns),
               K_(related_ctdefs));
  ExprFixedArray full_row_;
  ExprFixedArray delta_row_;
  ObDASUpdCtDef das_ctdef_;
  ColContentFixedArray assign_columns_;
  DASUpdCtDefArray related_ctdefs_;
  // for insert up begin
  ObDASDelCtDef *ddel_ctdef_;
  ObDASInsCtDef *dins_ctdef_;
  DASDelCtDefArray related_del_ctdefs_;
  DASInsCtDefArray related_ins_ctdefs_;
  // for insert up end
  common::ObIAllocator &alloc_;
};

struct ObTableUpdRtDef : ObTableDmlBaseRtDef
{
public:
  ObTableUpdRtDef()
      : ObTableDmlBaseRtDef(),
        das_rtdef_(),
        related_rtdefs_(),
        ddel_rtdef_(nullptr),
        dins_rtdef_(nullptr),
        related_del_rtdefs_(),
        related_ins_rtdefs_(),
        found_rows_(0)
  {
  }
  TO_STRING_KV(K_(das_rtdef),
               K_(related_rtdefs));
  ObDASUpdRtDef das_rtdef_;
  DASUpdRtDefArray related_rtdefs_;
  // for insert up begin
  ObDASDelRtDef *ddel_rtdef_;
  ObDASInsRtDef *dins_rtdef_;
  DASDelRtDefArray related_del_rtdefs_;
  DASInsRtDefArray related_ins_rtdefs_;
  int64_t found_rows_;
  // for insert up end
};

struct ObTableDelCtDef : ObTableDmlBaseCtDef
{
public:
  ObTableDelCtDef(common::ObIAllocator &alloc)
      : ObTableDmlBaseCtDef(alloc),
        das_ctdef_(alloc),
        related_ctdefs_(alloc),
        alloc_(alloc)
  {
  }
  TO_STRING_KV(K_(das_ctdef),
               K_(related_ctdefs));
  ObDASDelCtDef das_ctdef_;
  DASDelCtDefArray related_ctdefs_;
  common::ObIAllocator &alloc_;
};

struct ObTableDelRtDef : ObTableDmlBaseRtDef
{
public:
  ObTableDelRtDef()
      : ObTableDmlBaseRtDef(),
        das_rtdef_(),
        related_rtdefs_()
  {
  }
  TO_STRING_KV(K_(das_rtdef),
               K_(related_rtdefs));
  ObDASDelRtDef das_rtdef_;
  DASDelRtDefArray related_rtdefs_;
};

struct ObTableReplaceCtDef
{
public:
  ObTableReplaceCtDef(common::ObIAllocator &alloc)
      : ins_ctdef_(alloc),
        del_ctdef_(alloc),
        alloc_(alloc)
  {
  }
  TO_STRING_KV(K_(ins_ctdef),
               K_(del_ctdef));
  ObTableInsCtDef ins_ctdef_;
  ObTableDelCtDef del_ctdef_;
  common::ObIAllocator &alloc_;
};

struct ObTableReplaceRtDef
{
public:
  ObTableReplaceRtDef()
      : ins_rtdef_(),
        del_rtdef_()
  {
  }
  TO_STRING_KV(K_(ins_rtdef),
               K_(del_rtdef))
  ObTableInsRtDef ins_rtdef_;
  ObTableDelRtDef del_rtdef_;
};

struct ObTableInsUpdCtDef
{
public:
  ObTableInsUpdCtDef(common::ObIAllocator &alloc)
      : ins_ctdef_(alloc),
        upd_ctdef_(alloc),
        alloc_(alloc)
  {
  }
  TO_STRING_KV(K_(ins_ctdef),
               K_(upd_ctdef));
  ObTableInsCtDef ins_ctdef_;
  ObTableUpdCtDef upd_ctdef_;
  common::ObIAllocator &alloc_;
};

struct ObTableInsUpdRtDef
{
public:
  ObTableInsUpdRtDef()
      : ins_rtdef_(),
        upd_rtdef_()
  {
  }
  TO_STRING_KV(K_(ins_rtdef),
               K_(upd_rtdef))
  ObTableInsRtDef ins_rtdef_;
  ObTableUpdRtDef upd_rtdef_;
};

struct ObTableLockCtDef : ObTableDmlBaseCtDef
{
public:
  ObTableLockCtDef(common::ObIAllocator &alloc)
      : ObTableDmlBaseCtDef(alloc),
        das_ctdef_(alloc),
        alloc_(alloc)
  {
  }
  TO_STRING_KV(K_(das_ctdef));
  ObDASLockCtDef das_ctdef_;
  common::ObIAllocator &alloc_;
};

struct ObTableLockRtDef : ObTableDmlBaseRtDef
{
public:
  ObTableLockRtDef()
      : ObTableDmlBaseRtDef(),
        das_rtdef_()
  {
  }
  TO_STRING_KV(K_(das_rtdef));
  ObDASLockRtDef das_rtdef_;
};

struct ObTableTTLCtDef
{
public:
  ObTableTTLCtDef(common::ObIAllocator &alloc)
      : ins_ctdef_(alloc),
        del_ctdef_(alloc),
        upd_ctdef_(alloc),
        expire_expr_(nullptr),
        alloc_(alloc)
  {
  }
  TO_STRING_KV(K_(ins_ctdef),
               K_(del_ctdef));
  ObTableInsCtDef ins_ctdef_;
  ObTableDelCtDef del_ctdef_;
  ObTableUpdCtDef upd_ctdef_;
  ObExpr *expire_expr_;
  common::ObIAllocator &alloc_;
};

struct ObTableTTLRtDef
{
public:
  ObTableTTLRtDef()
      : ins_rtdef_(),
        del_rtdef_(),
        upd_rtdef_()
  {
  }
  TO_STRING_KV(K_(ins_rtdef),
               K_(del_rtdef),
               K_(upd_rtdef))
  ObTableInsRtDef ins_rtdef_;
  ObTableDelRtDef del_rtdef_;
  ObTableUpdRtDef upd_rtdef_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_CONTEXT_H_ */
