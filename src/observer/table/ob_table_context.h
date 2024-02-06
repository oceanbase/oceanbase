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

namespace oceanbase
{
namespace table
{
struct ObTableColumnItem : public sql::ColumnItem
{
  ObTableColumnItem()
      : sql::ColumnItem(),
        raw_expr_(nullptr),
        is_generated_column_(false),
        is_stored_generated_column_(false),
        is_virtual_generated_column_(false),
        is_auto_increment_(false),
        rowkey_position_(-1)
  {}
  TO_STRING_KV("ColumnItem", static_cast<const sql::ColumnItem &>(*this),
               KPC_(raw_expr),
               K_(is_generated_column),
               K_(is_stored_generated_column),
               K_(is_virtual_generated_column),
               K_(cascaded_column_ids),
               K_(generated_expr_str),
               K_(dependant_exprs),
               K_(is_auto_increment),
               K_(rowkey_position));
  sql::ObRawExpr *raw_expr_; // column ref expr or calculate expr
  bool is_generated_column_;
  bool is_stored_generated_column_;
  bool is_virtual_generated_column_;
  common::ObSEArray<uint64_t, 8> cascaded_column_ids_;
  // default equal item.default_value_.get_string()
  // specific value in append and increment operation
  common::ObString generated_expr_str_;
  common::ObSEArray<sql::ObRawExpr*, 8, common::ModulePageAllocator, true> dependant_exprs_;
  bool is_auto_increment_;
  int64_t rowkey_position_; // greater than zero if this is rowkey column, 0 if this is common column
};

struct ObTableAssignment : public sql::ObAssignment
{
  ObTableAssignment()
      : sql::ObAssignment(),
        column_item_(nullptr),
        is_inc_or_append_(false),
        delta_expr_(nullptr),
        is_assigned_(false)
  {}
  ObTableAssignment(ObTableColumnItem *item)
      : sql::ObAssignment(),
        column_item_(item),
        is_inc_or_append_(false),
        delta_expr_(nullptr),
        is_assigned_(false)
  {}
  TO_STRING_KV("ObAssignment", static_cast<const sql::ObAssignment &>(*this),
               KPC_(column_item),
               K_(is_inc_or_append),
               KPC_(delta_expr),
               K_(assign_value),
               K_(is_assigned));
  ObTableColumnItem *column_item_;
  bool is_inc_or_append_; // for append/increment
  sql::ObColumnRefRawExpr *delta_expr_; // for append/increment
  common::ObObj assign_value_;
  bool is_assigned_; // did user assign specific value or not
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

// 1.用于存放整个process过程中需要的通用上下文信息
// 2.在try_process()中进行初始化
class ObTableCtx
{
public:
  explicit ObTableCtx(common::ObIAllocator &allocator)
      : allocator_(allocator),
        ctx_allocator_("ObTableCtx", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        expr_info_(nullptr),
        exec_ctx_(allocator_),
        expr_factory_(allocator_),
        all_exprs_(false),
        loc_meta_(allocator_),
        agg_cell_proj_(allocator_),
        has_auto_inc_(false)
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
    table_schema_ = nullptr;
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
    is_for_insertup_ = false;
    entity_type_ = ObTableEntityType::ET_DYNAMIC;
    entity_ = nullptr;
    batch_op_ = nullptr;
    return_affected_entity_ = false;
    return_rowkey_ = false;
    cur_cluster_version_ = GET_MIN_CLUSTER_VERSION();
    is_ttl_table_ = false;
    is_skip_scan_ = false;
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
               K_(is_skip_scan));
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
  OB_INLINE common::ObString &get_table_name() { return table_name_; }
  OB_INLINE share::ObLSID& get_ls_id() { return ls_id_; }
  OB_INLINE int64_t get_timeout_ts() const { return timeout_ts_; }
  OB_INLINE const share::schema::ObTableSchema* get_table_schema() const { return table_schema_; }
  OB_INLINE const share::schema::ObSchemaGetterGuard& get_schema_guard() const { return schema_guard_; }
  OB_INLINE share::schema::ObSchemaGetterGuard& get_schema_guard() { return schema_guard_; }
  OB_INLINE sql::ObExprFrameInfo* get_expr_frame_info() { return expr_info_; }
  OB_INLINE sql::ObExecContext& get_exec_ctx() { return exec_ctx_; }
  OB_INLINE sql::ObRawExprFactory& get_expr_factory() { return expr_factory_; }
  OB_INLINE sql::ObRawExprUniqueSet& get_all_exprs() { return all_exprs_; }
  OB_INLINE ObIArray<sql::ObRawExpr *>& get_all_exprs_array() {
    return const_cast<ObIArray<ObRawExpr *> &>(all_exprs_.get_expr_array());
  }
  OB_INLINE sql::ObSQLSessionInfo& get_session_info()
  { return sess_guard_.get_sess_info();}
  OB_INLINE const sql::ObSQLSessionInfo& get_session_info() const
  { return sess_guard_.get_sess_info(); }
  OB_INLINE int64_t get_tenant_schema_version() const { return tenant_schema_version_; }
  OB_INLINE ObTableOperationType::Type get_opertion_type() const { return operation_type_; }
  OB_INLINE bool is_init() const { return is_init_; }
  OB_INLINE const ObIArray<ObTableColumnItem>& get_column_items() const { return column_items_; }
  OB_INLINE ObIArray<ObTableColumnItem>& get_column_items() { return column_items_; }
  OB_INLINE const ObIArray<ObTableAssignment>& get_assignments() const { return assigns_; }
  OB_INLINE ObIArray<ObTableAssignment>& get_assignments() { return assigns_; }
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
  OB_INLINE const ObIArray<sql::ObRawExpr *>& get_select_exprs() const { return select_exprs_; }
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
  // for update
  OB_INLINE bool is_for_update() const { return is_for_update_; }
  OB_INLINE bool is_inc_or_append() const
  {
    return ObTableOperationType::Type::APPEND == operation_type_
      || ObTableOperationType::Type::INCREMENT == operation_type_;
  }
  OB_INLINE bool is_dml() const
  {
    return ObTableOperationType::Type::GET != operation_type_ && !is_scan_;
  }
  // for dml
  OB_INLINE const ObIArray<common::ObTableID>& get_related_index_ids() const { return related_index_ids_; }
  OB_INLINE bool is_for_insertup() const { return is_for_insertup_; }
  OB_INLINE const ObITableEntity* get_entity() const { return entity_; }
  OB_INLINE ObTableEntityType get_entity_type() const { return entity_type_; }
  OB_INLINE bool is_htable() const { return ObTableEntityType::ET_HKV == entity_type_; }
  OB_INLINE bool is_insert() const
  {
    return ObTableOperationType::Type::INSERT == operation_type_;
  }
  // for htable
  OB_INLINE const ObTableBatchOperation* get_batch_operation() const { return batch_op_; }
  // for increment/append
  OB_INLINE bool return_affected_entity() const { return return_affected_entity_;}
  OB_INLINE bool return_rowkey() const { return return_rowkey_;}
  OB_INLINE uint64_t get_cur_cluster_version() const { return cur_cluster_version_;}
  OB_INLINE bool has_generated_column() const { return table_schema_->has_generated_column(); }
  // for aggregate
  OB_INLINE const common::ObIArray<uint64_t> &get_agg_projs() const { return agg_cell_proj_; }
  OB_INLINE ObPhysicalPlanCtx *get_physical_plan_ctx() { return exec_ctx_.get_physical_plan_ctx(); }
  OB_INLINE bool has_auto_inc() { return has_auto_inc_; }
  //////////////////////////////////////// setter ////////////////////////////////////////////////
  // for common
  OB_INLINE void set_init_flag(bool is_init) { is_init_ = is_init; }
  OB_INLINE void set_expr_info(ObExprFrameInfo *expr_info) { expr_info_ = expr_info; }
  // for scan
  OB_INLINE void set_scan(const bool &is_scan) { is_scan_ = is_scan; }
  OB_INLINE void set_limit(const int64_t &limit) { limit_ = limit; }
  OB_INLINE void set_read_latest(bool read_latest) { read_latest_ = read_latest; }
  // for dml
  OB_INLINE void set_entity(const ObITableEntity *entity) { entity_ = entity; }
  OB_INLINE void set_entity_type(const ObTableEntityType &type) { entity_type_ = type; }
  OB_INLINE void set_operation_type(const ObTableOperationType::Type op_type) { operation_type_ = op_type; }
  // for htable
  OB_INLINE void set_batch_operation(const ObTableBatchOperation *batch_op) { batch_op_ = batch_op; }
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
public:
  // 基于 table name 初始化common部分(不包括expr_info_, exec_ctx_)
  int init_common(ObTableApiCredential &credential,
                  const common::ObTabletID &arg_tablet_id,
                  const common::ObString &arg_table_name,
                  const int64_t &timeout_ts);

  // 基于 table id 初始化common部分(不包括expr_info_, exec_ctx_)
  int init_common(ObTableApiCredential &credential,
                  const common::ObTabletID &arg_tablet_id,
                  const uint64_t table_id,
                  const int64_t &timeout_ts);
  // 初始化 insert 相关
  int init_insert();
  // 初始化scan相关(不包括表达分类)
  int init_scan(const ObTableQuery &query,
                const bool &is_wead_read);
  // 初始化update相关
  int init_update();
  // 初始化delete相关
  int init_delete();
  // 初始化replace相关
  int init_replace();
  // 初始化insert_up相关
  int init_insert_up();
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
  int init_physical_plan_ctx(int64_t timeout_ts, int64_t tenant_schema_version);
  // 更新全局自增值
  int update_auto_inc_value();
  // init table context for ttl operation
  bool is_ttl_table() const { return is_ttl_table_; }

  void set_is_ttl_table(bool is_ttl_table) { is_ttl_table_ = is_ttl_table; }
  int init_ttl_delete(ObRowkey &start_key);
  int get_column_item_by_column_id(uint64_t column_id, const ObTableColumnItem *&item) const;
  int get_column_item_by_expr(sql::ObRawExpr *raw_expr, const ObTableColumnItem *&item) const;
  int get_column_item_by_expr(sql::ObColumnRefRawExpr *expr, const ObTableColumnItem *&item) const;
  int get_expr_from_column_items(const common::ObString &col_name, sql::ObRawExpr *&expr) const;
  int get_expr_from_assignments(const common::ObString &col_name, sql::ObRawExpr *&expr) const;
  int check_insert_up_can_use_put(bool &use_put);
public:
  // convert lob的allocator需要保证obj写入表达式后才能析构
  static int convert_lob(common::ObIAllocator &allocator, ObObj &obj);
  // read lob的allocator需要保证obj序列化到rpc buffer后才能析构
  static int read_real_lob(common::ObIAllocator &allocator, ObObj &obj);
private:
  // for common
  int get_tablet_by_rowkey(const common::ObRowkey &rowkey,
                           common::ObTabletID &tablet_id);
  int init_sess_info(ObTableApiCredential &credential);
  // for scan
  int init_index_info(const common::ObString &index_name);
  int generate_columns_type(common::ObIArray<sql::ObExprResType> &columns_type);
  int generate_key_range(const common::ObIArray<common::ObNewRange> &scan_ranges);
  // for dml
  int init_dml_related_tid();
  // for update
  int init_assignments(const ObTableEntity &entity);
  int add_stored_generated_column_assignment(const ObTableAssignment &assign);
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

  int add_auto_inc_param(const share::schema::ObColumnSchemaV2 &column_schema);

private:
  int construct_column_items();
  int cons_column_type(const share::schema::ObColumnSchemaV2 &column_schema,
                       sql::ObExprResType &column_type);
  int adjust_column_type(const ObExprResType &column_type, ObObj &obj);
  int adjust_column(const ObColumnSchemaV2 &col_schema, ObObj &obj);
  int adjust_rowkey();
  int adjust_properties();
  int adjust_entity();
  bool has_exist_in_columns(const common::ObIArray<common::ObString>& columns,
                            const common::ObString &name,
                            int64_t *idx = nullptr) const;
  // 获取索引表的tablet_id
  int get_related_tablet_id(const share::schema::ObTableSchema &index_schema,
                            common::ObTabletID &related_tablet_id);


  // 初始化 table schema 之后的 common 部分
  int inner_init_common(ObTableApiCredential &credential,
                        const common::ObTabletID &arg_tablet_id,
                        const common::ObString &table_name,
                        const int64_t &timeout_ts);
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
  const share::schema::ObTableSchema *table_schema_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  sql::ObExprFrameInfo *expr_info_;
  sql::ObExecContext exec_ctx_;
  sql::ObRawExprFactory expr_factory_;
  sql::ObRawExprUniqueSet all_exprs_;
  ObTableApiSessGuard sess_guard_;
  sql::ObDASTableLocMeta loc_meta_;
  int64_t tenant_schema_version_;
  common::ObSEArray<ObTableColumnItem, 8> column_items_;
  common::ObSEArray<ObTableAssignment, 8> assigns_;
  // for scan
  bool is_scan_;
  bool is_index_scan_;
  bool is_index_back_;
  bool is_weak_read_;
  bool is_get_;
  bool read_latest_; // default true, false in single get and multi get
  common::ObQueryFlag::ScanOrder scan_order_;
  common::ObSEArray<sql::ObRawExpr*, 32> select_exprs_;
  common::ObSEArray<sql::ObRawExpr*, 16> rowkey_exprs_;
  common::ObSEArray<sql::ObRawExpr*, 16> index_exprs_;
  common::ObSEArray<sql::ObRawExpr*, 8> filter_exprs_;
  common::ObSEArray<uint64_t, 32> select_col_ids_; // 基于schema序的select column id
  common::ObSEArray<uint64_t, 32> query_col_ids_; // 用户查询的select column id
  common::ObSEArray<common::ObString, 32> query_col_names_; // 用户查询的select column name，引用的是schema上的列名
  common::ObSEArray<uint64_t, 16> index_col_ids_;
  const share::schema::ObTableSchema *index_schema_;
  int64_t offset_;
  int64_t limit_;
  common::ObSEArray<common::ObNewRange, 16> key_ranges_;
  // for update
  bool is_for_update_;
  ObTableOperationType::Type operation_type_;
  // agg cell index in schema
  common::ObFixedArray<uint64_t, common::ObIAllocator> agg_cell_proj_;
  // for auto inc
  bool has_auto_inc_;
  // for increment/append
  bool return_affected_entity_;
  bool return_rowkey_;
  // for dml
  common::ObSEArray<common::ObTableID, 16> related_index_ids_;
  bool is_for_insertup_;
  ObTableEntityType entity_type_;
  const ObITableEntity *entity_;
  // for htable
  const ObTableBatchOperation *batch_op_;
  // for lob adapt
  uint64_t cur_cluster_version_;
  bool is_ttl_table_;
  // for delete skip scan
  bool is_skip_scan_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableCtx);
};

struct ObTableDmlBaseCtDef
{
public:
  virtual ~ObTableDmlBaseCtDef() = default;
  VIRTUAL_TO_STRING_KV(K_(column_ids),
                       K_(old_row),
                       K_(new_row));

  UIntFixedArray column_ids_;
  ExprFixedArray old_row_;
  ExprFixedArray new_row_;
protected:
  ObTableDmlBaseCtDef(common::ObIAllocator &alloc)
      : column_ids_(alloc),
        old_row_(alloc),
        new_row_(alloc)
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
        allocator_(allocator)
  {
  }
  TO_STRING_KV(K_(scan_ctdef),
               KPC_(lookup_ctdef),
               KPC_(lookup_loc_meta));
  sql::ObDASScanCtDef scan_ctdef_;
  sql::ObDASScanCtDef *lookup_ctdef_;
  sql::ObDASTableLocMeta *lookup_loc_meta_;

  ExprFixedArray output_exprs_;
  ExprFixedArray filter_exprs_;
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