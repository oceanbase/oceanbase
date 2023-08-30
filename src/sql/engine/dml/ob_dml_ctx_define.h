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

#ifndef DEV_SRC_SQL_ENGINE_DML_OB_DML_CTX_DEFINE_H_
#define DEV_SRC_SQL_ENGINE_DML_OB_DML_CTX_DEFINE_H_
#include "sql/das/ob_das_dml_ctx_define.h"
#include "sql/das/ob_das_ref.h"
#include "sql/das/ob_das_scan_op.h"
namespace oceanbase
{
namespace sql
{
typedef ObDASOpType ObDMLOpType;

class ObTableModifyOp;
class ObForeignKeyChecker;
typedef common::ObArrayWrap<ObForeignKeyChecker*> FkCheckerArray;

struct ObErrLogCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObErrLogCtDef(common::ObIAllocator &alloc)
    : is_error_logging_(false),
      err_log_database_name_(),
      err_log_table_name_(),
      reject_limit_(0),
      err_log_values_(alloc),
      err_log_column_names_(alloc)
  {
  }

  TO_STRING_KV(K_(is_error_logging),
               K_(err_log_database_name),
               K_(err_log_table_name),
               K_(reject_limit),
               K_(err_log_values),
               K_(err_log_column_names));

  bool is_error_logging_;
  ObString err_log_database_name_;
  ObString err_log_table_name_;
  int64_t reject_limit_;
  common::ObFixedArray<ObExpr *, common::ObIAllocator> err_log_values_;
  common::ObFixedArray<common::ObString, common::ObIAllocator> err_log_column_names_;
};

struct ObErrLogRtDef
{
public:
  ObErrLogRtDef() :
    curr_err_log_record_num_(0),
    first_err_ret_(OB_SUCCESS)
  {
    msg_[0] = '\0';
  }

  void reset()
  {
    first_err_ret_ = OB_SUCCESS;
    msg_[0] = '\0';
  }
  int64_t curr_err_log_record_num_;  // can’t be reset
  int first_err_ret_;
  char msg_[common::OB_MAX_ERROR_MSG_LEN];
};


class ObTriggerColumnsInfo
{
  OB_UNIS_VERSION(1);
public:
  class Flags
  {
    OB_UNIS_VERSION(1);
  public:
    Flags()
      : flags_(0)
    {}
    TO_STRING_KV(K(is_hidden_), K(is_update_), K(is_gen_col_), K(is_gen_col_dep_), K(is_rowid_));
    union
    {
      uint32_t flags_;
      struct
      {
        // is_update_属性貌似没用,trigger中update_columns_是通过ObDASUpdCtDef.updated_column_infos_
        // 中的column_name进行初始化的
        uint32_t is_hidden_:1;
        uint32_t is_update_:1;
        uint32_t is_gen_col_:1;
        uint32_t is_gen_col_dep_:1;
        uint32_t is_rowid_:1;
        uint32_t reserved_:27;
      };
    };
  };

public:
  ObTriggerColumnsInfo(common::ObIAllocator &allocator)
    : allocator_(&allocator),
      flags_(NULL),
      count_(0),
      capacity_(0)
  {}
  int init(int64_t count);
  int set_trigger_column(bool is_hidden, bool is_update, bool is_gen_col,
                         bool is_gen_col_dep, bool is_rowid);
  int set_trigger_rowid();
  Flags *get_flags() const { return flags_; }
  int64_t get_count() const { return count_; }
  int64_t get_rowtype_count() const { return count_; }
  void reset()
  {
    if (OB_NOT_NULL(flags_)) {
      if (OB_NOT_NULL(flags_)) {
        allocator_->free(flags_);
      }
    }
    allocator_ = NULL;
    flags_ = NULL;
    count_ = 0;
    capacity_ = 0;
  }
  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_NAME(N_FLAGS);
    J_COLON();
    (void)databuff_print_obj_array(buf, buf_len, pos, flags_, count_);
    J_OBJ_END();
    return pos;
  }
private:
  common::ObIAllocator *allocator_;
  Flags *flags_;
  int64_t count_;
  int64_t capacity_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTriggerColumnsInfo);
};

class ObTriggerArg
{
  OB_UNIS_VERSION(1);
public:
  ObTriggerArg()
    : trigger_id_(common::OB_INVALID_ID),
      trigger_events_(),
      timing_points_(),
      analyze_flag_(0)
  {}
  inline void reset()
  {
    trigger_id_ = common::OB_INVALID_ID;
    trigger_events_.reset();
    timing_points_.reset();
  }

  inline void set_trigger_id(uint64_t trigger_id)
  {
    trigger_id_ = trigger_id;
  }
  inline void set_trigger_events(uint64_t trigger_events)
  {
    trigger_events_.set_value(trigger_events);
  }
  inline void set_timing_points(uint64_t timing_points)
  {
    timing_points_.set_value(timing_points);
  }
  inline void set_analyze_flag(uint64_t flag) { analyze_flag_ = flag; }

  inline bool is_no_sql() const { return is_no_sql_; }
  inline bool is_reads_sql_data() const { return is_reads_sql_data_; }
  inline bool is_modifies_sql_data() const { return is_modifies_sql_data_; }
  inline bool is_contains_sql() const { return is_contains_sql_; }
  inline bool is_wps() const { return is_wps_; }
  inline bool is_rps() const { return is_rps_; }
  inline bool is_has_sequence() const { return is_has_sequence_; }
  inline bool is_has_out_param() const { return is_has_out_param_; }
  inline bool is_external_state() const { return is_external_state_; }

  inline bool is_execute_single_row() const
  {
    return (is_modifies_sql_data_ || is_wps_ || is_rps_ || is_has_sequence_ ||
            is_reads_sql_data_ || is_external_state_);
  }

  inline uint64_t get_trigger_id() const { return trigger_id_; }
  inline bool has_when_condition() const { return timing_points_.has_when_condition(); }
  inline bool has_trigger_events(uint64_t event) const { return trigger_events_.has_value(event); }
  inline bool has_before_row_point() const { return timing_points_.has_before_row(); }
  inline bool has_after_row_point() const { return timing_points_.has_after_row(); }
  inline bool has_before_stmt_point() const { return timing_points_.has_before_stmt(); }
  inline bool has_after_stmt_point() const { return timing_points_.has_after_stmt(); }
  inline const share::schema::ObTriggerEvents &get_trigger_events() const { return trigger_events_; }
  inline const share::schema::ObTimingPoints &get_timing_points() const { return timing_points_; }

  TO_STRING_KV(K(trigger_id_),
               K(trigger_events_.bit_value_),
               K(timing_points_.bit_value_));
private:
  /**
   * trigger_events is only used for ObTableMerge now, which has two kinds of
   * operation: insert and update, and trigger event has no option 'merge'.
   */
  uint64_t trigger_id_;
  share::schema::ObTriggerEvents trigger_events_;
  share::schema::ObTimingPoints timing_points_;
  common::ObString package_spec_;
  common::ObString package_body_;
  union {
    uint64_t analyze_flag_;
    struct {
      uint64_t is_no_sql_ : 1;            // it marks trigger do not contain sql stmt
      uint64_t is_reads_sql_data_ : 1;    // it marks trigger contain read sql stmt, such as select stmt
      uint64_t is_modifies_sql_data_ : 1; // it marks trigger contain write sql stmt
      uint64_t is_contains_sql_ : 1;      // it marks trigger do not contain read and write sql, but contain other sql stmt, such as set stmt
      uint64_t is_wps_ : 1;               // it marks trigger write package var
      uint64_t is_rps_ : 1;               // it marks trigger read package var
      uint64_t is_has_sequence_ : 1;      // it marks trigger used sequence
      uint64_t is_has_out_param_ : 1;     // it marks trigger has out param
      uint64_t is_external_state_ : 1;    // it marks trigger access other store routine or global var etc..
      uint64_t reserved_:54;
    };
  };
};
typedef common::ObFixedArray<ObTriggerArg, common::ObIAllocator> ObTriggerArgArray;

//trigger compile context definition
struct ObTrigDMLCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObTrigDMLCtDef(common::ObIAllocator &alloc)
    : tg_event_(0),
      tg_args_(alloc),
      all_tm_points_(),
      trig_col_info_(alloc),
      old_row_exprs_(alloc),
      new_row_exprs_(alloc),
      rowid_old_expr_(nullptr),
      rowid_new_expr_(nullptr)
  { }
  TO_STRING_KV(K_(tg_event),
               K_(tg_args),
               K_(trig_col_info),
               K_(all_tm_points_.bit_value));
  uint64_t tg_event_;
  ObTriggerArgArray tg_args_;
  share::schema::ObTimingPoints all_tm_points_;
  ObTriggerColumnsInfo trig_col_info_;
  ExprFixedArray old_row_exprs_;
  ExprFixedArray new_row_exprs_;
  ObExpr *rowid_old_expr_;
  ObExpr *rowid_new_expr_;
};

//trigger runtime context definition
struct ObTrigDMLRtDef
{
  ObTrigDMLRtDef()
    : old_record_(nullptr),
      new_record_(nullptr),
      tg_when_point_params_(nullptr),
      tg_row_point_params_(),
      tg_all_params_(nullptr),
      update_columns_(nullptr),
      tg_update_columns_(pl::PL_NESTED_TABLE_TYPE, OB_INVALID_ID)
  { }
  TO_STRING_KV(K_(old_record),
               K_(new_record),
               K_(tg_when_point_params),
               K_(tg_row_point_params),
               KPC_(tg_all_params));
  pl::ObPLRecord *old_record_;
  pl::ObPLRecord *new_record_;
  ParamStore *tg_when_point_params_;
  ParamStore *tg_row_point_params_;
  common::ObObjParam *tg_all_params_;
  ObObj *update_columns_;
  pl::ObPLCollection tg_update_columns_;
};

struct ObForeignKeyColumn
{
  OB_UNIS_VERSION(1);
public:
  ObForeignKeyColumn()
    : name_(),
      idx_(-1),
      name_idx_(-1),
      obj_meta_()
  {}
  inline void reset() { name_.reset(); idx_ = -1; name_idx_ = -1; }
  TO_STRING_KV(N_COLUMN_NAME, name_,
               N_INDEX, idx_,
               N_INDEX, name_idx_,
               N_META, obj_meta_);
  common::ObString name_;
  int32_t idx_;  // index of the column id in column_ids_ of ObTableModify. value column idx
  int32_t name_idx_;
  ObObjMeta obj_meta_; // This is used to cast fk type to parent key type
};

struct ObForeignKeyCheckerCtdef
{
  OB_UNIS_VERSION(1);
public:
   ObForeignKeyCheckerCtdef(ObIAllocator &alloc)
    : calc_part_id_expr_(nullptr),
      part_id_dep_exprs_(alloc),
      das_scan_ctdef_(alloc),
      loc_meta_(alloc),
      is_part_table_(false),
      tablet_id_(),
      rowkey_count_(0),
      rowkey_ids_(alloc)
  {}
  // 父表的主表/unique索引表对应的分区键

  TO_STRING_KV(KPC_(calc_part_id_expr),
               K_(part_id_dep_exprs),
               K_(das_scan_ctdef),
               K_(loc_meta),
               K_(is_part_table),
               K_(tablet_id),
               K_(rowkey_count));
  ObExpr *calc_part_id_expr_;
  // calc_part_id_expr_计算所依赖的表达式，用于clear_eval_flag
  ExprFixedArray part_id_dep_exprs_;
  // 回表查询，为了结构统一，主表也需要一次回表，第一次的insert都返回主表的主键
  ObDASScanCtDef das_scan_ctdef_;
  ObDASTableLocMeta loc_meta_;
  bool is_part_table_;
  ObTabletID tablet_id_;
  int64_t rowkey_count_;
  IntFixedArray rowkey_ids_; //save the index of parent key column in rowkey
};


class ObForeignKeyArg
{
  OB_UNIS_VERSION(1);
public:
  ObForeignKeyArg()
    : ref_action_(share::schema::ACTION_INVALID),
      database_name_(),
      table_name_(),
      columns_(),
      is_self_ref_(false),
      table_id_(0),
      fk_ctdef_(nullptr),
      use_das_scan_(false)
  {}

  ObForeignKeyArg(common::ObIAllocator &alloc)
    : ref_action_(share::schema::ACTION_INVALID),
      database_name_(),
      table_name_(),
      columns_(alloc),
      is_self_ref_(false),
      table_id_(0),
      fk_ctdef_(nullptr),
      use_das_scan_(false)
  {}
  inline void reset()
  {
    ref_action_ = share::schema::ACTION_INVALID;
    database_name_.reset();
    table_name_.reset();
    table_id_ = OB_INVALID_ID;
    columns_.reset();
  }
  TO_STRING_KV(K_(ref_action), K_(database_name), K_(table_name), K_(columns), K_(is_self_ref), K_(table_id));
public:
  share::schema::ObReferenceAction ref_action_;
  common::ObString database_name_;
  common::ObString table_name_;
  common::ObFixedArray<ObForeignKeyColumn, common::ObIAllocator> columns_;
  bool is_self_ref_;
  // the index table id of unique index for parent key, used to build das task to scan index table
  uint64_t table_id_;
  ObForeignKeyCheckerCtdef *fk_ctdef_;
  bool use_das_scan_;
};
typedef common::ObFixedArray<ObForeignKeyArg, common::ObIAllocator> ObForeignKeyArgArray;

struct ColumnContent
{
  OB_UNIS_VERSION(1);
  public:
  ColumnContent()
  : projector_index_(0),
    auto_filled_timestamp_(false),
    is_nullable_(false),
    is_implicit_(false),
    is_predicate_column_(false),
    srs_id_(UINT64_MAX),
    column_name_()
  {}

  TO_STRING_KV(N_INDEX, projector_index_,
               N_AUTO_FILL_TIMESTAMP, auto_filled_timestamp_,
               N_NULLABLE, is_nullable_,
               "implicit", is_implicit_,
               K_(is_predicate_column),
               K_(srs_id),
               N_COLUMN_NAME, column_name_);

  uint64_t projector_index_;
  bool auto_filled_timestamp_;
  bool is_nullable_;
  bool is_implicit_;
  bool is_predicate_column_;
  union { // only for gis
    struct {
      uint32_t geo_type_ : 5;
      uint32_t reserved_: 27;
      uint32_t srid_ : 32;
    } srs_info_;
    uint64_t srs_id_;
  };
  common::ObString column_name_;    // only for error message.
};
typedef common::ObFixedArray<ColumnContent, common::ObIAllocator> ColContentFixedArray;
typedef common::ObIArray<ColumnContent> ColContentIArray;

// for check_rowkey_whether_distinct
// to check if each rowkey of ObOpSpec is distinct. same as RowkeyItem
struct SeRowkeyItem
{
  SeRowkeyItem() : row_(NULL), datums_(NULL), cnt_(0) {}
  int init(const ObExprPtrIArray &row, ObEvalCtx &eval_ctx, ObIAllocator &alloc,
                  const int64_t rowkey_cnt);
  bool operator==(const SeRowkeyItem &other) const;
  uint64_t hash() const;
  int copy_datum_data(ObIAllocator &alloc);

  const ObExpr *const* row_;
  ObDatum *datums_;
  int64_t cnt_;
};
typedef common::hash::ObHashSet<ObRowkey, common::hash::NoPthreadDefendMode> SeRowkeyDistCtx;

//dml base compile info definition
struct ObDMLBaseCtDef
{
  OB_UNIS_VERSION(1);
public:
  virtual ~ObDMLBaseCtDef() = default;
  VIRTUAL_TO_STRING_KV(K_(dml_type),
                       K_(check_cst_exprs),
                       K_(fk_args),
                       K_(trig_ctdef),
                       K_(column_ids),
                       K_(old_row),
                       K_(new_row),
                       K_(full_row),
                       K_(view_check_exprs),
                       K_(is_primary_index),
                       K_(is_heap_table),
                       K_(has_instead_of_trigger),
                       KPC_(trans_info_expr));

  ObDMLOpType dml_type_;
  ExprFixedArray check_cst_exprs_;
  ObForeignKeyArgArray fk_args_;
  ObTrigDMLCtDef trig_ctdef_; //trigger compile context definition
  //dml column ids, corresponding with old_row or new_row, mainly used for problem diagnosis
  UIntFixedArray column_ids_;
  ExprFixedArray old_row_;
  ExprFixedArray new_row_;
  ExprFixedArray full_row_;
  //reference the das base ctdef to facilitate
  //some modules to access the das ctdef
  //don't need to serialize
  ObDASDMLBaseCtDef &das_base_ctdef_;
  // used by check_rowkey_whether_distinct
  static const int64_t MIN_ROWKEY_DISTINCT_BUCKET_NUM = 1 * 1024;
  static const int64_t MAX_ROWKEY_DISTINCT_BUCKET_NUM = 1 * 1024 * 1024;
  ObErrLogCtDef error_logging_ctdef_;
  ExprFixedArray view_check_exprs_;
  bool is_primary_index_;
  bool is_heap_table_;
  bool has_instead_of_trigger_;
  ObExpr *trans_info_expr_;
protected:
  ObDMLBaseCtDef(common::ObIAllocator &alloc,
                 ObDASDMLBaseCtDef &das_base_ctdef,
                 ObDMLOpType dml_type)
    : dml_type_(dml_type),
      check_cst_exprs_(alloc),
      fk_args_(alloc),
      trig_ctdef_(alloc),
      column_ids_(alloc),
      old_row_(alloc),
      new_row_(alloc),
      full_row_(alloc),
      das_base_ctdef_(das_base_ctdef),
      error_logging_ctdef_(alloc),
      view_check_exprs_(alloc),
      is_primary_index_(false),
      is_heap_table_(false),
      has_instead_of_trigger_(false),
      trans_info_expr_(nullptr)
  { }
};

//dml base runtime context definition
struct ObDMLBaseRtDef
{
  virtual ~ObDMLBaseRtDef();
  VIRTUAL_TO_STRING_KV(K_(trig_rtdef),
                       K_(cur_row_num));
  ObTrigDMLRtDef trig_rtdef_;
  int64_t cur_row_num_;
  ObTableLocation *check_location_;
  ObNewRow *check_row_; //only used for tablet defensive check
  //reference the das base ctdef to facilitate
  //some modules to access the das rtdef
  ObDASDMLBaseRtDef &das_base_rtdef_;
  FkCheckerArray fk_checker_array_;
protected:
  ObDMLBaseRtDef(ObDASDMLBaseRtDef &das_base_rtdef)
    : trig_rtdef_(),
      cur_row_num_(0),
      check_location_(nullptr),
      check_row_(nullptr),
      das_base_rtdef_(das_base_rtdef)
  { }
};

struct ObMultiInsCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObMultiInsCtDef(common::ObIAllocator &alloc)
    : calc_part_id_expr_(nullptr),
      hint_part_ids_(alloc),
      loc_meta_(alloc)
  {
  }
  TO_STRING_KV(KPC_(calc_part_id_expr),
               K_(hint_part_ids),
               K_(loc_meta));
  ObExpr *calc_part_id_expr_;
  ObjectIDFixedArray hint_part_ids_;
  ObDASTableLocMeta loc_meta_;
};

//insert compile info definition
struct ObInsCtDef : ObDMLBaseCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObInsCtDef(common::ObIAllocator &alloc)
    : ObDMLBaseCtDef(alloc, das_ctdef_, DAS_OP_TABLE_INSERT),
      das_ctdef_(alloc),
      related_ctdefs_(alloc),
      column_infos_(alloc),
      multi_ctdef_(nullptr),
      alloc_(alloc),
      is_single_value_(false)
  { }
  INHERIT_TO_STRING_KV("base_ctdef", ObDMLBaseCtDef,
                       K_(das_ctdef),
                       K_(related_ctdefs),
                       K_(column_infos),
                       KPC_(multi_ctdef),
                       K_(is_single_value));
  ObDASInsCtDef das_ctdef_;
  DASInsCtDefArray related_ctdefs_;
  ColContentFixedArray column_infos_;
  ObMultiInsCtDef *multi_ctdef_;
  common::ObIAllocator &alloc_;
  bool is_single_value_;
};

struct ObInsRtDef : ObDMLBaseRtDef
{
  ObInsRtDef()
    : ObDMLBaseRtDef(das_rtdef_),
      das_rtdef_(),
      related_rtdefs_()
  { }
  INHERIT_TO_STRING_KV("ObDMLBaseRtDef", ObDMLBaseRtDef,
               K_(das_rtdef),
               K_(related_rtdefs));
  ObDASInsRtDef das_rtdef_;
  DASInsRtDefArray related_rtdefs_;
};

struct ObMultiUpdCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObMultiUpdCtDef(common::ObIAllocator &alloc)
    : calc_part_id_old_(nullptr),
      calc_part_id_new_(nullptr),
      hint_part_ids_(alloc),
      is_enable_row_movement_(false),
      loc_meta_(alloc)
  {
  }
  TO_STRING_KV(KPC_(calc_part_id_old),
               KPC_(calc_part_id_new),
               K_(is_enable_row_movement),
               K_(loc_meta));
  ObExpr *calc_part_id_old_; //calc partition id for old_row
  ObExpr *calc_part_id_new_; //calc partition id for new_row
  ObjectIDFixedArray hint_part_ids_;
  bool is_enable_row_movement_;
  ObDASTableLocMeta loc_meta_;
};

struct ObUpdCtDef : ObDMLBaseCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObUpdCtDef(common::ObIAllocator &alloc)
    : ObDMLBaseCtDef(alloc, dupd_ctdef_, DAS_OP_TABLE_UPDATE),
      dupd_ctdef_(alloc),
      need_check_filter_null_(false),
      distinct_algo_(T_DISTINCT_NONE),
      assign_columns_(alloc),
      ddel_ctdef_(nullptr),
      dins_ctdef_(nullptr),
      dlock_ctdef_(nullptr),
      multi_ctdef_(nullptr),
      distinct_key_(alloc),
      related_upd_ctdefs_(alloc),
      related_del_ctdefs_(alloc),
      related_ins_ctdefs_(alloc),
      alloc_(alloc)
  { }
  INHERIT_TO_STRING_KV("ObDMLBaseCtDef", ObDMLBaseCtDef,
                       K_(dupd_ctdef),
                       K_(need_check_filter_null),
                       K_(distinct_algo),
                       K_(assign_columns),
                       K_(distinct_key),
                       KPC_(ddel_ctdef),
                       KPC_(dins_ctdef),
                       KPC_(dlock_ctdef),
                       KPC_(multi_ctdef),
                       K_(distinct_key),
                       K_(related_upd_ctdefs),
                       K_(related_del_ctdefs),
                       K_(related_ins_ctdefs));
  ObDASUpdCtDef dupd_ctdef_;
  bool need_check_filter_null_;
  DistinctType distinct_algo_;
  ColContentFixedArray assign_columns_;
  //if update target column involve the partition key,
  //the update operation may be split into delete and insert
  ObDASDelCtDef *ddel_ctdef_;
  ObDASInsCtDef *dins_ctdef_;
  //in mysql mode, if update target column's new value is same with old row
  // update operation will not update the row in storage, only lock this row
  ObDASLockCtDef *dlock_ctdef_;
  ObMultiUpdCtDef *multi_ctdef_;
  ExprFixedArray distinct_key_;
  DASUpdCtDefArray related_upd_ctdefs_;
  DASDelCtDefArray related_del_ctdefs_;
  DASInsCtDefArray related_ins_ctdefs_;
  common::ObIAllocator &alloc_;
};

struct ObUpdRtDef : ObDMLBaseRtDef
{
public:
  ObUpdRtDef()
    : ObDMLBaseRtDef(dupd_rtdef_),
      dupd_rtdef_(),
      se_rowkey_dist_ctx_(nullptr),
      ddel_rtdef_(nullptr),
      dins_rtdef_(nullptr),
      dlock_rtdef_(nullptr),
      primary_rtdef_(nullptr),
      is_row_changed_(false),
      found_rows_(0),
      related_upd_rtdefs_(),
      related_del_rtdefs_(),
      related_ins_rtdefs_(),
      table_rowkey_()
  { }
  virtual ~ObUpdRtDef()
  {
    if (se_rowkey_dist_ctx_ != nullptr) {
      se_rowkey_dist_ctx_->destroy();
      se_rowkey_dist_ctx_ = nullptr;
    }
    if (ddel_rtdef_ != nullptr) {
      ddel_rtdef_->~ObDASDelRtDef();
      ddel_rtdef_ = nullptr;
    }
    if (dins_rtdef_ != nullptr) {
      dins_rtdef_->~ObDASInsRtDef();
      dins_rtdef_ = nullptr;
    }
    if (dlock_rtdef_ != nullptr) {
      dlock_rtdef_->~ObDASLockRtDef();
      dlock_rtdef_ = nullptr;
    }
    table_rowkey_.reset();
  }
  INHERIT_TO_STRING_KV("base_rtdef", ObDMLBaseRtDef,
                       K_(dupd_rtdef),
                       KPC_(ddel_rtdef),
                       KPC_(dins_rtdef),
                       KPC_(dlock_rtdef),
                       K_(is_row_changed),
                       K_(found_rows),
                       K_(related_upd_rtdefs),
                       K_(related_del_rtdefs),
                       K_(related_ins_rtdefs));
  ObDASUpdRtDef dupd_rtdef_;
  SeRowkeyDistCtx *se_rowkey_dist_ctx_;
  ObDASDelRtDef *ddel_rtdef_;
  ObDASInsRtDef *dins_rtdef_;
  ObDASLockRtDef *dlock_rtdef_;
  ObUpdRtDef *primary_rtdef_; //reference the data table's rtdef
  bool is_row_changed_;
  int64_t found_rows_;
  DASUpdRtDefArray related_upd_rtdefs_;
  DASDelRtDefArray related_del_rtdefs_;
  DASInsRtDefArray related_ins_rtdefs_;
  ObRowkey table_rowkey_;
};

struct ObMultiLockCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObMultiLockCtDef(common::ObIAllocator &alloc)
    : calc_part_id_expr_(nullptr),
      partition_cnt_(0),
      loc_meta_(alloc)
  {
    UNUSED(alloc);
  }
  TO_STRING_KV(KPC_(calc_part_id_expr),
               K_(partition_cnt),
               K_(loc_meta));
  ObExpr *calc_part_id_expr_; //used to calc the partition id of the target row
  int64_t partition_cnt_; //the target table partition_cnt_
  ObDASTableLocMeta loc_meta_;
};

struct ObLockCtDef : ObDMLBaseCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObLockCtDef(common::ObIAllocator &alloc)
    : ObDMLBaseCtDef(alloc, das_ctdef_, DAS_OP_TABLE_LOCK),
      das_ctdef_(alloc),
      need_check_filter_null_(false),
      distinct_algo_(T_DISTINCT_NONE),
      multi_ctdef_(nullptr),
      alloc_(alloc)
  { }
  INHERIT_TO_STRING_KV("lock_ctdef", ObDMLBaseCtDef,
                       K_(das_ctdef),
                       KPC_(multi_ctdef),
                       K_(need_check_filter_null),
                       K_(distinct_algo));
  ObDASLockCtDef das_ctdef_;
  bool need_check_filter_null_;
  DistinctType distinct_algo_;
  ObMultiLockCtDef *multi_ctdef_;
  common::ObIAllocator &alloc_;
};

struct ObLockRtDef : ObDMLBaseRtDef
{
public:
  ObLockRtDef()
    : ObDMLBaseRtDef(das_rtdef_),
      das_rtdef_()
  { }
  INHERIT_TO_STRING_KV("base_rtdef", ObDMLBaseRtDef,
                       K_(das_rtdef));
  ObDASLockRtDef das_rtdef_;
};

struct ObMultiDelCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObMultiDelCtDef(common::ObIAllocator &alloc)
    : calc_part_id_expr_(nullptr),
      loc_meta_(alloc)
  {
    UNUSED(alloc);
  }
  TO_STRING_KV(KPC_(calc_part_id_expr),
               K_(loc_meta));
  ObExpr *calc_part_id_expr_; //used to calc the partition id of the target row
  ObDASTableLocMeta loc_meta_;
};

struct ObDelCtDef : ObDMLBaseCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDelCtDef(common::ObIAllocator &alloc)
    : ObDMLBaseCtDef(alloc, das_ctdef_, DAS_OP_TABLE_DELETE),
      das_ctdef_(alloc),
      related_ctdefs_(alloc),
      need_check_filter_null_(false),
      distinct_algo_(T_DISTINCT_NONE),
      multi_ctdef_(nullptr),
      distinct_key_(alloc),
      alloc_(alloc)
  { }
  INHERIT_TO_STRING_KV("base_ctdef", ObDMLBaseCtDef,
                       K_(das_ctdef),
                       K_(related_ctdefs),
                       KPC_(multi_ctdef),
                       K_(distinct_key));
  ObDASDelCtDef das_ctdef_;
  DASDelCtDefArray related_ctdefs_;
  bool need_check_filter_null_;
  DistinctType distinct_algo_;
  ObMultiDelCtDef *multi_ctdef_;
  ExprFixedArray distinct_key_;
  common::ObIAllocator &alloc_;
};

struct ObDelRtDef : ObDMLBaseRtDef
{
public:
  ObDelRtDef()
    : ObDMLBaseRtDef(das_rtdef_),
      das_rtdef_(),
      related_rtdefs_(),
      se_rowkey_dist_ctx_(nullptr),
      table_rowkey_()
  { }
  virtual ~ObDelRtDef()
  {
    if (se_rowkey_dist_ctx_ != nullptr) {
      // se_rowkey_dist_ctx_->destroy();
      se_rowkey_dist_ctx_ = nullptr;
    }
    table_rowkey_.reset();
  }
  INHERIT_TO_STRING_KV("base_rtdef", ObDMLBaseRtDef,
                       K_(das_rtdef),
                       K_(related_rtdefs));
  ObDASDelRtDef das_rtdef_;
  DASDelRtDefArray related_rtdefs_;
  SeRowkeyDistCtx *se_rowkey_dist_ctx_;
  ObRowkey table_rowkey_;
};
struct ObMergeCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObMergeCtDef(common::ObIAllocator &alloc)
    : ins_ctdef_(NULL),
      upd_ctdef_(NULL),
      del_ctdef_(NULL),
      alloc_(alloc)
  { }

  TO_STRING_KV(KPC_(ins_ctdef),
               KPC_(upd_ctdef),
               KPC_(del_ctdef))

  ObInsCtDef *ins_ctdef_;
  ObUpdCtDef *upd_ctdef_;
  ObDelCtDef *del_ctdef_;
  common::ObIAllocator &alloc_;
};

struct ObMergeRtDef
{
public:
  ObMergeRtDef()
    : ins_rtdef_(),
      upd_rtdef_(),
      del_rtdef_(),
      rowkey_dist_ctx_(NULL),
      table_rowkey_()
  { }

  ~ObMergeRtDef()
  {
    ins_rtdef_.~ObInsRtDef();
    upd_rtdef_.~ObUpdRtDef();
    del_rtdef_.~ObDelRtDef();
    if (rowkey_dist_ctx_ != nullptr) {
      rowkey_dist_ctx_->destroy();
      rowkey_dist_ctx_ = nullptr;
    }
    table_rowkey_.reset();
  }

  TO_STRING_KV(K_(ins_rtdef),
               K_(upd_rtdef),
               K_(del_rtdef))

  ObInsRtDef ins_rtdef_;
  ObUpdRtDef upd_rtdef_;
  ObDelRtDef del_rtdef_;
  SeRowkeyDistCtx *rowkey_dist_ctx_;
  ObRowkey table_rowkey_;
};

struct ObReplaceCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObReplaceCtDef(common::ObIAllocator &alloc)
    : ins_ctdef_(NULL),
      del_ctdef_(NULL),
      alloc_(alloc)
  { }
  TO_STRING_KV(KPC_(ins_ctdef),
               KPC_(del_ctdef))
  ObInsCtDef *ins_ctdef_;
  ObDelCtDef *del_ctdef_;
  common::ObIAllocator &alloc_;
};

struct ObReplaceRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObReplaceRtDef()
    : ins_rtdef_(),
      del_rtdef_()
  { }
  ~ObReplaceRtDef()
   {
     ins_rtdef_.~ObInsRtDef();
     del_rtdef_.~ObDelRtDef();
   }
  TO_STRING_KV(K_(ins_rtdef),
               K_(del_rtdef))
  ObInsRtDef ins_rtdef_;
  ObDelRtDef del_rtdef_;
};

struct ObInsertUpCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObInsertUpCtDef(common::ObIAllocator &alloc)
    : ins_ctdef_(NULL),
      upd_ctdef_(NULL),
      is_upd_rowkey_(false),
      alloc_(alloc)
  { }
  TO_STRING_KV(KPC_(ins_ctdef),
               KPC_(upd_ctdef),
               K_(is_upd_rowkey))
  ObInsCtDef *ins_ctdef_;
  ObUpdCtDef *upd_ctdef_;
  bool is_upd_rowkey_;
  common::ObIAllocator &alloc_;
};

struct ObInsertUpRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObInsertUpRtDef()
    : ins_rtdef_(),
      upd_rtdef_()
  { }
  ~ObInsertUpRtDef()
   {
     ins_rtdef_.~ObInsRtDef();
     upd_rtdef_.~ObUpdRtDef();
   }
  TO_STRING_KV(K_(ins_rtdef),
               K_(upd_rtdef))
  ObInsRtDef ins_rtdef_;
  ObUpdRtDef upd_rtdef_;
};

union DasTaskStatus
{
  DasTaskStatus() : task_status_(0) {}
  explicit DasTaskStatus(int64_t flag) : task_status_(flag) {}
  bool need_pick_del_task_first() { return PICK_DEL_TASK_FIRST; }
  bool need_non_sub_full_task() { return NON_SUB_FULL_TASK; }
  int64_t task_status_;
  struct {
    int64_t PICK_DEL_TASK_FIRST:1;
    // for replace into and insert up，insert need fetch all conflict row
    // so insert's das task can't submit when das_buff is full，
    // must frozen curr das list，then create an new insert das task
    // until all row has been written, submit all insert das tasks
    int64_t NON_SUB_FULL_TASK:1; // not submit the task until all row written
  };
};

struct ObDMLRtCtx
{
  ObDMLRtCtx(ObEvalCtx &eval_ctx, ObExecContext &exec_ctx, ObTableModifyOp &op)
    : das_ref_(eval_ctx, exec_ctx),
      das_task_status_(),
      op_(op),
      cached_row_size_(0)
  { }

  void reuse()
  {
    das_ref_.reuse();
    cached_row_size_ = 0;
  }

  void cleanup()
  {
    das_ref_.reset();
  }

  common::ObIAllocator &get_das_alloc() { return das_ref_.get_das_alloc(); }
  ObExecContext &get_exec_ctx() { return das_ref_.get_exec_ctx(); }
  ObEvalCtx &get_eval_ctx() { return das_ref_.get_eval_ctx(); }
  void set_pick_del_task_first() { das_task_status_.PICK_DEL_TASK_FIRST = 1; }
  void set_non_sub_full_task() { das_task_status_.NON_SUB_FULL_TASK = 1; }
  bool need_pick_del_task_first()
  { return das_task_status_.need_pick_del_task_first(); }
  bool need_non_sub_full_task()
  { return das_task_status_.need_non_sub_full_task(); }
  void add_cached_row_size(const int64_t row_size) { cached_row_size_ += row_size; }
  int64_t get_row_buffer_size() const { return cached_row_size_; }

  ObDASRef das_ref_;
  DasTaskStatus das_task_status_;
  ObTableModifyOp &op_;
  int64_t cached_row_size_;
};

template <typename T>
class ObDMLCtDefAllocator
{
public:
  ObDMLCtDefAllocator(common::ObIAllocator &alloc)
    : alloc_(alloc)
  { }

  T *alloc()
  {
    T *ctx = nullptr;
    void *buf = alloc_.alloc(sizeof(T));
    if (buf != nullptr) {
      ctx = new(buf) T(alloc_);
    }
    return ctx;
  }
private:
  common::ObIAllocator &alloc_;
};

struct ObDMLModifyRowNode
{
public:
  ObDMLModifyRowNode(ObTableModifyOp  *dml_op, const ObDMLBaseCtDef *dml_ctdef, ObDMLBaseRtDef *dml_rtdef, const ObDmlEventType dml_event)
          : new_row_(nullptr),
            old_row_(nullptr),
            full_row_(nullptr),
            dml_op_(dml_op),
            dml_ctdef_(dml_ctdef),
            dml_rtdef_(dml_rtdef),
            dml_event_(dml_event)
  {}
  ObChunkDatumStore::StoredRow *new_row_;
  ObChunkDatumStore::StoredRow *old_row_;
  ObChunkDatumStore::StoredRow *full_row_;
  ObTableModifyOp  *dml_op_;
  const ObDMLBaseCtDef *dml_ctdef_;
  ObDMLBaseRtDef *dml_rtdef_;
  ObDmlEventType dml_event_;
};

typedef common::ObList<ObDMLModifyRowNode, common::ObIAllocator> ObDMLModifyRowsList;
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_ENGINE_DML_OB_DML_CTX_DEFINE_H_ */
