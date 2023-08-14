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

#ifndef OCEANBASE_SCHEMA_COLUMN_SCHEMA_H_
#define OCEANBASE_SCHEMA_COLUMN_SCHEMA_H_
#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "share/schema/ob_schema_struct.h"
#include "common/rowkey/ob_rowkey_info.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

const char *const STR_COLUMN_TYPE_INT = "int";
const char *const STR_COLUMN_TYPE_UINT64 = "uint64";
const char *const STR_COLUMN_TYPE_FLOAT = "float";
const char *const STR_COLUMN_TYPE_UFLOAT = "ufloat";
const char *const STR_COLUMN_TYPE_DOUBLE = "double";
const char *const STR_COLUMN_TYPE_UDOUBLE = "udouble";
const char *const STR_COLUMN_TYPE_VCHAR = "varchar";
const char *const STR_COLUMN_TYPE_DATETIME = "datetime";
const char *const STR_COLUMN_TYPE_TIMESTAMP = "timestamp";
const char *const STR_COLUMN_TYPE_PRECISE_DATETIME = "precise_datetime";
const char *const STR_COLUMN_TYPE_C_TIME = "create_time";
const char *const STR_COLUMN_TYPE_M_TIME = "modify_time";
const char *const STR_COLUMN_TYPE_BOOLEAN = "bool";
const char *const STR_COLUMN_TYPE_NUMBER = "number";
const char *const STR_COLUMN_TYPE_RAW = "raw";
const char *const STR_COLUMN_TYPE_UNKNOWN = "unknown";

class ObTableSchema;
class ObColumnSchemaV2 : public ObSchema
{
    OB_UNIS_VERSION_V(1);
public:
  static const char *convert_column_type_to_str(common::ColumnType type);
  static common::ColumnType convert_str_to_column_type(const char *str);
  //constructor and destructor
  ObColumnSchemaV2();
  explicit ObColumnSchemaV2(common::ObIAllocator *allocator);
  ObColumnSchemaV2(const ObColumnSchemaV2 &src_schema);
  virtual ~ObColumnSchemaV2();

  //operators
  ObColumnSchemaV2 &operator=(const ObColumnSchemaV2 &src_schema);
  bool operator==(const ObColumnSchemaV2 &r) const;
  bool operator!=(const ObColumnSchemaV2 &r) const;

  int assign(const ObColumnSchemaV2 &other);

  //set methods
  inline void set_tenant_id(const uint64_t id) { tenant_id_ = id; }
  inline void set_table_id(const uint64_t id) { table_id_ = id; }
  inline void set_column_id(const uint64_t id) { column_id_ = id; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline void set_rowkey_position(const int64_t rowkey_position) { rowkey_position_ = rowkey_position; }
  inline void set_order_in_rowkey(const common::ObOrderType order_in_rowkey) { order_in_rowkey_ = order_in_rowkey; }
  inline void set_udt_set_id(const uint64_t id) { udt_set_id_ = id; }
  inline void set_sub_data_type(const uint64_t sub_type) { sub_type_ = sub_type; }

  int set_part_key_pos(const int64_t part_key_pos);
  inline int64_t get_part_key_pos() const
  { return part_pos_.part_key_pos_; }

  int set_subpart_key_pos(const int64_t subpart_key_pos);
  inline int64_t get_subpart_key_pos() const
  { return part_pos_.subpart_key_pos_; }

  inline void set_not_part_key()
  { tbl_part_key_pos_ = 0; del_column_flag(TABLE_PART_KEY_COLUMN_FLAG); }
  inline void set_tbl_part_key_pos(const int64_t tbl_part_key_pos)
  {
    tbl_part_key_pos_ = tbl_part_key_pos;
    if (0 != tbl_part_key_pos_) {
      add_column_flag(TABLE_PART_KEY_COLUMN_FLAG);
    }
  }
  inline int64_t get_tbl_part_key_pos() const
  { return tbl_part_key_pos_; }

  //is table partition key column
  inline bool is_tbl_part_key_column() const { return tbl_part_key_pos_ > 0; }
  //is table PARTITION_LEVEL_ONE partition key column
  inline bool is_part_key_column() const { return part_pos_.part_key_pos_ > 0; }
  //is table PARTITION_LEVEL_TWO partition key column
  inline bool is_subpart_key_column() const { return part_pos_.subpart_key_pos_ > 0; }
  bool is_prefix_column() const;
  bool is_func_idx_column() const;
  inline void set_data_type(const common::ColumnType type) { meta_type_.set_type(type); }
  inline void set_meta_type(const common::ObObjMeta type) { meta_type_ = type; }
  inline void set_accuracy(const common::ObAccuracy &accuracy) { accuracy_ = accuracy; }
  inline void set_data_length(const int64_t length) { set_data_length(static_cast<int32_t>(length)); } //TODO(xiaofeng.lby): fix it later
  inline void set_data_length(const int32_t length) { accuracy_.set_length(length); }
  inline void set_data_precision(const int16_t precision) { accuracy_.set_precision(precision); }
  inline void set_length_semantics(const int16_t value) { accuracy_.set_length_semantics(value); }
  inline void set_data_scale(const int16_t data_scale) { accuracy_.set_scale(data_scale); }
  inline void set_zero_fill(const bool is_zero_fill) { is_zero_fill_ = is_zero_fill; }
  inline void set_nullable(const bool is_nullable) { is_nullable_ = is_nullable; }
  // add default not null constraint
  inline void add_not_null_cst();
  inline void add_not_null_cst(const bool rely_flag, const bool enable_flag,
                               const bool validate_flag);
  inline void drop_not_null_cst();
  //inline void set_not_null(const ObColumnSchemaV2 &other);
  //inline void set_new_nullable();
  inline void set_autoincrement(const bool is_autoincrement) { is_autoincrement_ = is_autoincrement; }
  inline void set_is_hidden(const bool is_hidden) { is_hidden_ = is_hidden; }
  inline void set_charset_type(const common::ObCharsetType type) { charset_type_ = type; }
  inline void set_index_position(const int64_t index_position) { index_position_ = index_position; }
  // The following two functions is not used for juding if column's type is binary.
  inline void set_binary_collation(const bool is_binary) { is_binary_collation_ = is_binary; }
  inline bool is_binary_collation() const  { return is_binary_collation_; }
  inline void set_collation_type(const common::ObCollationType type) { meta_type_.set_collation_type(type); }
  inline int set_orig_default_value(const common::ObObj default_value) { return deep_copy_obj(default_value, orig_default_value_); }
  inline int set_orig_default_value_v2(const common::ObObj default_value) { return deep_copy_obj(default_value, orig_default_value_); }
  inline int set_cur_default_value(const common::ObObj default_value) { return deep_copy_obj(default_value, cur_default_value_); }
  inline int set_cur_default_value_v2(const common::ObObj default_value) { return deep_copy_obj(default_value, cur_default_value_); }
  inline int set_column_name(const char *col_name) { return deep_copy_str(col_name, column_name_); }
  inline int set_column_name(const common::ObString &col_name) { return deep_copy_str(col_name, column_name_); }
  inline int set_comment(const char *comment) { return deep_copy_str(comment, comment_); }
  inline int set_comment(const common::ObString &comment) { return deep_copy_str(comment, comment_); }
  inline void set_on_update_current_timestamp(const bool is_on_update_for_timestamp) { on_update_current_timestamp_ = is_on_update_for_timestamp; }
  int set_extended_type_info(const common::ObIArray<common::ObString> &info);
  int add_type_info(const common::ObString &type_info);
  inline void set_prev_column_id(const uint64_t id) { prev_column_id_ = id; }
  inline void set_next_column_id(const uint64_t id) { next_column_id_ = id; }
  inline void set_sequence_id(uint64_t sequence_id) { sequence_id_ = sequence_id; }
  inline void set_encoding_type(const int64_t type) { encoding_type_ = type; }
  int add_cascaded_column_id(uint64_t column_id);
  int del_cascaded_column_id(const uint64_t column_id);
  bool has_cascaded_column_id(uint64_t column_id) const;
  int get_cascaded_column_ids(common::ObIArray<uint64_t> &column_ids) const;
  const ColumnReferenceSet *get_column_ref_set() const { return column_ref_idxs_; }
  inline void set_srs_id(const uint64_t srs_id) { srs_id_ = srs_id; } // srs_id_ = struct{geotype + reserved + srid}
  inline void set_geo_col_id(const uint64_t col_id) { geo_col_id_ = col_id; }
  inline void set_srid(const uint32_t srid) { srs_info_.srid_ = srid; }
  inline void set_geo_type(const common::ObGeoType geo_type) { srs_info_.geo_type_ = static_cast<uint8_t>(geo_type); }
  int set_geo_type(const int32_t type_val);
  //get methods
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_table_id() const { return table_id_; }
  inline uint64_t get_column_id() const { return column_id_; }
  inline uint64_t& get_column_id() { return column_id_; }
  inline uint64_t get_udt_set_id() const { return udt_set_id_; }
  inline uint64_t get_sub_data_type() const { return sub_type_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline int64_t get_rowkey_position() const { return rowkey_position_; }
  inline int64_t get_index_position() const { return index_position_; }
  inline common::ObOrderType get_order_in_rowkey() const { return order_in_rowkey_; }
  inline common::ColumnType get_data_type() const { return meta_type_.get_type(); }
  inline common::ColumnTypeClass get_data_type_class() const { return meta_type_.get_type_class(); }
  inline common::ObObjMeta get_meta_type() const { return meta_type_; }
  inline const common::ObAccuracy &get_accuracy() const { return accuracy_; }
  inline int32_t get_data_length() const;
  inline static int32_t get_data_length(const common::ObAccuracy &accuracy,
                                        const common::ObObjMeta &meta_type);
  inline int16_t get_data_precision() const { return accuracy_.get_precision(); }
  inline int16_t get_data_scale() const { return accuracy_.get_scale(); }
  inline int16_t get_length_semantics() const { return accuracy_.get_length_semantics();}
  inline uint64_t get_srs_id() const { return srs_id_; } // srs_id_ = struct{geotype + reserved + srid}
  inline uint64_t get_geo_col_id() const { return geo_col_id_; }
  inline uint32_t get_srid() const { return srs_info_.srid_; }
  inline common::ObGeoType get_geo_type() const { return static_cast<common::ObGeoType>(srs_info_.geo_type_); }
  // Be careful with this interface, is_nullable_ is set only in Mysql mode and
  // for primary key and identity column in Oracle mode.
	// is_nullable() is usually used in Mysql mode, also used when schema interacts with inner table.
  // Following function is_not_null_for_read and is_not_null_for_write is more practical.
	// More info:
  inline bool is_nullable() const { return is_nullable_; }
  inline bool is_zero_fill() const { return is_zero_fill_; }
  inline bool is_autoincrement() const { return is_autoincrement_; }
  inline bool is_hidden() const { return is_hidden_; }
  inline bool is_string_type() const { return meta_type_.is_string_type(); }
  inline bool is_json() const { return meta_type_.is_json(); }
  inline bool is_geometry() const { return meta_type_.is_geometry(); }
  inline bool is_raw() const { return meta_type_.is_raw(); }

  inline bool is_xmltype() const {
    return ((meta_type_.is_ext() || meta_type_.is_user_defined_sql_type()) && sub_type_ == T_OBJ_XML)
           || meta_type_.is_xml_sql_type();
  }
  inline bool is_extend() const { return meta_type_.is_ext() || meta_type_.is_user_defined_sql_type(); }
  inline bool is_udt_hidden_column() const { return get_udt_set_id() > 0 && is_hidden(); }
  inline bool is_udt_related_column() const { return is_extend() || is_udt_hidden_column(); }
  inline common::ObCharsetType get_charset_type() const { return charset_type_; }
  inline common::ObCollationType get_collation_type() const { return meta_type_.get_collation_type(); }
  inline const common::ObObj &get_orig_default_value()  const { return orig_default_value_; }
  inline const common::ObObj &get_cur_default_value()  const { return cur_default_value_; }
  inline common::ObObj &get_cur_default_value() { return cur_default_value_; }
  inline const char *get_column_name() const { return extract_str(column_name_); }
  inline const common::ObString &get_column_name_str() const { return column_name_; }
  inline const char *get_comment() const {return extract_str(comment_);}
  inline const common::ObString &get_comment_str() const { return comment_; }
  inline const common::ObIArray<common::ObString> &get_extended_type_info() const { return extended_type_info_; }
  inline common::ObIArray<common::ObString> &get_extended_type_info() { return extended_type_info_; }
  inline uint64_t get_prev_column_id() const { return prev_column_id_; }
  inline uint64_t get_next_column_id() const { return next_column_id_; }
  inline uint64_t get_sequence_id() const { return sequence_id_; }
  inline int64_t get_encoding_type() const { return encoding_type_; }

  inline int64_t get_cte_generate_column_projector_offset() const { return get_column_id() - common::OB_APP_MIN_COLUMN_ID;}
  // true: primary key/hidden primary key(pk_increment/cluster_id/parition_id)/partitioned key of no-pk tables
  inline bool is_rowkey_column() const { return rowkey_position_ > 0; }
  // true: primary key/hidden primary key(pk_increment/cluster_id/parition_id)
  // false: ordinary column/partitioned key of no-pk tables
  inline bool is_original_rowkey_column() const
  {
    return rowkey_position_ > 0 && !is_heap_alter_rowkey_column();
  }
  inline bool is_index_column() const { return index_position_ > 0; }
  inline bool is_spatial_index_column() const { return has_generated_column_deps() && is_geometry(); }
  // true:
  //  - columns which are stored in SSTable
  //  - generated columns which are using for partitioned key of no-pk tables
  // false: generated columns or ROWID column
  inline bool is_column_stored_in_sstable() const
  { return !(is_virtual_generated_column() && !is_heap_alter_rowkey_column()); }
  inline bool is_virtual_generated_column() const { return column_flags_ & VIRTUAL_GENERATED_COLUMN_FLAG; }
  inline bool is_stored_generated_column() const { return column_flags_ & STORED_GENERATED_COLUMN_FLAG; }
  inline bool is_always_identity_column() const { return column_flags_ & ALWAYS_IDENTITY_COLUMN_FLAG; }
  inline bool is_default_identity_column() const { return column_flags_ & DEFAULT_IDENTITY_COLUMN_FLAG; }
  inline bool is_default_on_null_identity_column() const { return column_flags_ & DEFAULT_ON_NULL_IDENTITY_COLUMN_FLAG; }
  inline bool is_cte_generated_column() const { return column_flags_ & CTE_GENERATED_COLUMN_FLAG; }
  inline bool is_default_expr_v2_column() const { return column_flags_ & DEFAULT_EXPR_V2_COLUMN_FLAG; }
  inline bool is_generated_column() const { return (is_virtual_generated_column() || is_stored_generated_column()); }
  inline bool is_identity_column() const { return is_always_identity_column() || is_default_identity_column() || is_default_on_null_identity_column(); }
  inline bool is_generated_column_using_udf() const { return /*is_generated_column() && global index table clean the virtual gen col flag*/ !!(column_flags_ & GENERATED_COLUMN_UDF_EXPR); }
  // to check whether storing column in index table is specified by user.
  inline bool is_user_specified_storing_column() const { return column_flags_ & USER_SPECIFIED_STORING_COLUMN_FLAG; }
  inline bool is_default_srid() const { return UINT32_MAX == srs_info_.srid_; }
  inline void set_column_flags(int64_t flags) { column_flags_ = flags; }
  inline void erase_generated_column_flags()
  {
    del_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
    del_column_flag(STORED_GENERATED_COLUMN_FLAG);
    del_column_flag(GENERATED_COLUMN_UDF_EXPR);
  }
  inline void erase_identity_column_flags()
  {
    del_column_flag(ALWAYS_IDENTITY_COLUMN_FLAG);
    del_column_flag(DEFAULT_IDENTITY_COLUMN_FLAG);
    del_column_flag(DEFAULT_ON_NULL_IDENTITY_COLUMN_FLAG);
  }
  inline bool is_fulltext_column() const { return column_flags_ & GENERATED_CTXCAT_CASCADE_FLAG; }
  inline bool is_spatial_generated_column() const { return column_flags_ & SPATIAL_INDEX_GENERATED_COLUMN_FLAG; }
  inline bool is_spatial_cellid_column() const { return is_spatial_generated_column() && get_data_type() == common::ObUInt64Type; }
  inline bool has_generated_column_deps() const { return column_flags_ & GENERATED_DEPS_CASCADE_FLAG; }
  inline bool is_primary_vp_column() const { return column_flags_ & PRIMARY_VP_COLUMN_FLAG; }
  inline bool is_aux_vp_column() const { return column_flags_ & AUX_VP_COLUMN_FLAG; }
  inline bool is_invisible_column() const { return column_flags_ & INVISIBLE_COLUMN_FLAG; }
  inline bool is_label_se_column() const { return column_flags_ & LABEL_SE_COLUMN_FLAG; }
  inline bool has_column_flag(int64_t flag) const { return column_flags_ & flag; }
  inline int64_t get_column_flags() const { return column_flags_; }
  //these column flags must be stored in __all_column
  inline int64_t get_stored_column_flags() const { return column_flags_ & STORED_COLUMN_FLAGS_MASK; }
  inline bool is_heap_alter_rowkey_column() const {return column_flags_ & HEAP_ALTER_ROWKEY_FLAG; }
  inline bool is_not_null_for_read() const {
    return !is_nullable_ || is_not_null_rely_column() || is_not_null_validate_column();
  }
  inline bool is_not_null_for_write() const {
    return !is_nullable_ || is_not_null_enable_column();
  }
  inline bool has_not_null_constraint() const { return column_flags_ & COLUMN_NOT_NULL_CONSTRAINT_FLAG; }
  // for schema printer
  inline bool is_not_null_rely_column() const
  { return has_not_null_constraint() && (column_flags_ & NOT_NULL_RELY_FLAG); }
  inline bool is_not_null_enable_column() const
  { return has_not_null_constraint() && (column_flags_ & NOT_NULL_ENABLE_FLAG); }
  // for schema printer and desc
  inline bool is_not_null_validate_column() const
  { return has_not_null_constraint() && (column_flags_ & NOT_NULL_VALIDATE_FLAG); }

  inline void add_column_flag(int64_t flag) { column_flags_ |= flag; }
  inline void del_column_flag(int64_t flag) { column_flags_ &= ~flag; }
  inline void add_or_del_column_flag(int64_t flag, bool is_add);
  inline bool is_shadow_column() const { return column_id_ > common::OB_MIN_SHADOW_COLUMN_ID; }
  inline bool is_on_update_current_timestamp() const { return on_update_current_timestamp_; }
  inline bool is_enum_or_set() const { return meta_type_.is_enum_or_set(); }

  inline static bool is_hidden_pk_column_id(const uint64_t column_id);

  //other methods
  int64_t get_convert_size(void) const;
  void reset();
  void print_info() const;
  void print(FILE *fd) const;
  int get_byte_length(
      int64_t &length,
      const bool is_oracle_mode,
      const bool for_check_length) const;

  int convert_column_id(const hash::ObHashMap<uint64_t, uint64_t> &column_id_map);

  int serialize_extended_type_info(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_extended_type_info(const char *buf, const int64_t data_len, int64_t &pos);

  int get_vp_table_ids(uint64_t *vp_tid_array, int64_t &vp_cnt) const
  {
    int ret = common::OB_SUCCESS;
    // Column can't be shared by multi vertical-partitioned tables,
    // so we assume each column has only one vp table at most.
    vp_cnt = 1;
    vp_tid_array[0] = table_id_;
    return ret;
  }

  DECLARE_VIRTUAL_TO_STRING;
private:
  int alloc_column_ref_set();
private:
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t column_id_;
  int64_t schema_version_;
  int64_t rowkey_position_;  // greater than zero if this is rowkey column, 0 if this is common column
  int64_t index_position_;  // greater than zero if this is index column
  common::ObOrderType order_in_rowkey_; // asc or desc; if this column is index column this means order in index
  union {
    struct {
      uint64_t part_key_pos_:8;//partition key pos
      uint64_t subpart_key_pos_:8;//subpartition key pos
    } part_pos_;//partition key
    int64_t tbl_part_key_pos_;
  };//greater than zero if this column is used to calc part expr
  common::ObObjMeta meta_type_;
  common::ObAccuracy accuracy_;
  bool is_nullable_;
  bool is_zero_fill_;
  bool is_autoincrement_;
  bool is_hidden_;
  int64_t column_flags_;
  ColumnReferenceSet *column_ref_idxs_;
  common::ObCharsetType charset_type_;//default:utf8mb4
  bool is_binary_collation_;
  bool on_update_current_timestamp_;
  //default value
  common::ObObj orig_default_value_;//first default value, used for alter table add column; collation must be same with the column
  common::ObObj cur_default_value_; //collation must be same with the column
  common::ObString column_name_;
  common::ObString comment_;
  common::ObArrayHelper<common::ObString> extended_type_info_;//used for enum and set
  uint64_t prev_column_id_;
  uint64_t next_column_id_;
  int64_t encoding_type_; // for test, no need serialization
  uint64_t sequence_id_; // for identity column, used for find a sequence schema; store in orig_default_value_
  union {
    struct {
      uint32_t geo_type_ : 5;
      uint32_t reserved_: 27;
      uint32_t srid_ : 32;
    } srs_info_;
    uint64_t geo_col_id_;
    uint64_t srs_id_;
  };
  uint64_t udt_set_id_;
  uint64_t sub_type_;
};

inline int32_t ObColumnSchemaV2::get_data_length() const
{
  return get_data_length(accuracy_, meta_type_);
}

inline int32_t ObColumnSchemaV2::get_data_length(const common::ObAccuracy &accuracy,
    const common::ObObjMeta &meta_type)
{
  return (ob_is_accuracy_length_valid_tc(meta_type.get_type()) ?
      accuracy.get_length() : ob_obj_type_size(meta_type.get_type()));
}

inline bool ObColumnSchemaV2::is_hidden_pk_column_id(const uint64_t column_id)
{
  return common::OB_HIDDEN_PK_INCREMENT_COLUMN_ID == column_id;
}

inline void ObColumnSchemaV2::add_or_del_column_flag(int64_t flag, bool is_add)
{
  if (is_add) {
    add_column_flag(flag);
  } else {
    del_column_flag(flag);
  }
}

inline void ObColumnSchemaV2::add_not_null_cst()
{
  add_column_flag(COLUMN_NOT_NULL_CONSTRAINT_FLAG);
  del_column_flag(NOT_NULL_RELY_FLAG);
  add_column_flag(NOT_NULL_ENABLE_FLAG);
  add_column_flag(NOT_NULL_VALIDATE_FLAG);
}

inline void ObColumnSchemaV2::add_not_null_cst(const bool rely_flag, const bool enable_flag,
                                               const bool validate_flag)
{
  add_column_flag(COLUMN_NOT_NULL_CONSTRAINT_FLAG);
  add_or_del_column_flag(NOT_NULL_RELY_FLAG, rely_flag);
  add_or_del_column_flag(NOT_NULL_ENABLE_FLAG, enable_flag);
  add_or_del_column_flag(NOT_NULL_VALIDATE_FLAG, validate_flag);
}

inline void ObColumnSchemaV2::drop_not_null_cst()
{
  del_column_flag(COLUMN_NOT_NULL_CONSTRAINT_FLAG);
  del_column_flag(NOT_NULL_RELY_FLAG);
  del_column_flag(NOT_NULL_ENABLE_FLAG);
  del_column_flag(NOT_NULL_VALIDATE_FLAG);
}

inline static bool column_schema_compare(const ObColumnSchemaV2 &lhs, const ObColumnSchemaV2 &rhs)
{
  bool ret = false;
  if ((lhs.get_table_id() < rhs.get_table_id())
      || (lhs.get_table_id() == rhs.get_table_id() && lhs.get_column_id() < rhs.get_column_id())) {
    ret = true;
  }
  return ret;
}

struct ObColumnSchemaV2Compare
{
  inline bool operator()(const ObColumnSchemaV2 &lhs, const ObColumnSchemaV2 &rhs);
};

inline bool ObColumnSchemaV2Compare::operator()(const ObColumnSchemaV2 &lhs,
                                                const ObColumnSchemaV2 &rhs)
{
  return column_schema_compare(lhs, rhs);
}

struct ObColumnSchemaIndexCompare
{
  ObColumnSchemaIndexCompare(const ObColumnSchemaV2 *columns)
      : columns_(columns)
  {}
  inline bool operator()(const int64_t &lhs, const int64_t &rhs);
  const ObColumnSchemaV2 *columns_;
};

inline bool ObColumnSchemaIndexCompare::operator()(const int64_t &lhs, const int64_t &rhs)
{
  return column_schema_compare(columns_[lhs], columns_[rhs]);
}

}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase
#endif //OCEANBASE_SCHEMA_COLUMN_SCHEMA_H_
