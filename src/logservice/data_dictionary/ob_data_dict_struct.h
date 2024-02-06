/**
* Copyright (c) 2022 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*
* Data Dict Struct Define
* This file defines Struct of Data Dict
*
* NOTICE:
* Invoker of DataDictStruct should:
*   - construct DataDictStruct with an allocator
*     and make sure won't reset the allocator while the dict_struct is in use.
*   - initialize dict_struct by invoke init interface of each DictStruct
*/

#ifndef  OCEANBASE_DICT_SERVICE_META_DICT_STRUCT_
#define  OCEANBASE_DICT_SERVICE_META_DICT_STRUCT_

#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h" // share::ObLSID
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "share/ls/ob_ls_operator.h"  // ObLSAttr
#include "share/scn.h"

#include "ob_data_dict_utils.h"

#define NEED_SERIALIZE_AND_DESERIALIZE_DICT \
  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const; \
  int deserialize(const ObDictMetaHeader &header, const char* buf, const int64_t data_len, int64_t& pos); \
  int64_t get_serialize_size(void) const

namespace oceanbase
{
namespace common
{
class ObRowkeyInfo;
}
namespace share
{
namespace schema
{
class ObColumnSchemaV2;
class ObTableSchema;
}
}

namespace datadict
{

enum ObDictMetaType : uint8_t
{
  INVALID_META = 0,
  TENANT_META,
  DATABASE_META,
  TABLE_META,
  MAX_META
};

enum ObDictMetaStorageType : uint8_t
{
  INVALID = 0,
  FULL,
  FIRST,
  MIDDLE,
  LAST
};

class ObDictMetaHeader
{
  OB_UNIS_VERSION(1);
public:
  ObDictMetaHeader();
  ObDictMetaHeader(const ObDictMetaType &meta_type);
  virtual ~ObDictMetaHeader() { reset(); }
public:
  // NOTICE: update DEFAULT_VERSION if modify serialized fields in DictxxxMeta
  // update to 2 in 4.1 bp1: add column_ref_ids_ in ObDictColumnMeta
  // update to 3 in 4.2: add udt_set_id_ and sub_type_ in ObDictColumnMeta
  const static int64_t DEFAULT_VERSION = 3;
public:
  OB_INLINE bool is_valid() const
  {
    // won't check snapshot_scn because snapshot_scn is not valid if meta is generate by ddl_service
    return ObDictMetaType::INVALID_META < meta_type_
        && ObDictMetaStorageType::INVALID < storage_type_
        && 0 < dict_serialized_length_;
  }
  void reset();
  bool operator==(const ObDictMetaHeader &other) const;
public:
  OB_INLINE int64_t get_version() const { return version_; }
  OB_INLINE const ObDictMetaType &get_dict_meta_type() const { return meta_type_; }
  OB_INLINE void set_snapshot_scn(const share::SCN &snapshot_scn) { snapshot_scn_ = snapshot_scn; }
  OB_INLINE const share::SCN &get_snapshot_scn() const { return snapshot_scn_; }
  OB_INLINE void set_storage_type(const ObDictMetaStorageType &storage_type) { storage_type_ = storage_type; }
  OB_INLINE const ObDictMetaStorageType &get_storage_type() const { return storage_type_; }
  OB_INLINE void set_dict_serialize_length(int64_t serialized_length) { dict_serialized_length_ = serialized_length; }
  OB_INLINE int64_t get_dict_serialized_length() const { return dict_serialized_length_; }
  TO_STRING_KV(
      K_(version),
      K_(snapshot_scn),
      K_(meta_type),
      K_(storage_type),
      K_(dict_serialized_length));
private:
  int16_t version_;
  share::SCN snapshot_scn_;
  ObDictMetaType meta_type_;
  ObDictMetaStorageType storage_type_;
  int64_t dict_serialized_length_;
};

class ObDictTenantMeta
{
public:
  // allocator should keep memory for meta until meta is not in use anymore
  explicit ObDictTenantMeta(ObIAllocator *allocator);
  virtual ~ObDictTenantMeta() { reset(); }
  void reset(); // won't reset allocator
  bool operator==(const ObDictTenantMeta &other) const;
  ObDictTenantMeta &operator=(const ObDictTenantMeta &src_schema) = delete;

public:
  // For schema_service (without ls_info)
  int init(const share::schema::ObTenantSchema &tenant_schema);
  // For data_dict_service
  int init_with_ls_info(
      const share::schema::ObTenantSchema &tenant_schema,
      const share::ObLSArray &ls_array);
  // For incremental data update
  int incremental_data_update(const ObDictTenantMeta &new_tenant_meta);
  int incremental_data_update(const share::ObLSAttr &ls_attr);

public:
  // for user like OBCDC.
  OB_INLINE bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_
        && OB_INVALID_VERSION != schema_version_
        && ! tenant_name_.empty();
  }
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE const char *get_tenant_name() const { return extract_str(tenant_name_); }
  OB_INLINE int64_t get_schema_version() const { return schema_version_; }
  OB_INLINE common::ObCompatibilityMode get_compatibility_mode() const { return compatibility_mode_; }
  OB_INLINE share::schema::ObTenantStatus get_status() const { return tenant_status_; }
  OB_INLINE bool is_normal() const { return share::schema::TENANT_STATUS_NORMAL == tenant_status_; }
  OB_INLINE bool is_creating() const { return share::schema::TENANT_STATUS_CREATING == tenant_status_; }
  OB_INLINE bool is_dropping() const { return share::schema::TENANT_STATUS_DROPPING == tenant_status_; }
  OB_INLINE bool is_restore() const { return share::schema::TENANT_STATUS_RESTORE == tenant_status_
                                             || share::schema::TENANT_STATUS_CREATING_STANDBY == tenant_status_; }
  OB_INLINE common::ObCharsetType get_charset_type() const { return charset_type_; }
  OB_INLINE common::ObCollationType get_collation_type() const { return collation_type_; }
  OB_INLINE int64_t get_drop_tenant_time() const { return drop_tenant_time_; }
  OB_INLINE bool is_in_recyclebin() const { return in_recyclebin_; }
  OB_INLINE const share::ObLSArray &get_ls_array() const { return ls_arr_; }

  NEED_SERIALIZE_AND_DESERIALIZE_DICT;
  TO_STRING_KV(
      K_(tenant_id),
      K_(schema_version),
      K_(tenant_name),
      K_(tenant_status),
      K_(in_recyclebin),
      K_(ls_arr));

private:
  ObIAllocator *allocator_;
  // Won't serialize tenant_id in dict.
  // DATADICT of StandBy is the same with Promary tenant. However tenant_id of standby tenant may not consist with primary tenant
  //
  // OBCDC will set tenant_id when consume and replay DATADICT
  // Anyother consumer of DATADICT should also notice this feature.
  uint64_t tenant_id_;
  int64_t schema_version_;
  common::ObString tenant_name_;
  common::ObCompatibilityMode compatibility_mode_; //创建后不可修改
  share::schema::ObTenantStatus tenant_status_;
  common::ObCharsetType charset_type_;
  common::ObCollationType collation_type_;
  int64_t drop_tenant_time_;
  bool in_recyclebin_;
  share::ObLSArray ls_arr_;
};

class ObDictDatabaseMeta
{
public:
  ObDictDatabaseMeta(ObIAllocator *allocator);
  virtual ~ObDictDatabaseMeta() { reset(); }
  void reset();
  bool operator==(const ObDictDatabaseMeta &other) const;
  int assign(const ObDictDatabaseMeta &src_database_meat);

public:
  int init(const share::schema::ObDatabaseSchema &database_schema);

public:
  // for user like OBCDC
  OB_INLINE bool is_valid() const
  {
    return OB_INVALID_ID != database_id_
        && OB_INVALID_VERSION != schema_version_
        && ! database_name_.empty();
  }
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE uint64_t get_database_id() const { return database_id_; }
  OB_INLINE int64_t get_schema_version() const { return schema_version_; }
  OB_INLINE const char *get_database_name() const { return extract_str(database_name_); }
  OB_INLINE const common::ObString &get_database_name_str() const { return database_name_; }
  OB_INLINE common::ObCharsetType get_charset_type() const { return charset_type_; }
  OB_INLINE common::ObCollationType get_collation_type() const { return collation_type_; }
  OB_INLINE bool is_in_recyclebin() const { return in_recyclebin_; }
  OB_INLINE common::ObNameCaseMode get_name_case_mode() const { return name_case_mode_; }

  NEED_SERIALIZE_AND_DESERIALIZE_DICT;
  TO_STRING_KV(
      K_(tenant_id),
      K_(database_id),
      K_(schema_version),
      K_(database_name),
      K_(in_recyclebin));

private:
  template<class DATABASE_SCHEMA>
  int assign_(DATABASE_SCHEMA &src_database_meta);

private:
  ObIAllocator *allocator_;
  // Won't serialize tenant_id in dict.
  // DATADICT of StandBy is the same with Promary tenant. However tenant_id of standby tenant may not consist with primary tenant
  //
  // OBCDC will set tenant_id when consume and replay DATADICT
  // Anyother consumer of DATADICT should also notice this feature.
  uint64_t tenant_id_;
  uint64_t database_id_;
  int64_t schema_version_;
  // OB_MAX_DATABASE_NAME_LENGTH
  common::ObString database_name_;
  common::ObCharsetType charset_type_;//default:utf8mb4
  common::ObCollationType collation_type_;//default:utf8mb4_general_ci
  common::ObNameCaseMode name_case_mode_;
  bool in_recyclebin_;
};

class ObDictColumnMeta
{
public:
  ObDictColumnMeta(ObIAllocator *allocator);
  virtual ~ObDictColumnMeta() { reset(); }
  void reset();
  bool operator==(const ObDictColumnMeta &other) const;
  int init(const share::schema::ObColumnSchemaV2 &column_schema);
  int assign(const ObDictColumnMeta &src_column_meta);

public:
  // for user like OBCDC
  OB_INLINE uint64_t get_column_id() const { return column_id_; }
  OB_INLINE const char *get_column_name() const { return extract_str(column_name_); }
  OB_INLINE const common::ObString &get_column_name_str() const { return column_name_; }
  OB_INLINE common::ObCharsetType get_charset_type() const { return charset_type_; }
  OB_INLINE common::ObCollationType get_collation_type() const { return collation_type_; }
  int64_t get_data_length() const;
  OB_INLINE common::ColumnType get_data_type() const { return meta_type_.get_type(); }
  OB_INLINE common::ColumnTypeClass get_data_type_class() const { return meta_type_.get_type_class(); }
  OB_INLINE common::ObObjMeta get_meta_type() const { return meta_type_; }
  OB_INLINE const common::ObAccuracy &get_accuracy() const { return accuracy_; }
  OB_INLINE int16_t get_data_scale() const { return accuracy_.get_scale(); }
  OB_INLINE int16_t get_data_precision() const { return accuracy_.get_precision(); }
  OB_INLINE const common::ObIArray<common::ObString> &get_extended_type_info() const { return extended_type_info_; }
  OB_INLINE int64_t get_rowkey_position() const { return rowkey_position_; }
  OB_INLINE int64_t get_index_position() const { return index_position_; }
  OB_INLINE const common::ObObj &get_orig_default_value()  const { return orig_default_value_; }
  OB_INLINE const common::ObObj &get_cur_default_value()  const { return cur_default_value_; }
  OB_INLINE bool is_nullable() const { return is_nullable_; }
  OB_INLINE bool is_zero_fill() const { return is_zero_fill_; }
  OB_INLINE bool is_autoincrement() const { return is_autoincrement_; }
  OB_INLINE bool is_hidden() const { return is_hidden_; }
  OB_INLINE bool is_tbl_part_key_column() const { return is_part_key_col_; }
  OB_INLINE bool is_rowkey_column() const { return rowkey_position_ > 0; }
  OB_INLINE bool is_index_column() const { return index_position_ > 0; }
  OB_INLINE bool is_enum_or_set() const { return meta_type_.is_enum_or_set(); }
  OB_INLINE int64_t get_column_flags() const { return column_flags_; }
  OB_INLINE bool is_invisible_column() const { return column_flags_ & INVISIBLE_COLUMN_FLAG; }
  OB_INLINE bool is_heap_alter_rowkey_column() const { return column_flags_ & HEAP_ALTER_ROWKEY_FLAG; }
  OB_INLINE bool is_original_rowkey_column() const { return is_rowkey_column() && ! is_heap_alter_rowkey_column(); }
  OB_INLINE bool is_virtual_generated_column() const { return column_flags_ & VIRTUAL_GENERATED_COLUMN_FLAG; }
  OB_INLINE bool is_stored_generated_column() const { return column_flags_ & STORED_GENERATED_COLUMN_FLAG; }
  OB_INLINE bool is_generated_column() const { return is_virtual_generated_column() || is_stored_generated_column(); }
  OB_INLINE bool is_shadow_column() const { return column_id_ > common::OB_MIN_SHADOW_COLUMN_ID; }
  OB_INLINE bool has_generated_column_deps() const { return column_flags_ & GENERATED_DEPS_CASCADE_FLAG; }
  int get_cascaded_column_ids(ObIArray<uint64_t> &column_ids) const;

  OB_INLINE uint64_t get_udt_set_id() const { return udt_set_id_; }
  OB_INLINE uint64_t get_sub_data_type() const { return sub_type_; }
  OB_INLINE bool is_udt_column() const { return udt_set_id_ > 0 && OB_INVALID_ID != udt_set_id_; }
  OB_INLINE bool is_udt_hidden_column() const { return is_udt_column() && is_hidden(); }
  OB_INLINE bool is_xmltype() const {
    return is_udt_column()
        && (((meta_type_.is_ext() || meta_type_.is_user_defined_sql_type()) && sub_type_ == T_OBJ_XML)
            || meta_type_.is_xml_sql_type());
  }

  NEED_SERIALIZE_AND_DESERIALIZE_DICT;
  TO_STRING_KV(
      K_(column_id),
      K_(column_name),
      K_(colulmn_properties),
      K_(column_flags));

private:
  int deep_copy_default_val_(const ObObj &src_default_val, ObObj &dest_default_val);
  template<class COLUMN_SCHEMA>
  int assign_(COLUMN_SCHEMA &src_column_meta);

private:
  // for uint32_t union
  static const int8_t NULLABLE_BIT = 1;
  static const int8_t ZERO_FILL_BIT= 1;
  static const int8_t AUTO_INC_BIT = 1;
  static const int8_t HIDDEN_BIT = 1;
  static const int8_t PART_KEY_BIT = 1;
  static const int8_t RESERVE_BIT = 27;
private:
  ObIAllocator *allocator_;
  uint64_t column_id_;
  common::ObString column_name_;
  int64_t rowkey_position_;  // greater than zero if this is rowkey column, 0 if this is common column
  int64_t index_position_;  // greater than zero if this is index column
  common::ObObjMeta meta_type_;
  common::ObAccuracy accuracy_;
  union {
    uint32_t colulmn_properties_;
    struct{
      uint32_t is_nullable_       : NULLABLE_BIT;
      uint32_t is_zero_fill_      : ZERO_FILL_BIT;
      uint32_t is_autoincrement_  : AUTO_INC_BIT;
      uint32_t is_hidden_         : HIDDEN_BIT;
      uint32_t is_part_key_col_   : PART_KEY_BIT;
      uint32_t reserved_          : RESERVE_BIT;
    };
  };
  int64_t column_flags_;
  common::ObCharsetType charset_type_;//default:utf8mb4
  common::ObCollationType collation_type_;
  // default value
  // change value of ObObj won't affect operator==, please refer operator== of ObObj for detail.
  common::ObObj orig_default_value_;//first default value, used for alter table add column; collation must be same with the column
  common::ObObj cur_default_value_; //collation must be same with the column
  common::ObSEArray<common::ObString, 8> extended_type_info_;//used for enum and set
  common::ObSEArray<uint64_t, 2> column_ref_ids_;
  uint64_t udt_set_id_;
  uint64_t sub_type_;
};

class ObDictTableMeta
{
public:
  ObDictTableMeta(ObIAllocator *allocator);
  virtual ~ObDictTableMeta() { reset(); }
  void reset();
  bool operator==(const ObDictTableMeta &other) const;

public:
  int init(const share::schema::ObTableSchema &table_schema);
  int assign(const ObDictTableMeta &src_table_meta);

public:
  // for user like OBCDC
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE uint64_t get_database_id() const { return database_id_; }
  OB_INLINE uint64_t get_table_id() const { return table_id_; }
  OB_INLINE const char *get_table_name() const { return extract_str(table_name_); }
  OB_INLINE const common::ObString &get_table_name_str() const { return table_name_; }
  OB_INLINE int64_t get_schema_version() const { return schema_version_; }
  OB_INLINE share::schema::ObTableType get_table_type() const { return table_type_; }
  OB_INLINE share::schema::ObTableMode get_table_mode_struct() const { return table_mode_; }
  OB_INLINE common::ObCharsetType get_charset_type() const { return charset_type_; }
  OB_INLINE common::ObCollationType get_collation_type() const { return collation_type_; }
  OB_INLINE common::ObNameCaseMode get_name_case_mode() const { return name_case_mode_; }
  OB_INLINE uint64_t get_aux_lob_meta_tid() const { return aux_lob_meta_tid_; }
  OB_INLINE uint64_t get_aux_lob_piece_tid() const { return aux_lob_piece_tid_; }
  OB_INLINE bool is_in_recyclebin() const { return common::OB_RECYCLEBIN_SCHEMA_ID == database_id_; }
  OB_INLINE bool is_user_table() const { return share::schema::ObTableType::USER_TABLE == table_type_; }
  OB_INLINE bool is_sys_table() const { return share::schema::ObTableType::SYSTEM_TABLE == table_type_; }
  OB_INLINE bool is_user_hidden_table() const { return share::schema::TABLE_STATE_IS_HIDDEN_MASK & table_mode_.state_flag_; }
  OB_INLINE bool is_tmp_table() const
  {
    return share::schema::ObTableType::TMP_TABLE == table_type_
        || share::schema::ObTableType::TMP_TABLE_ORA_TRX == table_type_
        || share::schema::ObTableType::TMP_TABLE_ORA_SESS == table_type_;
  }
  OB_INLINE bool is_aux_lob_meta_table() const { return share::schema::ObTableType::AUX_LOB_META == table_type_; }
  OB_INLINE bool is_aux_lob_piece_table() const { return share::schema::ObTableType::AUX_LOB_PIECE == table_type_; }
  OB_INLINE bool is_aux_lob_table() const { return is_aux_lob_meta_table() || is_aux_lob_piece_table(); }
  OB_INLINE bool is_aux_vp_table() const { return share::schema::ObTableType::AUX_VERTIAL_PARTITION_TABLE == table_type_; }
  OB_INLINE bool is_heap_table() const
  { return share::schema::TOM_HEAP_ORGANIZED == (enum share::schema::ObTableOrganizationMode)table_mode_.organization_mode_; }
  OB_INLINE bool is_vir_table() const { return share::schema::ObTableType::VIRTUAL_TABLE == table_type_; }
  OB_INLINE bool is_view_table() const
  {
    return share::schema::ObTableType::USER_VIEW == table_type_
        || share::schema::ObTableType::SYSTEM_VIEW == table_type_
        || share::schema::ObTableType::MATERIALIZED_VIEW == table_type_;
  }
  OB_INLINE share::schema::ObIndexType get_index_type() const { return index_type_; }
  OB_INLINE bool is_index_table() const { return share::schema::is_index_table(table_type_); }
  OB_INLINE bool is_normal_index() const
  {
    return share::schema::INDEX_TYPE_NORMAL_LOCAL == index_type_
        || share::schema::INDEX_TYPE_NORMAL_GLOBAL == index_type_
        || share::schema::INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE == index_type_;
  }
  OB_INLINE bool is_unique_index() const
  {
    return share::schema::INDEX_TYPE_UNIQUE_LOCAL == index_type_
        || share::schema::INDEX_TYPE_UNIQUE_GLOBAL == index_type_
        || share::schema::INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE == index_type_;
  }
  OB_INLINE bool is_global_normal_index_table() const { return share::schema::INDEX_TYPE_NORMAL_GLOBAL == index_type_; }
  OB_INLINE bool is_global_unique_index_table() const { return share::schema::INDEX_TYPE_UNIQUE_GLOBAL == index_type_; }
  OB_INLINE bool is_global_index_table() const { return is_global_normal_index_table() || is_global_unique_index_table(); }
  OB_INLINE uint64_t get_data_table_id() const { return data_table_id_; }
  OB_INLINE int64_t get_index_tid_count() const { return unique_index_tid_arr_.count(); } // NOTICE: only return unique_index_table count.
  OB_INLINE const ObIArray<uint64_t> &get_unique_index_table_id_arr() const { return unique_index_tid_arr_; }
  OB_INLINE bool has_tablet() const // refer ObSimpleTableSchemaV2::has_tablet()
  { return ! (is_vir_table() || is_view_table() || is_aux_vp_table() || is_virtual_table(table_id_)); }
  OB_INLINE const common::ObTabletIDArray &get_tablet_ids() const { return tablet_id_arr_; }
  OB_INLINE uint64_t get_association_table_id() const { return association_table_id_; }
  OB_INLINE uint64_t get_max_used_column_id() const { return max_used_column_id_; }
  OB_INLINE const ObIArray<uint64_t> &get_column_id_arr_order_by_table_define() const { return column_id_arr_order_by_table_def_; };
  OB_INLINE int64_t get_column_count() const { return column_count_; }
  OB_INLINE const ObDictColumnMeta *get_column_metas() const { return col_metas_; }
  OB_INLINE int64_t get_index_column_count() const { return index_column_count_; }
  OB_INLINE const common::ObIndexColumn *get_index_cols() const { return index_cols_; }
  OB_INLINE int64_t get_rowkey_column_num() const { return rowkey_column_count_; }
  OB_INLINE const common::ObRowkeyColumn *get_rowkey_cols() const { return rowkey_cols_; }

  // get_rowkey_info
  // NOTICE: rowkey_info may be invalid if rowkey_column_count is zero, should precheck
  // rowkey_column_count and then get_rowkey_info.
  int get_rowkey_info(ObRowkeyInfo &rowkey_info) const;
  int get_index_info(ObIndexInfo &index_info) const;
  // NOTICE: only contains index table id and only unique_index table is included.
  int get_simple_index_infos(ObIArray<share::schema::ObAuxTableMetaInfo> &simple_index_infos_array) const;
  int get_column_ids(ObIArray<share::schema::ObColDesc> &column_ids, const bool no_virtual = false) const;
  // get column_meta for specified column_id.
  // @retval OB_SUCCESS           found column_meta success.
  // @retval OB_ENTRY_NOT_EXIST   column_id may nay exist in column_meta list.
  // @retval OB_ERR_UNEXPECTED    unexpected error.
  int get_column_meta(const uint64_t column_id, const ObDictColumnMeta *&column_meta) const;
  const ObDictColumnMeta *get_column_schema(const uint64_t column_id) const;
public:
  NEED_SERIALIZE_AND_DESERIALIZE_DICT;
  TO_STRING_KV(
      K_(tenant_id),
      K_(database_id),
      K_(table_id),
      K_(schema_version),
      K_(table_name),
      K_(table_type),
      "tablet_count", tablet_id_arr_.count(),
      K_(column_count),
      K_(rowkey_column_count),
      "index_table_count", unique_index_tid_arr_.count(),
      K_(index_column_count),
      K_(index_type)
      );
private:
  template<class TABLE_SCHEMA>
  int assign_(const TABLE_SCHEMA &table_schema);
  int build_index_info_(const share::schema::ObTableSchema &table_schema);
  int build_index_info_(const ObDictTableMeta &src_table_meta);
  int build_rowkey_info_(const share::schema::ObTableSchema &table_schema);
  int build_rowkey_info_(const ObDictTableMeta &src_table_meta);
  int build_column_info_(const share::schema::ObTableSchema &table_schema);
  int build_column_info_(const ObDictTableMeta &src_table_meta);
  int build_column_id_arr_(const share::schema::ObTableSchema &table_schema);

private:
  ObIAllocator *allocator_;
  // Won't serialize tenant_id in dict.
  // DATADICT of StandBy is the same with Promary tenant. However tenant_id of standby tenant may not consist with primary tenant
  //
  // OBCDC will set tenant_id when consume and replay DATADICT
  // Anyother consumer of DATADICT should also notice this feature.
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t table_id_;
  int64_t schema_version_;
  common::ObString table_name_;
  common::ObNameCaseMode name_case_mode_;
  share::schema::ObTableType table_type_;
  share::schema::ObTableMode table_mode_;
  common::ObCharsetType charset_type_;//default:utf8mb4
  common::ObCollationType collation_type_;//default:utf8mb4_general_ci
  common::ObTabletIDArray tablet_id_arr_; // get by ObSimpleTableSchemaV2::get_tablet_ids()
  // for lob table
  uint64_t aux_lob_meta_tid_;
  uint64_t aux_lob_piece_tid_;
  // columns
  // columns in table, which is stored in memtable(e.g. virtual generated column won't be included)
  // order by index in memtable stroage.
  uint64_t max_used_column_id_;
  // column id array, which store column_id order by table_define order(by prev_col_id and next_col_id)
  ObSEArray<uint64_t, 4> column_id_arr_order_by_table_def_;
  int64_t column_count_;
  // RowKey info
  ObDictColumnMeta *col_metas_;
  int64_t rowkey_column_count_;
  common::ObRowkeyColumn *rowkey_cols_;
  // for index table.
  ObSEArray<uint64_t, 8> unique_index_tid_arr_;
  share::schema::ObIndexType index_type_;
  int64_t index_column_count_;
  common::ObIndexColumn *index_cols_;
  uint64_t data_table_id_;
  uint64_t association_table_id_;
};

} // namespace datadict
} // namespace oceanbase

#endif // OCEANBASE_DICT_SERVICE_META_DICT_STRUCT_
