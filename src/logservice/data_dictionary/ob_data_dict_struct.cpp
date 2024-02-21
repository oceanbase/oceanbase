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
* Meta Dict Struct Define
* This file defines Struct of Meta Dict
*/

#include "ob_data_dict_struct.h"

#include "common/rowkey/ob_rowkey_info.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_table_param.h"

#define DEFINE_DESERIALIZE_DATA_DICT(TypeName) \
  int TypeName::deserialize(const ObDictMetaHeader &header, const char* buf, const int64_t data_len, int64_t& pos)

#define PRECHECK_SERIALIZE \
    int ret = OB_SUCCESS; \
    if (OB_ISNULL(buf) \
        || OB_UNLIKELY(buf_len <= 0) \
        || OB_UNLIKELY(pos < 0)) { \
      ret = OB_INVALID_ARGUMENT; \
      DDLOG(WARN, "invalid arguments", KR(ret), K(buf), K(buf_len), K(pos)); \
    }

#define PRECHECK_DESERIALIZE \
    int ret = OB_SUCCESS; \
    if (OB_ISNULL(buf) \
      || OB_ISNULL(allocator_) \
      || OB_UNLIKELY(data_len <= 0) \
      || OB_UNLIKELY(pos < 0) \
      || OB_UNLIKELY(data_len <= pos)) { \
      ret = OB_INVALID_ARGUMENT; \
      DDLOG(WARN, "invalid arguments", KR(ret), K(buf), K(data_len), K(pos), K_(allocator)); \
    } \

#define SEARILIZE_ARRAR(ARRAY_TYPE, array_ptr, array_size) \
    do { \
      if (OB_SUCC(ret)) { \
        if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, array_size))) { \
          DDLOG(WARN, #ARRAY_TYPE " encode array size failed", KR(ret), K(buf_len), K(pos)); \
        } else if (array_size > 0) { \
          for (int i = 0; OB_SUCC(ret) && i < array_size; i++) { \
            if (OB_FAIL(array_ptr[i].serialize(buf, buf_len, pos))) { \
              DDLOG(WARN, #ARRAY_TYPE " serialize array failed", KR(ret), K(i), K(array_size)); \
            } \
          } \
        } \
      } \
    } while(0)

#define DESERIALIZE_ARRAY(ARRAY_TYPE, array_ptr, array_size, allocator) \
    do { \
      if (OB_SUCC(ret)) { \
        if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &array_size))) { \
          DDLOG(WARN, #ARRAY_TYPE " decode array size failed", KR(ret), K(buf), K(data_len), K(pos)); \
        } else if (array_size > 0){ \
          const int64_t alloc_size = sizeof(ARRAY_TYPE) * array_size; \
          if (OB_ISNULL(allocator)) { \
            ret = oceanbase::common::OB_ERR_UNEXPECTED; \
            DDLOG(WARN, "expected valid allocator while deserialize meta", KR(ret), K(allocator)); \
          } else if (OB_ISNULL(array_ptr = static_cast<ARRAY_TYPE*>(allocator->alloc(alloc_size)))) { \
            ret = oceanbase::common::OB_ALLOCATE_MEMORY_FAILED; \
            DDLOG(WARN, #ARRAY_TYPE ": alloc memory for deserialize array", KR(ret), K(alloc_size)); \
          } else { \
            for (int i = 0; OB_SUCC(ret) && i < array_size; i++) { \
                new (array_ptr + i) ARRAY_TYPE(); \
              if (OB_FAIL(array_ptr[i].deserialize(buf, data_len, pos))) { \
                DDLOG(WARN, #ARRAY_TYPE " deserialize fail", KR(ret), K(array_size), K(i)); \
              } \
            } \
          } \
          if (OB_FAIL(ret) && OB_NOT_NULL(array_ptr)) { \
            allocator->free(array_ptr); \
            array_ptr = NULL; \
          } \
        } \
      } \
    } while(0)

#define DESERIALIZE_DICT_ARRAY_WITH_ARGS(ARRAY_TYPE, array_ptr, array_size, allocator, header, args... ) \
    do { \
      if (OB_SUCC(ret)) { \
        if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &array_size))) { \
          DDLOG(WARN, #ARRAY_TYPE " decode array size failed", KR(ret), K(buf), K(data_len), K(pos)); \
        } else if (array_size > 0){ \
          const int64_t alloc_size = sizeof(ARRAY_TYPE) * array_size; \
          if (OB_ISNULL(allocator)) { \
            ret = oceanbase::common::OB_ERR_UNEXPECTED; \
            DDLOG(WARN, "expected valid allocator while deserialize meta", KR(ret), K(allocator)); \
          } else if (OB_ISNULL(array_ptr = static_cast<ARRAY_TYPE*>(allocator->alloc(alloc_size)))) { \
            ret = oceanbase::common::OB_ALLOCATE_MEMORY_FAILED; \
            DDLOG(WARN, #ARRAY_TYPE ": alloc memory for deserialize array", KR(ret), K(alloc_size)); \
          } else { \
            for (int i = 0; OB_SUCC(ret) && i < array_size; i++) { \
                new (array_ptr + i) ARRAY_TYPE(args); \
              if (OB_FAIL(array_ptr[i].deserialize(header, buf, data_len, pos))) { \
                DDLOG(WARN, #ARRAY_TYPE " deserialize fail", KR(ret), K(array_size), K(i)); \
              } \
            } \
          } \
          if (OB_FAIL(ret) && OB_NOT_NULL(array_ptr)) { \
            allocator->free(array_ptr); \
            array_ptr = NULL; \
          } \
        } \
      } \
    } while(0)

#define ARRAY_SERIALIZE_LEN(array_ptr, array_size) \
    do { \
      len += serialization::encoded_length_vi64(array_size); \
      for (int i  = 0; i < array_size; i++) { \
        len += array_ptr[i].get_serialize_size(); \
      } \
    } while(0)

// usage:
// DEFINE_EQUAL(Type)
// {
//   bool is_equal = true; // filed must be named `equal` and value must init to true
//   LST_DO_CODE(IS_ARG_EQUAL, xxx, xxx);
//   IS_C_ARR_EQUAL(xxx);
//   IS_OBARRAY_EQUAL(xxx);
//   return is_equal;
// }
#define DEFINE_EQUAL(TypeName) \
    bool TypeName::operator==(const TypeName &other) const

#define IS_ARG_EQUAL(arg) \
    if (is_equal) { \
      if (arg == other.arg) { \
      } else { \
        is_equal = false; \
        DDLOG_RET(WARN, OB_ERR_UNEXPECTED, #arg " not equal", "current", arg, "other", other.arg); \
      } \
    }

#define IS_C_ARR_EQUAL(array_ptr, array_size) \
    if (is_equal) { \
      if (array_size == other.array_size) { \
        int i = 0; \
        for (i = 0; is_equal && i < array_size; i++) { \
          is_equal = ((array_ptr[i]) == (other.array_ptr[i])); \
        } \
        if (! is_equal) { \
          DDLOG_RET(WARN, OB_ERR_UNEXPECTED, #array_ptr " not equal", K(i), K(array_size), \
              "current", array_ptr[i], "other", other.array_ptr[i]); \
        } \
      } else { \
        is_equal = false; \
        DDLOG_RET(WARN, OB_ERR_UNEXPECTED, #array_size " not equal", K(array_size), "other", other.array_size); \
      } \
    }

#define IS_OBARRAY_EQUAL(array_ref) \
    if (is_equal) { \
      if (array_ref.count() == other.array_ref.count()) { \
        int i = 0; \
        for(i = 0; is_equal && i < array_ref.count(); i++) { \
          is_equal = array_ref.at(i) == other.array_ref.at(i); \
        } \
        if (! is_equal) { \
          DDLOG_RET(WARN, OB_ERR_UNEXPECTED, #array_ref " not equal", K(i), "count", array_ref.count(), \
              "current", array_ref.at(i), "other", other.array_ref.at(i)); \
        } \
      } else { \
        is_equal = false; \
        DDLOG_RET(WARN, OB_ERR_UNEXPECTED, #array_ref " size not equal", \
            "current", array_ref.count(), "other", other.array_ref.count()); \
      } \
    }

namespace oceanbase
{
using namespace common;
using namespace share;
namespace datadict
{

ObDictMetaHeader::ObDictMetaHeader()
  : version_(DEFAULT_VERSION),
    snapshot_scn_(),
    meta_type_(ObDictMetaType::INVALID_META),
    storage_type_(ObDictMetaStorageType::INVALID),
    dict_serialized_length_(0)
{}

ObDictMetaHeader::ObDictMetaHeader(const ObDictMetaType &meta_type)
  : ObDictMetaHeader()
{
  meta_type_ = meta_type;
}

void ObDictMetaHeader::reset()
{
  version_ = DEFAULT_VERSION;
  snapshot_scn_.reset();
  meta_type_ = ObDictMetaType::INVALID_META;
  storage_type_ = ObDictMetaStorageType::INVALID;
  dict_serialized_length_ = 0;
}

OB_SERIALIZE_MEMBER(ObDictMetaHeader,
    version_,
    snapshot_scn_,
    meta_type_,
    storage_type_,
    dict_serialized_length_);

DEFINE_EQUAL(ObDictMetaHeader)
{
  bool is_equal = true;
  LST_DO_CODE(IS_ARG_EQUAL,
      snapshot_scn_,
      meta_type_,
      storage_type_,
      dict_serialized_length_);
  return is_equal;
}

ObDictTenantMeta::ObDictTenantMeta(ObIAllocator *allocator) : allocator_(allocator), tenant_name_()
{
  reset();
}

void ObDictTenantMeta::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  schema_version_ = OB_INVALID_VERSION;
  if (nullptr != allocator_ && nullptr != tenant_name_.ptr()) {
    allocator_->free(tenant_name_.ptr());
  }
  tenant_name_.reset();
  compatibility_mode_ = ObCompatibilityMode::OCEANBASE_MODE;
  tenant_status_ = schema::ObTenantStatus::TENANT_STATUS_MAX;
  charset_type_ = ObCharsetType::CHARSET_INVALID;
  collation_type_ = ObCollationType::CS_TYPE_INVALID;
  drop_tenant_time_ = OB_INVALID_TIMESTAMP;
  in_recyclebin_ = false;
  ls_arr_.reset();
}

DEFINE_EQUAL(ObDictTenantMeta)
{
  bool is_equal = true;
  LST_DO_CODE(IS_ARG_EQUAL,
      schema_version_,
      tenant_name_,
      compatibility_mode_,
      tenant_status_,
      charset_type_,
      collation_type_,
      drop_tenant_time_,
      in_recyclebin_
      );
  IS_OBARRAY_EQUAL(ls_arr_);
  return is_equal;
}

DEFINE_SERIALIZE(ObDictTenantMeta)
{
  PRECHECK_SERIALIZE;

  if (OB_FAIL(ret)) {
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
      schema_version_,
      tenant_name_,
      compatibility_mode_,
      tenant_status_,
      charset_type_,
      collation_type_,
      drop_tenant_time_,
      in_recyclebin_,
      ls_arr_);
  }

  return ret;
}

DEFINE_DESERIALIZE_DATA_DICT(ObDictTenantMeta)
{
  PRECHECK_DESERIALIZE;

  if (OB_FAIL(ret)) {
  } else {
    ObString tmp_tenant_name;
    LST_DO_CODE(OB_UNIS_DECODE,
      schema_version_,
      tmp_tenant_name,
      compatibility_mode_,
      tenant_status_,
      charset_type_,
      collation_type_,
      drop_tenant_time_,
      in_recyclebin_,
      ls_arr_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(deep_copy_str(tmp_tenant_name, tenant_name_, *allocator_))) {
      DDLOG(WARN, "deep_copy_str for tenant_name failed", KR(ret), K(tmp_tenant_name));
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObDictTenantMeta)
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
      schema_version_,
      tenant_name_,
      compatibility_mode_,
      tenant_status_,
      charset_type_,
      collation_type_,
      drop_tenant_time_,
      in_recyclebin_,
      ls_arr_);

  return len;
}

int ObDictTenantMeta::init(const schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "expect valid allocator", KR(ret), K_(allocator));
  } else if (OB_FAIL(deep_copy_str(tenant_schema.get_tenant_name_str(), tenant_name_, *allocator_))) {
    DDLOG(WARN, "assign tenant_name failed", KR(ret), K(tenant_schema), KPC(this));
  } else {
    tenant_id_ = tenant_schema.get_tenant_id();
    schema_version_ = tenant_schema.get_schema_version();
    compatibility_mode_ = tenant_schema.get_compatibility_mode();
    tenant_status_ = tenant_schema.get_status();
    charset_type_ = tenant_schema.get_charset_type();
    collation_type_ = tenant_schema.get_collation_type();
    drop_tenant_time_ = tenant_schema.get_drop_tenant_time();
    in_recyclebin_ = tenant_schema.is_in_recyclebin();
  }

  return ret;
}

int ObDictTenantMeta::incremental_data_update(const ObDictTenantMeta &new_tenant_meta)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "expect valid allocator", KR(ret), K_(allocator));
  } else {
    if (nullptr != tenant_name_.ptr()) {
      allocator_->free(tenant_name_.ptr());
    }

    if (OB_FAIL(deep_copy_str(new_tenant_meta.get_tenant_name(), tenant_name_, *allocator_))) {
      DDLOG(WARN, "assign tenant_name failed", KR(ret), K(new_tenant_meta), KPC(this));
    } else {
      schema_version_ = new_tenant_meta.get_schema_version();
      compatibility_mode_ = new_tenant_meta.get_compatibility_mode();
      tenant_status_ = new_tenant_meta.get_status();
      charset_type_ = new_tenant_meta.get_charset_type();
      collation_type_ = new_tenant_meta.get_collation_type();
      drop_tenant_time_ = new_tenant_meta.get_drop_tenant_time();
      in_recyclebin_ = new_tenant_meta.is_in_recyclebin();
    }
  }

  return ret;
}

int ObDictTenantMeta::incremental_data_update(const share::ObLSAttr &ls_attr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! ls_attr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "ls attr is invalid", KR(ret), K(ls_attr));
  } else if (share::is_ls_create_end_op(ls_attr.get_ls_operation_type())) {
    if (OB_FAIL(ls_arr_.push_back(ls_attr.get_ls_id()))) {
      DDLOG(WARN, "ls_arr_ push_back failed", KR(ret), K(ls_attr), K(ls_arr_));
    } else {
      DDLOG(TRACE, "ls_arr_ push back succ", K(ls_attr), K(ls_arr_));
    }
  } else if (share::is_ls_drop_end_op(ls_attr.get_ls_operation_type())) {
    int64_t ls_idx = -1;
    const ObLSID &ls_id = ls_attr.get_ls_id();
    ARRAY_FOREACH(ls_arr_, idx) {
      const ObLSID &cur_ls_id = ls_arr_.at(idx);
      // assume there is no duplicate ls_id in ls_arr_
      if (cur_ls_id == ls_id) {
        ls_idx = idx;
        break;
      }
    }

    if (-1 != ls_idx) {
      if (OB_FAIL(ls_arr_.remove(ls_idx))) {
        DDLOG(ERROR, "remove ls from ls_arr failed", K(ls_idx), K(ls_arr_), K(ls_attr));
      } else {
        DDLOG(TRACE, "remove ls from ls_arr finished", K(ls_idx), K(ls_arr_), K(ls_attr));
      }
    }

  } else {}

  return ret;
}

int ObDictTenantMeta::init_with_ls_info(
    const schema::ObTenantSchema &tenant_schema,
    const share::ObLSArray &ls_array)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init(tenant_schema))) {
    DDLOG(WARN, "init tenant_meta by schema failed", KR(ret), K(tenant_schema));
  } else if (OB_FAIL(ls_arr_.assign(ls_array))) {
    DDLOG(WARN, "assign ls_info_arr failed", KR(ret), K(ls_array), K(tenant_schema), KPC(this));
  }

  return ret;
}

ObDictDatabaseMeta::ObDictDatabaseMeta(ObIAllocator *allocator) : allocator_(allocator), database_name_()
{
  reset();
}

void ObDictDatabaseMeta::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  database_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_TIMESTAMP;
  if (nullptr != allocator_ && nullptr != database_name_.ptr()) {
    allocator_->free(database_name_.ptr());
 }
  database_name_.reset();
  charset_type_ = ObCharsetType::CHARSET_INVALID;
  collation_type_ = ObCollationType::CS_TYPE_INVALID;
  name_case_mode_ = common::OB_NAME_CASE_INVALID;
  in_recyclebin_ = false;
}

DEFINE_EQUAL(ObDictDatabaseMeta)
{
  bool is_equal = true;
  LST_DO_CODE(IS_ARG_EQUAL,
      database_id_,
      schema_version_,
      database_name_,
      charset_type_,
      collation_type_,
      name_case_mode_,
      in_recyclebin_
      );
  return is_equal;
}

DEFINE_SERIALIZE(ObDictDatabaseMeta)
{
  PRECHECK_SERIALIZE;

  if (OB_FAIL(ret)) {
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
      database_id_,
      schema_version_,
      database_name_,
      charset_type_,
      collation_type_,
      name_case_mode_,
      in_recyclebin_);
  }

  return ret;
}

DEFINE_DESERIALIZE_DATA_DICT(ObDictDatabaseMeta)
{
  PRECHECK_DESERIALIZE;

  if (OB_FAIL(ret)) {
  } else {
    ObString tmp_db_name;
    LST_DO_CODE(OB_UNIS_DECODE,
      database_id_,
      schema_version_,
      tmp_db_name,
      charset_type_,
      collation_type_,
      name_case_mode_,
      in_recyclebin_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(deep_copy_str(tmp_db_name, database_name_, *allocator_))) {
      DDLOG(WARN, "deep_copy_str for database_name_ failed", KR(ret), K(tmp_db_name));
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObDictDatabaseMeta)
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
      database_id_,
      schema_version_,
      database_name_,
      charset_type_,
      collation_type_,
      name_case_mode_,
      in_recyclebin_);

  return len;
}

int ObDictDatabaseMeta::init(const schema::ObDatabaseSchema &database_schema)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(assign_(database_schema))) {
    DDLOG(WARN, "assign_ failed", KR(ret), K(database_schema), KPC(this));
  } else {
    // success
  }

  return ret;
}

int ObDictDatabaseMeta::assign(const ObDictDatabaseMeta &src_database_meta)
{
  int ret = OB_SUCCESS;

  if (this != &src_database_meta) {
    reset();

    if (OB_FAIL(assign_(src_database_meta))) {
      DDLOG(WARN, "assign_ failed", KR(ret), K(src_database_meta), KPC(this));
    } else {
      // success
    }
  }

  if (OB_FAIL(ret)) {
    DDLOG(WARN, "ObDictDatabaseMeta assign failed", KR(ret));
  }

  return ret;
}

template<class DATABASE_SCHEMA>
int ObDictDatabaseMeta::assign_(DATABASE_SCHEMA &database_schema)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "expect valid allocator", KR(ret), K_(allocator));
  } else if (OB_FAIL(deep_copy_str(database_schema.get_database_name_str(), database_name_, *allocator_))) {
    DDLOG(WARN, "assign tenant_name failed", KR(ret), K(database_schema), KPC(this));
  } else {
    tenant_id_ = database_schema.get_tenant_id();
    database_id_ = database_schema.get_database_id();
    schema_version_ = database_schema.get_schema_version();
    charset_type_ = database_schema.get_charset_type();
    collation_type_ = database_schema.get_collation_type();
    name_case_mode_ = database_schema.get_name_case_mode();
    in_recyclebin_ = database_schema.is_in_recyclebin();
  }

  return ret;
}

ObDictColumnMeta::ObDictColumnMeta(ObIAllocator *allocator)
  : allocator_(allocator), column_name_()
{
  reset();
}

void ObDictColumnMeta::reset()
{
  column_id_ = OB_INVALID_ID;
  if (nullptr != allocator_ && column_name_.ptr() != nullptr) {
    allocator_->free(column_name_.ptr());
    column_name_.reset();
  }
  rowkey_position_ = -1;
  index_position_ = -1;
  meta_type_.reset();
  accuracy_.reset();
  colulmn_properties_ = 0;
  column_flags_ = -1;
  charset_type_ = ObCharsetType::CHARSET_INVALID;
  collation_type_ = ObCollationType::CS_TYPE_INVALID;
  orig_default_value_.reset();
  cur_default_value_.reset();
  extended_type_info_.reset();
  column_ref_ids_.reset();
  udt_set_id_ = 0;
  sub_type_ = 0;
}

DEFINE_EQUAL(ObDictColumnMeta)
{
  bool is_equal = true;
  LST_DO_CODE(IS_ARG_EQUAL,
      column_id_,
      column_name_,
      rowkey_position_,
      index_position_,
      meta_type_,
      accuracy_,
      colulmn_properties_,
      column_flags_,
      charset_type_,
      collation_type_,
      orig_default_value_,
      cur_default_value_,
      udt_set_id_,
      sub_type_
      );
  IS_OBARRAY_EQUAL(extended_type_info_);
  IS_OBARRAY_EQUAL(column_ref_ids_);
  return is_equal;
}

DEFINE_SERIALIZE(ObDictColumnMeta)
{
  PRECHECK_SERIALIZE;

  if (OB_FAIL(ret)) {
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
      column_id_,
      column_name_,
      rowkey_position_,
      index_position_,
      meta_type_,
      accuracy_,
      colulmn_properties_,
      column_flags_,
      charset_type_,
      collation_type_,
      orig_default_value_,
      cur_default_value_,
      extended_type_info_,
      column_ref_ids_,
      udt_set_id_,
      sub_type_);
  }

  return ret;
}

DEFINE_DESERIALIZE_DATA_DICT(ObDictColumnMeta)
{
  PRECHECK_DESERIALIZE;
  if (OB_FAIL(ret)) {
  } else {
    ObString tmp_col_name;
    ObObj tmp_orig_default_val;
    ObObj tmp_cur_default_val;
    LST_DO_CODE(OB_UNIS_DECODE,
      column_id_,
      tmp_col_name,
      rowkey_position_,
      index_position_,
      meta_type_,
      accuracy_,
      colulmn_properties_,
      column_flags_,
      charset_type_,
      collation_type_,
      tmp_orig_default_val,
      tmp_cur_default_val
      );
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(deserialize_string_array(buf, data_len, pos, extended_type_info_, *allocator_))) {
      DDLOG(WARN, "deserialize_extinfo_array failed", KR(ret));
    } else if (OB_FAIL(deep_copy_str(tmp_col_name, column_name_, *allocator_))) {
      DDLOG(WARN, "deep copy column_name failed", KR(ret), K(tmp_col_name));
    } else if (OB_FAIL(deep_copy_default_val_(tmp_orig_default_val, orig_default_value_))) {
      DDLOG(WARN, "deep copy orig_default_value failed", KR(ret), K(tmp_orig_default_val));
    } else if (OB_FAIL(deep_copy_default_val_(tmp_cur_default_val, cur_default_value_))) {
      DDLOG(WARN, "deep copy cur_default_value failed", KR(ret), K(tmp_cur_default_val));
    } else {
      if (header.get_version() > 1) {
        // column_ref_ids_ is serialized when versin >= 2
        if (OB_FAIL(common::serialization::decode(buf, data_len, pos, column_ref_ids_))) {
          DDLOG(WARN, "deserialize column_ref_ids_ failed", KR(ret), K(data_len), K(pos));
        }
      }
      if (OB_SUCC(ret) && header.get_version() > 2) {
        //udt_set_id_ and sub_type_ is serialized when versin >= 3
        if (OB_FAIL(NS_::decode(buf, data_len, pos, udt_set_id_))) {
          DDLOG(WARN, "deserialize col_group_id failed", KR(ret));
        } else if (OB_FAIL(NS_::decode(buf, data_len, pos, sub_type_))) {
          DDLOG(WARN, "deserialize sub_type failed", KR(ret));
        }
      }
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObDictColumnMeta)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      column_id_,
      column_name_,
      rowkey_position_,
      index_position_,
      meta_type_,
      accuracy_,
      colulmn_properties_,
      column_flags_,
      charset_type_,
      collation_type_,
      orig_default_value_,
      cur_default_value_,
      extended_type_info_,
      column_ref_ids_,
      udt_set_id_,
      sub_type_);
  return len;
}

int ObDictColumnMeta::init(const schema::ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(assign_(column_schema))) {
    DDLOG(WARN, "assign failed", KR(ret), K(column_schema), KPC(this));
  } else {
    // success
  }

  return ret;
}

template<class COLUMN_META>
int ObDictColumnMeta::assign_(COLUMN_META &column_schema)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "expect valid allocator", KR(ret), K_(allocator));
  } else if (OB_FAIL(deep_copy_str(column_schema.get_column_name_str(), column_name_, *allocator_))) {
    DDLOG(WARN, "assign column name failed", KR(ret), K(column_schema), KPC(this));
  } else if (OB_FAIL(deep_copy_default_val_(column_schema.get_orig_default_value(), orig_default_value_))) {
    DDLOG(WARN, "copy_orig_default_value failed", KR(ret), K(column_schema), KPC(this));
  } else if (OB_FAIL(deep_copy_default_val_(column_schema.get_cur_default_value(), cur_default_value_))) {
    DDLOG(WARN, "copy_cur_default_value failed", KR(ret), K(column_schema), KPC(this));
  } else if (OB_FAIL(deep_copy_str_array(column_schema.get_extended_type_info(), extended_type_info_, *allocator_))) {
    DDLOG(WARN, "assign extended_type_info failed", KR(ret), K(column_schema), KPC(this));
  } else if (OB_FAIL(column_schema.get_cascaded_column_ids(column_ref_ids_))) {
    DDLOG(WARN, "get_cascaded_column_ids failed", KR(ret), K(column_schema));
  } else {
    column_id_ = column_schema.get_column_id();
    rowkey_position_ = column_schema.get_rowkey_position();
    index_position_ = column_schema.get_index_position();
    meta_type_ = column_schema.get_meta_type();
    accuracy_ = column_schema.get_accuracy();
    is_nullable_ = column_schema.is_nullable();
    is_zero_fill_ = column_schema.is_zero_fill();
    is_autoincrement_ = column_schema.is_autoincrement();
    is_hidden_ = column_schema.is_hidden();
    is_part_key_col_ = column_schema.is_tbl_part_key_column();
    column_flags_ = column_schema.get_column_flags();
    charset_type_ = column_schema.get_charset_type();
    collation_type_ = column_schema.get_collation_type();
    udt_set_id_ = column_schema.get_udt_set_id();
    sub_type_ = column_schema.get_sub_data_type();
  }

  return ret;
}

int ObDictColumnMeta::assign(const ObDictColumnMeta &src_column_meta)
{
  int ret = OB_SUCCESS;

  if (this != &src_column_meta) {
    reset();

    if (OB_FAIL(assign_(src_column_meta))) {
      DDLOG(WARN, "assign failed", KR(ret), K(src_column_meta), KPC(this));
    } else {
      // success
    }
  }

  if (OB_FAIL(ret)) {
    DDLOG(WARN, "ObDictColumnMeta assign failed", KR(ret));
  }

  return ret;
}

int ObDictColumnMeta::get_cascaded_column_ids(ObIArray<uint64_t> &column_ids) const
{
  int ret = OB_SUCCESS;
  column_ids.reset();

  if (OB_FAIL(column_ids.assign(column_ref_ids_))) {
    DDLOG(WARN, "assign cascaded_columns failed", KR(ret), KPC(this), K_(column_ref_ids));
  }

  return ret;
}

int ObDictColumnMeta::deep_copy_default_val_(const ObObj &src_default_val, ObObj &dest_default_val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "expect valid allocator_", KR(ret), K(allocator_));
  } else if (src_default_val.get_deep_copy_size() > 0) {
    char *buf = nullptr;
    int64_t pos = 0;
    const int64_t alloc_size = src_default_val.get_deep_copy_size();

    if (OB_ISNULL(buf = (char *)allocator_->alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DDLOG(WARN, "failed to alloc memory", KR(ret), K(alloc_size));
    } else if (OB_FAIL(dest_default_val.deep_copy(src_default_val, buf, alloc_size, pos))) {
      orig_default_value_.reset();
      allocator_->free(buf);
      buf = nullptr;
      DDLOG(WARN, "failed to deep copy", KR(ret), K(src_default_val), K(pos));
    }
  } else {
    dest_default_val = src_default_val;
  }
  return ret;
}

int64_t ObDictColumnMeta::get_data_length() const
{
  return share::schema::ObColumnSchemaV2::get_data_length(accuracy_, meta_type_);
}

ObDictTableMeta::ObDictTableMeta(ObIAllocator *allocator)
  : allocator_(allocator),
    table_name_(),
    column_count_(0),
    rowkey_column_count_(0),
    index_column_count_(0)
{
  reset();
}

void ObDictTableMeta::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  database_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_TIMESTAMP;
  if (nullptr != allocator_ && table_name_.ptr() != nullptr) {
    allocator_->free(table_name_.ptr());
    table_name_.reset();
  }
  name_case_mode_ = common::OB_NAME_CASE_INVALID;
  table_type_ = schema::ObTableType::USER_TABLE;
  table_mode_.reset();
  charset_type_ = ObCharsetType::CHARSET_INVALID;
  collation_type_ = ObCollationType::CS_TYPE_INVALID;
  tablet_id_arr_.reset();
  aux_lob_meta_tid_ = OB_INVALID_ID;
  aux_lob_piece_tid_ = OB_INVALID_ID;
  max_used_column_id_ = OB_INVALID_ID;
  column_id_arr_order_by_table_def_.reset();
  unique_index_tid_arr_.reset();
  index_type_ = schema::ObIndexType::INDEX_TYPE_IS_NOT;
  free_column_info_();
  free_rowkey_info_();
  free_index_info_();
  data_table_id_ = OB_INVALID_ID;
  association_table_id_ = OB_INVALID_ID;
}

DEFINE_EQUAL(ObDictTableMeta)
{
  bool is_equal = true;
  LST_DO_CODE(IS_ARG_EQUAL,
      database_id_,
      table_id_,
      schema_version_,
      table_name_,
      name_case_mode_,
      table_type_,
      table_mode_,
      charset_type_,
      collation_type_,
      aux_lob_meta_tid_,
      aux_lob_piece_tid_,
      max_used_column_id_,
      index_type_,
      data_table_id_,
      association_table_id_
      );
  IS_OBARRAY_EQUAL(column_id_arr_order_by_table_def_);
  IS_OBARRAY_EQUAL(tablet_id_arr_);
  IS_OBARRAY_EQUAL(unique_index_tid_arr_);
  IS_C_ARR_EQUAL(col_metas_, column_count_);
  IS_C_ARR_EQUAL(rowkey_cols_, rowkey_column_count_);
  IS_C_ARR_EQUAL(index_cols_, index_column_count_);
  return is_equal;
}

DEFINE_SERIALIZE(ObDictTableMeta)
{
  PRECHECK_SERIALIZE;

  if (OB_FAIL(ret)) {
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
        database_id_,
        table_id_,
        schema_version_,
        table_name_,
        name_case_mode_,
        table_type_,
        table_mode_,
        charset_type_,
        collation_type_,
        tablet_id_arr_,
        aux_lob_meta_tid_,
        aux_lob_piece_tid_,
        max_used_column_id_,
        column_id_arr_order_by_table_def_,
        unique_index_tid_arr_,
        index_type_,
        data_table_id_,
        association_table_id_);
    SEARILIZE_ARRAR(ObDictColumnMeta, col_metas_, column_count_);
    SEARILIZE_ARRAR(ObRowkeyColumn, rowkey_cols_, rowkey_column_count_);
    SEARILIZE_ARRAR(ObIndexColumn, index_cols_, index_column_count_);
  }

  return ret;
}

DEFINE_DESERIALIZE_DATA_DICT(ObDictTableMeta)
{
  PRECHECK_DESERIALIZE;

  if (OB_FAIL(ret)) {
  } else {
    ObString tmp_table_name;

    LST_DO_CODE(OB_UNIS_DECODE,
        database_id_,
        table_id_,
        schema_version_,
        tmp_table_name,
        name_case_mode_,
        table_type_,
        table_mode_,
        charset_type_,
        collation_type_,
        tablet_id_arr_,
        aux_lob_meta_tid_,
        aux_lob_piece_tid_,
        max_used_column_id_,
        column_id_arr_order_by_table_def_,
        unique_index_tid_arr_,
        index_type_,
        data_table_id_,
        association_table_id_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(deep_copy_str(tmp_table_name, table_name_, *allocator_))) {
      DDLOG(WARN, "deep_copy_str for table_name failed", KR(ret), K(tmp_table_name));
    } else {
      DESERIALIZE_DICT_ARRAY_WITH_ARGS(ObDictColumnMeta, col_metas_, column_count_, allocator_, header, allocator_);
      DESERIALIZE_ARRAY(ObRowkeyColumn, rowkey_cols_, rowkey_column_count_, allocator_);
      DESERIALIZE_ARRAY(ObIndexColumn, index_cols_, index_column_count_, allocator_);
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObDictTableMeta)
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
      database_id_,
      table_id_,
      schema_version_,
      table_name_,
      name_case_mode_,
      table_type_,
      table_mode_,
      charset_type_,
      collation_type_,
      tablet_id_arr_,
      aux_lob_meta_tid_,
      aux_lob_piece_tid_,
      max_used_column_id_,
      column_id_arr_order_by_table_def_,
      unique_index_tid_arr_,
      index_type_,
      data_table_id_,
      association_table_id_);
  ARRAY_SERIALIZE_LEN(col_metas_, column_count_);
  ARRAY_SERIALIZE_LEN(rowkey_cols_, rowkey_column_count_);
  ARRAY_SERIALIZE_LEN(index_cols_, index_column_count_);
  return len;
}

int ObDictTableMeta::init(const schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "invalid allocator_", KR(ret));
  } else if (OB_FAIL(build_column_info_(table_schema))) {
    DDLOG(WARN, "build_column_info_ failed", KR(ret), KPC(this));
  } else if (OB_FAIL(build_rowkey_info_(table_schema))) {
    DDLOG(WARN, "build_rowkey_info_ failed", KR(ret), KPC(this));
  } else if (OB_FAIL(build_index_info_(table_schema))) {
    DDLOG(WARN, "build_index_info_ failed", KR(ret), KPC(this));
  } else if (OB_FAIL(build_column_id_arr_(table_schema))) {
    DDLOG(WARN, "build_column_id_arr failed", KR(ret), K(table_schema), KPC(this));
  } else if (OB_FAIL(assign_(table_schema))) {
    DDLOG(WARN, "assign_ failed", KR(ret), KPC(this));
  } else {
    if (table_schema.has_tablet()) {
      if (OB_FAIL(table_schema.get_tablet_ids(tablet_id_arr_))) {
        DDLOG(WARN, "get_tablet_ids failed", KR(ret), KPC(this));
      }
    }
  }

  return ret;
}

template<class TABLE_SCHEMA>
int ObDictTableMeta::assign_(const TABLE_SCHEMA &table_schema)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "invalid allocator_", KR(ret));
  } else if (OB_FAIL(deep_copy_str(table_schema.get_table_name_str(), table_name_, *allocator_))) {
    DDLOG(WARN, "copy table_name failed", KR(ret), K(table_schema), KPC(this));
  } else {
    tenant_id_ = table_schema.get_tenant_id();
    database_id_ = table_schema.get_database_id();
    table_id_ = table_schema.get_table_id();
    schema_version_ = table_schema.get_schema_version();
    name_case_mode_ = table_schema.get_name_case_mode();
    table_type_ = table_schema.get_table_type();
    table_mode_ = table_schema.get_table_mode_struct();
    charset_type_ = table_schema.get_charset_type();
    collation_type_ = table_schema.get_collation_type();
    aux_lob_meta_tid_ = table_schema.get_aux_lob_meta_tid();
    aux_lob_piece_tid_ = table_schema.get_aux_lob_piece_tid();
    max_used_column_id_ = table_schema.get_max_used_column_id();
    index_type_ = table_schema.get_index_type();
    data_table_id_ = table_schema.get_data_table_id();
    association_table_id_ = table_schema.get_association_table_id();
  }

  return ret;
}

int ObDictTableMeta::assign(const ObDictTableMeta &src_table_meta)
{
  int ret = OB_SUCCESS;

  if (this != &src_table_meta) {
    reset();

    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      DDLOG(WARN, "expect valid allocator", KR(ret), K_(allocator));
    } else if (OB_FAIL(build_column_info_(src_table_meta))) {
      DDLOG(WARN, "build_column_info_ failed", KR(ret), KPC(this));
    } else if (OB_FAIL(build_rowkey_info_(src_table_meta))) {
      DDLOG(WARN, "build_rowkey_info_ failed", KR(ret), KPC(this));
    } else if (OB_FAIL(build_index_info_(src_table_meta))) {
      DDLOG(WARN, "build_index_info_ failed", KR(ret), KPC(this));
    } else if (OB_FAIL(assign_(src_table_meta))) {
      DDLOG(WARN, "assign_ failed", KR(ret), KPC(this));
    } else if (OB_FAIL(column_id_arr_order_by_table_def_.assign(src_table_meta.get_column_id_arr_order_by_table_define()))) {
      DDLOG(WARN, "assign_column_id_arr failed", KR(ret), K(src_table_meta),
          "src_column_id_arr", src_table_meta.get_column_id_arr_order_by_table_define());
    } else {
      if (src_table_meta.has_tablet()) {
        if (OB_FAIL(tablet_id_arr_.assign(src_table_meta.get_tablet_ids()))) {
          DDLOG(WARN, "tablet_id_arr_ assign failed", KR(ret), KPC(this));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    DDLOG(WARN, "ObDictTableMeta assign failed", KR(ret));
  }

  return ret;
}

int ObDictTableMeta::build_index_info_(const schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const common::ObIndexInfo &index_info = table_schema.get_index_info();
  const ObIArray<ObAuxTableMetaInfo> &simple_index_infos = table_schema.get_simple_index_infos();

  if (table_schema.is_user_table()) {
    // build index_table_id_arr
    for (int i = 0; OB_SUCC(ret) &&i < simple_index_infos.count(); i++) {
      ObAuxTableMetaInfo index_table_info;
      if (OB_FAIL(simple_index_infos.at(i, index_table_info))) {
      } else if (! ObTableSchema::is_unique_index(index_table_info.index_type_)) {
        DDLOG(DEBUG, "TableMeta ignore non-unique-index table for unique_index_tid_arr",
            K(index_table_info), K_(table_id));
      } else if (OB_FAIL(unique_index_tid_arr_.push_back(index_table_info.table_id_))) {
        DDLOG(WARN, "failed to push_back index_table_id into unique_index_tid_arr",
            KR(ret), K(i), K(simple_index_infos), K_(unique_index_tid_arr), KPC(this));
      }
    }
  } else if (table_schema.is_unique_index()) {
    // only build index column info for unique index table, cause OBCDC only use unique_index info.
    index_column_count_ = index_info.get_size();
    const int64_t alloc_size = sizeof(common::ObIndexColumn) * index_column_count_;

    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      DDLOG(WARN, "invalid allocator_", KR(ret));
    } else if (index_column_count_ <= 0) {
      DDLOG(TRACE, "not found index_cols_, skip", KPC(this));
    } else if (OB_ISNULL(index_cols_ = static_cast<ObIndexColumn*>(allocator_->alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DDLOG(WARN, "alloc for index_cols_ failed", KR(ret), K(alloc_size), KPC(this));
    } else {
      // build index_columns
      for (int i = 0; OB_SUCC(ret) && i < index_column_count_; i++) {
        ObIndexColumn *idx_col = new (index_cols_ + i) ObIndexColumn();
        ObIndexColumn tmp_idx_col;

        if (OB_FAIL(index_info.get_column(i, tmp_idx_col))) {
          DDLOG(WARN, "get_index_column from ObIndexInfo failed", KR(ret), K(index_info), K(i), KPC(this));
        } else if (OB_ISNULL(idx_col)) {
          ret = OB_INVALID_DATA;
          DDLOG(WARN, "expect valid index_column", KR(ret), K(index_info), K(i), KPC(this));
        } else {
          *idx_col = tmp_idx_col;
        }
      }
    }
  } else {
    // ignore index info of other table type
    // and other type of table exclude user table and unique index table
  }

  return ret;
}

int ObDictTableMeta::build_index_info_(const ObDictTableMeta &src_table_meta)
{
  int ret = OB_SUCCESS;
  index_column_count_ = src_table_meta.get_index_column_count();
  const int64_t alloc_size = sizeof(common::ObIndexColumn) * index_column_count_;
  const common::ObIndexColumn *src_index_cols = src_table_meta.get_index_cols();

  if (src_table_meta.is_user_table()) {
    // build index_table_id_arr
    if (OB_FAIL(unique_index_tid_arr_.assign(src_table_meta.get_unique_index_table_id_arr()))) {
      DDLOG(WARN, "unique_index_tid_arr_ assign failed", KR(ret), K(unique_index_tid_arr_), K(src_table_meta));
    }
  } else if (src_table_meta.is_unique_index()) {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      DDLOG(WARN, "invalid allocator_", KR(ret));
    } else if (index_column_count_ <= 0) {
      DDLOG(TRACE, "not found index_cols_, skip", KPC(this));
    } else if (OB_ISNULL(index_cols_ = static_cast<ObIndexColumn*>(allocator_->alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DDLOG(WARN, "alloc for index_cols_ failed", KR(ret), K(alloc_size), KPC(this));
    } else if (OB_ISNULL(src_index_cols)) {
      ret = OB_ERR_UNEXPECTED;
      DDLOG(WARN, "src_index_cols is nullptr", KR(ret), K(src_index_cols), K(src_table_meta));
    } else {
      // build index_columns
      for (int idx = 0; OB_SUCC(ret) && idx < index_column_count_; idx++) {
        ObIndexColumn *index_col = new (index_cols_ + idx) ObIndexColumn();
        const ObIndexColumn *src_index_col = src_index_cols + idx;

        if (OB_ISNULL(index_col) || OB_ISNULL(src_index_col)) {
          ret = OB_INVALID_DATA;
          DDLOG(WARN, "expect valid index_col", KR(ret), K(idx), K(index_col), K(src_index_col), KPC(this));
        } else {
          *index_col = *src_index_col;
        }
      }
    }
  }

  return ret;
}

int ObDictTableMeta::build_rowkey_info_(const schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;

  if (!table_schema.is_user_table()) {
    // only record rowkey column info for user table.
  } else {
    const common::ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();
    rowkey_column_count_ = rowkey_info.get_size();
    const int64_t alloc_size = sizeof(common::ObRowkeyColumn) * rowkey_column_count_;

    if (rowkey_column_count_ <= 0) {
      DDLOG(TRACE, "not found rowkey cols, skip", KPC(this));
    } else if (OB_ISNULL(rowkey_cols_ = static_cast<ObRowkeyColumn*>(allocator_->alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DDLOG(WARN, "alloc for rowkey_cols_ failed", KR(ret), K(alloc_size), KPC(this));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < rowkey_column_count_; i++) {
        ObRowkeyColumn *rowkey_col = new (rowkey_cols_ + i) ObRowkeyColumn();
        ObRowkeyColumn tmp_rowkey_col;

        if (OB_FAIL(rowkey_info.get_column(i, tmp_rowkey_col))) {
          DDLOG(WARN, "get_rowkey_column from ObRowkeyInfo failed", KR(ret), K(rowkey_info), K(i), KPC(this));
        } else if (OB_ISNULL(rowkey_col)) {
          ret = OB_INVALID_DATA;
          DDLOG(WARN, "expect valid rowkey_column", KR(ret), K(rowkey_info), K(i), KPC(this));
        } else {
          *rowkey_col = tmp_rowkey_col;
        }
      }
    }
  }

  return ret;
}

int ObDictTableMeta::build_rowkey_info_(const ObDictTableMeta &src_table_meta)
{
  int ret = OB_SUCCESS;
  rowkey_column_count_ = src_table_meta.get_rowkey_column_num();
  const ObRowkeyColumn *src_rowkey_cols = src_table_meta.get_rowkey_cols();
  const int64_t alloc_size = sizeof(common::ObRowkeyColumn) * rowkey_column_count_;

  if (rowkey_column_count_ <= 0) {
    DDLOG(TRACE, "not found rowkey cols, skip", KPC(this));
  } else if (OB_ISNULL(rowkey_cols_ = static_cast<ObRowkeyColumn*>(allocator_->alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    DDLOG(WARN, "alloc for rowkey_cols_ failed", KR(ret), K(alloc_size), KPC(this));
  } else if (OB_ISNULL(src_rowkey_cols)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "src_rowkey_cols is nullptr", KR(ret), K(src_rowkey_cols), K(src_table_meta));
  } else {
    for (int idx = 0; OB_SUCC(ret) && idx < rowkey_column_count_; idx++) {
      ObRowkeyColumn *rowkey_col = new (rowkey_cols_ + idx) ObRowkeyColumn();
      const ObRowkeyColumn *src_rowkey_col = src_rowkey_cols + idx;

      if (OB_ISNULL(rowkey_col) || OB_ISNULL(src_rowkey_col)) {
        ret = OB_INVALID_DATA;
        DDLOG(WARN, "expect valid rowkey_column", KR(ret), K(idx), K(rowkey_col), K(src_rowkey_col), KPC(this));
      } else {
        *rowkey_col = *src_rowkey_col;
      }
    }
  }

  return ret;
}

int ObDictTableMeta::build_column_info_(const schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::schema::ObColDesc> column_ids;

  if (!table_schema.is_user_table()) {
    // only build_column_info_ for user table.
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "invalid allocator_", KR(ret));
  } else if (OB_FAIL(table_schema.get_column_ids(column_ids))) {
    DDLOG(WARN, "get_column_ids from table_schema failed", KR(ret), K(table_schema));
  } else {
    column_count_ = column_ids.count();
    const int64_t alloc_size = sizeof(ObDictColumnMeta) * column_count_;

    if (column_count_ <= 0) {
      DDLOG(TRACE, "table don't have columns, skip", KR(ret), KPC(this));
    } else if (OB_ISNULL(col_metas_ = static_cast<ObDictColumnMeta*>(allocator_->alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DDLOG(WARN, "alloc for col_metas_ failed", KR(ret), K(alloc_size), KPC(this));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < column_count_; i++) {
        const schema::ObColDesc &col_desc = column_ids.at(i);
        const uint64_t column_id = col_desc.col_id_;
        const schema::ObColumnSchemaV2 *column_schema = NULL;
        ObDictColumnMeta *col_meta = new (col_metas_ + i) ObDictColumnMeta(allocator_);

        if (OB_ISNULL(col_meta)) {
          ret = OB_INVALID_DATA;
          DDLOG(WARN, "unexpected invalid ObDictColumnMeta", KR(ret),
              K(column_id), K(i), K(column_ids), KPC(this));
        } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(column_id))) {
          ret = OB_ERR_UNEXPECTED;
          DDLOG(WARN, "get_column_schema failed", KR(ret),
              K(column_id), K(i), K(column_ids), K(table_schema), KPC(this));
        } else if (OB_FAIL(col_meta->init(*column_schema))) {
          DDLOG(WARN, "init ObDictColumnMeta failed", KR(ret),
              K(column_schema), K(i), K(column_ids), K(table_schema), KPC(this));
        }
      }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(col_metas_)) {
      allocator_->free(col_metas_);
      col_metas_ = NULL;
    }
  }

  return ret;
}

int ObDictTableMeta::build_column_info_(const ObDictTableMeta &src_table_meta)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "invalid allocator_", KR(ret));
  } else {
    column_count_ = src_table_meta.get_column_count();
    const ObDictColumnMeta *src_col_metas = src_table_meta.get_column_metas();
    const int64_t alloc_size = sizeof(ObDictColumnMeta) * column_count_;

    if (column_count_ <= 0) {
      DDLOG(TRACE, "table don't have columns, skip", KPC(this));
    } else if (OB_ISNULL(col_metas_ = static_cast<ObDictColumnMeta*>(allocator_->alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DDLOG(WARN, "alloc for col_metas_ failed", KR(ret), K(alloc_size), KPC(this));
    } else if (OB_ISNULL(src_col_metas)) {
      ret = OB_ERR_UNEXPECTED;
      DDLOG(WARN, "src_col_metas is nullptr", KR(ret), K(src_col_metas), K(src_table_meta));
    } else {
      for (int idx = 0; OB_SUCC(ret) && idx < column_count_; idx++) {
        ObDictColumnMeta *col_meta = new (col_metas_ + idx) ObDictColumnMeta(allocator_);
        const ObDictColumnMeta *src_col_meta = src_col_metas + idx;

        if (OB_ISNULL(col_meta) ||OB_ISNULL(src_col_meta)) {
          ret = OB_INVALID_DATA;
          DDLOG(WARN, "unexpected invalid ObDictColumnMeta", KR(ret), K(idx), K(col_meta), K(src_col_meta),
              KPC(this), K(src_table_meta));
        } else if (OB_FAIL(col_meta->assign(*src_col_meta))) {
          DDLOG(WARN, "col_meta assign failed", KR(ret), KPC(src_col_meta));
        } else {}
      }
    }
  }

  return ret;
}

int ObDictTableMeta::build_column_id_arr_(const share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  column_id_arr_order_by_table_def_.reset();
  if (table_schema.is_view_table() && !table_schema.is_materialized_view()) {
    DDLOG(DEBUG, "build_column_id_arr_ skip view", KPC(this));
  } else {
    ObColumnIterByPrevNextID pre_next_id_iter(table_schema);

    while (OB_SUCC(ret)) {
      const ObColumnSchemaV2 *column_schema = nullptr;

      if (OB_FAIL(pre_next_id_iter.next(column_schema))) {
        if (OB_ITER_END != ret) {
          DDLOG(WARN, "pre_next_id_iter next fail", KR(ret), K(table_schema), KPC(column_schema));
        }
      } else if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        DDLOG(WARN, "column_schema is null", KR(ret), KPC(column_schema));
      } else if (OB_FAIL(column_id_arr_order_by_table_def_.push_back(column_schema->get_column_id()))) {
        DDLOG(WARN, "push_back column_id into column_id_arr_order_by_table_def_ failed", KR(ret),
            K(table_schema), KPC(column_schema), K_(column_id_arr_order_by_table_def));
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

void ObDictTableMeta::free_index_info_()
{
  if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(index_cols_)) {
    for (int i = 0; i < index_column_count_; i++) {
      common::ObIndexColumn *index_col = index_cols_ + i;
      if (OB_NOT_NULL(index_col)) {
        index_col->~ObIndexColumn();
      }
    }
    allocator_->free(index_cols_);
    index_cols_ = nullptr;
    index_column_count_ = 0;
  }
}

void ObDictTableMeta::free_rowkey_info_()
{
  if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(rowkey_cols_)) {
    for (int i = 0; i < rowkey_column_count_; i++) {
      common::ObRowkeyColumn *rowkey_col = rowkey_cols_ + i;
      if (OB_NOT_NULL(rowkey_col)) {
        rowkey_col->~ObIndexColumn();
      }
    }
    allocator_->free(rowkey_cols_);
    rowkey_cols_ = nullptr;
    rowkey_column_count_ = 0;
  }
}

void ObDictTableMeta::free_column_info_()
{
  if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(col_metas_)) {
    for (int i = 0; i < column_count_; i++) {
      ObDictColumnMeta *col_meta = col_metas_ + i;
      if (OB_NOT_NULL(col_meta)) {
        col_meta->~ObDictColumnMeta();
      }
    }
    allocator_->free(col_metas_);
    col_metas_ = nullptr;
    column_count_ = 0;
  }
}

int ObDictTableMeta::get_rowkey_info(ObRowkeyInfo &rowkey_info) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(rowkey_info.get_size() != 0)) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "invalid arguments", KR(ret), K(rowkey_info), KPC(this));
  } else if (OB_UNLIKELY(rowkey_column_count_ <= 0)) {
    // ignore cause table doesn't have rowkey_column.
  } else if (OB_FAIL(rowkey_info.reserve(rowkey_column_count_))) {
    DDLOG(WARN, "reserve rowkey_info size failed", KR(ret), K_(rowkey_column_count));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < rowkey_column_count_; i++) {
      const ObRowkeyColumn &rowkey_column = rowkey_cols_[i];
      if (OB_UNLIKELY(! rowkey_column.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        DDLOG(WARN, "rowkey_column is not valid", KR(ret), K(i), K(rowkey_column), KPC(this));
      } else if (OB_FAIL(rowkey_info.add_column(rowkey_column))) {
        DDLOG(WARN, "add_column to rowkey_info failed", KR(ret), K(i), K(rowkey_column), KPC(this));
      }
    }
  }

  return ret;
}

int ObDictTableMeta::get_index_info(ObIndexInfo &index_info) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(index_info.get_size() != 0)) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "expect empty index_info", KR(ret), K(index_info), KPC(this));
  } else if (OB_UNLIKELY(index_column_count_ <= 0)) {
    // ignore cause table doesn't have index_column
  } else {
    for (int i = 0; OB_SUCC(ret) && i < index_column_count_; i++) {
      const ObIndexColumn &index_column = index_cols_[i];
      if (OB_UNLIKELY(! index_column.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        DDLOG(WARN, "index_column is not valid", KR(ret), K(i), K(index_column), KPC(this));
      } else if (OB_FAIL(index_info.add_column(index_column))) {
        DDLOG(WARN, "add_column to index_info failed", KR(ret), K(i), K(index_column), KPC(this));
      }
    }
  }

  return ret;
}

int ObDictTableMeta::get_simple_index_infos(ObIArray<ObAuxTableMetaInfo> &simple_index_infos_array) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(0 != simple_index_infos_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "expect empty input simple_index_infos_array", KR(ret), K(simple_index_infos_array));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < unique_index_tid_arr_.count(); i++) {
      const uint64_t unique_index_tid = unique_index_tid_arr_.at(i);
      ObAuxTableMetaInfo info;
      // NOTICE: won't set table_type and index_type !!!
      info.table_id_ = unique_index_tid;

      if (OB_UNLIKELY(unique_index_tid == OB_INVALID_ID)) {
        ret = OB_ERR_UNEXPECTED;
        DDLOG(WARN, "invalid unique_index_tid", KR(ret), K(unique_index_tid), K(i), K_(unique_index_tid_arr), KPC(this));
      } else if (OB_FAIL(simple_index_infos_array.push_back(info))) {
        DDLOG(WARN, "push_back into simple_index_infos_array failed", KR(ret), K(info), K(i), K_(unique_index_tid_arr), KPC(this));
      }
    }
  }

  return ret;
}

int ObDictTableMeta::get_column_ids(ObIArray<ObColDesc> &column_ids, bool no_virtual) const
{
  int ret = OB_SUCCESS;

  // first push rowkey columns.
  for (int i = 0; OB_SUCC(ret) && i < rowkey_column_count_; i++) {
    const ObRowkeyColumn rowkey_column = rowkey_cols_[i];

    if (OB_UNLIKELY(! rowkey_column.is_valid())) {
      DDLOG(WARN, "rowkey_column is not valid", KR(ret), K(i), K(rowkey_column), KPC(this));
    } else {
      ObColDesc col_desc;
      col_desc.col_id_ = rowkey_column.column_id_;
      col_desc.col_type_ = rowkey_column.type_;
      col_desc.col_order_ = rowkey_column.order_;

      if (OB_FAIL(column_ids.push_back(col_desc))) {
        DDLOG(WARN, "Fail to add rowkey column to column_ids", KR(ret), K(i), K(rowkey_column), K(col_desc), KPC(this));
      }
    }
  }

  // then push non-rowkey columns.
  for (int i = 0; OB_SUCC(ret) &&i < column_count_; i++) {
    const ObDictColumnMeta column_meta = col_metas_[i];

    if (column_meta.is_rowkey_column()
        || (no_virtual && column_meta.is_virtual_generated_column())) {
      DDLOG(DEBUG, "skip non-rowkey_column and virtual_generate_column", K(no_virtual), K(column_meta), KPC(this));
    } else {
      ObColDesc col_desc;
      col_desc.col_id_ = column_meta.get_column_id();
      col_desc.col_type_ = column_meta.get_meta_type();
      //for non-rowkey, col_desc.col_order_ is not meaningful

      if (OB_FAIL(column_ids.push_back(col_desc))) {
        DDLOG(WARN, "Fail to add non-rowkey column to column_ids", KR(ret), K(i), K(column_meta), K(col_desc), KPC(this));
      }
    }
  }

  return ret;
}

int ObDictTableMeta::get_column_meta(const uint64_t column_id, const ObDictColumnMeta *&column_meta) const
{
  int ret = OB_SUCCESS;
  bool found = false;

  for (int i = 0; OB_SUCC(ret) && i < column_count_ && ! found; i++) {
    const ObDictColumnMeta &col_meta = col_metas_[i];
    if (col_meta.get_column_id() == column_id) {
      column_meta = &col_meta;
      found = true;
    }
  }

  if (OB_SUCC(ret)) {
    if (! found) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_ISNULL(column_meta)) {
      ret = OB_ERR_UNEXPECTED;
      DDLOG(WARN, "column_meta should be valid", KR(ret), K(column_id), KP(column_meta), KPC(this));
    }
  }

  return ret;
}

const ObDictColumnMeta *ObDictTableMeta::get_column_schema(const uint64_t column_id) const
{
  int ret = OB_SUCCESS;
  const ObDictColumnMeta *column_meta = NULL;

  if (OB_FAIL(get_column_meta(column_id, column_meta))) {
    DDLOG(WARN, "get_column_meta failed", KR(ret), K(column_id), KPC(this));
  }

  return column_meta;
}

} // namespace datadict
} // namespace oceanbase
