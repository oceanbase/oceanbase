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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_udt_info.h"
#include "pl/ob_pl_type.h"
#include "pl/ob_pl_stmt.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_package_info.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

ObUDTBase &ObUDTBase::operator=(const ObUDTBase &src_schema)
{
  if (this != &src_schema) {
    reset();
    tenant_id_ = src_schema.tenant_id_;
    schema_version_ = src_schema.schema_version_;
    properties_ = src_schema.properties_;
    charset_id_ = src_schema.charset_id_;
    charset_form_ = src_schema.charset_form_;
    length_ = src_schema.length_;
    precision_ = src_schema.precision_;
    scale_ = src_schema.scale_;
    zero_fill_ = src_schema.zero_fill_;
    coll_type_ = src_schema.coll_type_;
  }
  return *this;
}

void ObUDTBase::reset()
{
  tenant_id_ = OB_INVALID_ID;
  schema_version_ = 0;
  properties_ = 0;
  charset_id_ = OB_INVALID_ID;
  charset_form_ = OB_INVALID_ID;
  length_ = OB_INVALID_ID;
  precision_ = OB_INVALID_ID;
  scale_ = OB_INVALID_ID;
  zero_fill_ = 0;
  coll_type_ = OB_INVALID_ID;
  ObSchema::reset();
}

ObUDTTypeAttr& ObUDTTypeAttr::operator=(const ObUDTTypeAttr &src_schema)
{
  int ret = OB_SUCCESS;
  if (this != &src_schema) {
    reset();
    *(static_cast<ObUDTBase*>(this)) = static_cast<const ObUDTBase&>(src_schema);
    type_attr_id_ = src_schema.type_attr_id_;
    type_id_ = src_schema.type_id_;
    attribute_ = src_schema.attribute_;
    xflags_ = src_schema.xflags_;
    setter_ = src_schema.setter_;
    getter_ = src_schema.getter_;
    if (OB_FAIL(deep_copy_str(src_schema.name_, name_))) {
      LOG_WARN("deep copy name failed", K(ret), K(src_schema.name_), K(name_));
    } else if (OB_FAIL(deep_copy_str(src_schema.externname_, externname_))) {
      LOG_WARN("deep copy externname faile", K(ret), K(src_schema.externname_), K(externname_));
    }
    error_ret_ = ret;
  }
  return *this;
}

void ObUDTTypeAttr::reset()
{
  type_attr_id_ = OB_INVALID_ID;
  type_id_ = OB_INVALID_ID;
  reset_string(name_);
  attribute_ = 0;
  reset_string(externname_);
  xflags_ = 0;
  setter_ = 0;
  getter_ = 0;
  ObUDTBase::reset();
}

int64_t ObUDTTypeAttr::get_convert_size() const
{
  int64_t len = 0;
  len += static_cast<int64_t>(sizeof(ObUDTTypeAttr));
  len += name_.length() + 1;
  len += externname_.length() + 1;
  return len;
}

void ObUDTObjectType::reset()
{
  database_id_ = OB_INVALID_ID;
  owner_id_ = OB_INVALID_ID;
  object_type_id_ = OB_INVALID_ID;
  reset_string(object_name_);
  type_ = INVALID_UDT_OBJECT_TYPE;
  flag_ = 0;
  comp_flag_ = 0;
  reset_string(exec_env_);
  reset_string(source_);
  reset_string(comment_);
  reset_string(route_sql_);
  ObUDTBase::reset();
}

ObUDTObjectType& ObUDTObjectType::operator=(const ObUDTObjectType &src_schema)
{
  int ret = OB_SUCCESS;
  if (this != &src_schema) {
    reset();
    *(static_cast<ObUDTBase*>(this)) = static_cast<const ObUDTBase&>(src_schema);
    database_id_ = src_schema.database_id_;
    owner_id_ = src_schema.owner_id_;
    object_type_id_ = src_schema.object_type_id_;
    type_ = src_schema.type_;
    flag_ = src_schema.flag_;
    comp_flag_ = src_schema.comp_flag_;
    if (OB_FAIL(deep_copy_str(src_schema.object_name_, object_name_))) {
      LOG_WARN("failed to copy object name", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_schema.exec_env_, exec_env_))) {
      LOG_WARN("failed to copy exec env", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_schema.source_, source_))) {
      LOG_WARN("failed to copy source", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_schema.comment_, comment_))) {
      LOG_WARN("failed to copy comment", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_schema.route_sql_, route_sql_))) {
      LOG_WARN("failed to copy route sql", K(ret));
    }
    error_ret_ = ret;
  }
  return *this;
}

int64_t ObUDTObjectType::get_convert_size() const
{
  int64_t len = 0;
  len += static_cast<int64_t>(sizeof(ObUDTObjectType));
  len += object_name_.length() + 1;
  len += exec_env_.length() + 1;
  len += source_.length() + 1;
  len += comment_.length() + 1;
  len += route_sql_.length() + 1;
  return len;
}

uint64_t ObUDTObjectType::get_object_type_id(uint64_t tenant_id) const {
  UNUSED(tenant_id);
  uint64_t object_id = get_coll_type();
  if (is_sys_tenant(pl::get_tenant_id_by_object_id(object_id))) {
    object_id = mask_object_id(object_id);
  }
  return object_id;
}

ObUDTCollectionType& ObUDTCollectionType::operator=(const ObUDTCollectionType &src_schema)
{
  int ret = OB_SUCCESS;
  if (this != &src_schema) {
    reset();
    *(static_cast<ObUDTBase*>(this)) = static_cast<const ObUDTBase&>(src_schema);
    coll_type_id_ = src_schema.coll_type_id_;
    elem_type_id_ = src_schema.elem_type_id_;
    elem_schema_version_ = src_schema.elem_schema_version_;
    upper_bound_ = src_schema.upper_bound_;
    package_id_ = src_schema.package_id_;
    if (OB_FAIL(deep_copy_str(src_schema.coll_name_, coll_name_))) {
      LOG_WARN("failed to deep copy string", K(src_schema.coll_name_), K(coll_name_));
    }
    error_ret_ = ret;
  }
  return *this;
}

void ObUDTCollectionType::reset()
{
  coll_type_id_ = OB_INVALID_ID;
  elem_type_id_ = OB_INVALID_ID;
  elem_schema_version_ = 0;
  upper_bound_ = OB_INVALID_ID;
  package_id_ = OB_INVALID_ID;
  reset_string(coll_name_);
  ObUDTBase::reset();
}

int64_t ObUDTCollectionType::get_convert_size() const
{
  int64_t len = 0;
  len += static_cast<int64_t>(sizeof(ObUDTCollectionType));
  len += coll_name_.length() + 1;
  return len;
}

ObUDTTypeInfo::ObUDTTypeInfo(common::ObIAllocator *allocator)
  : ObSchema(allocator),
    type_attrs_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(*allocator)),
    object_type_infos_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(*allocator))
{
  reset();
}

ObUDTTypeInfo& ObUDTTypeInfo::operator=(const ObUDTTypeInfo &src_schema)
{
  int ret = OB_SUCCESS;
  if (this != &src_schema) {
    reset();
    tenant_id_ = src_schema.tenant_id_;
    database_id_ = src_schema.database_id_;
    schema_version_ = src_schema.schema_version_;
    type_id_ = src_schema.type_id_;
    typecode_ = src_schema.typecode_;
    properties_ = src_schema.properties_;
    attributes_ = src_schema.attributes_;
    methods_ = src_schema.methods_;
    hiddenmethods_ = src_schema.hiddenmethods_;
    supertypes_ = src_schema.supertypes_;
    externtype_ = src_schema.externtype_;
    local_attrs_ = src_schema.local_attrs_;
    local_methods_ = src_schema.local_methods_;
    supertypeid_ = src_schema.supertypeid_;
    package_id_ = src_schema.package_id_;
    if (OB_FAIL(deep_copy_str(src_schema.externname_, externname_))) {
      LOG_WARN("failed to deep copy str externname", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_schema.helperclassname_, helperclassname_))) {
      LOG_WARN("failed to deep copy str helperclassname", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_schema.type_name_, type_name_))) {
      LOG_WARN("failed to deep copy str type name", K(ret));
    }else if (OB_NOT_NULL(src_schema.coll_info_)
              && OB_FAIL(set_coll_info(*(src_schema.coll_info_)))) {
      LOG_WARN("failed to set coll info", K(ret));
    } else if (OB_FAIL(type_attrs_.reserve(src_schema.type_attrs_.count()))) {
      LOG_WARN("failed to reserve memory for type attrs", K(ret), K(src_schema));
    } else if (OB_FAIL(object_type_infos_.reserve(src_schema.object_type_infos_.count()))) {
      LOG_WARN("failed to reserve memory for object type infos", K(ret), K(src_schema));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < src_schema.type_attrs_.count(); ++i) {
      if (OB_ISNULL(src_schema.type_attrs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type attribute is null", K(ret));
      } else if (OB_FAIL(add_type_attr(*(src_schema.type_attrs_.at(i))))) {
        LOG_WARN("failed to add type attr", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < src_schema.object_type_infos_.count(); ++i) {
      if (OB_ISNULL(src_schema.object_type_infos_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("object type info is null", K(ret));
      } else if (OB_FAIL(add_object_type_info(*(src_schema.object_type_infos_.at(i))))) {
        LOG_WARN("failed to add object type info", K(ret));
      }
    }
    error_ret_ = ret;
  }
  return *this;
}

int ObUDTTypeInfo::assign(const ObUDTTypeInfo &other)
{
  int ret = OB_SUCCESS;
  this->operator=(other);
  ret = this->error_ret_;
  return ret;
}

void ObUDTTypeInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  type_id_ = OB_INVALID_ID;
  typecode_ = 0; // TODO
  properties_ = 0;
  attributes_ = 0;
  methods_ = 0;
  hiddenmethods_ = 0;
  supertypes_ = 0;
  subtypes_ = 0;
  externtype_ = 0;
  reset_string(externname_);
  reset_string(helperclassname_);
  local_attrs_ = 0;
  local_methods_ = 0;
  supertypeid_ = OB_INVALID_ID;
  reset_string(type_name_);
  package_id_ = OB_INVALID_ID;
  coll_info_ = NULL;
  type_attrs_.reset();
  object_type_infos_.reset();
  ObSchema::reset();
}

int64_t ObUDTTypeInfo::get_convert_size() const
{
  int64_t len = 0;
  len += static_cast<int64_t>(sizeof(ObUDTTypeInfo));
  len += externname_.length() + 1;
  len += helperclassname_.length() + 1;
  len += type_name_.length() + 1;
  len += (type_attrs_.count() + 1) * sizeof(ObUDTTypeAttr *);
  len += 2 * sizeof(ObUDTObjectType *);
  if (OB_NOT_NULL(coll_info_)) {
    len += coll_info_->get_convert_size();
  }
  len += type_attrs_.get_data_size();
  for (int64_t i = 0; i < type_attrs_.count(); ++i) {
    if (OB_NOT_NULL(type_attrs_.at(i))) {
      len += type_attrs_.at(i)->get_convert_size();
    }
  }
  len += object_type_infos_.get_data_size();
  for (int64_t i = 0; i < object_type_infos_.count(); ++i) {
    if (OB_NOT_NULL(object_type_infos_.at(i))) {
      len += object_type_infos_.at(i)->get_convert_size();
    }
  }
  return len;
}

int ObUDTTypeInfo::set_coll_info(const ObUDTCollectionType& coll_info)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(ptr = alloc(sizeof(ObUDTCollectionType)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc udt attr type", K(ret));
  } else {
    coll_info_ = new(ptr)ObUDTCollectionType(get_allocator());
    *coll_info_ = coll_info;
  }
  return ret;
}

int ObUDTTypeInfo::add_type_attr(const ObUDTTypeAttr& type_attr)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(ptr = alloc(sizeof(ObUDTTypeAttr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc udt attr type", K(ret));
  } else {
    ObUDTTypeAttr *attr = new(ptr)ObUDTTypeAttr(get_allocator());
    *attr = type_attr;
    if (OB_FAIL(type_attrs_.push_back(attr))) {
      LOG_WARN("failed to push back", K(ret), K(type_attrs_.count()), K(sizeof(ObUDTTypeAttr)));
    } else {
      attributes_++;
    }
  }
  return ret;
}

int ObUDTTypeInfo::add_object_type_info(const ObUDTObjectType &udt_object)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(ptr = alloc(sizeof(ObUDTObjectType)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc udt object type", K(ret));
  } else {
    ObUDTObjectType *obj_type = new(ptr)ObUDTObjectType(get_allocator());
    *obj_type = udt_object;
    if (OB_FAIL(object_type_infos_.push_back(obj_type))) {
      LOG_WARN("failed to push back udt object type", K(ret),
                  K(object_type_infos_.count()), K(sizeof(ObUDTObjectType)));
    } else {
      // do nothing
    }
  }

  return ret;
}

bool ObUDTTypeInfo::is_object_type_info_exist(const ObUDTObjectType &udt_object) const
{
  bool bool_ret = false;
  ObUDTObjectType *obj = NULL;
  for (int64_t i = 0; !bool_ret && i < object_type_infos_.count(); ++i) {
    obj = object_type_infos_.at(i);
    if (OB_NOT_NULL(obj)) {
      bool_ret = obj->is_object_equal(udt_object);
    }
  }
  return bool_ret;
}

int ObUDTTypeInfo::set_object_ddl_type(ObUDTTypeFlag type_flag)
{
  int ret = OB_SUCCESS;
  if (is_object_type()) {
    properties_ |= type_flag;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udt is not object type, cant set objec type spec prop",
             K(ret), K(properties_), K(type_flag), K(typecode_));
  }
  return ret;
}

const ObUDTObjectType *ObUDTTypeInfo::get_object_info() const
{
#define GET_OBJ_TYPE_INFO(type) \
do{ \
    for(int64_t i = 0; i < object_type_infos_.count(); ++i) { \
      tmp = object_type_infos_.at(i); \
      if (OB_NOT_NULL(tmp) && static_cast<int64_t>(type) == tmp->get_type()) { \
        res = tmp; \
        break; \
      } \
    } \
} while(0)
  const ObUDTObjectType *res = NULL;
  const ObUDTObjectType *tmp = NULL;
  if (is_object_spec_ddl()) {
    GET_OBJ_TYPE_INFO(UDT_OBJECT_SPEC_TYPE);
  } else if (is_object_body_ddl()) {
    GET_OBJ_TYPE_INFO(UDT_OBJECT_BODY_TYPE);
  } else {
    res = NULL;
  }
  return res;
}

int ObUDTObjectType::to_package_info(ObPackageInfo &pkg_info) const
{
  int ret = OB_SUCCESS;
  OZ (pkg_info.set_exec_env(get_exec_env()));
  OZ (pkg_info.set_source(get_source()));
  OZ (pkg_info.set_comment(get_comment()));
  OZ (pkg_info.set_route_sql(get_route_sql()));
  if (OB_SUCC(ret)) {
    pkg_info.set_tenant_id(get_tenant_id());
    pkg_info.set_database_id(get_database_id());
    pkg_info.set_owner_id(get_owner_id());
    pkg_info.set_flag(get_flag());
    pkg_info.set_comp_flag(get_comp_flag());
    pkg_info.set_type(static_cast<share::schema::ObPackageType>(get_type()));
  }
  return ret;
}

int ObUDTTypeInfo::deep_copy_name(common::ObIAllocator &allocator,
                                  const common::ObString &src,
                                  common::ObString &dst) const
{
  int ret = common::OB_SUCCESS;
  int32_t size = src.length() + 1;
  char* buf = static_cast<char *>(allocator.alloc(size));
  if (OB_ISNULL(buf)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret), K(size), K(src));
  } else {
    MEMCPY(buf, src.ptr(), src.length());
    buf[size-1] = '\0';
    dst.assign_ptr(buf, src.length());
  }
  return ret;
}

int ObUDTTypeInfo::transform_to_pl_type(const ObUDTCollectionType *coll_info, pl::ObPLDataType &pl_type) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(coll_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("coll info is null", K(ret), K(coll_info));
  } else if (coll_info->is_base_type()) {
    common::ObDataType data_type;
    data_type.set_obj_type(static_cast<ObObjType>(coll_info->get_elem_type_id()));
    data_type.set_length(static_cast<int32_t>(coll_info->get_length()));
    data_type.set_precision(static_cast<int16_t>(coll_info->get_precision()));
    data_type.set_scale(static_cast<int16_t>(coll_info->get_scale()));
    data_type.set_zero_fill(static_cast<const bool>(coll_info->get_zero_fill()));
    data_type.set_collation_type(static_cast<const ObCollationType>(coll_info->get_coll_type()));
    data_type.set_charset_type(ObCharset::charset_type_by_coll(data_type.get_collation_type()));
    pl_type.set_data_type(data_type);
  } else if (coll_info->is_coll_type()) {
    pl_type.set_user_type_id(pl::ObPLType::PL_NESTED_TABLE_TYPE, coll_info->get_elem_type_id());
    pl_type.set_type_from(pl::ObPLTypeFrom::PL_TYPE_UDT);
  } else if (coll_info->is_obj_type()) {
    pl_type.set_user_type_id(pl::ObPLType::PL_RECORD_TYPE, coll_info->get_elem_type_id());
    pl_type.set_type_from(pl::ObPLTypeFrom::PL_TYPE_UDT);
    pl_type.set_type(pl::ObPLType::PL_RECORD_TYPE);
  } else if (coll_info->is_opaque_type()) {
    pl_type.set_user_type_id(pl::ObPLType::PL_OPAQUE_TYPE, coll_info->get_elem_type_id());
    pl_type.set_type_from(pl::ObPLTypeFrom::PL_TYPE_UDT);
    pl_type.set_type(pl::ObPLType::PL_OPAQUE_TYPE);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("coll info type is invalid", K(ret), K(pl_type), KPC(coll_info));
  }
  OX (pl_type.set_not_null(coll_info->is_not_null()));
  return ret;
}

int ObUDTTypeInfo::transform_to_pl_type(const ObUDTTypeAttr* attr_info, pl::ObPLDataType &pl_type) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(attr_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attr info is NULL", K(ret), K(attr_info));
  } else if (attr_info->is_base_type()) {
    common::ObDataType data_type;
    data_type.set_obj_type(static_cast<ObObjType>(attr_info->get_type_attr_id()));
    data_type.set_length(static_cast<int32_t>(attr_info->get_length()));
    data_type.set_precision(static_cast<int16_t>(attr_info->get_precision()));
    data_type.set_scale(static_cast<int16_t>(attr_info->get_scale()));
    data_type.set_zero_fill(static_cast<const bool>(attr_info->get_zero_fill()));
    data_type.set_collation_type(static_cast<const ObCollationType>(attr_info->get_coll_type()));
    data_type.set_charset_type(ObCharset::charset_type_by_coll(data_type.get_collation_type()));
    pl_type.set_data_type(data_type);
  } else if (attr_info->is_obj_type()) {
    pl_type.set_user_type_id(pl::ObPLType::PL_RECORD_TYPE,
                             attr_info->get_type_attr_id());
    pl_type.set_type_from(pl::ObPLTypeFrom::PL_TYPE_UDT);
#ifdef OB_BUILD_ORACLE_PL
  } else if (attr_info->is_coll_type()) {
    pl_type.set_user_type_id(pl::ObPLType::PL_NESTED_TABLE_TYPE,
                             attr_info->get_type_attr_id());
    pl_type.set_type_from(pl::ObPLTypeFrom::PL_TYPE_UDT);
  } else if (attr_info->is_opaque_type()) {
    pl_type.set_user_type_id(pl::ObPLType::PL_OPAQUE_TYPE,
                             attr_info->get_type_attr_id());
    pl_type.set_type_from(pl::ObPLTypeFrom::PL_TYPE_UDT);
#endif
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attr info type is invalid", K(ret), KPC(attr_info));
  }
  return ret;
}

int ObUDTTypeInfo::transform_to_pl_type(common::ObIAllocator &allocator, const pl::ObUserDefinedType *&pl_type) const
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  pl::ObUserDefinedType *local_pl_type = NULL;
  pl_type = NULL;
#ifdef OB_BUILD_ORACLE_PL
  if (is_collection()) {
    pl::ObCollectionType *table_type = NULL;
    pl::ObPLDataType elem_type;
    if (OB_ISNULL(ptr = allocator.alloc(is_varray() ? sizeof(pl::ObVArrayType) : sizeof(pl::ObCollectionType)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ObNestedTableType", K(ret));
    } else if (OB_FAIL(transform_to_pl_type(coll_info_, elem_type))) {
      LOG_WARN("failed get collection elem type", K(ret));
    } else {
      if (is_varray()) {
        table_type = static_cast<pl::ObCollectionType*>(new(ptr)pl::ObVArrayType());
        pl::ObVArrayType *vt = static_cast<pl::ObVArrayType*> (ptr);
        //
        vt->set_capacity(coll_info_->get_upper_bound());
      } else {
        table_type = static_cast<pl::ObCollectionType*>(new(ptr)pl::ObNestedTableType());
      }
      table_type->set_user_type_id(get_type_id());
      table_type->set_type_from(pl::ObPLTypeFrom::PL_TYPE_UDT);
      table_type->set_element_type(elem_type);
      local_pl_type = table_type;
    }
  } else if (is_opaque()) {
    pl::ObOpaqueType *opaque_type = NULL;
    if (OB_ISNULL(ptr = allocator.alloc(sizeof(pl::ObOpaqueType)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ObOpaqueType", K(ret));
    } else {
      opaque_type = new(ptr)pl::ObOpaqueType();
      opaque_type->set_user_type_id(get_type_id());
      opaque_type->set_type_from(pl::ObPLTypeFrom::PL_TYPE_UDT);
      local_pl_type = opaque_type;
    }
  } else {
#endif
    pl::ObRecordType *record_type = NULL;
    if (OB_ISNULL(ptr = allocator.alloc(sizeof(pl::ObRecordType)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ObRecordType", K(ret));
    } else {
      record_type = new(ptr)pl::ObRecordType();
      record_type->set_user_type_id(get_type_id());
      record_type->set_type_from(pl::ObPLTypeFrom::PL_TYPE_UDT);
      OZ (record_type->record_members_init(&allocator, get_attrs_count()));
      for (int64_t i = 0; OB_SUCC(ret) && i < get_attrs_count(); ++i) {
        pl::ObPLDataType attr_type;
        ObString copy_attr_name;
        if (OB_ISNULL(get_attrs().at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("attribute info is NULL", K(ret), K(i));
        } else if (OB_FAIL(transform_to_pl_type(get_attrs().at(i), attr_type))) {
          LOG_WARN("failed to transform to pl type from ObUDTTypeAttr", K(ret));
        } else if (OB_FAIL(deep_copy_name(allocator, get_attrs().at(i)->get_name(), copy_attr_name))) {
          LOG_WARN("failed to deep copy attribute name", K(ret));
        } else if (OB_FAIL(record_type->add_record_member(copy_attr_name, attr_type))) {
          LOG_WARN("failed to add record member", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        local_pl_type = record_type;
      }
    }
#ifdef OB_BUILD_ORACLE_PL
  }
#endif
  if (OB_SUCC(ret)) {
    ObString copy_type_name;
    if (OB_FAIL(deep_copy_name(allocator, get_type_name(), copy_type_name))) {
      LOG_WARN("failed to deep copy type name", K(ret));
    } else if (OB_UNLIKELY(OB_ISNULL(local_pl_type))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("local_pl_type is null", K(ret));
    } else {
      local_pl_type->set_name(copy_type_name);
      pl_type = local_pl_type;
    }
  }
  return ret;
}

int ObUDTTypeInfo::copy_udt_info_in_require(const ObUDTTypeInfo &old_info)
{
  int ret = OB_SUCCESS;
  int64_t ac = old_info.get_attrs_count();
  for (int64_t i = 0; OB_SUCC(ret) && i < ac; ++i) {
    OZ (add_type_attr(*(old_info.get_attrs().at(i))));
  }

  if (OB_SUCC(ret)) {
    tenant_id_ = old_info.tenant_id_;
    database_id_ = old_info.database_id_;
    type_id_ = old_info.type_id_;
    methods_ = old_info.get_methods();
    hiddenmethods_ = old_info.get_hiddenmethods();
    supertypes_ = old_info.get_supertypes();
    subtypes_ = old_info.get_subtypes();
    supertypeid_ = old_info.get_supertypeid();
    local_methods_ = old_info.get_local_methods();
    local_attrs_ = old_info.get_local_attrs();
  }

  return ret;
}


OB_DEF_SERIALIZE(ObUDTBase)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              schema_version_,
              properties_,
              charset_id_,
              charset_form_,
              length_,
              precision_,
              scale_,
              zero_fill_,
              coll_type_);
  return ret;
}

OB_DEF_DESERIALIZE(ObUDTBase)
{
  int ret = OB_SUCCESS;
  reset();
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              schema_version_,
              properties_,
              charset_id_,
              charset_form_,
              length_,
              precision_,
              scale_,
              zero_fill_,
              coll_type_);
  return ret; 
}

OB_DEF_SERIALIZE_SIZE(ObUDTBase)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              schema_version_,
              properties_,
              charset_id_,
              charset_form_,
              length_,
              precision_,
              scale_,
              zero_fill_,
              coll_type_);
  return len;
}

OB_DEF_SERIALIZE(ObUDTTypeAttr)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)
      && ObUDTBase::serialize(buf, buf_len, pos)) {
    LOG_WARN("serialize udt type attr failed", K(ret));
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
              type_attr_id_,
              type_id_,
              name_,
              attribute_,
              externname_,
              xflags_,
              setter_,
              getter_);
  return ret;
}

OB_DEF_DESERIALIZE(ObUDTTypeAttr)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_SUCC(ret)
      && ObUDTBase::deserialize(buf, data_len, pos)) {
    LOG_WARN("deserialize udt type attr base", K(ret));
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              type_attr_id_,
              type_id_,
              name_,
              attribute_,
              externname_,
              xflags_,
              setter_,
              getter_);
  return ret; 
}

OB_DEF_SERIALIZE_SIZE(ObUDTTypeAttr)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              type_attr_id_,
              type_id_,
              name_,
              attribute_,
              externname_,
              xflags_,
              setter_,
              getter_);
  len += ObUDTBase::get_serialize_size();
  return len;
}

OB_DEF_SERIALIZE(ObUDTCollectionType)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)
      && ObUDTBase::serialize(buf, buf_len, pos)) {
    LOG_WARN("serialize udt type attr failed", K(ret));
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
              coll_type_id_,
              elem_type_id_,
              elem_schema_version_,
              upper_bound_,
              package_id_,
              coll_name_);
  return ret;
}

OB_DEF_DESERIALIZE(ObUDTCollectionType)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_SUCC(ret)
      && ObUDTBase::deserialize(buf, data_len, pos)) {
    LOG_WARN("deserialize udt type attr base", K(ret));
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              coll_type_id_,
              elem_type_id_,
              elem_schema_version_,
              upper_bound_,
              package_id_,
              coll_name_);
  return ret; 
}

OB_DEF_SERIALIZE_SIZE(ObUDTCollectionType)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              coll_type_id_,
              elem_type_id_,
              elem_schema_version_,
              upper_bound_,
              package_id_,
              coll_name_);
  len += ObUDTBase::get_serialize_size();
  return len;
}


OB_DEF_SERIALIZE(ObUDTObjectType)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)
      && ObUDTBase::serialize(buf, buf_len, pos)) {
    LOG_WARN("serialize udt type attr failed", K(ret));
  }

  LST_DO_CODE(OB_UNIS_ENCODE,
              database_id_,
              owner_id_,
              object_type_id_,
              object_name_,
              type_,
              flag_,
              comp_flag_,
              exec_env_,
              source_,
              comment_,
              route_sql_);
  return ret;
}

OB_DEF_DESERIALIZE(ObUDTObjectType)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_SUCC(ret)
      && ObUDTBase::deserialize(buf, data_len, pos)) {
    LOG_WARN("deserialize udt type attr base", K(ret));
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              database_id_,
              owner_id_,
              object_type_id_,
              object_name_,
              type_,
              flag_,
              comp_flag_,
              exec_env_,
              source_,
              comment_,
              route_sql_);
  return ret; 
}

OB_DEF_SERIALIZE_SIZE(ObUDTObjectType)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              database_id_,
              owner_id_,
              object_type_id_,
              object_name_,
              type_,
              flag_,
              comp_flag_,
              exec_env_,
              source_,
              comment_,
              route_sql_);
  len += ObUDTBase::get_serialize_size();
  return len;
}

OB_DEF_SERIALIZE(ObUDTTypeInfo)
{
  int ret = OB_SUCCESS;
  int64_t attr_cnt = type_attrs_.count();
  int64_t obj_type_cnt = object_type_infos_.count();
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              database_id_,
              schema_version_,
              type_id_,
              typecode_,
              properties_,
              attributes_,
              methods_,
              hiddenmethods_,
              supertypes_,
              subtypes_,
              externtype_,
              externname_,
              helperclassname_,
              local_attrs_,
              local_methods_,
              supertypeid_,
              type_name_,
              package_id_,
              attr_cnt);
  if (OB_SUCC(ret) && is_collection()) {
    if (OB_ISNULL(coll_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("collection info is null", K(ret));
    } else if (OB_FAIL(coll_info_->serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize collection info", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < attr_cnt; ++i) {
    if (OB_ISNULL(type_attrs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type attr is null", K(ret), K(i));
    } else if (OB_FAIL(type_attrs_.at(i)->serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize type attr failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, obj_type_cnt);
    if (OB_SUCC(ret) && is_object_type()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < obj_type_cnt; ++i) {
        if (OB_ISNULL(object_type_infos_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("object type info is null", K(ret), K(i));
        } else if (OB_FAIL(object_type_infos_.at(i)->serialize(buf, buf_len, pos))) {
          LOG_WARN("serialize object type info failed", K(ret));
        }
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObUDTTypeInfo)
{
  int ret = OB_SUCCESS;
  int64_t attr_cnt = 0;
  int64_t obj_type_cnt = 0;
  ObUDTTypeAttr attr;
  ObUDTCollectionType coll_info;
  ObUDTObjectType obj_info;
  reset();
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              database_id_,
              schema_version_,
              type_id_,
              typecode_,
              properties_,
              attributes_,
              methods_,
              hiddenmethods_,
              supertypes_,
              subtypes_,
              externtype_,
              externname_,
              helperclassname_,
              local_attrs_,
              local_methods_,
              supertypeid_,
              type_name_,
              package_id_,
              attr_cnt);
  if (OB_SUCC(ret) && is_collection()) {
    coll_info.reset();
    if (OB_FAIL(coll_info.deserialize(buf, data_len, pos))) {
      LOG_WARN("deserialize collection info failed", K(ret));
    } else if (OB_FAIL(set_coll_info(coll_info))) {
      LOG_WARN("failed to set coll info", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < attr_cnt; ++i) {
    attr.reset();
    if (OB_FAIL(attr.deserialize(buf, data_len, pos))) {
      LOG_WARN("deserialize attr type failed", K(ret));
    } else if (OB_FAIL(add_type_attr(attr))) {
      LOG_WARN("failed to add type attr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (pos == data_len) {
      // 这是老版本的数据，不需要处理。
    } else {
      LST_DO_CODE(OB_UNIS_DECODE, obj_type_cnt);
    }
    if (pos == data_len) {
    } else {
      if (OB_SUCC(ret) && is_object_type()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < obj_type_cnt; ++i) {
          obj_info.reset();
          if (OB_FAIL(obj_info.deserialize(buf, data_len, pos))) {
            LOG_WARN("deserialize object type info failed", K(ret));
          } else if (OB_FAIL(add_object_type_info(obj_info))) {
            LOG_WARN("failed to set object type info", K(ret));
          }
        }
      }
    }
  }
  return ret; 
}

OB_DEF_SERIALIZE_SIZE(ObUDTTypeInfo)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  int64_t attr_cnt = type_attrs_.count();
  int64_t obj_type_cnt = object_type_infos_.count();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_,
              database_id_,
              schema_version_,
              type_id_,
              typecode_,
              properties_,
              attributes_,
              methods_,
              hiddenmethods_,
              supertypes_,
              subtypes_,
              externtype_,
              externname_,
              helperclassname_,
              local_attrs_,
              local_methods_,
              supertypeid_,
              type_name_,
              package_id_,
              attr_cnt);
  if (OB_SUCC(ret) && is_collection()) {
    if (OB_ISNULL(coll_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("collection info is null", K(ret), K(coll_info_));
    } else {
      len += coll_info_->get_serialize_size();
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < attr_cnt; ++i) {
    const ObUDTTypeAttr* local_attr = type_attrs_.at(i);
    if (OB_ISNULL(local_attr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to cast", K(ret));
    } else {
      len += local_attr->get_serialize_size();
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, obj_type_cnt);
    if (OB_SUCC(ret) && is_object_type()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < obj_type_cnt; ++i) {
        const ObUDTObjectType* local_obj = object_type_infos_.at(i);
        if (OB_ISNULL(local_obj)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to cast", K(ret));
        } else {
          len += local_obj->get_serialize_size();
        }
      }
    }
  }
  return len;
}

}
}
}
