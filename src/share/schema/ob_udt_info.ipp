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
 *
 * Template implementations for ObUDTTypeInfo provider helpers.
 * Follow ob_schema_retrieve_utils.ipp: use SHARE_SCHEMA_LOG in header-included
 * templates, do not rely on USING_LOG_PREFIX / LOG_WARN.
 */

#ifndef OCEANBASE_SHARE_SCHEMA_OB_UDT_INFO_IPP_
#define OCEANBASE_SHARE_SCHEMA_OB_UDT_INFO_IPP_

namespace oceanbase
{
namespace share
{
namespace schema
{

template <typename SCHEMA_PROVIDER>
int ObUDTTypeInfo::check_dependency_valid(SCHEMA_PROVIDER &schema_provider) const
{
  int ret = OB_SUCCESS;

#define CHECK_ELEM_MATCH(elem_id, type_info)                                                                           \
  do {                                                                                                                 \
    const ObUDTTypeInfo *elem_info = nullptr;                                                                          \
    uint64_t mask_elem_id = (elem_id);                                                                                 \
    mask_elem_id = mask_elem_id & ~(OB_MOCK_TRIGGER_PACKAGE_ID_MASK);                                                  \
    mask_elem_id = mask_elem_id & ~(OB_MOCK_OBJECT_PACAKGE_ID_MASK);                                                   \
    mask_elem_id = mask_elem_id & ~(OB_MOCK_PACKAGE_BODY_ID_MASK);                                                     \
    mask_elem_id = mask_elem_id & ~(OB_MOCK_DBLINK_UDT_ID_MASK);                                                       \
    if (OB_INVALID_ID == (elem_id)) {                                                                                  \
      ret = OB_ERR_UNEXPECTED;                                                                                         \
      SHARE_SCHEMA_LOG(WARN, "unexpected invalid element type id", K(ret), K(*this), KPC(type_info));                  \
    } else if (is_inner_pl_object_id(mask_elem_id)) {                                                                  \
      /* sys/inner pl object, same as pl::get_tenant_id_by_object_id == OB_SYS_TENANT_ID */                            \
    } else if (OB_FAIL(schema_provider.get_udt_info(tenant_id_, elem_id, elem_info))) {                                \
      SHARE_SCHEMA_LOG(WARN, "failed to get_udt_info", K(ret), K(*this), K(elem_id), KPC(elem_info));                  \
    } else if (OB_ISNULL(elem_info)) {                                                                                 \
      /* dependency is dropped, the object must be invalid, do nothing */                                              \
    } else if ((type_info)->is_coll_type() && !elem_info->is_collection() ||                                           \
               (type_info)->is_obj_type() && !elem_info->is_obj_type()) {                                              \
        ret = OB_ERR_OBJECT_INVALID;                                                                                   \
        SHARE_SCHEMA_LOG(WARN, "original element type is replaced by another type",                                    \
                 K(ret), K(*this), K(elem_id), KPC(type_info), K((type_info)->get_typecode()), KPC(elem_info));        \
        LOG_USER_ERROR(OB_ERR_OBJECT_INVALID, get_type_name().length(), get_type_name().ptr());                         \
    }                                                                                                                  \
  } while (0)

  if (is_collection()) {
    if (OB_ISNULL(coll_info_)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "unexpected NULL coll_info_ of collection type", K(ret), K(*this));
    } else if (coll_info_->is_base_type()) {
      // do nothing
    } else {
      CHECK_ELEM_MATCH(coll_info_->get_elem_type_id(), coll_info_);
    }
  } else if (is_obj_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_attrs().count(); ++i) {
      const ObUDTTypeAttr *curr = get_attrs().at(i);

      if (OB_ISNULL(curr)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "unexpected NULL attr", K(ret), K(i), K(*this));
      } else if (curr->is_base_type()) {
        // do nothing
      } else {
        CHECK_ELEM_MATCH(curr->get_type_attr_id(), curr);
      }
    }
  }

#undef CHECK_ELEM_MATCH
  return ret;
}

template <typename SCHEMA_PROVIDER>
int ObUDTTypeInfo::transform_to_pl_type(const ObUDTTypeAttr* attr_info,
                                                      SCHEMA_PROVIDER &schema_provider,
                                                      pl::ObPLDataType &pl_type) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(attr_info)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "attr info is NULL", K(ret), K(attr_info));
  } else if (attr_info->is_base_type()) {
    common::ObDataType data_type;
    data_type.set_obj_type(static_cast<common::ObObjType>(attr_info->get_type_attr_id()));
    data_type.set_length(static_cast<int32_t>(attr_info->get_length()));
    data_type.set_precision(static_cast<int16_t>(attr_info->get_precision()));
    data_type.set_scale(static_cast<int16_t>(attr_info->get_scale()));
    data_type.set_zero_fill(static_cast<const bool>(attr_info->get_zero_fill()));
    data_type.set_collation_type(static_cast<const common::ObCollationType>(attr_info->get_coll_type()));
    data_type.set_charset_type(common::ObCharset::charset_type_by_coll(data_type.get_collation_type()));
    pl_type.set_data_type(data_type);
  } else if (attr_info->is_obj_type()) {
    pl_type.set_user_type_id(pl::ObPLType::PL_RECORD_TYPE, attr_info->get_type_attr_id());
    pl_type.set_type_from(pl::ObPLTypeFrom::PL_TYPE_UDT);
#ifdef OB_BUILD_ORACLE_PL
  } else if (attr_info->is_coll_type()) {
    const ObUDTTypeInfo *udt_info = NULL;
    pl::ObPLType type = pl::ObPLType::PL_NESTED_TABLE_TYPE;
    if (OB_FAIL(schema_provider.get_udt_info(attr_info->get_tenant_id(), attr_info->get_type_attr_id(), udt_info))) {
      SHARE_SCHEMA_LOG(WARN, "get udt info fail", K(ret), KPC(attr_info));
    } else {
      if (OB_NOT_NULL(udt_info)) {
        type = udt_info->is_varray() ? pl::ObPLType::PL_VARRAY_TYPE : pl::ObPLType::PL_NESTED_TABLE_TYPE;
      }
      pl_type.set_user_type_id(type, attr_info->get_type_attr_id());
      pl_type.set_type_from(pl::ObPLTypeFrom::PL_TYPE_UDT);
    }
  } else if (attr_info->is_opaque_type()) {
    pl_type.set_user_type_id(pl::ObPLType::PL_OPAQUE_TYPE, attr_info->get_type_attr_id());
    pl_type.set_type_from(pl::ObPLTypeFrom::PL_TYPE_UDT);
#endif
  } else {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "attr info type is invalid", K(ret), KPC(attr_info));
  }
  return ret;
}

template <typename SCHEMA_PROVIDER>
int ObUDTTypeInfo::transform_to_pl_type(common::ObIAllocator &allocator,
                                                      SCHEMA_PROVIDER &schema_provider,
                                                      const pl::ObUserDefinedType *&pl_type) const
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  pl::ObUserDefinedType *local_pl_type = NULL;
  pl_type = NULL;
#ifdef OB_BUILD_ORACLE_PL
  if (OB_FAIL(check_dependency_valid(schema_provider))) {
    SHARE_SCHEMA_LOG(WARN, "failed to check_dependency_valid", K(ret));
  } else if (is_collection()) {
    pl::ObCollectionType *table_type = NULL;
    pl::ObPLDataType elem_type;
    if (OB_ISNULL(ptr = allocator.alloc(is_varray() ? sizeof(pl::ObVArrayType) : sizeof(pl::ObCollectionType)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_SCHEMA_LOG(WARN, "failed to allocate memory for ObNestedTableType", K(ret));
    } else if (OB_FAIL(transform_to_pl_type(coll_info_, elem_type))) {
      SHARE_SCHEMA_LOG(WARN, "failed get collection elem type", K(ret));
    } else {
      if (is_varray()) {
        table_type = static_cast<pl::ObCollectionType*>(new(ptr)pl::ObVArrayType());
        pl::ObVArrayType *vt = static_cast<pl::ObVArrayType*>(ptr);
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
      SHARE_SCHEMA_LOG(WARN, "failed to allocate memory for ObOpaqueType", K(ret));
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
      SHARE_SCHEMA_LOG(WARN, "failed to allocate memory for ObRecordType", K(ret));
    } else {
      record_type = new(ptr)pl::ObRecordType();
      record_type->set_user_type_id(get_type_id());
      record_type->set_type_from(pl::ObPLTypeFrom::PL_TYPE_UDT);
      if (OB_FAIL(record_type->record_members_init(&allocator, get_attrs_count()))) {
        SHARE_SCHEMA_LOG(WARN, "record_members_init fail", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < get_attrs_count(); ++i) {
        pl::ObPLDataType attr_type;
        common::ObString copy_attr_name;
        if (OB_ISNULL(get_attrs().at(i))) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_SCHEMA_LOG(WARN, "attribute info is NULL", K(ret), K(i));
        } else if (OB_FAIL(transform_to_pl_type(get_attrs().at(i), schema_provider, attr_type))) {
          SHARE_SCHEMA_LOG(WARN, "failed to transform to pl type from ObUDTTypeAttr", K(ret));
        } else if (OB_FAIL(deep_copy_name(allocator, get_attrs().at(i)->get_name(), copy_attr_name))) {
          SHARE_SCHEMA_LOG(WARN, "failed to deep copy attribute name", K(ret));
        } else if (OB_FAIL(record_type->add_record_member(copy_attr_name, attr_type))) {
          SHARE_SCHEMA_LOG(WARN, "failed to add record member", K(ret));
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
    common::ObString copy_type_name;
    if (OB_FAIL(deep_copy_name(allocator, get_type_name(), copy_type_name))) {
      SHARE_SCHEMA_LOG(WARN, "failed to deep copy type name", K(ret));
    } else if (OB_ISNULL(local_pl_type)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "local_pl_type is null", K(ret));
    } else {
      local_pl_type->set_name(copy_type_name);
      pl_type = local_pl_type;
    }
  }
  return ret;
}

}  // namespace schema
}  // namespace share
}  // namespace oceanbase

#endif /* OCEANBASE_SHARE_SCHEMA_OB_UDT_INFO_IPP_ */
