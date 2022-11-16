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

#ifndef OCEANBASE_SRC_SHARE_SCHEMA_OB_UDT_INFO_H_
#define OCEANBASE_SRC_SHARE_SCHEMA_OB_UDT_INFO_H_

#include "share/schema/ob_schema_struct.h"
#include "common/object/ob_object.h"
#include "lib/container/ob_fixed_array.h"
#include "pl/ob_pl_type.h"
#include "pl/ob_pl_user_type.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObPackageInfo;

enum ObUDTTypeCode
{
  UDT_INVALID_TYPE_CODE = 0,// 无效的类型
  UDT_TYPE_COLLECTION = 1,  // 用户自定义数组类型 
  UDT_TYPE_OBJECT = 2,      // 用户自定义记录类型
  UDT_TYPE_BASE = 3,        // 系统预定义类型
  UDT_TYPE_OBJECT_BODY = 4, // OBJECT TYPE BODY
  UDT_TYPE_OPAQUE = 5,      // OPAQUE(ANYDATA, XMLTYPE, ANYTYPE etc...)
};

// use ObUDTBase->properties_ highest bit record collection not null property.
#define COLL_NOT_NULL_FLAG 0x8000000000000000

enum ObUDTTypeFlag
{
  UDT_FLAG_INVALID = 1,
  UDT_FLAG_NONEDITIONABLE = 2,
  UDT_FLAG_OBJECT_TYPE_SPEC = 4,
  UDT_FLAG_OBJECT_TYPE_BODY = 8,
  UDT_FLAG_OBJECT_DEFAULT_CONS = 16, // deine a default cons
};

class ObUDTBase : public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  ObUDTBase() { reset(); }
  explicit ObUDTBase(common::ObIAllocator *allocator) : ObSchema(allocator) { reset(); }
  ObUDTBase(const ObUDTBase &src_schema) : ObSchema() { reset(); *this = src_schema; }
  virtual ~ObUDTBase() {}

  ObUDTBase& operator= (const ObUDTBase &src_schema); 
  virtual void reset();
  virtual int64_t get_convert_size() const { return static_cast<int64_t>(sizeof(ObUDTBase)); }

  //getter
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE int64_t get_schema_version() const { return schema_version_; }
  OB_INLINE int64_t get_properties() const { return properties_; }
  OB_INLINE int64_t get_charset_id() const { return charset_id_; }
  OB_INLINE int64_t get_charset_form() const { return charset_form_; }
  OB_INLINE int64_t get_length() const { return length_; }
  OB_INLINE int64_t get_precision() const { return precision_; }
  OB_INLINE int64_t get_scale() const { return scale_; }
  OB_INLINE int64_t get_zero_fill() const { return zero_fill_; }
  OB_INLINE int64_t get_coll_type() const { return coll_type_; }

  //setter
  OB_INLINE void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE void set_schema_version(int64_t schema_version) { schema_version_ = schema_version; }
  OB_INLINE void set_properties(int64_t properties) { properties_ = properties; }
  OB_INLINE void set_charset_id(int64_t charset_id) { charset_id_ = charset_id; }
  OB_INLINE void set_charset_form(int64_t charset_form) { charset_form_ = charset_form; }
  OB_INLINE void set_length(int64_t length) {length_ = length; }
  OB_INLINE void set_number_precision(int64_t precision) { precision_ = precision; }
  OB_INLINE void set_precision(int64_t precision) { precision_ = precision; }
  OB_INLINE void set_scale(int64_t scale) { scale_ = scale; }
  OB_INLINE void set_zero_fill(int64_t zero_fill) { zero_fill_ = zero_fill; }
  OB_INLINE void set_coll_type(int64_t coll_type) { coll_type_ = coll_type; }

  OB_INLINE void set_not_null() { properties_ = properties_ | COLL_NOT_NULL_FLAG; }
  OB_INLINE bool is_not_null() const { return (properties_ & COLL_NOT_NULL_FLAG) != 0; }

  OB_INLINE void set_typecode(ObUDTTypeCode typecode) { properties_ = ((properties_ & COLL_NOT_NULL_FLAG) | typecode); }
  OB_INLINE int64_t get_typecode() const
  {
    return properties_ & ~COLL_NOT_NULL_FLAG;
  }
  OB_INLINE bool is_base_type() const { return UDT_TYPE_BASE == get_typecode(); }
  OB_INLINE bool is_coll_type() const { return UDT_TYPE_COLLECTION == get_typecode(); }
  OB_INLINE bool is_obj_type() const
  {
    return UDT_TYPE_OBJECT == get_typecode()
        || UDT_TYPE_OBJECT_BODY == get_typecode();
  }
  OB_INLINE bool is_opaque_type() const { return UDT_TYPE_OPAQUE == get_typecode(); }

  TO_STRING_KV(K_(tenant_id),
               K_(schema_version),
               K_(properties),
               K_(charset_id),
               K_(charset_form),
               K_(length),
               K_(precision),
               K_(scale),
               K_(zero_fill),
               K_(coll_type));

private:
  uint64_t tenant_id_;      // 租户ID
  int64_t schema_version_;  // schema_version
  int64_t properties_;      // 该type的一些属性, 暂时用来记录该Type是否是复杂类型
  int64_t charset_id_;
  int64_t charset_form_;
  int64_t length_;
  int64_t precision_;
  int64_t scale_;
  int64_t zero_fill_;
  int64_t coll_type_; // object 类型，这个被重用为object id
};

enum ObUDTObjType
{
  INVALID_UDT_OBJECT_TYPE = 0,
  UDT_OBJECT_SPEC_TYPE = 1,
  UDT_OBJECT_BODY_TYPE = 2,
};
class ObUDTObjectType : public ObUDTBase
{
OB_UNIS_VERSION(1);

public:
  ObUDTObjectType() { reset(); }
  explicit ObUDTObjectType(common::ObIAllocator *allocator) : ObUDTBase(allocator) { reset(); }
  ObUDTObjectType(const ObUDTObjectType &src_schema) : ObUDTBase() { reset(); *this = src_schema; }
  virtual ~ObUDTObjectType() {}

  ObUDTObjectType& operator= (const ObUDTObjectType &src_schema);
  void reset();
  int64_t get_convert_size() const;
  int to_package_info(share::schema::ObPackageInfo &pkg_info) const;

  #define ATTR_GET(ret_type, attr_name) \
  OB_INLINE ret_type get_##attr_name() const { return attr_name##_; }
  #define ATTR_SET(attr_type, attr_name) \
  OB_INLINE void set_##attr_name(attr_type val) { attr_name##_ = val; }
  #define STR_ATTR_GET(attr_name) \
  OB_INLINE const common::ObString &get_##attr_name() const { return attr_name##_; }
  #define STR_ATTR_SET(attr_name) \
  OB_INLINE int set_##attr_name(const common::ObString &val) \
  { return deep_copy_str(val, attr_name##_); }

  ATTR_GET(uint64_t, database_id);
  ATTR_SET(uint64_t, database_id);
  ATTR_GET(uint64_t, owner_id);
  ATTR_SET(uint64_t, owner_id);
  ATTR_GET(uint64_t, object_type_id);
  ATTR_SET(uint64_t, object_type_id);
  ATTR_GET(int64_t, type);
  ATTR_SET(int64_t, type);
  ATTR_GET(int64_t, flag);
  ATTR_SET(int64_t, flag);
  ATTR_GET(int64_t, comp_flag);
  ATTR_SET(int64_t, comp_flag);

  STR_ATTR_GET(object_name);
  STR_ATTR_SET(object_name);
  STR_ATTR_GET(exec_env);
  STR_ATTR_SET(exec_env);
  STR_ATTR_GET(source);
  STR_ATTR_SET(source);
  STR_ATTR_GET(comment);
  STR_ATTR_SET(comment);
  STR_ATTR_GET(route_sql);
  STR_ATTR_SET(route_sql);

#undef ATTR_GET
#undef ATTR_SET
#undef STR_ATTR_GET
#undef STR_ATTR_SET

 inline bool is_object_equal(const ObUDTObjectType &other) const
 {
   return owner_id_ == other.owner_id_
          && object_type_id_ == other.object_type_id_
          && type_ == other.type_;
 }
 inline bool is_valid() const
 {
   return OB_INVALID_ID != database_id_
          && OB_INVALID_ID != owner_id_
          && OB_INVALID_ID != object_type_id_
          && INVALID_UDT_OBJECT_TYPE != type_;
 }
 inline bool is_object_spec() const {
   return UDT_OBJECT_SPEC_TYPE == type_;
 }
 inline bool is_object_body() const {
   return UDT_OBJECT_BODY_TYPE == type_;
 }

  inline static bool is_object_id(uint64_t package_id)
  {
    return package_id != OB_INVALID_ID
        && 0 != (package_id & common::OB_MOCK_OBJECT_PACAKGE_ID_MASK);
  }
  inline static uint64_t mask_object_id(uint64_t udt_id)
  {
    return udt_id | common::OB_MOCK_OBJECT_PACAKGE_ID_MASK;;
  }
  inline static uint64_t clear_object_id_mask(uint64_t udt_id)
  {
    return udt_id & ~common::OB_MOCK_OBJECT_PACAKGE_ID_MASK;;
  }
  inline static bool is_object_id_masked(uint64_t udt_id)
  {
    return is_object_id(udt_id);
  }

uint64_t get_object_type_id(uint64_t tenant_id) const;

TO_STRING_KV(K_(database_id),
               K_(owner_id),
               K_(object_type_id),
               K(get_coll_type()),
               K_(object_name),
               K_(type),
               K_(comp_flag),
               K_(exec_env),
               K_(source),
               K_(comment),
               K_(route_sql));

private:
  uint64_t database_id_;
  uint64_t owner_id_;
  uint64_t object_type_id_;
  common::ObString object_name_;
  int64_t type_; //ObUDTObjType
  int64_t flag_;
  int64_t comp_flag_;
  common::ObString exec_env_;
  common::ObString source_;
  common::ObString comment_;
  common::ObString route_sql_;
};
class ObUDTTypeAttr : public ObUDTBase
{
  OB_UNIS_VERSION(1);
public:
  ObUDTTypeAttr() { reset(); }
  explicit ObUDTTypeAttr(common::ObIAllocator *allocator) : ObUDTBase(allocator) { reset(); }
  ObUDTTypeAttr(const ObUDTTypeAttr &src_schema) : ObUDTBase() { reset(); *this = src_schema; }
  virtual ~ObUDTTypeAttr() {}

  ObUDTTypeAttr& operator= (const ObUDTTypeAttr &src_schema);
  void reset();
  int64_t get_convert_size() const;

  // getter
  OB_INLINE int64_t get_type_attr_id() const { return type_attr_id_; }
  OB_INLINE int64_t get_type_id() const { return type_id_; }
  OB_INLINE const common::ObString& get_name() const { return name_; }
  OB_INLINE int64_t get_attribute() const { return attribute_; }
  OB_INLINE const common::ObString& get_externname() const { return externname_; }
  OB_INLINE int64_t get_xflags() const { return xflags_; }
  OB_INLINE int64_t get_setter() const { return setter_; }
  OB_INLINE int64_t get_getter() const { return getter_; }

  // setter
  OB_INLINE void set_type_attr_id(int64_t type_attr_id) { type_attr_id_ = type_attr_id; }
  OB_INLINE void set_type_id(int64_t type_id) { type_id_ = type_id; }
  OB_INLINE int set_name(const common::ObString &name) { return deep_copy_str(name, name_); }
  OB_INLINE void set_attribute(int64_t attribute) { attribute_ = attribute; }
  OB_INLINE int set_externname(const common::ObString &externname) { return deep_copy_str(externname, externname_); }
  OB_INLINE void set_xflags(int64_t xflags) { xflags_ = xflags; }
  OB_INLINE void set_setter(int64_t setter) { setter_ = setter; }
  OB_INLINE void set_getter(int64_t getter) { getter_ = getter; }

  TO_STRING_KV(K_(type_attr_id),
               K_(type_id),
               K_(name),
               K_(attribute),
               K_(externname),
               K_(xflags),
               K_(setter),
               K_(getter));

private:
  int64_t type_attr_id_;  // 属性ID
  int64_t type_id_;       // 所属Type的ID
  common::ObString name_; // 属性名
  int64_t attribute_;     // 该属性在Type中的位置
  common::ObString externname_;// 扩展(UNUSED)
  int64_t xflags_;        // 
  int64_t setter_;        // 扩展(UNUSED)
  int64_t getter_;        // 扩展(UNUSED)
};
class ObUDTCollectionType : public ObUDTBase
{
  OB_UNIS_VERSION(1);

public:
  ObUDTCollectionType() { reset(); }
  explicit ObUDTCollectionType(common::ObIAllocator *allocator) : ObUDTBase(allocator) { reset(); }
  ObUDTCollectionType(const ObUDTCollectionType &src_schema) : ObUDTBase() { reset(); *this = src_schema; }
  virtual ~ObUDTCollectionType() {}

  ObUDTCollectionType& operator= (const ObUDTCollectionType &src_schema);
  void reset();
  int64_t get_convert_size() const;

  // getter
  OB_INLINE int64_t get_coll_type_id() const { return coll_type_id_; }
  OB_INLINE int64_t get_elem_type_id() const { return elem_type_id_; }
  OB_INLINE int64_t get_elem_schema_version() const { return elem_schema_version_; }
  OB_INLINE int64_t get_upper_bound() const { return upper_bound_; }
  OB_INLINE int64_t get_package_id() const { return package_id_; }
  OB_INLINE const common::ObString& get_coll_name() const { return coll_name_; }
  // setter
  OB_INLINE void set_coll_type_id(int64_t coll_type_id) { coll_type_id_ = coll_type_id; }
  OB_INLINE void set_elem_type_id(int64_t elem_type_id) { elem_type_id_ = elem_type_id; }
  OB_INLINE void set_elem_schema_version(int64_t elem_schema_version) { elem_schema_version_ = elem_schema_version; }
  OB_INLINE void set_upper_bound(int64_t upper_bound) { upper_bound_ = upper_bound; }
  OB_INLINE void set_package_id(int64_t package_id) { package_id_ = package_id; }
  OB_INLINE int set_coll_name(const common::ObString &coll_name) { return deep_copy_str(coll_name, coll_name_); }

  OB_INLINE bool is_varray() const { return upper_bound_ != OB_INVALID_ID; }

  TO_STRING_KV(K_(coll_type_id),
               K_(elem_type_id),
               K_(elem_schema_version),
               K_(upper_bound),
               K_(package_id),
               K_(coll_name));
private:
  int64_t coll_type_id_;          // 集合 ID
  int64_t elem_type_id_;          // 集合元素类型ID
  int64_t elem_schema_version_;   // 集合元素类型的schema_version
  int64_t upper_bound_;           // 对于Varray代表最大大小
  int64_t package_id_;            // 如果该集合类型来自Package, 这里记录PackageID 
  common::ObString coll_name_;    // 集合名称
};


class ObUDTTypeInfo : public ObSchema, public IObErrorInfo
{
  OB_UNIS_VERSION(1);
public:
  ObUDTTypeInfo() { reset(); }
  explicit ObUDTTypeInfo(common::ObIAllocator *allocator);
  ObUDTTypeInfo(const ObUDTTypeInfo &src_schema) : ObSchema() { reset(); *this = src_schema; }
  virtual ~ObUDTTypeInfo() {}

  ObUDTTypeInfo& operator= (const ObUDTTypeInfo &src_schema);
  int assign(const ObUDTTypeInfo &other);
  void reset();
  int64_t get_convert_size() const;
  int add_type_attr(const ObUDTTypeAttr& attr);
  int64_t get_attrs_count() const { return type_attrs_.count(); }
  bool is_collection() const { return UDT_TYPE_COLLECTION == typecode_; }
  bool is_varray() const {
      return UDT_TYPE_COLLECTION == typecode_ && coll_info_->is_varray(); }
  int set_coll_info(const ObUDTCollectionType& coll_info);
  const ObUDTCollectionType* get_coll_info() const { return coll_info_; }
  ObUDTCollectionType* get_coll_info() { return coll_info_; }

  OB_INLINE void set_is_collection() { typecode_ = UDT_TYPE_COLLECTION; }
  OB_INLINE void set_is_object() { typecode_ = UDT_TYPE_OBJECT; }
  OB_INLINE void set_is_object_body() { typecode_ = UDT_TYPE_OBJECT_BODY; }
  OB_INLINE bool is_object_type() const
  {
    return UDT_TYPE_OBJECT == typecode_
          || UDT_TYPE_OBJECT_BODY == typecode_
          || UDT_TYPE_OPAQUE == typecode_;
  }
  OB_INLINE void set_is_opaque() { typecode_ = UDT_TYPE_OPAQUE; }
  OB_INLINE bool is_opaque() const { return UDT_TYPE_OPAQUE == typecode_; }

  int deep_copy_name(common::ObIAllocator &allocator, const common::ObString &src, common::ObString &dst) const;
  int transform_to_pl_type(common::ObIAllocator &allocator, const pl::ObUserDefinedType *&pl_type) const;
  int transform_to_pl_type(const ObUDTTypeAttr* attr_info, pl::ObPLDataType &pl_type) const;
  int transform_to_pl_type(const ObUDTCollectionType *coll_info, pl::ObPLDataType &pl_type) const;

  //getter
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE uint64_t get_database_id() const { return database_id_; }
  OB_INLINE int64_t get_schema_version() const { return schema_version_; }
  OB_INLINE int64_t get_type_id() const { return type_id_; }
  OB_INLINE int64_t get_masked_type_id() const { return ObUDTObjectType::mask_object_id(type_id_); }
  OB_INLINE int64_t get_typecode() const { return typecode_; }
  OB_INLINE int64_t get_properties() const { return properties_; }
  OB_INLINE int64_t get_attributes() const { return type_attrs_.count(); }
  OB_INLINE int64_t get_methods() const { return methods_; }
  OB_INLINE int64_t get_hiddenmethods() const { return hiddenmethods_; }
  OB_INLINE int64_t get_supertypes() const { return supertypes_; }
  OB_INLINE int64_t get_subtypes() const { return subtypes_; }
  OB_INLINE int64_t get_externtype() const { return externtype_; }
  OB_INLINE const common::ObString& get_externname() const { return externname_; }
  OB_INLINE const common::ObString& get_helperclassname() const { return helperclassname_; }
  OB_INLINE int64_t get_local_attrs() const { return local_attrs_; }
  OB_INLINE int64_t get_local_methods() const { return local_methods_; }
  OB_INLINE int64_t get_supertypeid() const { return supertypeid_; }
  OB_INLINE const common::ObString& get_type_name() const { return type_name_; }
  OB_INLINE int64_t get_package_id() const { return package_id_; }
  OB_INLINE const common::ObIArray<ObUDTTypeAttr*>& get_attrs() const { return type_attrs_; }
  OB_INLINE uint64_t get_object_id() const { return get_type_id(); }
  OB_INLINE ObObjectType get_object_type() const {
    ObObjectType obj_type = is_object_body_ddl() ? ObObjectType::TYPE_BODY : ObObjectType::TYPE;
    return obj_type;
  }
  //setter
  OB_INLINE void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  OB_INLINE void set_schema_version(int64_t schema_version) { schema_version_ = schema_version; }
  OB_INLINE void set_type_id(int64_t type_id) { type_id_ = type_id; }
  OB_INLINE void set_typecode(int64_t typecode) { typecode_ = typecode; }
  OB_INLINE void set_properties(int64_t properties) { properties_ = properties; }
  OB_INLINE void set_attributes(int64_t attributes) { UNUSED(attributes);/*attributes_ = attributes;*/ }
  OB_INLINE void set_methods(int64_t methods) { methods_ = methods; }
  OB_INLINE void set_hiddenmethods(int64_t hiddenmethods) { hiddenmethods_ = hiddenmethods; }
  OB_INLINE void set_supertypes(int64_t supertypes) { supertypes_ = supertypes; }
  OB_INLINE void set_subtypes(int64_t subtypes) { subtypes_ = subtypes; }
  OB_INLINE void set_externtype(int64_t externtype) { externtype_ = externtype; }
  OB_INLINE int set_externname(const common::ObString& externname) { return deep_copy_str(externname, externname_); }
  OB_INLINE int set_helperclassname(const common::ObString& helperclassname) { return deep_copy_str(helperclassname, helperclassname_); }
  OB_INLINE void set_local_attrs(int64_t local_attrs) { local_attrs_ = local_attrs; }
  OB_INLINE void set_local_methods(int64_t local_methods) { local_methods_ = local_methods; }
  OB_INLINE void set_supertypeid(int64_t supertypeid) { supertypeid_ = supertypeid; }
  OB_INLINE int set_type_name(const common::ObString& type_name) { return deep_copy_str(type_name, type_name_); }
  OB_INLINE void set_package_id(int64_t package_id) { package_id_ = package_id; }

  OB_INLINE void set_udt_invalid() { properties_ |= UDT_FLAG_INVALID; }
  OB_INLINE void set_udt_valid() { properties_ = properties_ & ~UDT_FLAG_INVALID; }
  OB_INLINE void set_noneditionable() { properties_ |= UDT_FLAG_NONEDITIONABLE; }

  OB_INLINE bool is_udt_invalid() const
  {
    return UDT_FLAG_INVALID == (properties_ & UDT_FLAG_INVALID);
  }
  OB_INLINE bool is_noneditionable() const
  {
    return UDT_FLAG_NONEDITIONABLE == (properties_ & UDT_FLAG_NONEDITIONABLE);
  }

  int add_object_type_info(const ObUDTObjectType &udt_object);
  OB_INLINE const common::ObIArray<ObUDTObjectType*>& get_object_type_infos() const 
  {
    return object_type_infos_;
  }

  OB_INLINE bool is_object_body_ddl() const 
  {
    return is_object_type()
     && UDT_FLAG_OBJECT_TYPE_BODY == (properties_ & UDT_FLAG_OBJECT_TYPE_BODY);
  }
  OB_INLINE bool is_object_spec_ddl() const
  {
    return is_object_type()
     && UDT_FLAG_OBJECT_TYPE_SPEC == (properties_ & UDT_FLAG_OBJECT_TYPE_SPEC);
  }
  int set_object_ddl_type(ObUDTTypeFlag type_flag);
  OB_INLINE void clear_property_flag(ObUDTTypeFlag flag)
  {
    properties_ = properties_ & !flag;
  }

  const ObUDTObjectType *get_object_info() const;

  int copy_udt_info_in_require(const ObUDTTypeInfo &old_info);
  bool is_object_type_info_exist(const ObUDTObjectType &udt_object) const;
  inline uint64_t get_object_spec_id (uint64_t tenant_id) const
  {
    uint64_t obj_id = OB_INVALID_ID;
    if (0 < object_type_infos_.count()) {
      obj_id = object_type_infos_.at(0)->get_object_type_id(tenant_id);
    }
    return obj_id;
  }
  inline uint64_t get_object_body_id(uint64_t tenant_id) const
  {
    uint64_t obj_id = OB_INVALID_ID;
    if (has_type_body()) {
      obj_id = object_type_infos_.at(1)->get_object_type_id(tenant_id);
    }
    return obj_id;
  }

  inline bool has_type_body() const
  {
    return is_object_type_legal() && 2 == object_type_infos_.count();
  }
  inline bool is_object_type_legal() const
  {
    bool bret = true;
    if (is_object_type()) {
      if (0 == object_type_infos_.count()) {
        // do nothing
      } else if (1 == object_type_infos_.count()) {
        ObUDTObjectType *spec = object_type_infos_.at(0);
        if (OB_ISNULL(spec)) {
          bret = false;
        } else {
          bret = spec->is_object_spec();
        }
      } else if (2 == object_type_infos_.count()) {
        ObUDTObjectType *spec = object_type_infos_.at(0);
        ObUDTObjectType *body = object_type_infos_.at(1);
        if (OB_ISNULL(spec) || OB_ISNULL(body)) {
          bret = false;
        } else {
          bret = (spec->is_object_spec() && body->is_object_body())
              && (spec->get_object_type_id() == body->get_object_type_id());
        }
      } else {
        bret = false;
      }
    }
    return bret;
  }

  
  TO_STRING_KV(K_(tenant_id),
               K_(database_id),
               K_(schema_version),
               K_(type_id),
               K_(typecode),
               K_(properties),
               K_(attributes),
               K_(methods),
               K_(hiddenmethods),
               K_(supertypes),
               K_(subtypes),
               K_(externtype),
               K_(externname),
               K_(helperclassname),
               K_(local_attrs),
               K_(local_methods),
               K_(supertypeid),
               K_(type_name),
               K_(package_id),
               K_(coll_info),
               K_(type_attrs),
               K_(object_type_infos));

private:
  uint64_t tenant_id_;         // 租户ID
  uint64_t database_id_;       // DatabaseID
  int64_t schema_version_;    //
  int64_t type_id_;           // TypeID, TypeID由两部分组成tenant_id和typeId
  int64_t typecode_;          // 记录Type的类型如Object or collection等
  int64_t properties_;        //
  int64_t attributes_;        // 该Type属性数
  int64_t methods_;           // 该Type方法数(EXTEND UNUSED)
  int64_t hiddenmethods_;     // 隐藏方法数(EXTEND UNUSED)
  int64_t supertypes_;        // (EXTEND UNUSED)
  int64_t subtypes_;          // (EXTEND UNUSED)
  int64_t externtype_;        // (EXTEND UNUSED)
  common::ObString externname_;       // (EXTEND UNUSED)
  common::ObString helperclassname_;  // (EXTEND UNUSED)
  int64_t local_attrs_;       // (EXTEND UNUSED)
  int64_t local_methods_;     // (EXTEND UNUSED)
  int64_t supertypeid_;       // (EXTEND UNUSED)
  common::ObString type_name_;        // 该Type名称
  // 如果该Type来自Package, 记录PackageID, 如果是一个type object，这个值不可能有效，
  // 因为不能在package里面创建object，所以会被复用，当做一个owner id，它被关联到object的routine的owner id
  int64_t package_id_;
  ObUDTCollectionType *coll_info_; // 如果该Type是Coll类型, 该字段有效
  common::ObSEArray<ObUDTTypeAttr *, 64> type_attrs_; // 记录该Type对象的属性相关信息
  // object type spec and body
  common::ObSEArray<ObUDTObjectType*, 2> object_type_infos_;
};

}  // namespace schema
}  // namespace share
}  // namespace oceanbase

#endif /* OCEANBASE_SRC_SHARE_SCHEMA_OB_UDT_INFO_H_ */
