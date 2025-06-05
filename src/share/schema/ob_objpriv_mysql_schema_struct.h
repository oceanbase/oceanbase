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

#ifndef _OB_OCEANBASE_SCHEMA_OBJPRIV_MYSQL_SCHEMA_STRUCT_H
#define _OB_OCEANBASE_SCHEMA_OBJPRIV_MYSQL_SCHEMA_STRUCT_H

#include "share/schema/ob_schema_struct.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
struct ObObjMysqlPrivSortKey
{
  ObObjMysqlPrivSortKey() : tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID), object_name_(), object_type_(common::OB_INVALID_ID)
  {}
  ObObjMysqlPrivSortKey(const uint64_t tenant_id, const uint64_t user_id,
                     const common::ObString &object_name, const uint64_t &object_type)
      : tenant_id_(tenant_id), user_id_(user_id), object_name_(object_name), object_type_(object_type)
  {}
  bool operator==(const ObObjMysqlPrivSortKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (user_id_ == rhs.user_id_)
           && (object_name_ == rhs.object_name_) && (object_type_ == rhs.object_type_);
  }
  bool operator!=(const ObObjMysqlPrivSortKey &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator<(const ObObjMysqlPrivSortKey &rhs) const
  {
    ObCompareNameWithTenantID name_cmp(tenant_id_);
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (false == bret && tenant_id_ == rhs.tenant_id_) {
      bret = user_id_ < rhs.user_id_;
      if (false == bret && user_id_ == rhs.user_id_) {
        bret = object_type_ < rhs.object_type_;
        if (false == bret && object_type_ == rhs.object_type_) {
          int name_cmp_ret = name_cmp.compare(object_name_, rhs.object_name_);
          if (name_cmp_ret < 0) {
            bret = true;
          } else {
            bret = false;
          }
        }
      }
    }
    return bret;
  }
  //Not used yet.
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&user_id_, sizeof(user_id_), hash_ret);
    hash_ret = common::murmurhash(object_name_.ptr(), object_name_.length(), hash_ret);
    hash_ret = common::murmurhash(&object_type_, sizeof(object_type_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID) && (user_id_ != common::OB_INVALID_ID);
  }

  int deep_copy(const ObObjMysqlPrivSortKey &src, common::ObIAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    tenant_id_ = src.tenant_id_;
    user_id_ = src.user_id_;
    object_type_ = src.object_type_;
    if (OB_FAIL(common::ob_write_string(allocator, src.object_name_, object_name_))) {
      SHARE_SCHEMA_LOG(WARN, "failed to deep copy object_name_", KR(ret), K(src.object_name_));
    }
    return ret;
  }

  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(object_name), K_(object_type));
  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString object_name_;
  uint64_t object_type_;
};

class ObObjMysqlPriv : public ObSchema, public ObPriv
{
  OB_UNIS_VERSION(1);

public:
  //constructor and destructor
  ObObjMysqlPriv()
      : ObSchema(), ObPriv(),
        obj_type_(common::OB_INVALID_ID)
  { }
  explicit ObObjMysqlPriv(common::ObIAllocator *allocator)
      : ObSchema(allocator), ObPriv(),
        obj_type_(common::OB_INVALID_ID)
  { }
  ObObjMysqlPriv(const ObObjMysqlPriv &other)
      : ObSchema(), ObPriv(),
        obj_type_(common::OB_INVALID_ID)
  { *this = other; }
  virtual ~ObObjMysqlPriv() { }

  //operator=
  ObObjMysqlPriv& operator=(const ObObjMysqlPriv &other);

  int assign(const ObObjMysqlPriv &other);

  //for sort
  ObObjMysqlPrivSortKey get_sort_key() const
  { return ObObjMysqlPrivSortKey(tenant_id_, user_id_, obj_name_, obj_type_); }
  static bool cmp(const ObObjMysqlPriv *lhs, const ObObjMysqlPriv *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_sort_key() < rhs->get_sort_key() : false; }
  static bool cmp_sort_key(const ObObjMysqlPriv *lhs, const ObObjMysqlPrivSortKey &sort_key)
  { return NULL != lhs ? lhs->get_sort_key() < sort_key : false; }
  static bool equal(const ObObjMysqlPriv *lhs, const ObObjMysqlPriv *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_sort_key() == rhs->get_sort_key() : false; }
  static bool equal_sort_key(const ObObjMysqlPriv *lhs, const ObObjMysqlPrivSortKey &sort_key)
  { return NULL != lhs ? lhs->get_sort_key() == sort_key : false; }

  //set methods
  inline int set_obj_name(const ObString& objname) { return deep_copy_str(objname, obj_name_); }
  inline void set_obj_type(const uint64_t objtype) { obj_type_ = objtype; }
  //get methods
  inline const ObString& get_obj_name_str() const { return obj_name_; }
  inline const char* get_obj_name() const { return extract_str(obj_name_); }
  inline uint64_t get_obj_type() const { return obj_type_; }

  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(obj_name), K_(obj_type),
               "privileges", ObPrintPrivSet(priv_set_));
  //other methods
  virtual bool is_valid() const;
  virtual void reset();
  int64_t get_convert_size() const;

private:
  common::ObString obj_name_;
  uint64_t obj_type_;
};

}
}
}
#endif