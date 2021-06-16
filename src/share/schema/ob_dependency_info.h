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

#ifndef OCEANBASE_SRC_SHARE_SCHEMA_OB_DEPENDENCY_INFO_H_
#define OCEANBASE_SRC_SHARE_SCHEMA_OB_DEPENDENCY_INFO_H_
#include "share/schema/ob_schema_struct.h"
#include "common/object/ob_object.h"
#include "lib/container/ob_fixed_array.h"

namespace oceanbase {

namespace common {
class ObIAllocator;
class ObISQLClient;
class ObMySQLTransaction;
class ObWarningBuffer;
}  // namespace common

namespace share {
class ObDMLSqlSplicer;
}

namespace share {
namespace schema {

class ObSchemaService;
extern const char* ob_object_type_str(const ObObjectType object_type);
class ObDependencyInfo : public ObSchema {
  OB_UNIS_VERSION(1);

public:
  ObDependencyInfo();
  ObDependencyInfo(const ObDependencyInfo& src_schema);
  explicit ObDependencyInfo(common::ObIAllocator* allocator);
  virtual ~ObDependencyInfo();

  ObDependencyInfo& operator=(const ObDependencyInfo& src_schema);
  int assign(const ObDependencyInfo& other);

#define DEFINE_GETTER(ret_type, name)   \
  OB_INLINE ret_type get_##name() const \
  {                                     \
    return name##_;                     \
  }

  DEFINE_GETTER(uint64_t, tenant_id)
  DEFINE_GETTER(uint64_t, dep_obj_id)
  DEFINE_GETTER(ObObjectType, dep_obj_type)
  DEFINE_GETTER(uint64_t, order)
  DEFINE_GETTER(int64_t, dep_timestamp)
  DEFINE_GETTER(uint64_t, ref_obj_id)
  DEFINE_GETTER(ObObjectType, ref_obj_type)
  DEFINE_GETTER(int64_t, ref_timestamp)
  DEFINE_GETTER(uint64_t, dep_obj_owner_id)
  DEFINE_GETTER(uint64_t, property)
  DEFINE_GETTER(int64_t, schema_version)
  DEFINE_GETTER(const common::ObString&, dep_attrs)
  DEFINE_GETTER(const common::ObString&, dep_reason)
  DEFINE_GETTER(const common::ObString&, ref_obj_name)

#undef DEFINE_GETTER

#define DEFINE_SETTER(name, type)      \
  OB_INLINE void set_##name(type name) \
  {                                    \
    name##_ = name;                    \
  }

  DEFINE_SETTER(tenant_id, uint64_t)
  DEFINE_SETTER(dep_obj_id, uint64_t)
  DEFINE_SETTER(dep_obj_type, ObObjectType)
  DEFINE_SETTER(order, uint64_t)
  DEFINE_SETTER(dep_timestamp, int64_t)
  DEFINE_SETTER(ref_obj_id, uint64_t)
  DEFINE_SETTER(ref_obj_type, ObObjectType)
  DEFINE_SETTER(ref_timestamp, int64_t)
  DEFINE_SETTER(dep_obj_owner_id, uint64_t)
  DEFINE_SETTER(property, uint64_t)
  DEFINE_SETTER(schema_version, int64_t)
  OB_INLINE int set_dep_attrs(common::ObString& dep_attrs)
  {
    return deep_copy_str(dep_attrs_, dep_attrs);
  }
  OB_INLINE int set_dep_reason(common::ObString& dep_reason)
  {
    return deep_copy_str(dep_reason, dep_reason_);
  }
  OB_INLINE int set_ref_obj_name(common::ObString& ref_obj_name)
  {
    return deep_copy_str(ref_obj_name, ref_obj_name_);
  }

#undef DEFINE_SETTER

  bool is_valid() const;
  bool is_user_field_valid() const;
  void reset();
  int64_t get_convert_size() const;
  int insert_schema_object_dependency(common::ObISQLClient& trans, bool is_replace = false, bool only_history = false);

  static int delete_schema_object_dependency(common::ObISQLClient& trans, uint64_t tenant_id, uint64_t dep_obj_id,
      int64_t schema_version, share::schema::ObObjectType dep_obj_type);

  static int collect_dep_infos(common::ObIArray<share::schema::ObSchemaObjVersion>& schema_objs,
      common::ObIArray<share::schema::ObDependencyInfo>& deps, ObObjectType dep_obj_type, uint64_t property,
      common::ObString& dep_attrs, common::ObString& dep_reason);

  int get_object_create_time(common::ObISQLClient& sql_client, share::schema::ObObjectType obj_type,
      int64_t& create_time, common::ObString& ref_obj_name);

  TO_STRING_KV(K_(tenant_id), K_(dep_obj_id), K_(dep_obj_type), K_(order), K_(dep_timestamp), K_(ref_obj_id),
      K_(ref_obj_type), K_(ref_timestamp), K_(dep_obj_owner_id), K_(property), K_(dep_attrs), K_(dep_reason),
      K_(ref_obj_name), K_(schema_version))
private:
  int gen_dependency_dml(const uint64_t exec_tenant_id, oceanbase::share::ObDMLSqlSplicer& dml);
  // uint64_t extract_tenant_id() const;
  uint64_t extract_ref_obj_id() const;
  // static uint64_t extract_tenant_id(uint64_t tenant_id);
  static uint64_t extract_obj_id(uint64_t exec_tenant_id, uint64_t id);

private:
  uint64_t tenant_id_;
  uint64_t dep_obj_id_;
  ObObjectType dep_obj_type_;
  uint64_t order_;
  int64_t dep_timestamp_;
  uint64_t ref_obj_id_;
  ObObjectType ref_obj_type_;
  int64_t ref_timestamp_;
  uint64_t dep_obj_owner_id_;
  uint64_t property_;
  common::ObString dep_attrs_;
  common::ObString dep_reason_;
  common::ObString ref_obj_name_;
  int64_t schema_version_;
};

}  // namespace schema
}  // namespace share
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SHARE_SCHEMA_OB_ERROR_INFO_H_ */
