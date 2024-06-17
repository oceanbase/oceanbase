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
#include "lib/hash/ob_hashset.h"
#include "share/schema/ob_multi_version_schema_service.h"


namespace oceanbase
{

namespace common
{
class ObIAllocator;
class ObISQLClient;
class ObMySQLTransaction;
class ObWarningBuffer;
}

namespace share
{
class ObDMLSqlSplicer;
}

namespace sql
{
class ObSqlCtx;
class ObMaintainObjDepInfoTask;
class ObMaintainDepInfoTaskQueue;
}

namespace obrpc
{
class ObDependencyObjDDLArg;
class ObCommonRpcProxy;
}

namespace rootserver
{
class ObDDLOperator;
}

namespace share
{
namespace schema
{

class ObSchemaService;
class ObReferenceObjTable;
extern const char *ob_object_type_str(const ObObjectType object_type);
using CriticalDepInfo = common::ObTuple<uint64_t /* dep_obj_id */,
                                        int64_t /* dep_obj_type */,
                                        int64_t /* schema_version */>;
class ObDependencyInfo : public ObSchema 
{
  OB_UNIS_VERSION(1);
public:
  ObDependencyInfo();
  ObDependencyInfo(const ObDependencyInfo &src_schema);
  explicit ObDependencyInfo(common::ObIAllocator *allocator);
  virtual ~ObDependencyInfo();

  ObDependencyInfo &operator=(const ObDependencyInfo &src_schema);
  int assign(const ObDependencyInfo &other);

#define DEFINE_GETTER(ret_type, name) \
  OB_INLINE ret_type get_##name() const { return name##_; }

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

#define DEFINE_SETTER(name, type) \
  OB_INLINE void set_##name(type name) { name##_ = name; }

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
  OB_INLINE int set_dep_attrs(const common::ObString &dep_attrs)
   { return deep_copy_str(dep_attrs, dep_attrs_); }
  OB_INLINE int set_dep_reason(const common::ObString &dep_reason)
   { return deep_copy_str(dep_reason, dep_reason_); }
  OB_INLINE int set_ref_obj_name(const common::ObString &ref_obj_name)
   { return deep_copy_str(ref_obj_name, ref_obj_name_); }

#undef DEFINE_SETTER

  bool is_valid() const;
  bool is_user_field_valid() const;
  void reset();
  int64_t get_convert_size() const;
  int parse_from(common::sqlclient::ObMySQLResult &result);
  int insert_schema_object_dependency(common::ObISQLClient &trans,
                                      bool is_replace = false, bool only_history = false);

  static int delete_schema_object_dependency(common::ObISQLClient &trans,
                                             uint64_t tenant_id,
                                             uint64_t dep_obj_id,
                                             int64_t schema_version,
                                             share::schema::ObObjectType dep_obj_type);

  static int collect_dep_info(ObIArray<ObDependencyInfo> &deps,
                              ObObjectType dep_obj_type,
                              int64_t ref_obj_id,
                              int64_t ref_timestamp,
                              ObDependencyTableType dependent_type);

  static int collect_dep_infos(
             const common::ObIArray<share::schema::ObSchemaObjVersion> &schema_objs,
             common::ObIArray<share::schema::ObDependencyInfo> &deps,
             ObObjectType dep_obj_type,
             uint64_t property,
             common::ObString &dep_attrs,
             common::ObString &dep_reason,
             bool is_pl = true);
  static int collect_dep_infos(ObReferenceObjTable &ref_objs,
                               common::ObIArray<ObDependencyInfo> &deps,
                               ObObjectType dep_obj_type,
                               uint64_t dep_obj_id,
                               int64_t &max_version);
  static int collect_dep_infos(
    const common::ObIArray<ObBasedSchemaObjectInfo> &based_schema_object_infos,
    common::ObIArray<ObDependencyInfo> &deps,
    const uint64_t tenant_id,
    const ObObjectType dep_obj_type,
    const uint64_t dep_obj_id,
    const uint64_t dep_obj_owner_id,
    const uint64_t property,
    const common::ObString &dep_attrs,
    const common::ObString &dep_reason,
    const int64_t schema_version);

  int get_object_create_time(common::ObISQLClient &sql_client,
                             share::schema::ObObjectType obj_type,
                             int64_t &create_time,
                             common::ObString &ref_obj_name);
  int gen_dependency_dml(const uint64_t exec_tenant_id, oceanbase::share::ObDMLSqlSplicer &dml);

  static int collect_ref_infos(uint64_t tenant_id,
                               uint64_t dep_obj_id,
                               common::ObISQLClient &sql_proxy,
                               common::ObIArray<ObDependencyInfo> &deps);
  static int collect_dep_infos(uint64_t tenant_id,
                               uint64_t ref_obj_id,
                               common::ObISQLClient &sql_proxy,
                               common::ObIArray<ObDependencyInfo> &deps);
  static int collect_all_dep_objs(uint64_t tenant_id,
                                  uint64_t ref_obj_id,
                                  common::ObISQLClient &sql_proxy,
                                  common::ObIArray<std::pair<uint64_t, share::schema::ObObjectType>> &objs);
  static int collect_all_dep_objs_inner(uint64_t tenant_id,
                                        uint64_t root_obj_id,
                                        uint64_t ref_obj_id,
                                        common::ObISQLClient &sql_proxy,
                                        common::ObIArray<std::pair<uint64_t, share::schema::ObObjectType>> &objs);
  static int collect_all_dep_objs(uint64_t tenant_id,
                                  uint64_t ref_obj_id,
                                  ObObjectType ref_obj_type,
                                  common::ObISQLClient &sql_proxy,
                                  common::ObIArray<CriticalDepInfo> &objs);
  static int batch_invalidate_dependents(const common::ObIArray<CriticalDepInfo> &objs,
                                         common::ObMySQLTransaction &trans,
                                         uint64_t tenant_id,
                                         uint64_t ref_obj_id);
  static int cascading_modify_obj_status(common::ObMySQLTransaction &trans,
                                         uint64_t tenant_id,
                                         uint64_t obj_id,
                                         rootserver::ObDDLOperator &ddl_operator,
                                         share::schema::ObMultiVersionSchemaService &schema_service);
  static int modify_dep_obj_status(common::ObMySQLTransaction &trans,
                                   uint64_t tenant_id,
                                   uint64_t obj_id,
                                   rootserver::ObDDLOperator &ddl_operator,
                                   share::schema::ObMultiVersionSchemaService &schema_service);
  static int modify_all_obj_status(const ObIArray<std::pair<uint64_t, share::schema::ObObjectType>> &objs,
                                   common::ObMySQLTransaction &trans,
                                   uint64_t tenant_id,
                                   rootserver::ObDDLOperator &ddl_operator,
                                   share::schema::ObMultiVersionSchemaService &schema_service);

  TO_STRING_KV(K_(tenant_id),
               K_(dep_obj_id),
               K_(dep_obj_type),
               K_(order),
               K_(dep_timestamp),
               K_(ref_obj_id),
               K_(ref_obj_type),
               K_(ref_timestamp),
               K_(dep_obj_owner_id),
               K_(property),
               K_(dep_attrs),
               K_(dep_reason),
               K_(ref_obj_name),
               K_(schema_version))
private:
  static uint64_t extract_obj_id(uint64_t exec_tenant_id, uint64_t id);
  static int collect_all_dep_objs(uint64_t tenant_id,
                                  const common::ObIArray<std::pair<uint64_t, int64_t>>& ref_obj_infos,
                                  common::ObISQLClient &sql_proxy,
                                  common::ObIArray<CriticalDepInfo> &objs);

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

class ObReferenceObjTable
{
public:
  enum ObSchemaRefObjOp
  {
    INVALID_OP = 0,
    INSERT_OP = 1,
    UPDATE_OP = 2,
    DELETE_OP = 3
  };

#define DEFINE_SETTER(name, type) \
OB_INLINE void set_##name(type name) { name##_ = name; }
#define DEFINE_GETTER(ret_type, name) \
OB_INLINE ret_type get_##name() const { return name##_; }

  struct ObDependencyObjKey
  {
    OB_UNIS_VERSION(1);
  public:
    ObDependencyObjKey()
      : dep_obj_id_(common::OB_INVALID_ID),
        dep_db_id_(common::OB_INVALID_ID),
        dep_obj_type_(ObObjectType::MAX_TYPE)
    {
    }
    ObDependencyObjKey(int64_t dep_obj_id, int64_t db_id, ObObjectType dep_obj_type)
      : dep_obj_id_(dep_obj_id),
        dep_db_id_(db_id),
        dep_obj_type_(dep_obj_type)
    {
    }
    DEFINE_GETTER(int64_t, dep_obj_id)
    DEFINE_GETTER(int64_t, dep_db_id)
    DEFINE_GETTER(ObObjectType, dep_obj_type)
    DEFINE_SETTER(dep_obj_id, int64_t)
    DEFINE_SETTER(dep_db_id, int64_t)
    DEFINE_SETTER(dep_obj_type, ObObjectType)

    int64_t hash() const;
    int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
    ObDependencyObjKey &operator=(const ObDependencyObjKey &other);
    int assign(const ObDependencyObjKey &other);
    bool operator==(const ObDependencyObjKey &other) const;
    inline void reset()
    {
      dep_obj_id_ = common::OB_INVALID_ID;
      dep_obj_type_ = ObObjectType::MAX_TYPE;
      dep_db_id_ = common::OB_INVALID_ID;
    }
    inline bool is_valid() const
    { 
      return dep_obj_id_ != common::OB_INVALID_ID 
             && dep_obj_type_ != ObObjectType::MAX_TYPE
             && dep_db_id_ != common::OB_INVALID_ID;
    }
    TO_STRING_KV(K_(dep_obj_id),
                 K_(dep_db_id),
                 K_(dep_obj_type));

    int64_t dep_obj_id_;
    int64_t dep_db_id_;
    ObObjectType dep_obj_type_;
  };


  struct ObDependencyObjItem
  {
    OB_UNIS_VERSION(1);
  public:
    typedef common::ObSEArray<share::schema::ObSchemaObjVersion, 16> RefObjVersion;
    ObDependencyObjItem()
      : error_ret_(common::OB_SUCCESS),
        ref_obj_op_(INVALID_OP),
        max_dependency_version_(0),
        max_ref_obj_schema_version_(common::OB_INVALID_VERSION),
        dep_obj_schema_version_(common::OB_INVALID_VERSION),
        ref_obj_versions_()
    {
    }
    ~ObDependencyObjItem() { reset(); }
    DEFINE_GETTER(int, error_ret)
    DEFINE_GETTER(int64_t, max_dependency_version)
    DEFINE_GETTER(int64_t, max_ref_obj_schema_version)
    DEFINE_GETTER(int64_t, dep_obj_schema_version)
    DEFINE_GETTER(ObSchemaRefObjOp, ref_obj_op)
    DEFINE_SETTER(error_ret, int)
    DEFINE_SETTER(max_dependency_version, int64_t)
    DEFINE_SETTER(max_ref_obj_schema_version, int64_t)
    DEFINE_SETTER(dep_obj_schema_version, int64_t)
    DEFINE_SETTER(ref_obj_op, ObSchemaRefObjOp)

    void reset();
    ObDependencyObjItem &operator=(const ObDependencyObjItem &other);
    int assign(const ObDependencyObjItem &other);
    int add_ref_obj_version(const ObSchemaObjVersion &ref_obj_version);
    inline bool is_valid() const { return common::OB_SUCCESS == error_ret_; }
    inline void set_is_del_schema_ref_obj() { ref_obj_op_ = DELETE_OP; }
    inline bool get_is_del_schema_ref_obj() const { return DELETE_OP == ref_obj_op_; }
    inline RefObjVersion &get_ref_obj_versions() { return ref_obj_versions_; }
    inline const RefObjVersion &get_ref_obj_versions() const { return ref_obj_versions_; }
    TO_STRING_KV(K_(error_ret),
                 K_(ref_obj_op),
                 K_(max_dependency_version),
                 K_(max_ref_obj_schema_version),
                 K_(ref_obj_versions),
                 K_(dep_obj_schema_version));

    int error_ret_;
    ObSchemaRefObjOp ref_obj_op_;
    int64_t max_dependency_version_;
    int64_t max_ref_obj_schema_version_;
    int64_t dep_obj_schema_version_;
    RefObjVersion ref_obj_versions_;
  };

#undef DEFINE_GETTER
#undef DEFINE_SETTER

  struct DependencyObjKeyItemPair
  {
    OB_UNIS_VERSION(1);
  public:
    DependencyObjKeyItemPair()
      : dep_obj_key_(),
        dep_obj_item_()
    {
    }
    DependencyObjKeyItemPair(ObDependencyObjKey &key, ObDependencyObjItem &item)
      : dep_obj_key_(key),
        dep_obj_item_(item)
    {
    }
    void reset();
    bool is_valid();
    DependencyObjKeyItemPair &operator=(const DependencyObjKeyItemPair &other);
    int assign(const DependencyObjKeyItemPair &other);
    TO_STRING_KV(K_(dep_obj_key),
                 K_(dep_obj_item));

    ObDependencyObjKey dep_obj_key_;
    ObDependencyObjItem dep_obj_item_;
  };


  typedef common::ObSEArray<DependencyObjKeyItemPair, 8> DependencyObjKeyItemPairs;
  struct ObGetDependencyObjOp
  {
  public:
    ObGetDependencyObjOp(DependencyObjKeyItemPairs *insert_dep_objs,
                         DependencyObjKeyItemPairs *update_dep_objs,
                         DependencyObjKeyItemPairs *delete_dep_objs)
      : insert_dep_objs_(insert_dep_objs),
        update_dep_objs_(update_dep_objs),
        delete_dep_objs_(delete_dep_objs),
        callback_ret_(common::OB_SUCCESS)
    {
    }
    int operator()(common::hash::HashMapPair<ObDependencyObjKey, ObDependencyObjItem *> &entry);
    int get_callback_ret() { return callback_ret_; }

  private:
    DependencyObjKeyItemPairs *insert_dep_objs_;
    DependencyObjKeyItemPairs *update_dep_objs_;
    DependencyObjKeyItemPairs *delete_dep_objs_;
    int callback_ret_;
  };

public:
  typedef common::hash::ObHashMap<ObDependencyObjKey, ObDependencyObjItem *> RefObjVersionMap;
  ObReferenceObjTable()
    : inited_(false),
      ref_obj_version_table_()
  {
  }
  ~ObReferenceObjTable() { reset(); }
  void reset();
  int process_reference_obj_table(
    const uint64_t tenant_id,
    const uint64_t dep_obj_id,
    const ObTableSchema *view_schema,
    sql::ObMaintainDepInfoTaskQueue &task_queue);
  int add_ref_obj_version(
    const uint64_t dep_obj_id,
    const uint64_t dep_db_id,
    const ObObjectType dep_obj_type,
    const ObSchemaObjVersion &ref_obj_version,
    common::ObIAllocator &allocator);
  int get_dep_obj_item(
    const uint64_t dep_obj_id,
    const uint64_t dep_db_id,
    const ObObjectType dep_obj_type,
    ObDependencyObjItem *&dep_obj_item);
  int set_obj_schema_version(
    const uint64_t dep_obj_id,
    const uint64_t dep_db_id,
    const ObObjectType dep_obj_type,
    const int64_t max_dependency_version,
    const int64_t dep_obj_schema_version,
    common::ObIAllocator &allocator);
  int set_ref_obj_op(
    const uint64_t dep_obj_id,
    const uint64_t dep_db_id,
    const ObObjectType dep_obj_type,
    const ObSchemaRefObjOp ref_obj_op,
    common::ObIAllocator &allocator);
  inline bool is_inited() const { return inited_; }
  inline int set_need_del_schema_dep_obj(
    const uint64_t dep_obj_id,
    const uint64_t dep_db_id,
    const ObObjectType dep_obj_type,
    common::ObIAllocator &allocator)
  { return set_ref_obj_op(dep_obj_id, dep_db_id, dep_obj_type, DELETE_OP, allocator); }
  inline RefObjVersionMap &get_ref_obj_table() { return ref_obj_version_table_; }
  inline const RefObjVersionMap &get_ref_obj_table() const { return ref_obj_version_table_; }
  static int batch_execute_insert_or_update_obj_dependency(
    const uint64_t tenant_id,
    const bool is_standby,
    const int64_t new_schema_version,
    const ObReferenceObjTable::DependencyObjKeyItemPairs &dep_objs,
    ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    rootserver::ObDDLOperator &ddl_operator);
  static int batch_execute_delete_obj_dependency(
    const uint64_t tenant_id,
    const bool is_standby,
    const ObReferenceObjTable::DependencyObjKeyItemPairs &dep_objs,
    ObMySQLTransaction &trans);
  static int update_max_dependency_version(
    const uint64_t tenant_id,
    const int64_t dep_obj_id,
    const int64_t max_dependency_version,
    ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    rootserver::ObDDLOperator &ddl_operator);

private:
  static int fill_rowkey_pairs(const uint64_t tenant_id,
                               const ObDependencyObjKey &dep_obj_key,
                               share::ObDMLSqlSplicer &dml);
  static int batch_fill_kv_pairs(const uint64_t tenant_id,
                                 const ObDependencyObjKey &dep_obj_key,
                                 const int64_t new_schema_version,
                                 common::ObIArray<ObDependencyInfo> &dep_infos,
                                 share::ObDMLSqlSplicer &splicer);
  int get_or_add_def_obj_item(const uint64_t dep_obj_id,
                              const uint64_t dep_db_id,
                              const ObObjectType dep_obj_type,
                              ObDependencyObjItem *&dep_obj_item,
                              common::ObIAllocator &allocator);

private:
  bool inited_;
  RefObjVersionMap ref_obj_version_table_;
  DISALLOW_COPY_AND_ASSIGN(ObReferenceObjTable);
};


}  // namespace schema
}  // namespace share
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SHARE_SCHEMA_OB_ERROR_INFO_H_ */
