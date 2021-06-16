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

#ifndef OB_OCEANBASE_SCHEMA_OB_SCHEMA_MGR_H_
#define OB_OCEANBASE_SCHEMA_OB_SCHEMA_MGR_H_

#include <stdint.h>
#include "share/ob_define.h"
#include "lib/container/ob_vector.h"
#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_priv_mgr.h"
#include "share/schema/ob_outline_mgr.h"
#include "share/schema/ob_synonym_mgr.h"
#include "share/schema/ob_udf_mgr.h"
#include "share/schema/ob_sequence_mgr.h"
#include "share/schema/ob_sys_variable_mgr.h"
#include "share/schema/ob_profile_mgr.h"
#include "share/schema/ob_dblink_mgr.h"

namespace oceanbase {
namespace common {
class ObIAllocator;
}
namespace share {
namespace schema {
class ObServerSchemaService;
class ObSchemaGetterGuard;

class ObSimpleTenantSchema : public ObSchema {
public:
  ObSimpleTenantSchema();
  explicit ObSimpleTenantSchema(common::ObIAllocator* allocator);
  ObSimpleTenantSchema(const ObSimpleTenantSchema& src_schema);
  virtual ~ObSimpleTenantSchema();
  ObSimpleTenantSchema& operator=(const ObSimpleTenantSchema& other);
  bool operator==(const ObSimpleTenantSchema& other) const;
  TO_STRING_KV(K_(tenant_id), K_(schema_version), K_(tenant_name), K_(name_case_mode), K_(read_only), K_(primary_zone),
      K_(locality), K_(previous_locality), K_(compatibility_mode), K_(gmt_modified), K_(drop_tenant_time), K_(status),
      K_(in_recyclebin));
  virtual void reset();
  inline bool is_valid() const;
  inline int64_t get_convert_size() const;
  inline void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline void set_schema_version(const int64_t schema_version)
  {
    schema_version_ = schema_version;
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  inline int set_tenant_name(const common::ObString& tenant_name)
  {
    return deep_copy_str(tenant_name, tenant_name_);
  }
  inline const char* get_tenant_name() const
  {
    return extract_str(tenant_name_);
  }
  inline const common::ObString& get_tenant_name_str() const
  {
    return tenant_name_;
  }
  inline void set_name_case_mode(const common::ObNameCaseMode cmp_mode)
  {
    name_case_mode_ = cmp_mode;
  }
  //  inline common::ObNameCaseMode get_name_case_mode() const { return name_case_mode_; }
  inline void set_read_only(const bool read_only)
  {
    read_only_ = read_only;
  }
  inline bool get_read_only() const
  {
    return read_only_;
  }

  inline int set_primary_zone(const common::ObString& primary_zone)
  {
    return deep_copy_str(primary_zone, primary_zone_);
  }
  inline const common::ObString& get_primary_zone() const
  {
    return primary_zone_;
  }

  inline int set_locality(const common::ObString& locality)
  {
    return deep_copy_str(locality, locality_);
  }
  inline const char* get_locality() const
  {
    return extract_str(locality_);
  }
  inline const common::ObString& get_locality_str() const
  {
    return locality_;
  }

  inline int set_previous_locality(const common::ObString& previous_locality)
  {
    return deep_copy_str(previous_locality, previous_locality_);
  }
  inline const char* get_previous_locality() const
  {
    return extract_str(previous_locality_);
  }
  inline const common::ObString& get_previous_locality_str() const
  {
    return previous_locality_;
  }

  inline void set_compatibility_mode(const common::ObCompatibilityMode compatibility_mode)
  {
    compatibility_mode_ = compatibility_mode;
  }
  inline common::ObCompatibilityMode get_compatibility_mode() const
  {
    return compatibility_mode_;
  }

  inline void set_gmt_modified(const int64_t gmt_modified)
  {
    gmt_modified_ = gmt_modified;
  }
  inline int64_t get_gmt_modified() const
  {
    return gmt_modified_;
  }

  inline void set_drop_tenant_time(const int64_t drop_tenant_time)
  {
    drop_tenant_time_ = drop_tenant_time;
  }
  inline int64_t get_drop_tenant_time() const
  {
    return drop_tenant_time_;
  }
  inline bool is_dropping() const
  {
    return TENANT_STATUS_DROPPING == status_;
  }
  inline bool is_in_recyclebin() const
  {
    return in_recyclebin_;
  }
  inline bool is_creating() const
  {
    return TENANT_STATUS_CREATING == status_;
  }
  inline bool is_restore() const
  {
    return TENANT_STATUS_RESTORE == status_;
  }
  inline bool is_normal() const
  {
    return TENANT_STATUS_NORMAL == status_;
  }
  inline void set_status(const ObTenantStatus status)
  {
    status_ = status;
  }
  inline ObTenantStatus get_status() const
  {
    return status_;
  }
  inline void set_in_recyclebin(const bool in_recyclebin)
  {
    in_recyclebin_ = in_recyclebin;
  }

private:
  uint64_t tenant_id_;
  int64_t schema_version_;
  common::ObString tenant_name_;
  common::ObNameCaseMode name_case_mode_;  // deprecated
  bool read_only_;                         // Subject to the value of the system variable
  common::ObString primary_zone_;
  common::ObString locality_;
  common::ObString previous_locality_;
  common::ObCompatibilityMode compatibility_mode_;
  int64_t gmt_modified_;
  int64_t drop_tenant_time_;
  ObTenantStatus status_;
  bool in_recyclebin_;
};

class ObSimpleUserSchema : public ObSchema {
public:
  ObSimpleUserSchema();
  explicit ObSimpleUserSchema(common::ObIAllocator* allocator);
  ObSimpleUserSchema(const ObSimpleUserSchema& src_schema);
  virtual ~ObSimpleUserSchema();
  ObSimpleUserSchema& operator=(const ObSimpleUserSchema& other);
  bool operator==(const ObSimpleUserSchema& other) const;
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(schema_version), K_(user_name), K_(host_name), K_(type));
  virtual void reset();
  inline bool is_valid() const;
  inline int64_t get_convert_size() const;
  inline void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline void set_user_id(const uint64_t user_id)
  {
    user_id_ = user_id;
  }
  inline uint64_t get_user_id() const
  {
    return user_id_;
  }
  inline void set_schema_version(const int64_t schema_version)
  {
    schema_version_ = schema_version;
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  inline int set_user_name(const common::ObString& user_name)
  {
    return deep_copy_str(user_name, user_name_);
  }
  inline int set_host(const common::ObString& host_name)
  {
    return deep_copy_str(host_name, host_name_);
  }
  inline const char* get_user_name() const
  {
    return extract_str(user_name_);
  }
  inline const char* get_host_name() const
  {
    return extract_str(host_name_);
  }
  inline const common::ObString& get_user_name_str() const
  {
    return user_name_;
  }
  inline const common::ObString& get_host_name_str() const
  {
    return host_name_;
  }
  inline ObTenantUserId get_tenant_user_id() const
  {
    return ObTenantUserId(tenant_id_, user_id_);
  }
  inline void set_type(const uint64_t type)
  {
    type_ = type;
  }
  inline uint64_t get_type() const
  {
    return type_;
  }
  inline bool is_role() const
  {
    return OB_ROLE == type_;
  }

private:
  uint64_t tenant_id_;
  uint64_t user_id_;
  int64_t schema_version_;
  common::ObString user_name_;
  common::ObString host_name_;
  uint64_t type_;
};

class ObSimpleDatabaseSchema : public ObSchema {
public:
  ObSimpleDatabaseSchema();
  explicit ObSimpleDatabaseSchema(common::ObIAllocator* allocator);
  ObSimpleDatabaseSchema(const ObSimpleDatabaseSchema& src_schema);
  virtual ~ObSimpleDatabaseSchema();
  ObSimpleDatabaseSchema& operator=(const ObSimpleDatabaseSchema& other);
  bool operator==(const ObSimpleDatabaseSchema& other) const;
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(schema_version), K_(database_name), K_(name_case_mode),
      K_(drop_schema_version));
  virtual void reset();
  inline bool is_valid() const;
  inline int64_t get_convert_size() const;
  inline void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline void set_database_id(const uint64_t database_id)
  {
    database_id_ = database_id;
  }
  inline uint64_t get_database_id() const
  {
    return database_id_;
  }
  inline void set_schema_version(const int64_t schema_version)
  {
    schema_version_ = schema_version;
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  inline void set_default_tablegroup_id(const uint64_t default_tablegroup_id)
  {
    default_tablegroup_id_ = default_tablegroup_id;
  }
  inline uint64_t get_default_tablegroup_id() const
  {
    return default_tablegroup_id_;
  }
  inline int set_database_name(const common::ObString& database_name)
  {
    return deep_copy_str(database_name, database_name_);
  }
  inline const char* get_database_name() const
  {
    return extract_str(database_name_);
  }
  inline const common::ObString& get_database_name_str() const
  {
    return database_name_;
  }
  inline void set_name_case_mode(const common::ObNameCaseMode cmp_mode)
  {
    name_case_mode_ = cmp_mode;
  }
  inline common::ObNameCaseMode get_name_case_mode() const
  {
    return name_case_mode_;
  }
  inline ObTenantDatabaseId get_tenant_database_id() const
  {
    return ObTenantDatabaseId(tenant_id_, database_id_);
  }
  inline void set_drop_schema_version(const int64_t schema_version)
  {
    drop_schema_version_ = schema_version;
  }
  inline int64_t get_drop_schema_version() const
  {
    return drop_schema_version_;
  }
  inline bool is_dropped_schema() const
  {
    return drop_schema_version_ > 0;
  }

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  int64_t schema_version_;
  uint64_t default_tablegroup_id_;
  common::ObString database_name_;
  common::ObNameCaseMode name_case_mode_;
  int64_t drop_schema_version_;
};

class ObSimpleTablegroupSchema : public ObSchema {
public:
  ObSimpleTablegroupSchema();
  explicit ObSimpleTablegroupSchema(common::ObIAllocator* allocator);
  ObSimpleTablegroupSchema(const ObSimpleTablegroupSchema& src_schema);
  virtual ~ObSimpleTablegroupSchema();
  ObSimpleTablegroupSchema& operator=(const ObSimpleTablegroupSchema& other);
  bool operator==(const ObSimpleTablegroupSchema& other) const;
  TO_STRING_KV(K_(tenant_id), K_(tablegroup_id), K_(schema_version), K_(tablegroup_name), K_(primary_zone),
      K_(locality), K_(previous_locality), K_(partition_status), K_(binding), K_(partition_schema_version));
  virtual void reset();
  inline bool is_valid() const;
  inline int64_t get_convert_size() const;
  inline void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline void set_tablegroup_id(const uint64_t tablegroup_id)
  {
    tablegroup_id_ = tablegroup_id;
  }
  inline uint64_t get_tablegroup_id() const
  {
    return tablegroup_id_;
  }
  inline void set_schema_version(const int64_t schema_version)
  {
    schema_version_ = schema_version;
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  inline int set_tablegroup_name(const common::ObString& tablegroup_name)
  {
    return deep_copy_str(tablegroup_name, tablegroup_name_);
  }
  inline const char* get_tablegroup_name_str() const
  {
    return extract_str(tablegroup_name_);
  }
  inline const common::ObString& get_tablegroup_name() const
  {
    return tablegroup_name_;
  }
  inline ObTenantTablegroupId get_tenant_tablegroup_id() const
  {
    return ObTenantTablegroupId(tenant_id_, tablegroup_id_);
  }

  inline int set_primary_zone(const common::ObString& primary_zone)
  {
    return deep_copy_str(primary_zone, primary_zone_);
  }
  inline const common::ObString& get_primary_zone() const
  {
    return primary_zone_;
  }

  inline int set_locality(const common::ObString& locality)
  {
    return deep_copy_str(locality, locality_);
  }
  inline const char* get_locality() const
  {
    return extract_str(locality_);
  }
  inline const common::ObString& get_locality_str() const
  {
    return locality_;
  }

  inline int set_previous_locality(const common::ObString& previous_locality)
  {
    return deep_copy_str(previous_locality, previous_locality_);
  }
  inline const char* get_previous_locality() const
  {
    return extract_str(previous_locality_);
  }
  inline const common::ObString& get_previous_locality_str() const
  {
    return previous_locality_;
  }

  inline bool get_binding() const
  {
    return binding_;
  }
  inline void set_binding(const bool binding)
  {
    binding_ = binding;
  }

  int get_zone_list(
      share::schema::ObSchemaGetterGuard& schema_guard, common::ObIArray<common::ObZone>& zone_list) const;

  inline void set_partition_status(const ObPartitionStatus partition_status)
  {
    partition_status_ = partition_status;
  }
  inline ObPartitionStatus get_partition_status() const
  {
    return partition_status_;
  }
  inline void set_partition_schema_version(const int64_t schema_version)
  {
    partition_schema_version_ = schema_version;
  }
  int64_t get_partition_schema_version() const
  {
    return partition_schema_version_;
  }
  bool is_in_splitting() const
  {
    return partition_status_ == PARTITION_STATUS_LOGICAL_SPLITTING ||
           partition_status_ == PARTITION_STATUS_PHYSICAL_SPLITTING;
  }
  bool has_self_partition() const
  {
    return get_binding();
  }
  bool is_mock_global_index_invalid() const
  {
    return is_mock_global_index_invalid_;
  }
  void set_mock_global_index_invalid(const bool is_invalid)
  {
    is_mock_global_index_invalid_ = is_invalid;
  }

private:
  uint64_t tenant_id_;
  uint64_t tablegroup_id_;
  int64_t schema_version_;
  common::ObString tablegroup_name_;
  common::ObString primary_zone_;
  common::ObString locality_;
  common::ObString previous_locality_;
  ObPartitionStatus partition_status_;
  bool binding_;
  bool is_mock_global_index_invalid_;  // Standalone cluster use; no serialization required
  int64_t partition_schema_version_;
};

template <class K, class V>
struct GetTableKeyV2 {
  void operator()(const K& k, const V& v)
  {
    UNUSED(k);
    UNUSED(v);
  }
};
template <>
struct GetTableKeyV2<uint64_t, ObSimpleTableSchemaV2*> {
  uint64_t operator()(const ObSimpleTableSchemaV2* table_schema) const
  {
    return NULL != table_schema ? table_schema->get_table_id() : common::OB_INVALID_ID;
  }
};
template <>
struct GetTableKeyV2<uint64_t, ObSimpleDatabaseSchema*> {
  uint64_t operator()(const ObSimpleDatabaseSchema* database_schema) const
  {
    return NULL != database_schema ? database_schema->get_database_id() : common::OB_INVALID_ID;
  }
};
template <>
struct GetTableKeyV2<ObDatabaseSchemaHashWrapper, ObSimpleDatabaseSchema*> {
  ObDatabaseSchemaHashWrapper operator()(const ObSimpleDatabaseSchema* database_schema) const
  {
    if (!OB_ISNULL(database_schema)) {
      ObDatabaseSchemaHashWrapper database_schema_hash_wrapper(database_schema->get_tenant_id(),
          database_schema->get_name_case_mode(),
          database_schema->get_database_name_str());
      return database_schema_hash_wrapper;
    } else {
      ObDatabaseSchemaHashWrapper null_wrap;
      return null_wrap;
    }
  }
};
template <>
struct GetTableKeyV2<ObTablegroupSchemaHashWrapper, ObSimpleTablegroupSchema*> {
  ObTablegroupSchemaHashWrapper operator()(const ObSimpleTablegroupSchema* tablegroup_schema) const
  {
    if (!OB_ISNULL(tablegroup_schema)) {
      ObTablegroupSchemaHashWrapper tablegroup_schema_hash_wrapper(
          tablegroup_schema->get_tenant_id(), tablegroup_schema->get_tablegroup_name_str());
      return tablegroup_schema_hash_wrapper;
    } else {
      ObTablegroupSchemaHashWrapper null_wrap;
      return null_wrap;
    }
  }
};
template <>
struct GetTableKeyV2<ObTableSchemaHashWrapper, ObSimpleTableSchemaV2*> {
  ObTableSchemaHashWrapper operator()(const ObSimpleTableSchemaV2* table_schema) const
  {
    if (!OB_ISNULL(table_schema)) {
      ObTableSchemaHashWrapper table_schema_hash_wrapper(table_schema->get_tenant_id(),
          table_schema->get_database_id(),
          table_schema->get_session_id(),
          table_schema->get_name_case_mode(),
          table_schema->get_table_name_str());
      return table_schema_hash_wrapper;
    } else {
      ObTableSchemaHashWrapper null_wrap;
      return null_wrap;
    }
  }
};
template <>
struct GetTableKeyV2<ObIndexSchemaHashWrapper, ObSimpleTableSchemaV2*> {
  ObIndexSchemaHashWrapper operator()(const ObSimpleTableSchemaV2* index_schema) const
  {
    if (!OB_ISNULL(index_schema)) {
      bool is_oracle_mode = false;
      if (OB_UNLIKELY(OB_SUCCESS != index_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
        ObIndexSchemaHashWrapper null_wrap;
        return null_wrap;
      } else if (is_oracle_mode && !index_schema->is_in_recyclebin()) {
        // oracle mode and index is not in recyclebin
        ObIndexSchemaHashWrapper index_schema_hash_wrapper(
            index_schema->get_tenant_id(), index_schema->get_database_id(), index_schema->get_origin_index_name_str());
        return index_schema_hash_wrapper;
      } else {  // mysql mode or index is in recyclebin
        ObIndexSchemaHashWrapper index_schema_hash_wrapper(
            index_schema->get_tenant_id(), index_schema->get_database_id(), index_schema->get_table_name_str());
        return index_schema_hash_wrapper;
      }
    } else {
      ObIndexSchemaHashWrapper null_wrap;
      return null_wrap;
    }
  }
};

template <>
struct GetTableKeyV2<ObForeignKeyInfoHashWrapper, ObSimpleForeignKeyInfo*> {
  ObForeignKeyInfoHashWrapper operator()(const ObSimpleForeignKeyInfo* simple_foreign_key_info) const
  {
    if (OB_NOT_NULL(simple_foreign_key_info)) {
      ObForeignKeyInfoHashWrapper fk_info_hash_wrapper(simple_foreign_key_info->tenant_id_,
          simple_foreign_key_info->database_id_,
          simple_foreign_key_info->foreign_key_name_);
      return fk_info_hash_wrapper;
    } else {
      ObForeignKeyInfoHashWrapper null_wrap;
      return null_wrap;
    }
  }
};

template <>
struct GetTableKeyV2<ObConstraintInfoHashWrapper, ObSimpleConstraintInfo*> {
  ObConstraintInfoHashWrapper operator()(const ObSimpleConstraintInfo* simple_constraint_info) const
  {
    if (OB_NOT_NULL(simple_constraint_info)) {
      ObConstraintInfoHashWrapper cst_info_hash_wrapper(simple_constraint_info->tenant_id_,
          simple_constraint_info->database_id_,
          simple_constraint_info->constraint_name_);
      return cst_info_hash_wrapper;
    } else {
      ObConstraintInfoHashWrapper null_wrap;
      return null_wrap;
    }
  }
};

class ObSchemaMgr {
  friend class ObServerSchemaService;
  friend class ObSchemaGetterGuard;
  friend class ObSchemaMgrCache;
  friend class MockSchemaService;
  typedef common::ObSortedVector<ObSimpleTenantSchema*> TenantInfos;
  typedef common::ObSortedVector<ObSimpleUserSchema*> UserInfos;
  typedef common::ObSortedVector<ObSimpleDatabaseSchema*> DatabaseInfos;
  typedef common::ObSortedVector<ObSimpleTablegroupSchema*> TablegroupInfos;
  typedef common::ObSortedVector<ObSimpleTableSchemaV2*> TableInfos;
  typedef common::ObSortedVector<ObDropTenantInfo*> DropTenantInfos;
  typedef TenantInfos::iterator TenantIterator;
  typedef TenantInfos::const_iterator ConstTenantIterator;
  typedef UserInfos::iterator UserIterator;
  typedef UserInfos::const_iterator ConstUserIterator;
  typedef DatabaseInfos::iterator DatabaseIterator;
  typedef DatabaseInfos::const_iterator ConstDatabaseIterator;
  typedef TablegroupInfos::iterator TablegroupIterator;
  typedef TablegroupInfos::const_iterator ConstTablegroupIterator;
  typedef TableInfos::iterator TableIterator;
  typedef TableInfos::const_iterator ConstTableIterator;
  typedef DropTenantInfos::iterator DropTenantInfoIterator;
  typedef DropTenantInfos::const_iterator ConstDropTenantInfoIterator;
  typedef common::hash::ObPointerHashMap<ObDatabaseSchemaHashWrapper, ObSimpleDatabaseSchema*, GetTableKeyV2>
      DatabaseNameMap;
  typedef common::hash::ObPointerHashMap<uint64_t, ObSimpleTableSchemaV2*, GetTableKeyV2> TableIdMap;
  typedef common::hash::ObPointerHashMap<uint64_t, ObSimpleDatabaseSchema*, GetTableKeyV2> DatabaseIdMap;
  typedef common::hash::ObPointerHashMap<ObTableSchemaHashWrapper, ObSimpleTableSchemaV2*, GetTableKeyV2> TableNameMap;
  typedef common::hash::ObPointerHashMap<ObIndexSchemaHashWrapper, ObSimpleTableSchemaV2*, GetTableKeyV2> IndexNameMap;
  typedef common::hash::ObPointerHashMap<ObForeignKeyInfoHashWrapper, ObSimpleForeignKeyInfo*, GetTableKeyV2>
      ForeignKeyNameMap;
  typedef common::hash::ObPointerHashMap<ObConstraintInfoHashWrapper, ObSimpleConstraintInfo*, GetTableKeyV2>
      ConstraintNameMap;

public:
  ObSchemaMgr();
  explicit ObSchemaMgr(common::ObIAllocator& allocator);
  virtual ~ObSchemaMgr();
  int init(const uint64_t tenant_id = common::OB_INVALID_TENANT_ID);
  void reset();
  ObSchemaMgr& operator=(const ObSchemaMgr& other);
  int assign(const ObSchemaMgr& other);
  int deep_copy(const ObSchemaMgr& other);
  void dump() const;
  inline void set_schema_version(const int64_t schema_version)
  {
    schema_version_ = schema_version;
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  inline void set_is_consistent(bool is_consistent)
  {
    is_consistent_ = is_consistent;
  }
  inline bool get_is_consistent() const
  {
    return is_consistent_;
  }
  // tenant
  int add_tenants(const common::ObIArray<ObSimpleTenantSchema>& tenant_schemas);
  int del_tenants(const common::ObIArray<uint64_t>& tenants);
  int add_tenant(const ObSimpleTenantSchema& tenant_schema);
  int del_tenant(const uint64_t tenant_id);
  int get_tenant_schema(const uint64_t tenant_id, const ObSimpleTenantSchema*& tenant_schema) const;
  int get_tenant_schema(const common::ObString& tenant_name, const ObSimpleTenantSchema*& tenant_schema) const;

  int get_tenant_name_case_mode(const uint64_t tenant_id, common::ObNameCaseMode& mode) const;
  int get_tenant_read_only(const uint64_t tenant_id, bool& read_only) const;

  // user
  int add_users(const common::ObIArray<ObSimpleUserSchema>& user_schemas);
  int del_users(const common::ObIArray<ObTenantUserId>& users);
  int add_user(const ObSimpleUserSchema& user_schema);
  int del_user(const ObTenantUserId user);
  int get_user_schema(const uint64_t user_id, const ObSimpleUserSchema*& user_schema) const;
  int get_user_schema(const uint64_t tenant_id, const common::ObString& user_name, const common::ObString& host_name,
      const ObSimpleUserSchema*& user_schema) const;
  int get_user_schema(const uint64_t tenant_id, const common::ObString& user_name,
      common::ObIArray<const ObSimpleUserSchema*>& users_schema) const;
  // database
  int add_databases(const common::ObIArray<ObSimpleDatabaseSchema>& database_schemas);
  int del_databases(const common::ObIArray<ObTenantDatabaseId>& databases);
  int add_database(const ObSimpleDatabaseSchema& database_schema);
  int del_database(const ObTenantDatabaseId database);
  int get_database_schema(const uint64_t database_id, const ObSimpleDatabaseSchema*& database_schema) const;
  int get_database_schema(const uint64_t tenant_id, const common::ObString& database_name,
      const ObSimpleDatabaseSchema*& database_schema) const;
  // tablegroup
  int add_tablegroups(const common::ObIArray<ObSimpleTablegroupSchema>& tablegroup_schemas);
  int del_tablegroups(const common::ObIArray<ObTenantTablegroupId>& tablegroups);
  int add_tablegroup(const ObSimpleTablegroupSchema& database_schema);
  int del_tablegroup(const ObTenantTablegroupId tablegroup);
  int get_tablegroup_schema(const uint64_t tablegroup_id, const ObSimpleTablegroupSchema*& tablegroup_schema) const;
  int get_tablegroup_schema(const uint64_t tenant_id, const common::ObString& tablegroup_name,
      const ObSimpleTablegroupSchema*& tablegroup_schema) const;
  int get_tablegroup_ids_in_tenant(const uint64_t tenant_id, common::ObIArray<uint64_t>& tablegroup_id_array);
  // table
  int add_tables(const common::ObIArray<ObSimpleTableSchemaV2>& table_schemas);
  int del_tables(const common::ObIArray<ObTenantTableId>& tables);
  int add_table(const ObSimpleTableSchemaV2& table_schema);
  int del_table(const ObTenantTableId table);
  int remove_aux_table(const ObSimpleTableSchemaV2& schema_to_del);
  int get_table_schema(const uint64_t table_id, const ObSimpleTableSchemaV2*& table_schema) const;
  int get_table_schema(const uint64_t tenant_id, const uint64_t database_id, const uint64_t session_id,
      const common::ObString& table_name, const bool is_index, const ObSimpleTableSchemaV2*& table_schema) const;
  // foreign key
  int add_foreign_keys_in_table(const common::ObIArray<ObSimpleForeignKeyInfo>& fk_info_array, const int over_write);
  int delete_given_fk_from_mgr(const ObSimpleForeignKeyInfo& fk_info);
  int delete_foreign_keys_in_table(const ObSimpleTableSchemaV2& table_schema);
  int check_and_delete_given_fk_in_table(
      const ObSimpleTableSchemaV2* replaced_table, const ObSimpleTableSchemaV2* new_table);
  int get_foreign_key_id(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& foreign_key_name,
      uint64_t& foreign_key_id) const;
  int get_foreign_key_info(const uint64_t tenant_id, const uint64_t database_id,
      const common::ObString& foreign_key_name, ObSimpleForeignKeyInfo& foreign_key_info) const;
  // constraint
  int add_constraints_in_table(const common::ObIArray<ObSimpleConstraintInfo>& cst_info_array, const int over_write);
  int delete_given_cst_from_mgr(const ObSimpleConstraintInfo& cst_info);
  int delete_constraints_in_table(const ObSimpleTableSchemaV2& table_schema);
  int check_and_delete_given_cst_in_table(
      const ObSimpleTableSchemaV2* replaced_table, const ObSimpleTableSchemaV2* new_table);
  int get_constraint_id(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& constraint_name,
      uint64_t& constraint_id) const;
  int get_constraint_info(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& constraint_name,
      ObSimpleConstraintInfo& constraint_info) const;
  // dblink.
  int get_dblink_schema(const uint64_t dblink_id, const ObDbLinkSchema*& dblink_schema) const;
  int get_dblink_schema(
      const uint64_t tenant_id, const common::ObString& dblink_name, const ObDbLinkSchema*& dblink_schema) const;
  // other
  int get_tenant_schemas(common::ObIArray<const ObSimpleTenantSchema*>& tenant_schemas) const;
  int get_tenant_ids(common::ObIArray<uint64_t>& tenant_ids) const;
  int get_available_tenant_ids(common::ObIArray<uint64_t>& tenant_ids) const;
#define GET_SCHEMAS_IN_TENANT_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE)                                                      \
  int get_##SCHEMA##_schemas_in_tenant(const uint64_t tenant_id, common::ObIArray<const SCHEMA_TYPE*>& schema_array) \
      const;
  GET_SCHEMAS_IN_TENANT_FUNC_DECLARE(user, ObSimpleUserSchema);
  GET_SCHEMAS_IN_TENANT_FUNC_DECLARE(database, ObSimpleDatabaseSchema);
  GET_SCHEMAS_IN_TENANT_FUNC_DECLARE(tablegroup, ObSimpleTablegroupSchema);
#undef GET_SCHEMAS_IN_TENANT_FUNC_DECLARE
#define GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DECLARE(DST_SCHEMA)  \
  int get_table_schemas_in_##DST_SCHEMA(const uint64_t tenant_id, \
      const uint64_t dst_schema_id,                               \
      bool need_reset,                                            \
      common::ObIArray<const ObSimpleTableSchemaV2*>& schema_array) const;
  GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DECLARE(database);
  GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DECLARE(tablegroup);
  int get_table_schemas_in_tenant(
      const uint64_t tenant_id, bool need_reset, common::ObIArray<const ObSimpleTableSchemaV2*>& schema_array) const;
#undef GET_TABLE_SCHEMAS_IN_DST_SCHEMA_FUNC_DECLARE
  int check_database_exists_in_tablegroup(
      const uint64_t tenant_id, const uint64_t tablegroup_id, bool& not_empty) const;
  int batch_get_next_table(const ObTenantTableId tenant_table_id, const int64_t get_size,
      common::ObIArray<ObTenantTableId>& table_array) const;
  int get_index_schemas(
      const uint64_t data_table_id, common::ObIArray<const ObSimpleTableSchemaV2*>& index_schemas) const;
  int get_aux_schemas(const uint64_t data_table_id, common::ObIArray<const ObSimpleTableSchemaV2*>& aux_schemas,
      const share::schema::ObTableType table_type) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);

  int get_tenant_mv_ids(const uint64_t tenant_id, common::ObIArray<uint64_t>& mv_ids) const;
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }

  int add_drop_tenant_info(const ObDropTenantInfo& drop_tenant_info);
  int add_drop_tenant_infos(const common::ObIArray<ObDropTenantInfo>& drop_tenant_infos);
  int del_drop_tenant_info(const uint64_t tenant_id);
  // drop_tenant_info is invalid, indicating that the DDL of the drop tenant has not been read
  int get_drop_tenant_info(const uint64_t tenant_id, ObDropTenantInfo& drop_tenant_info) const;

  /*schema statistics*/
  int get_schema_size(int64_t& total_size) const;
  int get_schema_count(int64_t& schema_count) const;
  int get_schema_statistics(common::ObIArray<ObSchemaStatisticsInfo>& schema_infos) const;

private:
  inline bool check_inner_stat() const;
  inline static bool compare_tenant(const ObSimpleTenantSchema* lhs, const ObSimpleTenantSchema* rhs);
  inline static bool equal_tenant(const ObSimpleTenantSchema* lhs, const ObSimpleTenantSchema* rhs);
  inline static bool compare_with_tenant_id(const ObSimpleTenantSchema* lhs, const uint64_t tenant_id);
  inline static bool equal_with_tenant_id(const ObSimpleTenantSchema* lhs, const uint64_t tenant_id);
  inline static bool compare_user(const ObSimpleUserSchema* lhs, const ObSimpleUserSchema* rhs);
  inline static bool equal_user(const ObSimpleUserSchema* lhs, const ObSimpleUserSchema* rhs);
  inline static bool compare_with_tenant_user_id(const ObSimpleUserSchema* lhs, const ObTenantUserId& tenant_user_id);
  inline static bool equal_with_tenant_user_id(const ObSimpleUserSchema* lhs, const ObTenantUserId& tenant_user_id);
  inline static bool compare_database(const ObSimpleDatabaseSchema* lhs, const ObSimpleDatabaseSchema* rhs);
  inline static bool equal_database(const ObSimpleDatabaseSchema* lhs, const ObSimpleDatabaseSchema* rhs);
  inline static bool compare_with_tenant_database_id(
      const ObSimpleDatabaseSchema* lhs, const ObTenantDatabaseId& tenant_database_id);
  inline static bool equal_with_tenant_database_id(
      const ObSimpleDatabaseSchema* lhs, const ObTenantDatabaseId& tenant_database_id);
  inline static bool compare_tablegroup(const ObSimpleTablegroupSchema* lhs, const ObSimpleTablegroupSchema* rhs);
  inline static bool equal_tablegroup(const ObSimpleTablegroupSchema* lhs, const ObSimpleTablegroupSchema* rhs);
  inline static bool compare_with_tenant_tablegroup_id(
      const ObSimpleTablegroupSchema* lhs, const ObTenantTablegroupId& tenant_tablegroup_id);
  inline static bool equal_with_tenant_tablegroup_id(
      const ObSimpleTablegroupSchema* lhs, const ObTenantTablegroupId& tenant_tablegroup_id);
  inline static bool compare_table(const ObSimpleTableSchemaV2* lhs, const ObSimpleTableSchemaV2* rhs);
  inline static bool compare_aux_table(const ObSimpleTableSchemaV2* lhs, const ObSimpleTableSchemaV2* rhs);
  // inline static bool compare_table_with_data_table_id(const ObSimpleTableSchemaV2 *lhs,
  //                                                    const ObSimpleTableSchemaV2 *rhs);
  inline static bool equal_table(const ObSimpleTableSchemaV2* lhs, const ObSimpleTableSchemaV2* rhs);
  inline static bool compare_with_tenant_table_id(
      const ObSimpleTableSchemaV2* lhs, const ObTenantTableId& tenant_table_id);
  inline static bool compare_with_tenant_data_table_id(
      const ObSimpleTableSchemaV2* lhs, const ObTenantTableId& tenant_table_id);
  inline static bool equal_with_tenant_table_id(
      const ObSimpleTableSchemaV2* lhs, const ObTenantTableId& tenant_table_id);
  inline static bool compare_tenant_table_id_up(
      const ObTenantTableId& tenant_table_id, const ObSimpleTableSchemaV2* lhs);
  int deal_with_table_rename(
      const ObSimpleTableSchemaV2& old_table_schema, const ObSimpleTableSchemaV2& new_table_schema);
  int deal_with_db_rename(const ObSimpleDatabaseSchema& old_db_schema, const ObSimpleDatabaseSchema& new_db_schema);

  static bool compare_drop_tenant_info(const ObDropTenantInfo* lhs, const ObDropTenantInfo* rhs);
  static bool equal_drop_tenant_info(const ObDropTenantInfo* lhs, const ObDropTenantInfo* rhs);

  // schema meta consistent related
  bool check_schema_meta_consistent();
  int rebuild_schema_meta_if_not_consistent();
  int rebuild_table_hashmap(uint64_t& fk_cnt, uint64_t& cst_cnt);
  int rebuild_db_hashmap();

  int get_table_schema(const uint64_t tenant_id, const uint64_t database_id, const uint64_t session_id,
      const common::ObString& table_name, const ObSimpleTableSchemaV2*& table_schema) const;
  int get_index_schema(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& table_name,
      const ObSimpleTableSchemaV2*& table_schema) const;
  int get_object_with_synonym(const uint64_t tenant_id, const uint64_t database_id,
      const common::ObString& synonym_name, common::ObString& table_name, uint64_t& out_database_id,
      uint64_t& synonym_id, bool& do_exist) const;
  int get_synonym_schema(const uint64_t synonym_id, const ObSimpleSynonymSchema*& synonym_schema) const;
  int get_sequence_schema(const uint64_t sequence_id, const ObSequenceSchema*& sequence_schema) const;
  int get_udf_schema(const uint64_t udf_id, const ObSimpleUDFSchema*& udf_schema) const
  {
    return udf_mgr_.get_udf_schema(udf_id, udf_schema);
  }
  int get_profile_schema(const uint64_t schema_id, const ObProfileSchema*& schema) const
  {
    return profile_mgr_.get_schema_by_id(schema_id, schema);
  }
  int get_idx_schema_by_origin_idx_name(const uint64_t tenant_id, const uint64_t database_id,
      const common::ObString& index_name, const ObSimpleTableSchemaV2*& table_schema) const;

  /*schema statistics*/
  int get_tenant_statistics(ObSchemaStatisticsInfo& schema_info) const;
  int get_user_statistics(ObSchemaStatisticsInfo& schema_info) const;
  int get_database_statistics(ObSchemaStatisticsInfo& schema_info) const;
  int get_tablegroup_statistics(ObSchemaStatisticsInfo& schema_info) const;
  int get_table_statistics(ObSchemaStatisticsInfo& schema_info) const;

private:
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator& allocator_;
  int64_t schema_version_;
  TenantInfos tenant_infos_;
  UserInfos user_infos_;
  DatabaseInfos database_infos_;
  DatabaseNameMap database_name_map_;
  TablegroupInfos tablegroup_infos_;
  TableInfos table_infos_;
  TableInfos index_infos_;
  TableIdMap table_id_map_;
  TableNameMap table_name_map_;
  IndexNameMap index_name_map_;
  ObOutlineMgr outline_mgr_;
  ObPrivMgr priv_mgr_;
  ObSynonymMgr synonym_mgr_;
  ObUDFMgr udf_mgr_;
  ObSequenceMgr sequence_mgr_;
  ObProfileMgr profile_mgr_;
  ForeignKeyNameMap foreign_key_name_map_;
  ConstraintNameMap constraint_name_map_;
  ObSysVariableMgr sys_variable_mgr_;
  uint64_t tenant_id_;
  DropTenantInfos drop_tenant_infos_;
  bool is_consistent_;
  TableIdMap delay_deleted_table_map_;
  DatabaseIdMap delay_deleted_database_map_;
  ObDbLinkMgr dblink_mgr_;
};

}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase
#endif  // OB_OCEANBASE_SCHEMA_OB_SCHEMA_MGR_H_
