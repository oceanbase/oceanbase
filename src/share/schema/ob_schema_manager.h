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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_MANAGER_H_
#define OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_MANAGER_H_
#include <stdint.h>
#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_vector.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/hash/ob_placement_hashmap.h"
#include "lib/allocator/ob_memfrag_recycle_allocator.h"
#include "common/ob_hint.h"
#include "common/rowkey/ob_rowkey_info.h"
#include "common/object/ob_object.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_priv_manager.h"
#include "share/part/ob_part_mgr.h"

namespace oceanbase {
namespace common {
class ObTimeZoneInfo;
class ObMySQLProxy;
class ObObj;
}  // namespace common

namespace storagetest {
class ObDataManager;
}

namespace share {
namespace schema {
static const uint64_t OB_SCHEMA_INVALID_ID = 0;

template <class K, class V>
struct GetTableKey {
  void operator()(const K& k, const V& v)
  {
    UNUSED(k);
    UNUSED(v);
  }
};

template <>
struct GetTableKey<uint64_t, ObTableSchema*> {
  uint64_t operator()(const ObTableSchema* table_schema) const
  {
    return NULL != table_schema ? table_schema->get_table_id() : common::OB_INVALID_ID;
  }
};

template <>
struct GetTableKey<common::ObString, ObTableSchema*> {
  common::ObString operator()(const ObTableSchema* table_schema) const
  {
    return NULL != table_schema ? table_schema->get_table_name_str() : common::ObString::make_string("");
  }
};

template <>
struct GetTableKey<ObTableSchemaHashWrapper, ObTableSchema*> {
  ObTableSchemaHashWrapper operator()(const ObTableSchema* table_schema) const
  {
    if (!OB_ISNULL(table_schema)) {
      ObTableSchemaHashWrapper table_schema_hash_wrapper(table_schema->get_tenant_id(),
          table_schema->get_database_id(),
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
struct GetTableKey<ObIndexSchemaHashWrapper, ObTableSchema*> {
  ObIndexSchemaHashWrapper operator()(const ObTableSchema* index_schema) const
  {
    if (!OB_ISNULL(index_schema)) {
      ObIndexSchemaHashWrapper index_schema_hash_wrapper(
          index_schema->get_tenant_id(), index_schema->get_database_id(), index_schema->get_table_name_str());
      return index_schema_hash_wrapper;
    } else {
      ObIndexSchemaHashWrapper null_wrap;
      return null_wrap;
    }
  }
};

template <>
struct GetTableKey<ObDatabaseSchemaHashWrapper, ObDatabaseSchema*> {
  ObDatabaseSchemaHashWrapper operator()(const ObDatabaseSchema* database_schema) const
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
struct GetTableKey<ObTablegroupSchemaHashWrapper, ObTablegroupSchema*> {
  ObTablegroupSchemaHashWrapper operator()(const ObTablegroupSchema* tablegroup_schema) const
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

class ObSchemaManager {
public:
  friend class oceanbase::storagetest::ObDataManager;
  static const int64_t SCHEMA_MEM_EXPIRE_TIME = 3600 * 1000 * 1000L;  // one hour
  typedef common::ObSortedVector<ObTableSchema*>::iterator table_iterator;
  typedef common::ObSortedVector<ObDatabaseSchema*>::iterator database_iterator;
  typedef common::ObSortedVector<ObTablegroupSchema*>::iterator tablegroup_iterator;
  typedef common::ObSortedVector<ObTableSchema*>::const_iterator const_table_iterator;
  typedef common::ObSortedVector<ObDatabaseSchema*>::const_iterator const_database_iterator;
  typedef common::ObSortedVector<ObTablegroupSchema*>::const_iterator const_tablegroup_iterator;

  ObSchemaManager();
  explicit ObSchemaManager(common::ObMemfragRecycleAllocator& global_allocator);
  int assign(const ObSchemaManager& schema, const bool full_copy);
  virtual ~ObSchemaManager();
  int init(const bool is_init_sys_tenant = true);
  void reset();

public:
  //---------memory related functions-----
  common::ObMemfragRecycleAllocator& get_allocator();
  int copy_schema_infos(const ObSchemaManager& schema_manager);
  virtual int deep_copy(const ObSchemaManager& schema);

  const_table_iterator table_begin() const
  {
    return table_infos_.begin();
  }
  const_table_iterator table_end() const
  {
    return table_infos_.end();
  }
  const_database_iterator database_begin() const
  {
    return database_infos_.begin();
  }
  const_database_iterator database_end() const
  {
    return database_infos_.end();
  }
  const_tablegroup_iterator tablegroup_begin() const
  {
    return tablegroup_infos_.begin();
  }
  const_tablegroup_iterator tablegroup_end() const
  {
    return tablegroup_infos_.end();
  }

  int column_can_be_dropped(const uint64_t table_id, const uint64_t column_id, bool& can_be_dropper) const;
  int64_t get_total_column_count() const;
  int64_t get_table_count() const
  {
    return table_infos_.count();
  }
  int64_t get_index_table_count() const
  {
    return index_table_infos_.count();
  }
  int64_t get_database_count() const
  {
    return database_infos_.count();
  }
  int64_t get_tablegroup_count() const
  {
    return tablegroup_infos_.count();
  }
  /**
   * @brief timestap is the version of schema,version_ is used for serialize syntax
   *
   * @return
   */
  int64_t get_schema_version() const
  {
    return schema_version_;
  }
  void set_schema_version(const int64_t version)
  {
    ATOMIC_SET(&schema_version_, version);
  }
  int check_table_exist(const uint64_t table_id, bool& exist) const;
  int check_table_exist(const uint64_t tenant_id, const common::ObString& database_name,
      const common::ObString& table_name, const bool is_index, bool& exist) const;
  int check_table_exist(const uint64_t tenant_id, const uint64_t database_id, const common::ObString& table_name,
      const bool is_index, bool& exist) const;
  int check_database_exist(
      const uint64_t tenant_id, const common::ObString& database_name, uint64_t& database_id, bool& is_exist) const;
  int check_database_exist(const uint64_t database_id, bool& is_exist) const;
  int check_tablegroup_exist(
      const uint64_t tenant_id, const common::ObString& tablegroup_name, uint64_t& tablegroup_id, bool& is_exist) const;
  int check_tablegroup_exist(const uint64_t tablegroup_id, bool& is_exist) const;

  /**
   * @brief get a column accroding to table_id/column_id     *
   * @param table_id: the id of the table
   * @param column_id: the id of the column
   * @return column or null
   */
  const ObColumnSchemaV2* get_column_schema(const uint64_t table_id, const uint64_t column_id) const;
  const ObColumnSchemaV2* get_column_schema(const uint64_t table_id, const common::ObString& column_name) const;
  const ObColumnSchemaV2* get_column_schema(const uint64_t tenant_id, const uint64_t database_id,
      const common::ObString& table_name, const common::ObString& column_name, const bool is_index) const;

  // get index schema if is_index is true, otherwise get table schema
  const ObTableSchema* get_table_schema(const uint64_t tenant_id, const uint64_t database_id,
      const common::ObString& table_name, const bool is_index) const;
  const ObTableSchema* get_table_schema(const uint64_t tenant_id, const common::ObString& database_name,
      const common::ObString& table_name, const bool is_index) const;
  const ObTableSchema* get_table_schema(const uint64_t table_id) const;

  const ObTableSchema* get_index_table_schema(const uint64_t data_table_id, const common::ObString& index_name) const;
  const ObTableSchema* get_index_table_schema(const uint64_t tenant_id, const uint64_t database_id,
      const common::ObString& data_table_name, const common::ObString& index_name) const;
  // get all table schemas' pointers belong to the specified tenant
  int get_table_schemas_in_tenant(
      const uint64_t tenant_id, common::ObIArray<const ObTableSchema*>& table_schemas) const;
  int get_table_ids_in_tenant(const uint64_t tenant_id, common::ObIArray<uint64_t>& table_ids) const;
  // get all table schemas' pointers belong to the specified database
  int get_table_schemas_in_database(const uint64_t tenant_id, const uint64_t database_id,
      common::ObIArray<const ObTableSchema*>& table_schemas) const;
  int get_table_ids_in_database(
      const uint64_t tenant_id, const uint64_t dataspace_id, common::ObIArray<uint64_t>& table_id_array) const;
  int get_table_ids_in_tablegroup(
      const uint64_t tenant_id, const uint64_t tablegroup_id, common::ObIArray<uint64_t>& table_id_array) const;
  int get_table_schemas_in_tablegroup(const uint64_t tenant_id, const uint64_t tablegroup_id,
      common::ObIArray<const ObTableSchema*>& table_schemas) const;
  int check_database_exists_in_tablegroup(
      const uint64_t tenant_id, const uint64_t tablegroup_id, bool& not_empty) const;
  int batch_get_next_table(const ObTenantTableId tenant_table_id, const int64_t get_size,
      common::ObIArray<ObTenantTableId>& table_array) const;

  inline int64_t get_code_version() const
  {
    return code_version_;
  }
  inline void set_code_version(const int64_t code_version)
  {
    code_version_ = code_version;
  }

  virtual int del_databases(const common::hash::ObHashSet<uint64_t>& database_id_set);
  virtual int del_tablegroups(const common::hash::ObHashSet<uint64_t>& tablegroup_id_set);
  virtual int del_tables(const common::hash::ObHashSet<uint64_t>& table_id_set);
  virtual int del_schemas_in_tenants(const common::hash::ObHashSet<uint64_t>& tenant_id_set);

  int update_max_used_table_ids(const common::ObIArray<uint64_t>& max_used_table_ids);

  inline int clear_max_used_table_id_map()
  {
    return max_used_table_id_map_.clear();
  }

  int get_database_schemas_in_tenant(
      const uint64_t tenant_id, common::ObIArray<const ObDatabaseSchema*>& database_schemas) const;
  int get_tablegroup_schemas_in_tenant(
      const uint64_t tenant_id, common::ObArray<const ObTablegroupSchema*>& tablegroup_schemas) const;
  int get_tablegroup_ids_in_tenant(const uint64_t tenant_id, common::ObArray<uint64_t>& tablegroup_id_array) const;

  // TODO  optimize if necessary
  const ObDatabaseSchema* get_database_schema(const uint64_t tenant_id, const uint64_t database_id) const;
  const ObDatabaseSchema* get_database_schema(const uint64_t database_id) const;
  const ObDatabaseSchema* get_database_schema(const uint64_t tenant_id, const common::ObString& database) const;
  const ObTablegroupSchema* get_tablegroup_schema(const uint64_t tenant_id, const uint64_t tablegroup_id) const;
  const ObTablegroupSchema* get_tablegroup_schema(const uint64_t tablegroup_id) const;
  // convert new table schemas into old one and insert it into the schema_manager
  virtual int add_new_table_schema_array(const common::ObArray<ObTableSchema>& table_schema_array);
  virtual int add_new_table_schema_array(const common::ObArray<ObTableSchema*>& table_schema_array);
  virtual int add_new_database_schema_array(const common::ObArray<ObDatabaseSchema>& database_schema_array);
  virtual int add_new_tablegroup_schema_array(const common::ObArray<ObTablegroupSchema>& tablegroup_schema_array);
  // convert new table schema into old one and insert it into the schema_manager
  virtual int add_new_table_schema(const ObTableSchema& tschema);  // add virtual for test
  int add_new_database_schema(const ObDatabaseSchema& db_schema);
  int add_new_tablegroup_schema(const ObTablegroupSchema& tg_schema);
  // TODO(): need to be private
  int add_table_schema(ObTableSchema* table_schema, const bool is_replace);
  int add_database_schema(ObDatabaseSchema* database_schema, const bool is_replace);
  int add_tablegroup_schema(ObTablegroupSchema* tablegroup_schema, const bool is_replace);

  // construct relation in mem from table id to index table id
  // call get_index_tid_array after this function
  virtual int cons_table_to_index_relation(const common::hash::ObHashSet<uint64_t>* data_table_id_set = NULL);
  int get_can_read_index_array(uint64_t table_id, uint64_t* index_tid_array, int64_t& size bool with_mv) const;
  int get_can_write_index_array(uint64_t table_id, uint64_t* index_tid_array, int64_t& size) const;

  const ObPrivManager& get_priv_mgr() const
  {
    return priv_mgr_;
  }
  ObPrivManager& get_priv_mgr()
  {
    return priv_mgr_;
  }
  const ObPrivManager& get_const_priv_mgr() const
  {
    return priv_mgr_;
  }

  int64_t get_pos_in_user_schemas() const
  {
    return pos_in_user_schemas_;
  }
  int set_pos_in_user_schemas(int64_t pos)
  {
    int ret = common::OB_SUCCESS;
    pos_in_user_schemas_ = pos;
    return ret;
  }
  int get_tenant_id(const common::ObString& tenant_name, uint64_t& tenant_id) const;
  int add_tenant_info(const ObTenantSchema& tenant_schema);
  const ObTenantSchema* get_tenant_info(const uint64_t tenant_id) const;
  const ObTenantSchema* get_tenant_info(const common::ObString& tenant_name) const;
  virtual int get_tenant_ids(common::ObIArray<uint64_t>& tenant_ids) const;
  int get_tenant_name_case_mode(const uint64_t tenant_id, common::ObNameCaseMode& mode) const;

  const ObUserInfo* get_user_info(const ObTenantUserId& user_key) const;
  const ObDBPriv* get_db_priv(const ObOriginalDBKey& db_priv_key) const;
  const ObTablePriv* get_table_priv(const ObTablePrivSortKey& able_priv_key) const;
  int check_db_access(ObSessionPrivInfo& s_priv, const common::ObString& database_name) const;
  int check_user_access(const ObUserLoginInfo& login_info, ObSessionPrivInfo& session_priv) const;

  int check_priv(const ObSessionPrivInfo& session_priv, const ObStmtNeedPrivs& stmt_need_privs) const;

  int check_priv_or(const ObSessionPrivInfo& session_priv, const ObStmtNeedPrivs& stmt_need_privs) const;
  int check_table_show(const ObSessionPrivInfo& s_priv, const common::ObString& db, const common::ObString& table,
      bool& allow_show) const;

  // check read_only attribute of database schema or table schema
  int verify_read_only(const uint64_t tenant_id, const ObStmtNeedPrivs& stmt_need_privs) const;

  int print_table_definition(
      uint64_t table_id, char* buf, const int64_t& buf_len, int64_t& pos, const common::ObTimeZoneInfo* tz_info) const;

  int print_view_definiton(uint64_t table_id, char* buf, const int64_t& buf_len, int64_t& pos) const;

  int print_database_definiton(
      uint64_t database_id, bool if_not_exists, char* buf, const int64_t& buf_len, int64_t& pos) const;

  void print_info(bool verbose = false) const;

  int print_table_definition_columns(const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos,
      const common::ObTimeZoneInfo* tz_info) const;
  int print_table_definition_indexes(
      const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos, bool is_unique_index) const;
  int print_table_definition_rowkeys(
      const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos) const;
  int print_table_definition_table_options(const ObTableSchema& table_schema, char* buf, const int64_t& buf_len,
      int64_t& pos, bool is_for_table_status) const;
  int print_table_definition_partition_options(
      const ObTableSchema& table_schema, char* buf, const int64_t& buf_len, int64_t& pos) const;
  int print_tenant_definition(
      uint64_t tenant_id, common::ObMySQLProxy* sql_proxy, char* buf, const int64_t& buf_len, int64_t& pos) const;
  int deep_copy_obj(const common::ObObj& src, common::ObObj& dest) const;

private:
  ObSchemaManager& operator=(const ObSchemaManager& schema);

  template <class T>
  void free(T*& p);
  static bool compare_table_schema(const ObTableSchema* lhs, const ObTableSchema* rhs);
  static bool compare_tenant_table_id(const ObTableSchema* lhs, const ObTenantTableId& table_id);
  static bool compare_tenant_table_id_up(const ObTenantTableId& table_id, const ObTableSchema* lhs);
  static bool equal_table_schema(const ObTableSchema* lhs, const ObTableSchema* rhs);
  static bool equal_tenant_table_id(const ObTableSchema* lhs, const ObTenantTableId& tenant_table_id);
  static bool compare_database_schema(const ObDatabaseSchema* lhs, const ObDatabaseSchema* rhs);
  static bool compare_tenant_database_id(const ObDatabaseSchema* lhs, const ObTenantDatabaseId& tenant_database_id);
  static bool compare_tenant_database_id_up(const ObTenantDatabaseId& tenant_database_id, const ObDatabaseSchema* lhs);
  static bool equal_database_schema(const ObDatabaseSchema* lhs, const ObDatabaseSchema* rhs);
  static bool equal_tenant_database_id(const ObDatabaseSchema* lhs, const ObTenantDatabaseId& tenant_database_id);
  static bool compare_tablegroup_schema(const ObTablegroupSchema* lhs, const ObTablegroupSchema* rhs);
  static bool compare_tenant_tablegroup_id(
      const ObTablegroupSchema* lhs, const ObTenantTablegroupId& tenant_tablegroup_id);
  static bool compare_tenant_tablegroup_id_up(
      const ObTenantTablegroupId& tenant_tablegroup_id, const ObTablegroupSchema* lhs);
  static bool equal_tablegroup_schema(const ObTablegroupSchema* lhs, const ObTablegroupSchema* rhs);
  static bool equal_tenant_tablegroup_id(
      const ObTablegroupSchema* lhs, const ObTenantTablegroupId& tenant_tablegroup_id);

  int do_table_name_check(const ObTableSchema* old_table_schema, const ObTableSchema* new_table_schema);
  int do_tablegroup_name_check(const ObTablegroupSchema* old_tg_schema, const ObTablegroupSchema* new_tg_schema);
  int do_database_name_check(const ObDatabaseSchema* old_db_schema, const ObDatabaseSchema* new_db_schema);

  int del_database(const ObTenantDatabaseId database_to_delete);
  int del_tablegroup(const ObTenantTablegroupId tablegroup_to_delete);
  int del_table(const ObTenantTableId table_to_delete);
  // delete all schemas of teant
  int del_schemas_in_tenant(const uint64_t tenant_id);

  int build_table_hashmap();
  int build_database_hashmap();
  int build_tablegroup_hashmap();
  int cons_all_table_to_index_relation();
  int cons_part_table_to_index_relation(const common::hash::ObHashSet<uint64_t>& data_table_id_set);

  int check_table_exist(
      const uint64_t tenant_id, const uint64_t database_id, const common::ObString& table_name, bool& exist) const;
  int check_index_exist(
      const uint64_t tenant_id, const uint64_t database_id, const common::ObString& index_name, bool& exist) const;
  const ObTableSchema* get_table_schema(
      const uint64_t tenant_id, const uint64_t database_id, const common::ObString& table_name) const;
  const ObTableSchema* get_table_schema(
      const uint64_t tenant_id, const common::ObString& database_name, const common::ObString& table_name) const;
  const ObTableSchema* get_index_schema(
      const uint64_t tenant_id, const uint64_t database_id, const common::ObString& index_name) const;
  const ObTableSchema* get_index_schema(
      const uint64_t tenant_id, const common::ObString& database_name, const common::ObString& index_name) const;
  int verify_db_read_only(const uint64_t tenant_id, const ObNeedPriv& need_priv) const;
  int verify_table_read_only(const uint64_t tenant_id, const ObNeedPriv& need_priv) const;

public:
  // TODO(XX) I need it, please not delete. 20150413
  int get_database_id(uint64_t tenant_id, const common::ObString& database_name, uint64_t& database_id) const;
  int get_database_name(const uint64_t tenant_id, const uint64_t database_id, common::ObString& database_name) const;
  int get_tablegroup_id(uint64_t tenant_id, const common::ObString& tablegroup_name, uint64_t& tablegroup_id) const;

  inline int64_t get_alloc_table_count() const
  {
    return n_alloc_tables_;
  }
  inline int64_t get_global_alloc_size() const
  {
    return global_alloc_size_;
  }

private:
  // how to be compatible
  static const int64_t OB_SCHEMA_MAGIC_NUMBER = 0x43532313;  // SC

  int64_t schema_magic_;
  int64_t code_version_;    // for upgrade use
  int64_t schema_version_;  // the version of schema,actually the time of last modification of the schema
  // number of new allocated table schemas, for stats usage
  int64_t n_alloc_tables_;
  int64_t global_alloc_size_;  // allocated size by schema mgr cache
  common::ObSortedVector<ObTableSchema*, common::ModulePageAllocator> table_infos_;
  // this vector redundantly storages index tables to construct index array of table schema
  common::ObSortedVector<ObTableSchema*, common::ModulePageAllocator> index_table_infos_;
  common::ObSortedVector<ObDatabaseSchema*, common::ModulePageAllocator> database_infos_;
  common::ObSortedVector<ObTablegroupSchema*, common::ModulePageAllocator> tablegroup_infos_;
  common::hash::ObPointerHashMap<uint64_t, ObTableSchema*, GetTableKey> table_id_map_;
  common::hash::ObPointerHashMap<ObTableSchemaHashWrapper, ObTableSchema*, GetTableKey> table_name_map_;
  common::hash::ObPointerHashMap<ObIndexSchemaHashWrapper, ObTableSchema*, GetTableKey> index_name_map_;
  // (Tenant_id, database_name) to database_schema mapping
  common::hash::ObPointerHashMap<ObDatabaseSchemaHashWrapper, ObDatabaseSchema*, GetTableKey> database_name_map_;
  // (Tenant_id, tablegroup_name) to tablegroup_schema mapping
  common::hash::ObPointerHashMap<ObTablegroupSchemaHashWrapper, ObTablegroupSchema*, GetTableKey> tablegroup_name_map_;
  // max used table_id of tenant (tenant_id -> table_id)
  common::hash::ObHashMap<uint64_t, uint64_t> max_used_table_id_map_;
  // TODO(): we did not constuct a index (tenant_id,tablegroup_id) ->ObTablegroupSchema *
  // if performance is not satisfatory ,the same to database
  bool use_global_allocator_;
  common::ObMemfragRecycleAllocator local_allocator_;
  common::ObMemfragRecycleAllocator& allocator_;
  ObPrivManager priv_mgr_;
  // index for schema in user_schemas
  int64_t pos_in_user_schemas_;
  common::ObPartMgr* part_mgr_;
};

template <class T>
void ObSchemaManager::free(T*& p)
{
  if (NULL != p) {
    p->~T();
    allocator_.free(p);
    BACKTRACE(INFO, true, "free schema[%p]", p);
    p = NULL;
  }
}

}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase

#endif
