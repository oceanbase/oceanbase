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
#ifndef OCEANBASE_ROOTSERVER_OB_DDL_HELPER_H_
#define OCEANBASE_ROOTSERVER_OB_DDL_HELPER_H_

#include "lib/hash/ob_hashmap.h"
#include "rootserver/ob_ddl_service.h"           // ObDDLTransController
#include "share/schema/ob_latest_schema_guard.h" // ObLatestSchemaGuard

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{
class ObIDGenerator;
namespace schema
{
class ObMultiVersionSchemaService;
class ObDDLTransController;
class ObLatestSchemaGuard;
}
}
namespace rootserver
{
class ObDDLSQLTransaction;
class ObDDLService;
class ObDDLHelperUtils
{
public:
  static int gen_task_id_and_schema_versions(share::schema::ObDDLTransController *controller,
                                             const uint64_t tenant_id,
                                             const uint64_t schema_version_cnt,
                                             int64_t &task_id);
  static int write_1503_ddl_operation(share::schema::ObMultiVersionSchemaService *schema_service, 
                                      const uint64_t tenant_id,
                                      ObDDLSQLTransaction &trans);
  static int wait_ddl_trans(share::schema::ObDDLTransController *controller, 
                            const uint64_t tenant_id, 
                            const int64_t task_id);
  static int end_ddl_trans(share::schema::ObMultiVersionSchemaService *schema_service,
                           share::schema::ObDDLTransController *ddl_trans_controller,
                           const uint64_t tenant_id, 
                           const int return_ret, 
                           const int64_t task_id, 
                           ObDDLSQLTransaction &trans);
  static int wait_and_end_ddl_trans(const int return_ret,
                                    share::schema::ObMultiVersionSchemaService *schema_service,
                                    share::schema::ObDDLTransController *ddl_trans_controller, 
                                    const uint64_t tenant_id, 
                                    const int64_t task_id, 
                                    ObDDLSQLTransaction &trans,
                                    bool &need_clean_failed);
  static int check_schema_version();
};
class ObDDLHelper
{
public:
  class ObLockObjPair{
  public:
    ObLockObjPair();
    ObLockObjPair(const uint64_t obj_id,
                  transaction::tablelock::ObTableLockMode lock_mode);
    ~ObLockObjPair() {}
    int init(
        const uint64_t obj_id,
        transaction::tablelock::ObTableLockMode lock_mode);
    void reset();
    static bool less_than(const ObLockObjPair &left, const ObLockObjPair &right);
    uint64_t get_obj_id() const { return obj_id_; }
    transaction::tablelock::ObTableLockMode get_lock_mode() const { return lock_mode_; }
    TO_STRING_KV(K_(obj_id), K_(lock_mode));
  private:
    uint64_t obj_id_;
    transaction::tablelock::ObTableLockMode lock_mode_;
  };

typedef common::hash::ObHashMap<uint64_t, transaction::tablelock::ObTableLockMode> ObjectLockMap;
public:
  ObDDLHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const uint64_t tenant_id,
    const char* parallel_ddl_type,
    ObDDLSQLTransaction *external_trans = nullptr);
  virtual ~ObDDLHelper();

  int init(ObDDLService &ddl_service);

  virtual int execute();
  static int obj_lock_database_name(
             ObDDLSQLTransaction &trans,
             const uint64_t tenant_id,
             const ObString &name,
             const transaction::tablelock::ObTableLockMode lock_mode);
  static int obj_lock_obj_name(
             ObDDLSQLTransaction &trans,
             const uint64_t tenant_id,
             const ObString &database_name,
             const ObString &obj_name,
             const transaction::tablelock::ObTableLockMode lock_mode);
  static int obj_lock_obj_id(
             ObDDLSQLTransaction &trans,
             const uint64_t tenant_id,
             const uint64_t obj_id,
             const transaction::tablelock::ObTableLockMode lock_mode);

  // sort and check if dep objs are consistent
  static int check_dep_objs_consistent(
             ObArray<std::pair<uint64_t, share::schema::ObObjectType>> &l,
             ObArray<std::pair<uint64_t, share::schema::ObObjectType>> &r);
public:
  int clean_on_fail_commit()
  {
    return clean_on_fail_commit_();
  }
protected:
  virtual int check_inner_stat_();
  virtual int init_() = 0;
  /* main actions */
  int start_ddl_trans_();
  virtual int lock_objects_() = 0;
  virtual int generate_schemas_() = 0;
  virtual int calc_schema_version_cnt_() = 0;
  int gen_task_id_and_schema_versions_();
  virtual int operate_schemas_() = 0;
  int serialize_inc_schema_dict_();
  virtual int operation_before_commit_() = 0;
  virtual int clean_on_fail_commit_() = 0;
  virtual int construct_and_adjust_result_(int &return_ret) = 0;
  virtual ObDDLSQLTransaction &get_trans_();
  virtual ObDDLSQLTransaction *get_external_trans_();
  /*--------------*/
protected:
  // lock database name
  int add_lock_object_by_database_name_(
      const ObString &database_name,
      const transaction::tablelock::ObTableLockMode lock_mode);
  int lock_databases_by_name_();
  
  /*--------------------------------*/
  // lock object name
  // MySQL temporary tables introduce parallel scenarios with objects of the same name,
  // therefore session_id need to be considered when lock table by name

  // add_lock_object_by_name_ is used to lock object by name, session id is 0 by default,
  // with default session id 0 means no need to consider temp table.

  // add_lock_table_by_name_with_session_id_zero_ is used to lock table by name, must pass session id,
  // it will lock both session_id and session_id 0.

  int add_lock_object_by_name_(
      const ObString &database_name,
      const ObString &object_name,
      const share::schema::ObSchemaType schema_type,
      const transaction::tablelock::ObTableLockMode lock_mode,
      const uint64_t session_id = 0);

  int add_lock_table_by_name_with_session_id_zero_(
      const ObString &database_name,
      const ObString &object_name,
      const transaction::tablelock::ObTableLockMode lock_mode,
      const uint64_t session_id);
  /*--------------------------------*/
  int lock_existed_objects_by_name_();
  // lock object id
  int add_lock_object_by_id_(
      const uint64_t lock_obj_id,
      const share::schema::ObSchemaType schema_type,
      const transaction::tablelock::ObTableLockMode lock_mode);
  int lock_existed_objects_by_id_();

  int gen_object_ids_(
      const int64_t object_cnt,
      share::ObIDGenerator &id_generator);
  int gen_partition_object_and_tablet_ids_(
      ObIArray<ObTableSchema> &table_schemas);

  int check_constraint_name_exist_(
      const share::schema::ObTableSchema &table_schema,
      const common::ObString &constraint_name,
      const bool is_foreign_key,
      bool &exist);
  int check_database_legitimacy_(const ObString &database_name, uint64_t &database_id);

  int check_parallel_ddl_conflict_(const common::ObIArray<share::schema::ObBasedSchemaObjectInfo> &based_schema_object_infos);
  int add_lock_table_udt_id_(const ObTableSchema &table_schema);
  int check_table_udt_exist_(const ObTableSchema &table_schema);
  // lock tablegroup name
  int add_lock_object_by_tablegroup_name_(
      const ObString &tablegroup_name,
      const transaction::tablelock::ObTableLockMode lock_mode);
private:
  int add_lock_object_to_map_(
      const uint64_t lock_obj_id,
      const transaction::tablelock::ObTableLockMode lock_mode,
      ObjectLockMap &lock_map);
  int lock_objects_in_map_(
      const transaction::tablelock::ObLockOBJType obj_type,
      ObjectLockMap &lock_map);
  static uint64_t cast_database_name_to_id_(const ObString &database_name);
  static uint64_t cast_obj_name_to_id_(const ObString &database_name, const ObString &obj_name);
  static int obj_lock_with_lock_id_(
             ObDDLSQLTransaction &trans,
             const uint64_t tenant_id,
             const uint64_t obj_id,
             const transaction::tablelock::ObTableLockMode lock_mode,
             const ObLockOBJType obj_type);
  static bool dep_compare_func_(const std::pair<uint64_t, share::schema::ObObjectType> &a,
                                const std::pair<uint64_t, share::schema::ObObjectType> &b)
  { return a.first > b.first || (a.first == b.first && a.second > b.second); }
  int check_schema_version_()
  {
    return ObDDLHelperUtils::check_schema_version();
  }
protected:
  bool inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObDDLService *ddl_service_;
  common::ObMySQLProxy *sql_proxy_;
  share::schema::ObDDLTransController *ddl_trans_controller_;

  uint64_t tenant_id_;          // normally, ObDDLHelper only deal with ddl in one tenant
  int64_t task_id_;             // allocated by ObDDLTransController
  int64_t schema_version_cnt_;  // used to allocate schema versions for this DDL
  int64_t object_id_cnt_;       // used to allocate object ids for this DDL
  ObDDLSQLTransaction *external_trans_;
  // used to lock databases by name
  ObjectLockMap lock_database_name_map_;
  // used to lock objects by name
  ObjectLockMap lock_object_name_map_;
  // used to lock objects by id
  ObjectLockMap lock_object_id_map_;
  // should use this guard after related objects are locked
  share::schema::ObLatestSchemaGuard latest_schema_guard_;
  common::ObArenaAllocator allocator_;
  const char* parallel_ddl_type_;
private:
  ObDDLSQLTransaction trans_;
private:
  static const int64_t OBJECT_BUCKET_NUM = 1024;
  DISALLOW_COPY_AND_ASSIGN(ObDDLHelper);
};

} // end namespace rootserver
} // end namespace oceanbase


#endif//OCEANBASE_ROOTSERVER_OB_DDL_HELPER_H_
