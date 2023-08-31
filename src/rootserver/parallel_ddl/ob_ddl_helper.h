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
    const uint64_t tenant_id);
  virtual ~ObDDLHelper();

  int init(rootserver::ObDDLService &ddl_service);

  virtual int execute();
protected:
  virtual int check_inner_stat_();

  /* main actions */
  int start_ddl_trans_();
  int gen_task_id_and_schema_versions_();
  int serialize_inc_schema_dict_();
  int wait_ddl_trans_();
  int end_ddl_trans_(const int return_ret);
  /*--------------*/
protected:
  // lock database name
  int add_lock_object_by_database_name_(
      const ObString &database_name,
      const transaction::tablelock::ObTableLockMode lock_mode);
  int lock_databases_by_name_();
  // lock object name
  int add_lock_object_by_name_(
      const ObString &database_name,
      const ObString &object_name,
      const share::schema::ObSchemaType schema_type,
      const transaction::tablelock::ObTableLockMode lock_mode);
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
private:
  int add_lock_object_to_map_(
      const uint64_t lock_obj_id,
      const transaction::tablelock::ObTableLockMode lock_mode,
      ObjectLockMap &lock_map);
  int lock_objects_in_map_(
      const transaction::tablelock::ObLockOBJType obj_type,
      ObjectLockMap &lock_map);
protected:
  bool inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  rootserver::ObDDLService *ddl_service_;
  common::ObMySQLProxy *sql_proxy_;
  share::schema::ObDDLTransController *ddl_trans_controller_;

  uint64_t tenant_id_;          // normally, ObDDLHelper only deal with ddl in one tenant
  int64_t task_id_;             // allocated by ObDDLTransController
  int64_t schema_version_cnt_;  // used to allocate schema versions for this DDL
  int64_t object_id_cnt_;       // used to allocate object ids for this DDL
  rootserver::ObDDLSQLTransaction trans_;
  // used to lock databases by name
  ObjectLockMap lock_database_name_map_;
  // used to lock objects by name
  ObjectLockMap lock_object_name_map_;
  // used to lock objects by id
  ObjectLockMap lock_object_id_map_;
  // should use this guard after related objects are locked
  share::schema::ObLatestSchemaGuard latest_schema_guard_;
  common::ObArenaAllocator allocator_;
private:
  static const int64_t OBJECT_BUCKET_NUM = 1024;
  DISALLOW_COPY_AND_ASSIGN(ObDDLHelper);
};

} // end namespace rootserver
} // end namespace oceanbase


#endif//OCEANBASE_ROOTSERVER_OB_DDL_HELPER_H_
