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

#define USING_LOG_PREFIX RS
#include "observer/ob_inner_sql_connection.h"  //ObInnerSQLConnection
#include "rootserver/parallel_ddl/ob_ddl_helper.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_ddl_sql_service.h"
#include "share/ob_max_id_fetcher.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h" //ObLockObjRequest
#include "storage/tablelock/ob_lock_inner_connection_util.h" //ObInnerConnectionLockUtil

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;

ObDDLHelper::ObLockObjPair::ObLockObjPair()
  : obj_id_(0),
    lock_mode_(transaction::tablelock::MAX_LOCK_MODE)
{
}

ObDDLHelper::ObLockObjPair::ObLockObjPair(
  const uint64_t obj_id,
  transaction::tablelock::ObTableLockMode lock_mode)
  : obj_id_(obj_id),
    lock_mode_(lock_mode)
{
}

int ObDDLHelper::ObLockObjPair::init(
  const uint64_t obj_id,
  transaction::tablelock::ObTableLockMode lock_mode)
{
  int ret = OB_SUCCESS;
  reset();
  obj_id_ = obj_id;
  lock_mode_ = lock_mode;
  return ret;
}

void ObDDLHelper::ObLockObjPair::reset()
{
  obj_id_ = 0;
  lock_mode_ = transaction::tablelock::MAX_LOCK_MODE;
}

bool ObDDLHelper::ObLockObjPair::less_than(
     const ObLockObjPair &left,
     const ObLockObjPair &right)
{
  bool bret = false;
  if (left.get_obj_id() != right.get_obj_id()) {
    bret = (left.get_obj_id() < right.get_obj_id());
  } else {
    bret = (left.get_lock_mode() < right.get_lock_mode());
  }
  return bret;
}

ObDDLHelper::ObDDLHelper(
  share::schema::ObMultiVersionSchemaService *schema_service,
  const uint64_t tenant_id)
  : inited_(false),
    schema_service_(schema_service),
    ddl_service_(NULL),
    sql_proxy_(NULL),
    ddl_trans_controller_(NULL),
    tenant_id_(tenant_id),
    task_id_(common::OB_INVALID_ID),
    schema_version_cnt_(0),
    object_id_cnt_(0),
    trans_(schema_service_,
           false, /*need_end_signal*/
           false, /*enable_query_stash*/
           true   /*enable_ddl_parallel*/),
    lock_database_name_map_(),
    lock_object_name_map_(),
    lock_object_id_map_(),
    latest_schema_guard_(schema_service, tenant_id)
{}

ObDDLHelper::~ObDDLHelper()
{
}

int ObDDLHelper::init(rootserver::ObDDLService &ddl_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ddl_helper already inited", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(lock_database_name_map_.create(
             OBJECT_BUCKET_NUM, "LockDBNameMap", "LockDBNameMap"))) {
    LOG_WARN("fail to create lock database name map", KR(ret));
  } else if (OB_FAIL(lock_object_name_map_.create(
             OBJECT_BUCKET_NUM, "LockObjNameMap", "LockObjNameMap"))) {
    LOG_WARN("fail to create lock object name map", KR(ret));
  } else if (OB_FAIL(lock_object_id_map_.create(
             OBJECT_BUCKET_NUM, "LockObjIDMap", "LockObjIDMap"))) {
    LOG_WARN("fail to create lock object id map", KR(ret));
  } else {
    ddl_service_ = &ddl_service;
    sql_proxy_ = &(ddl_service.get_sql_proxy());
    ddl_trans_controller_ = &(schema_service_->get_ddl_trans_controller());
    task_id_ = OB_INVALID_ID;
    schema_version_cnt_ = 0;
    object_id_cnt_ = 0;
    inited_ = true;
  }
  return ret;
}

int ObDDLHelper::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl_helper not init yet", KR(ret));
  } else if (OB_ISNULL(ddl_service_)
             || OB_ISNULL(sql_proxy_)
             || OB_ISNULL(schema_service_)
             || OB_ISNULL(ddl_trans_controller_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ptr is null", KR(ret), KP_(ddl_service), KP_(schema_service),
             KP_(sql_proxy), K_(ddl_trans_controller));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid tenant_id", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObDDLHelper::start_ddl_trans_()
{
  int ret = OB_SUCCESS;
  bool with_snapshot = false;
  int64_t fake_schema_version = 1000;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(trans_.start(sql_proxy_, tenant_id_, fake_schema_version, with_snapshot))) {
    LOG_WARN("fail to start trans", KR(ret), K_(tenant_id), K(fake_schema_version), K(with_snapshot));
  }
  RS_TRACE(start_ddl_trans);
  return ret;
}

int ObDDLHelper::gen_task_id_and_schema_versions_()
{
  int ret = OB_SUCCESS;
  // just for interface compatibility, schema version can be fetched from TSISchemaVersionGenerator
  ObArray<int64_t> schema_versions;
  int64_t version_cnt = OB_INVALID_INDEX;
  auto *tsi_generator = GET_TSI(TSISchemaVersionGenerator);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ddl_trans_controller_->create_task_and_assign_schema_version(
             tenant_id_, schema_version_cnt_, task_id_, schema_versions))) {
    LOG_WARN("fail to gen task id and schema_versions", KR(ret), K_(tenant_id), K_(schema_version_cnt));
  } else if (OB_UNLIKELY(OB_INVALID_ID == task_id_
             || schema_version_cnt_ != schema_versions.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task_id or schema version cnt not match", KR(ret), K_(tenant_id), K_(task_id),
             K_(schema_version_cnt), "schema_versions_cnt", schema_versions.count());
  } else if (OB_ISNULL(tsi_generator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tsi schema version generator is null", KR(ret));
  } else if (OB_FAIL(tsi_generator->get_version_cnt(version_cnt))) {
    LOG_WARN("fail to get id cnt", KR(ret));
  } else if (OB_UNLIKELY(schema_version_cnt_ != version_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema version cnt not match", KR(ret), K_(tenant_id), K_(task_id),
             K_(schema_version_cnt), K(version_cnt));
  }
  RS_TRACE(gen_task_id_and_versions);
  return ret;
}

int ObDDLHelper::serialize_inc_schema_dict_()
{
  int ret = OB_SUCCESS;
  auto *tsi_generator = GET_TSI(TSISchemaVersionGenerator);
  int64_t start_schema_version = OB_INVALID_VERSION;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_ISNULL(tsi_generator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tsi schema version generator is null", KR(ret));
  } else if (OB_FAIL(tsi_generator->get_start_version(start_schema_version))) {
    LOG_WARN("fail to get start schema version", KR(ret));
  } else if (OB_FAIL(trans_.serialize_inc_schemas(start_schema_version  - 1))) {
    LOG_WARN("fail to serialize inc schemas", KR(ret), K_(tenant_id),
             "start_schema_version", start_schema_version - 1);
  }
  RS_TRACE(inc_schema_dict);
  return ret;
}

int ObDDLHelper::wait_ddl_trans_()
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  const int64_t DEFAULT_TS = 10 * 1000 * 1000L; // 10s
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, DEFAULT_TS))) {
    LOG_WARN("fail to set default ts", KR(ret));
  } else if (OB_FAIL(ddl_trans_controller_->wait_task_ready(tenant_id_, task_id_, ctx.get_timeout()))) {
    LOG_WARN("fail to wait ddl trans", KR(ret), K_(tenant_id), K_(task_id));
  }
  RS_TRACE(wait_ddl_trans);
  return ret;
}

// this function should be always called
int ObDDLHelper::end_ddl_trans_(const int return_ret)
{
  int ret = return_ret;

  // write 1503 ddl operation
  if (OB_SUCC(ret)) {
    auto *tsi_generator = GET_TSI(TSISchemaVersionGenerator);
    int64_t version_cnt = OB_INVALID_INDEX;
    int64_t boundary_schema_version = OB_INVALID_VERSION;
    share::schema::ObSchemaService *schema_service_impl = NULL;
    if (OB_ISNULL(tsi_generator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tsi schema version generator is null", KR(ret));
    } else if (OB_FAIL(tsi_generator->get_version_cnt(version_cnt))) {
      LOG_WARN("fail to get version cnt", KR(ret), K(version_cnt));
    } else if (0 == version_cnt) {
      // no schema change, just skip
    } else if (OB_UNLIKELY(version_cnt < 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not enough version cnt for boudary ddl operation", KR(ret), K(version_cnt));
    } else if (OB_ISNULL(schema_service_)
               || OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ptr is null", KR(ret), KP_(schema_service));
    } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, boundary_schema_version))) {
      LOG_WARN("fail to gen new schema version", KR(ret), K_(tenant_id));
    } else {
      share::schema::ObDDLSqlService ddl_sql_service(*schema_service_impl);
      obrpc::ObDDLNopOpreatorArg arg;
      arg.schema_operation_.op_type_ = OB_DDL_END_SIGN;
      arg.schema_operation_.tenant_id_ = tenant_id_;
      if (OB_FAIL(ddl_sql_service.log_nop_operation(arg.schema_operation_,
                                                    boundary_schema_version,
                                                    NULL,
                                                    trans_))) {
        LOG_WARN("fail to log ddl operation", KR(ret), K(arg));
      }
    }
  }

  if (trans_.is_started()) {
    int tmp_ret = OB_SUCCESS;
    bool is_commit = OB_SUCC(ret);
    if (OB_TMP_FAIL(trans_.end(is_commit))) {
      LOG_WARN("trans end failed", KR(ret), KR(tmp_ret), K(is_commit));
      ret = is_commit ? tmp_ret : ret;
    }
  }
  if (OB_NOT_NULL(ddl_trans_controller_) && OB_INVALID_ID != task_id_) {
    ddl_trans_controller_->remove_task(tenant_id_, task_id_);
  }
  RS_TRACE(end_ddl_trans);
  return ret;
}

int ObDDLHelper::execute()
{
  return OB_NOT_IMPLEMENT;
  /*
   * Implement of parallel ddl should has following actions:
   *
   * ----------------------------------------------
   * 1. start ddl trans:
   * - to be exclusive with non-parallel ddl.
   * - to be concurrent with other parallel ddl.
   *
   * if (OB_FAIL(start_ddl_trans_())) {
   *   LOG_WARN("fail to start ddl trans", KR(ret));
   * }
   *
   * ----------------------------------------------
   * 2. lock object by name/object_id
   * - to be exclusive with other parallel ddl which involving the same objects.
   * - lock object in trans
   * Attension:
   * 1) lock objects just for mutual exclusion, should check if related objects changed after acquire locks.
   * 2) For same object, lock object by name first. After that, lock object by id if it's neccessary.
   *
   * ----------------------------------------------
   * 3. fetch & generate schema:
   * - fetch the latest schemas from inner table.
   * - generate schema with arg and the latests schemas.
   *
   * ----------------------------------------------
   * 4. register task id & generate schema versions:
   * - generate an appropriate number of schema versions for this DDL and register task id.
   * - concurrent DDL trans will be committed in descending order of version later.
   *
   * if (FAILEDx(gen_task_id_and_schema_versions_())) {
   *   LOG_WARN("fail to gen task id and schema versions", KR(ret));
   * }
   *
   * ----------------------------------------------
   * 5. create schema:
   * - persist schema in inner table.
   *
   * ----------------------------------------------
   * 6. [optional] serialize increment data dictionary:
   * - if table/database/tenant schema changed, records changed schemas in log and commits with DDL trans.
   *
   * if (FAILEDx(serialize_inc_schema_dict_())) {
   *   LOG_WARN("fail to serialize inc schema dict", KR(ret));
   * }
   *
   * ----------------------------------------------
   * 7. wait concurrent ddl trans ended:
   * - wait concurrent DDL trans with smallest schema version ended.
   *
   * if (FAILEDx(wait_ddl_trans_())) {
   *   LOG_WARN(fail to wait ddl trans, KR(ret));
   * }
   *
   * ----------------------------------------------
   * 8. end ddl trans:
   * - abort/commit ddl trans.
   *
   * if (OB_FAIL(end_ddl_trans_(ret))) { // won't overwrite ret
   *   LOG_WARN("fail to end ddl trans", KR(ret));
   * }
   */
}

int ObDDLHelper::add_lock_object_to_map_(
    const uint64_t lock_obj_id,
    const transaction::tablelock::ObTableLockMode lock_mode,
    ObjectLockMap &lock_map)
{

  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(transaction::tablelock::SHARE != lock_mode
             && transaction::tablelock::EXCLUSIVE != lock_mode)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support lock mode to lock object by name", KR(ret), K(lock_mode));
  } else {
    bool need_update = false;
    transaction::tablelock::ObTableLockMode existed_lock_mode = transaction::tablelock::MAX_LOCK_MODE;
    if (OB_FAIL(lock_map.get_refactored(lock_obj_id, existed_lock_mode))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        need_update = true;
      } else {
        LOG_WARN("fail to get lock object from map", KR(ret), K(lock_obj_id));
      }
    } else if (transaction::tablelock::SHARE == existed_lock_mode
               && transaction::tablelock::EXCLUSIVE == lock_mode) {
      // upgrade lock
      need_update = true;
    }

    if (OB_SUCC(ret) && need_update) {
      int overwrite = 1;
      if (OB_FAIL(lock_map.set_refactored(lock_obj_id, lock_mode, overwrite))) {
        LOG_WARN("fail to set lock object to map", KR(ret), K(lock_obj_id), K(lock_mode));
      }
    }
  }
  return ret;
}

int ObDDLHelper::lock_objects_in_map_(
    const transaction::tablelock::ObLockOBJType obj_type,
    ObjectLockMap &lock_map)
{
  int ret = OB_SUCCESS;
  ObArray<ObLockObjPair> lock_pairs;
  const int64_t lock_cnt = lock_map.size();
  ObTimeoutCtx ctx;
  observer::ObInnerSQLConnection *conn = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(lock_cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected lock cnt", KR(ret), K(lock_cnt));
  } else if (0 == lock_cnt) {
    // skip
  } else if (OB_FAIL(lock_pairs.reserve(lock_cnt))) {
    LOG_WARN("fail to reserve lock pairs", KR(ret), K(lock_cnt));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret));
  } else if (OB_ISNULL(conn = dynamic_cast<observer::ObInnerSQLConnection *>
                       (trans_.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans conn is NULL", KR(ret));
  } else {
    ObLockObjPair pair;
    FOREACH_X(it, lock_map, OB_SUCC(ret)) {
      if (OB_FAIL(pair.init(it->first, it->second))) {
        LOG_WARN("fail to init lock pair", KR(ret),
                 "obj_id", it->first, "lock_mode", it->second);
      } else if (OB_FAIL(lock_pairs.push_back(pair))) {
        LOG_WARN("fail to push back lock pair", KR(ret), K(pair));
      }
    } // end foreach
    if (OB_SUCC(ret)) {
      std::sort(lock_pairs.begin(), lock_pairs.end(), ObLockObjPair::less_than);
      FOREACH_X(it, lock_pairs, OB_SUCC(ret)) {
        const int64_t timeout = ctx.get_timeout();
        if (OB_UNLIKELY(timeout <= 0)) {
          ret = OB_TIMEOUT;
          LOG_WARN("already timeout", KR(ret), K(timeout));
        } else {
          transaction::tablelock::ObLockObjRequest lock_arg;
          lock_arg.obj_type_ = obj_type;
          lock_arg.owner_id_ = ObTableLockOwnerID(0);
          lock_arg.obj_id_ = it->get_obj_id();
          lock_arg.lock_mode_ = it->get_lock_mode();
          lock_arg.op_type_ = ObTableLockOpType::IN_TRANS_COMMON_LOCK;
          lock_arg.timeout_us_ = timeout;
          LOG_INFO("try lock object", KR(ret), K(lock_arg));
          if (OB_FAIL(ObInnerConnectionLockUtil::lock_obj(tenant_id_, lock_arg, conn))) {
            LOG_WARN("lock obj failed", KR(ret), K_(tenant_id), K(lock_arg));
          }
        }
      } // end foreach
    }
  }
  (void) lock_map.clear();
  return ret;
}

int ObDDLHelper::add_lock_object_by_database_name_(
    const ObString &database_name,
    const transaction::tablelock::ObTableLockMode lock_mode)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(database_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_name is invalid", KR(ret), K(database_name));
  } else {
    // use OB_ORIGIN_AND_INSENSITIVE and ignore end space to make more conficts for safety.
    common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(OB_ORIGIN_AND_INSENSITIVE);
    bool calc_end_space = false;
    uint64_t lock_obj_id = 0;
    lock_obj_id = common::ObCharset::hash(
                  cs_type, database_name.ptr(), database_name.length(),
                  lock_obj_id, calc_end_space, NULL);
    if (OB_FAIL(add_lock_object_to_map_(lock_obj_id, lock_mode, lock_database_name_map_))) {
      LOG_WARN("fail to add lock object to map", KR(ret), K(lock_obj_id), K(lock_mode));
    }
    LOG_INFO("add lock object by database name", KR(ret), K(database_name), K(lock_mode), K(lock_obj_id));
  }
  return ret;
}

int ObDDLHelper::lock_databases_by_name_()
{
  return lock_objects_in_map_(ObLockOBJType::OBJ_TYPE_DATABASE_NAME, lock_database_name_map_);
}

int ObDDLHelper::add_lock_object_by_name_(
    const ObString &database_name,
    const ObString &object_name,
    const share::schema::ObSchemaType schema_type,
    const transaction::tablelock::ObTableLockMode lock_mode)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(database_name.empty() || object_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("database_name/object_name is invalid", KR(ret), K(database_name), K(object_name));
  } else {
    // 1. use OB_ORIGIN_AND_INSENSITIVE and ignore end space to make more conficts for safety.
    // 2. encoded with database name to make less conficts between different databases/users.
    common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(OB_ORIGIN_AND_INSENSITIVE);
    bool calc_end_space = false;
    uint64_t lock_obj_id = 0;
    lock_obj_id = common::ObCharset::hash(
                  cs_type, database_name.ptr(), database_name.length(),
                  lock_obj_id, calc_end_space, NULL);
    lock_obj_id = common::ObCharset::hash(
                  cs_type, object_name.ptr(), object_name.length(),
                  lock_obj_id, calc_end_space, NULL);
    if (OB_FAIL(add_lock_object_to_map_(lock_obj_id, lock_mode, lock_object_name_map_))) {
      LOG_WARN("fail to add lock object to map", KR(ret), K(lock_obj_id), K(lock_mode));
    }
    LOG_INFO("add lock object by name", KR(ret), K(database_name),
             K(object_name), K(schema_type), K(lock_mode), K(lock_obj_id));
  }
  return ret;
}

int ObDDLHelper::lock_existed_objects_by_name_()
{
  return lock_objects_in_map_(ObLockOBJType::OBJ_TYPE_OBJECT_NAME, lock_object_name_map_);
}

int ObDDLHelper::add_lock_object_by_id_(
    const uint64_t lock_obj_id,
    const share::schema::ObSchemaType schema_type,
    const transaction::tablelock::ObTableLockMode lock_mode)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == lock_obj_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("object_id is invalid", KR(ret), K(lock_obj_id));
  } else if (OB_FAIL(add_lock_object_to_map_(lock_obj_id, lock_mode, lock_object_id_map_))) {
    LOG_WARN("fail to add lock object to map", KR(ret), K(lock_obj_id), K(lock_mode));
  }
  LOG_INFO("add lock object by id", KR(ret), K(lock_obj_id), K(schema_type), K(lock_mode));
  return ret;
}

int ObDDLHelper::lock_existed_objects_by_id_()
{
  return lock_objects_in_map_(ObLockOBJType::OBJ_TYPE_COMMON_OBJ, lock_object_id_map_);
}

// 1. constraint name and foreign key name are in the same namespace in oracle tenant.
// 2. constraint name and foreign key name are in different namespace in mysql tenant.
int ObDDLHelper::check_constraint_name_exist_(
    const share::schema::ObTableSchema &table_schema,
    const common::ObString &constraint_name,
    const bool is_foreign_key,
    bool &exist)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  uint64_t constraint_id = OB_INVALID_ID;
  const uint64_t database_id = table_schema.get_database_id();
  exist = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("check if oracle compat mode failed", KR(ret), K_(tenant_id));
  } else {
    const bool check_fk = (is_oracle_mode || is_foreign_key);
    if (OB_SUCC(ret) && check_fk) {
      if (OB_FAIL(latest_schema_guard_.get_foreign_key_id(
          database_id, constraint_name, constraint_id))) {
        LOG_WARN("fail to get foreign key id", KR(ret), K_(tenant_id), K(database_id), K(constraint_name));
      } else if (OB_INVALID_ID != constraint_id) {
        exist = true;
      }
    }
    const bool check_cst = (is_oracle_mode || !is_foreign_key);
    if (OB_SUCC(ret) && !exist && check_cst) {
      if (table_schema.is_mysql_tmp_table()) {
        // tmp table in mysql mode, do nothing
      } else if (OB_FAIL(latest_schema_guard_.get_constraint_id(
          database_id, constraint_name, constraint_id))) {
        LOG_WARN("fail to get constraint id", KR(ret), K_(tenant_id), K(database_id), K(constraint_name));
      } else if (OB_INVALID_ID != constraint_id) {
        exist = true;
      }
    }
  }
  return ret;
}

int ObDDLHelper::gen_object_ids_(
    const int64_t object_cnt,
    share::ObIDGenerator &id_generator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_
             || object_cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id or object_cnt", KR(ret), K_(tenant_id), K(object_cnt));
  } else if (0 == object_cnt) {
    // skip
  } else {
    uint64_t max_object_id = OB_INVALID_ID;
    uint64_t min_object_id = OB_INVALID_ID;
    share::schema::ObSchemaService *schema_service_impl = NULL;
    if (OB_ISNULL(schema_service_)
        || OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ptr is null", KR(ret), KP_(schema_service));
    } else if (OB_FAIL(schema_service_impl->fetch_new_object_ids(tenant_id_, object_cnt, max_object_id))) {
      LOG_WARN("fail to fetch new object ids", KR(ret), K_(tenant_id), K(object_cnt));
    } else if (OB_UNLIKELY(OB_INVALID_ID == max_object_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("object_id is invalid", KR(ret), K_(tenant_id), K(object_cnt));
    } else if (0 >= (min_object_id = max_object_id - object_cnt + 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("min_object_id should be greator than 0",
               KR(ret), K_(tenant_id), K(min_object_id), K(max_object_id), K(object_cnt));
    } else if (OB_FAIL(id_generator.init(1 /*step*/, min_object_id, max_object_id))) {
      LOG_WARN("fail to init id generator", KR(ret), K_(tenant_id),
               K(min_object_id), K(max_object_id), K(object_cnt));
    }
  }
  return ret;
}

int ObDDLHelper::gen_partition_object_and_tablet_ids_(
    ObIArray<ObTableSchema> &table_schemas)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ddl_service_->generate_object_id_for_partition_schemas(table_schemas))) {
    LOG_WARN("fail to generate object_ids", KR(ret));
  } else if (OB_FAIL(ddl_service_->generate_tables_tablet_id(table_schemas))) {
    LOG_WARN("fail to generate tablet_ids", KR(ret));
  }
  return ret;
}
