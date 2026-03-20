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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_JAVA_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_JAVA_H_


#include "sql/ob_sql_define.h"
#include "share/schema/ob_java_policy_mgr.h"
#include "jni.h"


namespace oceanbase
{

namespace common
{

class ObObj;

} // namespace common

namespace share
{

namespace schema
{

class ObSchemaGetterGuard;

} // namespace schema
} // namespace share

namespace pl
{

class ObPLExecCtx;

class ObDBMSJava
{
public:
  static int loadjava_mysql(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int dropjava_mysql(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);

#ifdef OB_BUILD_ORACLE_PL
  static int ob_loadjar(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int ob_dropjar(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
#endif // OB_BUILD_ORACLE_PL

  static int grant_permission(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int grant_permission_with_key(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int restrict_permission(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int restrict_permission_with_key(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int revoke_permission(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int disable_permission(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int enable_permission(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);
  static int delete_permission(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);

private:
#ifdef OB_BUILD_ORACLE_PL
  static int prepare_ora_jar_info(ObPLExecCtx &ctx,
                                  const ObString &jar_binary,
                                  uint64_t &tenant_id,
                                  uint64_t &database_id,
                                  share::schema::ObSchemaGetterGuard *&schema_guard,
                                  ObSqlString &jar_hash,
                                  ObIArray<std::pair<ObString, ObString>> &classes,
                                  JNIEnv *&env,
                                  jobject &buffer_handle);
#endif // OB_BUILD_ORACLE_PL

  static int check_java_policy_supported(ObPLExecCtx &ctx, uint64_t &tenant_id);

  static int get_java_policy_key(ObObjParam &param, uint64_t &key);

  static int create_permission_impl(ObPLExecCtx &ctx,
                                    const uint64_t tenant_id,
                                    const bool is_grant,
                                    const ObString &grantee_name,
                                    const ObString &permission_type,
                                    const ObString &name,
                                    const ObString &action,
                                    uint64_t &key);

  static int modify_permission_impl(ObPLExecCtx &ctx,
                                    const uint64_t tenant_id,
                                    const share::schema::ObSimpleJavaPolicySchema::JavaPolicyStatus status,
                                    const uint64_t key);

  static int check_permission_whitelist(const ObString &permission_type,
                                        const ObString &permission_name,
                                        const ObString &permission_action,
                                        bool &is_whitelisted);

};

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_JAVA_H_
