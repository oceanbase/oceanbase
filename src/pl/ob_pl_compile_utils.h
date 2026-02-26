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

#ifndef OCEANBASE_SRC_PL_OB_PL_COMPILE_UTILS_H_
#define OCEANBASE_SRC_PL_OB_PL_COMPILE_UTILS_H_

#include "share/schema/ob_routine_info.h"
#include "share/schema/ob_package_info.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObRoutineInfo;
class ObPackageInfo;
}
}
namespace pl
{

class ObPLCompilerUtils
{
public:

enum CompileType {
  COMPILE_INVALID = -1,
  COMPILE_PROCEDURE,
  COMPILE_FUNCTION,
  COMPILE_PACKAGE_SPEC,
  COMPILE_PACKAGE_BODY,
  COMPILE_TRIGGER,
#ifdef OB_BUILD_ORACLE_PL
  COMPILE_UDT
#endif
};

  static inline CompileType get_compile_type(share::schema::ObRoutineType routine_type) {
    CompileType type = COMPILE_INVALID;
    if (share::schema::ROUTINE_PROCEDURE_TYPE == routine_type) {
      type = COMPILE_PROCEDURE;
    } else if (share::schema::ROUTINE_FUNCTION_TYPE == routine_type) {
      type = COMPILE_FUNCTION;
    }
    return type;
  }

  static inline CompileType get_compile_type(share::schema::ObPackageType package_type) {
    CompileType type = COMPILE_INVALID;
    if (share::schema::PACKAGE_TYPE == package_type) {
      type = COMPILE_PACKAGE_SPEC;
    } else if (share::schema::PACKAGE_BODY_TYPE == package_type) {
      type = COMPILE_PACKAGE_BODY;
    }
    return type;
  }

  static inline CompileType get_compile_type(ObString &object_type) {
    CompileType type = COMPILE_INVALID;
    if (object_type.compare_equal("PROCEDURE")) {
      type = COMPILE_PROCEDURE;
    } else if (object_type.compare_equal("FUNCTION")) {
      type = COMPILE_FUNCTION;
    } else if (object_type.compare_equal("TRIGGER")) {
      type = COMPILE_TRIGGER;
    } else if (object_type.compare_equal("PACKAGE")) {
      type = COMPILE_PACKAGE_SPEC;
    } else if (object_type.compare_equal("PACKAGE BODY")) {
      type = COMPILE_PACKAGE_BODY;
    }
#ifdef OB_BUILD_ORACLE_PL
    else if (object_type.compare_equal("TYPE")) {
      type = COMPILE_UDT;
    }
#endif
    return type;
  }

  static int compile(sql::ObExecContext &ctx,
                     uint64_t tenant_id,
                     uint64_t database_id,
                     const ObString &object_name,
                     CompileType object_type,
                     int64_t schema_version = OB_INVALID_VERSION,
                     bool is_recompile = false);

  static int compile(sql::ObExecContext &ctx,
                     uint64_t tenant_id,
                     const ObString &database_name,
                     const ObString &object_name,
                     CompileType object_type,
                     int64_t schema_version = OB_INVALID_VERSION,
                     bool is_recompile = false);

private:
  static int compile_routine(sql::ObExecContext &ctx,
                             uint64_t tenant_id,
                             uint64_t database_id,
                             const ObString &routine_name,
                             share::schema::ObRoutineType routine_type,
                             int64_t schema_version,
                             bool is_recompile);
  static int compile_package(sql::ObExecContext &ctx,
                             uint64_t tenant_id,
                             uint64_t database_id,
                             const ObString &package_name,
                             share::schema::ObPackageType package_type,
                             int64_t schema_version,
                             bool is_recompile);
  static int compile_trigger(sql::ObExecContext &ctx,
                             uint64_t tenant_id,
                             uint64_t database_id,
                             const ObString &trigger_name,
                             int64_t schema_version,
                             bool is_recompile);
#ifdef OB_BUILD_ORACLE_PL
  static int compile_udt(sql::ObExecContext &ctx,
                         uint64_t tenant_id,
                         uint64_t database_id,
                         const ObString &udt_name,
                         int64_t schema_version,
                         bool is_recompile);
#endif
};

}
}

#endif /* OCEANBASE_SRC_PL_OB_PL_COMPILE_UTILS_H_ */
