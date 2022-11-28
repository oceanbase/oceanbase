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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_ANYDATA_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_ANYDATA_H_

#include "sql/engine/ob_exec_context.h"
#include "pl/ob_pl_user_type.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSAnyData
{
public:
  ObDBMSAnyData() {}
  virtual ~ObDBMSAnyData() {}

public:

#define DECLARE_FUNC(func) \
  static int func( \
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

#define DEFINE_FUNC(inner_func, func, typecode) \
  static int func( \
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result) \
  { \
    return inner_func(ctx, typecode, params, result); \
  }

#define DECLARE_FUNC_WITH_TYPE(func)                                                      \
  DEFINE_FUNC(func, func##number, ObPLAnyType::TypeCode::TYPECODE_NUMBER)                 \
  DEFINE_FUNC(func, func##date, ObPLAnyType::TypeCode::TYPECODE_DATE)                     \
  DEFINE_FUNC(func, func##char, ObPLAnyType::TypeCode::TYPECODE_CHAR)                     \
  DEFINE_FUNC(func, func##varchar, ObPLAnyType::TypeCode::TYPECODE_VARCHAR)               \
  DEFINE_FUNC(func, func##varchar2, ObPLAnyType::TypeCode::TYPECODE_VARCHAR2)             \
  DEFINE_FUNC(func, func##raw, ObPLAnyType::TypeCode::TYPECODE_RAW)                       \
  DEFINE_FUNC(func, func##blob, ObPLAnyType::TypeCode::TYPECODE_BLOB)                     \
  DEFINE_FUNC(func, func##clob, ObPLAnyType::TypeCode::TYPECODE_CLOB)                     \
  DEFINE_FUNC(func, func##bfile, ObPLAnyType::TypeCode::TYPECODE_BFILE)                   \
  DEFINE_FUNC(func, func##object, ObPLAnyType::TypeCode::TYPECODE_OBJECT)                 \
  DEFINE_FUNC(func, func##collection, ObPLAnyType::TypeCode::TYPECODE_TABLE)              \
  DEFINE_FUNC(func, func##timestamp, ObPLAnyType::TypeCode::TYPECODE_TIMESTAMP)           \
  DEFINE_FUNC(func, func##timestamptz, ObPLAnyType::TypeCode::TYPECODE_TIMESTAMP_TZ)      \
  DEFINE_FUNC(func, func##timestampltz, ObPLAnyType::TypeCode::TYPECODE_TIMESTAMP_LTZ)    \
  DEFINE_FUNC(func, func##intervalym, ObPLAnyType::TypeCode::TYPECODE_INTERVAL_YM)        \
  DEFINE_FUNC(func, func##intervalds, ObPLAnyType::TypeCode::TYPECODE_INTERVAL_DS)        \
  DEFINE_FUNC(func, func##nchar, ObPLAnyType::TypeCode::TYPECODE_NCHAR)                   \
  DEFINE_FUNC(func, func##nvarchar2, ObPLAnyType::TypeCode::TYPECODE_NVARCHAR2)           \
  DEFINE_FUNC(func, func##nclob, ObPLAnyType::TypeCode::TYPECODE_NCLOB)                   \
  DEFINE_FUNC(func, func##bfloat, ObPLAnyType::TypeCode::TYPECODE_BFLOAT)                 \
  DEFINE_FUNC(func, func##bdouble, ObPLAnyType::TypeCode::TYPECODE_BDOUBLE)               \
  DEFINE_FUNC(func, func##urowid, ObPLAnyType::TypeCode::TYPECODE_UROWID)

  DECLARE_FUNC_WITH_TYPE(convert);
  DECLARE_FUNC_WITH_TYPE(access);
  DECLARE_FUNC(begincreate);
  DECLARE_FUNC(endcreate);
  DECLARE_FUNC_WITH_TYPE(get);
  DECLARE_FUNC(gettype);
  DECLARE_FUNC(gettypename);
  DECLARE_FUNC(piecewise);
  DECLARE_FUNC_WITH_TYPE(set);

#undef DECLARE_FUNC_WITH_TYPE
#undef DEFINE_FUNC
#undef DECLARE_FUNC

private:
  static int convert(
    sql::ObExecContext &ctx,
    ObPLAnyType::TypeCode typecode, sql::ParamStore &params, common::ObObj &result);
  static int access(
    sql::ObExecContext &ctx,
    ObPLAnyType::TypeCode typecode, sql::ParamStore &params, common::ObObj &result);
  static int get(
    sql::ObExecContext &ctx,
    ObPLAnyType::TypeCode typecode, sql::ParamStore &params, common::ObObj &result);
  static int set(
    sql::ObExecContext &ctx,
    ObPLAnyType::TypeCode typecode, sql::ParamStore &params, common::ObObj &result);
  static int get_anydata(
    sql::ObExecContext &ctx, ObObj &obj, ObPLAnyData *&anydata, bool need_new = false);
  static int build_pl_type(sql::ObExecContext &ctx,
                           ObIAllocator &allocator,
                           ObPLAnyType::TypeCode type_code,
                           ObPLType pl_type,
                           int64_t udt_id,
                           const ObPLDataType *&type,
                           int64_t &rowsize);
};

} // end of pl
} // end of oceanbase

#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_ANYDATA_H_ */