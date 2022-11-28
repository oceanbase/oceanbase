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

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_ANYTYPE_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_ANYTYPE_H_

#include "sql/engine/ob_exec_context.h"
#include "pl/ob_pl_user_type.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSAnyType
{
public:
  ObDBMSAnyType() {}
  virtual ~ObDBMSAnyType() {}

public:

#define DECLARE_FUNC(func) \
  static int func( \
    sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);

  DECLARE_FUNC(begincreate);
  DECLARE_FUNC(setinfo);
  DECLARE_FUNC(addattr);
  DECLARE_FUNC(endcreate);
  DECLARE_FUNC(getpersistent);
  DECLARE_FUNC(getinfo);
  DECLARE_FUNC(getattreleminfo);

#undef DECLARE_FUNC

public:
  static int get_anytype(
    sql::ObExecContext &ctx, ObObj &obj, ObPLAnyType *&anytype, bool need_new = false);
  static int getinfo_common(
    ObPLAnyType::TypeCode typecode,
    const ObPLDataType &pl_type,
    ObObj &prec, ObObj &scale, ObObj &len, ObObj &csid, ObObj &csfrm);
  static int checkinfo_common(
    ObObj &o_prec, ObObj &o_scale, ObObj &o_len, ObObj &o_csid, ObObj &o_csfrm);
  static int setinfo_common(
    ObPLAnyType::TypeCode typecode,
    ObPLDataType &pl_type,
    ObObj &o_prec, ObObj &o_scale, ObObj &o_len, ObObj &o_csid, ObObj &o_csfrm);
  static int gettype_common(
    sql::ObExecContext &ctx, const ObPLDataType *pl_type, ObPLAnyType *&anytype);
  static int init_type_with_typecode(
    ObPLAnyType::TypeCode code, ObPLDataType &pl_type);
  static int alloc_pl_type(
    sql::ObExecContext &ctx, ObPLAnyType::TypeCode code, ObPLDataType *&pl_type);
  static int get_basic_type(
    ObIAllocator &allocator, const common::ObString &type_name, const ObPLDataType *&basic_type);
  static int get_rowsize(
    sql::ObExecContext &ctx, const ObPLDataType &pl_type, int64_t &rowsize);
  static const char* typecode_to_type_str(ObPLAnyType::TypeCode code);
  static int set_elem_with_atype(
    sql::ObExecContext &ctx, ObCollectionType *coll_type, ObObj &atype);
};

} // end of pl
} // end of oceanbase

#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_DBMS_ANYTYPE_H_ */