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

#ifndef _OCEANBASE_SQL_OB_EXPR_DBMS_LOB_H_
#define _OCEANBASE_SQL_OB_EXPR_DBMS_LOB_H_
#include "lib/ob_name_def.h"
#include "lib/allocator/ob_allocator.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {

// https://docs.oracle.com/cd/B28359_01/appdev.111/b28419/d_lob.htm#i998484
class ObExprDbmsLobGetLength : public ObFuncExprOperator {
public:
  explicit ObExprDbmsLobGetLength(common::ObIAllocator& alloc);
  virtual ~ObExprDbmsLobGetLength();
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& lob, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result1(common::ObObj& result, const common::ObObj& lob, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDbmsLobGetLength);
};

class ObExprDbmsLobAppend : public ObStringExprOperator {
public:
  explicit ObExprDbmsLobAppend(common::ObIAllocator& alloc);
  virtual ~ObExprDbmsLobAppend();
  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;
  static int calc(
      common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObCastCtx& cast_ctx);
  virtual int calc_result2(
      common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDbmsLobAppend);
};

class ObExprDbmsLobRead : public ObStringExprOperator {
public:
  explicit ObExprDbmsLobRead(common::ObIAllocator& alloc);
  virtual ~ObExprDbmsLobRead();
  virtual int calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprResType& type3,
      common::ObExprTypeCtx& type_ctx) const;
  static int calc(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      const common::ObObj& obj3, common::ObCastCtx& cast_ctx);
  virtual int calc_result3(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      const common::ObObj& obj3, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDbmsLobRead);
};

class ObExprDbmsLobConvertToBlob : public ObStringExprOperator {
public:
  explicit ObExprDbmsLobConvertToBlob(common::ObIAllocator& alloc);
  virtual ~ObExprDbmsLobConvertToBlob();
  virtual int calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprResType& type3,
      common::ObExprTypeCtx& type_ctx) const;
  static int calc(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      const common::ObObj& obj3, common::ObCastCtx& cast_ctx);
  virtual int calc_result3(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      const common::ObObj& obj3, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDbmsLobConvertToBlob);
};

class ObExprDbmsLobCastClobToBlob : public ObStringExprOperator {
public:
  explicit ObExprDbmsLobCastClobToBlob(common::ObIAllocator& alloc);
  virtual ~ObExprDbmsLobCastClobToBlob();
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& clob, common::ObExprTypeCtx& type_ctx) const;
  static int calc(common::ObObj& result, const common::ObObj& obj, common::ObCastCtx& cast_ctx);
  virtual int calc_result1(common::ObObj& result, const common::ObObj& clob, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDbmsLobCastClobToBlob);
};

class ObExprDbmsLobConvertClobCharset : public ObStringExprOperator {
public:
  explicit ObExprDbmsLobConvertClobCharset(common::ObIAllocator& alloc);
  virtual ~ObExprDbmsLobConvertClobCharset();
  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;
  static int calc(
      common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObCastCtx& cast_ctx);
  virtual int calc_result2(
      common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDbmsLobConvertClobCharset);
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OB_EXPR_DBMS_LOB_H_
