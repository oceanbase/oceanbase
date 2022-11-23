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

#ifndef OB_UDF_CTX_MGR_H_
#define OB_UDF_CTX_MGR_H_

#include "sql/engine/user_defined_function/ob_user_defined_function.h"

namespace oceanbase
{
namespace sql
{

class ObExprDllUdf;

/*
 * 通过expr的id作为执行期，expr获得属于自己的执行ctx的key。
 * 目前，相同列会指向同一个raw expr，相同的聚合（例如sum）会指向
 * 同一个raw expr，但是对于普通表达式没有做。
 * ATTENTION: 如果后续要做相同普通表达式共用同一个expr的优化，udf的expr必须
 * 保证不能共用expr。
 *
 * */
class ObUdfCtxMgr
{
private:
  static const int64_t BUKET_NUM = 100;
public:
  ObUdfCtxMgr() : allocator_(common::ObModIds::OB_SQL_UDF), ctxs_() {}
  ~ObUdfCtxMgr();
  int register_udf_expr(const ObExprDllUdf *expr, const ObNormalUdfFunction *func, ObNormalUdfExeUnit *&udf_exec_unit);
  int get_udf_ctx(uint64_t expr_id, ObNormalUdfExeUnit *&udf_exec_unit);
  int try_init_map();
  common::ObIAllocator &get_allocator() { return allocator_; }
  int reset();
private:
  common::ObArenaAllocator allocator_;
  common::hash::ObHashMap<uint64_t, ObNormalUdfExeUnit *, common::hash::NoPthreadDefendMode> ctxs_;
private:
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObUdfCtxMgr);
};

}
}

#endif
