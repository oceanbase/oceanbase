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

#ifndef _OB_PS_SESSION_INFO_H
#define _OB_PS_SESSION_INFO_H 1
#include "share/ob_define.h"
#include "lib/container/ob_2d_array.h"
#include "lib/objectpool/ob_pool.h"
#include "rpc/obmysql/ob_mysql_global.h" // for EMySQLFieldType
#include "sql/resolver/ob_stmt.h"
namespace oceanbase
{
namespace sql
{
static const int64_t OB_SESSION_SMALL_BLOCK_SIZE = 4 * 1024LL;

// Each prepared statements has one object of this type in the session.
// The object is immutable during the life of the prepared statement.
class ObPsSessionInfo
{
public:
  /*
    sizeof(oceanbase::obmysql::EMySQLFieldType)=4
    sizeof(oceanbase::sql::ObPsSessionInfo)=688
    ObPsSessionInfo由SmallBlockAllocator负责分配内存，小块大小为4K，所以需要调整ParamsType的定义使得size
    尽可能小，且用4K块切割后没有太多浪费。4096-688*5=656，每个4K块会浪费这么多内存。也就是每5个ps浪费656字节，
    可以接受。
    2DArray中存储block的一维数组为SEArray，其Local数组的大小是可调的。一方面，MySQL最大允许绑定变量个数为
    65535，如果要避免二维数组中block数组的内存分配，需要65535*4/4K=64个小块即可。所以SEArray大小设置为64.
   */
  typedef common::ObSegmentArray<oceanbase::obmysql::EMySQLFieldType, OB_SESSION_SMALL_BLOCK_SIZE,
                            common::ObWrapperAllocator,
                            false /* use set alloc */, common::ObSEArray<char *, 64> > ParamsType;
public:
  ObPsSessionInfo()
      : sql_id_(0),
        params_count_(0),
        stmt_type_(stmt::T_NONE),
        params_type_(),
        prepare_sql_(),
        is_dml_(false)
  {}
  ~ObPsSessionInfo() {};

  void init(const common::ObWrapperAllocator &block_alloc) {params_type_.set_block_allocator(block_alloc);}

  uint64_t get_sql_id() const { return this->sql_id_; }
  void set_sql_id(uint64_t sql_id) { this->sql_id_ = sql_id; }

  int64_t get_params_count() const { return this->params_count_; }
  void set_params_count(int64_t params_count) { this->params_count_ = params_count; }

  //const stmt::StmtType &get_stmt_type() const { return this->stmt_type_; }
  //void set_stmt_type(const stmt::StmtType &stmt_type) { this->stmt_type_ = stmt_type; }

  int set_params_type(const common::ObIArray<obmysql::EMySQLFieldType> &types);
  const ParamsType &get_params_type() const {return params_type_;};

  void set_prepare_sql(const common::ObString sql) {prepare_sql_ = sql;}
  common::ObString get_prepare_sql() const {return prepare_sql_;}
  void set_dml(const bool is_dml) {is_dml_ = is_dml;}
  bool is_dml() const {return is_dml_;}
private:
  DISALLOW_COPY_AND_ASSIGN(ObPsSessionInfo);
private:
  uint64_t sql_id_; //key of plan cache
  int64_t params_count_;
  stmt::StmtType stmt_type_; // used by query result cache before got the plan
  ParamsType params_type_; /* params type */
  common::ObString prepare_sql_;
  bool is_dml_;            // is dml;  only DML has physical plan
};

inline int ObPsSessionInfo::set_params_type(const common::ObIArray<obmysql::EMySQLFieldType> &types)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(params_count_ != types.count())) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_LOG(ERROR, "invalid params type count",
            "expected_count", params_count_, "real_count", types.count());
  } else if (0 < types.count()) {
    params_type_.reserve(types.count());
    // bound types
    if (OB_FAIL(params_type_.assign(types))) {
      SQL_LOG(WARN, "no memory");
      params_type_.reset();
    }
    SQL_LOG(DEBUG, "set ps param", K_(sql_id), "param_count", params_type_.count());
  }
  return ret;
}
}
} // end namespace oceanbase

#endif /* _OB_PS_SESSION_INFO_H */
