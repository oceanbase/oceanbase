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

#ifndef _OB_MYSQL_RESULT_SET_H_
#define _OB_MYSQL_RESULT_SET_H_

#include "rpc/obmysql/ob_mysql_field.h"
#include "rpc/obmysql/ob_mysql_row.h"
#include "sql/ob_result_set.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObIEndTransCallback;
}

namespace observer
{

using obmysql::ObMySQLField;
using obmysql::ObMySQLRow;

class ObMySQLResultSet
  : public ObResultSet, public common::ObDLinkBase<ObMySQLResultSet>
{
public:
  /**
   * 构造函数
   *
   * @param [in] obrs SQL执行起返回的数据集
   */
  ObMySQLResultSet(sql::ObSQLSessionInfo &session, common::ObIAllocator &allocator)
      : ObResultSet(session, allocator), field_index_(0), param_index_(0), has_more_result_(false)
  {
    is_user_sql_ = true;
  }

  /**
   * 析构函数
   */
  virtual ~ObMySQLResultSet() {};

  /**
   * 返回下一个字段的信息
   *
   * @param [out] obmf 下一个字段的信息
   *
   * @return 成功返回OB_SUCCESS。如果没有数据，则返回Ob_ITER_END
   */
  int next_field(ObMySQLField &obmf);

  /**
   * return next param
   *
   * @parm [out] obmp next param
   * @return return OB_SUCCESS if succeed, else return OB_ITER_END
   */
  int next_param(ObMySQLField &obmf);

  /**
   * 对于Multi-Query，指示是否为最后一个resultset
   */
  bool has_more_result() const
  {
    return has_more_result_;
  }
  void set_has_more_result(bool has_more)
  {
    has_more_result_ = has_more;
  }

  /**
   * 获取下一行的数据
   *
   * @param [out] obmr 下一行数据
   *
   * @return 成功返回OB_SUCCESS。如果没有数据，则返回Ob_ITER_END
   */
  int next_row(const ObNewRow *&obmr);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int32_t get_type() {return 0;};
  static int to_mysql_field(const ObField &field, ObMySQLField &mfield);

private:
  int64_t field_index_;     /**< 下一个需要读取的字段的序号 */
  int64_t param_index_;     /* < 下一个需要读取的参数的序号*/
  bool has_more_result_;
}; // end class ObMySQLResultSet

inline int64_t ObMySQLResultSet::to_string(char *buf, const int64_t buf_len) const
{
  return ObResultSet::to_string(buf, buf_len);
}

inline int ObMySQLResultSet::next_row(const ObNewRow *&obmr)
{
  return ObResultSet::get_next_row(obmr);
}

} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OB_MYSQL_RESULT_SET_H_ */
