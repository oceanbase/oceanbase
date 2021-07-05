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

namespace oceanbase {
namespace sql {
class ObIEndTransCallback;
}

namespace observer {

using obmysql::ObMySQLField;
using obmysql::ObMySQLRow;

class ObMySQLResultSet : public ObResultSet, public common::ObDLinkBase<ObMySQLResultSet> {
public:
  /**
   * construct
   *
   */
  ObMySQLResultSet(sql::ObSQLSessionInfo& session, common::ObIAllocator& allocator)
      : ObResultSet(session, allocator), field_index_(0), param_index_(0), has_more_result_(false)
  {
    is_user_sql_ = true;
  }

  // only for observer/mysql/obmp_connect.cpp:112
  ObMySQLResultSet(sql::ObSQLSessionInfo& session)
      : ObResultSet(session), field_index_(0), param_index_(0), has_more_result_(false)
  {
    is_user_sql_ = true;
  }

  /**
   * destory
   */
  virtual ~ObMySQLResultSet(){};

  /**
   * Return the information of the next field
   *
   * @param [out] obmf Information in the next field
   *
   * @return ret = OB_SUCCESS.is no data ret = Ob_ITER_END
   */
  int next_field(ObMySQLField& obmf);

  /**
   * return next param
   *
   * @parm [out] obmp next param
   * @return return OB_SUCCESS if succeed, else return OB_ITER_END
   */
  int next_param(ObMySQLField& obmf);

  /**
   * Multi-Query,whether have other resultset
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
   * get next row
   *
   * @param [out] obmr next row
   *
   * @return execute succeed ret = OB_SUCCESS.id no data, ret = Ob_ITER_END
   */
  int next_row(const ObNewRow*& obmr);
  int64_t to_string(char* buf, const int64_t buf_len) const;
  int32_t get_type()
  {
    return 0;
  };
  static int to_mysql_field(const ObField& field, ObMySQLField& mfield);

private:
  int64_t field_index_; /**< The sequence number of the next field to be read */
  int64_t param_index_; /* < The serial number of the next parameter to be read*/
  bool has_more_result_;
};  // end class ObMySQLResultSet

inline int64_t ObMySQLResultSet::to_string(char* buf, const int64_t buf_len) const
{
  return ObResultSet::to_string(buf, buf_len);
}

inline int ObMySQLResultSet::next_row(const ObNewRow*& obmr)
{
  return ObResultSet::get_next_row(obmr);
}

}  // namespace observer
}  // end of namespace oceanbase

#endif /* _OB_MYSQL_RESULT_SET_H_ */
