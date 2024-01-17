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

#ifndef OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_SEND_LONG_DATA_H_
#define OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_SEND_LONG_DATA_H_

#include "sql/ob_sql_context.h"
#include "observer/mysql/obmp_base.h"
#include "observer/mysql/ob_query_retry_ctrl.h"
#include "lib/rc/context.h"

namespace oceanbase
{
namespace sql
{
class ObMultiStmtItem;
}
namespace observer
{

struct ObGlobalContext;

class ObMPStmtSendLongData : public ObMPBase
{
public:
  static const obmysql::ObMySQLCmd COM = obmysql::COM_STMT_SEND_LONG_DATA;

  explicit ObMPStmtSendLongData(const ObGlobalContext &gctx);
  virtual ~ObMPStmtSendLongData() {}
  int64_t get_single_process_timestamp() const { return single_process_timestamp_; }
  int64_t get_exec_start_timestamp() const { return exec_start_timestamp_; }
  int64_t get_exec_end_timestamp() const { return exec_end_timestamp_; }
  int64_t get_send_timestamp() const { return get_receive_timestamp(); }
  virtual int flush_buffer(const bool is_last) override
  {
    return ObMPBase::flush_buffer(is_last);
  }
protected:
  virtual int deserialize() { return common::OB_SUCCESS; }
  virtual int before_process() override;
  virtual int process();
  virtual int send_error_packet(int err,
                                const char* errmsg,
                                bool is_partition_hit = true,
                                void *extra_err_info = NULL)
  { return ObMPBase::send_error_packet(err, errmsg, is_partition_hit, extra_err_info); }
  virtual int send_ok_packet(sql::ObSQLSessionInfo &session, ObOKPParam &ok_param)
  { return ObMPBase::send_ok_packet(session, ok_param); }
  virtual int send_eof_packet(const sql::ObSQLSessionInfo &session, const ObMySQLResultSet &result)
  { return ObMPBase::send_eof_packet(session, result); }
  virtual int response_packet(obmysql::ObMySQLPacket &pkt, sql::ObSQLSessionInfo* session)
  { return ObMPBase::response_packet(pkt, session); }
  virtual bool need_send_extra_ok_packet()
  { return OB_NOT_NULL(get_conn()) && get_conn()->need_send_extra_ok_packet(); }
private:
  int do_process(sql::ObSQLSessionInfo &session);
  int response_result(sql::ObSQLSessionInfo &session);

  int process_send_long_data_stmt(sql::ObSQLSessionInfo &session);
  int store_piece(sql::ObSQLSessionInfo &session);

private:
  sql::ObSqlCtx ctx_;
  int64_t single_process_timestamp_;
  int64_t exec_start_timestamp_;
  int64_t exec_end_timestamp_;
  int32_t stmt_id_;
  uint16_t param_id_;
  uint64_t buffer_len_;
  common::ObString buffer_;
  bool need_disconnect_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMPStmtSendLongData);


}; // end of class ObMPStmtSendLongData

} // end of namespace observer
} // end of namespace oceanbase

#endif //OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_SEND_LONG_DATA_H_
