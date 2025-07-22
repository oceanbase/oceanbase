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

#ifndef OCEANBASE_SHARE_DEADLOCK_OB_DEADLOCK_TRANS_SERVICE_
#define OCEANBASE_SHARE_DEADLOCK_OB_DEADLOCK_TRANS_SERVICE_
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "observer/ob_server_struct.h"
#include "ob_deadlock_detector_common_define.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_event_history_table_operator.h"

namespace oceanbase
{
namespace share
{
namespace detector
{

class ObDeadLockInnerConnHelper
{
public:
  ObDeadLockInnerConnHelper() : sql_client_(GCTX.sql_proxy_), conn_(nullptr)
  {}
  int init();
  ~ObDeadLockInnerConnHelper() { reset(); }
  void reset();
  bool is_valid() const { return conn_ != nullptr; }
  common::sqlclient::ObISQLConnection *get_connection() { return conn_; }
  int sql_write(const uint64_t tenant_id,
                const char *sql,
                int64_t &affected_rows);
  int sql_read(const uint64_t tenant_id,
               const char *sql,
               common::ObMySQLProxy::MySQLResult &result);
private:
  common::ObMySQLProxy *sql_client_;
  common::sqlclient::ObISQLConnection *conn_;
};


class  ObDeadLockInnerTableService
{
public:
  static int insert(ObDeadLockInnerConnHelper &conn_helper,
                    const ObDetectorInnerReportInfo &inner_info,
                    int64_t sequence,
                    int64_t size,
                    int64_t current_ts);
  static int insert_all(const common::ObIArray<ObDetectorInnerReportInfo> &infos);

  class ObDeadLockEventHistoryTableOperator : public share::ObEventHistoryTableOperator
  {
  public:
    virtual ~ObDeadLockEventHistoryTableOperator() {}
    virtual int async_delete() override;
    static ObDeadLockEventHistoryTableOperator &get_instance();
  private:
    ObDeadLockEventHistoryTableOperator() {};
    DISALLOW_COPY_AND_ASSIGN(ObDeadLockEventHistoryTableOperator);
  };
private:
  friend class ObDeadLockEventHistoryTableOperator;
};

#define DEALOCK_EVENT_INSTANCE (::oceanbase::share::detector::\
        ObDeadLockInnerTableService::\
        ObDeadLockEventHistoryTableOperator::get_instance())

}// namespace detector
}// namespace share
}// namespace oceanbase
#endif