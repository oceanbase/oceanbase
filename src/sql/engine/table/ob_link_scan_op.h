// Copyright 2021 Alibaba Inc. All Rights Reserved.
// Author:
//     shanting <>

#ifndef OCEANBASE_SQL_ENGINE_LINK_SCAN_OP_H_
#define OCEANBASE_SQL_ENGINE_LINK_SCAN_OP_H_

#include "sql/engine/dml/ob_link_op.h"

namespace oceanbase
{
namespace sql
{

class ObLinkScanSpec : public ObLinkSpec
{
  OB_UNIS_VERSION_V(1);
public:
  explicit ObLinkScanSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
  bool has_for_update_;
};

class ObLinkScanOp : public ObLinkOp
{
public:
  static constexpr int64_t CHECK_STATUS_ROWS_INTERVAL =  1 << 10;
  typedef common::ParamStore ObParamStore;
  explicit ObLinkScanOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObLinkScanOp() { destroy(); }

  virtual int inner_open() override;
  virtual int inner_get_next_row() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;

  virtual void reset();
  int init_tz_info(const common::ObTimeZoneInfo *tz_info);
  bool need_read() const { return OB_ISNULL(result_); }
  int inner_execute_link_stmt(const char *link_stmt);
  int get_next(const ObNewRow *&row);
  void reset_inner();
private:
  virtual void reset_dblink() override;
  void reset_result();
  bool need_tx(const ObSQLSessionInfo *my_session) const;
private:
  common::ObMySQLProxy::MySQLResult res_;
  common::sqlclient::ObMySQLResult *result_;
  const common::ObTimeZoneInfo *tz_info_;
  bool iter_end_;
  common::ObArenaAllocator row_allocator_;
  int64_t iterated_rows_;
  ObSQLSessionInfo *tm_session_;
  common::sqlclient::ObISQLConnection *tm_rm_connection_;
  ObReverseLink *reverse_link_;
  sql::DblinkGetConnType conn_type_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_LINK_SCAN_ */
