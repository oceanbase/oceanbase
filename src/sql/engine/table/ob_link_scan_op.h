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

#ifndef OCEANBASE_SQL_ENGINE_LINK_SCAN_OP_H_
#define OCEANBASE_SQL_ENGINE_LINK_SCAN_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/ob_sql_utils.h"
#include "lib/mysqlclient/ob_mysql_result.h"

namespace oceanbase
{
namespace sql
{

class ObLinkScanSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);


public:
  explicit ObLinkScanSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);


  int set_param_infos(const common::ObIArray<ObParamPosIdx> &param_infos);
  int set_stmt_fmt(const char *stmt_fmt_buf, int64_t stmt_fmt_len);

  common::ObIAllocator &allocator_;
  common::ObFixedArray<ObParamPosIdx, common::ObIAllocator> param_infos_;
  common::ObString stmt_fmt_;
  char *stmt_fmt_buf_;
  int64_t stmt_fmt_len_;
  uint64_t dblink_id_;
};

class ObLinkScanOp : public ObOperator
{
public:
  static constexpr int64_t CHECK_STATUS_ROWS_INTERVAL =  1 << 10;
  typedef common::ParamStore ObParamStore;
  explicit ObLinkScanOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObLinkScanOp() { destroy(); }
  virtual void destroy() { reset(); }

  virtual int inner_open() override;
  virtual int inner_get_next_row() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;

  inline void set_link_driver_proto(common::sqlclient::DblinkDriverProto type) { link_type_ = type; }
  void reset();
  int init_dblink(uint64_t dblink_id, common::ObDbLinkProxy *dblink_proxy);
  int init_tz_info(const common::ObTimeZoneInfo *tz_info);
  int read(const common::ObString &link_stmt_fmt,
            const common::ObIArray<ObParamPosIdx> &param_infos,
            const ObParamStore &param_store);
  bool need_read() const { return OB_ISNULL(result_); }
  int read(const char *link_stmt);
  int rollback();
  int get_next(const ObNewRow *&row);
  void reset_inner();
  static int init_dblink_param_ctx(ObExecContext &exec_ctx, common::sqlclient::dblink_param_ctx &param_ctx);
  static int get_charset_id(ObExecContext &exec_ctx, uint16_t &charset_id, uint16_t &ncharset_id);
private:
  int combine_link_stmt(const common::ObString &link_stmt_fmt,
                        const common::ObIArray<ObParamPosIdx> &param_infos,
                        const ObParamStore &param_store);
  int extend_stmt_buf(int64_t need_size = 0);
  void reset_dblink();
  void reset_stmt();
  void reset_result();
private:
  uint64_t tenant_id_;
  uint64_t dblink_id_;
  common::ObDbLinkProxy *dblink_proxy_;
  common::sqlclient::ObISQLConnection *dblink_conn_;
  common::ObMySQLProxy::MySQLResult res_;
  common::sqlclient::ObMySQLResult *result_;
  common::ObIAllocator &allocator_;
  const common::ObTimeZoneInfo *tz_info_;
  char *stmt_buf_;
  int64_t stmt_buf_len_;
  bool iter_end_;
  common::ObArenaAllocator row_allocator_;
  static const int64_t STMT_BUF_BLOCK;
  common::sqlclient::DblinkDriverProto link_type_;
  int64_t elapse_time_;
  uint32_t sessid_;
  int64_t iterated_rows_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_LINK_SCAN_ */
