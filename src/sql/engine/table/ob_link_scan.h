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

#ifndef OCEANBASE_SQL_ENGINE_LINK_SCAN_
#define OCEANBASE_SQL_ENGINE_LINK_SCAN_

#include "sql/engine/ob_no_children_phy_operator.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/ob_sql_utils.h"
#include "lib/mysqlclient/ob_mysql_result.h"

namespace oceanbase {
namespace sql {

class ObLinkScan : public ObNoChildrenPhyOperator {
  OB_UNIS_VERSION_V(1);

protected:
  class ObLinkScanCtx : public ObPhyOperatorCtx {
  public:
    typedef ObPhysicalPlanCtx::ParamStore ObParamStore;
    explicit ObLinkScanCtx(ObExecContext& ctx);
    virtual ~ObLinkScanCtx()
    {
      destroy();
    }
    virtual void destroy()
    {
      reset();
    }
    void reset();
    int init_dblink(uint64_t dblink_id, common::ObDbLinkProxy* dblink_proxy);
    int init_tz_info(const common::ObTimeZoneInfo* tz_info);
    int read(const common::ObString& link_stmt_fmt, const common::ObIArray<ObParamPosIdx>& param_infos,
        const ObParamStore& param_store);
    bool need_read() const
    {
      return OB_ISNULL(result_);
    }
    int read(const char* link_stmt);
    int rollback();
    int get_next(const ObNewRow*& row);
    int rescan();

  private:
    int combine_link_stmt(const common::ObString& link_stmt_fmt, const common::ObIArray<ObParamPosIdx>& param_infos,
        const ObParamStore& param_store);
    int extend_stmt_buf(int64_t need_size = 0);
    void reset_dblink();
    void reset_stmt();
    void reset_result();

  private:
    uint64_t dblink_id_;
    common::ObDbLinkProxy* dblink_proxy_;
    common::sqlclient::ObMySQLConnection* dblink_conn_;
    common::ObMySQLProxy::MySQLResult res_;
    common::sqlclient::ObMySQLResult* result_;
    common::ObIAllocator& allocator_;
    const common::ObTimeZoneInfo* tz_info_;
    char* stmt_buf_;
    int64_t stmt_buf_len_;
    static const int64_t STMT_BUF_BLOCK;
  };

public:
  explicit ObLinkScan(common::ObIAllocator& allocator);
  virtual ~ObLinkScan();
  virtual void reset();
  virtual void reuse();
  VIRTUAL_TO_STRING_KV(N_DBLINK_ID, dblink_id_, "stmt_fmt", stmt_fmt_, "param infos", param_infos_);

  int set_param_infos(const common::ObIArray<ObParamPosIdx>& param_infos);
  int set_stmt_fmt(const char* stmt_fmt_buf, int64_t stmt_fmt_len);
  inline void set_dblink_id(uint64_t dblink_id)
  {
    dblink_id_ = dblink_id;
  }

protected:
  virtual int init_op_ctx(ObExecContext& ctx) const;
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int rescan(ObExecContext& ctx) const;
  void reset_inner();

private:
  common::ObIAllocator& allocator_;
  common::ObFixedArray<ObParamPosIdx, common::ObIAllocator> param_infos_;
  common::ObString stmt_fmt_;
  char* stmt_fmt_buf_;
  int64_t stmt_fmt_len_;
  uint64_t dblink_id_;
  DISALLOW_COPY_AND_ASSIGN(ObLinkScan);
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_LINK_SCAN_ */
