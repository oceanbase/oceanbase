/*
 * Copyright (c) 2022 OceanBase Technology Co.,Ltd.
 * OceanBase is licensed under Mulan PubL v1.
 * You can use this software according to the terms and conditions of the Mulan PubL v1.
 * You may obtain a copy of Mulan PubL v1 at:
 *          http://license.coscl.org.cn/MulanPubL-1.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v1 for more details.
 * ---------------------------------------------------------------------------------------
 * Authors:
 *   Juehui <>
 * ---------------------------------------------------------------------------------------
 */
#ifndef OB_VIRTUAL_SPAN_INFO_H_
#define OB_VIRTUAL_SPAN_INFO_H_
#include "share/ob_virtual_table_scanner_iterator.h"
namespace oceanbase
{
namespace observer
{
class ObVirtualSpanInfo : public common::ObVirtualTableScannerIterator
{
public:
  ObVirtualSpanInfo();
  int inner_open();
  int check_ip_and_port(bool &is_valid);
  inline void set_addr(common::ObAddr &addr) {addr_ = &addr;}
  virtual int set_ip(common::ObAddr *addr);
  virtual ~ObVirtualSpanInfo();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  int extract_request_ids(const uint64_t tenant_id, int64_t &start_id,
                          int64_t &end_id, bool &is_valid);
  int fill_cells(sql::ObFLTSpanRec &record);
private:
  enum SYS_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    REQUEST_ID,
    TRACE_ID,
    SPAN_ID,
    PARENT_SPAN_ID,
    SPAN_NAME,
    REF_TYPE,
    START_TS,
    END_TS,
    TAGS,
    LOGS,
  };
  common::ObObj cells_[common::OB_ROW_MAX_COLUMNS_COUNT];
  //static const char FOLLOW[] = "FOLLOW";
  //static const char CHILD[] = "CHILD";

  const static int64_t PRI_KEY_IP_IDX        = 0;
  const static int64_t PRI_KEY_PORT_IDX      = 1;
  const static int64_t PRI_KEY_TENANT_ID_IDX = 2;
  const static int64_t PRI_KEY_TRACE_ID_IDX    = 3;
  const static int64_t PRI_KEY_REQ_ID_IDX    = 4;

  const static int64_t IDX_KEY_TENANT_ID_IDX = 0;
  const static int64_t IDX_KEY_REQ_ID_IDX    = 1;
  const static int64_t IDX_KEY_IP_IDX        = 2;
  const static int64_t IDX_KEY_TRACE_ID_IDX    = 3;
  const static int64_t IDX_KEY_PORT_IDX      = 4;
  DISALLOW_COPY_AND_ASSIGN(ObVirtualSpanInfo);
  sql::ObFLTSpanMgr *cur_flt_span_mgr_;
  int64_t start_id_;
  int64_t end_id_;
  int64_t cur_id_;
  common::ObRaQueue::Ref ref_;
  common::ObAddr *addr_;
  common::ObString ipstr_;
  int32_t port_;
  char server_ip_[common::MAX_IP_ADDR_LENGTH + 2];
  char client_ip_[common::MAX_IP_ADDR_LENGTH + 2];
  char user_client_ip_[common::MAX_IP_ADDR_LENGTH + 2];
  char trace_id_[128];
  bool is_first_get_;

  share::ObTenantSpaceFetcher *with_tenant_ctx_;
};
} /* namespace observer */
} /* namespace oceanbase */
#endif /* OB_VIRTUAL_SPAN_INFO_H_ */
