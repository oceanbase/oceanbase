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
#ifndef OB_VIRTUAL_SHOW_TRACE_H_
#define OB_VIRTUAL_SHOW_TRACE_H_
#include "share/ob_virtual_table_scanner_iterator.h"
namespace oceanbase
{
namespace observer
{

class ObVirtualShowTrace : public common::ObVirtualTableScannerIterator
{
public:
  ObVirtualShowTrace();
  int inner_open();
  int extract_tenant_ids();
  inline void set_addr(common::ObAddr &addr) {addr_ = &addr;}
  virtual int set_ip(common::ObAddr *addr);
  virtual ~ObVirtualShowTrace();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  int extract_request_ids(const uint64_t tenant_id,
                          int64_t &start_id,
                          int64_t &end_id,
                          bool &is_valid);
  int retrive_all_span_info();
  int generate_span_info_tree();
  int merge_span_info();
  int merge_range_span_info(int64_t l, int64_t r, sql::ObFLTShowTraceRec &rec);
  int find_child_span_info(sql::ObFLTShowTraceRec::trace_formatter::TreeLine *parent_type,
                            ObString parent_span_id,
                            ObIArray<sql::ObFLTShowTraceRec*> &arr,
                            int64_t depth);
  int format_flt_show_trace_record(sql::ObFLTShowTraceRec &rec);
  int read_show_trace_rec_from_result(sqlclient::ObMySQLResult &mysql_result, sql::ObFLTShowTraceRec &rec);
  int set_tenant_trace_id(const common::ObIArray<common::ObNewRange> &ranges);
  int fill_cells(sql::ObFLTShowTraceRec &record);
  int get_tag_buf(char *&tag_buf);
  int alloc_trace_rec(sql::ObFLTShowTraceRec *&rec);

private:
  enum SYS_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    TRACE_ID,
    REQUEST_ID,
    REC_SVR_IP,
    REC_SVR_PORT,
    SPAN_ID,
    PARENT_SPAN_ID,
    SPAN_NAME,
    REF_TYPE,
    START_TS,
    END_TS,
    ELAPSE,
    TAGS,
    LOGS,
  };
  common::ObObj cells_[common::OB_ROW_MAX_COLUMNS_COUNT];

  DISALLOW_COPY_AND_ASSIGN(ObVirtualShowTrace);

  common::ObRaQueue::Ref ref_;
  common::ObAddr *addr_;
  common::ObString ipstr_;
  int32_t port_;
  char server_ip_[common::MAX_IP_ADDR_LENGTH + 2];
  char client_ip_[common::MAX_IP_ADDR_LENGTH + 2];
  char user_client_ip_[common::MAX_IP_ADDR_LENGTH + 2];
  ObString trace_id_;
  int64_t tenant_id_;
  bool is_first_get_;
  bool is_use_index_;
  common::ObSEArray<uint64_t, 16> tenant_id_array_;
  int64_t show_trace_rec_idx_;
  ObArenaAllocator alloc_;
  ObSEArray<sql::ObFLTShowTraceRec*, 16> show_trace_arr_;
  char* tag_buf_;
  bool is_row_format_;

  share::ObTenantSpaceFetcher *with_tenant_ctx_;
};
} /* namespace observer */
} /* namespace oceanbase */
#endif /* OB_VIRTUAL_SHOW_TRACE_H_ */
