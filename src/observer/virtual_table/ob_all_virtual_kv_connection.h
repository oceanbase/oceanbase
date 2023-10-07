/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OB_ALL_VIRTUAL_KV_CONNECTION_H_
#define OB_ALL_VIRTUAL_KV_CONNECTION_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/table/ob_table_connection_mgr.h"
namespace oceanbase
{
namespace observer
{

class ObAllVirtualKvConnection : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualKvConnection();
  virtual ~ObAllVirtualKvConnection();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  inline void set_connection_mgr(table::ObTableConnectionMgr *connection_mgr) { connection_mgr_ = connection_mgr; }
  virtual void reset();

private:
  enum CONN_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    CLIENT_IP,
    CLIENT_PORT,
    TENANT_ID,
    USER_ID,
    DATABASE_ID,
    FIRST_ACTIVE_TIME,
    LAST_ACTIVE_TIME
  };
  class FillScanner
  {
  public:
    FillScanner()
        :allocator_(NULL),
         scanner_(NULL),
         cur_row_(NULL),
         output_column_ids_(),
         effective_tenant_id_(OB_INVALID_TENANT_ID)
    {
    }
    virtual ~FillScanner(){}
    int operator()(common::hash::HashMapPair<common::ObAddr, table::ObTableConnection> &entry);
    int init(ObIAllocator *allocator,
             common::ObScanner *scanner,
             common::ObNewRow *cur_row,
             const ObIArray<uint64_t> &column_ids,
             uint64_t tenant_id);
    inline void reset();
  private:
      ObIAllocator *allocator_;
      common::ObScanner *scanner_;
      common::ObNewRow *cur_row_;
      ObSEArray<uint64_t, common::OB_PREALLOCATED_NUM> output_column_ids_;
      char svr_ip_[common::OB_IP_STR_BUFF];
      int32_t svr_port_;
      char client_ip_[common::OB_IP_STR_BUFF];
      uint64_t effective_tenant_id_;
      DISALLOW_COPY_AND_ASSIGN(FillScanner);
  };
  table::ObTableConnectionMgr *connection_mgr_;
  FillScanner fill_scanner_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualKvConnection);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif