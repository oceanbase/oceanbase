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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_PX_P2P_DATAHUB_TABLE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_PX_P2P_DATAHUB_TABLE_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/net/ob_addr.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"
namespace oceanbase
{

namespace observer
{



class ObAllPxP2PDatahubTable : public common::ObVirtualTableScannerIterator
{
public:
  struct P2PDatahubNode {
    common::ObCurTraceId::TraceId trace_id_;
    int64_t p2p_datahub_id_;
    int64_t tenant_id_;
    int64_t msg_type_;
    int64_t hold_size_;
    int64_t timeout_ts_;
    int64_t start_time_;
    TO_STRING_KV(K(trace_id_), K(p2p_datahub_id_), K(tenant_id_),
                 K(msg_type_), K(hold_size_), K(timeout_ts_), K(start_time_));
  };
public:
  struct P2PMsgTraverseCall
  {
    P2PMsgTraverseCall(common::ObArray<P2PDatahubNode> &node_array, int64_t tenant_id) :
        node_array_(node_array), tenant_id_(tenant_id) {};
    ~P2PMsgTraverseCall() = default;
    int operator() (common::hash::HashMapPair<sql::ObP2PDhKey, sql::ObP2PDatahubMsgBase *> &entry);
    common::ObArray<P2PDatahubNode> &node_array_;
    int64_t tenant_id_;
  };
public:
  ObAllPxP2PDatahubTable();
  virtual ~ObAllPxP2PDatahubTable();
  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  inline void set_addr(common::ObAddr &addr) { addr_ = &addr; }
private:
  int p2p_datahub_map_to_array();
private:
  common::ObAddr *addr_;
  bool start_to_read_;
  char trace_id_[128];
  common::ObArray<P2PDatahubNode> node_array_;
  int64_t index_;

  enum INSPECT_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TRACE_ID,
    DATAHUB_ID,
    MESSAGE_TYPE,
    TENANT_ID,
    HOLD_SIZE,
    TIMEOUT_TS,
    START_TIME
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllPxP2PDatahubTable);
};

} // namespace observer
} // namespace oceanbase
#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_PX_P2P_DATAHUB_TABLE_
