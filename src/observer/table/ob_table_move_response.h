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

#ifndef _OB_TABLE_MOVE_RESPONSE_H
#define _OB_TABLE_MOVE_RESPONSE_H 1
#include "ob_table_rpc_response_sender.h"
#include "share/table/ob_table.h"

namespace oceanbase
{
namespace observer
{
class ObTableMoveResponseSender
{
public:
  ObTableMoveResponseSender(rpc::ObRequest *req, const int ret_code)
      :response_sender_(req, result_, ret_code)
  {
  }
  virtual ~ObTableMoveResponseSender() = default;
  OB_INLINE table::ObTableMoveResult& get_result() { return result_; }
  int init(const uint64_t table_id,
           const common::ObTabletID &tablet_id,
           share::schema::ObMultiVersionSchemaService &schema_service);
  int response() { return response_sender_.response(common::OB_SUCCESS); };
private:
  int get_replica(const uint64_t table_id,
                  const common::ObTabletID &tablet_id,
                  table::ObTableMoveReplicaInfo &replica);
private:
  table::ObTableMoveResult result_;
  obrpc::ObTableRpcResponseSender<table::ObTableMoveResult> response_sender_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableMoveResponseSender);
};

} // end namespace server
} // end namespace oceanbase

#endif /* _OB_TABLE_MOVE_RESPONSE_H */
