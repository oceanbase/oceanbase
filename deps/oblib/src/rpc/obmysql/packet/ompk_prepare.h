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

#ifndef OCEANBASE_RPC_OBMYSQL_OMPK_PREPARE_H_
#define OCEANBASE_RPC_OBMYSQL_OMPK_PREPARE_H_

#include "ompk_prepare.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace obmysql
{

/**
 * This packet is response to COM_STMT_PREPARE
 * following with param desc && column desc packets
 *
 *  status (1) -- [00] OK
 *  statement_id (4) -- statement-id
 *  num_columns (2) -- number of columns
 *  num_params (2) -- number of params
 *  reserved_ (1) -- [00] filler
 *  warning_count (2) -- number of warnings
 */

class OMPKPrepare: public ObMySQLPacket
{
public:
  OMPKPrepare() :
    status_(0),
    statement_id_(0),
    column_num_(0),
    param_num_(0),
    reserved_(0),
    warning_count_(0)
  {}
  virtual ~OMPKPrepare() {}

  virtual int serialize(char* buffer, int64_t length, int64_t& pos) const;
  virtual int64_t get_serialize_size() const;

  inline void set_statement_id(const uint32_t id) { statement_id_ = id; }

  inline void set_column_num(const uint16_t num) { column_num_ = num;}

  inline void set_param_num(const uint16_t num) { param_num_ = num; }

  inline void set_warning_count(const uint16_t count) { warning_count_ = count; }
  inline ObMySQLPacketType get_mysql_packet_type() { return ObMySQLPacketType::PKT_PREPARE; }

private:
  uint8_t  status_;
  uint32_t statement_id_;
  uint16_t column_num_;
  uint16_t param_num_;
  uint8_t  reserved_;
  uint16_t warning_count_;
  DISALLOW_COPY_AND_ASSIGN(OMPKPrepare);
};

} //end of obmysql
} //end of oceanbase


#endif //OCEANBASE_RPC_OBMYSQL_OMPK_PREPARE_H_
