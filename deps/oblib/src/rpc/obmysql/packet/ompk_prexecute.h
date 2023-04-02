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

#ifndef OCEANBASE_RPC_OBMYSQL_OMPK_PREXECUTE_H_
#define OCEANBASE_RPC_OBMYSQL_OMPK_PREXECUTE_H_

#include "ompk_prepare.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace obmysql
{

/**
 * This packet is response to COM_STMT_PREXECUTE
 * following with param desc && column desc packets
 *
 *  prepare packet (12)
 *  extend_flag_ (4) -- always 0 for now, use for prexecute 
 *  has_result_set (1) -- has_result_set, use for prexecute 
 * 
 */

union ObServerExtendFlag
{
  ObServerExtendFlag() : extend_flag_(0) {}
  explicit ObServerExtendFlag(int32_t flag) : extend_flag_(flag) {}
  int32_t extend_flag_;
  struct {
    int32_t IS_RETURNING_INTO_STMT:1; // is returning into ?
    int32_t IS_ARRAY_BINDING:1; // is arraybinding ?
    int32_t IS_PS_OUT:1; // is ps out ?
  };
};

class OMPKPrexecute: public OMPKPrepare
{
public:
  OMPKPrexecute() :
    OMPKPrepare(),
    extend_flag_(0),
    has_result_set_(0)
  {}
  virtual ~OMPKPrexecute() {}

  virtual int serialize(char* buffer, int64_t length, int64_t& pos) const;
  virtual int64_t get_serialize_size() const;

  inline ObServerExtendFlag get_extend_flag() { return extend_flag_; }
  inline void set_extend_flag(ObServerExtendFlag flag) { extend_flag_ = flag; }

  inline void set_has_result_set(int8_t has_result) { has_result_set_ = has_result; }
  inline ObMySQLPacketType get_mysql_packet_type() { return ObMySQLPacketType::PKT_PREXEC; }

private:
  ObServerExtendFlag extend_flag_;
  int8_t  has_result_set_;
  DISALLOW_COPY_AND_ASSIGN(OMPKPrexecute);
};

} //end of obmysql
} //end of oceanbase


#endif //OCEANBASE_RPC_OBMYSQL_OMPK_PREXECUTE_H_
