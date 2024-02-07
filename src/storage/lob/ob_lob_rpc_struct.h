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

#ifndef OCEANBASE_OB_LOB_RPC_STRUCT_
#define OCEANBASE_OB_LOB_RPC_STRUCT_

#include "share/ob_define.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "share/ob_rpc_struct.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{

namespace obrpc
{

struct ObLobQueryBlock
{
  OB_UNIS_VERSION(1);
public:
  ObLobQueryBlock();
  virtual ~ObLobQueryBlock() {}
  void reset();
  bool is_valid() const;

  TO_STRING_KV(K_(size));
  int64_t size_;
};

class ObLobQueryArg final
{
  OB_UNIS_VERSION(1);
public:
  enum QueryType {
    READ = 0,
    GET_LENGTH
  };
  ObLobQueryArg();
  ~ObLobQueryArg();
  TO_STRING_KV(K_(tenant_id), K_(offset), K_(len), K_(cs_type), K_(qtype), K_(scan_backward), K_(lob_locator));
public:
  static const int64_t OB_LOB_QUERY_BUFFER_LEN = 256*1024L;
  static const int64_t OB_LOB_QUERY_OLD_LEN_REFACTOR = 8;
  uint64_t tenant_id_;
  uint64_t offset_; // char offset
  uint64_t len_; // char len
  common::ObCollationType cs_type_;
  bool scan_backward_;
  QueryType qtype_;
  ObLobLocatorV2 lob_locator_;
  DISALLOW_COPY_AND_ASSIGN(ObLobQueryArg);
};

} // obrpc

} // oceanbase

#endif // OCEANBASE_OB_LOB_RPC_STRUCT_
