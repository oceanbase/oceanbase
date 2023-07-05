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

#ifndef _OB_SHARE_OB_GAIS_RPC_H_
#define _OB_SHARE_OB_GAIS_RPC_H_

#include "config/ob_server_config.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/utility.h"
#include "observer/ob_server_struct.h"
#include "share/ob_define.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_gais_msg.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_result_code.h"

namespace oceanbase
{
namespace observer
{
struct ObGlobalContext;
}
namespace obrpc
{
struct ObGAISNextValRpcResult
{
  ObGAISNextValRpcResult() : start_inclusive_(0), end_inclusive_(0), sync_value_(0) {}
  int init(const uint64_t start_inclusive, const uint64_t end_inclusive, const uint64_t sync_value);
  bool is_valid() const
  {
    return start_inclusive_ > 0 && end_inclusive_ > 0 && start_inclusive_ <= end_inclusive_
             && sync_value_ <= end_inclusive_;
  }
  TO_STRING_KV(K_(start_inclusive), K_(end_inclusive), K_(sync_value));

  uint64_t start_inclusive_;
  uint64_t end_inclusive_;
  uint64_t sync_value_;

  OB_UNIS_VERSION(1);
};

struct ObGAISCurrValRpcResult
{
  ObGAISCurrValRpcResult() : sequence_value_(0), sync_value_(0) {}
  int init(const uint64_t sequence_value, const uint64_t sync_value);
  bool is_valid() const
  {
    return sequence_value_ > 0 && sequence_value_ >= sync_value_;
  }
  void reset()
  {
    sequence_value_ = 0;
    sync_value_ = 0;
  }
  TO_STRING_KV(K_(sequence_value), K_(sync_value));

  uint64_t sequence_value_;
  uint64_t sync_value_;

  OB_UNIS_VERSION(1);
};

class ObGAISRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObGAISRpcProxy);

  RPC_S(PR5 next_autoinc_val, OB_GAIS_NEXT_AUTO_INC_REQUEST,
        (share::ObGAISNextAutoIncValReq), ObGAISNextValRpcResult);
  RPC_S(PR5 curr_autoinc_val, OB_GAIS_CURR_AUTO_INC_REQUEST,
        (share::ObGAISAutoIncKeyArg), ObGAISCurrValRpcResult);
  RPC_S(PR5 push_autoinc_val, OB_GAIS_PUSH_AUTO_INC_REQUEST,
        (share::ObGAISPushAutoIncValReq), uint64_t);
  RPC_S(PR5 clear_autoinc_cache, OB_GAIS_CLEAR_AUTO_INC_CACHE, (share::ObGAISAutoIncKeyArg));
};

class ObGAISNextAutoIncP : public ObRpcProcessor< obrpc::ObGAISRpcProxy::ObRpc<OB_GAIS_NEXT_AUTO_INC_REQUEST> >
{
public:
  ObGAISNextAutoIncP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObGAISNextAutoIncP);
};

class ObGAISCurrAutoIncP : public ObRpcProcessor< obrpc::ObGAISRpcProxy::ObRpc<OB_GAIS_CURR_AUTO_INC_REQUEST> >
{
public:
  ObGAISCurrAutoIncP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObGAISCurrAutoIncP);
};

class ObGAISPushAutoIncP : public ObRpcProcessor< obrpc::ObGAISRpcProxy::ObRpc<OB_GAIS_PUSH_AUTO_INC_REQUEST> >
{
public:
  ObGAISPushAutoIncP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObGAISPushAutoIncP);
};

class ObGAISClearAutoIncCacheP : public ObRpcProcessor< obrpc::ObGAISRpcProxy::ObRpc<OB_GAIS_CLEAR_AUTO_INC_CACHE> >
{
public:
  ObGAISClearAutoIncCacheP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObGAISClearAutoIncCacheP);
};

} // obrpc

namespace share
{

class ObGAISRequestRpc
{
public:
  ObGAISRequestRpc() : is_inited_(false), rpc_proxy_(NULL) {}
  ~ObGAISRequestRpc() { destroy(); }
  int init(obrpc::ObGAISRpcProxy *rpc_proxy, const common::ObAddr &self);
  void destroy();
public:
  /*
   * Returns the next (batch) auto-increment value of specified key,
   * and changes the current auto-increment value.
   */
  int next_autoinc_val(const common::ObAddr &server,
                       const ObGAISNextAutoIncValReq &msg,
                       obrpc::ObGAISNextValRpcResult &rpc_result);
  /*
   * Returns the current auto-increment value of specified key.
   */
  int curr_autoinc_val(const common::ObAddr &server,
                       const ObGAISAutoIncKeyArg &msg,
                       obrpc::ObGAISCurrValRpcResult &rpc_result);
  /*
   * Push local sync value to global auto-increment service. This function may
   * change global sync value and current auto-increment value, and return
   * updated latest sync value.
   */
  int push_autoinc_val(const common::ObAddr &server,
                       const ObGAISPushAutoIncValReq &msg,
                       uint64_t &sync_value);

  int clear_autoinc_cache(const common::ObAddr &server,
                          const ObGAISAutoIncKeyArg &msg);

private:
  bool is_inited_;
  obrpc::ObGAISRpcProxy *rpc_proxy_;
  common::ObAddr self_;
};

} // share
} // oceanbase

#endif // _OB_SHARE_OB_GAIS_RPC_H_
