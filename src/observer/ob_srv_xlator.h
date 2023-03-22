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

#ifndef _OCEABASE_OBSERVER_OB_SRV_XLATOR_H_
#define _OCEABASE_OBSERVER_OB_SRV_XLATOR_H_

#include "lib/utility/ob_macro_utils.h"
#include "rpc/frame/ob_req_translator.h"
#include "rpc/obrpc/ob_rpc_translator.h"
#include "rpc/obmysql/ob_mysql_translator.h"

#define RPCP_BUF_SIZE 1280
union EP_RPCP_BUF;
RLOCAL_EXTERN(EP_RPCP_BUF, co_ep_rpcp_buf);

#define RPC_PROCESSOR(ObRpcP, ...)                                             \
xlator->register_rpc_process_function(ObRpcP::PCODE,                           \
[](const ObGlobalContext &gctx_,                                               \
   ObReqProcessor *&processor,                                                 \
   ObRpcSessionHandler &handler_)                                              \
{                                                                              \
  UNUSED(gctx_);                                                               \
  int ret = OB_SUCCESS;                                                        \
  ObIAllocator *alloc = &get_sql_arena_allocator();                            \
  ObRpcP *p =  NULL;                                                           \
  if (sizeof(ObRpcP) <= RPCP_BUF_SIZE) {                                       \
    p = new (&co_ep_rpcp_buf) ObRpcP(__VA_ARGS__);                             \
  } else {                                                                     \
    p = OB_NEWx(ObRpcP, alloc, __VA_ARGS__);                                   \
  }                                                                            \
  if (NULL == p) {                                                             \
    ret = OB_ALLOCATE_MEMORY_FAILED;                                           \
  } else if (OB_FAIL(p->init())) {                                             \
    SERVER_LOG(ERROR, "Init " #ObRpcP "fail", K(ret));                         \
    worker_allocator_delete(p);                                                \
    p = NULL;                                                                  \
  } else {                                                                     \
    p->set_session_handler(handler_);                                          \
    processor = p;                                                             \
  }                                                                            \
  return ret;                                                                  \
});

namespace oceanbase
{
namespace observer
{

using rpc::frame::ObReqProcessor;
using obrpc::ObRpcTranslator;
using obrpc::ObRpcSessionHandler;
using obmysql::ObMySQLTranslator;
using common::ObIAllocator;
struct ObGlobalContext;


ObIAllocator &get_sql_arena_allocator();

template <typename T>
int ob_srv_rpc_processer(const ObGlobalContext &gctx_,
                         ObReqProcessor *&processor,
                         ObRpcSessionHandler &handler_);


template <typename T>
void worker_allocator_delete(T *&ptr)
{
  if (NULL != ptr) {
    ptr->~T();
    get_sql_arena_allocator().free(ptr);
    ptr = NULL;
  }
}

using RPCProcessFunc = int(*)(const ObGlobalContext &gctxb,
                              ObReqProcessor *&processor,
                              ObRpcSessionHandler &handler_);

class ObSrvRpcXlator;

void init_srv_xlator_for_storage(ObSrvRpcXlator *xlator);
void init_srv_xlator_for_rootserver(ObSrvRpcXlator *xlator);
void init_srv_xlator_for_sys(ObSrvRpcXlator *xlator);
void init_srv_xlator_for_schema_test(ObSrvRpcXlator *xlator);
void init_srv_xlator_for_transaction(ObSrvRpcXlator *xlator);
void init_srv_xlator_for_clog(ObSrvRpcXlator *xlator);
void init_srv_xlator_for_executor(ObSrvRpcXlator *xlator);
void init_srv_xlator_for_partition(ObSrvRpcXlator *xlator);
void init_srv_xlator_for_migrator(ObSrvRpcXlator *xlator);
void init_srv_xlator_for_others(ObSrvRpcXlator *xlator);
void init_srv_xlator_for_palfenv(ObSrvRpcXlator *xlator);
void init_srv_xlator_for_logservice(ObSrvRpcXlator *xlator);
void init_srv_xlator_for_cdc(ObSrvRpcXlator *xlator);
//new for log stream
void init_srv_xlator_for_migration(ObSrvRpcXlator *xlator);


class ObSrvRpcXlator
    : public ObRpcTranslator
{
public:
  explicit ObSrvRpcXlator(const ObGlobalContext &gctx)
      : gctx_(gctx)
  {
    memset(funcs_, 0, sizeof(funcs_));
    init_srv_xlator_for_storage(this);
    init_srv_xlator_for_rootserver(this);
    init_srv_xlator_for_sys(this);
    init_srv_xlator_for_schema_test(this);
    init_srv_xlator_for_transaction(this);
    init_srv_xlator_for_clog(this);
    init_srv_xlator_for_executor(this);
    init_srv_xlator_for_partition(this);
    init_srv_xlator_for_migrator(this);
    init_srv_xlator_for_others(this);
    init_srv_xlator_for_palfenv(this);
    init_srv_xlator_for_logservice(this);
    init_srv_xlator_for_cdc(this);
    init_srv_xlator_for_migration(this);
  }

  void register_rpc_process_function(int pcode, RPCProcessFunc func);

  int translate(rpc::ObRequest &req, ObReqProcessor *&processor);

protected:
  ObReqProcessor *get_processor(rpc::ObRequest &) { return NULL; }
private:
  const ObGlobalContext &gctx_;
  DISALLOW_COPY_AND_ASSIGN(ObSrvRpcXlator);
  RPCProcessFunc funcs_[MAX_PCODE];
}; // end of class ObSrvRpcXlator

class ObSrvMySQLXlator
    : public ObMySQLTranslator
{
public:
  explicit ObSrvMySQLXlator(const ObGlobalContext &gctx)
      : gctx_(gctx)
  {}

  int translate(rpc::ObRequest &req, ObReqProcessor *&processor);

protected:
  ObReqProcessor *get_processor(rpc::ObRequest &) { return NULL; }

  //mpconnect use high memory, more limit than common
  int get_mp_connect_processor(ObReqProcessor *&ret_proc);

private:
  const ObGlobalContext &gctx_;
  DISALLOW_COPY_AND_ASSIGN(ObSrvMySQLXlator);
}; // end of class ObSrvMySQLXlator

class ObSrvXlator
    : public rpc::frame::ObReqTranslator
{
public:
  explicit ObSrvXlator(const ObGlobalContext &gctx)
      : rpc_xlator_(gctx), mysql_xlator_(gctx), session_handler_()
  {}

  // Be aware those two functions would be invoked for each thread.
  int th_init();
  int th_destroy();

  int release(ObReqProcessor *processor);

  inline ObRpcSessionHandler &get_session_handler();

protected:
  ObReqProcessor *get_processor(rpc::ObRequest &);

private:
  // This method must return non-null processor
  ObReqProcessor *get_error_rpc_processor(const int ret);
  ObReqProcessor *get_error_mysql_processor(const int ret);

private:
  ObSrvRpcXlator rpc_xlator_;
  ObSrvMySQLXlator mysql_xlator_;
  ObRpcSessionHandler session_handler_;
  DISALLOW_COPY_AND_ASSIGN(ObSrvXlator);
}; // end of class ObSrvXlator

//////////////////////////////////////////////////////////////
//  inline functions definition
///
ObRpcSessionHandler &ObSrvXlator::get_session_handler()
{
  return rpc_xlator_.get_session_handler();
}


} // end of namespace observer
} // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_OB_SRV_XLATOR_H_ */
