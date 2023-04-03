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

#ifndef SRC_OBSERVER_DBMS_JOB_RPC_PROXY_H_
#define SRC_OBSERVER_DBMS_JOB_RPC_PROXY_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/net/ob_addr.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/ob_errno.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace obrpc
{

class ObDBMSJobArg
{
  OB_UNIS_VERSION(1);
public:
  ObDBMSJobArg():
    tenant_id_(OB_INVALID_ID), job_id_(OB_INVALID_ID), server_addr_(), master_addr_() {}

  ObDBMSJobArg(uint64_t tenant_id,
               uint64_t job_id,
               common::ObAddr &server_addr,
               common::ObAddr &master_addr)
    : tenant_id_(tenant_id), job_id_(job_id), server_addr_(server_addr), master_addr_(master_addr)
  {}

  inline bool is_valid() const
  {
    return common::is_valid_tenant_id(tenant_id_)
        && job_id_ != common::OB_INVALID_ID
        && server_addr_.is_valid()
        && master_addr_.is_valid();
  }

  TO_STRING_KV(K_(tenant_id),
               K_(job_id),
               K_(server_addr),
               K_(master_addr));

  uint64_t tenant_id_;
  uint64_t job_id_;
  common::ObAddr server_addr_;
  common::ObAddr master_addr_;
};

class ObDBMSJobResult
{
  OB_UNIS_VERSION(1);

public:
  ObDBMSJobResult() { reset(); };
  virtual ~ObDBMSJobResult () {};

  bool is_valid()
  {
    return tenant_id_ != common::OB_INVALID_ID
        && job_id_ != common::OB_INVALID_ID
        && server_addr_.is_valid();
  }
  
  virtual void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    job_id_ = common::OB_INVALID_ID;
    server_addr_.reset();
    status_code_ = common::OB_SUCCESS;
  }

  bool equal(const ObDBMSJobResult &other) const
  {
    return tenant_id_ == other.tenant_id_
        && job_id_ == other.job_id_
        && server_addr_ == other.server_addr_
        && status_code_ == other.status_code_;
  }

  int assign(const ObDBMSJobResult &other)
  {
    tenant_id_ = other.tenant_id_;
    job_id_ = other.job_id_;
    server_addr_ = other.server_addr_;
    status_code_ = other.status_code_;
    return common::OB_SUCCESS;
  }

  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_job_id(uint64_t job_id) { job_id_ = job_id; }
  inline void set_server_addr(common::ObAddr addr) { server_addr_ = addr; }
  inline void set_status_code(int code) { status_code_ = code; }

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_job_id() const { return job_id_; }
  inline const common::ObAddr &get_server_addr() const { return server_addr_; }
  inline int get_status_code() const { return status_code_; }

  TO_STRING_KV(K_(tenant_id),
               K_(job_id),
               K_(server_addr),
               K_(status_code));
protected:
  uint64_t tenant_id_;
  uint64_t job_id_;
  common::ObAddr server_addr_;
  int status_code_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDBMSJobResult);
};

class ObDBMSJobRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObDBMSJobRpcProxy);
  RPC_AP(PR5 run_dbms_job, obrpc::OB_RUN_DBMS_JOB, (ObDBMSJobArg), ObDBMSJobResult);

public:
  int run_dbms_job(
    uint64_t tenant_id, uint64_t job_id,
    common::ObAddr server_addr, common::ObAddr master_addr);
};

}
}

#endif /* SRC_OBSERVER_DBMS_JOB_RPC_PROXY_H_ */
