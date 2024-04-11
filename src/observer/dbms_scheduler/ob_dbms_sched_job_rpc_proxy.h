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

#ifndef SRC_OBSERVER_DBMS_SCHED_JOB_RPC_PROXY_H_
#define SRC_OBSERVER_DBMS_SCHED_JOB_RPC_PROXY_H_

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

class ObDBMSSchedJobArg
{
  OB_UNIS_VERSION(1);
public:
  ObDBMSSchedJobArg():
    tenant_id_(OB_INVALID_ID), job_id_(OB_INVALID_ID), server_addr_(), master_addr_(),
    is_oracle_tenant_(true), job_name_() {}

  ObDBMSSchedJobArg(uint64_t tenant_id,
               uint64_t job_id,
               common::ObAddr &server_addr,
               common::ObAddr &master_addr,
               bool is_oracle_tenant,
               common::ObString &job_name)
    : tenant_id_(tenant_id), job_id_(job_id), server_addr_(server_addr), master_addr_(master_addr),
      is_oracle_tenant_(is_oracle_tenant), job_name_(job_name)
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
               K_(job_name),
               K_(server_addr),
               K_(master_addr),
               K_(is_oracle_tenant));

  uint64_t tenant_id_;
  uint64_t job_id_;
  common::ObAddr server_addr_;
  common::ObAddr master_addr_;
  bool is_oracle_tenant_;
  common::ObString job_name_;
};

class ObDBMSSchedJobResult
{
  OB_UNIS_VERSION(1);

public:
  ObDBMSSchedJobResult() { reset(); };
  virtual ~ObDBMSSchedJobResult () {};

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

  bool equal(const ObDBMSSchedJobResult &other) const
  {
    return tenant_id_ == other.tenant_id_
        && job_id_ == other.job_id_
        && server_addr_ == other.server_addr_
        && status_code_ == other.status_code_;
  }

  int assign(const ObDBMSSchedJobResult &other)
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
  DISALLOW_COPY_AND_ASSIGN(ObDBMSSchedJobResult);
};

class ObDBMSSchedStopJobArg
{
  OB_UNIS_VERSION(1);
public:
  ObDBMSSchedStopJobArg():
    tenant_id_(OB_INVALID_ID), job_name_(), session_id_(OB_INVALID_ID), rpc_send_time_(OB_INVALID_TIMESTAMP) {}

  ObDBMSSchedStopJobArg(uint64_t tenant_id,
               common::ObString &job_name,
               uint64_t session_id,
               int64_t rpc_send_time)
    : tenant_id_(tenant_id), job_name_(job_name), session_id_(session_id), rpc_send_time_(rpc_send_time) {}

  inline bool is_valid() const
  {
    return common::is_valid_tenant_id(tenant_id_)
        && !job_name_.empty()
        && session_id_ != OB_INVALID_ID
        && rpc_send_time_ != OB_INVALID_TIMESTAMP;
  }

  TO_STRING_KV(K_(tenant_id),
               K_(job_name),
               K_(session_id));

  uint64_t tenant_id_;
  common::ObString job_name_;
  uint64_t session_id_;
  int64_t rpc_send_time_;
};

class ObDBMSSchedJobRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObDBMSSchedJobRpcProxy);
  RPC_AP(PR5 run_dbms_sched_job, obrpc::OB_RUN_DBMS_SCHED_JOB, (ObDBMSSchedJobArg), ObDBMSSchedJobResult);
  RPC_S(PR5 stop_dbms_sched_job, obrpc::OB_STOP_DBMS_SCHED_JOB, (ObDBMSSchedStopJobArg));

public:
  int run_dbms_sched_job(
    uint64_t tenant_id, bool is_oracle_tenant, uint64_t job_id, common::ObString &job_name,
    common::ObAddr server_addr, common::ObAddr master_addr);
  int stop_dbms_sched_job(
    uint64_t tenant_id, common::ObString &job_name,
    common::ObAddr server_addr, uint64_t session_id);
};

}
}

#endif /* SRC_OBSERVER_DBMS_SCHED_JOB_RPC_PROXY_H_ */
