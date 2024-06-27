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

#ifndef OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_SNAPSHOT_RPC_PROCESSOR_H_
#define OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_SNAPSHOT_RPC_PROCESSOR_H_

#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/wr/ob_wr_rpc_proxy.h"
#include "deps/oblib/src/lib/net/ob_addr.h"

namespace oceanbase {
namespace share {

enum class WrTaskType : int {
  TAKE_SNAPSHOT = 0,
  PURGE,
  USER_SNAPSHOT,
  USER_MODIFY_SETTINGS,
  INVALID,
};

class ObWrSnapshotArg {
  OB_UNIS_VERSION(1);

public:
  ObWrSnapshotArg(WrTaskType type) : task_type_(type)
  {}
  ObWrSnapshotArg() : task_type_(WrTaskType::INVALID)
  {}
  ~ObWrSnapshotArg() = default;
  WrTaskType get_task_type() const
  {
    return task_type_;
  };

  int assign(const ObWrSnapshotArg &other)
  {
    int ret = common::OB_SUCCESS;
    task_type_ = other.task_type_;
    return ret;
  }
  DECLARE_VIRTUAL_TO_STRING;

private:
  WrTaskType task_type_;
};

class ObWrCreateSnapshotArg : public ObWrSnapshotArg {
  OB_UNIS_VERSION(1);

public:
  explicit ObWrCreateSnapshotArg(uint64_t tenant_id, int64_t snap_id, int64_t begin_interval_time,
      int64_t end_interval_time, int64_t timeout_ts)
      : ObWrSnapshotArg(WrTaskType::TAKE_SNAPSHOT),
        tenant_id_(tenant_id),
        snap_id_(snap_id),
        snapshot_begin_time_(begin_interval_time),
        snapshot_end_time_(end_interval_time),
        timeout_ts_(timeout_ts)
  {}
  ObWrCreateSnapshotArg()
      : ObWrSnapshotArg(WrTaskType::TAKE_SNAPSHOT),
        tenant_id_(0),
        snap_id_(0),
        snapshot_begin_time_(0),
        snapshot_end_time_(0),
        timeout_ts_(0)
  {}
  ~ObWrCreateSnapshotArg() = default;
  int64_t get_snap_id() const
  {
    return snap_id_;
  };
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  };
  int64_t get_snapshot_begin_time() const
  {
    return snapshot_begin_time_;
  };
  int64_t get_snapshot_end_time() const
  {
    return snapshot_end_time_;
  };
  int64_t get_timeout_ts() const
  {
    return timeout_ts_;
  };

  int assign(const ObWrCreateSnapshotArg &other)
  {
    int ret = common::OB_SUCCESS;
    tenant_id_ = other.tenant_id_;
    snap_id_ = other.snap_id_;
    snapshot_begin_time_ = other.snapshot_begin_time_;
    snapshot_end_time_ = other.snapshot_end_time_;
    timeout_ts_ = other.timeout_ts_;
    if (OB_FAIL(ObWrSnapshotArg::assign(other))) {
      SHARE_LOG(WARN, "fail to assign wr snapshot arg", KR(ret));
    } else {
      /*do nothing*/
    }
    return ret;
  }
  DECLARE_VIRTUAL_TO_STRING;

private:
  uint64_t tenant_id_;
  int64_t snap_id_;
  int64_t snapshot_begin_time_;
  int64_t snapshot_end_time_;
  int64_t timeout_ts_;
};

class ObWrPurgeSnapshotArg : public ObWrSnapshotArg {
  OB_UNIS_VERSION(1);

public:
  explicit ObWrPurgeSnapshotArg(uint64_t tenant_id, int64_t timeout_ts)
      : ObWrSnapshotArg(WrTaskType::PURGE),
        tenant_id_(tenant_id),
        to_delete_snap_ids_(),
        timeout_ts_(timeout_ts)
  {}
  ObWrPurgeSnapshotArg()
      : ObWrSnapshotArg(WrTaskType::PURGE), tenant_id_(0), to_delete_snap_ids_(), timeout_ts_(0)
  {}
  ~ObWrPurgeSnapshotArg() = default;
  inline uint64_t get_tenant_id() const
  {
    return tenant_id_;
  };
  inline ObIArray<int64_t> &get_to_delete_snap_ids()
  {
    return to_delete_snap_ids_;
  }
  inline const ObIArray<int64_t> &get_to_delete_snap_ids() const
  {
    return to_delete_snap_ids_;
  }
  inline int64_t get_timeout_ts() const
  {
    return timeout_ts_;
  };
  int assign(const ObWrPurgeSnapshotArg &other)
  {
    int ret = common::OB_SUCCESS;
    tenant_id_ = other.tenant_id_;
    timeout_ts_ = other.timeout_ts_;
    if (OB_FAIL(ObWrSnapshotArg::assign(other))) {
      SHARE_LOG(WARN, "fail to assign wr snapshot arg", KR(ret));
    } else if (OB_FAIL(to_delete_snap_ids_.assign(other.to_delete_snap_ids_))) {
      SHARE_LOG(WARN, "fail to assign to delete snap ids", KR(ret));
    } else {
      /*do nothing*/
    }
    return ret;
  }
  DECLARE_VIRTUAL_TO_STRING;

private:
  uint64_t tenant_id_;
  common::ObSArray<int64_t> to_delete_snap_ids_;
  int64_t timeout_ts_;
};

class ObWrUserSubmitSnapArg : public ObWrSnapshotArg {
  OB_UNIS_VERSION(1);

public:
  explicit ObWrUserSubmitSnapArg(int64_t timeout_ts)
      : ObWrSnapshotArg(WrTaskType::USER_SNAPSHOT), timeout_ts_(timeout_ts)
  {}
  ObWrUserSubmitSnapArg() : ObWrSnapshotArg(WrTaskType::USER_SNAPSHOT), timeout_ts_(0)
  {}
  ~ObWrUserSubmitSnapArg() = default;
  inline int64_t get_timeout_ts() const
  {
    return timeout_ts_;
  };

  int assign(const ObWrUserSubmitSnapArg &other)
  {
    int ret = common::OB_SUCCESS;
    timeout_ts_ = other.timeout_ts_;
    if (OB_FAIL(ObWrSnapshotArg::assign(other))) {
      SHARE_LOG(WARN, "fail to assign wr snapshot arg", KR(ret));
    } else {
      /*do nothing*/
    }
    return ret;
  }
  DECLARE_VIRTUAL_TO_STRING;

private:
  int64_t timeout_ts_;
};

class ObWrUserModifySettingsArg : public ObWrSnapshotArg {
  OB_UNIS_VERSION(1);

public:
  explicit ObWrUserModifySettingsArg(int64_t tenant_id, int64_t retention, int64_t interval)
      : ObWrSnapshotArg(WrTaskType::USER_MODIFY_SETTINGS),
        tenant_id_(tenant_id),
        retention_(retention),
        interval_(interval),
        topnsql_(0)
  {}
  ObWrUserModifySettingsArg()
      : ObWrSnapshotArg(WrTaskType::USER_MODIFY_SETTINGS),
        tenant_id_(0),
        retention_(0),
        interval_(0),
        topnsql_(0)
  {}
  ~ObWrUserModifySettingsArg() = default;
  inline int64_t get_tenant_id() const
  {
    return tenant_id_;
  };
  inline int64_t get_retention() const
  {
    return retention_;
  };
  inline int64_t get_interval() const
  {
    return interval_;
  }
  int assign(const ObWrUserModifySettingsArg &other)
  {
    int ret = common::OB_SUCCESS;
    tenant_id_ = other.tenant_id_;
    retention_ = other.retention_;
    interval_ = other.interval_;
    topnsql_ = topnsql_;
    if (OB_FAIL(ObWrSnapshotArg::assign(other))) {
      SHARE_LOG(WARN, "fail to assign wr snapshot arg", KR(ret));
    } else { /*do nothing*/
    }
    return ret;
  }

  DECLARE_VIRTUAL_TO_STRING;

private:
  int64_t tenant_id_;
  int64_t retention_;
  int64_t interval_;
  int64_t topnsql_;
};

class ObWrUserSubmitSnapResp {
public:
  ObWrUserSubmitSnapResp() : snap_id_(0)
  {}
  ~ObWrUserSubmitSnapResp() = default;
  void set_snap_id(int64_t snap_id)
  {
    snap_id_ = snap_id;
  };
  int64_t get_snap_id()
  {
    return snap_id_;
  };
  TO_STRING_KV(K_(snap_id));
  OB_UNIS_VERSION(1);

private:
  int64_t snap_id_;
};

template <obrpc::ObRpcPacketCode pcode>
class ObWrBaseSnapshotTaskP : public obrpc::ObRpcProcessor<obrpc::ObWrRpcProxy::ObRpc<pcode>> {
public:
  typedef obrpc::ObRpcProcessor<obrpc::ObWrRpcProxy::ObRpc<pcode>> RpcProcessor;
  ObWrBaseSnapshotTaskP(const observer::ObGlobalContext &gctx)
  {}
  virtual ~ObWrBaseSnapshotTaskP()
  {}
  int init();
  virtual int before_process() override final;
  virtual int process() = 0;
  virtual int after_process(int error_code) override final;
  virtual void cleanup() override final;
};

class ObWrAsyncSnapshotTaskP final
    : public ObWrBaseSnapshotTaskP<obrpc::OB_WR_ASYNC_SNAPSHOT_TASK> {
public:
  ObWrAsyncSnapshotTaskP(const observer::ObGlobalContext &gctx) : ObWrBaseSnapshotTaskP(gctx)
  {}
  virtual ~ObWrAsyncSnapshotTaskP(){};
  virtual int process() override final;
};

class ObWrAsyncPurgeSnapshotTaskP final
    : public ObWrBaseSnapshotTaskP<obrpc::OB_WR_ASYNC_PURGE_SNAPSHOT_TASK> {
public:
  ObWrAsyncPurgeSnapshotTaskP(const observer::ObGlobalContext &gctx) : ObWrBaseSnapshotTaskP(gctx)
  {}
  virtual ~ObWrAsyncPurgeSnapshotTaskP()
  {}
  virtual int process() override final;
};

class ObWrSyncUserSubmitSnapshotTaskP final
    : public ObWrBaseSnapshotTaskP<obrpc::OB_WR_SYNC_USER_SUBMIT_SNAPSHOT_TASK> {
public:
  ObWrSyncUserSubmitSnapshotTaskP(const observer::ObGlobalContext &gctx)
      : ObWrBaseSnapshotTaskP(gctx)
  {}
  virtual ~ObWrSyncUserSubmitSnapshotTaskP()
  {}
  virtual int process() override final;

private:
  int schedule_next_wr_task(int64_t next_wr_task_ts);
};

class ObWrSyncUserModifySettingsTaskP final
    : public ObWrBaseSnapshotTaskP<obrpc::OB_WR_SYNC_USER_MODIFY_SETTINGS_TASK> {
public:
  ObWrSyncUserModifySettingsTaskP(const observer::ObGlobalContext &gctx)
      : ObWrBaseSnapshotTaskP(gctx)
  {}
  virtual ~ObWrSyncUserModifySettingsTaskP()
  {}
  virtual int process() override final;
};

}  // end namespace share
}  // end namespace oceanbase
#endif  // OCEANBASE_WR_OB_WORKLOAD_REPOSITORY_SNAPSHOT_RPC_PROCESSOR_H_
