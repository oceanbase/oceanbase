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

#ifndef OCEANBASE_ROOTSERVER_OB_PARTITION_SPLITER_H
#define OCEANBASE_ROOTSERVER_OB_PARTITION_SPLITER_H

#include "rootserver/ob_rs_reentrant_thread.h"
#include "rootserver/ob_thread_idling.h"
#include "ob_rs_async_rpc_proxy.h"
#include "lib/net/ob_addr.h"
namespace oceanbase {
namespace common {
class ObAddr;
}
namespace obrpc {
class ObCommonRpcProxy;
class ObSplitPartitionArg;
}  // namespace obrpc
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share
namespace rootserver {
class ObRootService;
class ObPartitionSpliter;
class ObPartitionSpliterIdling : public ObThreadIdling {
public:
  explicit ObPartitionSpliterIdling(volatile bool& stop, const ObPartitionSpliter& host)
      : ObThreadIdling(stop), host_(host)
  {}

  virtual int64_t get_idle_interval_us();

private:
  static const int64_t MIN_CHECK_SPLIT_INTERVAL_US = 50 * 1000LL;
  static const int64_t MAX_CHECK_SPLIT_INTERVAL_US = 10 * 1000LL * 1000LL;

  const ObPartitionSpliter& host_;
};

class ObPartitionSpliter : public ObRsReentrantThread {
public:
  friend class ObPartitionSpliterIdling;
  ObPartitionSpliter()
      : inited_(false),
        rpc_proxy_(NULL),
        schema_service_(NULL),
        root_service_(NULL),
        self_addr_(),
        table_in_splitting_(false),
        idling_(stop_, *this){};
  virtual ~ObPartitionSpliter();
  int init(obrpc::ObCommonRpcProxy& rpc_proxy, const common::ObAddr& self_addr,
      share::schema::ObMultiVersionSchemaService* schema_service, ObRootService* root_service);
  virtual void run3() override;
  void wakeup();
  void stop();
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }

private:
  int try_split_partition();

private:
  bool inited_;
  obrpc::ObCommonRpcProxy* rpc_proxy_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObRootService* root_service_;
  common::ObAddr self_addr_;
  bool table_in_splitting_;
  mutable ObPartitionSpliterIdling idling_;
};

class ObPartitionSplitExecutor {
public:
  ObPartitionSplitExecutor(obrpc::ObSrvRpcProxy& rpc_proxy)
      : result_row_(0),
        proxy_batch_(rpc_proxy, &obrpc::ObSrvRpcProxy::batch_split_partition),
        dest_addr_(),
        split_infos_()
  {}
  virtual ~ObPartitionSplitExecutor()
  {}
  int split_tablegroup_partition(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTablegroupSchema& tablegroup_schema, common::ObISQLClient& client,
      share::ObSplitProgress& split_status);
  int split_table_partition(const share::schema::ObTableSchema& schema, share::ObPartitionTableOperator& pt_operator,
      obrpc::ObSrvRpcProxy& rpc_proxy, obrpc::ObSplitPartitionArg& arg, share::ObSplitProgress& split_process);
  int split_binding_tablegroup_partition(const share::schema::ObTablegroupSchema& schema,
      share::ObPartitionTableOperator& pt_operator, obrpc::ObSrvRpcProxy& rpc_proxy, obrpc::ObSplitPartitionArg& arg,
      share::ObSplitProgress& split_process);

private:
  int check_result(share::ObSplitProgress& split_status);
  int send_rpc();
  int accumulate_split_info(const common::ObAddr& addr, const int64_t table_id, const int64_t partition_id,
      const obrpc::ObSplitPartitionArg& args);
  int add_split_arg(obrpc::ObSplitPartitionArg& args, const obrpc::ObSplitPartitionArg& arg) const;
  int get_split_status(const share::schema::ObPartitionSchema& schema, share::ObPartitionTableOperator& pt_operator,
      obrpc::ObSrvRpcProxy& rpc_proxy, obrpc::ObSplitPartitionArg& arg, share::ObSplitProgress& split_status);

private:
  int64_t result_row_;
  ObSplitPartitionBatchProxy proxy_batch_;
  common::ObArray<common::ObAddr> dest_addr_;
  common::ObArray<obrpc::ObSplitPartitionArg> split_infos_;
};
}  // namespace rootserver
}  // namespace oceanbase

#endif
