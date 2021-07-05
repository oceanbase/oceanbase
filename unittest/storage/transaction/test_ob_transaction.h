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

#ifndef TEST_OB_TRANSACTION_H_
#define TEST_OB_TRANSACTION_H_

#include "storage/transaction/ob_trans_service.h"

namespace oceanbase {
namespace common {
class ObAddr;
class ObPartitionKey;
}  // namespace common

namespace transaction {
class ObITransTimer;
class ObITransRpc;
class ObILocationAdapter;
class ObIClogAdapter;
}  // namespace transaction

namespace storage {
class ObPartitionService;
}

namespace memtable {
class ObIMemtableCtxFactory;
}

namespace unittest {

class TestServer : public transaction::ObTransService {
public:
  TestServer() : is_inited_(false)
  {}
  ~TestServer()
  {
    destroy();
  }
  int init(const common::ObAddr& self, transaction::ObITransTimer* timer, transaction::ObITransRpc* rpc,
      transaction::ObILocationAdapter* location_adapter, transaction::ObIClogAdapter* clog_adapter,
      memtable::ObIMemtableCtxFactory* mc_factory, storage::ObPartitionService* partition_service);
  void destroy();

private:
  bool is_inited_;
  common::ObAddr self_;
};

struct ServerInfo {
  ServerInfo() : test_server(NULL)
  {}
  common::ObPartitionKey partition;
  common::ObAddr server;
  TestServer* test_server;
};

typedef common::hash::ObHashMap<common::ObPartitionKey, common::ObAddr> PSMap;
typedef common::hash::ObHashMap<common::ObAddr, TestServer*> STMap;

}  // namespace unittest
}  // namespace oceanbase

#endif
