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

#ifndef OCEANBASE_TRANSACTION_OB_LOCATION_ADAPTER_H_
#define OCEANBASE_TRANSACTION_OB_LOCATION_ADAPTER_H_

#include <stdint.h>
#include "ob_trans_define.h"
#include "common/ob_partition_key.h"
#include "lib/net/ob_addr.h"
#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/config/ob_server_config.h"

#include "common/ob_clock_generator.h"

namespace oceanbase {

namespace common {
class ObAddr;
class ObPartitionKey;
}  // namespace common

namespace share {
class ObIPartitionLocationCache;
class ObPartitionLocation;
}  // namespace share

namespace transaction {

class ObILocationAdapter {
public:
  ObILocationAdapter()
  {}
  virtual ~ObILocationAdapter()
  {}
  virtual int init(
      share::ObIPartitionLocationCache* location_cache, share::schema::ObMultiVersionSchemaService* schema_service) = 0;
  virtual void destroy() = 0;

public:
  virtual int get_strong_leader(const common::ObPartitionKey& partition, common::ObAddr& server) = 0;
  virtual int nonblock_get_strong_leader(const common::ObPartitionKey& partition, common::ObAddr& server) = 0;
  virtual int nonblock_renew(const common::ObPartitionKey& partition, const int64_t expire_renew_time) = 0;
  virtual int nonblock_get(
      const uint64_t table_id, const int64_t partition_id, share::ObPartitionLocation& location) = 0;
};

class ObLocationAdapter : public ObILocationAdapter {
public:
  ObLocationAdapter();
  ~ObLocationAdapter()
  {}

  int init(
      share::ObIPartitionLocationCache* location_cache, share::schema::ObMultiVersionSchemaService* schema_service);
  void destroy();

public:
  int get_strong_leader(const common::ObPartitionKey& partition, common::ObAddr& server);
  int nonblock_get_strong_leader(const common::ObPartitionKey& partition, common::ObAddr& server);
  int nonblock_renew(const common::ObPartitionKey& partition, const int64_t expire_renew_time);
  int nonblock_get(const uint64_t table_id, const int64_t partition_id, share::ObPartitionLocation& location);

private:
  int get_strong_leader_(const common::ObPartitionKey& partition, const bool is_sync, common::ObAddr& server);
  void reset_statistics();
  void statistics();

private:
  bool is_inited_;
  share::ObIPartitionLocationCache* location_cache_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  // for statistics
  int64_t renew_access_;
  int64_t total_access_;
  int64_t error_count_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif
