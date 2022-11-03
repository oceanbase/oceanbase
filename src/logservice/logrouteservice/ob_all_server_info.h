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

#ifndef OCEANBASE_ALL_SERVER_INFO_H_
#define OCEANBASE_ALL_SERVER_INFO_H_

#include "lib/container/ob_se_array.h"              // ObSEArray
#include "share/ob_server_status.h"                 // ObServerStatus
#include "common/ob_zone_type.h"                    // ObZoneType

namespace oceanbase
{
namespace logservice
{
// Records in table __all_server
struct AllServerRecord
{
  typedef share::ObServerStatus::DisplayStatus StatusType;
  uint64_t         svr_id_;
  common::ObAddr   server_;
  StatusType       status_;
  common::ObZone   zone_;

  AllServerRecord() { reset(); }

  void reset()
  {
    svr_id_ = 0;
    server_.reset();
    status_ = share::ObServerStatus::OB_SERVER_ACTIVE;
    zone_.reset();
  }

  int init(const uint64_t svr_id,
      const common::ObAddr &server,
      StatusType &status,
      ObString &zone);

  TO_STRING_KV(K_(svr_id), K_(server), K_(status), K_(zone));
};

class ObAllServerInfo
{
public:
  static const int64_t ALL_SERVER_DEFAULT_RECORDS_NUM = 32;
  typedef common::ObSEArray<AllServerRecord, ALL_SERVER_DEFAULT_RECORDS_NUM> AllServerRecordArray;
  ObAllServerInfo() { reset(); }
  virtual ~ObAllServerInfo() { reset(); }

  int init(const int64_t cluster_id);
  void reset();
  inline int64_t get_cluster_id() { return cluster_id_; }
  inline AllServerRecordArray &get_all_server_array() { return all_srv_array_; }
  int add(AllServerRecord &record);

  TO_STRING_KV(K_(cluster_id), K_(all_srv_array));

private:
  int64_t cluster_id_;
  AllServerRecordArray all_srv_array_;

  DISALLOW_COPY_AND_ASSIGN(ObAllServerInfo);
};

} // namespace logservice
} // namespace oceanbase

#endif

