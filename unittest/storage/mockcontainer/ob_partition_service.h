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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_SERVICE
#define OCEANBASE_STORAGE_OB_PARTITION_SERVICE

#undef private
#include <gmock/gmock.h>
#define private public

#include "lib/container/ob_array_array.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "storage/access/ob_dml_param.h"
#include "sql/ob_end_trans_callback.h"
#include "share/ob_server_locality_cache.h"

namespace oceanbase
{

namespace blocksstable
{
struct ObStorageEnv;
}

namespace share
{
class ObSplitPartition;
class ObSplitPartitionPair;
class ObServerLocality;
}

namespace election
{
class ObIElectionMgr;
}

namespace transaction
{
class ObXAService;
}

namespace storage
{
class ObPartitionService;
class ObAddPartitionToPGLog;
struct ObChangePGLogArchiveMetaLogEntry;
struct ObSSTableInsertTabletParam;
class ObStoreCtx;

struct ObCbTask
{};


class ObPartitionService
{
public:
  ObPartitionService() {}
  ~ObPartitionService() {}
public:
  MOCK_METHOD0(reload_config,
               int());

  MOCK_METHOD1(internal_leader_revoke,
               int(const ObCbTask &revoke_task));
  MOCK_METHOD1(internal_leader_takeover,
               int(const ObCbTask &takeover_task));
  MOCK_CONST_METHOD2(get_server_region, int(const common::ObAddr &server,
                                            common::ObRegion &region));
  MOCK_CONST_METHOD2(get_server_idc, int(const common::ObAddr &server,
                                         common::ObIDC &idc));
  MOCK_CONST_METHOD2(get_server_cluster_id, int(const common::ObAddr &server,
                                                int64_t &cluster_id));
  MOCK_METHOD2(record_server_region,
               int(const common::ObAddr &server,
                   const common::ObRegion &region));
  MOCK_METHOD2(record_server_idc,
               int(const common::ObAddr &server,
                   const common::ObIDC &idc));
  MOCK_METHOD2(record_server_cluster_id,
               int(const common::ObAddr &server,
                   const int64_t cluster_id));

  MOCK_METHOD1(push_callback_task,
               int(const ObCbTask &task));
  MOCK_METHOD1(activate_tenant,
               int(const uint64_t tenant_id));
  MOCK_METHOD1(inactivate_tenant,
               int(const uint64_t tenant_id));

  MOCK_METHOD0(force_refresh_locality_info, int());

  MOCK_METHOD0(start,
               int());
  MOCK_METHOD0(stop,
               int());
  MOCK_METHOD0(wait,
               int());
  MOCK_METHOD0(destroy,
               int());
  MOCK_METHOD3(load_partition,
               int(const char *buf, const int64_t buf_len, int64_t &pos));
  MOCK_METHOD4(replay,
               int(const int64_t log_seq_num, const int64_t subcmd, const char *buf, const int64_t len));
  MOCK_METHOD4(parse,
               int(const int64_t subcmd, const char *buf, const int64_t len, FILE *stream));
  MOCK_METHOD0(remove_orphans,
               int());
  MOCK_METHOD0(freeze,
               int());
  MOCK_CONST_METHOD0(get_min_using_file_id,
                     int64_t());
  MOCK_METHOD2(kill_query_session, int(const transaction::ObTransDesc &trans_desc, const int status));
  MOCK_METHOD7(start_trans,
               int(const uint64_t tenant_id, const uint64_t thread_id, const transaction::ObStartTransParam &req,
                   const int64_t expired_time, const uint32_t session_id, uint64_t proxy_session_id, transaction::ObTransDesc &trans_desc));
  MOCK_METHOD4(end_trans,
               int(bool is_rollback, transaction::ObTransDesc &trans_desc, sql::ObIEndTransCallback &cb,
                   const int64_t stmt_expired_time));
  MOCK_METHOD2(table_scan,
               int(ObVTableScanParam &param, common::ObNewRowIterator *&result));
  MOCK_METHOD3(join_mv_scan, int(storage::ObTableScanParam &left_param,
      storage::ObTableScanParam &right_param, common::ObNewRowIterator *&result));
  MOCK_METHOD1(revert_scan_iter,
               int(common::ObNewRowIterator *iter));

  void clear()
  {
  }

  virtual int get_all_partition_status(int64_t &inactive_num, int64_t &total_num) const
  {
    inactive_num = 0;
    total_num = 0;
    return common::OB_SUCCESS;
  }
  MOCK_METHOD0(garbage_clean, void());
  MOCK_METHOD0(get_rs_rpc_proxy, obrpc::ObCommonRpcProxy & ());
  MOCK_METHOD2(get_frozen_status,
               int(const int64_t major_version, ObFrozenStatus &frozen_status));
  MOCK_METHOD1(insert_frozen_status, int(const ObFrozenStatus &src));
  MOCK_METHOD2(update_frozen_status,
               int(const ObFrozenStatus &src, const ObFrozenStatus &tgt));
  MOCK_METHOD1(delete_frozen_status, int(const int64_t major_version));

  MOCK_METHOD0(admin_wash_ilog_cache, int());
  MOCK_METHOD1(set_zone_priority, void(const int64_t zone_priority));
  MOCK_METHOD1(set_region, int(const common::ObRegion &region));
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObPartitionService);
};

}
}

#endif // OCEANBASE_STORAGE_OB_PARTITION_SERVICE
