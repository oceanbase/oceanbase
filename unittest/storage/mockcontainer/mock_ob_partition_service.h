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

#ifndef MOCK_OB_PARTITION_SERVICE_H_
#define MOCK_OB_PARTITION_SERVICE_H_
#define private public

#undef private
#undef protected
#include <gmock/gmock.h>
#define private public
#define protected public

#include "ob_partition_service.h"

#include "share/ob_rpc_struct.h"
#include "share/ob_server_locality_cache.h"
#include "common/storage/ob_freeze_define.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_service.h"
#include "sql/ob_end_trans_callback.h"

namespace oceanbase
{
using namespace blocksstable;
namespace storage
{
class MockObIPartitionService : public ObPartitionService
{
public:
  MOCK_METHOD0(reload_config,
               int());
  MOCK_METHOD1(internal_leader_revoke,
               int(const ObCbTask &revoke_task));
  MOCK_METHOD1(internal_leader_takeover,
               int(const ObCbTask &takeover_task));
  MOCK_METHOD1(internal_leader_active,
               int(const ObCbTask &active_task));
  MOCK_METHOD1(push_callback_task,
               int(const ObCbTask &task));
  MOCK_METHOD1(activate_tenant,
               int(const uint64_t tenant_id));
  MOCK_METHOD1(inactivate_tenant,
               int(const uint64_t tenant_id));

  MOCK_METHOD0(force_refresh_locality_info, int());


  MOCK_METHOD5(init,
               int(const blocksstable::ObStorageEnv &env, const common::ObAddr &self_addr,
                   share::schema::ObMultiVersionSchemaService *schema_service,
                   share::ObRsMgr *rs_mgr, rpc::frame::ObReqTransport *req_transport));

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
  // MOCK_METHOD5(start_stmt,
  //              int(const uint64_t tenant_id, const transaction::ObStmtDesc &stmt_desc,
  //                  transaction::ObTransDesc &trans_desc, const int64_t expired_time,
  //                  const common::ObPartitionArray &participants));
  // MOCK_METHOD5(start_stmt,
  //              int(const transaction::ObStmtParam &stmt_param, const transaction::ObStmtDesc &stmt_desc,
  //                  transaction::ObTransDesc &trans_desc,
  //                  const common::ObPartitionLeaderArray &pla, common::ObPartitionArray &participants));
  // MOCK_METHOD5(end_stmt,
  //              int(bool is_rollback, const ObPartitionArray &cur_stmt_all_participants,
  //              const transaction::ObPartitionEpochArray &epoch_arr,
  //              const ObPartitionArray &discard_participants, transaction::ObTransDesc &trans_desc));
  // MOCK_METHOD3(start_participant,
  //     int(transaction::ObTransDesc &trans_desc,
  //     const common::ObPartitionArray &participants,
  //     transaction::ObPartitionEpochArray &partition_epoch_arr));
  // MOCK_METHOD4(end_participant, int(bool is_rollback,
  //                                   transaction::ObTransDesc &trans_desc,
  //                                   const common::ObPartitionArray &participants,
  //                                   transaction::ObEndParticipantsRes &res));
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

  MOCK_CONST_METHOD0(is_empty,
                     bool());
  MOCK_METHOD0(get_trans_service,
               transaction::ObTransService * ());
  MOCK_METHOD2(sync_frozen_status,
               int(const ObFrozenStatus &frozen_status,
                   const bool &force));
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
};

}  // namespace storage
}  // namespace oceanbase
#endif /* MOCK_OB_PARTITION_SERVICE_H_ */
