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

#ifndef OCEANBASE_ROOTSERVER_FREEZE_OB_MAJOR_MERGE_PROGRESS_CHECKER_
#define OCEANBASE_ROOTSERVER_FREEZE_OB_MAJOR_MERGE_PROGRESS_CHECKER_

#include "share/ob_zone_merge_info.h"
#include "share/tablet/ob_tablet_info.h"
#include "share/compaction/ob_compaction_locality_cache.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/freeze/ob_checksum_validator.h"
#include "common/ob_tablet_id.h"
#include "rootserver/freeze/ob_major_freeze_util.h"
#include "rootserver/freeze/ob_major_merge_progress_util.h"

namespace oceanbase
{
namespace share
{
class ObIServerTrace;
class ObCompactionTabletMetaIterator;
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace compaction
{
struct ObTableCkmItems;
}
namespace common
{
class ObMySQLProxy;
}

namespace rootserver
{
class ObMajorMergeInfoManager;
typedef common::hash::ObHashMap<ObTabletID, compaction::ObTabletCompactionStatus> ObTabletStatusMap;
class ObMajorMergeProgressChecker
{
public:
  ObMajorMergeProgressChecker(
    const uint64_t tenant_id,
    volatile bool &stop);
  virtual ~ObMajorMergeProgressChecker() {}

  int init(const bool is_primary_service,
           common::ObMySQLProxy &sql_proxy,
           share::schema::ObMultiVersionSchemaService &schema_service,
           share::ObIServerTrace &server_trace,
           ObMajorMergeInfoManager &merge_info_mgr);

  int set_basic_info(
    share::SCN global_broadcast_scn,
    const int64_t expected_epoch); // For each round major_freeze, need invoke this once.
  int clear_cached_info();
  int get_uncompacted_tablets(common::ObArray<share::ObTabletReplica> &uncompacted_tablets) const;
  void reset_uncompacted_tablets();
  int check_progress(compaction::ObMergeProgress &progress);
private:
  int update_table_compaction_info(
    const uint64_t table_id,
    const common::ObFunction<void(compaction::ObTableCompactionInfo&)> &info_op,
    const bool need_update_progress = true);

  void reuse_batch_table(ObIArray<uint64_t> &unfinish_table_id_array, const bool reuse_rest_table);
  bool can_not_ignore_warning(int ret)
  {
    return OB_FREEZE_SERVICE_EPOCH_MISMATCH == ret || OB_CHECKSUM_ERROR == ret;
  }
  void get_check_batch_size(int64_t &tablet_id_batch_size, int64_t &table_id_batch_size) const;
  const static int64_t TABLET_ID_BATCH_CHECK_SIZE = 3000;
  const static int64_t TABLE_ID_BATCH_CHECK_SIZE = 200;
  const static int64_t TOTAL_TABLE_CNT_THREASHOLD = 100 * 1000; // 10w
  const static int64_t TABLE_MAP_BUCKET_CNT = 10000;
  const static int64_t DEFAULT_ARRAY_CNT = 200;
  int get_tablet_ls_pairs_by_tables(
    ObSchemaGetterGuard &schema_guard,
    compaction::ObUnfinishTableIds &table_ids,
    ObArray<share::ObTabletLSPair> &tablet_ls_pair_array);
  int generate_tablet_map_by_iter(
    share::ObCompactionTabletMetaIterator &iter);
  int check_verification(
    ObSchemaGetterGuard &schema_guard,
    ObIArray<uint64_t> &unfinish_table_id_array);
  int prepare_unfinish_table_ids();
  int check_schema_version();
  int prepare_check_progress();
  int check_index_and_rest_table();
  int validate_index_ckm();
  int get_idx_ckm_and_validate(
    const ObTableSchema &table_schema,
    const uint64_t index_table_id,
    ObSchemaGetterGuard &schema_guard,
    compaction::ObTableCkmItems &data_table_ckm);
  int loop_index_ckm_validate_array(
    ObIArray<uint64_t> &finish_validate_table_ids);
  int update_finish_index_cnt_for_data_table(
    const uint64_t data_table_id,
    const uint64_t finish_index_cnt);
  bool should_ignore_cur_table(const ObSimpleTableSchemaV2 *simple_schema);
  int deal_with_rest_data_table();
  bool is_extra_check_round() const { return 0 == (loop_cnt_ % 8); } // check every 8 rounds
  void print_unfinish_info(const int64_t cost_us);
  OB_INLINE int get_table_and_index_schema(
    ObSchemaGetterGuard &schema_guard,
    const uint64_t table_id,
    bool &is_table_valid,
    ObIArray<const ObSimpleTableSchemaV2 *> &index_schemas);
private:
  static const int64_t ADD_RS_EVENT_INTERVAL = 10L * 60 * 1000 * 1000; // 10m
  static const int64_t PRINT_LOG_INTERVAL = 2 * 60 * 1000 * 1000; // 2m
  static const int64_t DEBUG_INFO_CNT = 3;
  static const int64_t DEAL_REST_TABLE_CNT_THRESHOLD = 100;
  static const int64_t DEAL_REST_TABLE_INTERVAL = 10 * 60 * 1000 * 1000L; // 10m
private:
  bool is_inited_;
  bool first_loop_in_cur_round_;
  volatile bool &stop_;
  uint8_t loop_cnt_;
  int last_errno_;
  uint64_t tenant_id_;
  share::SCN compaction_scn_; // check merged scn
  uint64_t expected_epoch_;
  common::ObMySQLProxy *sql_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  share::ObIServerTrace *server_trace_;
  ObMajorMergeInfoManager *merge_info_mgr_;
  compaction::ObMergeProgress progress_;
  compaction::ObIndexCkmValidatePairArray idx_ckm_validate_array_;
  compaction::ObUnfinishTableIds table_ids_; // record unfinish table_id
  // record tablet whose status is COMPACTED/CAN_SKIP_VERIFYING
  compaction::ObTabletStatusMap tablet_status_map_;
  compaction::ObTabletLSPairArray tablet_ls_pair_array_;
  // record each table compaction/verify status
  compaction::ObTableCompactionInfoMap table_compaction_map_; // <table_id, compaction_info>
  ObChecksumValidator ckm_validator_;
  common::ObSEArray<share::ObTabletReplica, DEBUG_INFO_CNT> uncompacted_tablets_; // record for diagnose
  common::SpinRWLock diagnose_rw_lock_;
  // cache of ls_infos in __all_ls_meta_table
  share::ObCompactionLocalityCache ls_locality_cache_;
  // statistics section
  compaction::ObRSCompactionTimeGuard total_time_guard_;
  compaction::ObCkmValidatorStatistics validator_statistics_;
  DISALLOW_COPY_AND_ASSIGN(ObMajorMergeProgressChecker);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_FREEZE_OB_MAJOR_MERGE_PROGRESS_CHECKER_
