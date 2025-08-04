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
#include "share/compaction/ob_schedule_batch_size_mgr.h"
#include "rootserver/freeze/ob_fts_checksum_validate_util.h"

namespace oceanbase
{
namespace share
{
class ObIServerTrace;
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
typedef common::hash::ObHashMap<ObTabletID, compaction::ObTabletCompactionStatusEnum> ObTabletStatusMap;


class ObBasicMergeProgressChecker
{
public:
  ObBasicMergeProgressChecker() = default;
  virtual ~ObBasicMergeProgressChecker() {}

  virtual int init(
      const bool is_primary_service,
      common::ObMySQLProxy &sql_proxy,
      share::schema::ObMultiVersionSchemaService &schema_service,
      share::ObIServerTrace &server_trace,
      ObMajorMergeInfoManager &merge_info_mgr) = 0;
  virtual int set_basic_info(
      const share::ObFreezeInfo &freeze_info,
      const int64_t expected_epoch) = 0;
  virtual int clear_cached_info() = 0;
  virtual int check_progress() = 0;
  virtual void reset_uncompacted_tablets() {};
  virtual int get_uncompacted_tablets(
    common::ObArray<share::ObTabletReplica> &uncompacted_tablets,
    common::ObArray<uint64_t> &uncompacted_table_ids) const
  {
    UNUSEDx(uncompacted_tablets, uncompacted_table_ids);
    return OB_SUCCESS;
  }
  virtual const compaction::ObBasicMergeProgress &get_merge_progress() const = 0;
};


class ObMajorMergeProgressChecker : public ObBasicMergeProgressChecker
{
public:
  ObMajorMergeProgressChecker(
    const uint64_t tenant_id,
    volatile bool &stop);
  virtual ~ObMajorMergeProgressChecker() {}

  virtual int init(
      const bool is_primary_service,
      common::ObMySQLProxy &sql_proxy,
      share::schema::ObMultiVersionSchemaService &schema_service,
      share::ObIServerTrace &server_trace,
      ObMajorMergeInfoManager &merge_info_mgr) override;

  virtual int set_basic_info(
    const share::ObFreezeInfo &freeze_info,
    const int64_t expected_epoch) override; // For each round major_freeze, need invoke this once.
  virtual int clear_cached_info() override;
  virtual int get_uncompacted_tablets(
    common::ObArray<share::ObTabletReplica> &uncompacted_tablets,
    common::ObArray<uint64_t> &uncompacted_table_ids) const override;
  OB_INLINE virtual void reset_uncompacted_tablets() override { uncompact_info_.reset(); }
  virtual int check_progress() override;
  const compaction::ObBasicMergeProgress &get_merge_progress() const override { return progress_; }
  const compaction::ObTabletLSPairCache &get_tablet_ls_pair_cache() const { return tablet_ls_pair_cache_; }
private:
  int set_table_compaction_info_status(const uint64_t table_id, const compaction::ObTableCompactionInfo::Status status);

  void deal_with_unfinish_table_ids(
    const int error_no,
    ObIArray<uint64_t> &unfinish_table_id_array);
  bool can_not_ignore_warning(int ret)
  {
    return OB_FREEZE_SERVICE_EPOCH_MISMATCH == ret || OB_CHECKSUM_ERROR == ret;
  }
  const static int64_t TABLET_ID_BATCH_CHECK_SIZE = 10000;
  const static int64_t TABLE_ID_BATCH_CHECK_SIZE = 200;
  const static int64_t TABLE_MAP_BUCKET_CNT = 10000;
  const static int64_t DEFAULT_ARRAY_CNT = 200;
  int generate_tablet_status_map();
  int check_verification(
    ObSchemaGetterGuard &schema_guard,
    ObIArray<uint64_t> &unfinish_table_id_array);
  int prepare_unfinish_table_ids();
  int check_schema_version();
  int prepare_check_progress(
    compaction::ObRSCompactionTimeGuard &tmp_time_guard,
    bool &exist_uncompacted_table);
  int check_index_and_rest_table();
  int validate_index_ckm();
  int get_idx_ckm_and_validate(
    const uint64_t index_table_id,
    ObSchemaGetterGuard &schema_guard,
    compaction::ObTableCkmItems &data_table_ckm);
  int loop_index_ckm_validate_array();
  int update_finish_index_cnt_for_data_table(
    const uint64_t data_table_id,
    const int64_t finish_index_cnt,
    bool &idx_validate_finish);
  int deal_with_validated_table(
    const uint64_t data_table_id,
    const int64_t finish_index_cnt,
    const compaction::ObTableCkmItems &data_table_ckm);
  int rebuild_table_compaction_map(const int64_t table_id_count);
  bool should_ignore_cur_table(const ObSimpleTableSchemaV2 *simple_schema);
  int deal_with_rest_data_table();
  bool is_extra_check_round() const { return 0 == (loop_cnt_ % 8); } // check every 8 rounds
  void print_unfinish_info(const int64_t cost_us);
  OB_INLINE int get_table_and_index_schema(
    ObSchemaGetterGuard &schema_guard,
    const uint64_t table_id,
    bool &is_table_valid,
    ObIArray<const ObSimpleTableSchemaV2 *> &index_schemas);
  int rebuild_tablet_status_map();
  int prepare_fts_group(
    const int64_t table_id,
    const ObIArray<const ObSimpleTableSchemaV2 *> &index_schemas);
  int handle_fts_checksum();
  share::SCN get_compaction_scn() const { return freeze_info_.frozen_scn_; }
  int64_t get_compaction_scn_val() const { return get_compaction_scn().get_val_for_tx(); }
private:
  static const int64_t ADD_RS_EVENT_INTERVAL = 10L * 60 * 1000 * 1000; // 10m
  static const int64_t DEAL_REST_TABLE_CNT_THRESHOLD = 100;
  static const int64_t DEAL_REST_TABLE_INTERVAL = 10 * 60 * 1000 * 1000L; // 10m
  static const int64_t ASSGIN_FAILURE_RETRY_TIMES = 10;
private:
  bool is_inited_;
  bool first_loop_in_cur_round_;
  volatile bool &stop_;
  uint8_t loop_cnt_;
  int last_errno_;
  uint64_t tenant_id_;
  share::ObFreezeInfo freeze_info_;
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
  // record each table compaction/verify status
  compaction::ObTableCompactionInfoMap table_compaction_map_; // <table_id, compaction_info>
  ObFTSGroupArray fts_group_array_;
  share::ObCompactionLocalityCache ls_locality_cache_;
  ObChecksumValidator ckm_validator_;
  compaction::ObUncompactInfo uncompact_info_;
  // cache of ls_infos in __all_ls_meta_table
  // statistics section
  compaction::ObRSCompactionTimeGuard total_time_guard_;
  compaction::ObCkmValidatorStatistics validator_statistics_;
  compaction::ObTabletLSPairCache tablet_ls_pair_cache_;
  compaction::ObScheduleBatchSizeMgr batch_size_mgr_;
  ObArray<share::ObTabletLSPair> finish_tablet_ls_pair_array_;
  ObArray<share::ObTabletChecksumItem> finish_tablet_ckm_array_;
  DISALLOW_COPY_AND_ASSIGN(ObMajorMergeProgressChecker);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_FREEZE_OB_MAJOR_MERGE_PROGRESS_CHECKER_
