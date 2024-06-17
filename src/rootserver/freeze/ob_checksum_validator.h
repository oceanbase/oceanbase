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

#ifndef OCEANBASE_ROOTSERVER_FREEZE_OB_CHECKSUM_VALIDATOR_H_
#define OCEANBASE_ROOTSERVER_FREEZE_OB_CHECKSUM_VALIDATOR_H_

#include "share/ob_tablet_checksum_iterator.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/ob_zone_merge_info.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "rootserver/freeze/ob_major_freeze_util.h"
#include "share/compaction/ob_table_ckm_items.h"
#include "rootserver/freeze/ob_major_merge_progress_util.h"

namespace oceanbase
{
namespace share
{
class ObTabletChecksumItem;
}
namespace rootserver
{
class ObZoneMergeManager;
class ObServerManager;
struct ObFTSGroupArray;
struct ObFTSGroup;
struct ObFTSIndexInfo;
struct ObReplicaCkmItems
{
  ObReplicaCkmItems()
    : array_(),
      tablet_cnt_(0)
  {}
  DELEGATE_WITH_RET(array_, empty, bool);
  DELEGATE_WITH_RET(array_, count, int64_t);
  DELEGATE_WITH_RET(array_, at, const share::ObTabletReplicaChecksumItem&);
  void reuse()
  {
    array_.reuse();
    tablet_cnt_ = 0;
  }
  TO_STRING_KV(K_(array), K_(tablet_cnt));
  ObArray<share::ObTabletReplicaChecksumItem> array_;
  int64_t tablet_cnt_;
};

class ObChecksumValidator
{
public:
  ObChecksumValidator(
    const uint64_t tenant_id,
    volatile bool &stop,
    const compaction::ObTabletLSPairCache &tablet_ls_pair_cache,
    const compaction::ObTabletStatusMap &tablet_status_map,
    compaction::ObTableCompactionInfoMap &table_compaction_map,
    compaction::ObIndexCkmValidatePairArray &idx_ckm_validate_array,
    compaction::ObCkmValidatorStatistics &statistics,
    ObArray<share::ObTabletLSPair> &finish_tablet_ls_pair_array,
    ObArray<share::ObTabletChecksumItem> &finish_tablet_ckm_array,
    compaction::ObUncompactInfo &uncompact_info,
    ObFTSGroupArray &fts_group_array)
    : is_inited_(false),
      is_primary_service_(false),
      need_validate_index_ckm_(false),
      need_validate_cross_cluster_ckm_(false),
      cross_cluster_ckm_sync_finish_(false),
      stop_(stop),
      tenant_id_(tenant_id),
      expected_epoch_(OB_INVALID_ID),
      table_id_(OB_INVALID_ID),
      compaction_scn_(),
      major_merge_start_us_(0),
      statistics_(statistics),
      sql_proxy_(nullptr),
      tablet_ls_pair_cache_(tablet_ls_pair_cache),
      tablet_status_map_(tablet_status_map),
      table_compaction_map_(table_compaction_map),
      idx_ckm_validate_array_(idx_ckm_validate_array),
      finish_tablet_ls_pair_array_(finish_tablet_ls_pair_array),
      finish_tablet_ckm_array_(finish_tablet_ckm_array),
      uncompact_info_(uncompact_info),
      fts_group_array_(fts_group_array),
      schema_guard_(nullptr),
      simple_schema_(nullptr),
      table_compaction_info_(),
      replica_ckm_items_(),
      last_table_ckm_items_(tenant_id)
  {}
  ~ObChecksumValidator() {}
  int init(
    const bool is_primary_service,
    ObMySQLProxy &sql_proxy);

  int set_basic_info(
    const share::SCN &frozen_scn,
    const int64_t expected_epoch);
  const compaction::ObTableCompactionInfo &get_table_compaction_info() const
  {
    return table_compaction_info_;
  }
  int validate_checksum(
    const uint64_t table_id,
    share::schema::ObSchemaGetterGuard &schema_guard);
  int deal_with_special_table_at_last(bool &finish_validate);
  void clear_cached_info();
  void clear_array_index()
  {
    last_table_ckm_items_.clear();
  }
  int push_tablet_ckm_items_with_update(
    const ObIArray<share::ObTabletReplicaChecksumItem> &replica_ckm_items);
  int push_finish_tablet_ls_pairs_with_update(
    const common::ObIArray<share::ObTabletLSPair> &tablet_ls_pairs);
  int batch_write_tablet_ckm();
  int batch_update_report_scn();
  int handle_fts_checksum(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const ObFTSGroupArray &fts_group_array);
  static const int64_t SPECIAL_TABLE_ID = 1;
  TO_STRING_KV(K_(tenant_id), K_(is_primary_service), K_(table_id), K_(compaction_scn));
private:
  int check_inner_status();
  int get_table_compaction_info(const uint64_t table_id, compaction::ObTableCompactionInfo &table_compaction_info);
  int set_need_validate();
  int get_tablet_ls_pairs(const share::schema::ObSimpleTableSchemaV2 &simple_schema);
  int get_replica_ckm(const bool include_larger_than = false);
  /* Tablet Replica Checksum Section */
  int validate_tablet_replica_checksum();
  // check table compaction info according to tablet_status_map
  int update_table_compaction_info_by_tablet();
  int get_tablet_replica_checksum_and_validate(const bool include_larger_than);
  int verify_tablet_replica_checksum();

  /* Index Checksum Section */
  int validate_index_checksum();
  int handle_index_table(const share::schema::ObSimpleTableSchemaV2 &index_simple_schema);
  int verify_table_index(
    const share::schema::ObSimpleTableSchemaV2 &index_simple_schema,
    compaction::ObTableCompactionInfo &data_compaction_info,
    compaction::ObTableCompactionInfo &index_compaction_info);

  /* Cross Cluster Checksum Section */
  int validate_cross_cluster_checksum();
  int check_tablet_checksum_sync_finish(const bool force_check);
  int validate_replica_and_tablet_checksum();
  int check_column_checksum(
    const ObArray<share::ObTabletReplicaChecksumItem> &tablet_replica_checksum_items,
    const ObArray<share::ObTabletChecksumItem> &tablet_checksum_items);
  bool check_waiting_tablet_checksum_timeout() const;
  int try_update_tablet_checksum_items();
  /* FTS Checksum Section */
  int validate_rowkey_doc_indexs(const ObFTSGroup &fts_group, ObIArray<int64_t> &finish_table_ids);
  int validate_fts_indexs(const ObFTSIndexInfo &index_info, ObIArray<int64_t> &finish_table_ids);
  int build_ckm_item_for_fts(
    const int64_t table_id,
    compaction::ObTableCkmItems &data_table_ckm,
    ObIArray<int64_t> &finish_table_ids);
  int finish_verify_fts_ckm(const int64_t table_id);
  static const int64_t PRINT_CROSS_CLUSTER_LOG_INVERVAL = 10 * 60 * 1000 * 1000; // 10 mins
  static const int64_t MAX_TABLET_CHECKSUM_WAIT_TIME_US = 36 * 3600 * 1000 * 1000L;  // 36 hours
  static const int64_t MAX_BATCH_INSERT_COUNT = 1500;
  bool is_inited_;
  bool is_primary_service_;
  bool need_validate_index_ckm_;
  bool need_validate_cross_cluster_ckm_;
  bool cross_cluster_ckm_sync_finish_;
  volatile bool &stop_;
  uint64_t tenant_id_;
  int64_t expected_epoch_;
  uint64_t table_id_;
  share::SCN compaction_scn_;
  int64_t major_merge_start_us_;
  compaction::ObCkmValidatorStatistics &statistics_;
  common::ObMySQLProxy *sql_proxy_;
  /* reference to obj in PorgressChecker */
  const compaction::ObTabletLSPairCache &tablet_ls_pair_cache_;
  const compaction::ObTabletStatusMap &tablet_status_map_;
  compaction::ObTableCompactionInfoMap &table_compaction_map_;
  compaction::ObIndexCkmValidatePairArray &idx_ckm_validate_array_;
  ObArray<share::ObTabletLSPair> &finish_tablet_ls_pair_array_;
  ObArray<share::ObTabletChecksumItem> &finish_tablet_ckm_array_;
  compaction::ObUncompactInfo &uncompact_info_;
  ObFTSGroupArray &fts_group_array_;

  /* different for every table */
  share::schema::ObSchemaGetterGuard *schema_guard_;
  const share::schema::ObSimpleTableSchemaV2 *simple_schema_;
  compaction::ObTableCompactionInfo table_compaction_info_;
  ObArray<share::ObTabletLSPair> cur_tablet_ls_pair_array_;
  ObReplicaCkmItems replica_ckm_items_;
  compaction::ObTableCkmItems last_table_ckm_items_; // only cached last data table with index
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_FREEZE_OB_CHECKSUM_VALIDATOR_H_
