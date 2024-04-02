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
#include "share/ob_tablet_replica_checksum_iterator.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/ob_zone_merge_info.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

namespace oceanbase
{
namespace share
{
class ObTabletChecksumItem;
}
namespace rootserver
{
class ObZoneMergeManager;
class ObFreezeInfoManager;
struct ObMergeTimeStatistics;

// ensure access in single thread, thus use hash::NoPthreadDefendMode to decrease memory usage of buckets
typedef common::hash::ObHashMap<uint64_t, share::ObTableCompactionInfo, common::hash::NoPthreadDefendMode> TableCompactionMap;
typedef common::hash::ObHashMap<share::ObTabletLSPair, share::ObTabletCompactionStatus, common::hash::NoPthreadDefendMode> TabletCompactionMap;

class ObMergeErrorCallback
{
public:
  ObMergeErrorCallback()
    : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID),
      zone_merge_mgr_(nullptr)
  {}
  virtual ~ObMergeErrorCallback() {}

  int init(const uint64_t tenant_id, ObZoneMergeManager &zone_merge_mgr);

  int handle_merge_error(const int64_t error_type, const int64_t expected_epoch);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObZoneMergeManager *zone_merge_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObMergeErrorCallback);
};

class ObChecksumValidatorBase
{
public:
  ObChecksumValidatorBase()
    : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), is_primary_service_(true),
      need_validate_(false), sql_proxy_(NULL), zone_merge_mgr_(NULL), merge_err_cb_(),
      tablet_checksum_items_(), tablet_ls_pairs_(), table_ids_()
  {}
  virtual ~ObChecksumValidatorBase() {}
  virtual int init(const uint64_t tenant_id,
                   const bool is_primary_service,
                   common::ObMySQLProxy &sql_proxy,
                   ObZoneMergeManager &zone_merge_mgr);
  void set_need_validate(const bool need_validate) { need_validate_ = need_validate; }
  bool need_validate() const { return need_validate_; }
  int validate_checksum(const volatile bool &stop,
                        const share::SCN &frozen_scn,
                        const TabletCompactionMap &tablet_compaction_map,
                        int64_t &table_count,
                        TableCompactionMap &table_compaction_map,
                        const common::ObIArray<uint64_t> &ori_table_ids,
                        ObMergeTimeStatistics &merge_time_statistics,
                        const int64_t expected_epoch);
  // sync data from __all_tablet_replica_checksum to __all_tablet_checksum at table granularity
  int write_tablet_checksum_at_table_level(const volatile bool &stop,
                                           const ObArray<share::ObTabletLSPair> &pairs,
                                           const share::SCN &frozen_scn,
                                           const share::ObTableCompactionInfo &table_compaction_info,
                                           const uint64_t table_id,
                                           const int64_t expected_epoch);
  void reuse_tablet_checksum_items();
  void reuse_tablet_ls_pairs();
  void reuse_table_ids();
  int add_tablet_checksum_items(const ObIArray<share::ObTabletChecksumItem> &tablet_checksum_items);
  int add_tablet_ls_pairs(const ObIArray<share::ObTabletLSPair> &tablet_ls_pairs);
  int add_table_id(const uint64_t table_id);
  int batch_update_table_status(TableCompactionMap &table_compaction_map,
                                const share::ObTableCompactionInfo::Status &status);
  int batch_write_ckm_and_update_report_scn(const volatile bool &stop,
                                            const share::SCN &frozen_scn,
                                            const int64_t expected_epoch,
                                            ObMergeTimeStatistics &merge_time_statistics,
                                            TableCompactionMap &table_compaction_map);

  static const int64_t MIN_CHECK_INTERVAL = 10 * 1000 * 1000LL;
  // table_id of the table containing first tablet in sys ls
  static const uint64_t MAJOR_MERGE_SPECIAL_TABLE_ID = share::OB_ALL_CORE_TABLE_TID;
  // write_ckm_and_update_report_scn batch size.
  // keep UPDATE_BATCH_CNT <= ObTabletChecksumOperator::MAX_BATCH_COUNT, so as to avoid bad case.
  // e.g., if UPDATE_BATCH_CNT = 100 and ObTabletChecksumOperator::MAX_BATCH_COUNT = 99, then
  // each 100 tablet_checksum_items updates would be split into two batch (i.e., 99 + 1)
  static const int64_t UPDATE_BATCH_CNT = 90;

protected:
  int check_exist_in_table_set(const uint64_t table_id,
                               const hash::ObHashSet<uint64_t, hash::NoPthreadDefendMode> &table_id_set,
                               bool &is_exist) const;
  int get_table_compaction_info(const share::schema::ObSimpleTableSchemaV2 &simple_schema,
                                TableCompactionMap &table_compaction_map,
                                share::ObTableCompactionInfo &table_compaction_info);
  int convert_array_to_set(const ObIArray<uint64_t> &table_ids,
                           hash::ObHashSet<uint64_t, hash::NoPthreadDefendMode> &table_id_set);
  // compare 'table_compaction_map' with 'table_ids', and then remove those
  // whose table_id does not exist in 'table_ids' from 'table_compaction_map'.
  // because tables may be dropped during major freeze, and we should skip these dropped tables.
  int remove_not_exist_table(const ObArray<uint64_t> &table_ids,
                             TableCompactionMap &table_compaction_map);
  // infos of one table's tablets in __all_tablet_to_ls/__all_tablet_replica_checksum/__all_tablet_checksum
  // will change in case of truncate table (delete old tablets, create new tablets). when one
  // table's tablets chanage in these three inner tables, we directly mark this table as verified.
  int handle_table_can_not_verify(const uint64_t table_id,
                                  TableCompactionMap &table_compaction_map);
  // If one table finished all kinds of checksum verification, then
  // 1. write tablet checksum
  // 2. update report_scn
  // 3. mark it as VERIFIED
  int write_ckm_and_update_report_scn(const volatile bool &stop,
                                      const share::schema::ObSimpleTableSchemaV2 *simple_schema,
                                      const share::SCN &frozen_scn,
                                      TableCompactionMap &table_compaction_map,
                                      ObMergeTimeStatistics &merge_time_statistics,
                                      const int64_t expected_epoch);
  int contains_first_tablet_in_sys_ls(const ObArray<share::ObTabletLSPair> &pairs,
                                      bool &is_containing) const;
  int try_update_tablet_checksum_items(const volatile bool &stop,
                                       const ObArray<share::ObTabletLSPair> &pairs,
                                       const share::SCN &frozen_scn,
                                       const int64_t expected_epoch);
  bool is_first_tablet_in_sys_ls(const share::ObTabletReplicaChecksumItem &item) const;

private:
  virtual int check_all_table_verification_finished(const volatile bool &stop,
                                                    const share::SCN &frozen_scn,
                                                    const TabletCompactionMap &tablet_compaction_map,
                                                    int64_t &table_count,
                                                    TableCompactionMap &table_compaction_map,
                                                    const common::ObIArray<uint64_t> &ori_table_ids,
                                                    ObMergeTimeStatistics &merge_time_statistics,
                                                    const int64_t expected_epoch) = 0;
  int revert_unverified_table_status(TableCompactionMap &table_compaction_map);

protected:
  bool is_inited_;
  uint64_t tenant_id_;
  bool is_primary_service_;  // identify ObMajorFreezeServiceType::SERVICE_TYPE_PRIMARY
  bool need_validate_;
  common::ObMySQLProxy *sql_proxy_;
  ObZoneMergeManager *zone_merge_mgr_;
  ObMergeErrorCallback merge_err_cb_;

private:
  // in order to write_ckm_and_update_report_scn in batch, save relative data in
  // @tablet_checksum_items_, @tablet_ls_pairs_ and @table_ids_
  ObArray<share::ObTabletChecksumItem> tablet_checksum_items_;
  ObArray<share::ObTabletLSPair> tablet_ls_pairs_;
  ObArray<uint64_t> table_ids_;
};

// Mainly to verify checksum between each tablet replicas in primary/standby cluster
class ObTabletChecksumValidator : public ObChecksumValidatorBase
{
public:
  ObTabletChecksumValidator() {}
  virtual ~ObTabletChecksumValidator() {}

private:
  // each table has tablets should finish tablet replica checksum verification.
  // those tables has no tablets just skip verification.
  virtual int check_all_table_verification_finished(const volatile bool &stop,
                                                    const share::SCN &frozen_scn,
                                                    const TabletCompactionMap &tablet_compaction_map,
                                                    int64_t &table_count,
                                                    TableCompactionMap &table_compaction_map,
                                                    const common::ObIArray<uint64_t> &ori_table_ids,
                                                    ObMergeTimeStatistics &merge_time_statistics,
                                                    const int64_t expected_epoch) override;
  // check whether all tablets of this table finished compaction or not,
  // and execute tablet replica checksum verification if this table has tablet.
  int check_table_compaction_and_validate_checksum(const share::schema::ObSimpleTableSchemaV2 &simple_schema,
                                                   const share::SCN &frozen_scn,
                                                   const TabletCompactionMap &tablet_compaction_map,
                                                   TableCompactionMap &table_compaction_map,
                                                   ObMergeTimeStatistics &merge_time_statistics);
  // check table compaction info according to tablet_compaction_map
  int check_table_compaction_info(const ObArray<ObTabletID> &tablet_ids,
                                  const ObArray<share::ObTabletLSPair> &pairs,
                                  const TabletCompactionMap &tablet_compaction_map,
                                  share::ObTableCompactionInfo &latest_compaction_info);
  int validate_tablet_replica_checksum(const share::SCN &frozen_scn,
                                       const uint64_t table_id,
                                       const ObArray<share::ObTabletLSPair> &pairs,
                                       TableCompactionMap &table_compaction_map,
                                       bool &need_update_map);
};

// Mainly to verify checksum of cross-cluster's tablet which sync from primary cluster
class ObCrossClusterTabletChecksumValidator : public ObChecksumValidatorBase
{
public:
  ObCrossClusterTabletChecksumValidator();
  virtual ~ObCrossClusterTabletChecksumValidator() {}
  int check_and_set_validate(const bool is_primary_service,
                             const share::SCN &frozen_scn);
  void set_major_merge_start_time(const int64_t major_merge_start_us)
  {
    major_merge_start_us_ = major_merge_start_us;
  }

private:
  virtual int check_all_table_verification_finished(const volatile bool &stop,
                                                    const share::SCN &frozen_scn,
                                                    const TabletCompactionMap &tablet_compaction_map,
                                                    int64_t &table_count,
                                                    TableCompactionMap &table_compaction_map,
                                                    const common::ObIArray<uint64_t> &ori_table_ids,
                                                    ObMergeTimeStatistics &merge_time_statistics,
                                                    const int64_t expected_epoch) override;
  int validate_cross_cluster_checksum(const volatile bool &stop,
                                      const share::SCN &frozen_scn,
                                      const int64_t expected_epoch,
                                      const share::schema::ObSimpleTableSchemaV2 *simple_schema,
                                      TableCompactionMap &table_compaction_map,
                                      ObMergeTimeStatistics &merge_time_statistics);
  int check_cross_cluster_checksum(const share::schema::ObSimpleTableSchemaV2 &simple_schema,
                                   const share::SCN &frozen_scn);
  int check_column_checksum(const ObArray<share::ObTabletReplicaChecksumItem> &tablet_replica_checksum_items,
                            const ObArray<share::ObTabletChecksumItem> &tablet_checksum_items);
  int check_if_all_tablet_checksum_exist(const share::SCN &frozen_scn);
  bool check_waiting_tablet_checksum_timeout() const;
  int convert_array_to_map(const ObArray<share::ObTabletChecksumItem> &tablet_checksum_items,
                           hash::ObHashMap<share::ObTabletLSPair, share::ObTabletChecksumItem> &tablet_checksum_items_map);
  int construct_valid_tablet_checksum_item(share::ObTabletChecksumItem &tablet_checksum_item);

private:
  const static int64_t MAX_BATCH_INSERT_COUNT = 100;
  // record the time when starting to major merge, used for check_waiting_tablet_checksum_timeout
  int64_t major_merge_start_us_;
  bool is_all_tablet_checksum_exist_;
};

// Mainly to verify checksum between (global and local) index table and main table
class ObIndexChecksumValidator : public ObChecksumValidatorBase
{
public:
  ObIndexChecksumValidator() : is_need_val_cross_cluster_ckm_(false) {}
  virtual ~ObIndexChecksumValidator() {}
  void check_and_set_validate(const bool is_primary_service);
  void set_need_val_cross_cluster_ckm(const bool is_need_val_cross_cluster_ckm);

private:
  // valid '<data table, index table>' pair should finish index column checksum verification, other tables just skip verification.
  virtual int check_all_table_verification_finished(const volatile bool &stop,
                                                    const share::SCN &frozen_scn,
                                                    const TabletCompactionMap &tablet_compaction_map,
                                                    int64_t &table_count,
                                                    TableCompactionMap &table_compaction_map,
                                                    const common::ObIArray<uint64_t> &ori_table_ids,
                                                    ObMergeTimeStatistics &merge_time_statistics,
                                                    const int64_t expected_epoch) override;
  void check_need_validate(const bool is_primary_service, bool &need_validate) const;
  // handle data table which has tablet and index table(s). its all index tables may finish virification or not
  // If all finished, update table status.
  int update_data_table_verified(const uint64_t table_id,
                                 const share::ObTableCompactionInfo &data_table_compaction,
                                 const share::SCN &frozen_scn,
                                 TableCompactionMap &table_compaction_map,
                                 const volatile bool &stop,
                                 const share::schema::ObSimpleTableSchemaV2 *simple_schema,
                                 ObMergeTimeStatistics &merge_time_statistics,
                                 const int64_t expected_epoch);
  // handle the table, update its all tablets' status if needed. And update its compaction_info in @table_compaction_map
  int handle_table_verification_finished(const uint64_t table_id,
                                         const share::SCN &frozen_scn,
                                         TableCompactionMap &table_compaction_map,
                                         const volatile bool &stop,
                                         const share::schema::ObSimpleTableSchemaV2 *simple_schema,
                                         ObMergeTimeStatistics &merge_time_statistics,
                                         const int64_t expected_epoch);
  // handle tables that are not index_table, including:
  // 1. data table with index: if all its index tables finished verification, mark it as INDEX_CKM_VERIFIED
  // 2. data table without index: if it finished compaction, mark it as INDEX_CKM_VERIFIED
  // 3. there may be other types of tables that are not index_table: if it finished compaction, mark it as INDEX_CKM_VERIFIED
  int handle_data_table(const volatile bool &stop,
                        const share::SCN &frozen_scn,
                        const common::ObIArray<uint64_t> &table_ids,
                        const ObIArray<const share::schema::ObSimpleTableSchemaV2 *> &table_schemas,
                        const common::ObIArray<uint64_t> &ori_table_ids,
                        TableCompactionMap &table_compaction_map,
                        ObMergeTimeStatistics &merge_time_statistics,
                        share::schema::ObSchemaGetterGuard &schema_guard,
                        const int64_t expected_epoch);
  // check data tables, return those need to be marked as INDEX_CKM_VERIFIED:
  // 1. data table with index: if all its index tables finished verification, it need to be marked as INDEX_CKM_VERIFIED
  // 2. data table without index: if it finished compaction, it need to be marked as INDEX_CKM_VERIFIED
  int check_data_table(const common::ObIArray<const share::schema::ObSimpleTableSchemaV2 *> &table_schemas,
                       TableCompactionMap &table_compaction_map,
                       const common::ObIArray<uint64_t> &ori_table_ids,
                       hash::ObHashSet<uint64_t, hash::NoPthreadDefendMode> &data_tables_to_update_set);
  // handle index tables. validate column checksum if needed, and mark index tables as INDEX_CKM_VERIFIED
  int handle_index_table(const share::SCN &frozen_scn,
                         const share::ObTableCompactionInfo &index_compaction_info,
                         const share::schema::ObSimpleTableSchemaV2 *index_simple_schema,
                         share::schema::ObSchemaGetterGuard &schema_guard,
                         TableCompactionMap &table_compaction_map,
                         const volatile bool &stop,
                         ObMergeTimeStatistics &merge_time_statistics,
                         const int64_t expected_epoch);
  // This function is specially designed to make it easier for troubleshooting. Moreover, this
  // function will not modify table_compaction_map, which ensures major compaction will not be
  // affected by this function.
  int try_print_first_unverified_info(const share::schema::ObSimpleTableSchemaV2 *simple_schema,
                                      const ObArray<const share::schema::ObSimpleTableSchemaV2 *> &table_schemas,
                                      const TableCompactionMap &table_compaction_map,
                                      bool &already_print);

private:
  // Let index_validator to know if need validate cross cluster checksum. If no need to validate
  // cross cluster checksum, directly write tablet checksum and update report_scn after validate
  // index checksum. So as to avoid repeated checksum verification as much as possible in case of
  // switch leader.
  bool is_need_val_cross_cluster_ckm_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_FREEZE_OB_CHECKSUM_VALIDATOR_H_
