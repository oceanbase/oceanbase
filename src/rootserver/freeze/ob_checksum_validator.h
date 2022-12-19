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

namespace oceanbase
{
namespace rootserver
{
class ObZoneMergeManager;
class ObFreezeInfoManager;
class ObServerManager;

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
    : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID),
      sql_proxy_(NULL), zone_merge_mgr_(NULL), merge_err_cb_()
  {}
  virtual ~ObChecksumValidatorBase() {}
  virtual int init(const uint64_t tenant_id,
                   common::ObMySQLProxy &sql_proxy,
                   ObZoneMergeManager &zone_merge_mgr);
  virtual bool need_validate() const { return false; }

  bool is_primary_cluster() const;
  bool is_standby_cluster() const;

  static const int64_t MIN_CHECK_INTERVAL = 10 * 1000 * 1000LL;

protected:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  ObZoneMergeManager *zone_merge_mgr_;
  ObMergeErrorCallback merge_err_cb_;
};

// Mainly to verify checksum of cross-cluster's tablet which sync from primary cluster
class ObCrossClusterTableteChecksumValidator : public ObChecksumValidatorBase
{
public:
  ObCrossClusterTableteChecksumValidator() {}
  virtual ~ObCrossClusterTableteChecksumValidator() {}

public:
  int validate_checksum(const share::SCN &frozen_scn);
  virtual bool need_validate() const override;

  // sync data from __all_tablet_replica_checksum to __all_tablet_checksum
  int write_tablet_checksum_item();

private:
  int check_cross_cluster_checksum(const share::SCN &frozen_scn);

private:
  bool is_first_tablet_in_sys_ls(const share::ObTabletReplicaChecksumItem &item) const;

private:
  const static int64_t MAX_BATCH_INSERT_COUNT = 100;
};

// Mainly to verify checksum between (global and local) index table and main table
class ObIndexChecksumValidator : public ObChecksumValidatorBase
{
public:
  ObIndexChecksumValidator() {}
  virtual ~ObIndexChecksumValidator() {}

public:
  int validate_checksum(const share::SCN &frozen_scn,
                        const hash::ObHashMap<share::ObTabletLSPair, share::ObTabletCompactionStatus> &tablet_compaction_map,
                        int64_t &table_count,
                        hash::ObHashMap<uint64_t, share::ObTableCompactionInfo> &table_compaction_map);
  virtual bool need_validate() const override;

private:
  // valid '<data table, index table>' pair should finish index column checksum verification, other tables just skip verification.
  int check_all_table_verification_finished(const share::SCN &frozen_scn,
                                            const hash::ObHashMap<share::ObTabletLSPair, share::ObTabletCompactionStatus> &tablet_compaction_map,
                                            int64_t &table_count,
                                            hash::ObHashMap<uint64_t, share::ObTableCompactionInfo> &table_compaction_map);
  int check_table_compaction_finished(const share::schema::ObTableSchema &table_schema,
                                      const share::SCN &frozen_scn,
                                      const hash::ObHashMap<share::ObTabletLSPair, share::ObTabletCompactionStatus> &tablet_compaction_map,
                                      hash::ObHashMap<uint64_t, share::ObTableCompactionInfo> &table_compaction_map,
                                      share::ObTableCompactionInfo &latest_compaction_info);
  // handle data table which has tablet and index table(s). its all index tables may finish virification or not
  // If all finished, update tablet status.
  int update_data_table_verified(const int64_t table_id,
                                 const share::ObTableCompactionInfo &data_table_compaction,
                                 const share::SCN &frozen_scn,
                                 hash::ObHashMap<uint64_t, share::ObTableCompactionInfo> &table_compaction_map);
  // handle the table, update its all tablets' status if needed. And update its compaction_info in @table_compaction_map
  int handle_table_compaction_finished(const share::schema::ObTableSchema *table_schema,
                                       const share::SCN &frozen_scn,
                                       hash::ObHashMap<uint64_t, share::ObTableCompactionInfo> &table_compaction_map);
  bool is_index_table(const share::schema::ObSimpleTableSchemaV2 &simple_schema);
  bool exist_in_table_array(const uint64_t table_id, const common::ObIArray<uint64_t> &table_ids);
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_FREEZE_OB_CHECKSUM_VALIDATOR_H_
