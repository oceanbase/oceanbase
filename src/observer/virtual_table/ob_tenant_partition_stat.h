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

#ifndef OCEANBASE_VIRTUAL_TABLE_OB_TENANT_PARTITION_STAT_H_
#define OCEANBASE_VIRTUAL_TABLE_OB_TENANT_PARTITION_STAT_H_

#include "share/ob_virtual_table_projector.h"
#include "share/partition_table/ob_partition_table_iterator.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObTableSchema;
class ObColumnSchemaV2;
class ObMultiVersionSchemaService;
}  // namespace schema
}  // namespace share
namespace observer {
class ObTenantPartitionStat : public common::ObVirtualTableProjector {
public:
  ObTenantPartitionStat();
  virtual ~ObTenantPartitionStat();

  int init(share::ObPartitionTableOperator& pt_operator, share::schema::ObMultiVersionSchemaService& schema_service,
      const uint64_t tenant_id);

  virtual int inner_get_next_row(common::ObNewRow*& row);

private:
  struct PartitionStat {
    PartitionStat();
    void reset();
    bool is_valid() const;

    TO_STRING_KV(K_(table_id), K_(partition_id), K_(partition_cnt), K_(row_count), K_(diff_percentage));

    uint64_t table_id_;
    int64_t partition_id_;
    int64_t partition_cnt_;
    int64_t row_count_;
    int64_t diff_percentage_;
  };

  static int get_leader_replica(
      const share::ObPartitionInfo& partition_info, const share::ObPartitionReplica*& leader_replica);

  int get_next_partition_stat(PartitionStat& partition_stat);
  int gen_next_table_stat();
  int build_table_stat(const common::ObIArray<share::ObPartitionInfo>& partition_infos);
  int get_full_row(const share::schema::ObTableSchema* table, const PartitionStat& partition_stat,
      common::ObIArray<Column>& columns);

  bool inited_;
  share::ObTenantPartitionIterator partition_iter_;
  const share::schema::ObTableSchema* table_schema_;
  uint64_t table_id_;
  common::ObArray<PartitionStat> partition_stats_;
  int64_t partition_idx_;
  share::ObPartitionInfo prefetch_info_;
  bool tenant_end_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantPartitionStat);
};

}  // end namespace observer
}  // end namespace oceanbase

#endif  // OCEANBASE_VIRTUAL_TABLE_OB_TENANT_PARTITION_STAT_H_
