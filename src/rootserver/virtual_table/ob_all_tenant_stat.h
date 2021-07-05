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

#ifndef OCEANBASE_ROOTSERVER_OB_ALL_TENANT_STAT_H_
#define OCEANBASE_ROOTSERVER_OB_ALL_TENANT_STAT_H_

#include "share/ob_define.h"
#include "share/ob_virtual_table_projector.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
namespace sqlclient {
class ObMySQLResult;
}
}  // namespace common
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
class ObTableSchema;
}  // namespace schema
}  // namespace share
namespace rootserver {
class ObAllTenantStat : public common::ObVirtualTableProjector {
public:
  ObAllTenantStat();
  virtual ~ObAllTenantStat();

  int init(share::schema::ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy& proxy);
  virtual int inner_get_next_row(common::ObNewRow*& row);

private:
  struct PartitionStat {
    uint64_t table_id_;
    int64_t partition_id_;
    int64_t partition_cnt_;
    int64_t occupy_size_;
    int64_t row_count_;
    int64_t major_version_;
    int64_t minor_version_;

    PartitionStat();
    ~PartitionStat()
    {}

    static int check_same_partition(const PartitionStat& lhs, const PartitionStat& rhs, bool& same_partition);
    static int cmp_version(const PartitionStat& lhs, const PartitionStat& rhs, int64_t& cmp_ret);

    // used by EXTRACT_INT_FIELD_TO_CLASS_MYSQL
    inline void set_table_id(const uint64_t table_id)
    {
      table_id_ = table_id;
    }
    inline void set_partition_id(const int64_t partition_id)
    {
      partition_id_ = partition_id;
    }
    inline void set_partition_cnt(const int64_t partition_cnt)
    {
      partition_cnt_ = partition_cnt;
    }
    inline void set_occupy_size(const int64_t occupy_size)
    {
      occupy_size_ = occupy_size;
    }
    inline void set_row_count(const int64_t row_count)
    {
      row_count_ = row_count;
    }
    inline void set_major_version(const int64_t major_version)
    {
      major_version_ = major_version;
    }
    inline void set_minor_version(const int64_t minor_version)
    {
      minor_version_ = minor_version;
    }

    bool is_valid() const;
    void reset();

    TO_STRING_KV(K_(table_id), K_(partition_id), K_(partition_cnt), K_(occupy_size), K_(row_count), K_(major_version),
        K_(minor_version));
  };

  struct ComparePartition {
    ComparePartition()
    {}
    ~ComparePartition()
    {}
    bool operator()(const PartitionStat& l, const PartitionStat& h);
  };

  struct TenantStat {
    uint64_t tenant_id_;
    int64_t table_count_;
    int64_t row_count_;
    int64_t total_size_;

    TenantStat();
    ~TenantStat()
    {}

    bool is_valid() const;
    void reset();
    int add_partition_stat(const PartitionStat& partition_stat);

    TO_STRING_KV(K_(tenant_id), K_(table_count), K_(row_count), K_(total_size));
  };

  virtual int get_all_tenant_stats(common::ObIArray<TenantStat>& tenant_stats);
  virtual int get_all_partition_stats(common::ObIArray<PartitionStat>& partition_stats);
  virtual int retrieve_partition_stats(
      common::sqlclient::ObMySQLResult& result, common::ObIArray<PartitionStat>& partition_stats);
  virtual int fill_partition_stat(const common::sqlclient::ObMySQLResult& result, PartitionStat& partition_stat);
  int get_full_row(
      const share::schema::ObTableSchema* table, const TenantStat& tenant_stat, common::ObIArray<Column>& columns);

private:
  bool inited_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  common::ObMySQLProxy* proxy_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllTenantStat);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_ALL_TENANT_STAT_H_
