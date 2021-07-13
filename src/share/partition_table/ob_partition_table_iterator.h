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

#ifndef OCEANBASE_PARTITION_TABLE_OB_PARTITION_TABLE_ITERATOR_H_
#define OCEANBASE_PARTITION_TABLE_OB_PARTITION_TABLE_ITERATOR_H_

#include "lib/allocator/page_arena.h"
#include "share/schema/ob_table_iter.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "ob_replica_filter.h"

namespace oceanbase {
namespace share {

namespace schema {
class ObMultiVersionSchemaService;
class ObSimpleTableSchemaV2;
}  // namespace schema

class ObPartitionInfo;
class ObPartitionTableOperator;

class ObIPartitionTableIterator {
public:
  ObIPartitionTableIterator() : need_fetch_faillist_(false)
  {}
  virtual ~ObIPartitionTableIterator()
  {}

  virtual int next(ObPartitionInfo& partition) = 0;
  virtual void set_need_fetch_faillist(const bool need_fetch_faillist)
  {
    need_fetch_faillist_ = need_fetch_faillist;
  }
  virtual ObReplicaFilterHolder& get_filters() = 0;

protected:
  bool need_fetch_faillist_;
};

enum CheckType { INVALID_REPLICA = 0, LEGAL_REPLICA, REDUNDANT_REPLICA, LOST_REPLICA, OUTOF_SCHEMA_REPLICA };

/* due to the implementation restriction of this iterator,
 * we need to inc the offset manually
 * this iterator is used to iterate all partition_id for standlone table or binding tablegroup
 * The ObPartIdAscIterator does not check if the ObPartitionSchema is a standlone table
 * or a binding tablegroup, this need to be guaranteed by the upper level
 */
class ObPartIdAscIterator {
public:
  ObPartIdAscIterator() : sorted_part_id_array_(), part_level_(share::schema::PARTITION_LEVEL_MAX), cur_idx_(0)
  {}
  virtual ~ObPartIdAscIterator()
  {}

public:
  void reset();
  int build(const uint64_t partition_entity_id, const share::schema::ObPartitionSchema& partition_schema,
      const bool filter_dropped_schema);
  int generate_next_part_id(int64_t& part_id) const;
  bool is_iter_end() const;
  void inc_iter();
  int check_out_of_part_id_range(const int64_t partition_id, bool& out_of_range) const;

private:
  common::ObArray<int64_t> sorted_part_id_array_;
  share::schema::ObPartitionLevel part_level_;
  int64_t cur_idx_;
};

// iterator partition for one standlone table or binding tablegroup
class ObTablePartitionIterator : public ObIPartitionTableIterator {
public:
  class ObPrefetchInfo {
  public:
    friend class ObTablePartitionIterator;

  public:
    ObPrefetchInfo()
        : prefetch_idx_(0),
          table_id_(0),
          pt_operator_(NULL),
          prefetch_iter_end_(false),
          prefetch_partitions_(),
          need_fetch_faillist_(false)
    {}
    ~ObPrefetchInfo()
    {}
    void reset()
    {
      prefetch_idx_ = 0;
      prefetch_iter_end_ = false;
      prefetch_partitions_.reuse();
      need_fetch_faillist_ = false;
    }
    int init(uint64_t table_id, ObPartitionTableOperator& pt_operator);
    bool need_prefetch() const
    {
      return (prefetch_idx_ >= prefetch_count() && !prefetch_iter_end_);
    }
    int prefetch();
    int get(ObPartitionInfo& partition);
    int next(ObPartitionInfo& partition);
    int64_t prefetch_count() const
    {
      return prefetch_partitions_.count();
    }
    void set_need_fetch_faillist(const bool need_fetch_faillist)
    {
      need_fetch_faillist_ = need_fetch_faillist;
    }

  private:
    int64_t prefetch_idx_;
    uint64_t table_id_;
    ObPartitionTableOperator* pt_operator_;
    bool prefetch_iter_end_;
    common::ObArray<ObPartitionInfo> prefetch_partitions_;
    // false by defaulta,only set to true for load balance
    bool need_fetch_faillist_;
  };
  ObTablePartitionIterator();
  virtual ~ObTablePartitionIterator();

  // can be inited twice
  int init(
      const uint64_t table_id, share::schema::ObSchemaGetterGuard& schema_guard, ObPartitionTableOperator& pt_operator);
  bool is_inited()
  {
    return inited_;
  }

  virtual int next(ObPartitionInfo& partition);

  virtual ObReplicaFilterHolder& get_filters()
  {
    return filters_;
  }

  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  int mock_next_partition(ObPartitionInfo& info);
  // check if the partition_id is in a rational range
  int check_replica(ObPartitionInfo& info, CheckType& check_type);

private:
  bool inited_;
  uint64_t table_id_;  // may be a binding tablegroup id
  schema::ObPartitionLevel part_level_;
  ObPartIdAscIterator asc_part_id_iterator_;
  ObPartitionTableOperator* pt_operator_;
  common::ObArenaAllocator allocator_;
  ObPrefetchInfo prefetch_info_;
  ObReplicaFilterHolder filters_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTablePartitionIterator);
};

// iterator partition for one tenant,
// include standlone partition and binding tablegroup
class ObTenantPartitionIterator : public ObIPartitionTableIterator {
public:
  ObTenantPartitionIterator();
  virtual ~ObTenantPartitionIterator();

  // allow init twice
  int init(ObPartitionTableOperator& pt_operator, schema::ObMultiVersionSchemaService& schema_service,
      const uint64_t tenant_id, bool ignore_row_checksum);
  bool is_inited()
  {
    return inited_;
  }
  ObReplicaFilterHolder& get_filters()
  {
    return filters_;
  }
  ObPartitionEntityFilterHolder& get_partition_entity_filters()
  {
    return partition_entity_filters_;
  }

  virtual int next(ObPartitionInfo& partition);

  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  int next_partition_entity();
  int inner_next(ObPartitionInfo& partition);
  int prefetch();
  int prefetch(const uint64_t last_table_id, const int64_t last_partition_id);

private:
  bool inited_;
  ObPartitionTableOperator* pt_operator_;
  schema::ObMultiVersionSchemaService* schema_service_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  schema::ObTenantPartitionEntityIterator partition_entity_iterator_;
  common::ObArenaAllocator allocator_;
  ObReplicaFilterHolder filters_;

  uint64_t tenant_id_;
  common::ObArray<ObPartitionInfo> prefetch_partitions_;
  int64_t prefetch_idx_;
  ;
  bool tenant_end_;

  uint64_t partition_entity_id_;
  ObPartIdAscIterator asc_part_id_iterator_;
  bool ignore_row_checksum_;
  ObPartitionEntityFilterHolder partition_entity_filters_;
  bool filter_dropped_schema_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantPartitionIterator);
};

/* iterate all partition infos of all tenants
 */
class ObPartitionTableIterator : public ObIPartitionTableIterator {
public:
  ObPartitionTableIterator();
  virtual ~ObPartitionTableIterator();

  int init(ObPartitionTableOperator& pt_operator, schema::ObMultiVersionSchemaService& schema_service,
      bool ignore_row_checksum);
  bool is_inited()
  {
    return inited_;
  }
  ObReplicaFilterHolder& get_filters()
  {
    return tenant_partition_iter_.get_filters();
  }
  ObPartitionEntityFilterHolder& get_partition_entity_filters()
  {
    return tenant_partition_iter_.get_partition_entity_filters();
  }

  virtual int next(ObPartitionInfo& partition);

  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  int next_tenant();

private:
  bool inited_;
  ObPartitionTableOperator* pt_operator_;
  schema::ObMultiVersionSchemaService* schema_service_;
  ObTenantPartitionIterator tenant_partition_iter_;
  schema::ObTenantIterator tenant_iter_;
  bool ignore_row_checksum_;

  DISALLOW_COPY_AND_ASSIGN(ObPartitionTableIterator);
};

/*
 * designated partition_table(__all_virtual_core_meta_table,__all_root_table,__all_tenant_meta_table)
 * for the specific partitions
 */
class ObPTPartPartitionIterator : public ObIPartitionTableIterator {
public:
  ObPTPartPartitionIterator();
  virtual ~ObPTPartPartitionIterator();

  int init(ObPartitionTableOperator& pt_operator, const uint64_t pt_table_id, const int64_t pt_partition_id);
  inline bool is_inited() const
  {
    return inited_;
  }
  ObReplicaFilterHolder& get_filters()
  {
    return filters_;
  }
  uint64_t get_pt_table_id() const
  {
    return pt_table_id_;
  }
  int64_t get_pt_partition_id() const
  {
    return pt_partition_id_;
  }
  virtual int next(ObPartitionInfo& partition);

  TO_STRING_KV(K_(inited), K_(pt_table_id), K_(pt_partition_id), K_(prefetch_partitions), K_(prefetch_idx));

private:
  int prefetch();

private:
  bool inited_;
  ObPartitionTableOperator* pt_operator_;
  uint64_t pt_table_id_;
  int64_t pt_partition_id_;

  common::ObArray<ObPartitionInfo> prefetch_partitions_;
  int64_t prefetch_idx_;

  common::ObArenaAllocator allocator_;
  ObReplicaFilterHolder filters_;

  DISALLOW_COPY_AND_ASSIGN(ObPTPartPartitionIterator);
};

/*
 * iterate using the following sequence __all_virtual_core_meta_table
 *                                      __all_root_table
 *                                      __all_tenant_meta_table of all tenants
 */
class ObPartitionTableIdIterator {
public:
  ObPartitionTableIdIterator();
  virtual ~ObPartitionTableIdIterator();

  int init(schema::ObMultiVersionSchemaService& schema_service);
  virtual int get_next_partition(uint64_t& pt_table_id, int64_t& pt_partition_id);
  TO_STRING_KV(K_(inited), K_(tenant_iter), K_(pt_tables), K_(pt_table_id), K_(pt_partition_id));

private:
  int get_part_num(const uint64_t table_id, int64_t& part_num);
  static const int PT_TYPE_NUM = 4;

private:
  bool inited_;
  schema::ObMultiVersionSchemaService* schema_service_;
  schema::ObTenantIterator tenant_iter_;
  uint64_t pt_tables_[PT_TYPE_NUM];
  uint64_t pt_table_id_;
  int64_t pt_partition_id_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionTableIdIterator);
};

/* iterate all partitions of all partition tables
 */
class ObFullPartitionTableIterator : public ObIPartitionTableIterator {
public:
  ObFullPartitionTableIterator();
  virtual ~ObFullPartitionTableIterator();

  int init(ObPartitionTableOperator& pt_operator, schema::ObMultiVersionSchemaService& schema_service);
  ObReplicaFilterHolder& get_filters()
  {
    return part_iter_.get_filters();
  }

  virtual int next(ObPartitionInfo& partition);

  TO_STRING_KV(K_(inited), K_(part_iter), K_(pt_part_iter));

private:
  int next_partition();

private:
  bool inited_;
  ObPartitionTableOperator* pt_operator_;
  ObPTPartPartitionIterator part_iter_;
  ObPartitionTableIdIterator pt_part_iter_;
  DISALLOW_COPY_AND_ASSIGN(ObFullPartitionTableIterator);
};

// iterate __all_tenant_meta_table only
class ObFullMetaTableIterator : public ObIPartitionTableIterator {
public:
  ObFullMetaTableIterator();
  virtual ~ObFullMetaTableIterator();

  int init(ObPartitionTableOperator& pt_operator, schema::ObMultiVersionSchemaService& schema_service);
  ObReplicaFilterHolder& get_filters()
  {
    return part_iter_.get_filters();
  }

  virtual int next(ObPartitionInfo& partition);

  TO_STRING_KV(K_(inited), K_(part_iter), K_(tenant_iter), K_(pt_table));

private:
  int next_partition();
  int get_part_num(const uint64_t table_id, int64_t& part_num);

private:
  bool inited_;
  ObPartitionTableOperator* pt_operator_;
  schema::ObMultiVersionSchemaService* schema_service_;
  ObPTPartPartitionIterator part_iter_;
  schema::ObTenantIterator tenant_iter_;
  uint64_t pt_table_;
  DISALLOW_COPY_AND_ASSIGN(ObFullMetaTableIterator);
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_PARTITION_TABLE_OB_PARTITION_TABLE_ITERATOR_H_
