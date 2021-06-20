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

#ifndef _OB_STAT_MANAGER_H_
#define _OB_STAT_MANAGER_H_

#include "share/stat/ob_column_stat.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObTableSchema;
}
}  // namespace share
namespace common {
struct ObPartitionKey;
class ObColumnStatValueHandle;
class ObTableStat;
class ObColumnStatDataService {
public:
  virtual ~ObColumnStatDataService()
  {}
  virtual int get_column_stat(const ObColumnStat::Key& key, const bool force_new, ObColumnStatValueHandle& handle) = 0;
  virtual int get_column_stat(
      const ObColumnStat::Key& key, const bool force_new, ObColumnStat& cstat, ObIAllocator& alloc) = 0;
  virtual int get_batch_stat(const uint64_t table_id, const ObIArray<uint64_t>& partition_ids,
      const ObIArray<uint64_t>& column_ids, ObIArray<ObColumnStatValueHandle>& handles) = 0;
  virtual int get_batch_stat(const share::schema::ObTableSchema& table_schema, const uint64_t partition_id,
      ObIArray<ObColumnStat*>& stats, ObIAllocator& allocator) = 0;
  virtual int update_column_stats(const ObIArray<ObColumnStat*>& column_stats) = 0;
  virtual int update_column_stat(const ObColumnStat& cstat) = 0;
  virtual int erase_column_stat(const common::ObPartitionKey& pkey, const int64_t column_id) = 0;
};

class ObTableStatDataService {
public:
  virtual ~ObTableStatDataService()
  {}
  virtual int get_table_stat(const ObPartitionKey& key, ObTableStat& tstat) = 0;
};

class ObStatManager {
public:
  ObStatManager();
  virtual ~ObStatManager();

  int init(ObColumnStatDataService* cs, ObTableStatDataService* ts, ObColumnStatDataService* vcs = NULL,
      ObTableStatDataService* vts = NULL);
  static ObStatManager& get_instance();
  static const ObTableStat& get_default_table_stat();

public:
  int update_column_stat(const ObColumnStat& cstat);
  /**
   * item in column_stats must not NULL, or will return OB_ERR_UNEXPECTED
   */
  int update_column_stats(const ObIArray<ObColumnStat*>& column_stats);
  int get_column_stat(
      const ObColumnStat::Key& key, ObColumnStat& cstat, ObIAllocator& alloc, const bool force_new = false);
  virtual int get_column_stat(
      const uint64_t table_id, const uint64_t partition_id, const uint64_t column_id, ObColumnStatValueHandle& handle);
  virtual int get_batch_stat(const uint64_t table_id, const common::ObIArray<uint64_t>& partition_ids,
      const common::ObIArray<uint64_t>& column_id, ObIArray<ObColumnStatValueHandle>& handle);
  int get_batch_stat(const share::schema::ObTableSchema& table_schema, const uint64_t partition_id,
      ObIArray<ObColumnStat*>& cstats, ObIAllocator& alloc);
  virtual int get_table_stat(const ObPartitionKey& key, ObTableStat& tstat);
  int erase_table_stat(const common::ObPartitionKey& pkey);
  int erase_column_stat(const common::ObPartitionKey& pkey, const int64_t column_id);

private:
  int get_column_stat(const ObColumnStat::Key& key, ObColumnStatValueHandle& handle);

private:
  DISALLOW_COPY_AND_ASSIGN(ObStatManager);
  ObColumnStatDataService* column_stat_service_;
  ObTableStatDataService* table_stat_service_;
  ObColumnStatDataService* virtual_column_stat_service_;
  ObTableStatDataService* virtual_table_stat_service_;
  bool inited_;
};  // end of class ObStatManager

}  // namespace common
}  // end of namespace oceanbase

#endif /* _OB_STAT_MANAGER_H_ */
