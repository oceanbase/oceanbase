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

#ifndef OCEANBASE_STORAGE_OB_TABLET_DUMPED_MEDIUM_INFO
#define OCEANBASE_STORAGE_OB_TABLET_DUMPED_MEDIUM_INFO

#include <stdint.h>
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/compaction/ob_medium_compaction_info.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace storage
{
namespace mds
{
struct MdsDumpKey;
struct MdsDumpNode;
}

class ObTabletDumpedMediumInfo
{
public:
  typedef common::ObSEArray<compaction::ObMediumCompactionInfo*, 1>::iterator iterator;
public:
  ObTabletDumpedMediumInfo();
  ~ObTabletDumpedMediumInfo();
  ObTabletDumpedMediumInfo(const ObTabletDumpedMediumInfo &) = delete;
  ObTabletDumpedMediumInfo &operator=(const ObTabletDumpedMediumInfo &) = delete;
public:
  int init_for_first_creation(common::ObIAllocator &allocator);
  int init_for_evict_medium_info(
      common::ObIAllocator &allocator,
      const int64_t finish_medium_scn,
      const ObTabletDumpedMediumInfo &other);
  int init_for_mds_table_dump(
      common::ObIAllocator &allocator,
      const int64_t finish_medium_scn,
      const ObTabletDumpedMediumInfo &other1,
      const ObTabletDumpedMediumInfo &other2);
  void reset();

  // key order in array: big -> small
  int append(
      const mds::MdsDumpKey &key,
      const mds::MdsDumpNode &node);
  int append(const compaction::ObMediumCompactionInfo &medium_info);
  int assign(const ObTabletDumpedMediumInfo &other, common::ObIAllocator &allocator);
  bool is_valid() const;

  int get_min_medium_info_key(compaction::ObMediumCompactionInfoKey &key) const;
  int get_max_medium_info_key(compaction::ObMediumCompactionInfoKey &key) const;

  int64_t get_min_medium_snapshot() const;
  int64_t get_max_medium_snapshot() const;

  int is_contain(const compaction::ObMediumCompactionInfo &info, bool &contain) const;

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;

  int64_t to_string(char* buf, const int64_t buf_len) const;
  int64_t simple_to_string(char* buf, const int64_t buf_len, int64_t &pos) const;
public:
  static bool compare(const compaction::ObMediumCompactionInfo *lhs, const compaction::ObMediumCompactionInfo *rhs);
private:
  int do_append(const compaction::ObMediumCompactionInfo &medium_info);
public:
  bool is_inited_;
  common::ObIAllocator *allocator_;
  common::ObSEArray<compaction::ObMediumCompactionInfo*, 1> medium_info_list_;
};

class ObTabletDumpedMediumInfoIterator
{
public:
  ObTabletDumpedMediumInfoIterator();
  ~ObTabletDumpedMediumInfoIterator();
  ObTabletDumpedMediumInfoIterator(const ObTabletDumpedMediumInfoIterator &) = delete;
  ObTabletDumpedMediumInfoIterator &operator=(const ObTabletDumpedMediumInfoIterator &) = delete;
public:
  int init(
      common::ObIAllocator &allocator,
      const ObTabletDumpedMediumInfo *dumped_medium_info);
  void reset();
  int get_next_key(compaction::ObMediumCompactionInfoKey &key);
  int get_next_medium_info(
      common::ObIAllocator &allocator,
      compaction::ObMediumCompactionInfoKey &key,
      compaction::ObMediumCompactionInfo &medium_info);
  int get_next_medium_info(
      compaction::ObMediumCompactionInfoKey &key,
      const compaction::ObMediumCompactionInfo *&medium_info);
private:
  bool is_inited_;
  int64_t idx_;
  common::ObIAllocator *allocator_;
  common::ObSEArray<compaction::ObMediumCompactionInfo*, 1> medium_info_list_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_DUMPED_MEDIUM_INFO
