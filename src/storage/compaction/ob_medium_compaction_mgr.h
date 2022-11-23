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

#ifndef OB_STORAGE_COMPACTION_MEDIUM_COMPACTION_MGR_H_
#define OB_STORAGE_COMPACTION_MEDIUM_COMPACTION_MGR_H_

#include "storage/ob_storage_schema.h"

namespace oceanbase
{
namespace compaction
{

/*
 * TODO (@lixia.yq) add parallel merge info
struct ObParallelMergeInfo
{
    int64_t concurrent_cnt;
    ObDatumRowkey parallel_end_key[]; // concurrent_cnt - 1
};
*/

struct ObMediumCompactionInfo : public common::ObDLinkBase<ObMediumCompactionInfo>
{
public:
  enum ObCompactionType
  {
    MAJOR_COMPACTION = 0,
    MEDIUM_COMPACTION = 1,
    COMPACTION_TYPE_MAX,
  };
  const static char *ObCompactionTypeStr[];
  const static char *get_compaction_type_str(enum ObCompactionType type);
public:
  ObMediumCompactionInfo();
  ~ObMediumCompactionInfo();

  int init(ObIAllocator &allocator, const ObMediumCompactionInfo &medium_info);
  bool is_valid() const;
  void reset();
  int save_storage_schema(ObIAllocator &allocator, const storage::ObStorageSchema &storage_schema);

  // serialize & deserialize
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;

  void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const;
  TO_STRING_KV(K_(medium_compat_version),
      "compaction_type", ObMediumCompactionInfo::get_compaction_type_str((ObCompactionType)compaction_type_),
      K_(cluster_id), K_(medium_snapshot), K_(medium_log_ts), K_(is_schema_changed),
      K_(storage_schema));
public:
  static const int64_t MEIDUM_COMPAT_VERSION = 1;

private:
  static const int32_t SCS_ONE_BIT = 1;
  static const int32_t SCS_RESERVED_BITS = 57;

public:
  union {
    uint64_t info_;
    struct {
      uint64_t medium_compat_version_           : 4;
      uint64_t compaction_type_                 : 2;
      uint64_t is_schema_changed_               : SCS_ONE_BIT;  // TODO for progressive merge
      uint64_t reserved_                        : SCS_RESERVED_BITS;
    };
  };
  uint64_t cluster_id_; // for backup database to throw MEDIUM_COMPACTION clog
  int64_t medium_snapshot_;
  int64_t medium_log_ts_; // for follower minor merge
  storage::ObStorageSchema storage_schema_;
  //TODO(@lixia.yq) ObParallelMergeInfo parallel_merge_info;
};

class ObMediumCompactionInfoList
{
public:
  enum ObMediumListType
  {
    MEDIUM_LIST_IN_MEMORY = 0,
    MEDIUM_LIST_IN_STORAGE = 1,
    MEDIUM_LIST_MAX,
  };

public:
  ObMediumCompactionInfoList(ObMediumListType medium_list_type);
  ~ObMediumCompactionInfoList();

  typedef common::ObDList<ObMediumCompactionInfo> MediumInfoList;

  int init(common::ObIAllocator &allocator);

  int init(common::ObIAllocator &allocator,
      const ObMediumCompactionInfoList *old_list);

  void reset();
  OB_INLINE bool is_empty() const { return 0 == medium_info_list_.get_size(); }
  OB_INLINE int64_t size() const { return medium_info_list_.get_size(); }

  bool is_valid() const
  {
    return cur_medium_snapshot_ >= 0 && size() >= 0
        && (MEDIUM_LIST_IN_MEMORY == medium_list_type_
            || (MEDIUM_LIST_IN_STORAGE == medium_list_type_ && size() <= MAX_SERIALIZE_SIZE));
  }

  int add_medium_compaction_info(const ObMediumCompactionInfo &input_info);
  int save_medium_compaction_info(const ObMediumCompactionInfoList &input_list);

  const MediumInfoList &get_list() const { return medium_info_list_; }
  int64_t get_cur_medium_snapshot() const { return cur_medium_snapshot_; }

  int get_specified_snapshot_info(
      const int64_t snapshot,
      const ObMediumCompactionInfo *&compaction_info) const;
  OB_INLINE int64_t get_max_medium_snapshot() const
  {
    return 0 == size() ? 0 : medium_info_list_.get_last()->medium_snapshot_;
  }
  static bool need_select_inner_table_to_decide(const int64_t total_medium_info_size)
  {
    return total_medium_info_size > MAX_SERIALIZE_SIZE;
  }

  // serialize & deserialize
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;

  void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const;
  TO_STRING_KV(K_(is_inited), K_(medium_list_type), K_(cur_medium_snapshot),
      "list_size", size(), K_(medium_info_list));

private:
  OB_INLINE int append_list_with_deep_copy(const ObMediumCompactionInfoList &input_list)
  {
    int ret = OB_SUCCESS;
    DLIST_FOREACH_X(input_info, input_list.medium_info_list_, OB_SUCC(ret)) {
      ret = inner_deep_copy_node(*input_info);
    }
    return ret;
  }
  int inner_deep_copy_node(const ObMediumCompactionInfo &medium_info);

private:
  static const int64_t MAX_SERIALIZE_SIZE = 2;

private:
  bool is_inited_;
  ObMediumListType medium_list_type_; // no need to serialize
  int64_t cur_medium_snapshot_;
  common::ObIAllocator *allocator_;
  MediumInfoList medium_info_list_;
};


} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MEDIUM_COMPACTION_H_
