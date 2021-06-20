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

#ifndef OCEANBASE_CLOG_OB_ILOG_FILE_BUILDER_H_
#define OCEANBASE_CLOG_OB_ILOG_FILE_BUILDER_H_
#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "common/ob_member_list.h"
#include "ob_info_block_handler.h"

namespace oceanbase {
namespace common {}
namespace clog {

class PinnedMemory;

class MemberListInfo {
public:
  MemberListInfo()
      : memberlist_(),
        replica_num_(0),
        membership_log_id_(common::OB_INVALID_ID),
        memberlist_version_(common::OB_INVALID_TIMESTAMP)
  {}
  ~MemberListInfo()
  {}

public:
  const common::ObMemberList& get_memberlist() const
  {
    return memberlist_;
  }
  int64_t get_replica_num() const
  {
    return replica_num_;
  }
  uint64_t get_membership_log_id() const
  {
    return membership_log_id_;
  }
  int64_t get_memberlist_version() const
  {
    return memberlist_version_;
  }
  void set_memberlist_info(const common::ObMemberList& memberlist, const int64_t replica_num,
      const uint64_t membership_log_id, const int64_t memberlist_version)
  {
    memberlist_ = memberlist;
    replica_num_ = replica_num;
    membership_log_id_ = membership_log_id;
    memberlist_version_ = memberlist_version;
  }
  TO_STRING_KV(K(memberlist_), K(replica_num_), K(membership_log_id_), K(memberlist_version_));

private:
  common::ObMemberList memberlist_;
  int64_t replica_num_;
  uint64_t membership_log_id_;
  int64_t memberlist_version_;
};
typedef common::ObArrayHashMap<common::ObPartitionKey, MemberListInfo> MemberListMap;

class ObIlogMemstore;
class RawArray;
class ObIlogFileBuilder {
public:
  ObIlogFileBuilder();
  ~ObIlogFileBuilder();

public:
  int init(ObIlogMemstore* ilog_memstore, PinnedMemory* pinned_memory);
  void destroy();

  int get_file_buffer(char*& buffer, int64_t& size);
  IndexInfoBlockMap& get_index_info_block_map()
  {
    return index_info_block_map_;
  }

private:
  class PrepareRawArrayFunctor;
  class UpdateStartOffsetFunctor;
  class BuildArrayMapFunctor;
  class BuildMemberListMapFunctor;
  typedef common::ObLinearHashMap<common::ObPartitionKey, IndexInfoBlockEntry> PartitionMetaInfoMap;
  typedef common::ObLinearHashMap<common::ObPartitionKey, MemberListInfo> PartitionMemberListMap;

  int build_file_();
  int prepare_raw_array_(RawArray& raw_array);
  int build_file_buffer_(const RawArray& raw_array);
  int build_array_hash_map_(PartitionMetaInfoMap& partition_meta_info, IndexInfoBlockMap& index_info_block_map);
  int build_memberlist_map_(PartitionMemberListMap& partition_memberlist_map, MemberListMap& memberlist_map);
  int build_cursor_array_(const RawArray& raw_array, offset_t& next_offset);
  int build_info_block_(
      const IndexInfoBlockMap& index_info_block_map, const offset_t info_block_start_offset, offset_t& next_offset);
  int build_memberlist_block_(
      const MemberListMap& memberlist_map, const offset_t memberlist_block_start_offset, offset_t& next_offset);
  int calculate_checksum_(int64_t& file_content_checksum, const int64_t size) const;
  int build_file_trailer_(const offset_t trailer_start_offset, const offset_t info_block_start_offset,
      const int32_t info_block_size, const offset_t memberlist_block_start_offset, const int32_t memberlist_block_size,
      const int64_t file_content_checksum);
  bool check_ilog_continous_(
      const uint64_t min_log_id, const uint64_t max_log_id, const uint64_t last_log_id, const uint64_t curr_log_id);

private:
  bool is_inited_;
  ObIlogMemstore* ilog_memstore_;
  char* buffer_;
  int64_t total_size_;
  IndexInfoBlockMap index_info_block_map_;
  MemberListMap memberlist_map_;
  PinnedMemory* pinned_memory_;
  common::PageArena<> page_arena_;
  common::ObTimeGuard time_guard_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIlogFileBuilder);
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_ILOG_FILE_BUILDER_H_
