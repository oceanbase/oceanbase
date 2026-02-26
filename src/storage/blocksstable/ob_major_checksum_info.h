//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_BLOCKSSTABLE_MAJOR_CHECKSUM_INFO_H_
#define OB_STORAGE_BLOCKSSTABLE_MAJOR_CHECKSUM_INFO_H_
#include "lib/container/ob_array.h"
#include "storage/compaction/ob_compaction_util.h"
#include "storage/blocksstable/ob_column_checksum_struct.h"
namespace oceanbase
{
namespace compaction
{
class ObBasicTabletMergeCtx;
}
namespace storage
{
struct ObStorageColumnGroupSchema;
class ObStorageSchema;
}
namespace blocksstable
{
struct ObSSTableMergeRes;
class ObSSTable;

class ObMajorChecksumInfo
{
public:
  ObMajorChecksumInfo();
  virtual ~ObMajorChecksumInfo() { reset(); }
  void reset();
  int init_from_merge_result(
    ObArenaAllocator &allocator,
    const compaction::ObBasicTabletMergeCtx &ctx,
    const blocksstable::ObSSTableMergeRes &res);
  int init_from_sstable(
    ObArenaAllocator &allocator,
    const compaction::ObExecMode exec_mode,
    const storage::ObStorageSchema &storage_schema,
    const blocksstable::ObSSTable &sstable);
  bool is_empty() const;
  bool is_valid() const;
  int assign(const ObMajorChecksumInfo &other, ObArenaAllocator *allocator);
  int deep_copy(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      ObMajorChecksumInfo &dest) const;
  void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const;
  compaction::ObExecMode get_exec_mode() const { return (compaction::ObExecMode)exec_mode_; }
  int64_t get_compaction_scn() const { return is_empty() ? 0 : compaction_scn_; }
  int64_t get_report_compaction_scn() const { return (is_empty() || !is_output_exec_mode(get_exec_mode())) ? 0 : compaction_scn_; }
  int64_t get_row_count() const { return row_count_; }
  int64_t get_data_checksum() const { return data_checksum_; }
  const ObColumnCkmStruct &get_column_checksum_struct() const { return column_ckm_struct_; }
  int get_column_checksums(ObIArray<int64_t> &column_checksums) const
  {
    return column_ckm_struct_.get_column_checksums(column_checksums);
  }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(common::ObArenaAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  int64_t get_deep_copy_size() const { return column_ckm_struct_.get_deep_copy_size(); }
  int64_t to_string(char *buf, const int64_t buf_len) const;

protected:
  static const int32_t SRCS_ONE_BYTE = 8;
  static const int32_t SRCS_FOUR_BITS = 4;
  static const int32_t SRCS_RESERVED_BITS = 52;
  static const int64_t MAJOR_CHECKSUM_INFO_VERSION_V1 = 1;
  union {
    uint64_t info_;
    struct {
      uint64_t version_   : SRCS_ONE_BYTE;
      uint64_t exec_mode_ : SRCS_FOUR_BITS;
      uint64_t reserved_  : SRCS_RESERVED_BITS;
    };
  };
  int64_t compaction_scn_;
  int64_t row_count_;
  int64_t data_checksum_;
  ObColumnCkmStruct column_ckm_struct_;
};

class ObCOMajorChecksumInfo : public ObMajorChecksumInfo
{
public:
  ObCOMajorChecksumInfo()
    : ObMajorChecksumInfo(), lock_()
  {}
  virtual ~ObCOMajorChecksumInfo() {}
  int init_from_merge_result(
    ObArenaAllocator &allocator,
    const compaction::ObBasicTabletMergeCtx &ctx,
    const storage::ObStorageColumnGroupSchema &cg_schema,
    const blocksstable::ObSSTableMergeRes &res);
private:
  int prepare_column_ckm_array(
    ObArenaAllocator &allocator,
    const compaction::ObBasicTabletMergeCtx &ctx,
    const blocksstable::ObSSTableMergeRes &res);
  lib::ObMutex lock_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_BLOCKSSTABLE_MAJOR_CHECKSUM_INFO_H_
