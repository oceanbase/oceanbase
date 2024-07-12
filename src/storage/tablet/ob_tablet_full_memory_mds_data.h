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

#ifndef OCEANBASE_STORAGE_OB_TABLET_FULL_MEMORY_MDS_DATA
#define OCEANBASE_STORAGE_OB_TABLET_FULL_MEMORY_MDS_DATA

#include "lib/utility/ob_print_utils.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "storage/multi_data_source/adapter_define/mds_dump_node.h"
#include "storage/tablet/ob_tablet_complex_addr.h"
#include "storage/tablet/ob_tablet_full_medium_info.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObArenaAllocator;
}

namespace storage
{
class ObTabletMdsData;
class ObMdsMiniMergeOperator;

class ObTabletFullMemoryMdsData
{
public:
  ObTabletFullMemoryMdsData();
  ~ObTabletFullMemoryMdsData();
  ObTabletFullMemoryMdsData(const ObTabletFullMemoryMdsData &) = delete;
  ObTabletFullMemoryMdsData &operator=(const ObTabletFullMemoryMdsData &) = delete;
public:
  int init(common::ObArenaAllocator &allocator);
  int init(common::ObArenaAllocator &allocator, const ObTabletMdsData &mds_data);
  void reset();
  int assign(const ObTabletFullMemoryMdsData &other, common::ObIAllocator &allocator);
  int scan_all_mds_data_with_op(ObMdsMiniMergeOperator &op) const;
public:
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;

  TO_STRING_KV(K_(is_inited),
               K_(tablet_status_uncommitted_kv),
               K_(tablet_status_committed_kv),
               K_(aux_tablet_info_uncommitted_kv),
               K_(aux_tablet_info_committed_kv),
               K_(medium_info_list),
               K_(auto_inc_seq));
private:
  static int read_mds_dump_kv(
      common::ObArenaAllocator &allocator,
      const ObTabletComplexAddr<mds::MdsDumpKV> &mds_dump_kv_addr,
      mds::MdsDumpKV &dump_kv);
  static int read_medium_info_list(
      common::ObArenaAllocator &allocator,
      const ObTabletComplexAddr<ObTabletDumpedMediumInfo> &medium_info_list_addr,
      ObTabletDumpedMediumInfo &medium_info_list);
  static int read_auto_inc_seq(
      common::ObArenaAllocator &allocator,
      const ObTabletComplexAddr<share::ObTabletAutoincSeq> &auto_inc_seq_addr,
      share::ObTabletAutoincSeq &auto_inc_seq);
  static int mock_convert_auto_inc_seq_to_mds_dump_kv(
      common::ObArenaAllocator &allocator,
      const share::ObTabletAutoincSeq &auto_inc_seq,
      const share::SCN tablet_status_create_commit_scn,
      mds::MdsDumpKV &dump_kv);
  static int mock_convert_medium_info_to_mds_dump_kv(
      common::ObArenaAllocator &allocator,
      const compaction::ObMediumCompactionInfo& medium_info,
      const share::SCN tablet_status_create_commit_scn,
      mds::MdsDumpKV &dump_kv);
private:
  static const int64_t MEDIUM_LIST_VERSION = 1;
  static const int32_t MEDIUM_LIST_INFO_RESERVED_BITS = 52;
public:
  bool is_inited_;
  // if add new multi-source data, must adapt the scan_all_mds_data_with_op interface simultaneously, because it is closely related to compatibility.
  mds::MdsDumpKV tablet_status_uncommitted_kv_;
  mds::MdsDumpKV tablet_status_committed_kv_;
  mds::MdsDumpKV aux_tablet_info_uncommitted_kv_;
  mds::MdsDumpKV aux_tablet_info_committed_kv_;
  ObTabletFullMediumInfo medium_info_list_;
  share::ObTabletAutoincSeq auto_inc_seq_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_FULL_MEMORY_MDS_DATA
