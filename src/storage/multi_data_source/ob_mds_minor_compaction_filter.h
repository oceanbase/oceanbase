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

#ifndef OCEANBASE_STORAGE_OB_MDS_MINOR_COMPACTION_FILTER
#define OCEANBASE_STORAGE_OB_MDS_MINOR_COMPACTION_FILTER

#include "storage/compaction/ob_i_compaction_filter.h"

namespace oceanbase
{
namespace storage
{
class ObTabletDDLCompleteMdsUserData;
namespace mds
{
struct MdsDumpKVStorageAdapter;
}
class ObMdsMinorFilter : public compaction::ObICompactionFilter
{
public:
  ObMdsMinorFilter();
  virtual ~ObMdsMinorFilter() = default;
  int init(
    const int64_t first_major_snapshot,
    const int64_t last_major_snapshot,
    const int64_t multi_version_start,
    const share::ObLSID &ls_id);
  void reset()
  {
    last_major_snapshot_ = 0;
    truncate_filter_snapshot_ = 0;
    ls_id_ = share::ObLSID::INVALID_LS_ID;
    is_inited_ = false;
  }
  virtual CompactionFilterType get_filter_type() const override { return MDS_MINOR_FILTER_DATA; }
  virtual int filter(const blocksstable::ObDatumRow &row, ObFilterRet &filter_ret) override;
  INHERIT_TO_STRING_KV("ObICompactionFilter", ObICompactionFilter, "filter_name", "ObMdsMinorFilter", K_(is_inited),
      K_(last_major_snapshot), K_(truncate_filter_snapshot), K_(ls_id));
private:
  int filter_medium_info(
    const blocksstable::ObDatumRow &row,
    const mds::MdsDumpKVStorageAdapter &kv_adapter,
    ObFilterRet &filter_ret);
  int filter_truncate_info(
    const blocksstable::ObDatumRow &row,
    const mds::MdsDumpKVStorageAdapter &kv_adapter,
    ObFilterRet &filter_ret);
  int filter_ddl_complete_mds_info(
    const blocksstable::ObDatumRow &row,
    const mds::MdsDumpKVStorageAdapter &kv_adapter,
    ObFilterRet &filter_ret);
  int should_filter_ddl_inc_major_info(const ObTabletDDLCompleteMdsUserData &ddl_complete_info,
                                       bool &need_filter);
  int is_transaction_exist(ObLSHandle &ls_handle, const transaction::ObTransID &tx_id, bool &exist);
  int has_ddl_inc_major_sstables(ObLSHandle &ls_handle, const transaction::ObTransID &tx_id,
                                 ObTabletID &tablet_id, bool &exist);
  int is_ddl_inc_major_mds_expired(share::SCN start_scn, bool &expired);
private:
  bool is_inited_;
  int64_t last_major_snapshot_;
  int64_t truncate_filter_snapshot_;
  share::ObLSID ls_id_;
  ObArenaAllocator allocator_;
};

class ObCrossLSMdsMinorFilter : public compaction::ObICompactionFilter
{
public:
  ObCrossLSMdsMinorFilter();
  virtual ~ObCrossLSMdsMinorFilter() = default;
public:
  virtual int filter(const blocksstable::ObDatumRow &row, ObFilterRet &filter_ret) override;
  virtual CompactionFilterType get_filter_type() const override { return MDS_MINOR_CROSS_LS; }
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MDS_MINOR_COMPACTION_FILTER
