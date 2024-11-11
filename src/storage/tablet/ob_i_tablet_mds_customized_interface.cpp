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

#include "ob_i_tablet_mds_customized_interface.h"

namespace oceanbase
{
namespace storage
{

int ObITabletMdsCustomizedInterface::get_ddl_data(ObTabletBindingMdsUserData &ddl_data)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_latest_committed_data(ddl_data))) {
    if (OB_EMPTY_RESULT == ret) {
      ddl_data.set_default_value(); // use default value
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObITabletMdsCustomizedInterface::get_autoinc_seq(share::ObTabletAutoincSeq &inc_seq, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_latest_committed_data(inc_seq, &allocator))) {
    if (OB_EMPTY_RESULT == ret) {
      inc_seq.reset(); // use default value
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObITabletMdsCustomizedInterface::get_latest_split_data(
    ObTabletSplitMdsUserData &data,
    mds::MdsWriter &writer,
    mds::TwoPhaseCommitState &trans_stat,
    share::SCN &trans_version,
    const int64_t read_seq) const
{
  #define PRINT_WRAPPER KR(ret), K(data)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else {
    const ObTabletMeta &tablet_meta = get_tablet_meta_();
    const bool has_transfer_table = tablet_meta.has_transfer_table();
    ObITabletMdsInterface *src = nullptr;
    ObTabletHandle src_tablet_handle;
    if (has_transfer_table) {
      const share::ObLSID &src_ls_id = tablet_meta.transfer_info_.ls_id_;
      const common::ObTabletID &tablet_id = tablet_meta.tablet_id_;
      if (CLICK_FAIL(get_tablet_handle_and_base_ptr(src_ls_id, tablet_id, src_tablet_handle, src))) {
        MDS_LOG(WARN, "fail to get src tablet handle", K(ret), K(src_ls_id), K(tablet_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (CLICK_FAIL((cross_ls_get_latest<ObTabletSplitMdsUserData>(
        src,
        ReadSplitDataOp(data),
        writer,
        trans_stat,
        trans_version,
        read_seq)))) {
      if (OB_EMPTY_RESULT != ret) {
        MDS_LOG_GET(WARN, "fail to cross ls get latest", K(lbt()));
      }
    }
  }
  return ret;
}

int ObITabletMdsCustomizedInterface::get_latest_autoinc_seq(
    ObTabletAutoincSeq &data,
    ObIAllocator &allocator,
    mds::MdsWriter &writer,
    mds::TwoPhaseCommitState &trans_stat,
    share::SCN &trans_version,
    const int64_t read_seq) const
{
  #define PRINT_WRAPPER KR(ret), K(data)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else {
    const ObTabletMeta &tablet_meta = get_tablet_meta_();
    const bool has_transfer_table = tablet_meta.has_transfer_table();
    ObITabletMdsInterface *src = nullptr;
    ObTabletHandle src_tablet_handle;
    if (has_transfer_table) {
      const share::ObLSID &src_ls_id = tablet_meta.transfer_info_.ls_id_;
      const common::ObTabletID &tablet_id = tablet_meta.tablet_id_;
      if (CLICK_FAIL(get_tablet_handle_and_base_ptr(src_ls_id, tablet_id, src_tablet_handle, src))) {
        MDS_LOG(WARN, "fail to get src tablet handle", K(ret), K(src_ls_id), K(tablet_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (CLICK_FAIL((cross_ls_get_latest<ObTabletAutoincSeq>(
        src,
        ReadAutoIncSeqOp(allocator, data),
        writer,
        trans_stat,
        trans_version,
        read_seq)))) {
      if (OB_EMPTY_RESULT != ret) {
        MDS_LOG_GET(WARN, "fail to cross ls get latest", K(lbt()));
      }
    }
  }
  return ret;
}
}
}