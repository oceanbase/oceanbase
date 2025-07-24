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
    ObITabletMdsInterface *src = nullptr;
    ObTabletHandle src_tablet_handle;
    if (get_tablet_meta_().has_transfer_table()) {
      if (CLICK_FAIL(get_src_tablet_handle_and_base_ptr_(src_tablet_handle, src))) {
        MDS_LOG(WARN, "fail to get src tablet handle", K(ret), K(get_tablet_meta_()));
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
  #undef PRINT_WRAPPER
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
    ObITabletMdsInterface *src = nullptr;
    ObTabletHandle src_tablet_handle;
    if (get_tablet_meta_().has_transfer_table()) {
      if (CLICK_FAIL(get_src_tablet_handle_and_base_ptr_(src_tablet_handle, src))) {
        MDS_LOG(WARN, "fail to get src tablet handle", K(ret), K(get_tablet_meta_()));
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
  #undef PRINT_WRAPPER
}

int ObITabletMdsCustomizedInterface::get_split_info_data(const share::SCN &snapshot,
                                                         ObTabletSplitInfoMdsUserData &data,
                                                         const int64_t timeout) const
{
  #define PRINT_WRAPPER KR(ret), K(data)
  MDS_TG(10_ms);
  int ret  = OB_SUCCESS;
  if (CLICK_FAIL((get_snapshot<mds::DummyKey, ObTabletSplitInfoMdsUserData>(
      mds::DummyKey(),
      ReadSplitInfoDataOp(data),
      snapshot,
      timeout)))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "fail to get snapshot", K(ret));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsCustomizedInterface::get_latest_committed_tablet_status(ObTabletCreateDeleteMdsUserData &data) const
{
  #define PRINT_WRAPPER KR(ret), K(data)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (CLICK_FAIL((get_latest_committed<ObTabletCreateDeleteMdsUserData>(
      ReadTabletStatusOp(data))))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG_GET(WARN, "fail to get latest committed", K(ret));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsCustomizedInterface::get_latest_binding_info(
    ObTabletBindingMdsUserData &data,
    mds::MdsWriter &writer,
    mds::TwoPhaseCommitState &trans_stat,
    share::SCN &trans_version) const
{
  #define PRINT_WRAPPER KR(ret), K(data)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_latest(data, writer, trans_stat, trans_version))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "failed to get latest binding info", KR(ret));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}
}
}