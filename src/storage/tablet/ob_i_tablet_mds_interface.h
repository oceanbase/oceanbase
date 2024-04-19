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

#ifndef OCEANBASE_STORAGE_TABLET_OB_TABLET_MDS_PART
#define OCEANBASE_STORAGE_TABLET_OB_TABLET_MDS_PART

#include "lib/ob_errno.h"
#include "meta_programming/ob_meta_copy.h"
#include "storage/multi_data_source/mds_table_handle.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/tablet/ob_tablet_mds_data.h"
#include "storage/tablet/ob_tablet_member_wrapper.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/ls/ob_ls_switch_checker.h"

namespace oceanbase
{
namespace storage
{
class ObTabletCreateDeleteHelper;

template <typename T>
struct MdsDefaultDeepCopyOperation {
  MdsDefaultDeepCopyOperation(T &value, ObIAllocator *alloc) : value_(value), alloc_(alloc) {}
  int operator()(const T &value) {
    int ret = OB_SUCCESS;
    if (nullptr == alloc_) {
      ret = meta::copy_or_assign(value, value_);
    } else {
      ret = meta::copy_or_assign(value, value_, *alloc_);
    }
    return ret;
  }
  T &value_;
  ObIAllocator *alloc_;
};

class ObITabletMdsInterface
{
  friend class ObTabletCreateDeleteHelper;
public:
  // new mds
  // Currently, we only support read LATEST multi source data, so please pass MAX_SCN as snapshot.
  // Other value will cause OB_NOT_SUPPOTED error.
  // Snapshot read operation will be implemented after multi source data dumped into macro blocks.
  template <typename T>// general set for dummy key unit
  int set(T &&data, mds::MdsCtx &ctx, const int64_t lock_timeout_us = 0);
  template <typename Key, typename Value>// general set for multi key unit
  int set(const Key &key, Value &&data, mds::MdsCtx &ctx, const int64_t lock_timeout_us = 0);
  template <typename Key, typename Value>// general remove for multi key unit
  int remove(const Key &key, mds::MdsCtx &ctx, const int64_t lock_timeout_us = 0);
  // sometimes mds ndoes needed be forcely released, e.g.: ls offline
  template <typename T>
  int is_locked_by_others(bool &is_locked, const mds::MdsWriter &self = mds::MdsWriter()) const;

  int check_tablet_status_written(bool &written) const;
  // belows are wrapper interfaces for default getter for simple data structure
  // specialization get for each module
  // if trans_stat < BEFORE_PREPARE, trans_version is explained as prepare_version(which is MAX).
  // else if trans_stat < ON_PREAPRE, trans_version is explained as prepare_version(which is MIN).
  // else if trans_stat < ON_COMMIT, trans_version is explained as prepare_version(which is a valid data).
  // else if trans_stat == ON_COMMIT, trans_version is explained as commit_version(which is a valid data).
  template <typename T, typename T2 = T, ENABLE_IF_NOT_LIKE_FUNCTION(T2, int(const T &))>
  int get_latest(T &value,
                 mds::MdsWriter &writer,// FIXME(xuwang.txw): should not exposed, will be removed later
                 mds::TwoPhaseCommitState &trans_stat,// FIXME(xuwang.txw): should not exposed, will be removed later
                 share::SCN &trans_version,// FIXME(xuwang.txw): should not exposed, will be removed later
                 ObIAllocator *alloc = nullptr,
                 const int64_t read_seq = 0) const {
    MdsDefaultDeepCopyOperation<T> default_get_op(value, alloc);
    return get_latest<T, MdsDefaultDeepCopyOperation<T> &>(default_get_op, writer, trans_stat, trans_version, read_seq);
  }
  template <typename T, typename T2 = T, ENABLE_IF_NOT_LIKE_FUNCTION(T2, int(const T &))>
  int get_latest_committed(T &value, ObIAllocator *alloc = nullptr) const {
    MdsDefaultDeepCopyOperation<T> default_get_op(value, alloc);
    return get_latest_committed<T, MdsDefaultDeepCopyOperation<T> &>(default_get_op);
  }
  template <typename T, typename T2 = T, ENABLE_IF_NOT_LIKE_FUNCTION(T2, int(const T &))>
  int get_snapshot(T &value,
                   const share::SCN snapshot,
                   const int64_t timeout_us,
                   ObIAllocator *alloc = nullptr,
                   const int64_t read_seq = 0) const {
    MdsDefaultDeepCopyOperation<T> default_get_op(value, alloc);
    return get_snapshot<T, MdsDefaultDeepCopyOperation<T> &>(default_get_op, snapshot, timeout_us, read_seq);
  }
  // belows are general get interfaces, which could be customized for complicated data structure
  template <typename T, typename OP, ENABLE_IF_LIKE_FUNCTION(OP, int(const T &))>
  int get_latest(OP &&read_op,
                 mds::MdsWriter &writer,// FIXME(xuwang.txw): should not exposed, will be removed later
                 mds::TwoPhaseCommitState &trans_stat,// FIXME(xuwang.txw): should not exposed, will be removed later
                 share::SCN &trans_version,// FIXME(xuwang.txw): should not exposed, will be removed later
                 const int64_t read_seq = 0) const;
  template <typename T, typename OP, ENABLE_IF_LIKE_FUNCTION(OP, int(const T &))>
  int get_latest_committed(OP &&read_op) const;
  template <typename T, typename OP, ENABLE_IF_LIKE_FUNCTION(OP, int(const T &))>
  int get_snapshot(OP &&read_op,
                   const share::SCN snapshot,
                   const int64_t timeout_us,
                   const int64_t read_seq = 0) const;
  int fill_virtual_info(ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const;
  TO_STRING_KV(KP(this), "is_inited", check_is_inited_(), "ls_id", get_tablet_meta_().ls_id_,
               "tablet_id", get_table_id_(), KP(get_tablet_pointer_()));
  int get_mds_table_rec_scn(share::SCN &rec_scn);
  int mds_table_flush(const share::SCN &recycle_scn);
  // get tablet status from MDS, and check whether state is TRANSFER_IN and redo scn is valid.
  // @param [in] written : if current tablet status is TRANSFER_IN, set true if redo_scn is valid, otherwise set fasle
  // @return OB_STATE_NOT_MATCH : tablet status is not TRANSFER_IN.
  //         OB_EMPTY_RESULT : never has tablet status written.
  //         OB_LS_OFFLINE : read meet ls offline
  //         other error...
  // CAUTIONS: this interface is only for transfer! anyone else shouldn't call this!
  int check_transfer_in_redo_written(bool &written);
protected:// implemented by ObTablet
  virtual bool check_is_inited_() const = 0;
  virtual const ObTabletMdsData &get_mds_data_() const = 0;
  virtual const ObTabletMeta &get_tablet_meta_() const = 0;
  virtual int get_mds_table_handle_(mds::MdsTableHandle &handle,
                                    const bool create_if_not_exist) const = 0;
  virtual ObTabletPointer *get_tablet_pointer_() const = 0;
  template <typename T>
  int get_mds_data_from_tablet(const common::ObFunction<int(const T&)> &read_op) const;

  template <typename T>
  int replay(T &&mds,
             mds::MdsCtx &ctx,
             const share::SCN &scn);
private:
  template <typename Key, typename Value>
  int replay(const Key &key,
             Value &&mds,
             mds::MdsCtx &ctx,
             const share::SCN &scn);
  template <typename Key, typename Value>
  int replay_remove(const Key &key,
                    mds::MdsCtx &ctx,
                    const share::SCN &scn);
  common::ObTabletID get_table_id_() const;
  template <typename T>
  int obj_to_string_holder_(const T &obj, ObStringHolder &holder) const;
  template <typename T>
  int fill_virtual_info_by_obj_(const T &obj, const mds::NodePosition position, ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const;
  template <typename T>
  int fill_virtual_info_by_complex_addr_(const ObTabletComplexAddr<mds::MdsDumpKV> &addr,
                                         ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const;
};
}
}

#ifndef INCLUDE_OB_TABLET_MDS_PART_IPP
#define INCLUDE_OB_TABLET_MDS_PART_IPP
#include "ob_i_tablet_mds_interface.ipp"
#endif

#endif
