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
#include "storage/multi_data_source/mds_table_handle.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/tablet/ob_tablet_mds_data.h"
#include "storage/tablet/ob_tablet_member_wrapper.h"
#include "storage/ls/ob_ls_switch_checker.h"

namespace oceanbase
{
namespace storage
{
class ObTabletCreateDeleteHelper;

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

  int check_tablet_status_written(bool &written);
  // specialization get for each module
  int get_latest_tablet_status(ObTabletCreateDeleteMdsUserData &data, bool &is_committed) const;
  int get_tablet_status(const share::SCN &snapshot,
                        ObTabletCreateDeleteMdsUserData &data,
                        const int64_t timeout = 0) const;
  int get_ddl_data(const share::SCN &snapshot,
                   ObTabletBindingMdsUserData &data,
                   const int64_t timeout = 0) const;
  int get_autoinc_seq(ObIAllocator &allocator,
                      const share::SCN &snapshot,
                      share::ObTabletAutoincSeq &data,
                      const int64_t timeout = 0) const;
  int fill_virtual_info(ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const;
  TO_STRING_KV(KP(this), "is_inited", check_is_inited_(), "ls_id", get_tablet_meta_().ls_id_,
               "tablet_id", get_table_id_(), KP(get_tablet_ponter_()));
  int get_mds_table_rec_log_scn(share::SCN &rec_scn);
  int mds_table_flush(const share::SCN &recycle_scn);
protected:// implemented by ObTablet
  virtual bool check_is_inited_() const = 0;
  virtual const ObTabletMdsData &get_mds_data_() const = 0;
  virtual const ObTabletMeta &get_tablet_meta_() const = 0;
  virtual int get_mds_table_handle_(mds::MdsTableHandle &handle,
                                    const bool create_if_not_exist) const = 0;
  virtual ObTabletPointer *get_tablet_ponter_() const = 0;
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
                    const share::SCN &scn);// called only by ObTabletReplayExecutor
  template <typename T, typename OP>
  int get_latest(OP &&read_op, bool &is_committed, const int64_t read_seq) const;
  template <typename T, typename OP>
  int get_snapshot(OP &&read_op,
                   const share::SCN snapshot,
                   const int64_t read_seq,
                   const int64_t timeout_us) const;// general get for dummy key unit
  template <typename Key, typename Value, typename OP>
  int get_snapshot(const Key &key,
                   OP &&read_op,
                   const share::SCN snapshot,
                   const int64_t read_seq,
                   const int64_t timeout_us) const;// general get for multi key unit
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
