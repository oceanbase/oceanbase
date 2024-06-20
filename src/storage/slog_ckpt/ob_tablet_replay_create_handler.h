
/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_STORAGE_SLOG_CKPT_TBALET_REPLAY_CREATE_HANDLER_H
#define OB_STORAGE_SLOG_CKPT_TBALET_REPLAY_CREATE_HANDLER_H

#include "storage/meta_mem/ob_tablet_map_key.h"
#include "observer/ob_startup_accel_task_handler.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"


namespace oceanbase
{

namespace observer
{
class ObStartupAccelTaskHandler;
}
namespace share
{
class ObTenantBase;
class SCN;
}

namespace storage
{
class ObLSHandle;
class ObTabletReplayCreateHandler;
class ObTabletTransferInfo;
class ObLSTabletService;

enum class ObTabletRepalyOperationType
{
  REPLAY_CREATE_TABLET = 0,
  REPLAY_INC_MACRO_REF = 1,
  REPLAY_CLONE_TABLET = 2,
  INVALID_MODULE
};

struct ObTabletReplayItem
{
public:
  ObTabletReplayItem(const ObTabletMapKey &key, const ObMetaDiskAddr &addr)
    : key_(key), addr_(addr) {}
  ObTabletReplayItem()
    : key_(), addr_() {}
  ~ObTabletReplayItem() {}
  bool operator<(const ObTabletReplayItem &r) const;
  TO_STRING_KV(K_(key), K_(addr));

  ObTabletMapKey key_;
  ObMetaDiskAddr addr_;
};

using ObTabletReplayItemRange = std::pair<int64_t, int64_t>; // record the start_item_idx and end_item_idx in total_tablet_item_arr

class ObTabletReplayCreateTask : public observer::ObStartupAccelTask
{
public:
  enum Type
  {
    DISCRETE = 0,
    AGGREGATE = 1,
    MAX = 2,
  };
  ObTabletReplayCreateTask()
    : is_inited_(false),
      idx_(-1),
      type_(Type::MAX),
      replay_item_range_arr_(),
      tablet_cnt_(0),
      tenant_base_(nullptr),
      handler_(nullptr) {}

  virtual ~ObTabletReplayCreateTask()
  {
    destroy();
  }
  int init(const int64_t task_idx, const ObTabletReplayCreateTask::Type type,
      share::ObTenantBase *tenant_base, ObTabletReplayCreateHandler *handler);

  int execute() override;
  int add_item_range(const ObTabletReplayItemRange &range, bool &is_enough);

  VIRTUAL_TO_STRING_KV(K_(idx), K_(type), KP(this), KP_(tenant_base),
      "range_count", replay_item_range_arr_.count(), K_(tablet_cnt));


private:
  static const int64_t MAX_DISCRETE_TABLET_CNT_PER_TASK = 200;
  static const int64_t MAX_AGGREGATE_BLOCK_CNT_PER_TASK = 3;
  void destroy();

private:
  bool is_inited_;
  int64_t idx_;
  Type type_;
  common::ObSEArray<ObTabletReplayItemRange, MAX_DISCRETE_TABLET_CNT_PER_TASK> replay_item_range_arr_;
  int64_t tablet_cnt_;
  share::ObTenantBase *tenant_base_;
  ObTabletReplayCreateHandler *handler_;
};


//============================= ObTabletReplayCreateHandler ==============================//

class ObTabletReplayCreateHandler
{
public:
  ObTabletReplayCreateHandler();
  ~ObTabletReplayCreateHandler() {}

  int init(
      const common::hash::ObHashMap<ObTabletMapKey, ObMetaDiskAddr> &tablet_item_map,
      const ObTabletRepalyOperationType replay_type);
  int concurrent_replay(observer::ObStartupAccelTaskHandler* startup_accel_handler);

  int replay_discrete_tablets(const ObIArray<ObTabletReplayItemRange> &range_arr);
  int replay_aggregate_tablets(const ObIArray<ObTabletReplayItemRange> &range_arr);

  void inc_inflight_task_cnt() { ATOMIC_INC(&inflight_task_cnt_); }
  void dec_inflight_task_cnt() { ATOMIC_DEC(&inflight_task_cnt_); }
  void inc_finished_tablet_cnt(const int64_t cnt) { (void)ATOMIC_FAA(&finished_tablet_cnt_, cnt); }
  void set_errcode(const int errcode) { ATOMIC_STORE(&errcode_, errcode); };

private:
  static bool is_suitable_to_aggregate_(const int64_t tablet_cnt_in_block, const int64_t valid_size_in_block)
  {
    return tablet_cnt_in_block > AGGREGATE_CNT_THRESHOLD && valid_size_in_block > AGGREGATE_SIZE_THRESHOLD;
  }
  int do_replay(
      const ObTabletReplayItem &replay_item,
      const char *buf,
      const int64_t buf_len,
      ObArenaAllocator &allocator) const;
  static int replay_create_tablet(const ObTabletReplayItem &replay_item, const char *buf, const int64_t buf_len);
  static int replay_inc_macro_ref(
      const ObTabletReplayItem &replay_item,
      const char *buf,
      const int64_t buf_len,
      ObArenaAllocator &allocator);
  static int replay_clone_tablet(const ObTabletReplayItem &replay_item, const char *buf, const int64_t buf_len);
  static int get_tablet_svr_(const share::ObLSID &ls_id, ObLSTabletService *&ls_tablet_svr, ObLSHandle &ls_handle);
  static int record_ls_transfer_info_(
      const ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      const ObTabletTransferInfo &tablet_transfer_info);
  static int check_is_need_record_transfer_info_(
      const share::ObLSID &src_ls_id,
      const share::SCN &transfer_start_scn,
      bool &is_need);
  int add_item_range_to_task_(
      observer::ObStartupAccelTaskHandler* startup_accel_handler,
      const ObTabletReplayCreateTask::Type type,
      const ObTabletReplayItemRange &range, ObTabletReplayCreateTask *&task);
  int add_task_(
      observer::ObStartupAccelTaskHandler* startup_accel_handler,
      ObTabletReplayCreateTask *task);


private:
  static const int64_t AGGREGATE_CNT_THRESHOLD = 16;
  static const int64_t AGGREGATE_SIZE_THRESHOLD = 256 << 10; // 256K

  bool is_inited_;
  ObArenaAllocator allocator_;
  int64_t task_idx_;
  int64_t inflight_task_cnt_;
  int64_t finished_tablet_cnt_;
  int errcode_;
  ObTabletReplayItem *total_tablet_item_arr_;
  int64_t total_tablet_cnt_;
  ObTabletRepalyOperationType replay_type_;
  ObTabletReplayCreateTask *aggrgate_task_;
  ObTabletReplayCreateTask *discrete_task_;
};


} // namespace storage
} // namespace oceanbase

#endif
