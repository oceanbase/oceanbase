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

 #include <gmock/gmock.h>

 #define USING_LOG_PREFIX STORAGE

 #define protected public
 #define private public

#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/schema_utils.h"
#include "storage/test_dml_common.h"
#include "storage/test_tablet_helper.h"
#include "src/storage/slog_ckpt/ob_tenant_slog_checkpoint_workflow.h"

#include <map>

namespace oceanbase
{
using namespace common;
using namespace storage;
using omt::ObTenant;

static const int64_t INNER_TABLET_CNT = 4;
static const uint64_t TEST_TENANT_ID = 1;

static bool VERBOSE = true;
static FILE *VERBOSE_OUT = stdout;

const char *TAG = "TestTenantSlogCkpt";

#define ASSERT_SUCC(expr)                \
 do {                                    \
    ASSERT_EQ(OB_SUCCESS, ret = (expr)); \
 } while(0);                             \

 #define EXPECT_SUCC(expr)               \
 do {                                    \
    EXPECT_EQ(OB_SUCCESS, ret = (expr)); \
 } while(0);                             \

 #define LOG_AND_PRINT(level, fmt, args...) \
  do {                                      \
    LOG_##level(fmt, args);                 \
    if (VERBOSE) {                          \
      fprintf(VERBOSE_OUT, fmt"\n");      \
    }                                       \
  } while(0);                               \

const auto get_tablet_from_ls = std::bind(
  &ObLS::get_tablet,
  std::placeholders::_1,
  std::placeholders::_2,
  std::placeholders::_3,
  ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US * 10,
  ObMDSGetTabletMode::READ_WITHOUT_CHECK);

static bool verify_tablet_num(const int64_t &expected_tablet_num)
{
  bool all_cleaned = false;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  while (!all_cleaned) {
      t3m->gc_tables_in_queue(all_cleaned);
  }
  if (VERBOSE) {
      fprintf(VERBOSE_OUT, "total tablet count:%ld\n", t3m->tablet_map_.map_.size());
  }
  if (t3m->tablet_buffer_pool_.inner_allocated_num_ != INNER_TABLET_CNT) {
      return false;
  }
  return expected_tablet_num + t3m->tablet_buffer_pool_.inner_used_num_ == t3m->tablet_map_.map_.size();
}

struct DummyFilterOp final : public ObITabletFilterOp {
    int do_filter(const ObTabletResidentInfo &info, bool &is_skipped) override {
        is_skipped = false;
        return OB_SUCCESS;
    }
};

template<class FilterOp, class Func>
static int traverse_all_tablets(FilterOp &&filter_op, Func &&func)
{
  int ret = OB_SUCCESS;
  common::ObSharedGuard<ObLSIterator> ls_iter;
  ObLS *ls = nullptr;
  if (OB_FAIL(MTL(ObLSService*)->get_ls_iter(ls_iter, ObLSGetMod::STORAGE_MOD))) {
      LOG_AND_PRINT(WARN, "failed to get log stream iter", K(ret));
      return ret;
  }

  ObLSTabletFastIter tablet_iter(filter_op, ObMDSGetTabletMode::READ_WITHOUT_CHECK);

  while (OB_SUCC(ret)) {
      ls = nullptr;
      if (OB_FAIL(ls_iter->get_next(ls))) {
          if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
          }
          LOG_AND_PRINT(WARN, "failed to get next log stream", K(ret));
          return ret;
      }
      EXPECT_NE(nullptr, ls);

      tablet_iter.reset();
      if (OB_FAIL(ls->get_tablet_svr()->build_tablet_iter(tablet_iter, false))) {
          LOG_AND_PRINT(WARN, "failed to build tablet iterator", K(ret), KPC(ls));
          return ret;
      }

      ObTabletHandle tablet_handle;
      ObTablet *tablet = nullptr;
      while (OB_SUCC(ret)) {
          tablet_handle.reset();
          if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
              if (OB_ITER_END == ret) {
                  ret = OB_SUCCESS;
                  break;
              }
              LOG_AND_PRINT(WARN, "failed to get next tablet", K(ret));
              return ret;
          }
          func(tablet_handle);
      }
  }
  return ret;
}

static int get_slog_cursor(ObLogCursor &log_cursor)
{
  return MTL(ObTenantStorageMetaService*)->get_slogger().get_active_cursor(log_cursor);
}

static int64_t SLOG_CURSOR_SAVEPOINT()
{
  int ret = OB_SUCCESS;
  int64_t res = INT_MAX64;
  static ObLogCursor last_cursor;
  ObLogCursor current_cursor;
  EXPECT_SUCC(get_slog_cursor(current_cursor));
  if (last_cursor.is_valid()) {
      res = current_cursor.log_id_ - last_cursor.log_id_;
  }
  last_cursor = current_cursor;
  return res;
}

static void force_mark_and_sweep()
{
  if (VERBOSE) {
      fprintf(VERBOSE_OUT, "prepare to mark and sweep marco blocks...\n");
  }
  OB_SERVER_BLOCK_MGR.mark_and_sweep();
  if (VERBOSE) {
      fprintf(VERBOSE_OUT, "mark and sweep finished\n");
  }
}

static double cal_shared_macro_block_size_amp(bool ignore_current_block)
{
  int ret = OB_SUCCESS;
  ObTenantSlogCkptUtil::TabletDefragmentPicker picker(ObMemAttr(MTL_ID(), TAG));
  EXPECT_SUCC(picker.create(1024));
  traverse_all_tablets(ObTenantSlogCkptUtil::DiskedTabletFilterOp(), [&](const ObTabletHandle &tablet_handle){
          ObTablet *tablet = tablet_handle.get_obj();
          EXPECT_NE(nullptr, tablet);
          if (tablet->is_empty_shell()) {
              return;
          }
          ObTabletMapKey key(tablet->get_ls_id(), tablet->get_tablet_id());
          ObTabletStorageParam param;
          param.tablet_key_ = key;
          param.original_addr_ = tablet->get_tablet_addr();

          if (ignore_current_block) {
              MacroBlockId cur_block_id;
              MTL(ObTenantStorageMetaService*)->get_shared_object_raw_reader_writer().get_cur_shared_block(cur_block_id);
              if (param.original_addr_.block_id() == cur_block_id) {
                  return;
              }
          }

          EXPECT_SUCC(picker.add_tablet(param));
      });
  return ObTenantSlogCkptUtil::cal_size_amplification(picker.block_num(), picker.total_tablet_size());
}

template<typename T>
static bool random_select_n(
  const std::set<T> &in_set,
  const int64_t n,
  std::set<T> &out_set)
{
  if (n > in_set.size()) {
      return false;
  }
  ObRandom random;
  random.seed(ObTimeUtility::current_time());

  std::vector<T> copy(in_set.cbegin(), in_set.cend());

  for (int64_t i = 0; i < n; ++i) {
      int64_t idx = random.get(i, copy.size() - 1);
      std::swap(copy[i], copy[idx]);
      out_set.insert(copy[i]);
  }
  return out_set.size() == (size_t)n;
}


static int mock_sstable(
  ObArenaAllocator &allocator,
  const ObTableSchema &table_schema,
  const ObITable::TableType &type,
  const ObTabletID &tablet_id,
  const int64_t base_version,
  const int64_t snapshot_version,
  ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;

  ObITable::TableKey table_key;
  {
    table_key.reset();
    table_key.tablet_id_ = tablet_id;
    table_key.table_type_ = type;
    table_key.version_range_.base_version_ = base_version;
    table_key.version_range_.snapshot_version_ = snapshot_version;
  }

  ObTabletCreateSSTableParam param;
  ObSSTable *sstable = nullptr;

  ObStorageSchema storage_schema;
  if (OB_FAIL(storage_schema.init(allocator, table_schema, lib::Worker::CompatMode::MYSQL))) {
    LOG_WARN("failed to init storage schema", K(ret));
  } else if (OB_FAIL(param.init_for_empty_major_sstable(tablet_id, storage_schema, 100, -1, false, false))) {
    LOG_WARN("failed to build create sstable param", K(ret), K(table_key));
  } else {
    param.table_key_ = table_key;
    param.max_merged_trans_version_ = 200;
    param.filled_tx_scn_ = table_key.get_end_scn();
    if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param, allocator, table_handle))) {
      LOG_WARN("failed to create sstable", K(param));
    }
  }

  if (FAILEDx(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), K(table_handle));
  }
  return ret;
}

static int build_update_table_store_param(
  ObArenaAllocator &allocator,
  const ObTableSchema &schema,
  const ObLSHandle &ls_handle,
  const ObTabletHandle &tablet_handle,
  const compaction::ObMergeType &merge_type,
  ObTableHandleV2 &table_handle, // for sstable life
  ObUpdateTableStoreParam &param)
{
  int ret = OB_SUCCESS;
  ObITable::TableType table_type = merge_type == compaction::ObMergeType::MDS_MINOR_MERGE ? ObITable::TableType::MDS_MINOR_SSTABLE : ObITable::TableType::MAJOR_SSTABLE;
  ObSSTable *sstable = nullptr;
  if (OB_FAIL(mock_sstable(
      allocator, schema,
      table_type,
      tablet_handle.get_obj()->get_tablet_id(), 0, 200, table_handle))) {
    LOG_WARN("failed to generate new sstable", K(ret), K(schema), KPC(tablet_handle.get_obj()));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), K(table_handle));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable should not be nullptr");
  } else {
    SCN clog_checkpoint_scn = SCN::min_scn();
    param.snapshot_version_ = tablet_handle.get_obj()->get_snapshot_version();
    param.multi_version_start_ = tablet_handle.get_obj()->get_multi_version_start();

    ObStorageSchema *schema_on_tablet = NULL;
    if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(allocator, schema_on_tablet))) {
      LOG_WARN("failed to load storage schema", K(ret), K(tablet_handle));
    } else {
      param.storage_schema_ = schema_on_tablet;
    }

    param.rebuild_seq_ = ls_handle.get_ls()->get_rebuild_seq();
    const bool need_check_sstable = true;
    param.ddl_info_.update_with_major_flag_ = false;

    param.sstable_ = sstable;
    param.allow_duplicate_sstable_ = true;

    if (FAILEDx(param.init_with_ha_info(ObHATableStoreParam(
            tablet_handle.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_,
            need_check_sstable,
            true /*need_check_transfer_seq*/)))) {
      LOG_WARN("failed to init with ha info", KR(ret));
    } else if (OB_FAIL(param.init_with_compaction_info(ObCompactionTableStoreParam(
                      merge_type,
                      clog_checkpoint_scn,
                      false /*need_report*/,
                      tablet_handle.get_obj()->has_truncate_info())))) {
      LOG_WARN("failed to init with compaction info", KR(ret));
    } else {
      LOG_INFO("success to init ObUpdateTableStoreParam", KR(ret), K(param), KPC(param.sstable_));
    }
  }
  return ret;
}

static int check_empty_shell(const ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;
  ObTablet tmp_tablet;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), TAG));
  if (OB_FAIL(MTL(ObTenantStorageMetaService*)->read_from_disk(addr, /*ls_epoch*/0, allocator, buf, buf_len))) {
    LOG_AND_PRINT(WARN, "failed to read empty shell tablet from disk", K(ret), K(addr));
  } else if (tmp_tablet.deserialize(buf, buf_len, pos)) {
    LOG_AND_PRINT(WARN, "failed to deserialize empty shell tablet", K(ret));
  } else if (!tmp_tablet.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_AND_PRINT(WARN, "unexpected invalid empty shell tablet", K(ret));
  } else if (!tmp_tablet.is_empty_shell()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_AND_PRINT(WARN, "unexpected not empty shell tablet", K(ret));
  }
  return ret;
}

static int check_tablet_addr(const ObTabletMapKey &tablet_key, const ObMetaDiskAddr &tablet_addr)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr &t3m = *MTL(ObTenantMetaMemMgr*);
  ObTabletHandle tablet_handle;
  ObMetaDiskAddr addr_from_t3m;
  ObLSHandle ls_handle;
  if (OB_FAIL(MTL(ObLSService*)->get_ls(tablet_key.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_AND_PRINT(WARN, "failed to get ls", K(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_key.tablet_id_, tablet_handle,
    ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
    ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_AND_PRINT(WARN, "failed to get tablet", K(ret), K(tablet_key));
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet_handle is invalid", K(ret));
  } else if (FALSE_IT(addr_from_t3m = tablet_handle.get_obj()->tablet_addr_)) {
  } else if (!tablet_addr.is_equal_for_persistence(addr_from_t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_AND_PRINT(WARN, "tablet addr mismatch", K(ret), K(tablet_key), K(tablet_addr), K(addr_from_t3m));
  }
  return ret;
}

using TabletAddrMap = std::map<ObLSID, std::map<ObTabletID, ObMetaDiskAddr>>;

struct SlogChecker final: public ObTenantCheckpointSlogHandler
{
public:
  SlogChecker(
    TabletAddrMap &tablet_addrs,
    std::set<ObLSID> &ls_ids)
    : tablet_addrs_(tablet_addrs),
      ls_ids_(ls_ids)
  {
  }

  int replay(const ObRedoModuleReplayParam &param) override
  {
    int ret = OB_SUCCESS;
    ObRedoLogMainType main_type = ObRedoLogMainType::OB_REDO_LOG_MAX;
    enum ObRedoLogSubType sub_type;
    ObIRedoModule::parse_cmd(param.cmd_, main_type, sub_type);
    if (OB_UNLIKELY(!param.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(param));
    } else if (ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE != main_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wrong redo log main type.", K(ret), K(main_type), K(sub_type));
    } else {
      switch (sub_type) {
        case ObRedoLogSubType::OB_REDO_LOG_CREATE_LS_ABORT:
        case ObRedoLogSubType::OB_REDO_LOG_DELETE_LS: {
          if (OB_FAIL(inner_remove_ls(param))) {
            LOG_WARN("fail to remove ls", K(param));
          }
          break;
        }
        case ObRedoLogSubType::OB_REDO_LOG_UPDATE_TABLET: {
          if (OB_FAIL(inner_check_update_tablet(param))) {
            LOG_WARN("failed to check update tablet slog", K(ret), K(param));
          }
          break;
        }
        case ObRedoLogSubType::OB_REDO_LOG_EMPTY_SHELL_TABLET: {
          if (OB_FAIL(inner_check_empty_shell(param))) {
            LOG_WARN("failed to check empty shell", K(ret), K(param));
          }
          break;
        }
        case ObRedoLogSubType::OB_REDO_LOG_DELETE_TABLET: {
          if (OB_FAIL(inner_check_removed_tablet(param))) {
            LOG_WARN("failed to check removed tablet", K(ret), K(param));
          }
          break;
        }
        case ObRedoLogSubType::OB_REDO_LOG_PUT_OLD_TABLET: {
          if (OB_FAIL(inner_check_put_old_tablet(param))) {
            LOG_WARN("fail to check put old tablet slog", K(param));
          }
          break;
        }
        default:{}
      }
    }
    return ret;
  }

  int inner_check_update_tablet(const ObRedoModuleReplayParam &param)
  {
    int ret = OB_SUCCESS;
    ObUpdateTabletLog slog;
    ObTabletMapKey tablet_key;
    int64_t pos = 0;
    if (OB_FAIL(slog.deserialize(param.buf_, param.data_size_, pos))) {
      LOG_WARN("fail to deserialize create tablet slog", K(ret), K(param), K(slog));
    } else if (OB_UNLIKELY(!slog.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("slog is invalid", K(ret), K(slog));
    } else {
      tablet_key.ls_id_ = slog.ls_id_;
      tablet_key.tablet_id_ = slog.tablet_id_;
      const ObMetaDiskAddr &tablet_addr = slog.disk_addr_;
      tablet_addrs_[tablet_key.ls_id_][tablet_key.tablet_id_] = tablet_addr;
      if (verbose_) {
        LOG_INFO("add tablet to tablet addrs map", K(tablet_key), K_(param.disk_addr));
      }
    }
    return ret;
  }

  int inner_check_empty_shell(const ObRedoModuleReplayParam &param)
  {
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    ObEmptyShellTabletLog slog;
    if (OB_FAIL(slog.deserialize_id(param.buf_, param.disk_addr_.size(), pos))) {
      STORAGE_LOG(WARN, "failed to serialize tablet_id_", K(ret), K(param.disk_addr_.size()), K(pos));
    } else {
      const ObTabletMapKey tablet_key(slog.ls_id_, slog.tablet_id_);
      const ObMetaDiskAddr &tablet_addr = param.disk_addr_;
      tablet_addrs_[tablet_key.ls_id_][tablet_key.tablet_id_] = tablet_addr;
      if (verbose_) {
        LOG_INFO("add empty shell tablet to tablet addrs map", K(tablet_key), K_(param.disk_addr));
      }
    }
    return ret;
  }

  int inner_check_removed_tablet(const ObRedoModuleReplayParam &param)
  {
    int ret = OB_SUCCESS;
    ObDeleteTabletLog slog_entry;
    int64_t pos = 0;
    if (OB_FAIL(slog_entry.deserialize(param.buf_, param.disk_addr_.size(), pos))) {
      LOG_WARN("fail to deserialize delete tablet slog", K(param), K(pos));
    } else {
      const ObTabletMapKey tablet_key(slog_entry.ls_id_, slog_entry.tablet_id_);
      tablet_addrs_[tablet_key.ls_id_].erase(tablet_key.tablet_id_);
      if (verbose_) {
        LOG_INFO("remove tablet from tablet addrs map", K(tablet_key), K_(param.disk_addr));
      }
    }
    return ret;
  }

  int inner_check_put_old_tablet(const ObRedoModuleReplayParam &param)
  {
    int ret = OB_SUCCESS;
    ObTabletMapKey tablet_key;
    if (OB_FAIL(ObTablet::deserialize_id(param.buf_, param.data_size_, tablet_key.ls_id_, tablet_key.tablet_id_))) {
      LOG_WARN("fail to deserialize log stream id and tablet id", K(ret));
    } else {
      tablet_addrs_[tablet_key.ls_id_][tablet_key.tablet_id_] = param.disk_addr_;
      if (verbose_) {
        LOG_INFO("add tablet to tablet addrs map", K(tablet_key), K_(param.disk_addr));
      }
    }
    return ret;
  }

  int inner_remove_ls(const ObRedoModuleReplayParam &param)
  {
    int ret = OB_SUCCESS;
    ObLSID ls_id;
    ObLSIDLog slog_entry(ls_id);
    int64_t pos = 0;
    const bool is_replay = true;
    if (OB_FAIL(slog_entry.deserialize(param.buf_, param.disk_addr_.size(), pos))) {
      LOG_WARN("fail to deserialize remove log stream slog", K(param), K(pos));
    } else {
      ls_ids_.erase(ls_id);
      if (verbose_) {
        LOG_INFO("remove ls", K(ls_id));
      }
      tablet_addrs_.erase(ls_id);
    }
    return ret;
  }

public:
  TabletAddrMap &tablet_addrs_;
  std::set<ObLSID> &ls_ids_;
  bool verbose_ = false;
};

struct SlogCheckpointTabletChecker final
{
public:
  SlogCheckpointTabletChecker(TabletAddrMap &tablet_addrs)
    : tablet_addrs_(tablet_addrs)
  {
  }

  int operator()(const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len)
  {
    int ret = OB_SUCCESS;
    ObUpdateTabletLog slog;
    int64_t pos = 0;
    if (OB_FAIL(slog.deserialize(buf, buf_len, pos))) {
      LOG_WARN("fail to deserialize create tablet slog", K(ret), K(pos), K(buf_len), K(slog));
    } else if (OB_UNLIKELY(!slog.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("slog is invalid", K(ret), K(slog));
    } else {
      ObTabletMapKey tablet_key(slog.ls_id_, slog.tablet_id_);
      if (tablet_addrs_[slog.ls_id_].count(slog.tablet_id_) > 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_AND_PRINT(WARN, "tablet already being replayed", K(ret), K(tablet_key), K_(slog.disk_addr));
      } else {
        tablet_addrs_[slog.ls_id_][slog.tablet_id_] = slog.disk_addr_;
        //LOG_AND_PRINT(WARN, "add tablet to addrs map", K(tablet_key), K_(slog.disk_addr));
      }
    }
    return ret;
  }

public:
  std::map<ObLSID, std::map<ObTabletID, ObMetaDiskAddr>> &tablet_addrs_;
};

struct SlogCheckpointLSChecker final
{
public:
  SlogCheckpointLSChecker(
    std::set<ObLSID> &ls_ids,
    SlogCheckpointTabletChecker &tablet_checker)
    : ls_ids_(ls_ids),
      tablet_checker_(tablet_checker)
  {
  }

  int operator()(const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len)
  {
    int ret = OB_SUCCESS;
    UNUSEDx(addr);
    ObLSCkptMember ls_ckpt_member;
    int64_t pos = 0;
    if (OB_ISNULL(buf)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (OB_FAIL(ls_ckpt_member.deserialize(buf, buf_len, pos))) {
      LOG_WARN("fail to deserialize ls_ckpt_member", K(ret), KP(buf), K(buf_len));
    } else if (OB_UNLIKELY(!ls_ckpt_member.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid ls ckpt member", K(ret), K(ls_ckpt_member));
    } else {
      ls_ids_.insert(ls_ckpt_member.ls_meta_.ls_id_);
      ObTenantStorageCheckpointReader tablet_ckpt_reader;
       ObSArray<MacroBlockId> tmp_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("ReplayCKPT", MTL_ID()));
      if (OB_FAIL(tablet_ckpt_reader.iter_read_meta_item(ls_ckpt_member.tablet_meta_entry_,
        tablet_checker_, tmp_block_list))) {
        LOG_WARN("failed to check ls tablet", K(ret));
      }
    }
    return ret;
  }

public:
  std::set<ObLSID> &ls_ids_;
  SlogCheckpointTabletChecker &tablet_checker_;
};

/// ONLY SUPPORT For New Ver.
static int check_consistency_by_replay_slog()
{
  if (VERBOSE) {
    fprintf(VERBOSE_OUT, "start checking consistency...\n");
  }

  int ret = OB_SUCCESS;
  common::ObSharedGuard<ObLSIterator> ls_iter;
  ObLS *ls = nullptr;

  //lock all log streams
  std::vector<std::shared_ptr<common::ObBucketWLockAllGuard>> lock_guards;
  std::vector<std::shared_ptr<ObLSLockGuard>> ls_lock_guards;
  if (OB_FAIL(MTL(ObLSService*)->get_ls_iter(ls_iter, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream iter", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      ls = nullptr;
      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next log stream", K(ret));
        }
      }

      if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ls", K(ret), KP(ls));
        break;
      }
      if (OB_SUCC(ret)) {
        ls_lock_guards.emplace_back(std::make_shared<ObLSLockGuard>(ls, ls->lock_, 0, LSLOCKALL));
        lock_guards.emplace_back(std::make_shared<common::ObBucketWLockAllGuard>(ls->get_tablet_svr()->bucket_lock_));
        if (VERBOSE) {
          fprintf(VERBOSE_OUT, "lock ls@%ld\n", ls->get_ls_id().id());
        }
      }
    }
  }

  for (const auto &lock_guard : lock_guards) {
    if (OB_FAIL(lock_guard->get_ret())) {
      LOG_AND_PRINT(WARN, "failed to hold ls bucket lock");
      break;
    }
  }

  if (OB_FAIL(ret)) {
    return ret;
  }

  // temporary stop slogger
  auto &slogger = MTL(ObTenantStorageMetaService*)->get_slogger();

  std::set<ObLSID> ls_ids;
  TabletAddrMap tablet_addrs;
  ObTenant *tenant = static_cast<ObTenant*>(share::ObTenantEnv::get_tenant());
  ObTenantSuperBlock super_block = tenant->get_super_block();
  if (OB_UNLIKELY(!super_block.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_AND_PRINT(WARN, "unexpected invalid superblock", K(ret), K(super_block));
    return ret;
  }
  LOG_INFO("check superblock", K(super_block));
  // check checkpoint
  SlogCheckpointTabletChecker tablet_checker(tablet_addrs);
  SlogCheckpointLSChecker ls_checker(ls_ids, tablet_checker);
  ObSArray<MacroBlockId> meta_block_list(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("ReplayCKPT", MTL_ID()));
  ObTenantStorageCheckpointReader tenant_storage_ckpt_reader;
  if (OB_FAIL(ObTenantStorageCheckpointReader::iter_read_meta_item(super_block.ls_meta_entry_, ls_checker, meta_block_list))) {
    LOG_AND_PRINT(WARN, "failed to check ls meta", K(ret));
  } else {
    // check ls exists
    for (const ObLSID &ls_id : ls_ids) {
      ObLSHandle ls_handle;
      if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
        LOG_AND_PRINT(WARN, "ls not exists", K(ret));
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    // check slog
    SlogChecker slog_checker(tablet_addrs, ls_ids);
    ObStorageLogReplayer replayer;
    ObLogCursor replay_finish_point;
    blocksstable::ObLogFileSpec log_file_spec;
    {
      log_file_spec.retry_write_policy_ = "normal";
      log_file_spec.log_create_policy_ = "normal";
      log_file_spec.log_write_policy_ = "truncate";
    }
    if (OB_FAIL(replayer.init(slogger.get_dir(), log_file_spec))) {
      LOG_AND_PRINT(WARN, "failed to init slog replayer", K(ret));
    } else if (OB_FAIL(replayer.register_redo_module(ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE, &slog_checker))) {
      LOG_AND_PRINT(WARN, "failed to register checker", K(ret));
    } else if (OB_FAIL(replayer.replay(super_block.replay_start_point_, replay_finish_point, MTL_ID()))) {
      LOG_AND_PRINT(WARN, "failed to check slog", K(ret));
    } else {
      // check tablet addrs
      int64_t total_tablet_cnt = 0;
      for (const auto &ls_and_tablet : tablet_addrs) {
        total_tablet_cnt += ls_and_tablet.second.size();
        for (const auto &tablet_and_addr : ls_and_tablet.second) {
          const ObTabletMapKey key(ls_and_tablet.first, tablet_and_addr.first);
          if (OB_FAIL(check_tablet_addr(key, tablet_and_addr.second))) {
            LOG_AND_PRINT(WARN, "failed to check tablet addr", K(ret));
            break;
          }
        }
      }

      if (FAILEDx(verify_tablet_num(total_tablet_cnt))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_AND_PRINT(WARN, "tablet cnt mismatch!", K(total_tablet_cnt));
      }
    }
  }

  if (VERBOSE) {
    fprintf(VERBOSE_OUT, "check consistency finished\n");
  }
  return ret;
}


class TabletVersionChain {
private:
  struct TabletVersion {
    bool deleted;
    ObTabletHandle handle;
    TabletVersion *next;
    TabletVersion(bool _deleted = false, TabletVersion *_next = nullptr): deleted(_deleted), next(_next) {}
    void set(const ObTabletHandle &other)
    {
      EXPECT_EQ(OB_SUCCESS, handle.assign(other));
    }
  };

public:
  TabletVersionChain();
  ~TabletVersionChain();
  void append(const ObTabletHandle &handle);
  void append_deleted_tablet();
  bool get_tablet(ObTabletHandle &handle) const;
  bool check_tablet(const ObTabletHandle &handle, bool &is_old) const;
private:
  TabletVersion *dummy_head_;
  TabletVersion *tail_;
};

TabletVersionChain::TabletVersionChain()
{
  void *buf = common::ob_malloc(sizeof(TabletVersion), TAG);
  EXPECT_NE(nullptr, buf);
  dummy_head_ = new(buf) TabletVersion;
  tail_ = dummy_head_;
}

TabletVersionChain::~TabletVersionChain()
{
  TabletVersion *cur = dummy_head_;
  while (nullptr != cur) {
    TabletVersion *del = cur;
    cur = cur->next;
    del->~TabletVersion();
    common::ob_free(del);
  }
}

void TabletVersionChain::append(const ObTabletHandle &handle)
{
  // void *buf = common::ob_malloc(sizeof(TabletVersion), TAG);
  // EXPECT_NE(nullptr, buf);
  // TabletVersion *version = new(buf) TabletVersion();
  // version->set(handle);
  // tail_->next = version;
  // tail_ = version;
  dummy_head_->deleted = false;
  dummy_head_->handle.assign(handle);
}

void TabletVersionChain::append_deleted_tablet()
{
  // void *buf = common::ob_malloc(sizeof(TabletVersion), TAG);
  // EXPECT_NE(nullptr, buf);
  // TabletVersion *version = new(buf) TabletVersion(true);
  // tail_->next = version;
  // tail_ = version;
  dummy_head_->deleted = true;
  dummy_head_->handle.reset();
}

bool TabletVersionChain::get_tablet(ObTabletHandle &handle) const
{
  if (tail_ == dummy_head_) {
      return false;
  }
  if (tail_->deleted) {
      return false;
  }
  handle.assign(tail_->handle);
  return true;
}

bool TabletVersionChain::check_tablet(const ObTabletHandle &handle, bool &is_old) const
{
  if (tail_ == dummy_head_) {
      return false;
  }
  TabletVersion *cur = dummy_head_->next;
  while (cur != nullptr) {
      EXPECT_TRUE(cur->handle.type_ == ObTabletHandle::ObTabletHdlType::FROM_T3M);
      ObTablet *cur_tablet = cur->handle.get_obj();
      while (cur_tablet != nullptr) {
          if (cur_tablet == handle.get_obj()) {
              if (cur_tablet->next_tablet_ != nullptr) {
                  is_old = true;
              }
              return true;
          }
          cur_tablet = cur_tablet->next_tablet_;
      }
      cur = cur->next;
  }
  return false;
}

class TestTenantSlogCkpt : public ::testing::Test {
public:
    TestTenantSlogCkpt() = default;
    virtual ~TestTenantSlogCkpt() = default;

    virtual void SetUp() override
    {
      if (VERBOSE) {
          fprintf(VERBOSE_OUT, "check if env is inited...\n");
      }
      ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
      int ret = OB_SUCCESS;
      while (true) {
          if (!MTL(ObTenantMetaMemMgr*)->tablet_gc_queue_.is_empty()) {
              LOG_AND_PRINT(INFO, "wait t3m gc tablet clean...");
              usleep(300 * 1000); // wait 300ms
          } else {
              break;
          }
      }
    }

    virtual void TearDown() override
    {
      if (VERBOSE) {
          fprintf(VERBOSE_OUT, "prepare tear down: remove ls...\n");
      }
      for (const int64_t ls_id : LS_IDS) {
          MTL(ObLSService*)->remove_ls(ObLSID(ls_id));
      }
      LS_IDS.clear();
      tablet_map_.clear();
      if (VERBOSE) {
          fprintf(VERBOSE_OUT, "tear down finished\n");
      }
    }

    static void SetUpTestCase()
    {
      int ret = OB_SUCCESS;
      EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
      SERVER_STORAGE_META_SERVICE.is_started_ = true;
      EXPECT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
      fprintf(VERBOSE_OUT, "update tenant thread cnt up to 10\n");
      EXPECT_SUCC(share::ObTenantEnv::get_tenant()->update_thread_cnt(10));
      fprintf(VERBOSE_OUT, "disabling background checkpoint...\n");
      bool &is_write_checkpoint = MTL(ObTenantStorageMetaService*)->ckpt_slog_handler_.is_writing_checkpoint_;
      while (!ATOMIC_BCAS(&is_write_checkpoint, false, true)) {
          ob_usleep(100 * 1000);
      }
      fprintf(VERBOSE_OUT, "succeed to disable background checkpoint.\n");
    }

    static void TearDownTestCase()
    {
      bool &is_write_checkpoint = MTL(ObTenantStorageMetaService*)->ckpt_slog_handler_.is_writing_checkpoint_;
      ATOMIC_SET(&is_write_checkpoint, false);
      MockTenantModuleEnv::get_instance().destroy();
      if (VERBOSE) {
          fprintf(VERBOSE_OUT, "env destroyed\n");
      }
    }


    int create_tablet(
        ObLSHandle &ls_handle,
        const ObTabletID &tablet_id,
        ObTabletHandle &tablet_handle,
        bool persist_tablet,
        const ObTabletStatus::Status tablet_status = ObTabletStatus::NORMAL)
    {
      int ret = OB_SUCCESS;
      ObLS *ls = ls_handle.get_ls();
      if (nullptr == ls) {
          return OB_INVALID_ARGUMENT;
      }
      tablet_handle.reset();

      ObArenaAllocator allocator;
      share::schema::ObTableSchema schema;
      TestSchemaUtils::prepare_data_schema(schema);
      ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator, tablet_status);
      if (OB_SUCCESS != ret) {
        return ret;
      }
      ret = get_tablet_from_ls(ls_handle.get_ls(), tablet_id, tablet_handle);
      if (OB_SUCCESS != ret) {
        return ret;
      }
      ObTablet *tablet = tablet_handle.get_obj();
      if (nullptr == tablet) {
        return OB_INVALID_ARGUMENT;
      }
      if (persist_tablet) {
        uint64_t data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
          return ret;
        }
        const ObTabletPersisterParam persist_param(data_version, ls->get_ls_id(), ls->get_ls_epoch(), tablet_id,
            tablet->get_transfer_seq());
        ObTabletHandle tmp_handle;
        ret = ObTabletPersister::persist_and_transform_tablet(persist_param, *tablet, tmp_handle);
        if (OB_SUCCESS != ret) {
            return ret;
        }
        ObUpdateTabletPointerParam param;
        ret = tmp_handle.get_obj()->get_updating_tablet_pointer_param(param);
        if (OB_SUCCESS != ret) {
            return ret;
        }
        ret = MTL(ObTenantMetaMemMgr*)->compare_and_swap_tablet(ObTabletMapKey(ls->get_ls_id(), tablet_id), tablet_handle, tmp_handle, param);
      }
      if (OB_SUCCESS == ret) {
        ObTabletHandle tablet_handle;
        ret = get_tablet_from_ls(ls, tablet_id, tablet_handle);
        if (OB_SUCCESS != ret) {
            return ret;
        }
        tablet_map_[ls->get_ls_id()][tablet_id].append(tablet_handle);
      }
      return ret;
    }

    int create_empty_shell(
        ObLSHandle &ls_handle,
        const ObTabletID &tablet_id)
    {
      int ret = OB_SUCCESS;
      uint64_t data_version = 0;
      ObLS *ls = ls_handle.get_ls();
      if (nullptr == ls) {
        return OB_INVALID_ARGUMENT;
      }
      {
        ObTabletHandle tablet_handle;
        ret = create_tablet(ls_handle, tablet_id, tablet_handle, false, ObTabletStatus::DELETED);
        if (OB_SUCCESS != ret) {
            return ret;
        }
      }
      if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
        return ret;
      }
      ret = ls->get_tablet_svr()->update_tablet_to_empty_shell(data_version, tablet_id);
      if (OB_SUCCESS == ret) {
        ObTabletHandle tablet_handle;
        ret = get_tablet_from_ls(ls, tablet_id, tablet_handle);
        if (OB_SUCCESS != ret) {
            return ret;
        }
        tablet_map_[ls->get_ls_id()][tablet_id].append(tablet_handle);
      }
      return ret;
    }

    int update_tablet_table_store(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id)
    {
      ObLS *ls = ls_handle.get_ls();
      if (nullptr == ls) {
          return OB_INVALID_ARGUMENT;
      }
      int ret = OB_SUCCESS;
      ObUpdateTableStoreParam update_param;
      ObTableHandleV2 table_handle;
      share::schema::ObTableSchema schema;
      TestSchemaUtils::prepare_data_schema(schema);
      ObTabletHandle orig_tablet_handle;
      ret = get_tablet_from_ls(ls, tablet_id, orig_tablet_handle);
      if (OB_SUCCESS != ret) {
          return ret;
      }
      ret = build_update_table_store_param(
          ALLOCATOR,
          schema,
          ls_handle,
          orig_tablet_handle,
          compaction::ObMergeType::MAJOR_MERGE,
          table_handle,
          update_param);
      if (OB_SUCCESS != ret) {
          return ret;
      }
      {
          ObTabletHandle tmp_handle;
          ret = ls->update_tablet_table_store(tablet_id, update_param, tmp_handle);
      }
      if (OB_SUCCESS == ret) {
          ObTabletHandle tablet_handle;
          ret = get_tablet_from_ls(ls, tablet_id, tablet_handle);
          if (OB_SUCCESS != ret) {
              return ret;
          }
          tablet_map_[ls->get_ls_id()][tablet_id].append(tablet_handle);
      }
      return ret;
    }

    int remove_tablet(
        ObLSHandle &ls_handle,
        const ObTabletID &tablet_id)
    {
      int ret = OB_SUCCESS;
      ObLS *ls = ls_handle.get_ls();
      if (nullptr == ls) {
          return OB_INVALID_ARGUMENT;
      }
      ret = ls->get_tablet_svr()->do_remove_tablet(ls->get_ls_id(), tablet_id);
      if (OB_SUCCESS != ret) {
          return ret;
      }
      tablet_map_[ls->get_ls_id()][tablet_id].append_deleted_tablet();
      return ret;
    }

    int create_ls(int64_t &ls_id, ObLSHandle &ls_handle)
    {
      static int64_t inner_ls_id = 100;
      int ret = TestDmlCommon::create_ls(TEST_TENANT_ID, ObLSID(inner_ls_id + 1), ls_handle);
      if (OB_SUCCESS != ret) {
          return ret;
      }
      ls_id = ++inner_ls_id;
      LS_IDS.insert(ls_id);
      // add ls inner tablet into version chain
      ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
      DummyFilterOp filter_op;
      ObLSTabletFastIter tablet_iter(filter_op, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
      if (OB_FAIL(ls_handle.get_ls()->build_tablet_iter(tablet_iter))) {
          if (VERBOSE) {
              fprintf(VERBOSE_OUT, "failed to build tablet iter(ret:%d)\n", ret);
          }
          return ret;
      }
      while (OB_SUCC(ret)) {
          ObTabletHandle tablet_handle;
          if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
              if (OB_ITER_END == ret) {
                  ret = OB_SUCCESS;
                  break;
              }
              LOG_AND_PRINT(WARN, "failed to get next tablet", K(ret));
              return ret;
          }
          ObTablet *tablet = tablet_handle.get_obj();
          // if (VERBOSE) {
          //     fprintf(VERBOSE_OUT, "add inner tablet(ls_id:%ld, tablet_id:%ld) into version chain\n", tablet->get_ls_id().id(),
          //         tablet->get_tablet_id().id());
          // }
          EXPECT_NE(nullptr, tablet);
          tablet_map_[tablet->get_ls_id()][tablet->get_tablet_id()].append(tablet_handle);
      }
      return ret;
    }

    int get_ls(const int64_t ls_id, ObLSHandle &ls_handle)
    {
      return MTL(ObLSService*)->get_ls(ObLSID(ls_id), ls_handle, ObLSGetMod::STORAGE_MOD);
    }

    bool check_tablet_exists(const int64_t ls_id, const int64_t tablet_id) const
    {
      if (tablet_map_.count(ObLSID(ls_id)) == 0) {
          return false;
      }
      if (tablet_map_[ObLSID(ls_id)].count(ObTabletID(tablet_id)) == 0) {
          return false;
      }
      return true;
    }

    void write_nblocks(const int64_t ls_id, int64_t start_tablet_id, const int64_t block_num, std::set<int64_t> &tablet_ids);

    void update_tablets_until_reach_threshold(ObLSHandle &ls_handle, int64_t &start_tablet_id, const std::set<int64_t>& update_tablet_ids)
    {
      int ret = OB_SUCCESS;
      if (VERBOSE) {
          fprintf(VERBOSE_OUT, "start updating tablets...\n");
      }
      while (cal_shared_macro_block_size_amp(true) < ObTenantSlogCheckpointWorkflow::TabletDefragmentHelper::SIZE_AMPLIFICATION_THRESHOLD + 0.2) {
        ObRandom random;
        random.seed(ObTimeUtility::current_time());
        for (const int64_t &id : update_tablet_ids) {
            EXPECT_SUCC(update_tablet_table_store(ls_handle, ObTabletID(id)));
        }
        ObTabletHandle tablet_handle;
        EXPECT_SUCC(create_tablet(ls_handle, ObTabletID(++start_tablet_id), tablet_handle, random.get(0, 2) < 1));
        EXPECT_SUCC(create_empty_shell(ls_handle, ObTabletID(++start_tablet_id)));
      }
      if (VERBOSE) {
        fprintf(VERBOSE_OUT, "size amp after update:%lf\n", cal_shared_macro_block_size_amp(true));
      }
    }

public:
    static std::set<int64_t> LS_IDS;

    static ObArenaAllocator ALLOCATOR;

public:
    mutable std::map<ObLSID, std::map<ObTabletID, TabletVersionChain>> tablet_map_;
};

void TestTenantSlogCkpt::write_nblocks(
  const int64_t ls_id,
  int64_t start_tablet_id,
  const int64_t block_num,
  std::set<int64_t> &tablet_ids)
{
  constexpr int64_t block_size = OB_DEFAULT_MACRO_BLOCK_SIZE;
  constexpr int64_t min_tablet_occupied_size = DIO_READ_ALIGN_SIZE;
  constexpr int64_t max_tablets_per_block = block_size / min_tablet_occupied_size;
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  EXPECT_SUCC(get_ls(ls_id, ls_handle));

  int64_t tablets_num = max_tablets_per_block * block_num;
  for (int64_t i = 0; i < tablets_num; ++i) {
      ObTabletHandle tablet_handle;
      EXPECT_SUCC(create_tablet(ls_handle, ObTabletID(start_tablet_id), tablet_handle, true));
      tablet_ids.insert(start_tablet_id);
      ++start_tablet_id;
  }
}

std::set<int64_t> TestTenantSlogCkpt::LS_IDS;

ObArenaAllocator TestTenantSlogCkpt::ALLOCATOR(TAG);


TEST_F(TestTenantSlogCkpt, test_disked_filter) {
  int ret = OB_SUCCESS;
  int64_t expected_tablet_num = 0;
  int64_t ls_id = -1;
  ObLSHandle ls_handle;
  std::set<int64_t> disked_tablet_ids;

  SLOG_CURSOR_SAVEPOINT();

  ASSERT_SUCC(create_ls(ls_id, ls_handle));
  ls_handle.reset();
  ASSERT_SUCC(get_ls(ls_id, ls_handle));
  EXPECT_NE(nullptr, ls_handle.get_ls());
  EXPECT_EQ(ObLSID(ls_id), ls_handle.get_ls()->get_ls_id());

  {
    ObTabletHandle tablet_handle;
    ASSERT_SUCC(create_tablet(ls_handle, ObTabletID(1001), tablet_handle, true));
    disked_tablet_ids.insert(1001);
    ++expected_tablet_num;
  }
  {
    EXPECT_TRUE(check_tablet_exists(ls_id, 1001));
    ObTabletHandle tablet_handle;
    ASSERT_SUCC(get_tablet_from_ls(ls_handle.get_ls(), ObTabletID(1001), tablet_handle));
    EXPECT_TRUE(tablet_handle.is_valid());
    EXPECT_TRUE(tablet_handle.get_obj()->get_tablet_addr().is_block());
  }
  ASSERT_SUCC(create_empty_shell(ls_handle, ObTabletID(1002)));
  ++expected_tablet_num;
  disked_tablet_ids.insert(1002);

  {
    ObTabletHandle tablet_handle;
    ASSERT_SUCC(create_tablet(ls_handle, ObTabletID(1003), tablet_handle, false));
    ++expected_tablet_num;
  }

  verify_tablet_num(expected_tablet_num);
  {
    bool ok = true;
    ASSERT_SUCC(traverse_all_tablets(ObTenantSlogCkptUtil::DiskedTabletFilterOp(), [&](const ObTabletHandle &tablet_handle){
        ObCStringHelper helper;
        if (!tablet_handle.is_valid()) {
          ok = false;
          if (VERBOSE) {
            fprintf(VERBOSE_OUT, "unexpected invalid tablet_handle(%s)\n", helper.convert(tablet_handle));
          }
        }
        if (tablet_handle.get_obj()->get_tablet_addr().is_none()) {
          ok = false;
          if (VERBOSE) {
            fprintf(VERBOSE_OUT, "unexpected none addr tablet(%s)\n", helper.convert(tablet_handle));
          }
          return;
        }
        if (!tablet_handle.get_obj()->get_tablet_addr().is_disked()) {
          ok = false;
          if (VERBOSE) {
            fprintf(VERBOSE_OUT, "unexpected un-disked tablet(%s)\n", helper.convert(tablet_handle));
          }
          return;
        }
        disked_tablet_ids.erase(tablet_handle.get_obj()->get_tablet_id().id());
      }));
    EXPECT_TRUE(ok && disked_tablet_ids.empty());
  }

  EXPECT_LE(SLOG_CURSOR_SAVEPOINT(), 10);
  EXPECT_EQ(cal_shared_macro_block_size_amp(true), 0.);
}

TEST_F(TestTenantSlogCkpt, test_tablet_defragment) {
  int ret = OB_SUCCESS;
  int64_t expected_tablet_num = 0;
  int64_t ls_id = -1;
  ObLSHandle ls_handle;
  std::set<int64_t> tablet_ids;

  SLOG_CURSOR_SAVEPOINT();

  ASSERT_SUCC(create_ls(ls_id, ls_handle));
  ls_handle.reset();
  ASSERT_SUCC(get_ls(ls_id, ls_handle));
  EXPECT_NE(nullptr, ls_handle.get_ls());
  EXPECT_EQ(ObLSID(ls_id), ls_handle.get_ls()->get_ls_id());

  write_nblocks(ls_id, 1001, 3, tablet_ids);
  if (VERBOSE) {
    fprintf(VERBOSE_OUT, "slog margin since last time:%ld\n", SLOG_CURSOR_SAVEPOINT());
  }

  size_t update_tablets_num = std::floor(tablet_ids.size() * 0.1); // 10%
  std::set<int64_t> update_tablet_ids;
  EXPECT_TRUE(random_select_n(tablet_ids, update_tablets_num, update_tablet_ids));
  int64_t start_tablet_id = 1001 + (int64_t)tablet_ids.size();
  // update tablets
  update_tablets_until_reach_threshold(ls_handle, start_tablet_id, update_tablet_ids);

  // execute workflow
  ObTenantCheckpointSlogHandler &ckpt_slog_handler = MTL(ObTenantStorageMetaService*)->ckpt_slog_handler_;
  EXPECT_SUCC(ObTenantSlogCheckpointWorkflow::execute(ObTenantSlogCheckpointWorkflow::normal_type(), ckpt_slog_handler));
  if (VERBOSE) {
    fprintf(VERBOSE_OUT, "slog margin since last time:%ld, size amp:%lf\n", SLOG_CURSOR_SAVEPOINT(), cal_shared_macro_block_size_amp(true));
  }

  force_mark_and_sweep(); // force trigger mark&sweep
  ob_usleep(1000);

  ASSERT_SUCC(check_consistency_by_replay_slog());
}


TEST_F(TestTenantSlogCkpt, test_tablet_defragment_with_parallel_updating) {
  int ret = OB_SUCCESS;
  int64_t expected_tablet_num = 0;
  int64_t ls_id = -1;
  ObLSHandle ls_handle;
  std::set<int64_t> tablet_ids;

  SLOG_CURSOR_SAVEPOINT();

  ASSERT_SUCC(create_ls(ls_id, ls_handle));
  ls_handle.reset();
  ASSERT_SUCC(get_ls(ls_id, ls_handle));
  EXPECT_NE(nullptr, ls_handle.get_ls());
  EXPECT_EQ(ObLSID(ls_id), ls_handle.get_ls()->get_ls_id());

  write_nblocks(ls_id, 1001, 3, tablet_ids);

  size_t update_tablets_num = std::floor(tablet_ids.size() * 0.1); // 10%
  std::set<int64_t> update_tablet_ids;
  EXPECT_TRUE(random_select_n(tablet_ids, update_tablets_num, update_tablet_ids));
  int64_t start_tablet_id = 1001 + (int64_t)tablet_ids.size();
  // update tablets
  update_tablets_until_reach_threshold(ls_handle, start_tablet_id, update_tablet_ids);

  ObTenant *tenant = static_cast<ObTenant*>(share::ObTenantEnv::get_tenant());
  std::atomic_bool quit{false};
  std::thread update_thread([&]{
      ObTenantEnv::set_tenant(tenant);
      if (VERBOSE) {
        fprintf(VERBOSE_OUT, "start background updating tablets...\n");
      }
      std::vector<int64_t> tablet_ids_vec(tablet_ids.cbegin(), tablet_ids.cend());
      ObRandom random;
      static const int64_t timeout = 5_s;
      bool is_timeout = false;
      int64_t start_time = ObTimeUtility::current_time();
      random.seed(ObTimeUtility::current_time());
      while (!quit.load(std::memory_order_acquire)) {
        if (ObTimeUtility::current_time() - start_time >= timeout) {
          is_timeout = true;
          break;
        }
        int64_t idx = random.get(0, tablet_ids_vec.size() - 1);
        EXPECT_SUCC(update_tablet_table_store(ls_handle, ObTabletID(tablet_ids_vec[idx])));
        ob_usleep(500_ms);
      }
      if (VERBOSE) {
        fprintf(VERBOSE_OUT, "background updating thread quit gracefully(timeout:%d)\n", is_timeout);
      }
    });

  // execute workflow
  ObTenantCheckpointSlogHandler &ckpt_slog_handler = MTL(ObTenantStorageMetaService*)->ckpt_slog_handler_;
  EXPECT_SUCC(ObTenantSlogCheckpointWorkflow::execute(ObTenantSlogCheckpointWorkflow::normal_type(), ckpt_slog_handler));

  quit.store(true, std::memory_order_release);
  update_thread.join();

  force_mark_and_sweep(); // force trigger mark&sweep
  ob_usleep(1000);

  if (VERBOSE) {
    fprintf(VERBOSE_OUT, "slog margin since last time:%ld, size amp:%lf\n", SLOG_CURSOR_SAVEPOINT(), cal_shared_macro_block_size_amp(true));
  }
  ASSERT_SUCC(check_consistency_by_replay_slog());
}

TEST_F(TestTenantSlogCkpt, test_force_slog_ckpt) {
  int ret = OB_SUCCESS;
  int64_t expected_tablet_num = 0;
  int64_t ls_id = -1;
  ObLSHandle ls_handle;
  std::set<int64_t> tablet_ids;

  SLOG_CURSOR_SAVEPOINT();

  ASSERT_SUCC(create_ls(ls_id, ls_handle));
  ls_handle.reset();
  ASSERT_SUCC(get_ls(ls_id, ls_handle));
  EXPECT_NE(nullptr, ls_handle.get_ls());
  EXPECT_EQ(ObLSID(ls_id), ls_handle.get_ls()->get_ls_id());

  write_nblocks(ls_id, 1001, 3, tablet_ids);
  if (VERBOSE) {
    fprintf(VERBOSE_OUT, "slog margin since last time:%ld\n", SLOG_CURSOR_SAVEPOINT());
  }

  size_t update_tablets_num = std::floor(tablet_ids.size() * 0.1); // 10%
  std::set<int64_t> update_tablet_ids;
  EXPECT_TRUE(random_select_n(tablet_ids, update_tablets_num, update_tablet_ids));
  int64_t start_tablet_id = 1001 + (int64_t)tablet_ids.size();
  // update tablets
  update_tablets_until_reach_threshold(ls_handle, start_tablet_id, update_tablet_ids);

  ObTenant *tenant = static_cast<ObTenant*>(share::ObTenantEnv::get_tenant());
  std::atomic_bool quit{false};
  std::thread empty_shell_creating_thead([&]{
      ObTenantEnv::set_tenant(tenant);
      if (VERBOSE) {
        fprintf(VERBOSE_OUT, "start background creating empty shells...\n");
      }
      ObRandom random;
      random.seed(ObTimeUtility::current_time());
      static const int64_t timeout = 5_s;
      bool is_timeout = false;
      int64_t start_time = ObTimeUtility::current_time();
      while (!quit.load(std::memory_order_acquire)) {
        if (ObTimeUtility::current_time() - start_time >= timeout) {
          is_timeout = true;
          break;
        }
        EXPECT_SUCC(create_empty_shell(ls_handle, ObTabletID(++start_tablet_id)));
        ob_usleep(500_ms);
      }
      if (VERBOSE) {
        fprintf(VERBOSE_OUT, "background empty shell creating thread quit gracefully(timeout:%d)\n", is_timeout);
      }
    });

  // execute workflow
  ObTenantCheckpointSlogHandler &ckpt_slog_handler = MTL(ObTenantStorageMetaService*)->ckpt_slog_handler_;
  EXPECT_SUCC(ObTenantSlogCheckpointWorkflow::execute(ObTenantSlogCheckpointWorkflow::FORCE, ckpt_slog_handler));
  // EXPECT_SUCC(force_trigger_old_ckpt());

  quit.store(true, std::memory_order_release);
  empty_shell_creating_thead.join();

  if (VERBOSE) {
    fprintf(VERBOSE_OUT, "slog margin since last time:%ld, size amp:%lf\n", SLOG_CURSOR_SAVEPOINT(), cal_shared_macro_block_size_amp(true));
  }

  force_mark_and_sweep(); // force trigger mark&sweep
  ob_usleep(1000);

  ASSERT_SUCC(check_consistency_by_replay_slog());
}

using PSTHdl = ObTenantSlogCkptUtil::ParallelStartupTaskHandler;

using PITask = PSTHdl::ITask;

TEST_F(TestTenantSlogCkpt, test_psth_basic) {
  int ret = OB_SUCCESS;
  struct DummyTask final: public PITask {
    DummyTask(PSTHdl &hdl): PITask(hdl), task_id_(0), task_ids_(nullptr) {}

    int execute() override {
      if (!is_inited_) {
        return OB_NOT_INIT;
      }
      if (nullptr == task_ids_) {
        return OB_ERR_UNEXPECTED;
      }
      if (task_id_ >= task_ids_->size()) {
        return OB_ERR_UNEXPECTED;
      }
      {
        // do something
        ob_usleep(task_id_ * 10);
        (*task_ids_)[task_id_] = task_id_;
      }
      on_exec_succeed_();
      return OB_SUCCESS;
    }

    int init(const int64_t task_id, std::vector<int64_t> &task_ids) {
      task_id_ = task_id;
      task_ids_ = &task_ids;
      on_init_succeed_();
      return OB_SUCCESS;
    }
    int64_t task_id_;
    std::vector<int64_t> *task_ids_;
  };

  PSTHdl hdl;

  const int64_t task_num = 1000;
  std::vector<int64_t> task_ids(task_num, 0);
  for (int64_t i = 0; i < task_num; ++i) {
    ASSERT_SUCC(hdl.add_task<DummyTask>(i, task_ids));
  }

  ASSERT_SUCC(hdl.wait());

  ASSERT_EQ(task_num, hdl.get_finished_task_cnt_());
  for (int64_t i = 0; i < task_num; ++i) {
    ASSERT_EQ(task_ids[i], i);
  }
}

TEST_F(TestTenantSlogCkpt, test_psth_failed_task) {
  int ret = OB_SUCCESS;

  int64_t failed_num = 0;

  struct DummyTask final: public PITask {
    DummyTask(PSTHdl &hdl): PITask(hdl), task_id_(0), task_ids_(nullptr) {}

    int execute() override {
      if (!is_inited_) {
        return OB_NOT_INIT;
      }
      if (nullptr == task_ids_) {
        return OB_ERR_UNEXPECTED;
      }
      if (task_id_ >= task_ids_->size()) {
        return OB_ERR_UNEXPECTED;
      }
      {
        // do something
        ob_usleep(task_id_ * 10);
        (*task_ids_)[task_id_] = task_id_;
      }
      // 1/3 may failed
      if (random_.get(0, 2) >= 1) {
        on_exec_succeed_();
      } else {
        on_exec_error_(OB_ERR_NULL_VALUE);
        ATOMIC_INC(failed_num_);
      }
      return OB_SUCCESS;
    }

    int init(const int64_t task_id, std::vector<int64_t> &task_ids, int64_t &failed_num) {
      int ret = OB_SUCCESS;
      task_id_ = task_id;
      task_ids_ = &task_ids;
      random_.seed(ObTimeUtility::current_time());
      failed_num_ = &failed_num;
      on_init_succeed_();
      return ret;
    }

    ObRandom random_;
    int64_t task_id_;
    std::vector<int64_t> *task_ids_;
    int64_t *failed_num_ = nullptr;
  };

  PSTHdl hdl;

  const int64_t task_num = 1000;
  std::vector<int64_t> task_ids(task_num, 0);
  int64_t pushed_task_num = 0;
  for (int64_t i = 0; i < task_num; ++i) {
    if (OB_FAIL(hdl.add_task<DummyTask>(i, task_ids, failed_num))) {
      break;
    }
    ++pushed_task_num;
  }

  if (OB_FAIL(ret)) {
    EXPECT_EQ(hdl.get_errcode_(), ret);
    EXPECT_GT(failed_num, 0);
  }

  ret = hdl.wait();

  if (failed_num > 0) {
    if (VERBOSE) {
      fprintf(VERBOSE_OUT, "some tasks failed(%ld/%ld)\n", failed_num, pushed_task_num);
    }
    ASSERT_TRUE(hdl.get_errcode_() != OB_SUCCESS && hdl.get_errcode_() == ret);
    ASSERT_EQ(pushed_task_num, hdl.get_finished_task_cnt_() + failed_num);
  } else {
    ASSERT_SUCC(ret);
    ASSERT_EQ(task_num, hdl.get_finished_task_cnt_());
    for (int64_t i = 0; i < task_num; ++i) {
      ASSERT_EQ(task_ids[i], i);
    }
  }
}

TEST_F(TestTenantSlogCkpt, test_tablet_compat_upgrade) {
  int ret = OB_SUCCESS;
  int64_t expected_tablet_num = 0;
  int64_t ls_id = -1;
  ObLSHandle ls_handle;
  std::set<int64_t> tablet_ids;

  SLOG_CURSOR_SAVEPOINT();

  ASSERT_SUCC(create_ls(ls_id, ls_handle));
  ls_handle.reset();
  ASSERT_SUCC(get_ls(ls_id, ls_handle));
  EXPECT_NE(nullptr, ls_handle.get_ls());
  EXPECT_EQ(ObLSID(ls_id), ls_handle.get_ls()->get_ls_id());

  write_nblocks(ls_id, 1001, 10, tablet_ids);

  {
    const int64_t start_time = ObTimeUtility::current_time_ms();
    // execute workflow
    ObTenantCheckpointSlogHandler &ckpt_slog_handler = MTL(ObTenantStorageMetaService*)->ckpt_slog_handler_;
    EXPECT_SUCC(ObTenantSlogCheckpointWorkflow::execute(ObTenantSlogCheckpointWorkflow::COMPAT_UPGRADE, ckpt_slog_handler));
    int64_t duration_ms = ObTimeUtility::current_time_ms() - start_time;
    if (VERBOSE) {
      fprintf(VERBOSE_OUT, "slog margin since last time:%ld, size amp:%lf, parallel compat upgrade time costs:%ldms\n",
        SLOG_CURSOR_SAVEPOINT(),
        cal_shared_macro_block_size_amp(true),
        duration_ms);
    }
  }

  force_mark_and_sweep(); // force trigger mark&sweep
  ob_usleep(1000);

  ASSERT_SUCC(check_consistency_by_replay_slog());
}



#undef ASSERT_SUCC
#undef EXPECT_SUCC

} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tenant_slog_ckpt.log*");
  OB_LOGGER.set_file_name("test_tenant_slog_ckpt.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}