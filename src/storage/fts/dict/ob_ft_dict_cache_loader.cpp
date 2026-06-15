/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/fts/dict/ob_ft_dict_cache_loader.h"

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "ob_smart_var.h"
#include "share/scn.h"
#include "storage/fts/dict/ob_ft_cache.h"
#include "storage/fts/dict/ob_ft_cache_dict.h"
#include "storage/fts/dict/ob_ft_dat_dict.h"
#include "storage/fts/dict/ob_ft_dict_iterator.h"
#include "storage/fts/dict/ob_ft_dict_mgr.h"
#include "storage/fts/dict/ob_ft_dict_table_iter.h"
#include "storage/fts/dict/ob_ft_range_dict.h"
#include "storage/fts/dict/ob_ft_trie.h"
#include "lib/charset/ob_charset.h"
#include "storage/tx/ob_trans_service.h"

#define USING_LOG_PREFIX STORAGE_FTS

namespace oceanbase
{
namespace storage
{

static constexpr int64_t SNAPSHOT_WAIT_TIMEOUT_US = 10 * 1000 * 1000;  // 10s

ObFTDictCacheLoaderBase::ObFTDictCacheLoaderBase() : dict_mgr_(MTL(ObFTDictMgr*)), tenant_id_(MTL_ID())
{
}

int ObFTDictCacheLoaderBase::do_full_build(const ObFTDictDesc &desc,
                                            ObFTCacheRangeContainer &range_container)
{
  int ret = OB_SUCCESS;
  ObFTDictMgr::DictTableSnapshot scn_info;

  if (OB_FAIL(get_current_snapshot_version(scn_info.snapshot_version_))) {
    LOG_WARN("Failed to get current snapshot version", K(ret));
  } else if (OB_FAIL(build_ranges_from_table(desc, scn_info.snapshot_version_, range_container))) {
    LOG_WARN("Failed to build full ranges", K(ret), K(desc));
  } else if (range_container.get_handles().empty()) {
    // empty table, nothing to do
  } else if (OB_FAIL(get_table_row_scn_and_count(desc, scn_info.snapshot_version_, scn_info.row_scn_, scn_info.row_count_))) {
    if (OB_SNAPSHOT_DISCARDED != ret) {
      LOG_WARN("Failed to get current row_scn and row_count at snapshot", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(dict_mgr_->update_table_snapshot(desc.table_id_, scn_info))) {
    LOG_WARN("Failed to update table row scn info", K(ret), K(desc.table_id_));
  }

  return ret;
}

int ObFTDictCacheLoaderBase::build_ranges_from_table(const ObFTDictDesc &desc,
                                                      const int64_t snapshot_version,
                                                      ObFTCacheRangeContainer &range_container,
                                                      const ObIArray<ObMissingRangeInfo> *partial_ranges)
{
  int ret = OB_SUCCESS;

  SMART_VAR(ObISQLClient::ReadResult, result)
  {
    ObFTDictTableIter iter_table(result);
    if (OB_FAIL(iter_table.init(desc.table_name_, tenant_id_, snapshot_version, desc.need_casedown_, partial_ranges))) {
      if (OB_SNAPSHOT_DISCARDED == ret) {
        // need more build
      } else if (OB_ITER_END != ret) {
        LOG_WARN("Failed to init iterator", K(ret), K(desc));
      } else {
        ret = OB_SUCCESS;  // Empty table
      }
    } else if (OB_FAIL(build_ranges(desc, iter_table, range_container, snapshot_version, partial_ranges))) {
      LOG_WARN("Failed to build ranges", K(ret), K(desc));
    }
  }

  return ret;
}

int ObFTDictCacheLoaderBase::get_table_row_scn_and_count(const ObFTDictDesc &desc,
                                                          const int64_t snapshot_version,
                                                          int64_t &row_scn,
                                                          int64_t &row_count)
{
  int ret = OB_SUCCESS;
  row_scn = 0;
  row_count = 0;

  ObMySQLProxy *sql_proxy = MTL(transaction::ObTransService *)->get_mysql_proxy();
  ObSqlString sql_string;

  SMART_VAR(ObISQLClient::ReadResult, result)
  {
    if (OB_FAIL(sql_string.append_fmt("SELECT MAX(ORA_ROWSCN) as max_version, COUNT(*) as cnt FROM %.*s",
                                       static_cast<int>(desc.table_name_.length()), desc.table_name_.ptr()))) {
      LOG_WARN("Failed to build sql for row_scn and count", K(ret));
    } else if (snapshot_version > 0 && OB_FAIL(sql_string.append_fmt(" AS OF SNAPSHOT %ld", snapshot_version))) {
      LOG_WARN("Failed to append snapshot version", K(ret));
    } else {
      ObSessionParam session_param;
      session_param.sql_mode_ = nullptr;
      session_param.tz_info_wrap_ = nullptr;
      InnerDDLInfo ddl_info;
      ddl_info.set_is_dummy_ddl_for_inner_visibility(true);
      ddl_info.set_source_table_hidden(false);
      ddl_info.set_dest_table_hidden(false);
      if (OB_FAIL(session_param.ddl_info_.init(ddl_info, 0))) {
        LOG_WARN("fail to init ddl info", K(ret), K(ddl_info));
      } else if (OB_FAIL(sql_proxy->read(result, tenant_id_, sql_string.ptr(), &session_param))) {
        LOG_WARN("Failed to execute sql for row_scn and count", K(ret), K(sql_string), K(snapshot_version));
      }
    }
    if (OB_SUCC(ret)) {
      sqlclient::ObMySQLResult *mysql_result = result.get_result();
      if (OB_UNLIKELY(OB_ISNULL(mysql_result))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to get mysql result", K(ret));
      } else if (OB_FAIL(mysql_result->next())) {
        LOG_WARN("Failed to get next row", K(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*mysql_result, "max_version", row_scn, int64_t,
                                                    true /* skip_null_error */, false /* skip_column_error */, 0);
        EXTRACT_INT_FIELD_MYSQL(*mysql_result, "cnt", row_count, int64_t);
      }
    }
  }

  return ret;
}

int ObFTDictCacheLoaderBase::try_load_cache(const ObFTDictDesc &desc,
                                             ObFTCacheRangeContainer &range_container,
                                             ObIArray<ObMissingRangeInfo> *missing_ranges)
{
  int ret = OB_SUCCESS;
  int32_t range_count = 0;

  if (OB_INVALID_ID == desc.table_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid table_id in desc", K(ret), K(desc));
  } else {
    ObDictCacheKey first_key(desc.table_id_, tenant_id_, 0);
    ObFTCacheRangeHandle *first_info = nullptr;

    if (OB_FAIL(range_container.fetch_info_for_dict(first_info))) {
      LOG_WARN("Failed to fetch info for dict.", K(ret));
    } else if (OB_FAIL(ObDictCache::get_instance().get_dict(first_key, first_info->value_, first_info->handle_))
               && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("Failed to get first dict from kv cache", K(ret));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      // go to full build
    } else if (OB_UNLIKELY(OB_ISNULL(first_info->value_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value is null", K(ret));
    } else {
      first_info->key_ = first_key;
      range_count = first_info->value_->get_range_count();
      for (int32_t i = 1; OB_SUCC(ret) && i < range_count; ++i) {
        ObDictCacheKey key(desc.table_id_, tenant_id_, i);
        ObFTCacheRangeHandle tmp;
        ObFTCacheRangeHandle *info = nullptr;
        if (OB_FAIL(ObDictCache::get_instance().get_dict(key, tmp.value_, tmp.handle_))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("Failed to get dict from kvcache", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        } else if (OB_FAIL(range_container.fetch_info_for_dict(info))) {
          LOG_WARN("Failed to fetch info for dict", K(ret));
        } else {
          tmp.key_ = key;
          info->move_from(tmp);
        }
      }

      if (OB_SUCC(ret) && nullptr != missing_ranges) {
        const int64_t snapshot_version = first_info->value_->get_snapshot_version();
        if (OB_FAIL(handle_missing_ranges(range_container, range_count, snapshot_version, missing_ranges))) {
          LOG_WARN("Failed to handle missing ranges", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    range_container.reset();
  } else if (range_container.get_handles().size() != range_count) {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

int ObFTDictCacheLoaderBase::check_row_scn(const ObFTDictDesc &desc,
                                            bool &is_need_build,
                                            ObFTDictMgr::DictTableSnapshot &row_scn_info)
{
  int ret = OB_SUCCESS;

  is_need_build = false;
  ObFTDictMgr::DictTableSnapshot cached_info;

  if (OB_FAIL(dict_mgr_->get_table_snapshot(desc.table_id_, cached_info)) && OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("Failed to get table row scn info from cache", K(ret), K(desc.table_id_));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    is_need_build = true;
    ret = OB_SUCCESS;
  } else if (OB_FAIL(get_current_snapshot_version(row_scn_info.snapshot_version_))) {
    LOG_WARN("Failed to get current snapshot version for cache update", K(ret));
  } else if (OB_FAIL(get_table_row_scn_and_count(desc, row_scn_info.snapshot_version_, row_scn_info.row_scn_, row_scn_info.row_count_))) {
    if (OB_SNAPSHOT_DISCARDED != ret) {
      LOG_WARN("Failed to get current row_scn and row_count at snapshot", K(ret));
    } else {
      is_need_build = true;
      ret = OB_SUCCESS;
    }
  } else if (cached_info.row_scn_ != row_scn_info.row_scn_ || cached_info.row_count_ != row_scn_info.row_count_) {
    is_need_build = true;
  }

  return ret;
}

int ObFTDictCacheLoaderBase::get_first_range_handle(const ObFTCacheRangeContainer &range_container,
                                                    ObFTCacheRangeHandle *&first_handle)
{
  int ret = OB_SUCCESS;
  const ObList<ObFTCacheRangeHandle *, ObIAllocator> &handles = range_container.get_handles();

  if (OB_UNLIKELY(handles.empty())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("No cached ranges found", K(ret));
  } else {
    ObList<ObFTCacheRangeHandle *, ObIAllocator>::const_iterator first_it = handles.begin();
    first_handle = *first_it;
    if (OB_UNLIKELY(OB_ISNULL(first_handle))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("First range handle is null", K(ret));
    }
  }

  return ret;
}

int ObFTDictCacheLoaderBase::update_first_range_cache(const ObFTDictDesc &desc,
                                                      const ObFTDAT *dat_block,
                                                      const int64_t snapshot_version,
                                                      const int32_t range_count,
                                                      ObFTCacheRangeContainer &range_container)
{
  int ret = OB_SUCCESS;
  ObFTCacheRangeHandle *first_handle = nullptr;

  if (OB_FAIL(get_first_range_handle(range_container, first_handle))) {
    LOG_WARN("Failed to get first range handle", K(ret));
  } else if (OB_FAIL(ObFTCacheDict::put_and_fetch_cache_entry(
             desc.table_id_, tenant_id_, 0/*range_id = 0*/,
             dat_block, snapshot_version, range_count,
             *first_handle))) {
    LOG_WARN("Failed to update first range cache entry", K(ret), K(snapshot_version), K(range_count));
  }

  return ret;
}

int ObFTDictCacheLoaderBase::update_snapshot_version(const ObFTDictDesc &desc,
                                                      const int64_t new_snapshot_version,
                                                      ObFTCacheRangeContainer &range_container)
{
  int ret = OB_SUCCESS;
  ObFTCacheRangeHandle *first_handle = nullptr;

  if (OB_FAIL(get_first_range_handle(range_container, first_handle))) {
    LOG_WARN("Failed to get first range handle", K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(first_handle->value_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("First range handle value is null", K(ret));
  } else {
    const ObDictCacheValue *old_value = first_handle->value_;
    const ObFTDAT *dat_block = old_value->get_dat_block();
    const int32_t range_count = old_value->get_range_count();
    const int64_t blk_sz = OB_ISNULL(dat_block) ? 0 : dat_block->mem_block_size_;
    if (OB_UNLIKELY(blk_sz <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid dict cache dat block", K(ret), KP(dat_block), K(blk_sz));
    } else {
      ObArenaAllocator dup_alloc(lib::ObMemAttr(tenant_id_, "FTDictSnapDup"));
      char *dup_buf = static_cast<char *>(dup_alloc.alloc(blk_sz));
      if (OB_ISNULL(dup_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc dict dup for snapshot update failed", K(ret), K(blk_sz));
      } else {
        MEMCPY(dup_buf, dat_block, blk_sz);
        const ObFTDAT *dup_dat = reinterpret_cast<const ObFTDAT *>(dup_buf);
        if (OB_FAIL(ObFTCacheDict::put_and_fetch_cache_entry(
                desc.table_id_, tenant_id_, 0/*range_id = 0*/,
                dup_dat, new_snapshot_version, range_count,
                *first_handle))) {
          LOG_WARN("Failed to update first range cache entry", K(ret), K(new_snapshot_version), K(range_count));
        }
      }
    }
  }

  return ret;
}

int ObFTDictCacheLoaderBase::validate_cache(const ObFTDictDesc &desc,
                                             ObFTCacheRangeContainer &range_container,
                                             bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_FAIL(try_load_cache(desc, range_container))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("Failed to load cache", K(ret), K(desc));
    } else {
      range_container.reset();
      ret = OB_SUCCESS;
    }
  } else {
    is_valid = true;
  }
  return ret;
}

int ObFTDictCacheLoaderBase::refresh_cache_common(const ObFTDictDesc &desc,
                                                   ObFTDictMgr::DictTableSnapshot &row_scn_info,
                                                   ObFTCacheRangeContainer &range_container)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  bool is_need_build = false;

  if (OB_FAIL(check_row_scn(desc, is_need_build, row_scn_info))) {
    LOG_WARN("Failed to check row scn", K(ret), K(desc.table_id_));
  } else if (!is_need_build) {
    if (OB_FAIL(validate_cache(desc, range_container, is_valid))) {
      LOG_WARN("Failed to validate cache", K(ret), K(desc));
    } else if (!is_valid) {
      is_need_build = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!is_need_build) {
    if (OB_FAIL(update_snapshot_version(desc, row_scn_info.snapshot_version_, range_container))) {
      LOG_WARN("Failed to update snapshot_version", K(ret), K(desc.table_id_));
    } else if (OB_FAIL(dict_mgr_->update_table_snapshot(desc.table_id_, row_scn_info))) {
      LOG_WARN("Failed to update table row scn info", K(ret));
    }
  } else if (OB_FAIL(do_full_build(desc, range_container))) {
    LOG_WARN("Failed to build in slot", K(ret), K(desc));
  }

  return ret;
}

int ObFTDictCacheLoaderBase::get_current_snapshot_version(int64_t &snapshot_version)
{
  int ret = OB_SUCCESS;
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  snapshot_version = 0;

  if (OB_UNLIKELY(OB_ISNULL(txs))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans service is null", K(ret));
  } else {
    share::SCN snapshot_scn;
    const int64_t expire_ts = ObTimeUtility::current_time() + SNAPSHOT_WAIT_TIMEOUT_US;
    if (OB_FAIL(txs->get_read_snapshot_version(expire_ts, snapshot_scn))) {
      LOG_WARN("Failed to get read snapshot version", K(ret));
    } else {
      snapshot_version = snapshot_scn.get_val_for_tx();
    }
  }

  return ret;
}

int ObFTDictCacheLoaderBase::build_ranges(const ObFTDictDesc &desc,
                                           ObIFTDictIterator &iter,
                                           ObFTCacheRangeContainer &range_container,
                                           const int64_t snapshot_version,
                                           const ObIArray<ObMissingRangeInfo> *partial_ranges)
{
  int ret = OB_SUCCESS;
  bool build_next_range = true;
  int32_t range_id = (nullptr != partial_ranges) ? partial_ranges->at(0).start_range_id_ : 0;
  const int32_t partial_range_count = (nullptr != partial_ranges) ? partial_ranges->count() : 0;
  int32_t cur_range_idx = 0;
  ObFTDAT *pending_dat_buff = nullptr;
  ObArenaAllocator tmp_alloc(lib::ObMemAttr(tenant_id_, "PendingDatBuff"));

  while (OB_SUCC(ret) && build_next_range) {
    // When current range_id reaches end of a partial segment, jump to next segment start
    if (partial_range_count > 0 && cur_range_idx + 1 < partial_range_count) {
      if (partial_ranges->at(cur_range_idx).start_range_id_ + partial_ranges->at(cur_range_idx).range_count_ == range_id) {
        range_id = partial_ranges->at(cur_range_idx + 1).start_range_id_;
        cur_range_idx++;
      }
    }

    if (OB_FAIL(build_one_range(
            desc, range_id, iter, range_container,
            build_next_range, tmp_alloc, pending_dat_buff))) {
      LOG_WARN("Fail to build range", K(ret), K(range_id), K(desc));
    } else {
      range_id++;
    }
  }

  const int32_t total_range_count = range_container.get_handles().size();
  const int32_t expected_end_id = (partial_range_count > 0)
      ? partial_ranges->at(partial_range_count - 1).start_range_id_
        + static_cast<int32_t>(partial_ranges->at(partial_range_count - 1).range_count_)
      : total_range_count;
  if (OB_FAIL(ret)) {
  } else if (expected_end_id != range_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Range count is not consistent", K(ret), K(partial_range_count),
             K(total_range_count), K(range_id), K(expected_end_id), K(desc));
  } else if (partial_range_count <= 0) {
    if (0 == total_range_count) {
      // empty table, nothing to do
    } else if (OB_ISNULL(pending_dat_buff)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to get first data buffer", K(ret));
    } else if (OB_FAIL(update_first_range_cache(desc, pending_dat_buff, snapshot_version, total_range_count, range_container))) {
      LOG_WARN("Failed to put first range with range_count", K(ret));
    } else {
      LOG_INFO("Dict cache built successfully", K(desc), K(total_range_count));
    }
  }

  tmp_alloc.reset();

  return ret;
}

int ObFTDictCacheLoaderBase::build_one_range(const ObFTDictDesc &desc,
                                              const int32_t range_id,
                                              ObIFTDictIterator &iter,
                                              ObFTCacheRangeContainer &container,
                                              bool &build_next_range,
                                              ObIAllocator &persistent_alloc,
                                              ObFTDAT *&pending_dat_buff)
{
  int ret = OB_SUCCESS;
  build_next_range = true;
  static constexpr int DEFAULT_KEY_PER_RANGE = 50000; // by estimated

  ObArenaAllocator tmp_alloc(lib::ObMemAttr(tenant_id_, "Temp trie"));
  ObIAllocator &alloc = (0 == range_id) ? persistent_alloc : tmp_alloc;
  ObFTDATBuilder<void> builder(alloc);
  storage::ObFTTrie<void> trie(alloc, desc.coll_type_);
  int count = 0;
  bool range_end = false;
  int64_t first_char_len = 0;
  ObFTSingleToken range_end_char;
  ObFTDAT *dat_buff = nullptr;
  bool need_check_first_char = false;

  while (OB_SUCC(ret) && !range_end) {
    ObString key;
    if (OB_FAIL(iter.get_key(key))) {
      LOG_WARN("Failed to get key", K(ret));
    } else if (OB_FALSE_IT(++count)) {
    } else if (DEFAULT_KEY_PER_RANGE == count) {
      if (OB_FAIL(ObCharset::first_valid_char(desc.coll_type_,
                                              key.ptr(),
                                              key.length(),
                                              first_char_len))) {
        LOG_WARN("First char is not valid", K(ret));
      } else if (OB_FAIL(range_end_char.set_token(key.ptr(), first_char_len))) {
        LOG_WARN("Failed to record first char", K(ret));
      } else {
        need_check_first_char = true;
      }
    } else if (need_check_first_char) {
      if (OB_FAIL(ObCharset::first_valid_char(desc.coll_type_,
                                              key.ptr(),
                                              key.length(),
                                              first_char_len))) {
        LOG_WARN("First char is not valid", K(ret));
      } else if (range_end_char.get_token() != ObString(static_cast<int32_t>(first_char_len), key.ptr())) {
        range_end = true;
      }
    }
    if (OB_SUCC(ret) && !range_end) {
      if (OB_FAIL(trie.insert(key, {}))) {
        LOG_WARN("Failed to insert key to trie", K(ret));
      } else if (OB_FAIL(iter.next()) && OB_ITER_END != ret) {
        LOG_WARN("Failed to step to next word entry", K(ret));
      }
    }
  }

  if (OB_ITER_END == ret) {
    build_next_range = false;
    ret = OB_SUCCESS;
  }

  ObFTCacheRangeHandle *info = nullptr;

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(builder.init(trie))) {
    LOG_WARN("Failed to build dat", K(ret));
  } else if (OB_FAIL(builder.build_from_trie(trie))) {
    LOG_WARN("Failed to build datrie", K(ret));
  } else if (FALSE_IT(builder.get_mem_block(dat_buff))) {
  } else if (OB_FAIL(container.fetch_info_for_dict(info))) {
    LOG_WARN("Failed to fetch info for dict", K(ret));
  } else if (0 == range_id) {
    pending_dat_buff = dat_buff;
    info->value_ = nullptr;
  } else if (OB_FAIL(ObFTCacheDict::put_and_fetch_cache_entry(
             desc.table_id_, tenant_id_, range_id,
             dat_buff, 0/*snapshot_version not used*/,
             0/*range_count not used*/, *info))) {
    LOG_WARN("Failed to put dict into kv cache", K(ret));
  }

  if (0 != range_id) {
    alloc.reset();
  }

  return ret;
}

int ObFTDictCacheLoaderBase::collect_missing_ranges(const ObFTSingleToken &start_token,
                                                     const ObFTSingleToken &end_token,
                                                     const int32_t start_range_id,
                                                     const int64_t range_count,
                                                     const int64_t snapshot_version,
                                                     ObIArray<ObMissingRangeInfo> *missing_ranges)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_ISNULL(missing_ranges))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("missing_ranges is null", K(ret));
  } else {
    ObMissingRangeInfo missing_range;
    missing_range.start_token_ = start_token;
    missing_range.end_token_ = end_token;
    missing_range.start_range_id_ = start_range_id;
    missing_range.range_count_ = range_count;
    missing_range.snapshot_version_ = snapshot_version;
    if (OB_FAIL(missing_ranges->push_back(missing_range))) {
      LOG_WARN("Failed to push back missing range", K(ret));
    }
  }

  return ret;
}

int ObFTDictCacheLoaderBase::handle_missing_ranges(const ObFTCacheRangeContainer &range_container,
                                                    const int32_t range_count,
                                                    const int64_t snapshot_version,
                                                    ObIArray<ObMissingRangeInfo> *missing_ranges)
{
  int ret = OB_SUCCESS;

  if (range_count == range_container.get_handles().size()) {
    // no missing ranges, skip it
  } else if (OB_ISNULL(missing_ranges)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("missing_ranges is null", K(ret));
  } else {
    const ObList<ObFTCacheRangeHandle *, ObIAllocator> &handles = range_container.get_handles();
    int32_t prev_range_id = -1;
    const ObFTCacheRangeHandle *prev_handle = nullptr;

    for (ObList<ObFTCacheRangeHandle *, ObIAllocator>::const_iterator handle_it = handles.begin(); OB_SUCC(ret) && handle_it != handles.end(); ++handle_it) {
      const ObFTCacheRangeHandle *curr_handle = *handle_it;
      int32_t curr_range_id = curr_handle->key_.get_range_id();
      // Attention: first existing range id must be 0, so we can skip the first range to collect missing ranges
      if (curr_range_id != prev_range_id + 1) {
        if (OB_FAIL(collect_missing_ranges(prev_handle->value_->get_dat_block()->end_token_,
                                           curr_handle->value_->get_dat_block()->start_token_,
                                           prev_range_id + 1,
                                           curr_range_id - prev_range_id - 1,
                                           snapshot_version,
                                           missing_ranges))) {
          LOG_WARN("Failed to collect missing range", K(ret));
        }
      }

      prev_range_id = curr_range_id;
      prev_handle = curr_handle;
    }

    if (OB_SUCC(ret) && prev_range_id + 1 < range_count && OB_NOT_NULL(prev_handle)) {
      ObFTSingleToken start_token = prev_handle->value_->get_dat_block()->end_token_;
      int32_t missing_start_range_id = prev_range_id + 1;
      int64_t missing_count = range_count - missing_start_range_id;
      if (OB_FAIL(collect_missing_ranges(start_token, ObFTSingleToken(), missing_start_range_id,
                                          missing_count, snapshot_version, missing_ranges))) {
        LOG_WARN("Failed to collect trailing missing range", K(ret));
      }
    }
  }

  return ret;
}

int ObFTDictCacheLoaderDDL::load_cache(const ObFTDictDesc &desc,
                                        ObFTCacheRangeContainer &range_container)
{
  int ret = OB_SUCCESS;
  ObFTDictMgr::DictTableSnapshot row_scn_info;
  ObFTSlotGuard slot_guard(dict_mgr_, desc.table_id_);

  if (OB_FAIL(slot_guard.acquire()) && OB_EAGAIN != ret) {
    LOG_WARN("Failed to try build", K(ret), K(desc));
  } else if (OB_EAGAIN == ret) {
    if (OB_FAIL(slot_guard.wait_build_complete())) {
      LOG_WARN("Failed to wait for build complete", K(ret), K(desc));
    } else if (OB_FAIL(try_load_cache(desc, range_container)) && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("Failed to load cache after wait", K(ret), K(desc.table_id_));
    }
  } else if (OB_FAIL(build_and_fetch_cache(desc, row_scn_info, range_container))) {
    LOG_WARN("Failed to update or build cache", K(ret), K(desc));
  }

  if (OB_FAIL(ret)) {
    range_container.reset();
  }

  return ret;
}

int ObFTDictCacheLoaderDDL::build_and_fetch_cache(const ObFTDictDesc &desc,
                                                   ObFTDictMgr::DictTableSnapshot &row_scn_info,
                                                   ObFTCacheRangeContainer &range_container)
{
  return refresh_cache_common(desc, row_scn_info, range_container);
}

int ObFTDictCacheLoaderExec::load_cache(const ObFTDictDesc &desc,
                                        ObFTCacheRangeContainer &range_container)
{
  int ret = OB_SUCCESS;
  ObArray<ObMissingRangeInfo> missing_ranges;
  ObFTSlotGuard slot_guard(dict_mgr_, desc.table_id_);

  if (OB_SUCC(try_load_cache(desc, range_container, &missing_ranges))) {
    dict_mgr_->push_access_row_scn_cache_task(desc.table_id_);
    // get complete cache, use it directly
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("Failed to load cache", K(ret), K(desc));
  } else if (OB_FAIL(slot_guard.acquire()) && OB_EAGAIN != ret) {
    LOG_WARN("Failed to try build", K(ret), K(desc));
  } else if (OB_EAGAIN == ret) {
    if (OB_FAIL(slot_guard.wait_build_complete())) {
      LOG_WARN("Failed to wait for build complete", K(ret), K(desc));
    } else if (OB_FAIL(try_load_cache(desc, range_container)) && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("Failed to load cache after wait", K(ret), K(desc.table_id_));
    }
  } else {
    bool is_need_reload_cache = false;
    bool is_need_full_build = true;
    if (!missing_ranges.empty()) {
      if (OB_FAIL(check_snapshot_version(desc, missing_ranges.at(0).snapshot_version_, is_need_reload_cache))) {
        LOG_WARN("Failed to check snapshot version", K(ret), K(desc));
      } else if (is_need_reload_cache) {
        // partial build may cover newer data, just reload cache
      } else if (OB_FAIL(do_partial_build(desc, range_container, missing_ranges, is_need_full_build))) {
        LOG_WARN("Failed to build partial ranges", K(ret), K(desc));
      }
    }
    if (OB_SUCC(ret) && is_need_full_build && OB_FAIL(do_full_build(desc, range_container))) {
      LOG_WARN("Failed to build full ranges", K(ret), K(desc));
    }
  }

  if (OB_FAIL(ret)) {
    range_container.reset();
  }

  return ret;
}

int ObFTDictCacheLoaderExec::check_snapshot_version(const ObFTDictDesc &desc,
                                                     const int64_t snapshot_version,
                                                     bool &is_need_reload_cache)
{
  int ret = OB_SUCCESS;
  is_need_reload_cache = false;
  ObFTDictMgr::DictTableSnapshot cached_info;

  if (snapshot_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid snapshot version", K(ret));
  } else if (OB_FAIL(dict_mgr_->get_table_snapshot(desc.table_id_, cached_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("Failed to get table row scn info from cache", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (cached_info.snapshot_version_ > snapshot_version) {
    is_need_reload_cache = true;
  }

  return ret;
}

int ObFTDictCacheLoaderExec::do_partial_build(const ObFTDictDesc &desc,
                                               ObFTCacheRangeContainer &range_container,
                                               ObIArray<ObMissingRangeInfo> &missing_ranges,
                                               bool &is_need_full_build)
{
  int ret = OB_SUCCESS;
  is_need_full_build = false;
  int64_t row_scn = 0;
  int64_t row_count = 0;
  int64_t snapshot_version = 0;

  if (missing_ranges.empty()) {
    // skip
  } else if (FALSE_IT(snapshot_version = missing_ranges.at(0).snapshot_version_)) {
  } else if (OB_FAIL(build_ranges_from_table(desc, snapshot_version, range_container, &missing_ranges))) {
    if (OB_SNAPSHOT_DISCARDED != ret) {
      LOG_WARN("Failed to build partial ranges", K(ret), K(desc));
    } else {
      is_need_full_build = true;
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(get_table_row_scn_and_count(desc, snapshot_version, row_scn, row_count))) {
    if (OB_SNAPSHOT_DISCARDED != ret) {
      LOG_WARN("Failed to get current row_scn and row_count at snapshot", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    ObFTDictMgr::DictTableSnapshot scn_info;
    scn_info.row_scn_ = row_scn;
    scn_info.row_count_ = row_count;
    scn_info.snapshot_version_ = snapshot_version;
    if (OB_FAIL(dict_mgr_->update_table_snapshot(desc.table_id_, scn_info))) {
      LOG_WARN("Failed to update table row scn info", K(ret), K(desc.table_id_));
    }
  }

  return ret;
}

int ObFTDictCacheLoaderRefresh::load_cache(const ObFTDictDesc &desc,
                                            ObFTCacheRangeContainer &range_container)
{
  int ret = OB_SUCCESS;
  UNUSED(range_container);
  bool is_valid = false;
  ObFTDictMgr::DictTableSnapshot row_scn_info;
  ObFTSlotGuard slot_guard(dict_mgr_, desc.table_id_);

  if (OB_FAIL(slot_guard.acquire()) && OB_EAGAIN != ret) {
    LOG_WARN("Failed to try build", K(ret), K(desc));
  } else if (OB_EAGAIN == ret) {
    if (OB_FAIL(slot_guard.wait_build_complete())) {
      LOG_WARN("Failed to wait for build complete", K(ret), K(desc));
    } else if (OB_FAIL(check_cache_valid(desc, is_valid))) {
      LOG_WARN("Failed to load cache after wait", K(ret), K(desc.table_id_));
    } else if (!is_valid) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("Cache is not valid", K(ret), K(desc.table_id_));
    }
  } else if (OB_FAIL(build_and_refresh_cache(desc, row_scn_info))) {
    LOG_WARN("Failed to refresh cache", K(ret), K(desc));
  }

  return ret;
}

int ObFTDictCacheLoaderRefresh::load_cache(const ObFTDictDesc &desc)
{
  ObArenaAllocator tmp_alloc(lib::ObMemAttr(tenant_id_, "FTRangeRefresh"));
  ObFTCacheRangeContainer dummy_container(tmp_alloc);
  return load_cache(desc, dummy_container);
}

int ObFTDictCacheLoaderRefresh::check_cache_valid(const ObFTDictDesc &desc,
                                                   bool &is_valid,
                                                   ObFTCacheRangeContainer *range_container)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObFTCacheRangeHandle first_info;
  ObFTCacheRangeHandle *first_info_ptr = nullptr;
  ObDictCacheKey first_key(desc.table_id_, tenant_id_, 0);

  if (OB_INVALID_ID == desc.table_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid table_id in desc", K(ret), K(desc));
  } else if (OB_ISNULL(range_container)) {
    first_info_ptr = &first_info;
  } else if (OB_FAIL(range_container->fetch_info_for_dict(first_info_ptr))) {
    LOG_WARN("Failed to fetch info for dict.", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDictCache::get_instance().get_dict(first_key, first_info_ptr->value_, first_info_ptr->handle_))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("Failed to get first dict from kvcache", K(ret), K(first_key));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_UNLIKELY(OB_ISNULL(first_info_ptr->value_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("value is null", K(ret));
  } else {
    int32_t range_count = first_info_ptr->value_->get_range_count();
    // Check all ranges exist
    for (int32_t i = 1; OB_SUCC(ret) && i < range_count; ++i) {
      ObDictCacheKey key(desc.table_id_, tenant_id_, i);
      ObFTCacheRangeHandle tmp;
      if (OB_FAIL(ObDictCache::get_instance().get_dict(key, tmp.value_, tmp.handle_))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("Failed to get dict from kvcache", K(ret), K(key));
        }
      }
    }

    if (OB_SUCC(ret)) {
      is_valid = true;
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }

  if (OB_FAIL(ret) || !is_valid) {
    if (OB_NOT_NULL(range_container)) {
      range_container->reset();
    }
  }

  return ret;
}

int ObFTDictCacheLoaderRefresh::build_and_refresh_cache(const ObFTDictDesc &desc,
                                                        ObFTDictMgr::DictTableSnapshot &row_scn_info)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(lib::ObMemAttr(tenant_id_, "FTRangeRefresh"));
  ObFTCacheRangeContainer range_container(allocator);
  if (OB_FAIL(refresh_cache_common(desc, row_scn_info, range_container))) {
    LOG_WARN("Failed to refresh cache", K(ret), K(desc));
  }
  range_container.reset();
  return ret;
}

int ObFTDictCacheLoaderRefresh::validate_cache(const ObFTDictDesc &desc,
                                               ObFTCacheRangeContainer &range_container,
                                               bool &is_valid)
{
  return check_cache_valid(desc, is_valid, &range_container);
}

} // namespace storage
} // namespace oceanbase
