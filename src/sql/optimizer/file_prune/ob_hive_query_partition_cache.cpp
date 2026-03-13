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
#define USING_LOG_PREFIX SQL_OPT
#include "lib/oblog/ob_log_module.h"
#include "lib/container/ob_iarray.h"
#include "sql/optimizer/file_prune/ob_hive_query_partition_cache.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

void HiveTableFileCache::reset()
{
  partition_infos_.reset();
  if (cached_files_.created()) {
    cached_files_.destroy();
  }
  if (partition_map_.created()) {
    partition_map_.destroy();
  }
}

int HiveTableFileCache::init_partition_info(ObIArray<HivePartitionInfo*> &partition_infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!partition_infos_.empty())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("hive table file cache init twice");
  } else if (OB_FAIL(partition_infos_.assign(partition_infos))) {
    LOG_WARN("failed to assign partition infos");
  } else if (!partition_map_.created() &&
             OB_FAIL(partition_map_.create(partition_infos.count(), "HivePartMap"))) {
    LOG_WARN("failed to create hive part map");
  }
  return ret;
}

int HiveTableFileCache::get_cached_files(const ObIArray<int64_t> &part_ids,
                                         const ObIArray<ObString> &part_paths,
                                         ObIArray<ObHiveFileDesc> &part_files,
                                         ObIArray<int64_t> &unhit_part_id,
                                         ObIArray<ObString> &unhit_part_path)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cached_files_.empty())) {
    if (OB_FAIL(unhit_part_id.assign(part_ids))) {
      LOG_WARN("failed to assign part ids");
    } else if (OB_FAIL(unhit_part_path.assign(part_paths))) {
      LOG_WARN("failed to assign part paths");
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_ids.count(); ++i) {
      ObArray<ObHiveFileDesc> *files = nullptr;
      if (OB_FAIL(cached_files_.get_refactored(part_ids.at(i), files))) {
        if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
          if (OB_FAIL(unhit_part_id.push_back(part_ids.at(i)))) {
            LOG_WARN("failed to push back unhit_part_id");
          } else if (OB_FAIL(unhit_part_path.push_back(part_paths.at(i)))) {
            LOG_WARN("failed to push back unhit_part_path");
          }
        } else {
          LOG_WARN("failed to get from hashmap");
        }
      } else if (OB_ISNULL(files)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("files is null");
      } else if (OB_FAIL(append(part_files, *files))) {
        LOG_WARN("failed to append files");
      }
    }
  }
  return ret;
}

int HiveTableFileCache::add_cached_file(const int64_t part_id,
                                        ObIArrayWrap<ObHiveFileDesc> &part_files)
{
  int ret = OB_SUCCESS;
  void *buf = allocator_.alloc(sizeof(ObArray<ObHiveFileDesc>));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate ObArray");
  } else {
    ObArray<ObHiveFileDesc> *files = new (buf) ObArray<ObHiveFileDesc>();
    files->set_block_allocator(ModulePageAllocator(allocator_));
    if (OB_FAIL(append(*files, part_files))) {
      LOG_WARN("failed to append part files");
    } else if (OB_FAIL(cached_files_.set_refactored(part_id, files))) {
      LOG_WARN("failed to set hashmap");
    }
  }
  return ret;
}

void ObHiveQueryPartitionCache::reset()
{
  if (cache_map_.created()) {
    for (HiveTableFileCacheMap::const_iterator iter = cache_map_.begin(); iter != cache_map_.end(); ++iter) {
      if (iter->second != nullptr) {
        iter->second->reset();
      }
    }
    cache_map_.destroy();
  }
}
int ObHiveQueryPartitionCache::get_cache_info(uint64_t table_id,
                                              common::ObIAllocator &allocator,
                                              HiveTableFileCache *&cache_info)
{
  int ret = OB_SUCCESS;
  cache_info = nullptr;
  if (!cache_map_.created() && OB_FAIL(cache_map_.create(16, "HiveTableCache"))) {
    LOG_WARN("failed to create hive table file cache");
  } else if (OB_FAIL(cache_map_.get_refactored(table_id, cache_info))) {
    if (OB_LIKELY(ret == OB_HASH_NOT_EXIST)) {
      ret = OB_SUCCESS;
      void *buf = allocator.alloc(sizeof(HiveTableFileCache));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for hive table file cache");
      } else {
        cache_info = new (buf) HiveTableFileCache(allocator);
        if (OB_FAIL(cache_info->cached_files_.create(16, "HiveTableCache"))) {
          LOG_WARN("failed to create cached files hash map");
        } else if (OB_FAIL(cache_map_.set_refactored(table_id, cache_info))) {
          LOG_WARN("failed to set hive table file cache");
        }
      }
    } else {
      LOG_WARN("failed to get hive table file cache", K(table_id));
    }
  }
  if (OB_FAIL(ret) && cache_info != nullptr) {
    if (cache_info->cached_files_.created()) {
      cache_info->cached_files_.destroy();
    }
    cache_info->~HiveTableFileCache();
    allocator.free(cache_info);
    cache_info = nullptr;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
