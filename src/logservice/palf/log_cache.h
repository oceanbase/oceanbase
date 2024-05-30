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

#ifndef OCEANBASE_PALF_LOG_CACHE_
#define OCEANBASE_PALF_LOG_CACHE_

#include <cstdint>                                       // int64_t
#include "share/cache/ob_kv_storecache.h"                // ObIKVCache
#include "log_reader_utils.h"                            // ReadBuf
#include "lsn.h"
#include "log_storage.h"
#include "log_storage_interface.h"                       // LogIteratorInfo

#define OB_LOG_KV_CACHE oceanbase::palf::LogKVCache::get_instance()
namespace oceanbase
{
namespace palf
{
class LSN;
class IPalfHandleImpl;
class IPalfEnvImpl;
class LogStorage;
class LogEngine;
class LogStateMgr;

static const int64_t CACHE_LINE_SIZE = LOG_CACHE_ALIGN_SIZE;  // 64KB
static const int64_t LAST_CACHE_LINE_SIZE  = CACHE_LINE_SIZE - MAX_INFO_BLOCK_SIZE; // 60KB
static const char *const OB_LOG_KV_CACHE_NAME = "log_kv_cache";

class LogHotCache
{
public:
  LogHotCache();
  ~LogHotCache();
  void destroy();
  void reset();
  int init(const int64_t palf_id, IPalfHandleImpl *palf_handle_impl);
  int read(const LSN &read_begin_lsn,
           const int64_t in_read_size,
           char *buf,
           int64_t &out_read_size) const;
private:
  int64_t palf_id_;
  IPalfHandleImpl *palf_handle_impl_;
  mutable int64_t read_size_;
  mutable int64_t hit_count_;
  mutable int64_t read_count_;
  mutable int64_t last_print_time_;
  bool is_inited_;
};

class FillCacheFsCb : public PalfFSCb
{
public:
  FillCacheFsCb();
  ~FillCacheFsCb();
  int init(IPalfEnvImpl *palf_env_impl, LogStateMgr *state_mgr, LogEngine *log_engine);
  void destroy();
  int update_end_lsn(int64_t id,
                     const palf::LSN &end_lsn,
                     const share::SCN &end_scn,
                     const int64_t proposal_id) override final;
private:
  IPalfEnvImpl *palf_env_impl_;
  LogStateMgr *state_mgr_;
  LogEngine *log_engine_;
  bool is_inited_;
};

class LogCacheUtils
{
public:
  LogCacheUtils() {};
  ~LogCacheUtils() {};
public:
  static LSN lower_align_with_start(const LSN &input, const int64_t align);
  // @brief: return the next aligned lsn in the same block.
  // @param[in] const LSN &input: input should be in the range [block_start_lsn, next_block_start_lsn - CACHE_LINE_SIZE + MAX_INFO_BLOCK_SIZE)
  // @param[in] const int64_t align: CACHE_LINE_SIZE
  static LSN upper_align_with_start(const LSN &input, const int64_t align);
  static bool is_lower_align_with_start(const LSN &input, const int64_t align);
  static bool up_to_next_block(const LSN &lsn, const int64_t remained_size);
  static LSN next_block_start_lsn(const LSN &input);
  static bool is_in_last_cache_line(const LSN &input);
};

class LogKVCacheKey : public common::ObIKVCacheKey
{
public:
  LogKVCacheKey();
  LogKVCacheKey(const uint64_t tenant_id, const int64_t palf_id, const LSN aligned_lsn, const int64_t flashback_version);
  ~LogKVCacheKey();
  bool is_valid() const;
  void reset();
public: // derived from ObIKVCacheKey
  bool operator ==(const ObIKVCacheKey &other) const override;
  uint64_t hash() const override;
  uint64_t get_tenant_id() const override;
  int64_t size() const override;
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;
  TO_STRING_KV(K_(tenant_id), K_(palf_id), K_(aligned_lsn), K_(flashback_version));

private:
  uint64_t tenant_id_;
  int64_t palf_id_;
  LSN aligned_lsn_;
  int64_t flashback_version_;
};

class LogKVCacheValue : public common::ObIKVCacheValue
{
public:
  LogKVCacheValue();
  LogKVCacheValue(char *buf, const int64_t buf_size);
  ~LogKVCacheValue();
  int init(char *buf, const int64_t buf_size);
  void reset();
  bool is_valid() const;
  char *get_buf() const;
  int64_t get_buf_size() const;
public: // derived from ObIKVCacheValue
  int64_t size() const override;
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;
  TO_STRING_KV(KP_(buf), K_(buf), K_(buf_size));
private:
  char *buf_;
  int64_t buf_size_;
};

struct LogKVCacheValueHandle
{
public:
  LogKVCacheValueHandle() : value_(NULL), handle_() {}
  ~LogKVCacheValueHandle() {}
  void reset() {
    handle_.reset();
    value_ = NULL;
  }
  TO_STRING_KV(K_(value), K_(handle));
  const LogKVCacheValue *value_;
  common::ObKVCacheHandle handle_;
};

class LogKVCache : public common::ObKVCache<LogKVCacheKey, LogKVCacheValue>
{
public:
  static LogKVCache &get_instance();
  LogKVCache();
  ~LogKVCache();
  int get_log(const LogKVCacheKey &key, LogKVCacheValueHandle &val_handle);
  int put_log(const LogKVCacheKey &key, const LogKVCacheValue &value);
};

struct FillBuf
{
public:
  FillBuf();
  ~FillBuf();
  void reset();
  bool is_valid();
  char *get_writable_buf();
  bool is_full();
  void update_state(const LSN &aligned_lsn, const int64_t flashback_version);
  TO_STRING_KV(K(aligned_lsn_), K(fill_pos_), K(buf_len_));
  LSN aligned_lsn_;
  int64_t fill_pos_;
  int64_t buf_len_;
  int64_t flashback_version_;
  char *buf_;
  common::ObKVCachePair *kvpair_;
  common::ObKVCacheHandle handle_;
  common::ObKVCacheInstHandle inst_handle_;
};

class LogColdCache
{
public:
  LogColdCache();
  ~LogColdCache();
  int init(int64_t palf_id,
           IPalfEnvImpl *palf_env_impl,
           LogStorage *log_storage);
  void destroy();
  // @brief: read logs from cold cache. If miss cold cache, read logs from the disk and fill cold cache
  // @param[in] const int64_t flashback_version: flashback_version from LogStorage
  // @param[in] const LSN &lsn: start lsn for read
  // @param[in] const int64_t in_read_size: needed read size
  // @param[out] ReadBuf &read_buf: buf for read logs
  // @param[out] int64_t &out_read_size: actual read size
  // @param[out] LogIteratorInfo *iterator_info: iterator info
  // @return
  // - OB_SUCCESS: read logs successfully
  // - OB_INVALID_ARGUEMENTS: invalid arguments
  // - OB_IO_ERROR: error when reading disk
  // - other: bug
  int read(const int64_t flashback_version,
           const LSN &lsn,
           const int64_t in_read_size,
           ReadBuf &read_buf,
           int64_t &out_read_size,
           LogIteratorInfo *iterator_info);
  int fill_cache_line(FillBuf &fill_buf);
  int alloc_kv_pair(const int64_t flashback_version, const LSN &aligned_lsn, FillBuf &fill_buf);
  TO_STRING_KV(K(is_inited_), K(palf_id_), K(log_cache_stat_));
private:
  int allow_filling_cache_(LogIteratorInfo *iterator_info, bool &enable_fill_cache);
  /*
  this func is used to adujst read position(lsn) and read size(in_read_size) before reading from the disk:
  1. if read from cache successfully, lsn must be aligned to CACHE_LINE_SIZE in the block.
     lsn and in_read_size should be adjusted to statisfy the requirements of DIO.
  2. if cache miss, lsn and in_read_size should be adjusted to cover the first missing cache line.
     When it is aligned to the cache line, it must also be aligned to 4K for DIO.
  */
  int deal_with_miss_(const bool enable_fill_cache,
                      const int64_t has_read_size,
                      const int64_t buf_len,
                      LSN &lsn,
                      int64_t &in_read_size,
                      int64_t &out_read_size,
                      LogIteratorInfo *iterator_info);
  int get_cache_lines_(const LSN &lsn,
                       const int64_t flashback_version,
                       const int64_t in_read_size,
                       char *buf,
                       int64_t &out_read_size,
                       LogIteratorInfo *iterator_info);
  int get_cache_line_(const LSN &cache_read_lsn,
                      const int64_t flashback_version,
                      const int64_t in_read_size,
                      const int64_t read_pos,
                      char *buf,
                      int64_t &out_read_size);
  int fill_cache_lines_(const int64_t flashback_version,
                        const LSN &lsn,
                        const int64_t fill_size,
                        char *buf);
  int fill_cache_line_(const int64_t flashback_version,
                       const LSN &fill_lsn,
                       const int64_t fill_size,
                       const int64_t fill_pos,
                       char *buf);
  int read_from_disk_(const LSN &read_lsn,
                      const int64_t in_read_size,
                      ReadBuf &read_buf,
                      int64_t &out_read_size,
                      LogIteratorInfo *iterator_info);
  offset_t get_phy_offset_(const LSN &lsn) const;
private:
  class LogCacheStat
  {
  public:
    LogCacheStat();
    ~LogCacheStat();
    void reset();
    void inc_hit_cnt();
    void inc_miss_cnt();
    void inc_cache_read_size(int64_t cache_read_size);
    void inc_cache_fill_amplification(int64_t cache_fill_amplification_);
    void print_stat_info(int64_t cache_store_size, int64_t palf_id);
    TO_STRING_KV(K(hit_cnt_), K(miss_cnt_), K(cache_read_size_), K(cache_fill_amplification_));
  private:
    // cache stat for accumulate
    int64_t hit_cnt_;
    int64_t miss_cnt_;
    int64_t cache_read_size_;
    int64_t cache_fill_amplification_;
    // cache stat for real time
    int64_t last_print_time_;
    int64_t last_record_hit_cnt_;
    int64_t last_record_miss_cnt_;
    int64_t last_record_cache_read_size_;
  };
private:
  int64_t palf_id_;
  IPalfEnvImpl *palf_env_impl_;
  LogReader *log_reader_;
  LogKVCache *kv_cache_;
  int64_t logical_block_size_;
  LogCacheStat log_cache_stat_;
  bool is_inited_;
};

class LogCache
{
public:
  LogCache();
  ~LogCache();
  void destroy();
  int init(const int64_t palf_id,
           IPalfHandleImpl *palf_handle_impl,
           IPalfEnvImpl *palf_env_impl,
           LogStorage *log_storage);
  bool is_inited();
  int read(const int64_t flashback_version,
           const LSN &lsn,
           const int64_t in_read_size,
           ReadBuf &read_buf,
           int64_t &out_read_size,
           LogIteratorInfo *iterator_info);
  int fill_cache_when_slide(const LSN &lsn,
                            const int64_t size,
                            const int64_t flashback_version);
  TO_STRING_KV(K(is_inited_), K(palf_id_), K(cold_cache_));
private:
  int read_hot_cache_(const LSN &read_begin_lsn,
                      const int64_t in_read_size,
                      char *buf,
                      int64_t &out_read_size);
  int read_cold_cache_(const int64_t flashback_version,
                       const LSN &lsn,
                       const int64_t in_read_size,
                       ReadBuf &read_buf,
                       int64_t &out_read_size,
                       LogIteratorInfo *iterator_info);
  int try_update_fill_buf_(const int64_t flashback_version,
                           LSN &fill_lsn,
                           int64_t &fill_size);
  int update_fill_buf_(const int64_t flashback_version,
                       LSN &fill_lsn);
  int64_t cal_in_read_size_(const int64_t fill_size);
private:
  int64_t palf_id_;
  LogHotCache hot_cache_;
  LogColdCache cold_cache_;
  // used for fill cache actively
  FillBuf fill_buf_;
  bool is_inited_;
};

} // end namespace palf
} // end namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_CACHE_
