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

#define USING_LOG_PREFIX PALF
#include "lib/stat/ob_session_stat.h"
#include "log_cache.h"
#include "palf_handle_impl.h"
#include "palf_handle_impl_guard.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace palf
{
LogHotCache::LogHotCache()
  : palf_id_(INVALID_PALF_ID),
    palf_handle_impl_(NULL),
    read_size_(0),
    hit_count_(0),
    read_count_(0),
    last_print_time_(0),
    is_inited_(false)
{}

LogHotCache::~LogHotCache()
{
  destroy();
}

void LogHotCache::destroy()
{
  reset();
}

void LogHotCache::reset()
{
  is_inited_ = false;
  palf_handle_impl_ = NULL;
  palf_id_ = INVALID_PALF_ID;
}

int LogHotCache::init(const int64_t palf_id, IPalfHandleImpl *palf_handle_impl)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (false == is_valid_palf_id(palf_id) || OB_ISNULL(palf_handle_impl)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    palf_id_ = palf_id;
    palf_handle_impl_ = palf_handle_impl;
    is_inited_ = true;
    PALF_LOG(TRACE, "init hot cache successfully", K(palf_id_));
  }
  return ret;
}

int LogHotCache::read(const LSN &read_begin_lsn,
                      const int64_t in_read_size,
                      char *buf,
                      int64_t &out_read_size) const
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0, hit_cnt = 0, read_cnt = 0;
  out_read_size = 0;
  int64_t start_ts = ObTimeUtility::fast_current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "hot cache is not inited", K(ret), K(palf_id_));
  } else if (!read_begin_lsn.is_valid() || in_read_size <= 0 || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguments", K(ret), K_(palf_id), K(read_begin_lsn), K(in_read_size),
        KP(buf));
  } else if (OB_FAIL(palf_handle_impl_->read_data_from_buffer(read_begin_lsn, in_read_size, \
          buf, out_read_size))) {
    if (OB_ERR_OUT_OF_LOWER_BOUND != ret) {
      PALF_LOG(WARN, "read_data_from_buffer failed", K(ret), K_(palf_id), K(read_begin_lsn),
          K(in_read_size));
    }
  } else {
    int64_t cost_ts = ObTimeUtility::fast_current_time() - start_ts;
    hit_cnt = ATOMIC_AAF(&hit_count_, 1);
    read_size = ATOMIC_AAF(&read_size_, out_read_size);
    EVENT_TENANT_INC(ObStatEventIds::PALF_READ_COUNT_FROM_HOT_CACHE, MTL_ID());
    EVENT_ADD(ObStatEventIds::PALF_READ_SIZE_FROM_HOT_CACHE, out_read_size);
    EVENT_ADD(ObStatEventIds::PALF_READ_TIME_FROM_HOT_CACHE, cost_ts);
    PALF_LOG(TRACE, "read_data_from_buffer success", K(ret), K_(palf_id), K(read_begin_lsn),
        K(in_read_size), K(out_read_size));
  }
  read_cnt = ATOMIC_AAF(&read_count_, 1);
  if (palf_reach_time_interval(PALF_STAT_PRINT_INTERVAL_US, last_print_time_)) {
    read_cnt = read_cnt == 0 ? 1 : read_cnt;
    PALF_LOG(INFO, "[PALF STAT HOT CACHE HIT RATE]", K_(palf_id), K(read_size), K(hit_cnt), K(read_cnt), "hit rate", hit_cnt * 1.0 / read_cnt);
    hit_count_ = 0;
    read_size_ = 0;
    read_count_ = 0;
  }
  return ret;
}

// ==================== FillCacheFsCb =========================
FillCacheFsCb::FillCacheFsCb() : palf_env_impl_(NULL), state_mgr_(NULL), log_engine_(NULL), is_inited_(false)
{}

FillCacheFsCb::~FillCacheFsCb()
{
  destroy();
}

int FillCacheFsCb::init(IPalfEnvImpl *palf_env_impl, LogStateMgr *state_mgr, LogEngine *log_engine)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(WARN, "FillCacheFsCb has been inited!", K(ret));
  } else if (OB_ISNULL(palf_env_impl) || OB_ISNULL(log_engine)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument!");
  } else {
    palf_env_impl_ = palf_env_impl;
    state_mgr_ = state_mgr;
    log_engine_ = log_engine;
    is_inited_ = true;
  }
  return ret;
}

void FillCacheFsCb::destroy()
{
  palf_env_impl_ = NULL;
  state_mgr_ = NULL;
  log_engine_ = NULL;
  is_inited_ = false;
}

int FillCacheFsCb::update_end_lsn(int64_t id,
                                  const palf::LSN &end_lsn,
                                  const share::SCN &end_scn,
                                  const int64_t proposal_id)
{
  int ret = OB_SUCCESS;
  UNUSED(id);
  UNUSED(end_scn);
  UNUSED(proposal_id);

  PalfOptions options;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "FillCacheFsCb is not inited", K(ret));
  } else if (!state_mgr_->is_leader_active()) {
    // don't submit fill cache task when it isn't a leader
  } else if (OB_FAIL(palf_env_impl_->get_options(options))) {
    PALF_LOG(WARN, "get options failed", K(ret));
  } else if (!options.enable_log_cache_) {
    // don't submit fill cache task when it isn't allowed
  } else {
    // it will be enable in 4.4
    //LSN begin_lsn = end_lsn - log_size;
    //log_engine_->submit_fill_cache_task(begin_lsn, log_size);
  }
  return ret;
}

//============================================= LogCacheUtils ==========================
LSN LogCacheUtils::lower_align_with_start(const LSN &input, int64_t align)
{
  offset_t start = lsn_2_block(input, PALF_BLOCK_SIZE) * PALF_BLOCK_SIZE;
  offset_t ret = start + lower_align(input.val_ - start, align);
  LSN aligned_lsn(ret);
  return aligned_lsn;
}

LSN LogCacheUtils::upper_align_with_start(const LSN &input, int64_t align)
{
  OB_ASSERT(!is_in_last_cache_line(input));
  return lower_align_with_start(input, align) + align;
}

bool LogCacheUtils::is_lower_align_with_start(const LSN &input, int64_t align)
{
  return input == lower_align_with_start(input, align);
}

bool LogCacheUtils::up_to_next_block(const LSN &input, const int64_t remained_size)
{
  return input + remained_size == next_block_start_lsn(input);
}

LSN LogCacheUtils::next_block_start_lsn(const LSN &input)
{
  int64_t curr_block_id = lsn_2_block(input, PALF_BLOCK_SIZE);
  return LSN((curr_block_id + 1) * PALF_BLOCK_SIZE);
}

bool LogCacheUtils::is_in_last_cache_line(const LSN &input)
{
  return LogCacheUtils::next_block_start_lsn(input) - input <= LAST_CACHE_LINE_SIZE;
}

//============================================= LogKVCacheKey ==========================
LogKVCacheKey::LogKVCacheKey()
    : tenant_id_(OB_INVALID_TENANT_ID), palf_id_(INVALID_PALF_ID), aligned_lsn_(LOG_INVALID_LSN_VAL),
      flashback_version_(OB_INVALID_TIMESTAMP) {}

LogKVCacheKey::LogKVCacheKey(const uint64_t tenant_id,
                             const int64_t palf_id,
                             const LSN aligned_lsn,
                             const int64_t flashback_version)
    : tenant_id_(tenant_id), palf_id_(palf_id), aligned_lsn_(aligned_lsn),
      flashback_version_(flashback_version) {}

LogKVCacheKey::~LogKVCacheKey()
{
  reset();
}

bool LogKVCacheKey::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && aligned_lsn_.is_valid() &&
         is_valid_palf_id(palf_id_) && is_valid_flashback_version(flashback_version_);
}

void LogKVCacheKey::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  palf_id_ = INVALID_PALF_ID;
  aligned_lsn_.reset();
  flashback_version_ = OB_INVALID_TIMESTAMP;
}

bool LogKVCacheKey::operator ==(const ObIKVCacheKey &other) const
{
  const LogKVCacheKey &other_key = reinterpret_cast<const LogKVCacheKey &>(other);
  return tenant_id_ == other_key.tenant_id_ && palf_id_ == other_key.palf_id_ &&
         aligned_lsn_ == other_key.aligned_lsn_ &&
         flashback_version_ == other_key.flashback_version_;
}

uint64_t LogKVCacheKey::hash() const
{
  uint64_t hash_code = 0;
  hash_code = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_code);
  hash_code = murmurhash(&palf_id_, sizeof(palf_id_), hash_code);
  hash_code = murmurhash(&aligned_lsn_, sizeof(aligned_lsn_), hash_code);
  hash_code = murmurhash(&flashback_version_, sizeof(flashback_version_), hash_code);
  return hash_code;
}

uint64_t LogKVCacheKey::get_tenant_id() const
{
  return tenant_id_;
}

int64_t LogKVCacheKey::size() const
{
  return sizeof(*this);
}

int LogKVCacheKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(buf_len), K(size()));
  } else {
    LogKVCacheKey *new_key = new (buf) LogKVCacheKey(tenant_id_, palf_id_, aligned_lsn_, flashback_version_);
    if (OB_ISNULL(new_key)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "new_key ptr is null", K(ret), K(*this));
    } else {
      key = new_key;
    }
  }

  return ret;
}

//============================================= LogKVCacheValue ==========================
LogKVCacheValue::LogKVCacheValue() : buf_(NULL), buf_size_(0) {}
LogKVCacheValue::LogKVCacheValue(char *buf, const int64_t buf_size) : buf_(buf), buf_size_(buf_size) {}
LogKVCacheValue::~LogKVCacheValue()
{
  reset();
}

void LogKVCacheValue::reset()
{
  if (OB_NOT_NULL(buf_)) {
     mtl_free(buf_);
   }
   buf_ = NULL;
   buf_size_ = 0;
}

int LogKVCacheValue::init(char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_ = reinterpret_cast<char *>(mtl_malloc(size, "LOG_KV_CACHE")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "allocate memory for log cold cache failed", K(ret), K(size));
  } else {
    MEMCPY(buf_, buf, size);
    buf_size_ = size;
  }
  return ret;
}

bool LogKVCacheValue::is_valid() const
{
  return buf_size_ != 0;
}

char *LogKVCacheValue::get_buf() const
{
  return buf_;
}

int64_t LogKVCacheValue::get_buf_size() const
{
  return buf_size_;
}

int64_t LogKVCacheValue::size() const
{
  return is_valid() ? (sizeof(*this) + buf_size_) : 0;
}

int LogKVCacheValue::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(buf_len), K(size()));
  } else {
    LogKVCacheValue *new_value = new (buf) LogKVCacheValue();
    if (OB_ISNULL(new_value)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "new_value is null", K(ret), K(*this));
    } else {
      new_value->buf_ = buf + sizeof(*this);
      new_value->buf_size_ = buf_size_;
      MEMCPY(new_value->buf_, buf_, buf_size_);
      value = new_value;
    }
  }

  return ret;
}

//============================================= LogKVCache ==========================
LogKVCache &LogKVCache::get_instance()
{
  static LogKVCache kv_cache;
  return kv_cache;
}

LogKVCache::LogKVCache()
{}

LogKVCache::~LogKVCache()
{}

int LogKVCache::get_log(const LogKVCacheKey &key, LogKVCacheValueHandle &val_handle)
{
  int ret = OB_SUCCESS;
  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(key));
  } else if (OB_FAIL(get(key, val_handle.value_, val_handle.handle_))) {
    CLOG_LOG(WARN, "get from cache failed", K(ret), K(key));
  } else if (OB_ISNULL(val_handle.value_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "get a null value from kv cache", K(ret), K(key));
  }

  return ret;
}

int LogKVCache::put_log(const LogKVCacheKey &key, const LogKVCacheValue &value)
{
  int ret = OB_SUCCESS;
  if (!key.is_valid() || !value.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(key), K(value));
  } else if (OB_FAIL(put(key, value, false /* overwrite */))) {
    CLOG_LOG(WARN, "put log failed", K(ret), K(key), K(value));
  }
  return ret;
}

//============================================= FillBuf ==========================
FillBuf::FillBuf()
    : aligned_lsn_(LOG_INVALID_LSN_VAL), fill_pos_(0),
      buf_len_(0), flashback_version_(OB_INVALID_TIMESTAMP),
      buf_(NULL), kvpair_(NULL), handle_(), inst_handle_() {}

FillBuf::~FillBuf()
{
  reset();
}

void FillBuf::reset()
{
  aligned_lsn_.reset();
  fill_pos_ = 0;
  buf_len_ = 0;
  flashback_version_ = OB_INVALID_TIMESTAMP;
  buf_ = NULL;
  kvpair_ = NULL;
  handle_.reset();
  inst_handle_.reset();
}

char *FillBuf::get_writable_buf()
{
  return buf_ + fill_pos_;
}

bool FillBuf::is_valid()
{
  return OB_NOT_NULL(buf_) && 0 < buf_len_ && aligned_lsn_.is_valid() && OB_NOT_NULL(kvpair_) &&
         handle_.is_valid() && inst_handle_.is_valid();
}

bool FillBuf::is_full()
{
  bool bool_ret = false;
  if (fill_pos_ == buf_len_) {
    bool_ret = true;
  } else if (LogCacheUtils::is_in_last_cache_line(aligned_lsn_) && (LAST_CACHE_LINE_SIZE == fill_pos_)) {
    bool_ret = true;
  }
  return bool_ret;
}

void FillBuf::update_state(const LSN &aligned_lsn, const int64_t flashback_version)
{
  aligned_lsn_ = aligned_lsn;
  fill_pos_ = 0;
  buf_len_ = CACHE_LINE_SIZE;
  flashback_version_ = flashback_version;
}

//============================================= LogColdCache ==========================
LogColdCache::LogColdCache()
    : palf_id_(INVALID_PALF_ID), palf_env_impl_(NULL), log_reader_(NULL),
      kv_cache_(NULL), logical_block_size_(0), log_cache_stat_(), is_inited_(false) {}

int LogColdCache::init(int64_t palf_id,
                       IPalfEnvImpl *palf_env_impl,
                       LogStorage *log_storage) {
  int ret = OB_SUCCESS;
  if (INVALID_PALF_ID == palf_id || OB_ISNULL(log_storage)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "LogColdCache init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(log_storage->get_logical_block_size(logical_block_size_))) {
    PALF_LOG(WARN, "get_logical_block_size failed", K(ret), K(palf_id));
  } else {
    palf_id_ = palf_id;
    palf_env_impl_ = palf_env_impl;
    log_reader_ = log_storage->get_log_reader();
    kv_cache_ = &OB_LOG_KV_CACHE.get_instance();
    is_inited_ = true;
    PALF_LOG(INFO, "LogColdCache init successfully", K(is_inited_), K(palf_id), K(log_storage));
  }

  return ret;
}

LogColdCache::~LogColdCache()
{
  destroy();
}

void LogColdCache::destroy()
{
  palf_id_ = INVALID_PALF_ID;
  palf_env_impl_ = NULL;
  log_reader_ = NULL;
  kv_cache_ = NULL;
  log_cache_stat_.reset();
  is_inited_ = false;
}

int LogColdCache::alloc_kv_pair(const int64_t flashback_version,
                                const LSN &aligned_lsn,
                                FillBuf &fill_buf)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogColdCache is not inited", K(ret));
  } else if (OB_FAIL(kv_cache_->alloc(MTL_ID(), sizeof(LogKVCacheKey),
                                      sizeof(LogKVCacheValue) + CACHE_LINE_SIZE,
                                      fill_buf.kvpair_, fill_buf.handle_,
                                      fill_buf.inst_handle_))) {
    PALF_LOG(WARN, "alloc kvpair failed", K(ret), K(palf_id_));
  } else {
    fill_buf.kvpair_->key_ = new (fill_buf.kvpair_->key_) LogKVCacheKey(MTL_ID(), palf_id_, aligned_lsn, flashback_version);
    fill_buf.buf_ = reinterpret_cast<char *>(fill_buf.kvpair_->value_) + sizeof(LogKVCacheValue);
    fill_buf.kvpair_->value_ = new (fill_buf.kvpair_->value_) LogKVCacheValue(fill_buf.buf_, CACHE_LINE_SIZE);

    PALF_LOG(TRACE, "alloc kvpair successfully", K(palf_id_), K(fill_buf.kvpair_),  K(fill_buf.kvpair_->value_), KP(fill_buf.kvpair_->value_));
  }

  return ret;
}

int LogColdCache::read(const int64_t flashback_version,
                       const LSN &lsn,
                       const int64_t in_read_size,
                       ReadBuf &read_buf,
                       int64_t &out_read_size,
                       LogIteratorInfo *iterator_info)
{
  #define PRINT_INFO K(palf_id_), K(MTL_ID())

  int ret = OB_SUCCESS;
  bool enable_fill_cache = false;
  int64_t cache_lines_read_size = 0;
  int64_t cache_out_read_size = 0;
  LSN read_lsn = lsn;
  int64_t real_read_size = in_read_size;
  int64_t disk_out_read_size = 0;
  ObTenantStatEstGuard tenant_stat_guard(MTL_ID());
  // read process:
  // 1. read from kv cache
  // 2. deal with miss if miss happens
  // 3. read from disk
  // 4. fill kv cache when it's allowed
  if (OB_SUCC(get_cache_lines_(lsn, flashback_version,
                               in_read_size, read_buf.buf_, cache_lines_read_size, iterator_info))) {
    // read all logs from kv cache successfully
    out_read_size += cache_lines_read_size;
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    PALF_LOG(WARN, "fail to get cache lines", K(ret), K(lsn), K(flashback_version),
             K(in_read_size), K(cache_lines_read_size), PRINT_INFO);
  } else if (OB_FAIL(allow_filling_cache_(iterator_info, enable_fill_cache))) {
    PALF_LOG(WARN, "allow_filling_cache failed", K(ret), K(enable_fill_cache), PRINT_INFO);
  } else if (OB_FAIL(deal_with_miss_(enable_fill_cache, cache_lines_read_size, read_buf.buf_len_, read_lsn,
                                     real_read_size, cache_out_read_size, iterator_info))) {
    PALF_LOG(WARN, "fail to deal with miss", K(ret), K(cache_lines_read_size), K(enable_fill_cache),
             K(read_lsn), K(real_read_size), K(out_read_size), K(cache_out_read_size), PRINT_INFO);
  } else if (FALSE_IT(read_buf.buf_ += cache_out_read_size)) {
  } else if (OB_FAIL(read_from_disk_(read_lsn, real_read_size, read_buf,
                                     disk_out_read_size, iterator_info))) {
    read_buf.buf_ -= cache_out_read_size;
    PALF_LOG(WARN, "read_from_disk_ failed", K(ret), K(read_lsn), K(real_read_size), K(out_read_size));
  } else {
    read_buf.buf_ -= cache_out_read_size;
    out_read_size = out_read_size + cache_out_read_size + disk_out_read_size;

    if (!enable_fill_cache) {
    } else if (OB_FAIL(fill_cache_lines_(flashback_version, read_lsn, disk_out_read_size, read_buf.buf_ + cache_out_read_size))) {
      PALF_LOG(WARN, "fail to fill cache", K(ret), K(read_lsn), K(flashback_version), K(out_read_size), PRINT_INFO);
    } else if (0 == cache_out_read_size) {
      // if read_buf isn't large enough, read_lsn is equal to lsn, which means 'diff' is 0;
      // if read_buf is large enough, adjust read_lsn to new_read_lsn to read more log (up to 'diff') for filling first missing cache line
      // so, adjust buf_ to ignore 'diff' part before return
      offset_t diff = lsn - read_lsn;
      out_read_size = out_read_size - diff;
      MEMMOVE(read_buf.buf_, read_buf.buf_ + diff, out_read_size);
      PALF_LOG(TRACE, "ignore redundant logs in read_buf", K(lsn), K(read_lsn), K(diff), K(out_read_size), K(enable_fill_cache));
    }
  }

  #undef PRINT_INFO

  return ret;
}

int LogColdCache::fill_cache_line(FillBuf &fill_buf)
{
  int ret = OB_SUCCESS;
  if (!fill_buf.is_full()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "can't fill incomplete cache line", K(fill_buf));
  } else if (OB_FAIL(kv_cache_->put_kvpair(fill_buf.inst_handle_, fill_buf.kvpair_, fill_buf.handle_, true /* overwrite*/))) {
    PALF_LOG(WARN, "failed to put kvpair", K(ret), K(fill_buf));
  } else {
    PALF_LOG(TRACE, "put kvpair into cold cache successfully", K(fill_buf));
  }

  return ret;
}

int LogColdCache::allow_filling_cache_(LogIteratorInfo *iterator_info, bool &enable_fill_cache)
{
  int ret = OB_SUCCESS;
  PalfOptions options;
  enable_fill_cache = false;
  if (OB_FAIL(palf_env_impl_->get_options(options))) {
    PALF_LOG(WARN, "get options failed", K(ret));
  } else {
    enable_fill_cache = options.enable_log_cache_ && iterator_info->get_allow_filling_cache();
  }

  return ret;
}

int LogColdCache::deal_with_miss_(const bool enable_fill_cache,
                                  const int64_t has_read_size,
                                  const int64_t buf_len,
                                  LSN &lsn,
                                  int64_t &in_read_size,
                                  int64_t &out_read_size,
                                  LogIteratorInfo *iterator_info)
{
  int ret = OB_SUCCESS;
  if (!lsn.is_valid() || 0 >= in_read_size || 0 > has_read_size) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid arguement", K(ret), K(lsn), K(in_read_size), K(has_read_size));
  } else if (0 != has_read_size) {
    // adjust to statify DIO requirements
    int64_t real_read_size = lower_align(has_read_size, LOG_DIO_ALIGN_SIZE);
    lsn.val_ += real_read_size;
    in_read_size -= real_read_size;
    out_read_size = real_read_size;
  } else {
    // try to fill the first missing cache line
    LSN new_read_start_lsn = LogCacheUtils::lower_align_with_start(lsn, CACHE_LINE_SIZE);
    offset_t backoff = lsn - new_read_start_lsn;

    if (buf_len < in_read_size + backoff) {
      // buf isn't large enough to fill the first missing cache line
    } else if (enable_fill_cache) {
      // adjust lsn to lower_aligned_lsn to fill first miss cache line
      in_read_size += backoff;
      lsn = new_read_start_lsn;
    }
  }

  iterator_info->inc_miss_cnt();
  log_cache_stat_.inc_miss_cnt();
  PALF_LOG(TRACE, "deal with miss", K(ret), K(enable_fill_cache), K(lsn), K(in_read_size), K(has_read_size), K(out_read_size));
  return ret;
}

int LogColdCache::get_cache_lines_(const LSN &lsn,
                                   const int64_t flashback_version,
                                   const int64_t in_read_size,
                                   char *buf,
                                   int64_t &out_read_size,
                                   LogIteratorInfo *iterator_info)
{
  #define PRINT_INFO K(palf_id_), K(MTL_ID())

  int ret = OB_SUCCESS;
  int64_t read_pos = 0;
  LSN cache_read_lsn = lsn;

  while (OB_SUCC(ret) && out_read_size < in_read_size) {
    int64_t curr_round_read_size = 0;
    if (OB_FAIL(get_cache_line_(cache_read_lsn, flashback_version,
                                in_read_size, read_pos, buf,
                                curr_round_read_size))) {
      PALF_LOG(WARN, "fail to get cache line", K(ret), K(cache_read_lsn),
               K(flashback_version), K(in_read_size), K(read_pos),
               K(out_read_size), PRINT_INFO);
    } else {
      read_pos += curr_round_read_size;
      out_read_size += curr_round_read_size;
      cache_read_lsn = cache_read_lsn + curr_round_read_size;
    }
  }

  if (out_read_size != 0) {
    log_cache_stat_.inc_hit_cnt();
    log_cache_stat_.inc_cache_read_size(out_read_size);

    iterator_info->inc_hit_cnt();
    iterator_info->inc_cache_read_size(out_read_size);
  }

  log_cache_stat_.print_stat_info(kv_cache_->store_size(MTL_ID()), palf_id_);

  #undef PRINT_INFO
  return ret;
}

int LogColdCache::get_cache_line_(const LSN &cache_read_lsn,
                                  const int64_t flashback_version,
                                  const int64_t in_read_size,
                                  const int64_t read_pos,
                                  char *buf,
                                  int64_t &curr_round_read_size)
{
  #define PRINT_INFO K(palf_id_), K(MTL_ID())

  int ret = OB_SUCCESS;
  curr_round_read_size = 0;
  LSN aligned_lsn = LogCacheUtils::lower_align_with_start(cache_read_lsn, CACHE_LINE_SIZE);
  offset_t diff = cache_read_lsn - aligned_lsn;
  LogKVCacheKey key(MTL_ID(), palf_id_, aligned_lsn, flashback_version);
  LogKVCacheValueHandle val_handle;
  int tmp_ret = OB_SUCCESS;
  char *cache_log_buf = NULL;
  if (OB_FAIL(kv_cache_->get_log(key, val_handle))) {
    PALF_LOG(WARN, "fail to get log from kv cache", K(ret), K(key), PRINT_INFO);
  } else if (OB_ISNULL((cache_log_buf = val_handle.value_->get_buf()))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "get null buf from log kv cache", K(ret), K(key));
  } else {
    // buf_size is either CACHE_LINE_SIZE or LAST_CACHE_LINE_SIZE
    int64_t buf_size = val_handle.value_->get_buf_size();
    if (0 == diff) {
      // start with aligned_lsn
      int64_t remained_size = in_read_size - read_pos;
      curr_round_read_size = (remained_size <= buf_size) ? remained_size : buf_size;
    } else {
      // start with lsn
      int64_t remained_cache_line_size = buf_size - diff;
      curr_round_read_size = (in_read_size <= remained_cache_line_size) ? in_read_size : remained_cache_line_size;
    }

    MEMCPY(buf + read_pos, cache_log_buf + diff, curr_round_read_size);

    PALF_LOG(TRACE, "cache hit, read from log cold cache", K(key), K(read_pos), K(cache_read_lsn), K(flashback_version),
             K(aligned_lsn), K(in_read_size), K(curr_round_read_size), PRINT_INFO);
  }

  #undef PRINT_INFO
  return ret;
}

int LogColdCache::fill_cache_lines_(const int64_t flashback_version,
                                    const LSN &lsn,
                                    const int64_t fill_size,
                                    char *buf)
{
  int ret = OB_SUCCESS;
  #define PRINT_INFO K(palf_id_), K(MTL_ID())

  if (!is_valid_flashback_version(flashback_version) || !lsn.is_valid() || 0 >= fill_size) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(lsn), K(fill_size), K(flashback_version));
  } else {
    // lsn is always expected to be aligned to CACHE_LINE_SIZE
    // In the following situations, it's not align to CACHE_LINE_SIZE:
    // 1. lsn is aligned to 4KB in deal_with_miss_() for DIO. This part is already in kvcache, so need to ignore
    // 2. the buf isn't large enough to fill first missing cache line. Filling log cache from the second cache line
    LSN fill_lsn = LogCacheUtils::is_lower_align_with_start(lsn, CACHE_LINE_SIZE) ?
                   lsn : LogCacheUtils::upper_align_with_start(lsn, CACHE_LINE_SIZE);
    int64_t diff = fill_lsn - lsn;
    int64_t fill_pos = diff;
    int64_t remained_size = fill_size - diff;

    // only fill in cache in two cases for simplicity:
    // 1. remained_size is big enough to fill a complete cache line
    // 2. fill an uncomplete cache line only if it's the last cache line in the block
    while (OB_SUCC(ret) && 0 < remained_size) {
      int64_t curr_round_fill_size = 0;
      if (remained_size >= CACHE_LINE_SIZE) {
        curr_round_fill_size = CACHE_LINE_SIZE;
      } else if (LogCacheUtils::up_to_next_block(fill_lsn, remained_size)) {
        // fill last cache line in the block, remained_size is equal to CACHE_LINE_SIZE - 4KB
        OB_ASSERT(LAST_CACHE_LINE_SIZE == remained_size);
        curr_round_fill_size = remained_size;
      } else {
        break;
      }

      OB_ASSERT(LogCacheUtils::is_lower_align_with_start(fill_lsn, CACHE_LINE_SIZE));
      if (OB_FAIL(fill_cache_line_(flashback_version, fill_lsn, curr_round_fill_size, fill_pos, buf))) {
        PALF_LOG(WARN, "fill cache line failed", K(ret), K(flashback_version),
                 K(fill_lsn), K(curr_round_fill_size), K(fill_pos), PRINT_INFO);
      } else {
        remained_size -= curr_round_fill_size;
        fill_pos += curr_round_fill_size;
        fill_lsn = fill_lsn + curr_round_fill_size;
      }
    }

    if (OB_SIZE_OVERFLOW == ret || OB_ALLOCATE_MEMORY_FAILED == ret) {
      ret = OB_SUCCESS;
      PALF_LOG(TRACE, "can't fill logs because of memory limit",  K(flashback_version), K(fill_lsn), PRINT_INFO);
    }
  }

  #undef PRINT_INFO

  return ret;
}

int LogColdCache::fill_cache_line_(const int64_t flashback_version,
                                   const LSN &fill_lsn,
                                   const int64_t fill_size,
                                   const int64_t fill_pos,
                                   char *buf)
{
  int ret = OB_SUCCESS;
  LogKVCacheKey new_key(MTL_ID(), palf_id_, fill_lsn, flashback_version);
  LogKVCacheValue new_value;
  if (OB_FAIL(new_value.init(buf + fill_pos, fill_size))) {
    PALF_LOG(WARN, "new value init failed", K(ret), K(new_key), K(new_value));
  } else if (OB_SUCC(kv_cache_->put_log(new_key, new_value))) {
    // fill successfully
    PALF_LOG(TRACE, "fill cache successfully", K(fill_lsn), K(fill_pos), K(fill_size), K(palf_id_), K(MTL_ID()));
  } else if (OB_ENTRY_EXIST != ret){
    PALF_LOG(WARN, "put log into kv cache failed", K(ret), K(new_key), K(new_value));
  } else {
    ret = OB_SUCCESS;
    log_cache_stat_.inc_cache_fill_amplification(fill_size);
    PALF_LOG(TRACE, "LogKVCacheKey has existed", K(new_key), K(fill_pos), K(fill_size), K(palf_id_), K(MTL_ID()));
  }
  return ret;
}

int LogColdCache::read_from_disk_(const LSN &read_lsn,
                                  const int64_t in_read_size,
                                  ReadBuf &read_buf,
                                  int64_t &out_read_size,
                                  LogIteratorInfo *iterator_info)
{
  int ret = OB_SUCCESS;
  const block_id_t read_block_id = lsn_2_block(read_lsn, logical_block_size_);
  const offset_t real_read_offset = get_phy_offset_(read_lsn);

  if (OB_FAIL(log_reader_->pread(read_block_id, real_read_offset,
                                 in_read_size, read_buf, out_read_size, iterator_info))) {
    PALF_LOG(WARN, "read_from_disk failed", K(ret), K(read_lsn), K(in_read_size));
  } else {
    PALF_LOG(TRACE, "read_from_disk succeed", K(read_lsn), K(in_read_size),
             K(in_read_size), K(read_lsn), K(out_read_size));
  }
  return ret;
}

offset_t LogColdCache::get_phy_offset_(const LSN &lsn) const
{
  return lsn_2_offset(lsn, logical_block_size_) + MAX_INFO_BLOCK_SIZE;
}

// =======================================LogCacheStat=======================================
LogColdCache::LogCacheStat::LogCacheStat()
    : hit_cnt_(0), miss_cnt_(0), cache_read_size_(0), cache_fill_amplification_(0), last_print_time_(0),
      last_record_hit_cnt_(0), last_record_miss_cnt_(0),
      last_record_cache_read_size_(0) {}

LogColdCache::LogCacheStat::~LogCacheStat()
{
  reset();
}

void LogColdCache::LogCacheStat::reset()
{
  hit_cnt_ = 0;
  miss_cnt_ = 0;
  cache_read_size_ = 0;
  cache_fill_amplification_ = 0;
  last_print_time_ = 0;
  last_record_hit_cnt_ = 0;
  last_record_miss_cnt_ = 0;
  last_record_cache_read_size_ = 0;
}

void LogColdCache::LogCacheStat::inc_hit_cnt()
{
  EVENT_INC(ObStatEventIds::LOG_KV_CACHE_HIT);
  ATOMIC_INC(&hit_cnt_);
}

void LogColdCache::LogCacheStat::inc_miss_cnt()
{
  EVENT_INC(ObStatEventIds::LOG_KV_CACHE_MISS);
  ATOMIC_INC(&miss_cnt_);
}

void LogColdCache::LogCacheStat::inc_cache_read_size(int64_t cache_read_size)
{
  ATOMIC_AAF(&cache_read_size_, cache_read_size);
}

void LogColdCache::LogCacheStat::inc_cache_fill_amplification(int64_t cache_fill_amplification_)
{
  ATOMIC_AAF(&cache_fill_amplification_, cache_fill_amplification_);
}

void LogColdCache::LogCacheStat::print_stat_info(int64_t cache_store_size, int64_t palf_id)
{
  if (palf_reach_time_interval(PALF_STAT_PRINT_INTERVAL_US, last_print_time_)) {
    int64_t interval_hit_cnt = hit_cnt_ - last_record_hit_cnt_;
    int64_t interval_miss_cnt = miss_cnt_ - last_record_miss_cnt_;
    int64_t interval_cache_read_size = cache_read_size_ - last_record_cache_read_size_;
    int64_t total_cnt = (interval_hit_cnt + interval_miss_cnt == 0) ? 1 : interval_hit_cnt + interval_miss_cnt;
    PALF_LOG(INFO, "[PALF STAT LOG COLD CACHE HIT RATE]", "hit_cnt", interval_hit_cnt,
             "miss_cnt", interval_miss_cnt, "hit_rate",
             interval_hit_cnt * 1.0 / total_cnt,
             "cache_read_size", interval_cache_read_size, K(cache_store_size),
             K(cache_fill_amplification_), K(palf_id), K(MTL_ID()));
    last_record_hit_cnt_ = hit_cnt_;
    last_record_miss_cnt_ = miss_cnt_;
    last_record_cache_read_size_ = cache_read_size_;
  }
}

// =======================================LogCache=======================================
LogCache::LogCache() : hot_cache_(), cold_cache_(), fill_buf_(), is_inited_(false) {}

LogCache::~LogCache()
{
  destroy();
}

void LogCache::destroy()
{
  palf_id_ = INVALID_PALF_ID;
  hot_cache_.destroy();
  cold_cache_.destroy();
  fill_buf_.reset();
  is_inited_ = false;
}

int LogCache::init(const int64_t palf_id,
                   IPalfHandleImpl *palf_handle_impl,
                   IPalfEnvImpl *palf_env_impl,
                   LogStorage *log_storage)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(WARN, "LogCache init failed", K(ret));
  } else if (OB_FAIL(hot_cache_.init(palf_id, palf_handle_impl))){
    PALF_LOG(WARN, "hot cache init failed", K(ret), K(palf_id));
  } else if (OB_FAIL(cold_cache_.init(palf_id, palf_env_impl, log_storage))) {
    PALF_LOG(WARN, "cold cache init failed", K(ret), K(palf_id));
  } else {
    palf_id_ = palf_id;
    is_inited_ = true;
    PALF_LOG(INFO, "LogCache init successfully", K(palf_id));
  }

  return ret;
}

bool LogCache::is_inited()
{
  return is_inited_;
}

int LogCache::read(const int64_t flashback_version,
                   const LSN &lsn,
                   const int64_t in_read_size,
                   ReadBuf &read_buf,
                   int64_t &out_read_size,
                   LogIteratorInfo *iterator_info)
{
  int ret = OB_SUCCESS;
  const bool is_cold_cache = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogCache is not inited!", K(ret));
  } else if (!lsn.is_valid() || 0 >= in_read_size || !read_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "Invalid argument!!!", K(ret), K(lsn), K(in_read_size), K(read_buf));
  } else if (OB_SUCC(read_hot_cache_(lsn, in_read_size, read_buf.buf_, out_read_size))) {
    // read data from hot_cache successfully
    iterator_info->inc_hit_cnt(is_cold_cache);
    iterator_info->inc_cache_read_size(out_read_size, is_cold_cache);
  } else if (FALSE_IT(iterator_info->inc_miss_cnt(is_cold_cache))) {
  } else if (OB_FAIL(read_cold_cache_(flashback_version, lsn, in_read_size,
                                      read_buf, out_read_size, iterator_info))) {
    PALF_LOG(WARN, "fail to read from cold cache", K(ret), K(lsn), K(in_read_size), K(read_buf), K(out_read_size));
  } else {
    // read data from kv cache successfully
  }

  return ret;
}

int LogCache::fill_cache_when_slide(const LSN &lsn,
                                    const int64_t fill_size,
                                    const int64_t flashback_version)
{
  int ret = OB_SUCCESS;
  LSN fill_lsn = lsn;
  int64_t remained_size = fill_size;
  char *buf = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogCache has not been inited", K(ret), K(lsn), K(fill_size), K(flashback_version));
  } else if (!lsn.is_valid() || 0 >= fill_size || 0 > flashback_version) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(lsn), K(fill_size), K(flashback_version));
    // make sure that fill_lsn, fill_size, fill_buf_.fill_pos is match before enter 'while'
  } else if (OB_FAIL(try_update_fill_buf_(flashback_version, fill_lsn, remained_size))) {
    PALF_LOG(WARN, "try update fill_buf failed", K(ret), K(lsn), K(flashback_version), K(fill_buf_), K(remained_size));
  } else {
    OB_ASSERT(fill_buf_.is_valid() && fill_lsn == fill_buf_.aligned_lsn_ + fill_buf_.fill_pos_);

    while (OB_SUCC(ret) && 0 < remained_size) {
      int64_t out_read_size = 0;
      int64_t in_read_size = cal_in_read_size_(remained_size);

      if (OB_ISNULL(buf = fill_buf_.get_writable_buf())) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "value buf is NULL in fill_buf_", K(ret), K(fill_lsn), K(remained_size));
      } else if (OB_FAIL(read_hot_cache_(fill_lsn, in_read_size, buf, out_read_size)) && OB_READ_NOTHING != ret) {
        PALF_LOG(WARN, "read hot cache failed", K(ret), K(fill_lsn), K(in_read_size), K(out_read_size));
      } else if (out_read_size < in_read_size) {
        // partitally miss hot cache, give up to fill current cache line
        ret = OB_ENTRY_NOT_EXIST;
        PALF_LOG(TRACE, "can not read enough logs from group buffer, skip this cache line",
                 K(ret), K(fill_lsn), K(remained_size), K(in_read_size), K(out_read_size), K(fill_buf_));
      } else {
        fill_buf_.fill_pos_ += out_read_size;
        remained_size = remained_size - out_read_size;

        if (!fill_buf_.is_full()) {
          PALF_LOG(TRACE, "successfully memcpy committed logs from hot cache to fill_buf",
                   K(fill_lsn), K(remained_size), K(out_read_size), K(fill_buf_));
        } else if (OB_FAIL(cold_cache_.fill_cache_line(fill_buf_))) {
          PALF_LOG(WARN, "fill_cache_line failed", K(ret), K(fill_lsn), K(remained_size), K(fill_buf_));
        } else if (FALSE_IT(fill_lsn = fill_lsn + out_read_size)) {
        // current cache line is full, update fill_buf for next fill
        } else if (OB_FAIL(update_fill_buf_(flashback_version, fill_lsn))) {
          PALF_LOG(WARN, "failed to update fill_buf, stop filling cold cache", K(ret), K(flashback_version), K(fill_lsn));
        } else {
          OB_ASSERT(fill_lsn == LogCacheUtils::lower_align_with_start(fill_lsn, CACHE_LINE_SIZE));
          PALF_LOG(TRACE, "insert kv into cold cache and update fill_buf successfully", K(fill_lsn), K(remained_size), K(fill_buf_));
        }
      }
    }
  }

  if (OB_FAIL(ret) && fill_buf_.is_valid()) {
    fill_buf_.reset();
  }

  return ret;
}

int LogCache::read_hot_cache_(const LSN &read_begin_lsn,
                             const int64_t in_read_size,
                             char *buf,
                             int64_t &out_read_size)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(hot_cache_.read(read_begin_lsn, in_read_size, buf, out_read_size))
      && out_read_size > 0) {
    // read data from hot_cache successfully
    PALF_LOG(TRACE, "read hot cache successfully", K(read_begin_lsn), K(in_read_size), K(out_read_size));
  } else if (OB_SUCCESS == ret && 0 == out_read_size){
    ret = OB_READ_NOTHING;
    PALF_LOG(WARN, "read nothing from hot cache", K(ret), K(read_begin_lsn), K(in_read_size), K(out_read_size));
  } else {
    PALF_LOG(WARN, "read hot cache failed", K(ret), K(read_begin_lsn), K(in_read_size), K(out_read_size));
  }

  return ret;
}

int LogCache::read_cold_cache_(const int64_t flashback_version,
                              const LSN &lsn,
                              const int64_t in_read_size,
                              ReadBuf &read_buf,
                              int64_t &out_read_size,
                              LogIteratorInfo *iterator_info)
{
  int ret = OB_SUCCESS;
  if (!lsn.is_valid() || 0 >= in_read_size || !read_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(lsn), K(in_read_size), K(read_buf));
  } else if (OB_FAIL(cold_cache_.read(flashback_version, lsn, in_read_size,
                                      read_buf, out_read_size,
                                      iterator_info))) {
    PALF_LOG(WARN, "read cold cache failed", K(ret), K(lsn), K(in_read_size), K(read_buf), K(out_read_size));
  } else {
    PALF_LOG(TRACE, "read cold cache successfully", K(lsn), K(in_read_size), K(read_buf), K(out_read_size));
  }

  return ret;
}

// in normal case, fill_lsn should be equal to fill_buf_.aligned_lsn_ + fill_buf_.fill_pos_
// if not, we should update fill_buf
int LogCache::try_update_fill_buf_(const int64_t flashback_version,
                                   LSN &fill_lsn,
                                   int64_t &fill_size)
{
  int ret = OB_SUCCESS;
  LSN aligned_lsn = LogCacheUtils::lower_align_with_start(fill_lsn, CACHE_LINE_SIZE);

  if (!fill_buf_.is_valid()) {
    if (aligned_lsn == fill_lsn) {
      if (OB_FAIL(update_fill_buf_(flashback_version, fill_lsn))) {
        PALF_LOG(WARN, "update fill_buf failed", K(ret), K(fill_lsn), K(aligned_lsn), K(flashback_version), K(fill_buf_));
      }
    } else {
      ret = OB_DISCONTINUOUS_LOG;
      PALF_LOG(WARN, "fill_buf_ is invalid, fill_lsn and aligned_lsn are not match", K(ret), K(fill_lsn), K(aligned_lsn), K(fill_buf_));
    }
    // when fill_buf_ is valid, in the two following cases, we can't continue to fill cold cache
    // 1. flashback_version changes
    // 2. logs are not continuous, which means fill_lsn is not equal to fill_buf_.aligned_lsn_ + fill_buf_.fill_pos_
  } else if (flashback_version != fill_buf_.flashback_version_) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "flashback happened during filling cache, skip it", K(ret),
             K(flashback_version), K(fill_buf_));
  } else if (aligned_lsn != fill_buf_.aligned_lsn_ ||
             fill_buf_.fill_pos_ != fill_lsn - aligned_lsn) {
    ret = OB_DISCONTINUOUS_LOG;
    PALF_LOG(WARN, "fill_lsn and fill_buf_.aligned_lsn are not match", K(ret),
             K(fill_lsn), K(aligned_lsn), K(fill_buf_));
  }

  if (OB_SUCC(ret)) {
  } else if (LogCacheUtils::is_in_last_cache_line(fill_lsn)) {
    PALF_LOG(TRACE, "give up to fill last cache line", K(fill_lsn), K(aligned_lsn));
  } else {
    // Although we can't fill the cache line starting with aligned_lsn
    // we can check whether we can start to fill the next cache line
    LSN upper_aligned_lsn = LogCacheUtils::upper_align_with_start(fill_lsn, CACHE_LINE_SIZE);
    int64_t diff = upper_aligned_lsn - fill_lsn;
    if (fill_size <= diff) {
      PALF_LOG(WARN, "current group entry is not enough to start next cache line",
               K(ret), K(fill_size), K(diff), K(upper_aligned_lsn), K(fill_lsn));
    } else if (FALSE_IT(fill_lsn = fill_lsn + diff)) {
    } else if (OB_FAIL(update_fill_buf_(flashback_version, fill_lsn))) {
      PALF_LOG(WARN, "update fill_buf failed", K(ret), K(fill_lsn), K(aligned_lsn), K(flashback_version), K(fill_buf_));
    } else {
      ret = OB_SUCCESS;
      fill_size = fill_size - diff;

      PALF_LOG(TRACE, "skip current cache line, fill next cache line",
               K(fill_lsn), K(fill_size), K(diff));
    }
  }

  return ret;
}

int LogCache::update_fill_buf_(const int64_t flashback_version,
                               LSN &aligned_lsn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cold_cache_.alloc_kv_pair(flashback_version, aligned_lsn, fill_buf_))) {
    PALF_LOG(WARN, "failed to alloc kv pair", K(ret), K(flashback_version), K(aligned_lsn));
  } else {
    fill_buf_.update_state(aligned_lsn, flashback_version);
    PALF_LOG(TRACE, "update fill_buf successfully", K(fill_buf_));
  }
  return ret;
}

int64_t LogCache::cal_in_read_size_(const int64_t fill_size)
{
  return (fill_size > fill_buf_.buf_len_ - fill_buf_.fill_pos_)
             ? fill_buf_.buf_len_ - fill_buf_.fill_pos_
             : fill_size;
}

} // end namespace palf
} // end namespace oceanbase
