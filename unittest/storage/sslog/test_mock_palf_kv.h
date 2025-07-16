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

#ifndef OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SSLOG_MOCK_PALF_KV
#define OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SSLOG_MOCK_PALF_KV

// #include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/incremental/sslog/ob_sslog_kv_define.h"
#include <cstring>
#include <unordered_map>
#include <vector>
namespace oceanbase
{

namespace unittest
{

using namespace common;
using namespace std;
using namespace sslog;

struct VectorHash
{
  std::size_t operator()(const std::vector<char> &vec) const
  {
    std::size_t hash = 0;
    for (char c : vec) {
      hash ^= std::hash<char>()(c) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
    }
    return hash;
  }
};

class ObMockPalfKV : public sslog::ObPalfKVAdpaterInterface
{
public:
  std::unordered_map<vector<char>, vector<char>, VectorHash> map_;
  int64_t base_gts_;
  ObSpinLock lock_;

  share::SCN max_gc_version_;

 ModulePageAllocator allocator_;

  bool watch_key_errsim_;

public:
  void clear() { map_.clear(); }

  ObMockPalfKV() : map_(), base_gts_(0), lock_(), max_gc_version_(),allocator_(),watch_key_errsim_(false) {}

  void reset()
  {}

  virtual int init(const uint64_t cluster_id, const uint64_t tenant_id)
  {
    int ret = OB_SUCCESS;

    return ret;
  }
  virtual int get(const common::ObString &key, common::ObString &value)
  {
    int ret = OB_SUCCESS;
    vector<char> palf_key;
    palf_key.resize(key.length());
    memcpy(palf_key.data(), key.ptr(), key.length());

    if (map_.find(palf_key) == map_.end()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      memcpy(value.ptr(), map_.at(palf_key).data(), map_[palf_key].size());
    }

    return ret;
  }
  virtual int put(const common::ObString &key, const common::ObString &value)
  {
    int ret = OB_SUCCESS;
    vector<char> palf_key, palf_value;
    palf_key.resize(key.length());
    palf_value.resize(value.length());
    memcpy(palf_key.data(), key.ptr(), key.length());
    memcpy(palf_value.data(), value.ptr(), value.length());

    auto res = map_.insert({palf_key, palf_value});

    if (res.second == false) {
      ret = OB_ENTRY_EXIST;
    }

    return ret;
  }
  virtual int set(const common::ObString &key, const common::ObString &value)
  {
    int ret = OB_SUCCESS;
    vector<char> palf_key;
    vector<char> palf_new_val;
    palf_key.resize(key.length());
    palf_new_val.resize(value.length());
    memcpy(palf_key.data(), key.ptr(), key.length());
    memcpy(palf_new_val.data(), value.ptr(), value.length());
    auto iter = map_.find(palf_key);
    if (iter == map_.end()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      iter->second.resize(palf_new_val.size());
      memcpy(iter->second.data(), palf_new_val.data(), palf_new_val.size());
    }
    return ret;
  }
  virtual int cas(const common::ObString &key,
                  const common::ObString &old_value,
                  const common::ObString &new_value,
                  bool &expected)
  {
    int ret = OB_SUCCESS;

    // STORAGE_LOG(INFO, "print cas op",K(key), K(old_value), K(new_value));
    expected = false;
    vector<char> palf_key;
    vector<char> palf_old_val;
    vector<char> palf_new_val;
    palf_key.resize(key.length());
    palf_old_val.resize(old_value.length());
    palf_new_val.resize(new_value.length());
    memcpy(palf_key.data(), key.ptr(), key.length());
    memcpy(palf_old_val.data(), old_value.ptr(), old_value.length());
    memcpy(palf_new_val.data(), new_value.ptr(), new_value.length());

    // print_one_kv("old_kv", palf_key, palf_old_val);
    // print_one_kv("new_kv", palf_key, palf_new_val);

    auto iter = map_.find(palf_key);
    if (iter == map_.end()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (iter->second == palf_old_val) {
      iter->second.resize(palf_new_val.size());
      memcpy(iter->second.data(), palf_new_val.data(), palf_new_val.size());
      expected = true;
    } else {
      expected = false;
      // print_one_kv("diff old_kv in map", iter->first, iter->second);
      // print_one_kv("diff old_kv in arg", palf_key, palf_old_val);
      // print_one_kv("diff new_kv", iter->first, palf_new_val);
    }

    return ret;
  }

  virtual int watch_cas(const common::ObString &key,
                        const common::ObString &watch_key,
                        const common::ObString &old_value,
                        const common::ObString &new_value,
                        bool &expected)
  {
    int ret = OB_SUCCESS;

    expected = false;

    vector<char> palf_key;
    vector<char> palf_old_val;
    vector<char> palf_new_val;
    vector<char> watch_key_val;
    palf_key.resize(key.length());
    palf_old_val.resize(old_value.length());
    palf_new_val.resize(new_value.length());
    watch_key_val.resize(watch_key.length());
    memcpy(palf_key.data(), key.ptr(), key.length());
    memcpy(palf_old_val.data(), old_value.ptr(), old_value.length());
    memcpy(palf_new_val.data(), new_value.ptr(), new_value.length());
    memcpy(watch_key_val.data(), watch_key.ptr(), watch_key.length());

    auto iter = map_.find(palf_key);
    auto watch_iter = map_.find(watch_key_val);

    if (watch_iter == map_.end() || watch_key_errsim_) {
      ret = OB_ENTRY_NOT_EXIST;
      TRANS_LOG(WARN, "the watch key is not existed", K(ret), K(watch_key));
    } else if (iter == map_.end()) {
      ret = OB_ENTRY_NOT_EXIST;
      TRANS_LOG(WARN, "the palf key is not existed", K(ret), K(key));
    } else if (iter->second == palf_old_val) {
      iter->second.resize(palf_new_val.size());
      memcpy(iter->second.data(), palf_new_val.data(), palf_new_val.size());
      expected = true;
    } else {
      expected = false;
    }

    return ret;
  }

  virtual int delete_kv(const common::ObString &key)
  {
    int ret = OB_SUCCESS;
    vector<char> palf_key;
    palf_key.resize(key.length());
    memcpy(palf_key.data(), key.ptr(), key.length());
    auto iter = map_.find(palf_key);

    if (iter == map_.end()) {
      TRANS_LOG(INFO, "delete key -- no need to delete", K(ret),K(key));
    } else {
      map_.erase(iter);
    }
    return ret;
  }
  virtual int list(const common::ObString &prefix,
                   const int32_t limit,
                   ObSSLogKVTempCache &kv_pairs)
  {
    int ret = OB_SUCCESS;
    auto iter = map_.begin();
    bool prefix_match = false;
    int64_t id_count = 0;
    while (OB_SUCC(ret) && iter != map_.end()) {
      const vector<char> &key = iter->first;

      // char *key_raw = new char[key.size()];
      // memcpy(key_raw, key.data(), key.size());
      ObStringBuffer &key_str = kv_pairs.get_cur_key_buf();
      key_str.append(key.data(), key.size());

      prefix_match = true;
      for(int i = 0; i < prefix.length(); i++)
      {
        if(*(prefix.ptr() + i) != key[i])
        {
          prefix_match =false;
        }
      }
      if (prefix_match) {
        const vector<char> &val = iter->second;
        // char *val_raw = new char[ObSSLogKVRowPhysicalBuf::MAX_VAL_BUF_LEN];
        // memcpy(val_raw, val.data(), val.size());
        ObStringBuffer &val_str = kv_pairs.get_cur_val_buf();
        val_str.append(val.data(), val.size());
        // kv_pairs.push_back({key_str, val_str});
        TRANS_LOG(INFO, "list match", K(kv_pairs),K(id_count),K(iter->first.size()), K(iter->second.size()));
        if (OB_FAIL(kv_pairs.store_into_cache())) {
          STORAGE_LOG(WARN, "store into sslog kv cache failed", K(ret), K(key_str), K(val_str));
        }
      }

      kv_pairs.reuse_cur_buf();
      // ObSSLogKVRowUserKey my_key;
      // ObSSLogKVRowPhysicalKey cur_key;
      // int64_t tmp_pos = 0;
      // my_key.deserialize(prefix.ptr(), prefix.length(), tmp_pos);
      // tmp_pos = 0;
      // cur_key.deserialize(key.data(), key.size(), tmp_pos);
      // TRANS_LOG(INFO, "qc debug", K(cur_key), K(my_key));
      //

      id_count++;
      iter++;
    }

    return ret;
  }

  virtual int get_gts(share::SCN &gts)
  {
    int ret = OB_SUCCESS;

    ObSpinLockGuard guard(lock_);

    base_gts_++;
    gts.convert_from_ts(base_gts_);

    return ret;
  }

  virtual int get_limit_id(share::SCN &limit_id)
  {
    int ret = OB_NOT_SUPPORTED;

    return ret;
  }

  virtual void inc_unused_kv_cnt() {}
  virtual int64_t get_unused_kv_cnt() { return  0;};
  virtual void clear_unused_kv_cnt() {}
  virtual void set_max_gc_version(const share::SCN &gc_version) {}
  virtual share::SCN get_max_gc_version() const { return max_gc_version_; }

  virtual void set_min_gc_succ_version(const share::SCN &gc_succ_version) {};
  virtual share::SCN get_min_gc_succ_version() const {return max_gc_version_;};

  void print_all_kv(const char *mod)
  {
    int ret = OB_SUCCESS;
    auto iter = map_.begin();
    int64_t count = 0;
    ObSSLogKVRowPhysicalKey cur_key;
    ObSSLogKVRowPhysicalValue cur_val;
    int tmp_ret1, tmp_ret2;
    int64_t tmp_pos1, tmp_pos2;
    while (iter != map_.end()) {
      count++;
      tmp_pos1 = tmp_pos2 = 0;
      tmp_ret1 = cur_key.deserialize(iter->first.data(), iter->first.size(), tmp_pos1);
      tmp_ret2 = cur_val.deserialize(iter->second.data(), iter->second.size(), tmp_pos2);
      TRANS_LOG(INFO, "PRINT EACH KV", K(mod), K(count), K(tmp_ret1), K(tmp_ret2), K(tmp_pos1),
                K(tmp_pos2), K(cur_key), K(cur_val));
      iter++;
    }
  }

  void print_one_kv(const char *kv_name, const vector<char> &key_buf, const vector<char> &val_buf)
  {
    ObSSLogKVRowPhysicalKey cur_key;
    ObSSLogKVRowPhysicalValue cur_val;
    int64_t tmp_pos1, tmp_pos2;
    int tmp_ret1, tmp_ret2;
    tmp_pos1 = tmp_pos2 = 0;
    tmp_ret1 = cur_key.deserialize(key_buf.data(), key_buf.size(), tmp_pos1);
    tmp_ret2 = cur_val.deserialize(val_buf.data(), val_buf.size(), tmp_pos2);
    TRANS_LOG(INFO, "PRINT ONE KV", K(kv_name), K(tmp_ret1), K(tmp_ret2), K(tmp_pos1), K(tmp_pos2),
              K(cur_key), K(cur_val));
  }
};

class ObMockFirstVersionIterator : public sslog::ObPrefixFirstVersionBaseIterator
{
public:
  ObMockFirstVersionIterator(const uint64_t tenant_id, ObMockPalfKV *palf_kv)
      : ObPrefixFirstVersionBaseIterator(tenant_id), cache_index_(0), kv_tmp_cache_(tenant_id),
        mock_palf_kv_(palf_kv)
  {}

  virtual void reset()
  {
    cache_index_ = 0;
    kv_tmp_cache_.reset();
    ObPrefixFirstVersionBaseIterator::reset();
  }
  virtual int inner_get_next_kv_(ObString &key_buf, ObString &val_buf)
  {
    int ret = OB_SUCCESS;

    if (OB_FAIL(ret) || kv_tmp_cache_.count() > 0) {
      // do nothing
    } else if (OB_FAIL(mock_palf_kv_->list(common_prefix_, INT32_MAX, kv_tmp_cache_))) {
      TRANS_LOG(INFO, "list kv failed", K(ret), K(kv_tmp_cache_));
    }

    if (OB_FAIL(ret)) {
    } else if (kv_tmp_cache_.count() <= cache_index_) {
      ret = OB_ITER_END;
      TRANS_LOG(INFO, "iter the all kv_tmp_cache", K(ret), K(cache_index_), K(kv_tmp_cache_));
    } else if (OB_FAIL(kv_tmp_cache_.key_at(cache_index_, key_buf))) {
      TRANS_LOG(WARN, "get key failed", K(ret), K(cache_index_), K(key_buf));
    } else if (OB_FAIL(kv_tmp_cache_.val_at(cache_index_, val_buf))) {
      TRANS_LOG(WARN, "get val failed", K(ret), K(cache_index_), K(val_buf));
    } else {
      cache_index_++;
    }
    return ret;
  }

private:
  int64_t cache_index_;
  ObSSLogKVTempCache kv_tmp_cache_;
  ObMockPalfKV *mock_palf_kv_;
};

class ObMockUserKeyAllVersionBaseIterator : public sslog::ObUserKeyAllVersionBaseIterator
{
public:
  ObMockUserKeyAllVersionBaseIterator(const uint64_t tenant_id)
      : ObUserKeyAllVersionBaseIterator(tenant_id), cache_index_(0), kv_tmp_cache_(tenant_id)
  {}

  virtual void reset()
  {
    cache_index_ = 0;
    kv_tmp_cache_.reset();
    sslog::ObUserKeyAllVersionBaseIterator::reset();
  }

  virtual int inner_get_next_kv_(ObString &key_buf, ObString &val_buf)
  {
    int ret = OB_SUCCESS;

    if (OB_FAIL(ret) || kv_tmp_cache_.count() > 0) {
      // do nothing
    } else if (OB_FAIL(interface_->list(user_key_buf_, INT32_MAX, kv_tmp_cache_))) {
      TRANS_LOG(INFO, "list kv failed", K(ret), K(kv_tmp_cache_));
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (kv_tmp_cache_.count() <= cache_index_) {
      ret = OB_ITER_END;
      TRANS_LOG(INFO, "iter the all kv_tmp_cache", K(ret), K(cache_index_), K(kv_tmp_cache_));
    } else if (OB_FAIL(kv_tmp_cache_.key_at(cache_index_, key_buf))) {
      TRANS_LOG(WARN, "get key failed", K(ret), K(cache_index_), K(key_buf));
    } else if (OB_FAIL(kv_tmp_cache_.val_at(cache_index_, val_buf))) {
      TRANS_LOG(WARN, "get val failed", K(ret), K(cache_index_), K(val_buf));
    } else {
      cache_index_++;
    }

    return ret;
  }

private:
  int64_t cache_index_;
  ObSSLogKVTempCache kv_tmp_cache_;
};

} // namespace unittest
} // namespace oceanbase
#endif
