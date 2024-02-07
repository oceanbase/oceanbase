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

#ifndef _OB_OPT_SYSTEM_STAT_H_
#define _OB_OPT_SYSTEM_STAT_H_

#include <stdint.h>
#include <cstddef>
#include "lib/hash_func/murmur_hash.h"
#include "lib/utility/ob_print_utils.h"
#include "share/cache/ob_kvcache_struct.h"

namespace oceanbase {
namespace common {

/**
 * Optimizer System Level Statistics
 */
class ObOptSystemStat : public common::ObIKVCacheValue
{
  OB_UNIS_VERSION_V(1);
public:
  struct Key : public common::ObIKVCacheKey
  {
    Key() : tenant_id_(0)
    {
    }
    explicit Key(uint64_t tenant_id) :
      tenant_id_(tenant_id)
    {
    }
    void init(uint64_t tenant_id)
    {
      tenant_id_ = tenant_id;
    }
    uint64_t hash() const
    {
      return common::murmurhash(this, sizeof(Key), 0);
    }
    int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
    bool operator==(const ObIKVCacheKey &other) const
    {
      const Key &other_key = reinterpret_cast<const Key&>(other);
      return tenant_id_ == other_key.tenant_id_;
    }
    uint64_t get_tenant_id() const
    {
      return tenant_id_;
    }

    int64_t size() const
    {
      return sizeof(*this);
    }

    int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
    {
      int ret = OB_SUCCESS;
      Key *tmp = NULL;
      if (NULL == buf || buf_len < size()) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid argument.",
            K(ret), KP(buf), K(buf_len), K(size()));
      } else {
        tmp = new (buf) Key();
        *tmp = *this;
        key = tmp;
      }
      return ret;
    }

    bool is_valid() const
    {
      return tenant_id_ != 0;
    }

    void reset()
    {
      tenant_id_ = 0;
    }

    TO_STRING_KV(K_(tenant_id));

    uint64_t tenant_id_;
  };
  ObOptSystemStat()
    : last_analyzed_(0),
      cpu_speed_(0),
      disk_seq_read_speed_(0),
      disk_rnd_read_speed_(0),
      network_speed_(0) {}
  ObOptSystemStat(int64_t last_analyzed,
                 int64_t cpu_speed,
                 int64_t disk_seq_read_speed,
                 int64_t disk_rnd_read_speed,
                 int64_t network_speed) :
      last_analyzed_(last_analyzed),
      cpu_speed_(cpu_speed),
      disk_seq_read_speed_(disk_seq_read_speed),
      disk_rnd_read_speed_(disk_rnd_read_speed),
      network_speed_(network_speed) {}

  virtual ~ObOptSystemStat() {}

  int64_t get_last_analyzed() const { return last_analyzed_; }
  void set_last_analyzed(int64_t last_analyzed) { last_analyzed_ = last_analyzed; }

  int64_t get_cpu_speed() const { return cpu_speed_; }
  void set_cpu_speed(int64_t cpu_speed) { cpu_speed_ = cpu_speed; }

  int64_t get_disk_seq_read_speed() const { return disk_seq_read_speed_; }
  void set_disk_seq_read_speed(int64_t disk_seq_read_speed) { disk_seq_read_speed_ = disk_seq_read_speed; }

  int64_t get_disk_rnd_read_speed() const { return disk_rnd_read_speed_; }
  void set_disk_rnd_read_speed(int64_t disk_rnd_read_speed) { disk_rnd_read_speed_ = disk_rnd_read_speed; }

  int64_t get_network_speed() const { return network_speed_; }
  void set_network_speed(int64_t network_speed) { network_speed_ = network_speed; }

  virtual int64_t size() const
  {
    return sizeof(*this);
  }

  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
  {
    int ret = OB_SUCCESS;
    ObOptSystemStat *tstat = nullptr;
    if (nullptr == buf || buf_len < size()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(buf_len), K(size()));
    } else {
      tstat = new (buf) ObOptSystemStat();
      *tstat = *this;
      value = tstat;
    }
    return ret;
  }

  virtual int deep_copy(char *buf, const int64_t buf_len, ObOptSystemStat *&stat) const
  {
    int ret = OB_SUCCESS;
    ObOptSystemStat *tstat = nullptr;
    if (nullptr == buf || buf_len < size()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(buf_len), K(size()));
    } else {
      tstat = new (buf) ObOptSystemStat();
      *tstat = *this;
      stat = tstat;
    }
    return ret;
  }

  bool is_valid() const
  {
    return cpu_speed_ > 0 &&
            disk_seq_read_speed_ > 0 &&
            disk_rnd_read_speed_ > 0 &&
            network_speed_ > 0;
  }

  void reset() {
    last_analyzed_ = 0;
    cpu_speed_ = 0;
    disk_seq_read_speed_ = 0;
    disk_rnd_read_speed_ = 0;
    network_speed_ = 0;
  }

  TO_STRING_KV(K(last_analyzed_),
               K(cpu_speed_),
               K(disk_seq_read_speed_),
               K(disk_rnd_read_speed_),
               K(network_speed_));

private:
  int64_t last_analyzed_;
  int64_t cpu_speed_;
  int64_t disk_seq_read_speed_;
  int64_t disk_rnd_read_speed_;
  int64_t network_speed_;
};

class OptSystemIoBenchmark {
public:
  OptSystemIoBenchmark()
  :disk_seq_read_speed_(0),
  disk_rnd_read_speed_(0),
  init_(false)
  {}

  ~OptSystemIoBenchmark()
  {}

  inline bool is_init() const { return init_; }
  inline int64_t get_disk_seq_read_speed() const { return disk_seq_read_speed_; }
  inline int64_t get_disk_rnd_read_speed() const { return disk_rnd_read_speed_; }
  void reset();
  static OptSystemIoBenchmark& get_instance();

  int run_benchmark(ObIAllocator &allocator);

private:
  int64_t disk_seq_read_speed_;
  int64_t disk_rnd_read_speed_;
  bool init_;
  DISALLOW_COPY_AND_ASSIGN(OptSystemIoBenchmark);
};

}
}

#endif /* _OB_OPT_SYSTEM_STAT_H_ */
