/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ATOMIC_REFERENCE_H_
#define OB_ATOMIC_REFERENCE_H_
#include <stdint.h>

namespace oceanbase
{
namespace common
{

union AtomicInt64
{
  uint64_t atomic;
  struct
  {
    uint32_t buffer;
    uint32_t pairs;
  };
  struct
  {
    uint32_t ref;
    uint32_t seq;
  };
};


class ObAtomicReference final
{
public:
  ObAtomicReference();
  ~ObAtomicReference();
  void reset();
  int inc_ref_cnt();
  int check_seq_num_and_inc_ref_cnt(const uint32_t seq_num);
  int check_and_inc_ref_cnt();
  int dec_ref_cnt_and_inc_seq_num(uint32_t &ref_cnt);
  bool try_inc_seq_num();
  bool try_check_and_inc_seq_num(const uint32_t seq_num);
  inline uint32_t get_seq_num() const { return atomic_num_.seq; }
  inline uint32_t get_ref_cnt() const { return atomic_num_.ref; }
private:
  AtomicInt64 atomic_num_;
};

}
}

#endif /* OB_ATOMIC_REFERENCE_H_ */
