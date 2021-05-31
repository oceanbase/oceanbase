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

#include "lib/allocator/ob_buddy_allocator.h"
#include "lib/utility/ob_print_utils.h"
namespace oceanbase {
namespace common {
ObBuddyAllocator::ObBuddyAllocator(ObIAllocator& alloc)
    : inited_(false), allocator_(alloc), min_size_(128), max_order_(13), base_addr_in_(NULL)
{
  memset(free_area_, 0, sizeof(free_area_));
  memset(index_array_, 0, sizeof(index_array_));
  /*
   int32_t block_order = 7;//128B
   if (min_block_size <= 0 && min_block_size > (1 << MAX_MEMORY_ORDER))
   {
     //block_size is too big
     min_block_size = -1;//7
     max_order_ = -1;
     min_size_ = -1;
   }
   else if (is_pow_of_2(min_block_size))
   {
     block_order = log2(min_block_size);
     max_order_ = static_cast<int32_t>(MAX_MEMORY_ORDER - block_order);
     min_size_ = pow(block_order);
    }
    else
    {//default block = 128B
      max_order_ = -1;
      min_size_ = -1;
    }*/
}

int32_t ObBuddyAllocator::init(const int64_t min_block_size)
{
  int32_t err = OB_SUCCESS;
  int32_t i = 0;
  int32_t block_order = 7;  // 128B
  if (true != inited_) {
    // inited_ = true;
    // check user input:parameter
    if (min_block_size <= 0 || min_block_size > (1 << MAX_MEMORY_ORDER)) {
      max_order_ = -1;
      min_size_ = -1;
    } else if (is_pow_of_2(min_block_size)) {
      block_order = log2(min_block_size);
      max_order_ = static_cast<int32_t>(MAX_MEMORY_ORDER - block_order);
      min_size_ = pow(block_order);
    } else {
      max_order_ = -1;
      min_size_ = -1;
    }
    if (max_order_ == -1) {
      err = OB_INVALID_ARGUMENT;
      _OB_LOG(WARN, "examine:min_block_size = %ld (1<<i,i<=20). err = %d", min_block_size, err);
    }
    if (max_order_ >= MAX_MEMORY_ORDER) {
      err = OB_INVALID_ARGUMENT;
      _OB_LOG(ERROR, "max_order_ = %d is bigger than threshold value. err = %d", max_order_, err);
    }
    // alloc memory for free_area_[][]
    if (OB_SUCCESS == err) {
      for (i = 0; i <= max_order_; i++) {
        free_area_[i] = static_cast<char*>(allocator_.alloc(pow(max_order_ - i)));
        if (free_area_[i] == NULL) {
          err = OB_ALLOCATE_MEMORY_FAILED;  // alloc error
          break;
        } else if (free_area_[i] != NULL) {
          memset(free_area_[i], 0, sizeof(unsigned char) * pow(max_order_ - i));
        }
      }
      if (OB_SUCCESS != err) {
        _OB_LOG(WARN, "alloc memory for free_area_[][] failed. err = %d", err);
        inited_ = false;
      }
    }
    // init index_array_[]
    if (OB_SUCCESS == err) {  // index_array = [-1, -1, ... , -1, 0]
      index_array_[max_order_] = 0;
      for (i = 0; i < max_order_; i++) {
        index_array_[i] = -1;
      }
    }
    if (OB_SUCCESS == err) {
      base_addr_in_ = allocator_.alloc(1024 * 1024);
      if (base_addr_in_ == NULL) {
        err = OB_ALLOCATE_MEMORY_FAILED;
        _OB_LOG(ERROR, "init memory base_addr_in failed. err = %d", err);
        inited_ = false;
      } else {
        free_area_[max_order_][0] = 1;
      }
    }
    if (OB_SUCCESS == err) {
      inited_ = true;
    }
  } else {
    err = OB_INIT_TWICE;
    _OB_LOG(WARN, "init buddy_allocator twice: err = %d", err);
  }
  return err;
}

ObBuddyAllocator::~ObBuddyAllocator()
{
  int32_t i = 0;
  int32_t err = OB_SUCCESS;
  if (inited_) {
    if (max_order_ >= MAX_MEMORY_ORDER) {
      err = OB_CONFLICT_VALUE;
      _OB_LOG(ERROR, "max_order_(%d) is bigger than threshold value(%d), err = %d", max_order_, MAX_MEMORY_ORDER, err);
    } else {
      for (i = 0; i <= max_order_; i++) {
        allocator_.free(free_area_[i]);
      }
      allocator_.free(base_addr_in_);
    }
  }
}

void* ObBuddyAllocator::alloc(const int64_t sz)
{
  OB_ASSERT(max_order_ < MAX_MEMORY_ORDER && max_order_ > 0 && min_size_ > 0 && is_pow_of_2(min_size_));
  int64_t size = 0;  // 1B for order
  int32_t err = OB_SUCCESS;
  int32_t order = -1;
  void* addr_in = NULL;
  void* addr_user = NULL;
  int64_t real_size = 0;
  // check user input:alloc(sz)
  if (sz <= 0) {
    size = 0;
    err = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "alloc(%ld),negative number,err = %d", sz, err);
  } else if (sz <= (min_size_ - 1)) {
    size = min_size_;
  } else if (sz > min_size_ * (1L << max_order_)) {
    size = 0;
    err = OB_SIZE_OVERFLOW;
    _OB_LOG(WARN, "alloc: size(=%ld) overflow: threshold = %ld, err = %d", sz, min_size_ * (1L << max_order_), err);
  } else {
    size = sz + 1;
  }  // end of check user input
  if (err == OB_SUCCESS && size > 0) {
    real_size = get_next_pow(size);
    order = log2(real_size / min_size_);
    if (order > max_order_) {
      err = OB_SIZE_OVERFLOW;
      _OB_LOG(WARN, "alloc(%ld) overflow: order(%d)>max_order_(%d). err = %d", size, order, max_order_, err);
    } else if (order >= 0) {
      err = pre_alloc(order, addr_in, addr_user);
    } else {
      ob_abort();
    }
  }
  if (OB_SUCCESS != err || size <= 0) {
    addr_user = NULL;
    _OB_LOG(WARN, "alloC %ldB failed. err = %d", sz, err);
  }
  return addr_user;
}

int32_t ObBuddyAllocator::pre_alloc(const int32_t size_order, void*& address_inner, void*& address_user)
{
  OB_ASSERT(max_order_ < MAX_AREA_SIZE);
  OB_ASSERT(size_order >= 0);
  OB_ASSERT(size_order <= max_order_);
  int32_t order = size_order;
  int64_t ancesstor_index = -1;
  int64_t target_index = -1;
  int32_t ancesstor_order = order;
  int32_t target_order = order;
  char* tmp = NULL;
  int32_t i = order;
  int32_t err = OB_SUCCESS;
  bool no_memory = true;
  if (max_order_ >= MAX_MEMORY_ORDER) {
    err = OB_CONFLICT_VALUE;
    _OB_LOG(ERROR,
        "max_order_(%d) is bigger than threshold value:MAX_MEMORY_ORDER(%d), err = %d",
        max_order_,
        MAX_MEMORY_ORDER,
        err);
  }
  // find ancesstor, if can't, means no memory.
  if (OB_SUCCESS == err) {
    for (i = order; i <= max_order_; i++) {
      if (-1 != index_array_[i] && free_area_[i][index_array_[i]] == 1) {
        no_memory = false;
        ancesstor_index = index_array_[i];
        ancesstor_order = i;
        break;
      }
    }
    if (no_memory) {
      err = OB_BUF_NOT_ENOUGH;  //?
      _OB_LOG(WARN, "no memory, err = %d", err);
    }
  }
  // call linear_alloc or single_alloc
  if (OB_SUCCESS == err) {
    if (ancesstor_order == size_order) {  // ancestor is target, single alloc
      target_index = ancesstor_index;
      target_order = ancesstor_order;
      address_inner = single_alloc(target_index, target_order);
    } else {
      target_order = order;
      target_index = ancesstor_index * pow(ancesstor_order - order);
      address_inner = linear_alloc(target_index, target_order, ancesstor_index, ancesstor_order);
    }
  }
  // write order to memory block head
  if (OB_SUCCESS == err) {
    tmp = static_cast<char*>(address_inner);
    tmp++;
    address_user = tmp;
  } else {
    address_user = NULL;
  }
  return err;
}
void* ObBuddyAllocator::linear_alloc(
    int64_t target_index, int32_t target_order, int64_t ancesstor_index, int32_t ancesstor_order)
{
  // linear_alloc: construct a tree path:
  // from A(ancesstor_index, ancesstor_order) to B(target_index, target_order)
  //      1             0
  //    0   0  ==>    0    1
  //   0 0 0 0      0  1  0  0    : free_area_[][]
  //  0 0 00000    0 1 00 00 00
  // along most left path:
  // node=0,means alloc area to user; while node=1 means node area is free to alloc
  OB_ASSERT(ancesstor_order > target_order);
  int32_t i = 0;
  int32_t step = ancesstor_order - target_order;
  int64_t diff_quotient = 1;
  int64_t tmp_index = ancesstor_index;
  int32_t tmp_order = 0;
  void* addr_in = NULL;
  char* tmp_write_order = NULL;
  if (step > MAX_MEMORY_ORDER) {
    _OB_LOG(ERROR, "linear_alloc():step(%d) is over threshold value(%d), err = ", step, MAX_MEMORY_ORDER);
    addr_in = NULL;
  } else if (step < 0) {
    _OB_LOG(ERROR, "ancesstor_order or target_order is wronge");
    addr_in = NULL;
  } else {  // construct the path and fix index_array
    for (i = 0; i < step; i++) {
      tmp_order = target_order + i;
      diff_quotient = pow(i);
      tmp_index = target_index / diff_quotient;
      free_area_[tmp_order][tmp_index] = 0;
      free_area_[tmp_order][tmp_index + 1] = 1;
      // fix index_array[tmp_order], faster than fix_index_array(tmp_order);
      // if failed call:
      // fix_index_array(tmp_order);
      if (tmp_index < index_array_[tmp_order] - 1) {
        index_array_[tmp_order] = tmp_index;
      } else if (-1 == index_array_[tmp_order]) {
        index_array_[tmp_order] = tmp_index + 1;
      }
    }
    // construct the path endpoint
    free_area_[target_order][target_index] = 0;
    free_area_[target_order][target_index + 1] = 1;
    free_area_[ancesstor_order][ancesstor_index] = 0;
    // fix index_array_[target_order]
    if (index_array_[target_order] > target_index + 1) {
      index_array_[target_order] = target_index + 1;
    }
    if (target_index < index_array_[target_order] - 1) {
      index_array_[target_order] = target_index + 1;
    }
    if (ancesstor_index == index_array_[ancesstor_order]) {
      fix_index_array(ancesstor_order);
    }
    // fix index_array end
    // add order to head of memory block
    addr_in = compute_addr_in(target_index, target_order);
    tmp_write_order = static_cast<char*>(addr_in);
    *tmp_write_order = static_cast<unsigned char>(target_order);
  }
  return addr_in;
}
void* ObBuddyAllocator::single_alloc(int64_t target_index, int32_t target_order)
{
  OB_ASSERT(target_index >= 0L && target_index < pow(max_order_ - target_order));
  OB_ASSERT(target_order >= 0 && target_order <= max_order_);
  void* addr_in = NULL;
  char* tmp = NULL;
  addr_in = compute_addr_in(target_index, target_order);
  tmp = (char*)addr_in;
  *tmp = static_cast<unsigned char>(target_order);
  free_area_[target_order][target_index] = 0;
  if (index_array_[target_order] == target_index) {
    fix_index_array(target_order);
  }
  return addr_in;
}
// fix_index_array--cost: 2^order
// call this function as less as possible
int32_t ObBuddyAllocator::fix_index_array(const int32_t order)
{
  OB_ASSERT(order <= max_order_ && order >= 0);
  int32_t target_order = order;
  int64_t i = 0;
  int64_t max = 0;
  int64_t next_index = -1;
  int32_t err = OB_SUCCESS;
  if (order <= max_order_) {
    max = pow(max_order_ - target_order);
    for (i = 0; i < max; i++) {
      if (free_area_[target_order][i] != 0) {
        next_index = i;
        break;
      }
    }
    /*if (next_index == (max - 1)) {
      next_index = -1;
    }*/
    index_array_[target_order] = next_index;
    err = OB_SUCCESS;
  } else {
    err = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "order(%d) is over threshould. err = %d", order, err);
  }
  return err;
}
// functions about order, deal with binary number
int64_t ObBuddyAllocator::pow(const int32_t order) const
{
  OB_ASSERT(order >= 0);
  int64_t tmp = 1L;
  int32_t count = order;
  if (count < MAX_AREA_SIZE) {
    tmp <<= count;
  } else {
    tmp = -3;
  }
  return tmp;
}
bool ObBuddyAllocator::is_pow_of_2(const int64_t number) const
{
  OB_ASSERT(number > 0);
  int64_t tmp = number;
  return !(tmp & (tmp - 1));
}
int64_t ObBuddyAllocator::get_next_pow(const int64_t number) const
{
  OB_ASSERT(number > 0);
  int64_t tmp = number;
  int64_t real_size = -1;
  if (is_pow_of_2(number)) {
    real_size = number;
  } else {
    tmp |= tmp >> 1;
    tmp |= tmp >> 2;
    tmp |= tmp >> 4;
    tmp |= tmp >> 8;
    tmp |= tmp >> 16;
    tmp = tmp + 1;
    real_size = tmp;
  }
  return real_size;
}
int32_t ObBuddyAllocator::log2(const int64_t number) const
{
  int32_t count = 0;
  int32_t ret = 0;
  int64_t tmp = number;
  for (count = 0; count < MAX_AREA_SIZE; count++) {
    if (tmp > 0) {
      tmp >>= 1;
    } else {
      break;
    }
  }
  if (count >= MAX_AREA_SIZE) {
    ret = -3;
  } else {
    count--;
    ret = count;
  }
  return ret;
}
// free part
void ObBuddyAllocator::free(void* ptr)
{
  // declaration
  int64_t buddy_index = 0;
  char* p1 = static_cast<char*>(ptr);
  char* p2 = static_cast<char*>(base_addr_in_);
  int64_t len = 0;            // offset ptr from base_addr_in;
  int32_t err = OB_SUCCESS;   // result
  int32_t current_order = 0;  // here is a cast: unsigned char to int32_t
  int64_t block_size = 0;     // pow(current_order);
  int64_t index = -1;         // len / block_size;
  // check user input
  if (ptr == NULL) {
    err = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "free null pointer. err = %d", err);
  }
  if (OB_SUCCESS == err) {
    len = p1 - p2;
    if (len > (1 << MAX_MEMORY_ORDER)) {
      err = OB_INVALID_ARGUMENT;
      _OB_LOG(WARN, "pointer outof memory area. err = %d", err);
    }
  }
  // send free request to combine_free()&single_free();
  if (OB_SUCCESS == err) {
    // initialize virable;
    current_order = *(p1 - 1);
    block_size = pow(current_order);
    index = len / (block_size * min_size_);
    // free part
    if (find_buddy(index, buddy_index, current_order)) {
      err = combine_free(index, buddy_index, current_order);
    } else {
      err = single_free(ptr, index, current_order);
    }
  }
  return;
}
bool ObBuddyAllocator::find_buddy(int64_t index, int64_t& buddy_index, int32_t order) const
{
  OB_ASSERT(index >= 0 && index < (1L << (max_order_ - order)));  //
  OB_ASSERT(order <= max_order_ && order >= 0);
  bool ret = true;
  if (index == 0) {
    buddy_index = 1;
  } else {
    buddy_index = (index % 2) ? (index - 1) : (index + 1);
  }
  if (0 != free_area_[order][buddy_index]) {
    ret = true;
  } else {
    ret = false;
  }
  return ret;
}
void* ObBuddyAllocator::compute_addr_in(int64_t index, int32_t order) const
{
  OB_ASSERT(index >= 0 && index < pow(max_order_ - order) && order >= 0 && order <= max_order_);
  void* ptr = NULL;
  char* tmp = static_cast<char*>(base_addr_in_);
  tmp += index * min_size_ * pow(order);
  ptr = tmp;
  return ptr;
}
int32_t ObBuddyAllocator::combine_free(int64_t index, int64_t buddy_index, int32_t current_order)
{
  OB_ASSERT(current_order >= 0 && current_order <= max_order_);
  int64_t index_father = index >> 1;
  int32_t order_father = current_order + 1;
  int64_t buddy_father = buddy_index >> 1;
  int32_t err = OB_SUCCESS;
  int32_t order = current_order;
  char* tmp = NULL;
  void* ptr = NULL;
  if (index_father != buddy_father) {
    err = OB_CONFLICT_VALUE;
    _OB_LOG(ERROR,
        "find buddy error!(A = (%ld,%d), A's buddy(%ld,%d)). err = %d",
        index,
        current_order,
        buddy_index,
        current_order,
        err);
  } else if (OB_SUCCESS == err) {
    ptr = compute_addr_in(index, current_order);
    order = current_order;
    free_area_[order][index] = 0;
    free_area_[order][buddy_index] = 0;
    free_area_[order + 1][index_father] = 1;
    tmp = static_cast<char*>(ptr);
    *tmp = 0;  // static_cast<unsigned char>(order + 1);
    // if his buddy is free too, combine them recursively
    if (find_buddy(index_father, buddy_father, order + 1) && order < (max_order_ - 1)) {
      err = combine_free(index_father, buddy_father, order + 1);
    }

    if (OB_SUCCESS == err) {
      err = fix_index_array(order);
    }

    if (OB_SUCCESS == err) {
      err = fix_index_array(order_father);
    }
  }
  if (err != OB_SUCCESS) {
    _OB_LOG(WARN, "combin_free() error. err = %d", err);
  }
  return err;
}
int32_t ObBuddyAllocator::single_free(void*& ptr, int64_t index, int32_t order)
{
  OB_ASSERT(order >= 0 && order < max_order_);
  int32_t err = OB_SUCCESS;
  char* cptr_writer = static_cast<char*>(ptr);
  free_area_[order][index] = 1;
  *cptr_writer = 0;  // order = 0;
  ptr = NULL;
  cptr_writer = NULL;
  if (index == index_array_[order]) {
    err = fix_index_array(order);
    if (OB_SUCCESS != err) {
      _OB_LOG(WARN, "fix index_array while single_free failed");
    }
  }
  return err;
}
int ObBuddyAllocator::print_info(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  for (int32_t i = max_order_; OB_SUCC(ret) && i >= 0; --i) {
    int64_t block_count = pow(max_order_ - i);
    for (int64_t j = 0; OB_SUCC(ret) && j < block_count; ++j) {
      if (OB_SUCCESS !=
          (ret = databuff_printf(buf, buf_len, pos, "val[%d][%ld]=%d, ", i, j, free_area_[i][j] == 1 ? 1 : 0))) {
        LIB_LOG(WARN, "fail to print", K(ret));
      }
    }
    if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, "\n"))) {
      LIB_LOG(WARN, "fail to print", K(ret));
    }
  }
  for (int64_t i = max_order_; OB_SUCC(ret) && i >= 0; --i) {
    if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, "index_array_[%ld]=%ld, ", i, index_array_[i]))) {
      LIB_LOG(WARN, "fail to print", K(ret));
    }
  }
  return ret;
}
}  // namespace common
}  // namespace oceanbase
