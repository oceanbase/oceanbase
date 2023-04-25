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

#include "ob_fd_simulator.h"

namespace oceanbase
{
namespace common
{

union ob_sim_fd_id
{
  struct fd_sim_id {
    int64_t first_pos_ : 28;
    int64_t second_pos_ : 28;
    int64_t op_type_ : 4;
    int64_t device_type_ : 4;
    fd_sim_id() : first_pos_(0), second_pos_(0),
                  op_type_(0), device_type_(0)
                  {}
  };
  int64_t fd_id_;
  fd_sim_id sim_id_;
  ob_sim_fd_id() : fd_id_(0) {}
};

int validate_fd(ObIOFd fd, bool expect);

ObFdSimulator::ObFdSimulator() : array_size_(DEFAULT_ARRAY_SIZE), second_array_num_(0),
                                 first_array_(NULL), allocator_(SET_USE_500("FdSimulator")),
              lock_(ObLatchIds::DEFAULT_SPIN_LOCK),used_fd_cnt_(0),total_fd_cnt_(0),is_init_(false)
{
}

/*
array_size: first_array and second_array share the same array size
extend_slot_id : every slot in the first array point a second array,
                 extend_slot_id is the
*/
int ObFdSimulator::init_manager_array(FdSlot* second_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(second_array)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "second array is null!");
  } else {
    /*when first slot id is 0, it means it is the first init, should init the first array*/
    if (0 == second_array_num_) {
      for (int i = 0; i < array_size_; i++) {
        first_array_[i].second_array_p = NULL;
        first_array_[i].second_array_free_hd = 0;
      }
    }

    /*init the second array*/
    for (int j = 0; j < array_size_; j++) {
      if (j == (array_size_ - 1)) {
        second_array[j].pointer.index_id = ObFdSimulator::INVALID_SLOT_ID;
      } else {
        second_array[j].pointer.index_id = j + 1;
      }
      second_array[j].slot_version = 0;
    }

    first_array_[second_array_num_].second_array_p = second_array;
    second_array_num_++;
    total_fd_cnt_ += array_size_;
  }

  return OB_SUCCESS;
}

/*init the fd management unit*/
int ObFdSimulator::init()
{
  int ret = OB_SUCCESS;
  FdSlot* second_array = NULL;
  common::ObSpinLockGuard guard(lock_);

  if (is_init_) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "fd simulator has inited!");
  } else {
    //alloc mem
    first_array_ = static_cast<FirstArray*>(allocator_.alloc(sizeof(FirstArray)*array_size_));
    second_array = static_cast<FdSlot*>(allocator_.alloc(sizeof(FdSlot)*array_size_));
    if (OB_ISNULL(first_array_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc mem for init fd management(first stage)!");
    } else if (OB_ISNULL(second_array)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc mem for init fd management(second stage)!");
    } else if (OB_FAIL(init_manager_array(second_array))) {
      OB_LOG(WARN, "fail to init fd mng!");
    } else {
      is_init_ = true;
    }
  }

  return ret;
}

int ObFdSimulator::extend_second_array()
{
  int ret = OB_SUCCESS;
  if (second_array_num_ == array_size_) {
    OB_LOG(WARN, "can not alloc second arraym, first array is full!", K(second_array_num_));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    FdSlot *second_array_p = static_cast<FdSlot*>(allocator_.alloc(sizeof(FdSlot)*array_size_));
    if (OB_ISNULL(second_array_p)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to extend second array for fd mng!", K(second_array_num_), K(ret));
    } else if (OB_FAIL(init_manager_array(second_array_p))) {
      OB_LOG(WARN, "fail to init extend second array for fd mng!", K(second_array_num_), K(ret));
    }
  }
  return ret;
}

/*this fun only return success and OB_BUF_NOT_ENOUGH*/
int try_get_fd_inner(ObFdSimulator::FirstArray* first_array, int32_t second_array_num, void* ctx, ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  int i = 0;
  int64_t free_id = ObFdSimulator::INVALID_SLOT_ID;
  ObFdSimulator::FdSlot *second_array_p = NULL;
  for (; i < second_array_num; i++) {
    free_id = first_array[i].second_array_free_hd;
    second_array_p = first_array[i].second_array_p;
    if (free_id != ObFdSimulator::INVALID_SLOT_ID) {
      int64_t next_free_id = second_array_p[free_id].pointer.index_id;
      ob_sim_fd_id sim_fd;
      sim_fd.sim_id_.first_pos_ = i;
      sim_fd.sim_id_.second_pos_ = free_id;
      fd.first_id_ = sim_fd.fd_id_;
      fd.second_id_ = second_array_p[free_id].slot_version;

      second_array_p[free_id].pointer.ctx_pointer = ctx;
      first_array[i].second_array_free_hd = next_free_id;
      break;
    }
  }

  if (i >= second_array_num) {
      OB_LOG(WARN, "no enough fd entry, maybe need extend!", K(second_array_num));
      ret = OB_BUF_NOT_ENOUGH ;
  }
  return ret;
}

/*
For object device, the fd(first_id_) is used to locate the ctx,
the fd(second_id_) is used to record the version(validate the fd)
*/
int ObFdSimulator::get_fd(void* ctx, const int device_type, const int flag, ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  fd.reset();
  if (!is_init_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd simulater is not init!", K(ret));
  } else if (OB_ISNULL(ctx)) {
    OB_LOG(WARN, "fail to alloc fd with empty ctx!");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(try_get_fd_inner(first_array_, second_array_num_, ctx, fd))) {
    OB_LOG(WARN, "fail to alloc fd, maybe need extend second array!");
    /*after the first fail, try to extend*/
    if (OB_FAIL(extend_second_array())) {
      OB_LOG(WARN, "fail to extand second fd array, it is full!", K(second_array_num_));
    } else if (OB_FAIL(try_get_fd_inner(first_array_, second_array_num_, ctx, fd))) {
      OB_LOG(WARN, "fail to alloc fd, it is impossible!");
    }
  }

  if (OB_SUCCESS == ret && validate_fd(fd, true)) {
    //format some info into first fd
    OB_LOG(DEBUG, "success get fd!", K(fd.first_id_), K(fd.second_id_), K(ctx));
    set_fd_device_type(fd, device_type);
    set_fd_flag(fd, flag);
    used_fd_cnt_++;
  }

  return ret;
}

/*
********4**********4***********28***********28******
* device_type:device_flag:first_fd_slot:second_fd_slot
*/
void ObFdSimulator::set_fd_device_type(ObIOFd& fd, int device_type)
{
  ob_sim_fd_id fd_sim;
  fd_sim.fd_id_ = fd.first_id_;
  fd_sim.sim_id_.device_type_ = device_type;
  fd.first_id_ = fd_sim.fd_id_;
}

void ObFdSimulator::get_fd_device_type(const ObIOFd& fd, int &device_type)
{
  ob_sim_fd_id fd_sim;
  fd_sim.fd_id_ = fd.first_id_;
  device_type = fd_sim.sim_id_.device_type_;
}

void ObFdSimulator::set_fd_flag(ObIOFd& fd, int flag)
{
  ob_sim_fd_id fd_sim;
  fd_sim.fd_id_ = fd.first_id_;
  fd_sim.sim_id_.op_type_ = flag;
  fd.first_id_ = fd_sim.fd_id_;
}

void ObFdSimulator::get_fd_flag(const ObIOFd& fd, int &flag)
{
  ob_sim_fd_id fd_sim;
  fd_sim.fd_id_ = fd.first_id_;
  flag = fd_sim.sim_id_.op_type_;
}

void ObFdSimulator::get_fd_slot_id(const ObIOFd& fd, int64_t& first_id, int64_t& second_id)
{
  ob_sim_fd_id fd_sim;
  fd_sim.fd_id_ = fd.first_id_;
  first_id = fd_sim.sim_id_.first_pos_;
  second_id = fd_sim.sim_id_.second_pos_;
}

bool ObFdSimulator::validate_fd(const ObIOFd& fd, bool expect)
{
  int64_t first_id = 0;
  int64_t second_id = 0;
  int ret = OB_SUCCESS;
  int valid = false;
  get_fd_slot_id(fd, first_id, second_id);
  if (first_id >= array_size_ || second_id >= array_size_ ||
      first_id < 0 || second_id < 0) {
    if (expect) {
      OB_LOG(WARN, "invaild fd, fd first/second id is wrong!", K(first_id), K(second_id));
    }
  } else {
    FdSlot *second_array = first_array_[first_id].second_array_p;
    if (OB_ISNULL(second_array)) {
      OB_LOG(WARN, "fd maybe wrong, second fd array is null!", K(first_id), K(second_id), K(total_fd_cnt_), K(used_fd_cnt_));
    } else if (second_array[second_id].slot_version != fd.second_id_){
      OB_LOG(WARN, "fd slot_version is invalid, maybe double free!", K(first_id), K(fd.second_id_),
                    K(second_array[second_id].slot_version));
    } else {
      valid = true;
    }
  }
  return valid;
}

int ObFdSimulator::fd_to_ctx(const ObIOFd& fd, void*& ctx)
{
  int ret = OB_SUCCESS;
  int64_t first_id = 0;
  int64_t second_id = 0;
  ctx = NULL;
  /*validate the fd*/
  get_fd_slot_id(fd, first_id, second_id);

  if (!is_init_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd simulater is not init!", K(ret));
  } else if (!validate_fd(fd, true)) {
    OB_LOG(WARN, "fail to get fd ctx, since fd is invalid!", K(first_id), K(second_id));
  } else {
    FdSlot *second_array = first_array_[first_id].second_array_p;
    ctx = second_array[second_id].pointer.ctx_pointer;
    if (OB_ISNULL(ctx)) {
      OB_LOG(WARN, "fail to get fd ctx, it is null!", K(first_id), K(second_id));
      ret = OB_INVALID_ARGUMENT;
    }
  }
  return ret;
}

/*Notice: the ctx mem free outside*/
int ObFdSimulator::release_fd(const ObIOFd& fd)
{
  int ret = OB_SUCCESS;
  int64_t first_id = 0;
  int64_t second_id = 0;

  common::ObSpinLockGuard guard(lock_);
  get_fd_slot_id(fd, first_id, second_id);
  if (!is_init_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd simulater is not init!", K(ret));
  } else if (!validate_fd(fd, true)) {
    OB_LOG(WARN, "fail to get fd ctx, since fd is invalid!", K(first_id), K(second_id));
    ret = OB_NOT_INIT;
  } else {
    FdSlot *second_array = first_array_[first_id].second_array_p;
    second_array[second_id].slot_version++;
    second_array[second_id].pointer.index_id = first_array_[first_id].second_array_free_hd;
    first_array_[first_id].second_array_free_hd = second_id;
    used_fd_cnt_--;
  }
  return ret;
}


}
}
