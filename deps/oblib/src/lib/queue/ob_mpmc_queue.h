/*
 * Copyright 2015 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OCEANBASE_MPMC_QUEUE_
#define OCEANBASE_MPMC_QUEUE_

#include "lib/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/thread_local/ob_tsi_utils.h"

namespace oceanbase {
namespace common {
// A ObObTurnSequencer allows threads to order their execution according to
// a monotonically increasing (with wraparound) "turn" value.  The two
// operations provided are to wait for turn T, and to move to the next
// turn.  Every thread that is waiting for T must have arrived before
// that turn is marked completed (for MPMCQueue only one thread waits
// for any particular turn, so this is trivially true).
//
// ObTurnSequencer's state_ holds 26 bits of the current turn (shifted
// left by 6), along with a 6 bit saturating value that records the
// maximum waiter minus the current turn.  Wraparound of the turn space
// is expected and handled.  This allows us to atomically adjust the
// number of outstanding waiters when we perform a FUTEX_WAKE operation.
// Compare this strategy to sem_t's separate num_waiters field, which
// isn't decremented until after the waiting thread gets scheduled,
// during which time more enqueues might have occurred and made pointless
// FUTEX_WAKE calls.
//
// ObTurnSequencer uses futex() directly.  It is optimized for the
// case that the highest awaited turn is 32 or less higher than the
// current turn.  We use the FUTEX_WAIT_BITSET variant, which lets
// us embed 32 separate wakeup channels in a single futex.  See
// http://locklessinc.com/articles/futex_cheat_sheet for a description.
//
// We only need to keep exact track of the delta between the current
// turn and the maximum waiter for the 32 turns that follow the current
// one, because waiters at turn t+32 will be awoken at turn t.  At that
// point they can then adjust the delta using the higher base.  Since we
// need to encode waiter deltas of 0 to 32 inclusive, we use 6 bits.
// We actually store waiter deltas up to 63, since that might reduce
// the number of CAS operations a tiny bit.
//
// To avoid some futex() calls entirely, ObTurnSequencer uses an adaptive
// spin cutoff before waiting.  The overheads (and convergence rate)
// of separately tracking the spin cutoff for each ObTurnSequencer would
// be prohibitive, so the actual storage is passed in as a parameter and
// updated atomically.  This also lets the caller use different adaptive
// cutoffs for different operations (read versus write, for example).
// To avoid contention, the spin cutoff is only updated when requested
// by the caller.
struct ObTurnSequencer {
  explicit ObTurnSequencer(const uint32_t first_turn = 0) noexcept : state_(encode(first_turn << TURN_SHIFT_NUM, 0))
  {}

  // Returns true iff a call to wait_for_turn(turn, ...) won't block
  bool is_turn(const uint32_t turn) const
  {
    uint32_t state = ATOMIC_LOAD(&state_);
    return decode_current_sturn(state) == (turn << TURN_SHIFT_NUM);
  }

  // See try_wait_for_turn
  // Requires that `turn` is not a turn in the past.
  void wait_for_turn(const uint32_t turn, uint32_t& spin_cutoff, const bool is_update_spin_cutoff)
  {
    try_wait_for_turn(turn, spin_cutoff, is_update_spin_cutoff);
  }

  // Internally we always work with shifted turn values, which makes the
  // truncation and wraparound work correctly.  This leaves us bits at
  // the bottom to store the number of waiters.  We call shifted turns
  // "sturns" inside this class.

  // Blocks the current thread until turn has arrived.  If
  // is_update_spin_cutoff is true then this will spin for up to MAX_SPINS tries
  // before blocking and will adjust spin_cutoff based on the results,
  // otherwise it will spin for at most spin_cutoff spins.
  // Returns true if the wait succeeded, false if the turn is in the past
  // or the abs_time_us time value is not nullptr and is reached before the turn
  // arrives
  bool try_wait_for_turn(
      const uint32_t turn, uint32_t& spin_cutoff, const bool is_update_spin_cutoff, const int64_t abs_time_us = 0)
  {
    bool ret = true;
    uint32_t prev_thresh = ATOMIC_LOAD(&spin_cutoff);
    const uint32_t effective_spin_cutoff = is_update_spin_cutoff || 0 == prev_thresh ? MAX_SPINS : prev_thresh;

    uint32_t tries = 0;
    const uint32_t sturn = turn << TURN_SHIFT_NUM;
    for (;; ++tries) {
      uint32_t state = ATOMIC_LOAD(&state_);
      uint32_t current_sturn = decode_current_sturn(state);
      if (current_sturn == sturn) {
        break;
      }

      // wrap-safe version of (current_sturn >= sturn)
      if (sturn - current_sturn >= std::numeric_limits<uint32_t>::max() / 2) {
        // turn is in the past
        ret = false;
        break;
      }

      // the first effect_spin_cutoff tries are spins, after that we will
      // record ourself as a waiter and block with futex_wait
      if (tries < effective_spin_cutoff) {
        PAUSE();
        continue;
      }

      uint32_t current_max_waiter_delta = decode_max_waiters_delta(state);
      uint32_t our_waiter_delta = (sturn - current_sturn) >> TURN_SHIFT_NUM;
      uint32_t new_state = 0;
      if (our_waiter_delta <= current_max_waiter_delta) {
        // state already records us as waiters, probably because this
        // isn't our first time around this loop
        new_state = state;
      } else {
        new_state = encode(current_sturn, our_waiter_delta);
        if (state != new_state && !ATOMIC_BCAS(&state_, state, new_state)) {
          continue;
        }
      }

      if (abs_time_us > 0) {
        struct timespec ts;
        make_timespec(&ts, abs_time_us);
        int futex_result = futex_wait_until(reinterpret_cast<int32_t*>(&state_), new_state, &ts, futex_channel(turn));
        if (ETIMEDOUT == futex_result) {
          ret = false;
          break;
        }
      } else {
        futex_wait_until(reinterpret_cast<int32_t*>(&state_), new_state, NULL, futex_channel(turn));
      }
    }

    if (ret && (is_update_spin_cutoff || 0 == prev_thresh)) {
      // if we hit MAX_SPINS then spinning was pointless, so the right
      // spin_cutoff is MIN_SPINS
      uint32_t target = 0;
      if (tries >= MAX_SPINS) {
        target = MIN_SPINS;
      } else {
        // to account for variations, we allow ourself to spin 2*N when
        // we think that N is actually required in order to succeed
        target = std::min<uint32_t>(MAX_SPINS, std::max<uint32_t>(MIN_SPINS, tries * 2));
      }

      if (0 == prev_thresh) {
        // bootstrap
        ATOMIC_STORE(&spin_cutoff, target);
      } else {
        // try once, keep moving if CAS fails.  Exponential moving average
        // with alpha of 7/8
        // Be careful that the quantity we add to prev_thresh is signed.
        ATOMIC_BCAS(&spin_cutoff, prev_thresh, prev_thresh + (target - prev_thresh) / 8);
      }
    }

    return ret;
  }

  // Unblocks a thread running wait_for_turn(turn + 1)
  void complete_turn(const uint32_t turn)
  {
    while (true) {
      uint32_t state = ATOMIC_LOAD(&state_);
      uint32_t max_waiter_delta = decode_max_waiters_delta(state);
      uint32_t new_state = encode((turn + 1) << TURN_SHIFT_NUM, 0 == max_waiter_delta ? 0 : max_waiter_delta - 1);
      if (ATOMIC_BCAS(&state_, state, new_state)) {
        if (max_waiter_delta != 0) {
          futex_wake(reinterpret_cast<int32_t*>(&state_), std::numeric_limits<int>::max(), futex_channel(turn + 1));
        }
        break;
      }
      // failing compare exchange updates first arg to the value
      // that caused the failure, so no need to reread state_
    }
  }

  // Returns the least-most significant byte of the current uncompleted
  // turn.  The full 32 bit turn cannot be recovered.
  uint8_t uncompleted_turn_lsb() const
  {
    return static_cast<uint8_t>(ATOMIC_LOAD(&state_) >> TURN_SHIFT_NUM);
  }

private:
  // Returns the bitmask to pass futex_wait or futex_wake when communicating
  // about the specified turn
  int futex_channel(uint32_t turn) const
  {
    return 1 << (turn & 31);
  }

  uint32_t decode_current_sturn(uint32_t state) const
  {
    return state & ~WAITERS_MASK;
  }

  uint32_t decode_max_waiters_delta(uint32_t state) const
  {
    return state & WAITERS_MASK;
  }

  uint32_t encode(uint32_t current_sturn, uint32_t max_waiter_delta) const
  {
    return current_sturn | std::min(WAITERS_MASK, max_waiter_delta);
  }

  static struct timespec* make_timespec(struct timespec* ts, int64_t us)
  {
    ts->tv_sec = us / 1000000;
    ts->tv_nsec = 1000 * (us % 1000000);
    return ts;
  }

private:
  // TURN_SHIFT_NUM counts the bits that are stolen to record the delta
  // between the current turn and the maximum waiter. It needs to be big
  // enough to record wait deltas of 0 to 32 inclusive.  Waiters more
  // than 32 in the future will be woken up 32*n turns early (since
  // their BITSET will hit) and will adjust the waiter count again.
  // We go a bit beyond and let the waiter count go up to 63, which
  // is free and might save us a few CAS
  static const uint32_t TURN_SHIFT_NUM = 6;
  static const uint32_t WAITERS_MASK = (1 << TURN_SHIFT_NUM) - 1;

  // The minimum spin count that we will adaptively select
  static const int64_t MIN_SPINS = 1;

  // The maximum spin count that we will adaptively select, and the
  // spin count that will be used when probing to get a new data point
  // for the adaptation
  static const int64_t MAX_SPINS = 1;

  // This holds both the current turn, and the highest waiting turn,
  // stored as (current_turn << 6) | min(63, max(waited_turn - current_turn))
  uint32_t state_;
};

// ObSingleElementQueue implements a blocking queue that holds at most one
// pointer item, and that requires its users to assign incrementing identifiers
// (turns) to each enqueue and dequeue operation.  Note that the turns
// used by ObSingleElementQueue are doubled inside the ObTurnSequencer
struct ObSingleElementQueue {
  ~ObSingleElementQueue()
  {
    if (1 == (sequencer_.uncompleted_turn_lsb() & 1)) {
      // we are pending a dequeue, so we have a constructed item
      // TODO:destroy it?
    }
  }

  void enqueue(const uint32_t turn, uint32_t& spin_cutoff, const bool update_spin_cutoff, void* elem)
  {
    sequencer_.wait_for_turn(turn * 2, spin_cutoff, update_spin_cutoff);
    data_ = elem;
    sequencer_.complete_turn(turn * 2);
  }

  // Waits until either:
  // 1: the dequeue turn preceding the given enqueue turn has arrived
  // 2: the given deadline has arrived
  // Case 1 returns true, case 2 returns false.
  bool try_wait_for_enqueue_turn_until(
      const uint32_t turn, uint32_t& spin_cutoff, const bool update_spin_cutoff, const int64_t abs_time_us)
  {
    return sequencer_.try_wait_for_turn(turn * 2, spin_cutoff, update_spin_cutoff, abs_time_us);
  }

  bool may_enqueue(const uint32_t turn) const
  {
    return sequencer_.is_turn(turn * 2);
  }

  void dequeue(uint32_t turn, uint32_t& spin_cutoff, const bool update_spin_cutoff, void*& elem)
  {
    sequencer_.wait_for_turn(turn * 2 + 1, spin_cutoff, update_spin_cutoff);
    elem = data_;
    data_ = NULL;
    sequencer_.complete_turn(turn * 2 + 1);
  }

  bool may_dequeue(const uint32_t turn) const
  {
    return sequencer_.is_turn(turn * 2 + 1);
  }

private:
  // Even turns are pushes, odd turns are pops
  ObTurnSequencer sequencer_;
  void* data_;
} CACHE_ALIGNED;

template <class Derived>
class ObMPMCQueueBase {
public:
  typedef ObSingleElementQueue Slot;

  ObMPMCQueueBase()
      : capacity_(0),
        slots_(NULL),
        stride_(0),
        dstate_(0),
        dcapacity_(0),
        push_ticket_(0),
        pop_ticket_(0),
        push_spin_cutoff_(0),
        pop_spin_cutoff_(0)
  {}

  // MPMCQueue can only be safely destroyed when there are no
  // pending enqueuers or dequeuers (this is not checked).
  ~ObMPMCQueueBase()
  {
    ob_free(slots_);
  }

  // Returns the number of writes (including threads that are blocked waiting
  // to write) minus the number of reads (including threads that are blocked
  // waiting to read). So effectively, it becomes:
  // elements in queue + pending(calls to write) - pending(calls to read).
  // If nothing is pending, then the method returns the actual number of
  // elements in the queue.
  // The returned value can be negative if there are no writers and the queue
  // is empty, but there is one reader that is blocked waiting to read (in
  // which case, the returned size will be -1).
  int64_t size() const
  {
    // since both pushes and pops increase monotonically, we can get a
    // consistent snapshot either by bracketing a read of pop_ticket_ with
    // two reads of push_ticket_ that return the same value, or the other
    // way around.  We maximize our chances by alternately attempting
    // both bracketings.
    uint64_t pushes = ATOMIC_LOAD(&push_ticket_);  // A
    uint64_t pops = ATOMIC_LOAD(&pop_ticket_);     // B
    int64_t ret_size = 0;
    while (true) {
      uint64_t next_pushes = ATOMIC_LOAD(&push_ticket_);  // C
      if (pushes == next_pushes) {
        // push_ticket_ didn't change from A (or the previous C) to C,
        // so we can linearize at B (or D)
        ret_size = pushes - pops;
        break;
      }
      pushes = next_pushes;
      uint64_t next_pops = ATOMIC_LOAD(&pop_ticket_);  // D
      if (pops == next_pops) {
        // pop_ticket_ didn't chance from B (or the previous D), so we
        // can linearize at C
        ret_size = pushes - pops;
        break;
      }
      pops = next_pops;
    }
    return ret_size;
  }

  // Returns true if there are no items available for dequeue
  bool is_empty() const
  {
    return size() <= 0;
  }

  // Returns true if there is currently no empty space to enqueue
  bool is_full() const
  {
    // careful with signed -> unsigned promotion, since size can be negative
    return size() >= capacity_;
  }

  // Returns is a guess at size() for contexts that don't need a precise
  // value, such as stats. More specifically, it returns the number of writes
  // minus the number of reads, but after reading the number of writes, more
  // writers could have came before the number of reads was sampled,
  // and this method doesn't protect against such case.
  // The returned value can be negative.
  int64_t size_guess() const
  {
    return static_cast<int64_t>(write_count() - read_count());
  }

  // Doesn't change
  int64_t capacity() const
  {
    return capacity_;
  }

  // Doesn't change for non-dynamic
  int64_t allocated_capacity() const
  {
    return capacity_;
  }

  // Returns the total number of calls to blocking_write or successful
  // calls to write, including those blocking_write calls that are
  // currently blocking
  uint64_t write_count() const
  {
    return ATOMIC_LOAD(&push_ticket_);
  }

  // Returns the total number of calls to blocking_read or successful
  // calls to read, including those blocking_read calls that are currently
  // blocking
  uint64_t read_count() const
  {
    return ATOMIC_LOAD(&pop_ticket_);
  }

  int push(void* p, int64_t abs_time_us = 0, bool is_block = false)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(p)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (is_block) {
      static_cast<Derived*>(this)->blocking_write(p);
    } else if (abs_time_us > 0) {
      ret = static_cast<Derived*>(this)->try_write_until(abs_time_us, p) ? OB_SUCCESS : OB_SIZE_OVERFLOW;
    } else {
      ret = static_cast<Derived*>(this)->write(p) ? OB_SUCCESS : OB_SIZE_OVERFLOW;
    }
    return ret;
  }

  int pop(void*& p, int64_t abs_time_us = 0)
  {
    UNUSED(abs_time_us);
    static_cast<Derived*>(this)->blocking_read(p);
    return OB_SUCCESS;
  }

  // Enqueues a T constructed from args, blocking until space is
  // available.  Note that this method signature allows enqueue via
  // move, if args is a T rvalue, via copy, if args is a T lvalue, or
  // via emplacement if args is an initializer list that can be passed
  // to a T constructor.
  void blocking_write(void* elem)
  {
    enqueue_with_ticket_base(ATOMIC_FAA(&push_ticket_, 1), slots_, capacity_, stride_, elem);
  }

  // If an item can be enqueued with no blocking, does so and returns
  // true, otherwise returns false.  This method is similar to
  // write_if_not_full, but if you don't have a specific need for that
  // method you should use this one.
  //
  // One of the common usages of this method is to enqueue via the
  // move constructor, something like q.write(std::move(x)).  If write
  // returns false because the queue is full then x has not actually been
  // consumed, which looks strange.  To understand why it is actually okay
  // to use x afterward, remember that std::move is just a typecast that
  // provides an rvalue reference that enables use of a move constructor
  // or operator.  std::move doesn't actually move anything.  It could
  // more accurately be called std::rvalue_cast or std::move_permission.
  bool write(void* elem)
  {
    bool ret = false;
    uint64_t ticket = 0;
    Slot* slots = NULL;
    int64_t cap = 0;
    int64_t stride = 0;
    if (static_cast<Derived*>(this)->try_obtain_ready_push_ticket(ticket, slots, cap, stride)) {
      // we have pre-validated that the ticket won't block
      enqueue_with_ticket_base(ticket, slots, cap, stride, elem);
      ret = true;
    } else {
      ret = false;
    }
    return ret;
  }

  bool try_write_until(const int64_t abs_time_us, void* elem)
  {
    bool ret = false;
    uint64_t ticket = 0;
    Slot* slots = NULL;
    int64_t cap = 0;
    int64_t stride = 0;
    if (try_obtain_promised_push_ticket_until(ticket, slots, cap, stride, abs_time_us)) {
      // we have pre-validated that the ticket won't block, or rather that
      // it won't block longer than it takes another thread to dequeue an
      // element from the slot it identifies.
      enqueue_with_ticket_base(ticket, slots, cap, stride, elem);
      ret = true;
    } else {
      ret = false;
    }
    return ret;
  }

  // If the queue is not full, enqueues and returns true, otherwise
  // returns false.  Unlike write this method can be blocked by another
  // thread, specifically a read that has linearized (been assigned
  // a ticket) but not yet completed.  If you don't really need this
  // function you should probably use write.
  //
  // MPMCQueue isn't lock-free, so just because a read operation has
  // linearized (and is_full is false) doesn't mean that space has been
  // made available for another write.  In this situation write will
  // return false, but write_if_not_full will wait for the dequeue to finish.
  // This method is required if you are composing queues and managing
  // your own wakeup, because it guarantees that after every successful
  // write a read_if_not_empty will succeed.
  bool write_if_not_full(void* elem)
  {
    bool ret = false;
    uint64_t ticket = 0;
    Slot* slots = NULL;
    int64_t cap = 0;
    int64_t stride = 0;
    if (static_cast<Derived*>(this)->try_obtain_promised_push_ticket(ticket, slots, cap, stride)) {
      // some other thread is already dequeuing the slot into which we
      // are going to enqueue, but we might have to wait for them to finish
      enqueue_with_ticket_base(ticket, slots, cap, stride, elem);
      ret = true;
    } else {
      ret = false;
    }
    return ret;
  }

  // Moves a dequeued element onto elem, blocking until an element
  // is available
  void blocking_read(void*& elem)
  {
    uint64_t ticket = 0;
    static_cast<Derived*>(this)->blocking_read_with_ticket(ticket, elem);
  }

  // Same as blocking_read() but also records the ticket nunmer
  void blocking_read_with_ticket(uint64_t& ticket, void*& elem)
  {
    ticket = ATOMIC_FAA(&pop_ticket_, 1);
    dequeue_with_ticket_base(ticket, slots_, capacity_, stride_, elem);
  }

  // If an item can be dequeued with no blocking, does so and returns
  // true, otherwise returns false.
  bool read(void*& elem)
  {
    uint64_t ticket = 0;
    return read_and_get_ticket(ticket, elem);
  }

  // Same as read() but also records the ticket nunmer
  bool read_and_get_ticket(uint64_t& ticket, void*& elem)
  {
    bool ret = false;
    Slot* slots = NULL;
    int64_t cap = 0;
    int64_t stride = 0;
    if (static_cast<Derived*>(this)->try_obtain_ready_pop_ticket(ticket, slots, cap, stride)) {
      // the ticket has been pre-validated to not block
      dequeue_with_ticket_base(ticket, slots, cap, stride, elem);
      ret = true;
    } else {
      ret = false;
    }
    return ret;
  }

  // If the queue is not empty, dequeues and returns true, otherwise
  // returns false.  If the matching write is still in progress then this
  // method may block waiting for it.  If you don't rely on being able
  // to dequeue (such as by counting completed write) then you should
  // prefer read.
  bool read_if_not_empty(void*& elem)
  {
    bool ret = false;
    uint64_t ticket = 0;
    Slot* slots = NULL;
    int64_t cap = 0;
    int64_t stride = 0;
    if (static_cast<Derived*>(this)->try_obtain_promised_pop_ticket(ticket, slots, cap, stride)) {
      // the matching enqueue already has a ticket, but might not be done
      dequeue_with_ticket_base(ticket, slots, cap, stride, elem);
      ret = true;
    } else {
      ret = false;
    }
    return ret;
  }

protected:
  // We assign tickets in increasing order, but we don't want to
  // access neighboring elements of slots_ because that will lead to
  // false sharing (multiple cores accessing the same cache line even
  // though they aren't accessing the same bytes in that cache line).
  // To avoid this we advance by stride slots per ticket.
  //
  // We need gcd(capacity, stride) to be 1 so that we will use all
  // of the slots.  We ensure this by only considering prime strides,
  // which either have no common divisors with capacity or else have
  // a zero remainder after dividing by capacity.  That is sufficient
  // to guarantee correctness, but we also want to actually spread the
  // accesses away from each other to avoid false sharing (consider a
  // stride of 7 with a capacity of 8).  To that end we try a few taking
  // care to observe that advancing by -1 is as bad as advancing by 1
  // when in comes to false sharing.
  //
  // The simple way to avoid false sharing would be to pad each
  // SingleElementQueue, but since we have capacity_ of them that could
  // waste a lot of space.
  static int64_t compute_stride(int64_t capacity)
  {
    static const int64_t small_primes[] = {2, 3, 5, 7, 11, 13, 17, 19, 23};

    int64_t stride = 0;
    int64_t best_stride = 1;
    int64_t best_sep = 1;
    for (int64_t i = 0; i < sizeof(small_primes) / sizeof(int64_t); ++i) {
      stride = small_primes[i];
      if (0 == (stride % capacity) || 0 == (capacity % stride)) {
        continue;
      }
      int64_t sep = stride % capacity;
      sep = std::min(sep, capacity - sep);
      if (sep > best_sep) {
        best_stride = stride;
        best_sep = sep;
      }
    }
    return best_stride;
  }

  // Returns the index into slots_ that should be used when enqueuing or
  // dequeuing with the specified ticket
  int64_t idx(uint64_t ticket, int64_t cap, int64_t stride)
  {
    return ((ticket * stride) % cap) + SLOT_PADDING;
  }

  // Maps an enqueue or dequeue ticket to the turn should be used at the
  // corresponding SingleElementQueue
  uint32_t turn(uint64_t ticket, int64_t cap)
  {
    return static_cast<uint32_t>(ticket / cap);
  }

  // Tries to obtain a push ticket for which SingleElementQueue::enqueue
  // won't block.  Returns true on immediate success, false on immediate
  // failure.
  bool try_obtain_ready_push_ticket(uint64_t& ticket, Slot*& slots, int64_t& cap, int64_t& stride)
  {
    bool ret = false;
    ticket = ATOMIC_LOAD(&push_ticket_);  // A
    slots = slots_;
    cap = capacity_;
    stride = stride_;
    while (true) {
      if (!slots[idx(ticket, cap, stride)].may_enqueue(turn(ticket, cap))) {
        // if we call enqueue(ticket, ...) on the SingleElementQueue
        // right now it would block, but this might no longer be the next
        // ticket.  We can increase the chance of tryEnqueue success under
        // contention (without blocking) by rechecking the ticket dispenser
        uint64_t prev = ticket;
        ticket = ATOMIC_LOAD(&push_ticket_);  // B
        if (prev == ticket) {
          // may_enqueue was bracketed by two reads (A or prev B or prev
          // failing CAS to B), so we are definitely unable to enqueue
          ret = false;
          break;
        }
      } else {
        // we will bracket the may_enqueue check with a read (A or prev B
        // or prev failing CAS) and the following CAS.  If the CAS fails
        // it will effect a load of push_ticket_
        if (ATOMIC_BCAS(&push_ticket_, ticket, ticket + 1)) {
          ret = true;
          break;
        }
      }
    }
    return ret;
  }

  // Tries until when to obtain a push ticket for which
  // SingleElementQueue::enqueue  won't block.  Returns true on success, false
  // on failure.
  // ticket is filled on success AND failure.
  bool try_obtain_promised_push_ticket_until(
      uint64_t& ticket, Slot*& slots, int64_t& cap, int64_t& stride, const int64_t abs_time_us)
  {
    bool ret = false;
    bool deadline_reached = false;
    while (!deadline_reached) {
      if (static_cast<Derived*>(this)->try_obtain_promised_push_ticket(ticket, slots, cap, stride)) {
        ret = true;
        break;
      }
      // ticket is a blocking ticket until the preceding ticket has been
      // processed: wait until this ticket's turn arrives. We have not reserved
      // this ticket so we will have to re-attempt to get a non-blocking ticket
      // if we wake up before we time-out.
      deadline_reached = !slots[idx(ticket, cap, stride)].try_wait_for_enqueue_turn_until(
          turn(ticket, cap), push_spin_cutoff_, 0 == (ticket % ADAPTATION_FREQ), abs_time_us);
    }
    return ret;
  }

  // Tries to obtain a push ticket which can be satisfied if all
  // in-progress pops complete.  This function does not block, but
  // blocking may be required when using the returned ticket if some
  // other thread's pop is still in progress (ticket has been granted but
  // pop has not yet completed).
  bool try_obtain_promised_push_ticket(uint64_t& ticket, Slot*& slots, int64_t& cap, int64_t& stride)
  {
    bool ret = false;
    uint64_t num_pushes = ATOMIC_LOAD(&push_ticket_);  // A
    slots = slots_;
    cap = capacity_;
    stride = stride_;
    while (true) {
      uint64_t num_pops = ATOMIC_LOAD(&pop_ticket_);  // B
      // n will be negative if pops are pending
      int64_t n = num_pushes - num_pops;
      ticket = num_pushes;
      if (n >= capacity_) {
        // Full, linearize at B.  We don't need to recheck the read we
        // performed at A, because if num_pushes was stale at B then the
        // real num_pushes value is even worse
        ret = false;
        break;
      }
      if (ATOMIC_BCAS(&push_ticket_, num_pushes, num_pushes + 1)) {
        ret = true;
        break;
      }
    }
    return ret;
  }

  // Tries to obtain a pop ticket for which SingleElementQueue::dequeue
  // won't block.  Returns true on immediate success, false on immediate
  // failure.
  bool try_obtain_ready_pop_ticket(uint64_t& ticket, Slot*& slots, int64_t& cap, int64_t& stride)
  {
    bool ret = false;
    ticket = ATOMIC_LOAD(&pop_ticket_);
    slots = slots_;
    cap = capacity_;
    stride = stride_;
    while (true) {
      if (!slots[idx(ticket, cap, stride)].may_dequeue(turn(ticket, cap))) {
        uint64_t prev = ticket;
        ticket = ATOMIC_LOAD(&pop_ticket_);
        if (prev == ticket) {
          ret = false;
          break;
        }
      } else {
        if (ATOMIC_BCAS(&pop_ticket_, ticket, ticket + 1)) {
          ret = true;
          break;
        }
      }
    }
    return ret;
  }

  // Similar to try_obtain_ready_pop_ticket, but returns a pop ticket whose
  // corresponding push ticket has already been handed out, rather than
  // returning one whose corresponding push ticket has already been
  // completed.  This means that there is a possibility that the caller
  // will block when using the ticket, but it allows the user to rely on
  // the fact that if enqueue has succeeded, try_obtain_promised_pop_ticket
  // will return true.  The "try" part of this is that we won't have
  // to block waiting for someone to call enqueue, although we might
  // have to block waiting for them to finish executing code inside the
  // MPMCQueue itself.
  bool try_obtain_promised_pop_ticket(uint64_t& ticket, Slot*& slots, int64_t& cap, int64_t& stride)
  {
    bool ret = false;
    uint64_t num_pops = ATOMIC_LOAD(&pop_ticket_);  // A
    while (true) {
      uint64_t num_pushes = ATOMIC_LOAD(&push_ticket_);  // B
      if (num_pops >= num_pushes) {
        // Empty, or empty with pending pops.  Linearize at B.  We don't
        // need to recheck the read we performed at A, because if num_pops
        // is stale then the fresh value is larger and the >= is still true
        ret = false;
        break;
      }
      if (ATOMIC_BCAS(&pop_ticket_, num_pops, num_pops + 1)) {
        ticket = num_pops;
        slots = slots_;
        cap = capacity_;
        stride = stride_;
        ret = true;
        break;
      }
    }
    return ret;
  }

  // Given a ticket, constructs an enqueued item using args
  void enqueue_with_ticket_base(uint64_t ticket, Slot* slots, int64_t cap, int64_t stride, void* elem)
  {
    slots[idx(ticket, cap, stride)].enqueue(
        turn(ticket, cap), push_spin_cutoff_, 0 == (ticket % ADAPTATION_FREQ), elem);
  }

  // To support tracking ticket numbers in MPMCPipelineStageImpl
  void enqueue_with_ticket(uint64_t ticket, void* elem)
  {
    enqueue_with_ticket_base(ticket, slots_, capacity_, stride_, elem);
  }

  // Given a ticket, dequeues the corresponding element
  void dequeue_with_ticket_base(uint64_t ticket, Slot* slots, int64_t cap, int64_t stride, void*& elem)
  {
    slots[idx(ticket, cap, stride)].dequeue(turn(ticket, cap), pop_spin_cutoff_, 0 == (ticket % ADAPTATION_FREQ), elem);
  }

protected:
  // Once every ADAPTATION_FREQ we will spin longer, to try to estimate
  // the proper spin backoff
  static const int64_t ADAPTATION_FREQ = INT64_MAX;

  // To avoid false sharing in slots_ with neighboring memory
  // allocations, we pad it with this many SingleElementQueue-s at
  // each end
  static const int64_t SLOT_PADDING = (64 - 1) / sizeof(Slot) + 1;

  // The maximum number of items in the queue at once
  int64_t capacity_ CACHE_ALIGNED;

  // Anonymous union for use when Dynamic = false and true, respectively
  union {
    // An array of capacity_ SingleElementQueue-s, each of which holds
    // either 0 or 1 item.  We over-allocate by 2 * SLOT_PADDING and don't
    // touch the slots at either end, to avoid false sharing
    Slot* slots_;
    // Current dynamic slots array of dcapacity_ SingleElementQueue-s
    Slot* dslots_;
  };

  // Anonymous union for use when Dynamic = false and true, respectively
  union {
    // The number of slots_ indices that we advance for each ticket, to
    // avoid false sharing.  Ideally slots_[i] and slots_[i + stride_]
    // aren't on the same cache line
    int64_t stride_;
    // Current stride
    int64_t dstride_;
  };

  // The following two memebers are used by dynamic MPMCQueue.
  // Ideally they should be in MPMCQueue, but we get
  // better cache locality if they are in the same cache line as
  // dslots_ and dstride_.
  //
  // Dynamic state. A packed seqlock and ticket offset
  uint64_t dstate_;
  // Dynamic capacity
  int64_t dcapacity_;

  // Enqueuers get tickets from here
  uint64_t push_ticket_ CACHE_ALIGNED;

  // Dequeuers get tickets from here
  uint64_t pop_ticket_ CACHE_ALIGNED;

  // This is how many times we will spin before using FUTEX_WAIT when
  // the queue is full on enqueue, adaptively computed by occasionally
  // spinning for longer and smoothing with an exponential moving average
  uint32_t push_spin_cutoff_ CACHE_ALIGNED;

  // The adaptive spin cutoff when the queue is empty on dequeue
  uint32_t pop_spin_cutoff_ CACHE_ALIGNED;

  // Alignment doesn't prevent false sharing at the end of the struct,
  // so fill out the last cache line
  char padding_[64 - sizeof(uint32_t)];
};

// MPMCQueue is a high-performance bounded concurrent queue that
// supports multiple producers, multiple consumers, and optional blocking.
// The queue has a fixed capacity, for which all memory will be allocated
// up front.  The bulk of the work of enqueuing and dequeuing can be
// performed in parallel.
//
// MPMCQueue is linearizable.  That means that if a call to write(A)
// returns before a call to write(B) begins, then A will definitely end up
// in the queue before B, and if a call to read(X) returns before a call
// to read(Y) is started, that X will be something from earlier in the
// queue than Y.  This also means that if a read call returns a value, you
// can be sure that all previous elements of the queue have been assigned
// a reader (that reader might not yet have returned, but it exists).
//
// The underlying implementation uses a ticket dispenser for the head and
// the tail, spreading accesses across N single-element queues to produce
// a queue with capacity N.  The ticket dispensers use atomic increment,
// which is more robust to contention than a CAS loop.  Each of the
// single-element queues uses its own CAS to serialize access, with an
// adaptive spin cutoff.  When spinning fails on a single-element queue
// it uses futex()'s _BITSET operations to reduce unnecessary wakeups
// even if multiple waiters are present on an individual queue (such as
// when the MPMCQueue's capacity is smaller than the number of enqueuers
// or dequeuers).
//
// In benchmarks (contained in tao/queues/ConcurrentQueueTests)
// it handles 1 to 1, 1 to N, N to 1, and N to M thread counts better
// than any of the alternatives present in fbcode, for both small (~10)
// and large capacities.  In these benchmarks it is also faster than
// tbb::concurrent_bounded_queue for all configurations.  When there are
// many more threads than cores, MPMCQueue is _much_ faster than the tbb
// queue because it uses futex() to block and unblock waiting threads,
// rather than spinning with sched_yield.
//
// NOEXCEPT INTERACTION: tl;dr; If it compiles you're fine.  Ticket-based
// queues separate the assignment of queue positions from the actual
// construction of the in-queue elements, which means that the T
// constructor used during enqueue must not throw an exception.  This is
// enforced at compile time using type traits, which requires that T be
// adorned with accurate noexcept information.  If your type does not
// use noexcept, you will have to wrap it in something that provides
// the guarantee.  We provide an alternate safe implementation for types
// that don't use noexcept but that are marked folly::IsRelocatable
// and boost::has_nothrow_constructor, which is common for folly types.
// In particular, if you can declare FOLLY_ASSUME_FBVECTOR_COMPATIBLE
// then your type can be put in MPMCQueue.
//
// If you have a pool of N queue consumers that you want to shut down
// after the queue has drained, one way is to enqueue N sentinel values
// to the queue.  If the producer doesn't know how many consumers there
// are you can enqueue one sentinel and then have each consumer requeue
// two sentinels after it receives it (by requeuing 2 the shutdown can
// complete in O(log P) time instead of O(P)).
class ObFixedMPMCQueue : public ObMPMCQueueBase<ObFixedMPMCQueue> {
public:
  typedef ObSingleElementQueue Slot;
  ObFixedMPMCQueue() : ObMPMCQueueBase<ObFixedMPMCQueue>()
  {}
  ~ObFixedMPMCQueue()
  {}

  int init(int64_t queue_capacity, const lib::ObLabel& label = nullptr)
  {
    int ret = OB_SUCCESS;
    Slot* slots = NULL;
    if (queue_capacity <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(ERROR, "invalid argument", K(queue_capacity));
    } else if (NULL != slots_) {
      ret = OB_INIT_TWICE;
    } else if (OB_ISNULL(slots = reinterpret_cast<Slot*>(
                             ob_malloc(sizeof(Slot) * (queue_capacity + 2 * SLOT_PADDING), label)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(ERROR, "allocate slots memory failed.", K(queue_capacity));
    } else {
      memset(slots, 0, sizeof(Slot) * (queue_capacity + 2 * SLOT_PADDING));
      capacity_ = queue_capacity;
      stride_ = compute_stride(queue_capacity);
      slots_ = slots;
    }
    return ret;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObFixedMPMCQueue);
};

// The dynamic version of MPMCQueue allows dynamic expansion of queue
// capacity, such that a queue may start with a smaller capacity than
// specified and expand only if needed. Users may optionally specify
// the initial capacity and the expansion multiplier.
//
// The design uses a seqlock to enforce mutual exclusion among
// expansion attempts. Regular operations read up-to-date queue
// information (slots array, capacity, stride) inside read-only
// seqlock sections, which are unimpeded when no expansion is in
// progress.
//
// An expansion computes a new capacity, allocates a new slots array,
// and updates stride. No information needs to be copied from the
// current slots array to the new one. When this happens, new slots
// will not have sequence numbers that match ticket numbers. The
// expansion needs to compute a ticket offset such that operations
// that use new arrays can adjust the calculations of slot indexes
// and sequence numbers that take into account that the new slots
// start with sequence numbers of zero. The current ticket offset is
// packed with the seqlock in an atomic 64-bit integer. The initial
// offset is zero.
//
// Lagging write and read operations with tickets lower than the
// ticket offset of the current slots array (i.e., the minimum ticket
// number that can be served by the current array) must use earlier
// closed arrays instead of the current one. Information about closed
// slots arrays (array address, capacity, stride, and offset) is
// maintained in a logarithmic-sized structure. Each entry in that
// structure never need to be changed once set. The number of closed
// arrays is half the value of the seqlock (when unlocked).
//
// The acquisition of the seqlock to perform an expansion does not
// prevent the issuing of new push and pop tickets concurrently. The
// expansion must set the new ticket offset to a value that couldn't
// have been issued to an operation that has already gone through a
// seqlock read-only section (and hence obtained information for
// older closed arrays).
//
// Note that the total queue capacity can temporarily exceed the
// specified capacity when there are lagging consumers that haven't
// yet consumed all the elements in closed arrays. Users should not
// rely on the capacity of dynamic queues for synchronization, e.g.,
// they should not expect that a thread will definitely block on a
// call to blocking_write() when the queue size is known to be equal
// to its capacity.
//
// The dynamic version is a partial specialization of MPMCQueue with
// Dynamic == true
class ObDynamicMPMCQueue : public ObMPMCQueueBase<ObDynamicMPMCQueue> {
  friend class ObMPMCQueueBase<ObDynamicMPMCQueue>;
  typedef ObSingleElementQueue Slot;

  struct ObClosedArray {
    ObClosedArray() : offset_(0), slots_(NULL), capacity_(0), stride_(0)
    {}

    uint64_t offset_;
    Slot* slots_;
    int64_t capacity_;
    int64_t stride_;
  };

public:
  explicit ObDynamicMPMCQueue() : ObMPMCQueueBase<ObDynamicMPMCQueue>(), dmult_(0), closed_(NULL), label_(nullptr)
  {}

  ~ObDynamicMPMCQueue()
  {
    if (NULL != closed_) {
      for (int64_t i = get_num_closed(ATOMIC_LOAD(&dstate_)) - 1; i >= 0; --i) {
        ob_free(closed_[i].slots_);
      }
      delete[] closed_;
    }
  }

  int init(int64_t queue_capacity, int64_t min_capacity = DEFAULT_MIN_DYNAMIC_CAPACITY,
      int64_t expansion_multiplier = DEFAULT_EXPANSION_MULTIPLIER, const lib::ObLabel& label = nullptr)
  {
    int ret = OB_SUCCESS;
    Slot* slots = NULL;
    if (queue_capacity <= 0 || min_capacity <= 0 || expansion_multiplier <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(ERROR, "invalid argument", K(queue_capacity), K(min_capacity), K(expansion_multiplier));
    } else if (NULL != slots_) {
      ret = OB_INIT_TWICE;
    } else {
      int64_t cap = std::min<int64_t>(std::max(static_cast<int64_t>(1), min_capacity), queue_capacity);
      if (OB_ISNULL(slots = reinterpret_cast<Slot*>(ob_malloc(sizeof(Slot) * (cap + 2 * SLOT_PADDING), label)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(ERROR, "allocate slots memory failed.", K(cap));
      } else {
        memset(slots, 0, sizeof(Slot) * (cap + 2 * SLOT_PADDING));
        stride_ = compute_stride(cap);
        slots_ = slots;
        dmult_ = std::max<int64_t>(static_cast<int64_t>(2), expansion_multiplier);
        capacity_ = cap;
        label_ = label;
        ATOMIC_STORE(&dstate_, 0);
        ATOMIC_STORE(&dcapacity_, cap);
        int64_t max_closed = 0;
        for (int64_t expanded = cap; expanded < capacity_; expanded *= dmult_) {
          ++max_closed;
        }
        closed_ = (max_closed > 0) ? new ObClosedArray[max_closed] : NULL;
      }
    }
    return ret;
  }

  int64_t allocated_capacity() const
  {
    return ATOMIC_LOAD(&dcapacity_);
  }

  void blocking_write(void* elem)
  {
    uint64_t ticket = ATOMIC_FAA(&push_ticket_, 1);
    Slot* slots = NULL;
    int64_t cap = 0;
    int64_t stride = 0;
    uint64_t state = 0;
    uint64_t offset = 0;
    do {
      if (!try_seqlock_read_section(state, slots, cap, stride)) {
        continue;
      }
      offset = get_offset(state);
      if (ticket < offset) {
        // There was an expansion after this ticket was issued.
        update_from_closed(state, ticket, offset, slots, cap, stride);
        break;
      }
      if (slots[idx((ticket - offset), cap, stride)].may_enqueue(turn(ticket - offset, cap))) {
        // A slot is ready. No need to expand.
        break;
      } else if (ATOMIC_LOAD(&pop_ticket_) + cap > ticket) {
        // May block, but a pop is in progress. No need to expand.
        // Get seqlock read section info again in case an expansion
        // occurred with an equal or higher ticket.
        continue;
      } else {
        // May block. See if we can expand.
        if (try_expand(state, cap)) {
          // This or another thread started an expansion. Get updated info.
          continue;
        } else {
          // Can't expand.
          break;
        }
      }
    } while (true);
    enqueue_with_ticket_base(ticket - offset, slots, cap, stride, elem);
  }

  void blocking_read_with_ticket(uint64_t& ticket, void*& elem)
  {
    ticket = ATOMIC_FAA(&pop_ticket_, 1);
    Slot* slots = NULL;
    int64_t cap = 0;
    int64_t stride = 0;
    uint64_t state = 0;
    uint64_t offset = 0;
    while (!try_seqlock_read_section(state, slots, cap, stride))
      ;
    offset = get_offset(state);
    if (ticket < offset) {
      // There was an expansion after the corresponding push ticket
      // was issued.
      update_from_closed(state, ticket, offset, slots, cap, stride);
    }
    dequeue_with_ticket_base(ticket - offset, slots, cap, stride, elem);
  }

private:
  bool try_obtain_ready_push_ticket(uint64_t& ticket, Slot*& slots, int64_t& cap, int64_t& stride)
  {
    bool ret = false;
    uint64_t state = 0;
    do {
      ticket = ATOMIC_LOAD(&push_ticket_);  // A
      if (!try_seqlock_read_section(state, slots, cap, stride)) {
        continue;
      }
      uint64_t offset = get_offset(state);
      if (ticket < offset) {
        // There was an expansion with offset greater than this ticket
        update_from_closed(state, ticket, offset, slots, cap, stride);
      }
      if (slots[idx((ticket - offset), cap, stride)].may_enqueue(turn(ticket - offset, cap))) {
        // A slot is ready.
        if (ATOMIC_BCAS(&push_ticket_, ticket, ticket + 1)) {
          // Adjust ticket
          ticket -= offset;
          ret = true;
          break;
        } else {
          continue;
        }
      } else {
        if (ticket != ATOMIC_LOAD(&push_ticket_)) {  // B
          // Try again. Ticket changed.
          continue;
        }
        // Likely to block.
        // Try to expand unless the ticket is for a closed array
        if (offset == get_offset(state)) {
          if (try_expand(state, cap)) {
            // This or another thread started an expansion. Get up-to-date info.
            continue;
          }
        }
        ret = false;
        break;
      }
    } while (true);
    return ret;
  }

  bool try_obtain_promised_push_ticket(uint64_t& ticket, Slot*& slots, int64_t& cap, int64_t& stride)
  {
    bool ret = false;
    uint64_t state = 0;
    do {
      ticket = ATOMIC_LOAD(&push_ticket_);
      uint64_t num_pops = ATOMIC_LOAD(&pop_ticket_);
      if (!try_seqlock_read_section(state, slots, cap, stride)) {
        continue;
      }
      int64_t n = ticket - num_pops;
      if (n >= capacity_) {
        ret = false;
        break;
      }
      if ((n >= cap)) {
        if (try_expand(state, cap)) {
          // This or another thread started an expansion. Start over
          // with a new state.
          continue;
        } else {
          // Can't expand.
          ret = false;
          break;
        }
      }
      uint64_t offset = get_offset(state);
      if (ticket < offset) {
        // There was an expansion with offset greater than this ticket
        update_from_closed(state, ticket, offset, slots, cap, stride);
      }
      if (ATOMIC_BCAS(&push_ticket_, ticket, ticket + 1)) {
        // Adjust ticket
        ticket -= offset;
        ret = true;
        break;
      }
    } while (true);
    return ret;
  }

  bool try_obtain_ready_pop_ticket(uint64_t& ticket, Slot*& slots, int64_t& cap, int64_t& stride)
  {
    bool ret = false;
    uint64_t state = 0;
    do {
      ticket = ATOMIC_LOAD(&pop_ticket_);
      if (!try_seqlock_read_section(state, slots, cap, stride)) {
        continue;
      }
      uint64_t offset = get_offset(state);
      if (ticket < offset) {
        // There was an expansion after the corresponding push ticket
        // was issued.
        update_from_closed(state, ticket, offset, slots, cap, stride);
      }
      if (slots[idx((ticket - offset), cap, stride)].may_dequeue(turn(ticket - offset, cap))) {
        if (ATOMIC_BCAS(&pop_ticket_, ticket, ticket + 1)) {
          // Adjust ticket
          ticket -= offset;
          ret = true;
          break;
        }
      } else {
        ret = false;
        break;
      }
    } while (true);
    return ret;
  }

  bool try_obtain_promised_pop_ticket(uint64_t& ticket, Slot*& slots, int64_t& cap, int64_t& stride)
  {
    bool ret = false;
    uint64_t state = 0;
    do {
      ticket = ATOMIC_LOAD(&pop_ticket_);
      uint64_t num_pushes = ATOMIC_LOAD(&push_ticket_);
      if (!try_seqlock_read_section(state, slots, cap, stride)) {
        continue;
      }
      if (ticket >= num_pushes) {
        ret = false;
        break;
      }
      if (ATOMIC_BCAS(&pop_ticket_, ticket, ticket + 1)) {
        // Adjust ticket
        uint64_t offset = get_offset(state);
        if (ticket < offset) {
          // There was an expansion after the corresponding push
          // ticket was issued.
          update_from_closed(state, ticket, offset, slots, cap, stride);
        }
        // Adjust ticket
        ticket -= offset;
        ret = true;
        break;
      }
    } while (true);
    return ret;
  }

  // Enqueues an element with a specific ticket number
  void enqueue_with_ticket(const uint64_t ticket, void* elem)
  {
    Slot* slots = NULL;
    int64_t cap = 0;
    int64_t stride = 0;
    uint64_t state = 0;
    uint64_t offset = 0;
    while (!try_seqlock_read_section(state, slots, cap, stride)) {}
    offset = get_offset(state);
    if (ticket < offset) {
      // There was an expansion after this ticket was issued.
      update_from_closed(state, ticket, offset, slots, cap, stride);
    }
    enqueue_with_ticket_base(ticket - offset, slots, cap, stride, elem);
  }

  uint64_t get_offset(const uint64_t state) const
  {
    return state >> SEQLOCK_BITS;
  }

  int64_t get_num_closed(const uint64_t state) const
  {
    return (state & ((1 << SEQLOCK_BITS) - 1)) >> 1;
  }

  // Try to expand the queue. Returns true if this expansion was
  // successful or a concurent expansion is in progress. Returns
  // false if the queue has reached its maximum capacity or
  // allocation has failed.
  bool try_expand(const uint64_t state, const int64_t cap)
  {
    bool ret = false;
    if (cap != capacity_) {
      // Acquire seqlock
      uint64_t oldval = state;
      if (ATOMIC_BCAS(&dstate_, oldval, state + 1)) {
        assert(cap == ATOMIC_LOAD(&dcapacity_));
        uint64_t ticket = 1 + std::max(ATOMIC_LOAD(&push_ticket_), ATOMIC_LOAD(&pop_ticket_));
        int64_t new_capacity = std::min(dmult_ * cap, capacity_);
        Slot* new_slots = NULL;
        if (OB_ISNULL(new_slots = reinterpret_cast<Slot*>(
                          ob_malloc(sizeof(Slot) * (new_capacity + 2 * SLOT_PADDING), label_)))) {
          LIB_LOG(ERROR, "allocate slots memory failed.", K(new_capacity));
          // Expansion failed. Restore the seqlock
          ATOMIC_STORE(&dstate_, state);
          ret = false;
        } else {
          // Successful expansion
          // calculate the current ticket offset
          uint64_t offset = get_offset(state);
          // calculate index in closed array
          int64_t index = get_num_closed(state);
          // fill the info for the closed slots array
          closed_[index].offset_ = offset;
          closed_[index].slots_ = ATOMIC_LOAD(&dslots_);
          closed_[index].capacity_ = cap;
          closed_[index].stride_ = ATOMIC_LOAD(&dstride_);
          // update the new slots array info
          ATOMIC_STORE(&dslots_, new_slots);
          ATOMIC_STORE(&dcapacity_, new_capacity);
          ATOMIC_STORE(&dstride_, compute_stride(new_capacity));
          // Release the seqlock and record the new ticket offset
          ATOMIC_STORE(&dstate_, (ticket << SEQLOCK_BITS) + (2 * (index + 1)));
          ret = true;
        }
      } else {  // failed to acquire seqlock
        // Someone acaquired the seqlock. Go back to the caller and get
        // up-to-date info.
        ret = true;
      }
    }
    return ret;
  }

  // Seqlock read-only section
  bool try_seqlock_read_section(uint64_t& state, Slot*& slots, int64_t& cap, int64_t& stride)
  {
    bool ret = false;
    state = ATOMIC_LOAD(&dstate_);
    if (state & 1) {
      // Locked.
      ret = false;
    } else {
      // Start read-only section.
      slots = ATOMIC_LOAD(&dslots_);
      cap = ATOMIC_LOAD(&dcapacity_);
      stride = ATOMIC_LOAD(&dstride_);
      // End of read-only section. Validate seqlock.
      MEM_BARRIER();
      ret = (state == ATOMIC_LOAD(&dstate_));
    }
    return ret;
  }

  // Update local variables of a lagging operation using the
  // most recent closed array with offset <= ticket
  void update_from_closed(
      const uint64_t state, const uint64_t ticket, uint64_t& offset, Slot*& slots, int64_t& cap, int64_t& stride)
  {
    for (int64_t i = get_num_closed(state) - 1; i >= 0; --i) {
      offset = closed_[i].offset_;
      if (offset <= ticket) {
        slots = closed_[i].slots_;
        cap = closed_[i].capacity_;
        stride = closed_[i].stride_;
        break;
      }
    }
    // A closed array with offset <= ticket should have been found
  }

private:
  static const int64_t SEQLOCK_BITS = 6;
  static const int64_t DEFAULT_MIN_DYNAMIC_CAPACITY = 1024;
  static const int64_t DEFAULT_EXPANSION_MULTIPLIER = 2;

  int64_t dmult_;

  //  Info about closed slots arrays for use by lagging operations
  ObClosedArray* closed_;
  lib::ObLabel label_;

  DISALLOW_COPY_AND_ASSIGN(ObDynamicMPMCQueue);
};

}  // end namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_MPMC_QUEUE_
