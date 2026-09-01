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

 #ifndef OCEANBASE_LOG_FETCHER_LS_PROGRESS_H_
 #define OCEANBASE_LOG_FETCHER_LS_PROGRESS_H_

 #include "lib/lock/ob_small_spin_lock.h"    // ObByteLock
 #include "logservice/palf/lsn.h"            // LSN
 #include "lib/utility/ob_print_utils.h"     // TO_STRING_KV
 #include "logservice/logfetcher/ob_log_utils.h" // get_timestamp
 #include "lib/oblog/ob_log_module.h"        // OBLOG_FETCHER_LOG

 namespace oceanbase
 {
 namespace logfetcher
 {

 ///////////////////////////////// LSProgress /////////////////////////////////
 //
 // At the moment of startup, only the startup timestamp of the LS is known, not the specific log progress, using the following convention.
 // 1. set the start timestamp to the log_progress
 // 2. next_lsn is invalid
 // 3. wait for the start lsn locator to look up the start_lsn and set it to next_lsn
 // 4. start lsn may have fallback, during fetch the fallback log, the log_progress is not updated,
 // since the lag log progress is less than the start timestamp, the log progress remains unchanged; but touch_tstamp remains updated
 struct LSProgress
 {
   // Log progress
   // 1. log_progress normally refers to the lower bound of the next log timestamp
   // 2. log_progress and next_lsn are invalid at startup
   palf::LSN next_lsn_;            // next LSN
   int64_t   log_progress_;        // log progress(nanosecond)
   int64_t   log_touch_tstamp_;    // Log progress last update time

   // Lock: Keeping read and write operations atomic
   mutable common::ObByteLock  lock_;

   LSProgress() { reset(); }
   ~LSProgress() { reset(); }

   TO_STRING_KV(K_(next_lsn),
       "log_progress", NTS_TO_STR(log_progress_),
       "log_touch_tstamp", TS_TO_STR(log_touch_tstamp_));

   void reset()
   {
     next_lsn_.reset();
     log_progress_ = OB_INVALID_TIMESTAMP;
     log_touch_tstamp_ = OB_INVALID_TIMESTAMP;
   }

   // Note: start_lsn may be invalid, but start_tstamp_ns should be valid
   // start_lsn refers to the start LSN, which may not be valid
   // start_tstamp_ns refers to the LS start timestamp, not the start_lsn log timestamp
   //
   // Therefore, this function sets start_tstamp_ns to the current progress
   void reset(const palf::LSN start_lsn, const int64_t start_tstamp_ns)
   {
     // Update next_sn
     next_lsn_ = start_lsn;
     // Set start-up timestamp to progress
     log_progress_ = start_tstamp_ns;
     log_touch_tstamp_ = get_timestamp();
   }

   const palf::LSN &get_next_lsn() const { return next_lsn_; }
   void set_next_lsn(const palf::LSN start_lsn) { next_lsn_ = start_lsn; }

   // Get current progress
   int64_t get_progress() const { return log_progress_; }
   int64_t get_touch_tstamp() const { return log_touch_tstamp_; }

   // Copy the entire progress item to ensure atomicity
   void atomic_copy(LSProgress &prog) const
   {
     // protected by lock
     common::ObByteLockGuard guard(lock_);

     prog.next_lsn_ = next_lsn_;
     prog.log_progress_ = log_progress_;
     prog.log_touch_tstamp_ = log_touch_tstamp_;
   }

   // Update the touch timestamp if progress is greater than the upper limit
   // If the progress is greater than the upper limit, the touch timestamp of the corresponding progress is updated
   // NOTE: The purpose of this function is to prevent the touch timestamp from not being updated for a long time if the progress
   // is greater than the upper limit, which could lead to a false detection of a progress timeout if the upper limit suddenly increases.
   void update_touch_tstamp_if_progress_beyond_upper_limit(const int64_t upper_limit)
   {
     common::ObByteLockGuard guard(lock_);

     if (OB_INVALID_TIMESTAMP != log_progress_
         && OB_INVALID_TIMESTAMP != upper_limit
         && log_progress_ >= upper_limit) {
       log_touch_tstamp_ = get_timestamp();
     }
   }

   // Update log progress
   // Update both the LSN and the log progress
   // Require LSN to be updated sequentially, otherwise return OB_LOG_NOT_SYNC
   //
   // Update log progress once for each log parsed to ensure sequential update
   int update_log_progress(const palf::LSN &new_next_lsn,
       const int64_t new_lsn_length,
       const int64_t new_log_progress)
   {
     common::ObByteLockGuard guard(lock_);

     int ret = OB_SUCCESS;

     // Require next_lsn to be valid
     if (OB_UNLIKELY(! next_lsn_.is_valid())) {
       ret = OB_INVALID_ERROR;
       // In header files, use OBLOG_FETCHER_LOG instead of LOG_ERROR
       // because LOG_ERROR requires USING_LOG_PREFIX to be defined in .cpp files
       OBLOG_FETCHER_LOG(ERROR, "invalid next_lsn", KR(ret), K(next_lsn_), K_(log_progress));
     }
     // Verifying log continuity
     else if (OB_UNLIKELY((next_lsn_ + new_lsn_length) != new_next_lsn)) {
       ret = OB_LOG_NOT_SYNC;
       OBLOG_FETCHER_LOG(ERROR, "log not sync", KR(ret), K(next_lsn_), K(new_next_lsn), K(new_lsn_length));
     } else {
       next_lsn_ = new_next_lsn;

       // Update log progress if it is invalid, or if log progress has been updated
       if (OB_INVALID_TIMESTAMP == log_progress_ ||
           (OB_INVALID_TIMESTAMP != new_log_progress && new_log_progress > log_progress_)) {
         log_progress_ = new_log_progress;
       }

       // Log progress update, update the log_touch_tstamp_, the reason is:
       //
       // 1. Normally, if the log progress is updated, indicating that the log was fetched and that the progress was updated anyway
       // 2. At startup, if there is a log rollback and the progress is equal to the startup timestamp and cannot be rolled back,
       // so the fetched log progress is less than the start progress and the update of the log progress does not update the progress,
       // but the LS does fetched the log, in which case the "update timestamp of progress" needs to be updated
       log_touch_tstamp_ = get_timestamp();
     }

     return ret;
   }
 };

 } // namespace logfetcher
 } // namespace oceanbase

 #endif /* OCEANBASE_LOG_FETCHER_LS_PROGRESS_H_ */


