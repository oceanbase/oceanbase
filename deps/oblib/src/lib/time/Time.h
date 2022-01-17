// **********************************************************************
//
// Copyright (c) 2003-2016 ZeroC, Inc. All rights reserved.
//
// This copy of Ice is licensed to you under the terms described in the
// ICE_LICENSE file included in this distribution.
//
// **********************************************************************
#ifndef TBSYS_TIME_H
#define TBSYS_TIME_H
#include <sys/time.h>
#include <stdint.h>
#include <string>
namespace tbutil {
/**
 * @brief Time class provide interface for time operation
 */
class Time {
public:
  Time();

  enum Clock { Realtime, Monotonic };
  /**
   * @brief get current time
   *
   * @param clock Realtime: system rtc time; Monotonic: system time from boost
   *
   * @return
   */
  static Time now(Clock clock = Realtime);
  /**
   * @brief construct a Time object
   *
   * @param usec: time of construct(seconds)
   *
   * @return
   */
  static Time seconds(int64_t usec);
  /**
   * @brief construct a Time object
   *
   * @param milli : time of construct(milliseconds)
   *
   * @return
   */
  static Time milliSeconds(int64_t milli);
  /**
   * @brief construct a Time object
   *
   * @param micro : time of construct(microseconds)
   *
   * @return
   */
  static Time microSeconds(int64_t micro);

  /**
   * @brief convert Time to timeval
   *
   * @return
   */
  operator timeval() const;

  /**
   * @brief convert time to second
   *
   * @return
   */
  int64_t toSeconds() const;
  /**
   * @brief convert time to millisecond
   *
   * @return
   */
  int64_t toMilliSeconds() const;
  /**
   * @brief convert time to microseconds
   *
   * @return
   */
  int64_t toMicroSeconds() const;

  double toSecondsDouble() const;
  double toMilliSecondsDouble() const;
  double toMicroSecondsDouble() const;

  /**
   * @brief convert time to string, such as: 2009-10-26 10:47:47.932
   *
   * @return
   */
  std::string toDateTime() const;
  /**
   * @brief convert time to string, such as: 14543d 02:47:47.932
   *
   * @return
   */
  std::string toDuration() const;

  Time operator-() const
  {
    return Time(-_usec);
  }

  Time operator-(const Time& rhs) const
  {
    return Time(_usec - rhs._usec);
  }

  Time operator+(const Time& rhs) const
  {
    return Time(_usec + rhs._usec);
  }

  Time& operator+=(const Time& rhs)
  {
    _usec += rhs._usec;
    return *this;
  }

  Time& operator-=(const Time& rhs)
  {
    _usec -= rhs._usec;
    return *this;
  }

  bool operator<(const Time& rhs) const
  {
    return _usec < rhs._usec;
  }

  bool operator<=(const Time& rhs) const
  {
    return _usec <= rhs._usec;
  }

  bool operator>(const Time& rhs) const
  {
    return _usec > rhs._usec;
  }

  bool operator>=(const Time& rhs) const
  {
    return _usec >= rhs._usec;
  }

  bool operator==(const Time& rhs) const
  {
    return _usec == rhs._usec;
  }

  bool operator!=(const Time& rhs) const
  {
    return _usec != rhs._usec;
  }

  double operator/(const Time& rhs) const
  {
    return (double)_usec / (double)rhs._usec;
  }

  Time& operator*=(int rhs)
  {
    _usec *= rhs;
    return *this;
  }

  Time operator*(int rhs) const
  {
    Time t;
    t._usec = _usec * rhs;
    return t;
  }

  Time& operator/=(int rhs)
  {
    _usec /= rhs;
    return *this;
  }

  Time operator/(int rhs) const
  {
    Time t;
    t._usec = _usec / rhs;
    return t;
  }

  Time& operator*=(int64_t rhs)
  {
    _usec *= rhs;
    return *this;
  }

  Time operator*(int64_t rhs) const
  {
    Time t;
    t._usec = _usec * rhs;
    return t;
  }

  Time& operator/=(int64_t rhs)
  {
    _usec /= rhs;
    return *this;
  }

  Time operator/(int64_t rhs) const
  {
    Time t;
    t._usec = _usec / rhs;
    return t;
  }

  Time& operator*=(double rhs)
  {
    _usec = static_cast<int64_t>(static_cast<double>(_usec) * rhs);
    return *this;
  }

  Time operator*(double rhs) const
  {
    Time t;
    t._usec = static_cast<int64_t>(static_cast<double>(_usec) * rhs);
    return t;
  }

  Time& operator/=(double rhs)
  {
    _usec = static_cast<int64_t>(static_cast<double>(_usec) / rhs);
    return *this;
  }

  Time operator/(double rhs) const
  {
    Time t;
    t._usec = static_cast<int64_t>(static_cast<double>(_usec) / rhs);
    return t;
  }

  Time(int64_t);

private:
  int64_t _usec;
};
}  // namespace tbutil
#endif
