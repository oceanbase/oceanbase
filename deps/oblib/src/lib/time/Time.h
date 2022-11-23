#ifndef TBSYS_TIME_H
#define TBSYS_TIME_H
#include <sys/time.h>
#include <stdint.h>
#include <string>
namespace obutil
{
/**
* @brief Time class provide interface for time operation
*/
class ObSysTime
{
public:

    ObSysTime();

    enum Clock { Realtime, Monotonic };
    /**
     * @brief get current time
     *
     * @param clock Realtime: system rtc time; Monotonic: system time from boost
     *
     * @return
     */
    static ObSysTime now(Clock clock= Realtime);
    /**
     * @brief construct a Time object
     *
     * @param usec: time of construct(seconds)
     *
     * @return
     */
    static ObSysTime seconds(int64_t usec);
    /**
     * @brief construct a Time object
     *
     * @param milli : time of construct(milliseconds)
     *
     * @return
     */
    static ObSysTime milliSeconds(int64_t milli);
    /**
     * @brief construct a Time object
     *
     * @param micro : time of construct(microseconds)
     *
     * @return
     */
    static ObSysTime microSeconds(int64_t micro);

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

    ObSysTime operator-() const
    {
        return ObSysTime(-_usec);
    }

    ObSysTime operator-(const ObSysTime& rhs) const
    {
        return ObSysTime(_usec - rhs._usec);
    }

    ObSysTime operator+(const ObSysTime& rhs) const
    {
        return ObSysTime(_usec + rhs._usec);
    }

    ObSysTime& operator+=(const ObSysTime& rhs)
    {
        _usec += rhs._usec;
        return *this;
    }

    ObSysTime& operator-=(const ObSysTime& rhs)
    {
        _usec -= rhs._usec;
        return *this;
    }

    bool operator<(const ObSysTime& rhs) const
    {
        return _usec < rhs._usec;
    }

    bool operator<=(const ObSysTime& rhs) const
    {
        return _usec <= rhs._usec;
    }

    bool operator>(const ObSysTime& rhs) const
    {
        return _usec > rhs._usec;
    }

    bool operator>=(const ObSysTime& rhs) const
    {
        return _usec >= rhs._usec;
    }

    bool operator==(const ObSysTime& rhs) const
    {
        return _usec == rhs._usec;
    }

    bool operator!=(const ObSysTime& rhs) const
    {
        return _usec != rhs._usec;
    }

    double operator/(const ObSysTime& rhs) const
    {
        return (double)_usec / (double)rhs._usec;
    }

    ObSysTime& operator*=(int rhs)
    {
        _usec *= rhs;
        return *this;
    }

    ObSysTime operator*(int rhs) const
    {
        ObSysTime t;
        t._usec = _usec * rhs;
        return t;
    }

    ObSysTime& operator/=(int rhs)
    {
        _usec /= rhs;
        return *this;
    }

    ObSysTime operator/(int rhs) const
    {
        ObSysTime t;
        t._usec = _usec / rhs;
        return t;
    }

    ObSysTime& operator*=(int64_t rhs)
    {
        _usec *= rhs;
        return *this;
    }

    ObSysTime operator*(int64_t rhs) const
    {
        ObSysTime t;
        t._usec = _usec * rhs;
        return t;
    }

    ObSysTime& operator/=(int64_t rhs)
    {
        _usec /= rhs;
        return *this;
    }

    ObSysTime operator/(int64_t rhs) const
    {
        ObSysTime t;
        t._usec = _usec / rhs;
        return t;
    }

    ObSysTime& operator*=(double rhs)
    {
        _usec = static_cast<int64_t>(static_cast<double>(_usec) * rhs);
        return *this;
    }

    ObSysTime operator*(double rhs) const
    {
        ObSysTime t;
        t._usec = static_cast<int64_t>(static_cast<double>(_usec) * rhs);
        return t;
    }

    ObSysTime& operator/=(double rhs)
    {
        _usec = static_cast<int64_t>(static_cast<double>(_usec) / rhs);
        return *this;
    }

    ObSysTime operator/(double rhs) const
    {
        ObSysTime t;
        t._usec = static_cast<int64_t>(static_cast<double>(_usec) / rhs);
        return t;
    }

    ObSysTime(int64_t);

private:

    int64_t _usec;
};
}//end namespace
#endif
