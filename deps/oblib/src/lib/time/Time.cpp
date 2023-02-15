#include "lib/time/Time.h"

#include <iomanip>
#include <sys/time.h>
#include <cassert>
#include <stdint.h>
#include <sstream>
#include "lib/oblog/ob_log.h"

namespace obutil
{
ObSysTime::ObSysTime() :
    _usec(0)
{
}

ObSysTime ObSysTime::now(Clock clock)
{
    if(clock == Realtime)
    {
        struct timeval tv;
        if(gettimeofday(&tv, 0) < 0)
        {
#ifdef _NO_EXCEPTION
            _OB_LOG_RET(ERROR, oceanbase::common::OB_ERROR, "%s","SyscallException");
            assert( 0 );
#else
            throw SyscallException(__FILE__, __LINE__, errno);
#endif
        }
        return ObSysTime(tv.tv_sec * INT64_C(1000000) + tv.tv_usec);
    }
    else // Monotonic
    {
        struct timespec ts;
        if(clock_gettime(CLOCK_MONOTONIC, &ts) < 0)
        {
#ifdef _NO_EXCEPTION
            _OB_LOG_RET(ERROR, oceanbase::common::OB_ERROR, "%s","SyscallException");
            assert(0);
#else
            throw SyscallException(__FILE__, __LINE__, errno);
#endif
        }
        return ObSysTime(ts.tv_sec * INT64_C(1000000) + ts.tv_nsec / INT64_C(1000));
    }
}

ObSysTime ObSysTime::seconds(int64_t t)
{
    return ObSysTime(t * INT64_C(1000000));
}

ObSysTime ObSysTime::milliSeconds(int64_t t)
{
    return ObSysTime(t * INT64_C(1000));
}

ObSysTime ObSysTime::microSeconds(int64_t t)
{
    return ObSysTime(t);
}

ObSysTime::operator timeval() const
{
    timeval tv;
    tv.tv_sec = static_cast<long>(_usec / 1000000);
    tv.tv_usec = static_cast<long>(_usec % 1000000);
    return tv;
}

int64_t ObSysTime::toSeconds() const
{
    return _usec / 1000000;
}

int64_t ObSysTime::toMilliSeconds() const
{
    return _usec / 1000;
}

int64_t ObSysTime::toMicroSeconds() const
{
    return _usec;
}

double ObSysTime::toSecondsDouble() const
{
  return static_cast<double>(_usec) / 1000000.0;
}

double ObSysTime::toMilliSecondsDouble() const
{
  return static_cast<double>(_usec) / 1000.0;
}

double ObSysTime::toMicroSecondsDouble() const
{
    return static_cast<double>(_usec);
}

std::string ObSysTime::toDateTime() const
{
    time_t time = static_cast<long>(_usec / 1000000);

    struct tm* t;
    struct tm tr;
    localtime_r(&time, &tr);
    t = &tr;

    char buf[32];
    strftime(buf, sizeof(buf), "%F %H:%M:%S", t);
    //strftime(buf, sizeof(buf), "%x %H:%M:%S", t);

    std::ostringstream os;
    os << buf << ".";
    os.fill('0');
    os.width(3);
    os << static_cast<long>(_usec % 1000000 / 1000);
    return os.str();
}

std::string ObSysTime::toDuration() const
{
    int64_t usecs = _usec % 1000000;
    int64_t secs = _usec / 1000000 % 60;
    int64_t mins = _usec / 1000000 / 60 % 60;
    int64_t hours = _usec / 1000000 / 60 / 60 % 24;
    int64_t days = _usec / 1000000 / 60 / 60 / 24;

    using namespace std;

    ostringstream os;
    if(days != 0)
    {
        os << days << "d ";
    }
    os << setfill('0') << setw(2) << hours << ":" << setw(2) << mins << ":" << setw(2) << secs;
    if(usecs != 0)
    {
        os << "." << setw(3) << (usecs / 1000);
    }

    return os.str();
}

ObSysTime::ObSysTime(int64_t usec) :
    _usec(usec)
{
}

}//end namespace obutil
