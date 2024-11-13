
// Copyright 2024-present the vsag project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include "vsag/logger.h"
#include "vsag/options.h"

namespace vsag {
namespace logger {

enum class level {
    trace = Logger::Level::kTRACE,
    debug = Logger::Level::kDEBUG,
    info = Logger::Level::kINFO,
    warn = Logger::Level::kWARN,
    err = Logger::Level::kERR,
    critical = Logger::Level::kCRITICAL,
    off = Logger::Level::kOFF
};

inline void
set_level(level log_level) {
    Options::Instance().logger()->SetLevel((Logger::Level)log_level);
}

inline void
trace(const std::string& msg) {
    Options::Instance().logger()->Trace(msg);
}

inline void
debug(const std::string& msg) {
    Options::Instance().logger()->Debug(msg);
}

inline void
info(const std::string& msg) {
    Options::Instance().logger()->Info(msg);
}

inline void
warn(const std::string& msg) {
    Options::Instance().logger()->Warn(msg);
}

inline void
error(const std::string& msg) {
    Options::Instance().logger()->Error(msg);
}

inline void
critical(const std::string& msg) {
    Options::Instance().logger()->Critical(msg);
}

template <typename... Args>
inline void
trace(fmt::format_string<Args...> fmt, Args&&... args) {
    trace(fmt::format(fmt, std::forward<Args>(args)...));
}

template <typename... Args>
inline void
debug(fmt::format_string<Args...> fmt, Args&&... args) {
    debug(fmt::format(fmt, std::forward<Args>(args)...));
}

template <typename... Args>
inline void
info(fmt::format_string<Args...> fmt, Args&&... args) {
    info(fmt::format(fmt, std::forward<Args>(args)...));
}

template <typename... Args>
inline void
warn(fmt::format_string<Args...> fmt, Args&&... args) {
    warn(fmt::format(fmt, std::forward<Args>(args)...));
}

template <typename... Args>
inline void
error(fmt::format_string<Args...> fmt, Args&&... args) {
    error(fmt::format(fmt, std::forward<Args>(args)...));
}

template <typename... Args>
inline void
critical(fmt::format_string<Args...> fmt, Args&&... args) {
    critical(fmt::format(fmt, std::forward<Args>(args)...));
}

}  // namespace logger
}  // namespace vsag
