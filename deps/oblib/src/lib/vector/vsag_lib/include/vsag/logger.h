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

#include <memory>
#include <string>

namespace vsag {

class Logger;

class Logger {
public:
    enum Level : int {
        kTRACE = 0,
        kDEBUG = 1,
        kINFO = 2,
        kWARN = 3,
        kERR = 4,
        kCRITICAL = 5,
        kOFF = 6,
        kN_LEVELS
    };

    virtual void
    SetLevel(Level log_level) = 0;

    virtual void
    Trace(const std::string& msg) = 0;

    virtual void
    Debug(const std::string& msg) = 0;

    virtual void
    Info(const std::string& msg) = 0;

    virtual void
    Warn(const std::string& msg) = 0;

    virtual void
    Error(const std::string& msg) = 0;

    virtual void
    Critical(const std::string& msg) = 0;

public:
    virtual ~Logger() = default;
};

}  // namespace vsag
