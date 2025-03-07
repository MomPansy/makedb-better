#pragma once 
#include "ILogger.h"
#include "console_logger.h"
// The global logger is a singleton class that provides a single point of access to the logger.

class GlobalLogger {
    public: 
        static ILogger &instance() {
            static ConsoleLogger logger; 
            return logger;
        }
};