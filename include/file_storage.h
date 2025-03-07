#pragma once
#include <fstream>
#include "IStorage.h"
#include "ILogger.h"
#include "global_logger.h"

class FileStorage : public IStorage
{
public:
    FileStorage(ILogger &logger = GlobalLogger::instance()) : logger_(logger) {};
    bool writeFile(const std::string &filename, char *data, std::size_t size, std::streampos offset = 0) override;
    bool readFile(const std::string &filename, char *buffer, std::size_t size, std::streampos offset = 0) override;
    void appendFile(const std::string &filename, const char *data, std::size_t size);
    bool fileExists(const std::string &filename) override;
    bool createFile(const std::string &filename) override;

private:
    ILogger &logger_;
};