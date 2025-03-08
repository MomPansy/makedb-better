#pragma once
#include <string>
#include <vector>
#include <stdexcept>
#include <fstream>
#include <sstream>

#include "schema.h"
#include "ILogger.h"
#include "global_logger.h"
#include "row.h"
 
struct DataObject
{
    std::vector<std::vector<char>> serializedData;
    size_t serializedDataSize;
    size_t numRows;
};

class Parser
{
public:
    Parser(ILogger &logger) : logger_(logger) {};
    DataObject parseFile(const std::string &filename, char delimiter, const std::vector<Column> &columns); 
private:
    // Splits a single line of text on the given delimiter, returning tokens
    // in a vector of strings.
    std::vector<std::string> split(const std::string &s, char delimiter);
    ILogger &logger_;
};
