#pragma once
#include <string>
#include <vector>
#include <cstring>

#include "ILogger.h"
#include "global_logger.h"
#include "schema.h"
#include "page_manager.h"
#include "parser.h"
#include "IStorage.h"

class Table
{
public:
    Table(const std::string &name, ILogger &logger, PageManager &pageManager, Schema &schema, Parser &parser, IStorage &storage)
        : tableDir_(name), logger_(logger), pageManager_(pageManager), schema_(schema), parser_(parser) {}

    bool initialize();
    bool createSchema(std::vector<Column> &columns);
    std::vector<Column> getSchema();
    bool writeDataFromFile(const std::string &filename, char delimiter = '\t');

private:
    const std::string &tableDir_;
    ILogger &logger_;
    PageManager &pageManager_;
    Schema &schema_;
    Parser &parser_;
    bool initialized_ = false; // flag to check if the table has been initialized
};