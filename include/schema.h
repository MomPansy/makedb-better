#pragma once

#include <string>
#include <vector>
#include <cstdint>

#include "IStorage.h"
#include "ILogger.h"
#include "global_logger.h"

enum class DataType
{
    INT,
    FLOAT,
    TEXT,
    DATE
};

struct Column
{
    std::string name;
    DataType type;
};

struct SchemaHeader
{
    uint16_t num_columns;
};

class Schema
{
public:
    Schema(const std::string &tableName, IStorage &storage, ILogger &logger = GlobalLogger::instance()) : storage_(storage),
                                                                                                          filepath_(tableName + "/schema.dat"),
                                                                                                          logger_(logger) {};

    // write schema to disk
    bool write(const std::vector<Column> &schema);
    std::vector<Column> read();
    bool initialize();
    bool exists();
    const std::vector<Column> &getSchema() { return schema_; }
    
private:
    IStorage &storage_;
    std::string filepath_;
    std::vector<Column> schema_;
    ILogger &logger_;
    SchemaHeader header_;
};