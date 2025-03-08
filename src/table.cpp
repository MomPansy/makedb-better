#include "table.h"

bool Table::initialize()
{
    logger_.log("Initializing table: " + tableDir_);

    // Initialize schema
    bool schemaInitialization = schema_.initialize();
    bool pageManagerInitialization = pageManager_.initialize();

    if (!schemaInitialization || !pageManagerInitialization)
    {
        return false;
    }
    initialized_ = true;
    return true;
}

bool Table::createSchema(std::vector<Column> &columns)
{
    if (!initialized_)
    {
        return false;
    }

    logger_.log("Creating schema for table: " + tableDir_);
    if (!schema_.write(columns))
    {
        return false;
    }
    return true;
}

std::vector<Column> Table::getSchema()
{
    if (!initialized_)
    {
        return {};
    }

    return schema_.getSchema();
}

bool Table::writeDataFromFile(const std::string &filename, char delimiter)
{
    if (!initialized_)
    {
        return {};
    }

    DataObject data = parser_.parseFile(filename, delimiter, schema_.getSchema());
    if (data.serializedData.empty())
    {
        return false;
    }
    pageManager_.insertData(data.serializedData, data.serializedDataSize, data.numRows);
    return true;
}
