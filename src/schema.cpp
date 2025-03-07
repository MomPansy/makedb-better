#include "schema.h"

void Schema::initialize()
{
    // check if file exists
    logger_.log("Checking if schema file exists: " + filepath_);
    if (storage_.fileExists(filepath_))
    {
        logger_.log("Schema file exists: " + filepath_ + ". Reading schema file.");
        read();
    }
    else
    {
        // create schema file
        logger_.log("Schema file does not exist: " + filepath_ + ". Creating schema file.");
        storage_.createFile(filepath_);
    }
}

bool Schema::write(const std::vector<Column> &schema)
{
    SchemaHeader tmpHeader;
    tmpHeader.num_columns = static_cast<uint16_t>(schema.size());

    // Create a buffer large enough for header + columns.
    std::vector<char> buffer(sizeof(SchemaHeader) + schema.size() * sizeof(Column));

    // Copy header
    std::memcpy(buffer.data(), &tmpHeader, sizeof(SchemaHeader));
    // Copy columns
    if (!schema.empty())
    {
        std::memcpy(buffer.data() + sizeof(SchemaHeader),
                    schema.data(),
                    schema.size() * sizeof(Column));
    }

    bool success = storage_.writeFile(
        filepath_,
        buffer.data(),
        buffer.size());

    if (!success)
    {
        // Either throw or return false, but not both
        logger_.log("Failed to write schema to file: " + filepath_);
        return false;
    }

    // If everything succeeded, update your in-memory state.
    header_ = tmpHeader;
    schema_ = schema;
    return true;
}

std::vector<Column> Schema::read()
{
    // First read the header
    if (!storage_.readFile(filepath_, reinterpret_cast<char *>(&header_), sizeof(SchemaHeader)))
    {
        logger_.log("Failed to read schema header from file: " + filepath_);
        throw std::runtime_error("Failed to read schema header from file: " + filepath_);
    }

    // Now read the columns
    schema_.resize(header_.num_columns);
    if (header_.num_columns > 0)
    {
        bool success = storage_.readFile(
            filepath_,
            reinterpret_cast<char *>(schema_.data()),
            header_.num_columns * sizeof(Column),
            sizeof(SchemaHeader));

        if (!success)
        {
            logger_.log("Failed to read schema columns from file: " + filepath_);
            throw std::runtime_error("Failed to read schema columns from file: " + filepath_);
        }
    }

    return schema_;
}
