#include "schema.h"
#include <cstring>

bool Schema::initialize()
{
    // check if file exists
    logger_.log("Checking if schema file exists: " + filepath_);
    if (storage_.fileExists(filepath_))
    {
        logger_.log("Schema file exists: " + filepath_ + ". Reading schema file.");
        try
        {
            read();
        }
        catch (const std::runtime_error &e)
        {
            throw std::runtime_error("Failed to read schema file: " + filepath_);
        }
    }
    else
    {
        // create schema file
        logger_.log("Schema file does not exist: " + filepath_ + ". Creating schema file.");
        try
        {
            storage_.createFile(filepath_);
        }
        catch (const std::runtime_error &e)
        {
            throw std::runtime_error("Failed to create schema file: " + filepath_);
        }
    }

    return true;
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
    size_t fileSize = storage_.getSize(filepath_);
    if (fileSize < sizeof(SchemaHeader))
    {
        logger_.log("Schema file is too small or empty: " + filepath_);
        throw std::runtime_error("Schema file is empty or too small to contain a header: " + filepath_);
    }

    if (!storage_.readFile(filepath_, reinterpret_cast<char *>(&header_), sizeof(SchemaHeader)))
    {
        logger_.log("Failed to read schema header from file: " + filepath_);
        throw std::runtime_error("Failed to read schema header from file: " + filepath_);
    }
    // If num_columns == 0, we consider it an empty schema
    if (header_.num_columns == 0)
    {
        logger_.log("Schema indicates 0 columns. Treating as empty schema.");
        schema_.clear();
        return schema_;
    }

    // 3) Check if there's enough file size for columns
    size_t neededSize = sizeof(SchemaHeader) + header_.num_columns * sizeof(Column);
    if (fileSize < neededSize)
    {
        logger_.log("Schema file size (" + std::to_string(fileSize) +
                    ") is smaller than required (" + std::to_string(neededSize) +
                    ") to hold " + std::to_string(header_.num_columns) + " columns.");
        throw std::runtime_error("Schema file is corrupted or incomplete: " + filepath_);
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
