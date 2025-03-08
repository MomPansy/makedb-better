#include "row.h"

Row::Row(const std::vector<Column> &row, const std::vector<Value> &typedData) : schema_(row), values_(typedData)
{
    if (schema_.size() != values_.size())
    {
        throw std::runtime_error("Schema and data size mismatch");
    }
    // check if the data types match the schema
    for (size_t i = 0; i < schema_.size(); i++)
    {
        switch (schema_[i].type)
        {
        case DataType::INT:
            if (!std::holds_alternative<int32_t>(values_[i]))
            {
                throw std::runtime_error("Data type mismatch at index: " + std::to_string(i));
            }
            break;
        case DataType::FLOAT:
            if (!std::holds_alternative<float>(values_[i]))
            {
                throw std::runtime_error("Data type mismatch at index: " + std::to_string(i));
            }
            break;
        case DataType::TEXT:
            if (!std::holds_alternative<std::string>(values_[i]))
            {
                throw std::runtime_error("Data type mismatch at index: " + std::to_string(i));
            }
            break;
        case DataType::DATE:
        {
            if (!std::holds_alternative<std::string>(values_[i]))
            {
                throw std::runtime_error("Data type mismatch at index " + std::to_string(i) +
                                         " (expected DATE as string)");
            }
            // Optionally validate the date format
            const auto &dateStr = std::get<std::string>(values_[i]);
            if (!isValidDateFormat(dateStr))
            {
                throw std::runtime_error("Invalid DATE format at index " + std::to_string(i) +
                                         ", got: " + dateStr);
            }
            break;
        }
        default:
            throw std::runtime_error("Unsupported data type in schema");
        }
    }
}

void Row::setValue(size_t colIndex, const int32_t &value)
{
    if (colIndex >= values_.size() || schema_[colIndex].type != DataType::INT)
    {
        throw std::out_of_range("Column index out of range or type mismatch (expected INT)");
    }

    values_[colIndex] = value;
}

void Row::setValue(size_t colIndex, const float &value)
{
    if (colIndex >= values_.size() || schema_[colIndex].type != DataType::FLOAT)
    {
        throw std::out_of_range("Column index out of range or type mismatch (expected FLOAT)");
    }

    values_[colIndex] = value;
}

void Row::setValue(size_t colIndex, const std::string &value)
{
    if (colIndex >= values_.size() || schema_[colIndex].type != DataType::TEXT)
    {
        throw std::out_of_range("Column index out of range or type mismatch (expected TEXT)");
    }

    values_[colIndex] = value;
}

void Row::setDate(size_t colIndex, const std::string &value)
{
    if (colIndex >= values_.size() || schema_[colIndex].type != DataType::DATE)
    {
        throw std::out_of_range("Column index out of range or type mismatch (expected DATE)");
    }
    if (!isValidDateFormat(value))
    {
        throw std::invalid_argument("Date string does not match DD/MM/YYYY: " + value);
    }
    values_[colIndex] = value;
}

int32_t Row::getInt(size_t colIndex) const
{
    if (colIndex >= values_.size() || schema_[colIndex].type != DataType::INT)
    {
        throw std::out_of_range("Column index out of range or type mismatch (expected INT)");
    }

    return std::get<int32_t>(values_[colIndex]);
}

float Row::getFloat(size_t colIndex) const
{
    if (colIndex >= values_.size() || schema_[colIndex].type != DataType::FLOAT)
    {
        throw std::out_of_range("Column index out of range or type mismatch (expected FLOAT)");
    }

    return std::get<float>(values_[colIndex]);
}

std::string Row::getText(size_t colIndex) const
{
    if (colIndex >= values_.size() || schema_[colIndex].type != DataType::TEXT)
    {
        throw std::out_of_range("Column index out of range or type mismatch (expected TEXT)");
    }

    return std::get<std::string>(values_[colIndex]);
}

std::string Row::getDate(size_t colIndex) const
{
    if (colIndex >= values_.size() || schema_[colIndex].type != DataType::DATE)
    {
        throw std::out_of_range("Column index out of range or type mismatch (expected DATE)");
    }
    return std::get<std::string>(values_[colIndex]);
}

std::vector<char> Row::serialize() const
{
    size_t totalSize = getSerializedSize();
    std::vector<char> buffer;
    buffer.reserve(totalSize);

    // process each column
    for (size_t i = 0; i < schema_.size(); i++)
    {
        DataType type = schema_[i].type;
        switch (type)
        {
        case DataType::INT:
        {
            int32_t value = std::get<int32_t>(values_[i]);
            buffer.insert(buffer.end(), reinterpret_cast<char *>(&value), reinterpret_cast<char *>(&value) + sizeof(int32_t));
            break;
        }
        case DataType::FLOAT:
        {
            float value = std::get<float>(values_[i]);
            buffer.insert(buffer.end(), reinterpret_cast<char *>(&value), reinterpret_cast<char *>(&value) + sizeof(float));
            break;
        }
        case DataType::TEXT:
        {
            // Using const auto &value just gives a constant reference to the string, so we don’t create a new copy.
            const auto &value = std::get<std::string>(values_[i]);
            uint16_t length = value.size();
            buffer.insert(buffer.end(), reinterpret_cast<char *>(&length), reinterpret_cast<char *>(&length) + sizeof(uint16_t));
            buffer.insert(buffer.end(), value.begin(), value.end());
            break;
        }
        case DataType::DATE:
        {
            // We'll serialize it like TEXT: length + string data
            const auto &dateStr = std::get<std::string>(values_[i]);
            uint16_t length = static_cast<uint16_t>(dateStr.size());
            const char *lenPtr = reinterpret_cast<const char *>(&length);
            buffer.insert(buffer.end(), lenPtr, lenPtr + sizeof(uint16_t));
            buffer.insert(buffer.end(), dateStr.begin(), dateStr.end());
            break;
        }
        default:
            throw std::runtime_error("Unsupported data type in schema");
        }
    }
    return buffer;
}

size_t Row::getSerializedSize() const
{
    size_t size = 0;
    for (size_t i = 0; i < schema_.size(); i++)
    {
        DataType type = schema_[i].type;
        switch (type)
        {
        case DataType::INT:
        {
            size += sizeof(int32_t);
            break;
        }
        case DataType::FLOAT:
        {
            size += sizeof(float);
            break;
        }
        case DataType::TEXT:
        {
            std::string value = std::get<std::string>(values_[i]);
            size += sizeof(uint16_t) + value.size();
            break;
        }
        case DataType::DATE:
        {
            const auto &dateStr = std::get<std::string>(values_[i]);
            size += sizeof(uint16_t); // length
            size += dateStr.size();   // data
            break;
        }
        default:
            throw std::runtime_error("Unsupported data type in schema");
        }
    }
    return size;
}
bool Row::isValidDateFormat(const std::string &date) const
{
    // Basic check: must be dd/mm/yyyy, each dd, mm, yyyy numeric
    // More advanced checks could parse, check day range, leap year, etc.
    // For example:
    // - Must have two '/' separators
    // - dd, mm, yyyy must be integers
    // - dd, mm must be 2 digits, yyyy can be 4 digits, etc.

    // A simple quick check might be:
    // 1. The string must have exactly 10 characters: dd/mm/yyyy
    // 2. The slash positions are date[2] == '/', date[5] == '/'
    // 3. All other positions are digits.

    if (date.size() != 10)
        return false;
    if (date[2] != '/' || date[5] != '/')
        return false;

    // Check digits in dd, mm, yyyy
    for (size_t i = 0; i < date.size(); ++i)
    {
        if (i == 2 || i == 5)
            continue; // skip '/'
        if (!std::isdigit(static_cast<unsigned char>(date[i])))
            return false;
    }

    // (Optional) Range check:
    int day = std::stoi(date.substr(0, 2));
    int month = std::stoi(date.substr(3, 2));
    int year = std::stoi(date.substr(6, 4));
    if (day < 1 || day > 31)
        return false;
    if (month < 1 || month > 12)
        return false;
    // Year range check not strictly necessary, but you could do:
    // if (year < 1900 || year > 2100) return false;

    return true;
}

static Row::Value convertValue(const std::string &str, DataType type)
{
    switch (type)
    {
    case DataType::INT:
        try
        {
            return std::stoi(str);
        }
        catch (...)
        {
            throw std::runtime_error("Failed to convert '" + str + "' to INT");
        }
    case DataType::FLOAT:
        try
        {
            return std::stof(str);
        }
        catch (...)
        {
            throw std::runtime_error("Failed to convert '" + str + "' to FLOAT");
        }
    case DataType::TEXT:
    case DataType::DATE:
        return str;
    default:
        throw std::runtime_error("Unsupported DataType encountered");
    }
}