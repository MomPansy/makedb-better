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
        default:
            throw std::runtime_error("Unsupported data type in schema");
        }
    }
}

void Row::setValue(size_t colIndex, int32_t value)
{
    if (colIndex >= values_.size() || schema_[colIndex].type != DataType::INT)
    {
        throw std::out_of_range("Column index out of range or type mismatch (expected INT)");
    }

    values_[colIndex] = value;
}

void Row::setValue(size_t colIndex, float value)
{
    if (colIndex >= values_.size() || schema_[colIndex].type != DataType::FLOAT)
    {
        throw std::out_of_range("Column index out of range or type mismatch (expected FLOAT)");
    }

    values_[colIndex] = value;
}

void Row::setValue(size_t colIndex, std::string value)
{
    if (colIndex >= values_.size() || schema_[colIndex].type != DataType::TEXT)
    {
        throw std::out_of_range("Column index out of range or type mismatch (expected TEXT)");
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

std::vector<char> Row::serialize() const
{
    std::vector<char> buffer;
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
        default:
            throw std::runtime_error("Unsupported data type in schema");
        }
    }
    return size;
}