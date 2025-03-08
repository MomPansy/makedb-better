#include "parser.h"

DataObject Parser::parseFile(const std::string &filename, char delimiter, const std::vector<Column> &columns)
{
    std::ifstream infile(filename);
    if (!infile.is_open())
    {
        throw std::runtime_error("Failed to open file: " + filename);
    }

    std::string line;

    // read and split the header line
    if (!std::getline(infile, line))
    {
        throw std::runtime_error("File: " + filename + " is empty");
    }

    std::vector<std::string> fileHeader = split(line, delimiter);
    if (fileHeader.size() != columns.size())
    {
        throw std::runtime_error("Header column count mismatch. File has " +
                                 std::to_string(fileHeader.size()) + " columns, but schema defines " +
                                 std::to_string(columns.size()));
    }
    for (size_t i = 0; i < columns.size(); i++)
    {
        if (fileHeader[i] != columns[i].name) {
            throw std::runtime_error("Header column name mismatch at index " + std::to_string(i) +
                                     ". Expected: " + columns[i].name + ", got: " + fileHeader[i]);
        }
    }

    std::vector<std::vector<std::string>> allRows;
    std::vector<std::vector<char>> serializedRows;

    size_t serializedDataSize = 0;
    size_t numRows = 0;

    while (std::getline(infile, line))
    {
        // skip empty lines
        if (line.empty())
            continue;

        // split row into its fields
        std::vector<std::string> fields = split(line, delimiter);
        if (fields.size() != columns.size())
        {
            logger_.log("Data row has unexpected number of columns: " + line);
            continue;
        }

        // save the raw string data
        allRows.push_back(fields);
        std::vector<Row::Value> convertedValues;
        for (size_t i = 0; i < columns.size(); i++)
        {
            try
            {
                convertedValues.push_back(Row::convertValue(fields[i], columns[i].type));
            }
            catch (const std::exception &e)
            {
                logger_.log("Failed to convert value at row " + std::to_string(allRows.size()) + ", column " + std::to_string(i) + ": " + e.what());
                continue;
            }
        }

        try
        {
            Row row(columns, convertedValues);
            std::vector<char> serializedRow = row.serialize();
            serializedDataSize += row.getSerializedSize();
            serializedRows.push_back(serializedRow);
            numRows++;
        }
        catch (const std::exception &e)
        {
            logger_.log("Failed to serialize row " + std::to_string(allRows.size()) + ": " + e.what());
            continue;
        }
    }
    
    DataObject data;
    data.serializedData = serializedRows;
    data.serializedDataSize = serializedDataSize;
    data.numRows = numRows;
    return data;
};
