#include <string>
#include <variant>
#include <vector>
#include <cstdint>

#include "schema.h"

class Row
{
public:
    using Value = std::variant<int32_t, float, std::string>;
    explicit Row(const std::vector<Column> &row, const std::vector<Value> &values = {});

    void setValue(size_t colIndex, const int32_t &value);
    void setValue(size_t colIndex, const float &value);
    void setValue(size_t colIndex, const std::string &value);
    void setDate(size_t colIndex, const std::string &value);

    int32_t getInt(size_t colIndex) const;
    float getFloat(size_t colIndex) const;
    std::string getText(size_t colIndex) const;
    std::string getDate(size_t colIndex) const;

    static Value convertValue(const std::string &value, DataType type);


    // Serializes the row into a contiguous byte buffer
    // INT: raw uint16_t bytes
    // FLOAT: raw float bytes
    // TEXT: uint16_t length followed by the string bytes
    std::vector<char> serialize() const;

    // returns the size (in bytes) that the row will occupy when serialized

    size_t getSerializedSize() const;

private:
    std::vector<Column> schema_;
    std::vector<Value> values_;
    bool isValidDateFormat(const std::string &date) const;

};