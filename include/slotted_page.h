#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include "ILogger.h"
#include "global_logger.h"
#include "location.h"
#include "page_size.h"
#include "page_directory.h"

struct SlotEntry
{

    uint32_t id; // id of the row
    uint16_t offset;
    uint16_t length;
};

struct SlottedPageHeader
{
    uint16_t numSlots;
    // freeDataOffset points to the first byte of the last row in the data area
    // for example if the last row starts at byte 100, freeDataOffset will be 100
    uint16_t lastDataOffset;
};

struct Data
{
    uint32_t id;
    std::vector<char> data;
};

struct ReturnType
{
    Data data;
    Location location;
};

class SlottedPage
{
public:
    SlottedPage(ILogger &logger = GlobalLogger::instance()) : logger_(logger) {};

    std::vector<ReturnType> insert(std::vector<Data> serializedData, std::vector<char> &page, PageDirectoryEntry &entry);
    bool verifyPage(std::vector<char> &buffer);

private:
    ILogger &logger_;
    SlottedPageHeader header_;
};