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
    uint16_t offset;
    uint16_t length;
};

struct SlottedPageHeader
{
    uint16_t numSlots;
    uint16_t freeDataOffset;
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

    std::vector<ReturnType> insert(std::vector<Data> serializedData, char *page, size_t pageSize, PageDirectoryEntry &entry);

private:
    ILogger &logger_;
    SlottedPageHeader header_;
};