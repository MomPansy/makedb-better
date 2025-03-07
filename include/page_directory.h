#pragma once
#include <cstdint>
#include <vector>
#include <string>
#include <cstring>
#include "IStorage.h"
#include "ILogger.h"
#include "global_logger.h"
#include "page_size.h"

// The header of the page directory contains metadata for managing pages and rows.
struct PageDirectoryHeader
{
    uint32_t num_pages;    // Total number of pages in the directory.
    uint32_t next_page_id; // The ID to be assigned to the next new page.
    uint32_t num_rows;     // Total number of rows stored across pages.
    uint32_t next_row_id;  // The ID to be assigned to the next new row.
};

// Each page directory entry contains the page id and the available space in that page.
struct PageDirectoryEntry
{
    uint16_t page_id;
    uint16_t available_space;
};

class PageDirectory
{
public:
    // Default logger provided by GlobalLogger::instance() declared here only.
    PageDirectory(const std::string &tableName, IStorage &storage, ILogger &logger = GlobalLogger::instance()) : storage_(storage),
                                                                                                                 filename_(tableName + "/pagedirectory.dat"),
                                                                                                                 pagefilename_(tableName + "/pages.dat"),
                                                                                                                 logger_(logger),
                                                                                                                 header_{0, 0, 0, 0} {

                                                                                                                 };

    void initialize();
    uint16_t getAndIncrementNextPageId();
    void persistPageDirectory();
    void updatePageDirectoryEntry(PageDirectoryEntry &entry);
    void addPageDirectoryEntry(PageDirectoryEntry &entry);
    PageDirectoryEntry *getPageDirectoryEntry(uint16_t pageId);
    PageDirectoryEntry *getPageDirectoryBySize(uint16_t size);
    void loadPage(PageDirectoryEntry &entry, char *buffer);

private:
    std::string filename_;
    std::string pagefilename_;
    IStorage &storage_;
    ILogger &logger_;
    std::vector<PageDirectoryEntry> entries_;
    PageDirectoryHeader header_;
};
