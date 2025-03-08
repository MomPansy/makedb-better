#pragma once
#include <string>

#include "slotted_page.h"
#include "page_directory.h"
#include "ILogger.h"
#include "IStorage.h"

class PageManager {
    public:
        PageManager(const std::string &tableName, ILogger &logger, IStorage &storage)
          : pageFilePath_(tableName + "/page.dat"),
            logger_(logger),
            storage_(storage),
            slottedPage_(logger),
            pageDirectory_(tableName, storage, logger),
            initialized_(false)
        {
            // Allocate the page buffer (for one page)
            page_ = std::vector<char>(PAGE_SIZE);
            memset(page_.data(), 0, PAGE_SIZE);
        }
    
        bool loadPage(PageDirectoryEntry &entry);
        bool persistPage(std::vector<char> buffer, PageDirectoryEntry &entry);
        bool insertData(std::vector<std::vector<char>> &serializedData, const size_t &expectedSerializedDataSize, const size_t &expectedNumRows); 
        bool initialize(); 
    
    private:
        std::string pageFilePath_;
        ILogger &logger_;
        IStorage &storage_;
        SlottedPage slottedPage_;
        PageDirectory pageDirectory_;
        bool initialized_; 
        std::vector<char> page_; // Reusable buffer for one page
    };
    