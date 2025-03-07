#include "page_directory.h"

PageDirectory::PageDirectory(const std::string &tableName, IStorage &storage, ILogger &logger = GlobalLogger::instance())
    : storage_(storage),
      filename_(tableName + "/pagedirectory.dat"),
      pagefilename_(tableName + "/pages.dat"),
      logger_(logger),
      header_{0, 0, 0, 0}
{
    initializeFile();
};

void PageDirectory::initializeFile()
{
    // check if file exists
    logger_.log("Checking if page directory file exists: " + filename_);
    if (storage_.fileExists(filename_))
    {
        logger_.log("Page directory file exists: " + filename_);
        logger_.log("Reading page directory file: " + filename_);
        storage_.readFile(filename_, reinterpret_cast<char *>(&header_), sizeof(header_));
    }
    else
    {
        logger_.log("Page directory file does not exist: " + filename_);
        logger_.log("Creating page directory file: " + filename_);
        storage_.writeFile(filename_, reinterpret_cast<char *>(&header_), sizeof(header_));
    }

    logger_.log("Page directory header: num_pages=" + std::to_string(header_.num_pages) + ", next_page_id=" + std::to_string(header_.next_page_id) + ", num_rows=" + std::to_string(header_.num_rows) + ", next_row_id=" + std::to_string(header_.next_row_id));
    // load the page directory entries into memory
    for (size_t i = 0; i < header_.num_pages; i++)
    {
        PageDirectoryEntry entry;
        // Calculate the offset for the i-th entry.
        std::streampos offset = sizeof(header_) + i * sizeof(entry);
        storage_.readFile(filename_, reinterpret_cast<char *>(&entry), sizeof(entry), offset);
        entries_.push_back(entry);
    }
}

uint16_t PageDirectory::getAndIncrementNextPageId()
{
    uint16_t nextPageId = header_.next_page_id;
    header_.next_page_id++;
    return nextPageId;
}

void PageDirectory::persistPageDirectory()
{
    logger_.log("Persisting page directory to file: " + filename_);
    // Compute total size: header + all entries.
    size_t totalSize = sizeof(header_) + entries_.size() * sizeof(PageDirectoryEntry);

    std::vector<char> buffer(totalSize);
    std::memcpy(buffer.data(), &header_, sizeof(header_));

    // Copy each entry.
    for (size_t i = 0; i < entries_.size(); i++)
    {
        std::memcpy(buffer.data() + sizeof(header_) + i * sizeof(PageDirectoryEntry),
                    &entries_[i],
                    sizeof(PageDirectoryEntry));
    }
    try
    {
        storage_.writeFile(filename_, buffer.data(), totalSize);
    }
    catch (const std::exception &e)
    {
        logger_.log("Failed to persist page directory to file: " + filename_ + ", error: " + e.what());
    }
}

void PageDirectory::updatePageDirectoryEntry(PageDirectoryEntry &entry)
{
    logger_.log("Updating page directory entry: page_id=" + std::to_string(entry.page_id) + ", available_space=" + std::to_string(entry.available_space));
    for (size_t i = 0; i < entries_.size(); i++)
    {
        if (entries_[i].page_id == entry.page_id)
        {
            entries_[i] = entry;
            return;
        }
    }
    entries_.push_back(entry);
    persistPageDirectory();
}

void PageDirectory::addPageDirectoryEntry(PageDirectoryEntry &entry)
{
    logger_.log("Adding page directory entry: page_id=" + std::to_string(entry.page_id) + ", available_space=" + std::to_string(entry.available_space));
    entries_.push_back(entry);
    persistPageDirectory();
}

PageDirectoryEntry *PageDirectory::getPageDirectoryEntry(uint16_t pageId)
{
    for (size_t i = 0; i < entries_.size(); i++)
    {
        if (entries_[i].page_id == pageId)
        {
            logger_.log("Page directory entry: page_id=" + std::to_string(entries_[i].page_id) + ", available_space=" + std::to_string(entries_[i].available_space));
            return &entries_[i];
        }
    }
    logger_.log("Page directory entry not found: page_id=" + std::to_string(pageId));
    return nullptr;
}

PageDirectoryEntry *PageDirectory::getPageDirectoryBySize(uint16_t size)
{
    for (size_t i = 0; i < entries_.size(); i++)
    {
        if (entries_[i].available_space >= size)
        {
            logger_.log("Page directory entry: page_id=" + std::to_string(entries_[i].page_id) + ", available_space=" + std::to_string(entries_[i].available_space));
            return &entries_[i];
        }
    }
    logger_.log("Page directory entry not found for size: " + std::to_string(size));
    return nullptr;
}

// move this to Page Manager
void PageDirectory::loadPage(PageDirectoryEntry &entry, char *buffer)
{
    logger_.log("Loading page: page_id=" + std::to_string(entry.page_id) + ", available_space=" + std::to_string(entry.available_space));
    // Calculate the offset for the i-th entry.
    std::streampos offset = entry.page_id * PAGE_SIZE;
    storage_.readFile(pagefilename_, buffer, PAGE_SIZE, offset);
    logger_.log("Page loaded: page_id=" + std::to_string(entry.page_id) + ", available_space=" + std::to_string(entry.available_space));
}
