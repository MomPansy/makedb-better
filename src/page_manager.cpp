#include "page_manager.h"

bool PageManager::loadPage(PageDirectoryEntry &entry)
{
    initialize();

    logger_.log("Loading page: page_id=" + std::to_string(entry.page_id) + ", available_space=" + std::to_string(entry.available_space));
    // Calculate the offset for the i-th entry.
    std::streampos offset = entry.page_id * PAGE_SIZE;

    // temp page buffer
    std::vector<char> tempBuffer(PAGE_SIZE, 0);

    bool success = storage_.readFile(pageFilePath_, tempBuffer.data(), PAGE_SIZE, offset);
    if (!success)
    {
        logger_.log("Failed to load page: page_id=" + std::to_string(entry.page_id));
        return false;
    }

    try
    {
        slottedPage_.verifyPage(tempBuffer);
    }
    catch (const std::runtime_error &e)
    {
        logger_.log("Failed to verify page: page_id=" + std::to_string(entry.page_id) + ", error=" + e.what());
        return false;
    }

    page_ = tempBuffer;
    logger_.log("Page loaded: page_id=" + std::to_string(entry.page_id) + ", available_space=" + std::to_string(entry.available_space));

    return true;
}

bool PageManager::persistPage(std::vector<char> buffer, PageDirectoryEntry &entry)
{
    // Write the buffer to the storage.
    bool success = storage_.writeFile(pageFilePath_, buffer.data(), PAGE_SIZE, entry.page_id * PAGE_SIZE);
    if (!success)
    {
        logger_.log("Failed to persist page");
        return false;
    }

    return true;
}

bool PageManager::insertData(std::vector<std::vector<char>> &serializedData,
                             const size_t &expectedSerializedDataSize,
                             const size_t &expectedNumRows)
{
    // 1) Initialize the system.
    if (!initialize())
    {
        logger_.log("Initialization failed in insertData.");
        return false;
    }
    logger_.log("Starting insertion of " + std::to_string(expectedNumRows) + " rows.");

    // 2) Convert each serialized row into a Data struct with a unique row ID.
    std::vector<Data> formattedData;
    formattedData.reserve(serializedData.size());
    for (const auto &vec : serializedData)
    {
        Data tempData;
        tempData.id = pageDirectory_.getAndIncrementNextRowId(); // Unique row ID
        tempData.data = vec;
        formattedData.push_back(tempData);
    }
    logger_.log("Assigned row IDs from " + std::to_string(formattedData.front().id) +
                " to " + std::to_string(formattedData.back().id));

    // 3) Calculate total required space.
    size_t requiredSpace = 0;
    for (const auto &d : formattedData)
    {
        // Each row is its data + SlotEntry overhead
        requiredSpace += d.data.size() + sizeof(SlotEntry);
    }
    logger_.log("Total required space for insertion: " + std::to_string(requiredSpace) + " bytes.");

    // 4) Check if an existing page can hold the entire batch in one go.
    PageDirectoryEntry *entry = pageDirectory_.getPageDirectoryBySize(requiredSpace);
    if (entry != nullptr)
    {
        logger_.log("Found existing page with enough space: page_id=" + std::to_string(entry->page_id) +
                    " (avail=" + std::to_string(entry->available_space) + " bytes)");

        // Load the page into our member buffer (page_).
        if (!loadPage(*entry))
        {
            throw std::runtime_error("Failed to load existing page: page_id=" + std::to_string(entry->page_id));
        }

        // Insert all rows into the loaded page_ buffer.
        auto results = slottedPage_.insert(formattedData, page_, *entry);

        // Re-read the final header from page_ to compute leftover space accurately.
        SlottedPageHeader finalHeader;
        std::memcpy(&finalHeader, page_.data(), sizeof(SlottedPageHeader));

        // Calculate the final slot directory end and free space.
        size_t slotDirEnd = sizeof(SlottedPageHeader) + finalHeader.numSlots * sizeof(SlotEntry);
        size_t freeSpace = 0;
        if (slotDirEnd < finalHeader.lastDataOffset)
        {
            freeSpace = finalHeader.lastDataOffset - slotDirEnd;
        }
        logger_.log("After insertion: numSlots=" + std::to_string(finalHeader.numSlots) +
                    ", lastDataOffset=" + std::to_string(finalHeader.lastDataOffset) +
                    ", freeSpace=" + std::to_string(freeSpace));

        // Update entry->available_space to the precise freeSpace.
        entry->available_space = static_cast<uint16_t>(freeSpace);

        // Persist the updated page back to disk.
        if (!persistPage(page_, *entry))
        {
            throw std::runtime_error("Failed to persist updated page: page_id=" + std::to_string(entry->page_id));
        }

        // Update the page directory entry with the new free space.
        pageDirectory_.updatePageDirectoryEntry(*entry);

        // Validate insertion
        if (results.size() != expectedNumRows || expectedSerializedDataSize != requiredSpace)
        {
            logger_.log("Insertion size mismatch (existing page)!");
            return false;
        }
    }
    else
    {
        // 5) No single page can hold all rows at once. We'll create new pages as needed.
        logger_.log("No existing page has enough space. Creating new pages...");

        size_t currentRow = 0;
        size_t totalInserted = 0;

        while (currentRow < formattedData.size())
        {
            // Create a new page directory entry with a fresh page_id and full PAGE_SIZE free.
            uint16_t newPageId = pageDirectory_.getAndIncrementNextPageId();
            PageDirectoryEntry newEntry{newPageId, PAGE_SIZE};
            pageDirectory_.addPageDirectoryEntry(newEntry);

            logger_.log("Created new page: page_id=" + std::to_string(newPageId));

            // Allocate a local buffer for this new page.
            std::vector<char> localPage(PAGE_SIZE, 0);
            SlottedPageHeader emptyHeader = {0, PAGE_SIZE};
            std::memcpy(localPage.data(), &emptyHeader, sizeof(SlottedPageHeader));

            // We'll fill this page with as many rows as fit.
            size_t availableSpace = PAGE_SIZE - sizeof(SlottedPageHeader);
            std::vector<Data> batch;
            size_t pageUsed = 0;

            while (currentRow < formattedData.size())
            {
                size_t rowReq = formattedData[currentRow].data.size() + sizeof(SlotEntry);
                if (pageUsed + rowReq <= availableSpace)
                {
                    batch.push_back(formattedData[currentRow]);
                    pageUsed += rowReq;
                    currentRow++;
                }
                else
                {
                    break;
                }
            }

            logger_.log("Inserting " + std::to_string(batch.size()) +
                        " rows into new page_id=" + std::to_string(newPageId));

            // Insert batch into localPage
            auto results = slottedPage_.insert(batch, localPage, newEntry);
            totalInserted += results.size();

            // Re-read the final header from localPage to compute leftover space
            SlottedPageHeader finalHeader;
            std::memcpy(&finalHeader, localPage.data(), sizeof(SlottedPageHeader));
            size_t slotDirEnd = sizeof(SlottedPageHeader) + finalHeader.numSlots * sizeof(SlotEntry);

            size_t freeSpace = 0;
            if (slotDirEnd < finalHeader.lastDataOffset)
            {
                freeSpace = finalHeader.lastDataOffset - slotDirEnd;
            }
            logger_.log("After insertion (new page): numSlots=" + std::to_string(finalHeader.numSlots) +
                        ", lastDataOffset=" + std::to_string(finalHeader.lastDataOffset) +
                        ", freeSpace=" + std::to_string(freeSpace));

            // Update newEntry.available_space precisely
            newEntry.available_space = static_cast<uint16_t>(freeSpace);
            pageDirectory_.updatePageDirectoryEntry(newEntry);

            // Persist this new page to disk
            if (!persistPage(localPage, newEntry))
            {
                throw std::runtime_error("Failed to persist new page: page_id=" + std::to_string(newPageId));
            }
        }

        // Confirm that we inserted as many rows as expected.
        if (totalInserted != expectedNumRows || expectedSerializedDataSize != requiredSpace)
        {
            logger_.log("Insertion mismatch in new-page branch. Inserted " +
                        std::to_string(totalInserted) + " rows; expected " +
                        std::to_string(expectedNumRows));
            return false;
        }
    }

    // 6) Persist the updated page directory overall
    pageDirectory_.persistPageDirectory();
    logger_.log("Insertion completed successfully. Directory persisted.");

    return true;
}

bool PageManager::initialize()
{
    if (initialized_)
    {
        return true;
    }
    // Load the page directory.
    bool success = pageDirectory_.initialize();
    if (!success)
    {
        logger_.log("Failed to initialize page directory.");
        return false;
    }
    initialized_ = true;
    return true;
}

// Helper function to create a vector-of-vectors of chars with a certain pattern
// std::vector<std::vector<char>> makeTestData(size_t numRows, size_t rowSize, char fillChar) {
//     std::vector<std::vector<char>> data;
//     data.reserve(numRows);
//     for (size_t i = 0; i < numRows; i++) {
//         // rowSize bytes filled with fillChar
//         std::vector<char> row(rowSize, fillChar);
//         data.push_back(std::move(row));
//     }
//     return data;
// }

// int main() {
//     // Clean up any previous test artifacts (optional)
//     // Suppose PageManager writes to "test_table/page.dat" etc. We'll remove them if they exist:
//     std::remove("test_table/page.dat");
//     std::remove("test_table/pagedirectory.dat");

//     // 1) Prepare the environment
//     ConsoleLogger logger; 
//     FileStorage storage(logger);        // or any storage implementing IStorage
//     std::string tableName = "test_table";
    
//     // Create the PageManager
//     PageManager pageManager(tableName, logger, storage);

//     // ---------------------------------------------------
//     // Test 1: Insert a small batch that fits in a single page
//     // ---------------------------------------------------
//     {
//         std::cout << "\n[Test 1] Inserting a small batch that should fit in one page.\n";

//         // Suppose each row is 32 bytes, and we insert 5 rows => total row data = 5 * 32 = 160 bytes
//         // plus overhead (SlotEntry=8 bytes each => 5 * 8 = 40), total ~200 bytes
//         // and PAGE_SIZE is likely 4096, so it fits easily.

//         size_t numRows = 5;
//         size_t rowSize = 32;
//         auto testData = makeTestData(numRows, rowSize, 'A');  // fill with 'A'

//         // Expected total row data
//         size_t rowDataSize = numRows * rowSize;
//         // Overhead for 5 rows => 5 * sizeof(SlotEntry). 
//         // Suppose sizeof(SlotEntry)= 8 bytes (id=4, offset=2, length=2).
//         size_t overhead = numRows * 8; 
//         size_t expectedTotalSize = rowDataSize + overhead;

//         bool success = pageManager.insertData(
//             testData,               // the vector-of-vectors
//             expectedTotalSize,      // expected serializedDataSize
//             numRows                 // expectedNumRows
//         );

//         if (success) {
//             std::cout << "[Test 1] PASSED: Inserted " << numRows 
//                       << " small rows into a single page.\n";
//         } else {
//             std::cout << "[Test 1] FAILED: Insert data returned false.\n";
//         }
//     }

//     // ---------------------------------------------------
//     // Test 2: Insert a larger batch that requires multiple pages
//     // ---------------------------------------------------
//     {
//         std::cout << "\n[Test 2] Inserting a larger batch that should span multiple pages.\n";

//         // For example, each row is 1024 bytes, and we insert 10 rows => total row data= ~10KB
//         // With overhead, it will exceed a single 4096-byte page. We expect multiple pages to be created.
//         size_t numRows = 10;
//         size_t rowSize = 1024; 
//         auto testData = makeTestData(numRows, rowSize, 'B'); // fill with 'B'

//         size_t rowDataSize = numRows * rowSize;
//         size_t overhead = numRows * 8;  // if SlotEntry is 8 bytes
//         size_t expectedTotalSize = rowDataSize + overhead;

//         bool success = pageManager.insertData(
//             testData,
//             expectedTotalSize,
//             numRows
//         );

//         if (success) {
//             std::cout << "[Test 2] PASSED: Inserted " << numRows 
//                       << " large rows across multiple pages.\n";
//         } else {
//             std::cout << "[Test 2] FAILED: Insert data returned false.\n";
//         }
//     }

//     // Optionally add more tests, e.g. partial fits or error scenarios

//     return 0;
// }