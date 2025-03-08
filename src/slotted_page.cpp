#include "slotted_page.h"

std::vector<ReturnType> SlottedPage::insert(std::vector<Data> serializedData, std::vector<char> &page, PageDirectoryEntry &entry)
{

    if (!verifyPage(page))
    {
        throw std::runtime_error("Page is corrupted");
    }

    SlottedPageHeader localHeader;
    std::memcpy(&localHeader, page.data(), sizeof(SlottedPageHeader));

    std::vector<ReturnType> results;
    results.reserve(serializedData.size());

    for (auto &d : serializedData)
    {
        size_t rowSize = d.data.size();

        // offset for the next slot entry
        size_t slotDirOffset = sizeof(SlottedPageHeader) + localHeader.numSlots * sizeof(SlotEntry);

        // location for the row’s payload
        size_t dataOffset = localHeader.lastDataOffset - rowSize;

        // Check if there is enough space
        if (dataOffset < slotDirOffset)
        {
            throw std::runtime_error("Not enough space for new row in slotted page.");
        }

        // Copy row data into page
        std::memcpy(page.data() + dataOffset, d.data.data(), rowSize);

        // Create a new slot entry
        SlotEntry newSlot;
        newSlot.offset = static_cast<uint16_t>(dataOffset);
        newSlot.length = static_cast<uint16_t>(rowSize);
        newSlot.id = static_cast<uint32_t>(d.id);

        // Write slot entry to slot directory area
        std::memcpy(page.data() + slotDirOffset, &newSlot, sizeof(SlotEntry));

        // Update the header
        localHeader.numSlots++;
        localHeader.lastDataOffset = static_cast<uint16_t>(dataOffset);

        // Create return info
        ReturnType ret;
        ret.data.id = d.id;
        ret.location.page_id = entry.page_id; // or real page_id
        ret.location.slot_id = static_cast<uint16_t>(localHeader.numSlots - 1);

        results.push_back(ret);
    }

    // Write updated header back
    std::memcpy(page.data(), &localHeader, sizeof(SlottedPageHeader));

    return results;
}

bool SlottedPage::verifyPage(std::vector<char> &buffer)
{
    logger_.log("Checking overall page validity");
    if (buffer.size() != PAGE_SIZE)
    {
        throw std::runtime_error("Invalid page buffer: must be PAGE_SIZE bytes.");
    }

    logger_.log("Reading the header");
    SlottedPageHeader localHeader;
    std::memcpy(&localHeader, buffer.data(), sizeof(SlottedPageHeader));

    logger_.log("Sanity checks on header");
    const uint16_t maxSlots = (PAGE_SIZE - sizeof(SlottedPageHeader)) / sizeof(SlotEntry);
    if (localHeader.numSlots > maxSlots)
    {
        throw std::runtime_error("Corrupt page header: numSlots exceeds possible slot directory capacity.");
    }

    if (localHeader.lastDataOffset > PAGE_SIZE)
    {
        logger_.log("Corrupt page header: freeDataOffset is beyond the page size");
        throw std::runtime_error("Corrupt page header: freeDataOffset is beyond the page size.");
    }

    // For an empty page, we expect no slots and the data area (after the header) to be zero.
    if (localHeader.numSlots == 0)
    {
        if (localHeader.lastDataOffset != PAGE_SIZE)
        {
            throw std::runtime_error("Invalid header for an empty page: lastDataOffset must be PAGE_SIZE. "
                                     "Please ensure the page buffer is properly memset to 0.");
        }
        
        // Check that the data area (after the header) is zeroed.
        bool isDataZeroed = true;
        for (size_t i = sizeof(SlottedPageHeader); i < buffer.size(); ++i)
        {
            if (buffer[i] != 0)
            {
                isDataZeroed = false;
                break;
            }
        }
        if (!isDataZeroed)
        {
            throw std::runtime_error("Empty page data area not properly initialized: please memset the data area to 0 with PAGE_SIZE bytes.");
        }
    }
    else
    {
        // For non-empty pages, check that the slot directory does not overlap the free data region.
        size_t slotDirectoryEnd = sizeof(SlottedPageHeader) + localHeader.numSlots * sizeof(SlotEntry);
        if (slotDirectoryEnd > localHeader.lastDataOffset)
        {
            throw std::runtime_error("Invalid header: slot directory (ends at " + std::to_string(slotDirectoryEnd) +
                                       ") overlaps or exceeds free data region (lastDataOffset " + std::to_string(localHeader.lastDataOffset) + ").");
        }
    }
    return true;
}


// int main() {
//     std::cout << "Test 1: Insertion on an empty page\n";

//     // Create a page buffer of PAGE_SIZE bytes.
//     std::vector<char> page(PAGE_SIZE);
//     // Zero out the entire buffer.
//     memset(page.data(), 0, PAGE_SIZE);

//     // Initialize an empty slotted page header.
//     SlottedPageHeader header;
//     header.numSlots = 0;
//     header.lastDataOffset = PAGE_SIZE;
//     memcpy(page.data(), &header, sizeof(SlottedPageHeader));

//     // Create a dummy PageDirectoryEntry (assume page id 1).
//     PageDirectoryEntry pde;
//     pde.page_id = 1;
//     // Available space is from the end of the header to the free area.
//     pde.available_space = header.lastDataOffset - sizeof(SlottedPageHeader);

//     // Create a SlottedPage instance.
//     SlottedPage sp;

//     // Prepare one Data item to insert.
//     Data dataItem;
//     dataItem.id = 1;
//     std::string rowStr = "TestRow"; // for example, 7 bytes
//     dataItem.data.assign(rowStr.begin(), rowStr.end());

//     std::vector<Data> rowsToInsert = { dataItem };

//     // Call insert on the empty page.
//     auto results1 = sp.insert(rowsToInsert, page, pde);

//     // Print header after insertion for debugging.
//     SlottedPageHeader newHeader;
//     memcpy(&newHeader, page.data(), sizeof(SlottedPageHeader));
//     std::cout << "After insertion: numSlots = " << newHeader.numSlots
//               << ", lastDataOffset = " << newHeader.lastDataOffset << "\n";

//     // Debug print: iterate over slot entries.
//     for (size_t i = 0; i < newHeader.numSlots; ++i) {
//         size_t slotDirOffset = sizeof(SlottedPageHeader) + i * sizeof(SlotEntry);
//         SlotEntry slot;
//         memcpy(&slot, page.data() + slotDirOffset, sizeof(SlotEntry));
//         std::cout << "Slot " << i << ": id = " << slot.id 
//                   << ", offset = " << slot.offset 
//                   << ", length = " << slot.length << "\n";
//     }

//     // Check that we have one inserted row, with slot_id 0.
//     if (results1.size() == 1 &&
//         results1[0].data.id == 1 &&
//         results1[0].location.page_id == pde.page_id &&
//         results1[0].location.slot_id == 0) {
//         std::cout << "Test 1 passed.\n";
//     } else {
//         std::cout << "Test 1 failed.\n";
//     }

//     return 0;
// }
