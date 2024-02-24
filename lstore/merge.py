#each record has a new column called base_rid

'''
load a copy of all base pages of the selected range into memory (this
could be a space outside your normal bufferpool for simplicity). 
'''

def update_record():
    #the process of updating the record

    #tail page stack is a list of stacks
    #the first key is the page range and each page range has a stack of tail pages
    tail_page_merge_stack = [][]

    #if when updating the record the the tail page is full, and we need to create a new tail page
    #Then we append the old tail page (which in this case is stable and not going to be changed anymore) to the tail_page_merge_stack
    tail_page_merge_stack[page_range].append(old_tail_page_pointer)

    #then if we see that the corresponding page range stack has more than 32 tail pages, we merge them
    if tail_page_merge_stack[page_range].length >= 32:
        merge()

def merge():
    
    #because we are only merging one page range, we know that each page range can only correspond to the 128 or 2^7 base pages
    #this amount is small enough to be loaded into memory
    #load a copy of all of the basePage of the page range into memory
    basePageCopy = page_range['corresponding page range'].base_page
    
    #we have a visied hashmap to keep track of the base pages that we have already merged
    visited = []

    #we iterate through 32 tail pages in the stack
    while tail_page_merge_stack.length > 0:

        #we get the latest tail_page_pointer to merge
        curr_tail_page_pointer = tail_page_merge_stack.pop()

        #we get the tail page itself
        tail_page = getTailPage(curr_tail_page_pointer)

        #we get the latest tail page record of the tail page
        last_tail_page_record = tail_page.size - 1

        #we iterate through the tail page records from the latest to the oldest (last index to 0)
        for tail_page_record_idx in range(last_tail_page_record, 0, -1):

            #we get the rid of the tail page record
            curr_tail_record_RID = parse(tail_page_record_idx)
            #we get the base page rid of the tail page record
            base_rid = getRecord(curr_tail_record_RID).base_rid #should be quite fast because we know the physical address of the record

            #if we have already merged the base page for this specific rid, we skip it. This is because we iterate from the latest tail record
            #so we don't need to worry about the older tail records
            if visited[base_rid] == true:
                continue

            # if the tail record is cumulative (a.k.a has the latest data in its data_columns) + End Time
            # we directly update all of the data columns and end time of the base page
            basePageCopy[baseRID].data_columns = getRecord(curr_tail_record_RID).data_columns

            # if the tail record is NOT CUMULATIVE
            # Iterate through all of the back pointers up to the base page and the update it accordingly
            while curr_tail_record_RID is not base_page:
                #the current tail record
                tail_record = getRecord(curr_tail_record_RID)
                #find out which data column is updated
                #say the data is [null, 1, null, null] then
                #update the second column in the base page
                #flag that the second column in the schema is changed and
                #do not update the second column anymore
                for i in range(len(data)):
                    if data[i] is not None:
                        column_schema = tail_record.schema & (1 << i)
                        if column_schema is 1:
                            # the updated column in schema is set to 0
                            schema &= 0 << i
                            # the corresponding data column is updated
                            basePageCopy.data_columns[i] = data[i]
                            # this prevents old value to be written in the merged base page
                #Get the next tail record
                curr_tail_record_RID = getRecord(curr_tail_record_RID).indirection_pointer

            #flag the base page as visited
            visted[baseRID] = true
            #update the tail page sequence of the base page to the latest tail record that has been merged
            basePageCopy[baseRID].tailPageSequence = curr_tail_record_RID

    #deallocate the old base pages
    pageDirectory[page_range][base_page_pointer] = basePageCopy


