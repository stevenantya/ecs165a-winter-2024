from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.logger = [] 
        self.logger_counter = 0
        self.transaction_manager = 0 # 0 for fail, 1 for fail&move-on, 2 for success
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            print(query, args) # Query objects
            self.logger_counter = 0
            tupleSelf = tuple([self])
            args = tupleSelf + args

            result = query(*args) #this is executing the query #PASSING TRANSACTION OBJECT ALONG IN THE ARGS TODO: EDIT ALL QUERIES AND TABLES

            latest_log = self.logger[-(self.logger_counter):]
            # write logger to disk
            with open('ECS165/logger.txt', 'a') as log:
                str_log = ','.join(str(e) for e in latest_log)
                log.write(str_log)
                log.write('\n')

            # put the old_metadata and new_metadata into the query_executed

            print(result)
            # LOCK here if you want to do along the way~
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations

        for index, (query, args) in enumerate(reversed(self.queries)):
            if self.logger[index] == False: # if the query has not been executed, then we don't need to do UNDO changes
                continue

            # todo: ALEX do the roll-backs here
            # Keep track what record has been altered
            # ADD UPDATE DELETE
            # MAKING THE ADDED -> DELETE
            # MAKING THE DELETE -> POINT BACK BASE RECORD TO THE LATEST INDIRECTION COLUMNS
            # UPDATE THE -> MAKE THE NEW TAIL RECORD POINT TO NULL, MAKE THE BASE RECORD POINT TO THE OLD TAIL RECORD
            
            # Case 1 - ADD NEW RECORD OK
            if len(self.logger[index]) == 1:
                b_rid = self.logger[index][0]
                query.table.delete_record(b_rid)


            # Case 2 - DELETE RECORD OK

            if len(self.logger[index]) == 2:
                b_rid = self.logger[index][0]
                lrid = self.logger[index][1]
                query.table.update_record()




            # Case 3 - UPDATE RECORD 

            if len(self.logger[index]) == 3:
                b_rid = self.logger[index][0]
                otr = self.logger[index][1]
                ntr = self.logger[index][2]
                query.table

            '''
            Q1(INSERT) -> Q2(DELETE) -> Q2(UPDATE) 

            logger = [[BASE_RID], [BASE_RID, LATEST_TAIL_RID] , [BASE_RID, OLD_TAIL_RID, NEW_TAIL_RID]]
            '''


            #todo: Ethan. Unlock records
            
            #todo: Ethan. Unlock page range -> unfamiliar with page range logic

        return False

    # Transaction: Query1 -> Query2 -> Query3
    # Write a page in bufferpool for each query
    # But we never evict it until we commit
    '''
    if i run this sequentially, all of this will be evicted not at the same time
    '''
    def commit(self):
        #todo: Ethan. Unlock records -> Not sure if below code works/is correct
        
        #todo: Ethan. Unlock page range -> unfamiliar with page range logic

        for query, args in self.queries:
            rid = args[0]
            if query.__name__ == 'delete':
                del self.table.lock_manager[rid]
            elif query.__name__ == 'insert':
                continue
            elif query.__name__ == 'update':
                self.table.lock_manager[rid].release_exclusive_lock
            else:
                self.table.lock_manager[rid].release_shared_lock
        return True

