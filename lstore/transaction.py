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
        self.queries.append((query, table, args))
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, table, args in self.queries:
            #print(query, args) # Query objects
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

            #print(result)
            # LOCK here if you want to do along the way~
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations

        dummy_t = Transaction()

        for index, log in enumerate(reversed(self.logger)):

            cur_query = self.queries[len(self.logger)-1-index]

            cur_table = cur_query[1]

            # cur_db = cur_table.db


            # todo: ALEX do the roll-backs here
            # Keep track what record has been altered
            # ADD UPDATE DELETE
            # MAKING THE ADDED -> DELETE
            # MAKING THE DELETE -> POINT BACK BASE RECORD TO THE LATEST INDIRECTION COLUMNS
            # UPDATE THE -> MAKE THE NEW TAIL RECORD POINT TO NULL, MAKE THE BASE RECORD POINT TO THE OLD TAIL RECORD
            
            # Case 1 - ADD NEW RECORD OK
            if len(log) == 1:
                b_rid = log[0]

                cur_table.delete_record(dummy_t, b_rid)

            # Case 2 - DELETE RECORD OK

            if len(log) == 2:
                b_rid = log[0]
                l_tid = log[1]
                cur_table.undo_delete(b_rid, l_tid)

            # Case 3 - UPDATE RECORD 

            if len(log) == 3:
                b_rid = log[0]
                o_tid = log[1]
                n_tid = log[2]

                cur_table.undo_update(b_rid, o_tid, n_tid)


            '''
            Q1(INSERT) -> Q2(DELETE) -> Q3(UPDATE) 

            logger = [[BASE_RID], [BASE_RID, LATEST_TAIL_RID] , [BASE_RID, OLD_TAIL_RID, NEW_TAIL_RID]]
            '''


        #todo: Ethan. Unlock records
            
        #todo: Ethan. Unlock page range -> unfamiliar with page range logic
        for query_tuple in self.queries:
            # self.queries.append((query, table, args))
            args = query_tuple[2]
            query = query_tuple[0]
            rid = args[0]
            
            if query.__name__ == 'delete':
                del self.table.lock_manager[rid]
            elif query.__name__ == 'insert':
                continue
            elif query.__name__ == 'update':
                self.table.lock_manager[rid].release_exclusive_lock
            else:
                self.table.lock_manager[rid].release_shared_lock

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

