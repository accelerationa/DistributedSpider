def log_and_execute(cursor, query):
        print(query)
        cursor.execute(query)

class TaskDaoInterface:
    def __init__(self, database, table, use_local_database = False):
        self.table = table
        self.database = database
        self.use_local_database = use_local_database

    # Returns task URL. Returns None if there's no unprocessed task.
    def findAndReturnAnUnprocessedTask(self) -> str:
        pass

    def removeTask(self, task_url: str) -> None:
        pass

    def newTask(self, url: str) -> None:
        pass