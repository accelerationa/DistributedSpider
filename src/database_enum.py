import enum
  
class Database(enum.Enum):
    MySQL = 'mysql'
    Mongo = 'mongo'

class DatabaseStack(enum.Enum):
    Local = 'local'
    Atlas = 'atlas'
    Aws = 'aws'