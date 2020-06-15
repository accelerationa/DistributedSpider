import enum 

class TaskStatus(enum.Enum): 
    new = 'New'
    downloading = 'Downloading'
    done = 'Done'