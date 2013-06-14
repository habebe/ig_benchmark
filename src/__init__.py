import db
import db_model
import db_types
import db_report

import operations

def usage(fileName):
    operations.populate()
    for i in operations.operations:
        operation = operations.operations[i]
        operation.usage(fileName)
        pass
    pass
