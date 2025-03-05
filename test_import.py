#!/usr/bin/env python3

import sys
print("Python path:")
for p in sys.path:
    print(f"  {p}")

print("\nTrying to import bettensor module:")
try:
    import bettensor
    print("Success importing bettensor")
    print(f"bettensor.__file__: {bettensor.__file__}")
    print(f"dir(bettensor): {dir(bettensor)}")
    
    print("\nTrying to import validator module:")
    try:
        import bettensor.validator
        print("Success importing bettensor.validator")
        print(f"bettensor.validator.__file__: {bettensor.validator.__file__}")
        print(f"dir(bettensor.validator): {dir(bettensor.validator)}")
    except ImportError as e:
        print(f"Failed importing bettensor.validator: {e}")
    
    print("\nTrying to import database module:")
    try:
        import bettensor.validator.utils.database
        print("Success importing database module")
    except ImportError as e:
        print(f"Failed importing database module: {e}")
        
    print("\nTrying to import database_manager:")
    try:
        from bettensor.validator.utils.database.database_manager import DatabaseManager
        print("Success importing DatabaseManager")
    except ImportError as e:
        print(f"Failed importing DatabaseManager: {e}")

except ImportError as e:
    print(f"Failed importing bettensor: {e}") 