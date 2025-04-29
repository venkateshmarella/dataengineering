import os

source_dir = "src/main/pyspark"
test_dir = "tests/pyspark"

for root, dirs, files in os.walk(source_dir):
    for file in files:
        if file.endswith(".py"):
            source_class = file[:-3]  # Remove the ".py" extension
            test_file = f"test_{source_class}.py"
            if os.path.exists(os.path.join(test_dir, test_file)):
                print(f"Test file for {source_class}: {test_file}")