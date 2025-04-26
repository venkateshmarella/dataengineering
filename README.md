# Project: Venkatesh's Coding Playground 🚀

## Directory structure:
```
├── resources
│   ├── transform_sales_data.py
│   ├── normalize_customer_info.py
├── src/main/
│   ├── pyspark/*spark with python based programs*
│   ├── python/*pure python programs*
│   ├── utils/*general utilities*
├── utils/
│   ├── spark_session.py
│   ├── file_utils.py
│── test/
|   |── pyspark/*test cases based in pyspark*
|   |── python/*test cases based in python language*
├── requirements.txt
|── __init__.py
|──  .gitignore
├── README.md
```

This repository contains Python scripts and PySpark jobs for various data engineering tasks, including ETL transformations, file processing, and automation. Each job comes with test cases using `pytest`.

## 📁 Structure
- `python/ pyspark/`: Core data transformation scripts
- `tests/`: Pytest-based test cases
- `utils/`: Reusable utility functions (e.g., Spark session, file helpers)

## 🛠️ Setup
```bash
pip install -r requirements.txt
```

# Happy coding! ✨