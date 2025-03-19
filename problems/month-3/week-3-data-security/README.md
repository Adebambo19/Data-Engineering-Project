# Week 3 Project: Data Security with Encryption and RBAC

## Problem Statement
- Encrypt sensitive data in a pipeline to ensure data security.
- Implement **role-based access control (RBAC)** for a cloud database.
- Use **DuckDB** as the database for storing and querying encrypted data.

## Requirements
1. **Encrypt Sensitive Data**:
   - Use encryption algorithms (e.g., AES) to encrypt sensitive data before storing it in the database.
   - Decrypt data when retrieving it for authorized users.

2. **Implement Role-Based Access Control (RBAC)**:
   - Define roles (e.g., admin, user, viewer) and permissions for accessing the database.
   - Restrict access to sensitive data based on user roles.

3. **Store and Query Data in DuckDB**:
   - Load encrypted data into a **DuckDB** database.
   - Create a table schema to store the encrypted data.
   - Ensure data integrity and handle errors (e.g., duplicate entries, schema mismatches).

4. **Query and Analyze Data**:
   - Use DuckDB to query the encrypted data (e.g., filter, aggregate, join).
   - Decrypt data for authorized users before displaying results.

5. **Logging and Monitoring**:
   - Implement logging to track data access and encryption/decryption processes.
   - Monitor access attempts and handle unauthorized access attempts.

6. **Testing**:
   - Write unit tests to validate the encryption, RBAC, and data querying processes.
   - Test data encryption, role-based access, and DuckDB operations.

## Resources
- [DuckDB Documentation](https://duckdb.org/docs/): Official documentation for DuckDB.
- [PyCryptodome Documentation](https://pycryptodome.readthedocs.io/): Documentation for encryption in Python.
- [Pytest Documentation](https://docs.pytest.org/en/stable/): Official documentation for writing tests using `pytest`.
- [Python Logging Documentation](https://docs.python.org/3/library/logging.html): Documentation for implementing logging in Python.
- [Sample Dataset](https://www.kaggle.com/datasets): A resource for finding sample datasets for testing.

## Setup Instructions
1. **Install Dependencies**:
   ```bash
   pip install duckdb pytest pandas pycryptodome
   ```