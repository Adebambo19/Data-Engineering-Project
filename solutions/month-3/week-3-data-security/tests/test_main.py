import pytest
import duckdb
import pandas as pd
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from scripts.main import encrypt_data, decrypt_data, load_to_duckdb, query_duckdb, check_access

# Fixture for DuckDB connection
@pytest.fixture
def db_connection():
    conn = duckdb.connect(":memory:")  # Use an in-memory DuckDB database for testing
    yield conn
    conn.close()

# Fixture for sample sensitive data
@pytest.fixture
def sample_data():
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "ssn": ["123-45-6789", "987-65-4321", "456-78-9123"]
    })

# Fixture for encryption key
@pytest.fixture
def encryption_key():
    return b"16bytesecretkey!"

# Test data encryption
def test_encrypt_data(sample_data, encryption_key):
    encrypted_data = encrypt_data(sample_data, encryption_key)
    assert isinstance(encrypted_data, pd.DataFrame), "Encrypted data should be a DataFrame."
    assert "ssn" in encrypted_data.columns, "Encrypted data should have an 'ssn' column."
    assert encrypted_data["ssn"][0] != "123-45-6789", "SSN should be encrypted."

# Test data decryption
def test_decrypt_data(sample_data, encryption_key):
    encrypted_data = encrypt_data(sample_data, encryption_key)
    decrypted_data = decrypt_data(encrypted_data, encryption_key)
    assert decrypted_data.equals(sample_data), "Decrypted data should match the original data."

# Test loading encrypted data into DuckDB
def test_load_to_duckdb(db_connection, sample_data, encryption_key):
    encrypted_data = encrypt_data(sample_data, encryption_key)
    load_to_duckdb(db_connection, encrypted_data, "encrypted_data")
    result = db_connection.execute("SELECT * FROM encrypted_data").fetchall()
    assert len(result) == 3, "Encrypted data should be loaded into DuckDB."

# Test querying encrypted data from DuckDB
def test_query_duckdb(db_connection, sample_data, encryption_key):
    encrypted_data = encrypt_data(sample_data, encryption_key)
    load_to_duckdb(db_connection, encrypted_data, "encrypted_data")
    query_result = query_duckdb(db_connection, "SELECT COUNT(*) as total FROM encrypted_data")
    assert query_result[0][0] == 3, "Query should return the correct count."

# Test role-based access control (RBAC)
def test_check_access():
    # Test admin access
    assert check_access("admin", "read"), "Admin should have read access."
    assert check_access("admin", "write"), "Admin should have write access."

    # Test user access
    assert check_access("user", "read"), "User should have read access."
    assert not check_access("user", "write"), "User should not have write access."

    # Test viewer access
    assert check_access("viewer", "read"), "Viewer should have read access."
    assert not check_access("viewer", "write"), "Viewer should not have write access."

# Test error handling for encryption
def test_encrypt_data_error_handling(sample_data):
    with pytest.raises(Exception):
        encrypt_data(sample_data, b"invalidkey")

# Test logging and error handling
def test_logging_and_error_handling(caplog):
    # Mock a function that logs errors
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.error("Test error message")
    assert "Test error message" in caplog.text, "Error message should be logged."