import pandas as pd
import duckdb
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad

# Encryption and Decryption Functions
def encrypt_data(data: pd.DataFrame, key: bytes) -> pd.DataFrame:
    """
    Encrypt sensitive data in a DataFrame using AES encryption.
    """
    if len(key) not in [16, 24, 32]:
        raise ValueError("Key must be 16, 24, or 32 bytes long.")

    cipher = AES.new(key, AES.MODE_ECB)
    encrypted_data = data.copy()

    for col in encrypted_data.columns:
        if col == "ssn":  # Encrypt only the 'ssn' column
            encrypted_data[col] = encrypted_data[col].apply(
                lambda x: cipher.encrypt(pad(x.encode(), AES.block_size)).hex()
            )
    return encrypted_data

def decrypt_data(data: pd.DataFrame, key: bytes) -> pd.DataFrame:
    """
    Decrypt sensitive data in a DataFrame using AES decryption.
    """
    if len(key) not in [16, 24, 32]:
        raise ValueError("Key must be 16, 24, or 32 bytes long.")

    cipher = AES.new(key, AES.MODE_ECB)
    decrypted_data = data.copy()

    for col in decrypted_data.columns:
        if col == "ssn":  # Decrypt only the 'ssn' column
            decrypted_data[col] = decrypted_data[col].apply(
                lambda x: unpad(cipher.decrypt(bytes.fromhex(x)), AES.block_size).decode()
            )
    return decrypted_data

# DuckDB Functions
def load_to_duckdb(conn, data: pd.DataFrame, table_name: str):
    """
    Load a DataFrame into a DuckDB table.
    """
    conn.register(table_name, data)
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {table_name}")

def query_duckdb(conn, query: str):
    """
    Execute a query on a DuckDB database and return the result.
    """
    return conn.execute(query).fetchall()

# Role-Based Access Control (RBAC)
def check_access(role: str, action: str) -> bool:
    """
    Check if a role has permission to perform an action.
    """
    permissions = {
        "admin": {"read": True, "write": True},
        "user": {"read": True, "write": False},
        "viewer": {"read": True, "write": False},
    }
    return permissions.get(role, {}).get(action, False)
