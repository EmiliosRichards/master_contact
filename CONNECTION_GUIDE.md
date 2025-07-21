# How to Connect to the Database

The ETL pipeline connects to a PostgreSQL database. Because the database is not publicly accessible, you must first establish a secure connection using an SSH tunnel.

## Connection Flow

```mermaid
graph TD
    subgraph Your Local Machine
        A[ETL Application] --> B[localhost:5433];
    end

    subgraph Remote Server
        D[PostgreSQL Database];
    end

    B -- SSH Tunnel --> C[Remote Host:5432];
    C --> D;

    style A fill:#cde4f9,stroke:#333,stroke-width:2px
    style D fill:#f9d4c2,stroke:#333,stroke-width:2px
```

## Steps to Connect

1.  **Establish the SSH Tunnel**:
    You need to forward a local port (e.g., `5433`) to the PostgreSQL port (`5432`) on the remote server. Open a terminal and run the following command:

    ```bash
    ssh -L 5433:localhost:5432 emilios@173.249.24.215
    ```

    Alternatively, you can now run the `connect_db.sh` script, which automates this for you.

    Keep this terminal window open. The tunnel will remain active as long as the SSH session is running.

2.  **Run the Application**:
    Once the tunnel is active, the application can connect to the database. The connection string in the `.env` file is already configured to use `localhost:5433`:

    `DATABASE_URL="postgresql://emilios:P0stgresContabo%24Admin98%3B@localhost:5433/master_contact"`

    The application, when run, will automatically connect through the tunnel.

## Security Recommendation: Managing Credentials

Storing your database credentials directly in the `.env` file is convenient for local development but is not recommended for production environments. For enhanced security, consider using a dedicated secrets management tool.

### Recommended Approach: HashiCorp Vault

A powerful and widely-used tool for this purpose is [HashiCorp Vault](https://www.vaultproject.io/).

**How it Works:**

1.  **Store Secrets in Vault**: Instead of writing your `DATABASE_URL` in a text file, you would store it securely inside Vault.
2.  **Application Authentication**: The ETL application would authenticate with Vault using a token or another secure method.
3.  **Dynamic Secrets**: Upon successful authentication, Vault would dynamically provide the `DATABASE_URL` to the application at runtime. The credential would not be stored on disk.

This approach ensures that your database credentials are encrypted, access is tightly controlled and audited, and you can easily rotate credentials without changing the application's code.