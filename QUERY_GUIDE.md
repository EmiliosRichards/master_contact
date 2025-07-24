# How to Query the Contacts Database

This guide explains how to use the `reporting.py` script to query the contacts database and get information about your contacts.

## Prerequisites

- You must have the required Python packages installed. If you haven't already, run the following command in your terminal:
  ```bash
  pip install -r etl/requirements.txt
  ```
- Ensure your database connection is configured correctly in the `.env` file as described in the `CONNECTION_GUIDE.md`.

## Running the Reporting Script

All commands are run from the root of the project directory. The basic syntax is:

```bash
python etl/scripts/reporting.py [COMMAND]
```

## Available Commands

Here are the most useful commands for querying the database:

### 1. Count Total Contacts

To find out how many contacts are in your database, use the `count-contacts` command.

**Command:**
```bash
python etl/scripts/reporting.py count-contacts
```

**Example Output:**
```
Total number of contacts: 42
```

### 2. View Recent Contacts

To see a list of the most recently added contacts, use the `view-contacts` command.

**Command:**
```bash
python etl/scripts/reporting.py view-contacts
```

By default, this shows the 10 most recent contacts. You can change this with the `--limit` option.

**Example with a limit:**
```bash
python etl/scripts/reporting.py view-contacts --limit 5
```

### 3. Export All Contacts

To export all contacts to an Excel file, use the `export-contacts` command.

**Command:**
```bash
python etl/scripts/reporting.py export-contacts
```

You can specify a different filename with the `--filename` option.

**Example with a custom filename:**
```bash
python etl/scripts/reporting.py export-contacts --filename my_contacts.xlsx
```

### 4. Audit a Specific Contact

To view all the details for a single contact, use the `audit-contact` command with the contact's ID.

**Command:**
```bash
python etl/scripts/reporting.py audit-contact --id [CONTACT_ID]
```

      ```sql
      -- Example 1: View the first 10 contacts
      SELECT * FROM contacts LIMIT 10;

      -- Example 2: Count all contacts
      SELECT COUNT(*) FROM contacts;

      -- Example 3: Find contacts in a specific industry
      SELECT company_name, phone_number, url
      FROM contacts
      WHERE industry = 'Technology';

      -- Example 4: Find all B2B contacts
      SELECT company_name, phone_number
      FROM contacts
      WHERE is_b2b = TRUE;
      ```
**Example:**
```bash
python etl/scripts/reporting.py audit-contact --id 123
```

This will display a full report for the specified contact.

---

## Using a GUI for Easier Querying

For a more interactive and user-friendly way to explore the database, you can use a graphical user interface (GUI) tool. Since you are using VS Code, the recommended approach is to use the official **PostgreSQL** extension.

### Setting Up the VS Code PostgreSQL Extension

1.  **Install the Extension**:
    *   Go to the Extensions view in VS Code (Ctrl+Shift+X).
    *   Search for `PostgreSQL`.
    *   Install the one published by **Microsoft**.

2.  **Connect to the Database**:
    *   Make sure your SSH tunnel is active. You can run this command in a separate terminal:
      ```bash
      ssh -L 5433:localhost:5432 your_ssh_username@your_ssh_host
      ```
    *   In VS Code, open the Command Palette (Ctrl+Shift+P).
    *   Run the command `PostgreSQL: New Query`. This will prompt you to create a connection profile.
    *   Select `Create a new connection profile`.
    *   Enter the following details when prompted, using the credentials from your `.env` file:
        *   **Hostname**: `localhost`
        *   **Port**: `5433`
        *   **Database**: `master_contact`
        *   **Username**: Your database username
        *   **Password**: Your URL-encoded database password

3.  **Query Your Data**:
    *   Once connected, you can see your database, schemas, and tables in the **PostgreSQL** tab in the Explorer sidebar.
    *   To run a query, right-click the database or a table and select `New Query`.
    *   A new SQL editor will open. You can write your queries there, for example:
      ```sql
      SELECT * FROM contacts LIMIT 10;
      ```
    *   To execute the query, press `Ctrl+Enter`. The results will appear in a new tab.

### Alternative GUI Tools

If you prefer a standalone application, here are two popular free options:

*   **DBeaver**: A powerful, free, and open-source universal database tool.
*   **pgAdmin**: The official and feature-rich open-source management tool for PostgreSQL.

The connection details (host, port, user, password, database) will be the same for these tools.