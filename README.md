# clickhouse_utils
python functions to streamline using clickhouse, such as automating table creation.
> **Version 0.1.0**

## Getting Started

To start, first clone the repo to your local machine using either: `git clone git@github.com:rogerjbos/clickhouse_utils.git` if you have SSH keys set up on linked github account or alternatively `git clone https://github.com/rogerjbos/clickhouse_utils.git` if you don't.

You will need the git package installed on you machine for this.

Next you'll want to navigate to the project directory using `cd clickhouse_utils`, then install project dependencies with `pip install -r requirements.txt`.  Finally, run `pip install -e .`.

After this, navigate to the example.env file and save it as `.env` and input the necessary values.

```
CLICKHOUSE_HOST='localhost'
CLICKHOUSE_USER='user name'
CLICKHOUSE_PASSWORD='user password'
CLICKHOUSE_DEFAULT_PASSWORD='default user password'
```
Note that there are two passwords listed in the environment file.  The user password is the one associated with the user name, whereas the default password is the one associated with the default username, which will be used for high-level database operations like creating databases and granting user permissions.

## Examples

#### Creating a new database
```
create_database("my_db", default_pw)
```

#### Granting common rights to user for user bob on database my_db
This should cover most operations: SELECT, CREATE, DROP, ALTER, OPTIMIZE, and INSERT
```
grant_admin("my_db", "bob", default_pw)
```

#### Granting read-only rights to user for user bob on database my_db
This should cover read-only operations: SELECT
```
grant_read_only("my_db", "bob", default_pw)
```
#### Show all databases
```
show_databases("my_db")
```

#### Show all tables
```
show_tables("my_db")
```

### Save DataFrame to a table (creating the table if necessary)
```
save(data_frame=df, table='my_table', primary_keys='date, name', append=True)
```
This is the key function of this library, since clickhouse does not automatically create tables.  This functions assumes you want a `ReplicatingMergeTree` table, so it is important to provide the proper primary keys so that the table will be set up correctly.  If you desire a different type of Engine, you code can easily be modified to add that.

Using `append=True` (the default) will add the passed DataFrame to the existing table or create one if it doesn't already exist.  Using `append=False` will drop the current table (if it exists) and create a new table, so be careful with this setting.

### test

test 2