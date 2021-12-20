## Cred to access Postgres SQL Server
* Username: postgres
* Password: my_password
* Server Address: localhost
* Database Name: my_database
* Port: 5432

## Setup instructions
* Keep source code in PDE folder in root directory of user
* Execute `bash sql_start.sh`

## To connect data to a visualisation tool
* The port 5432 is exposed to host machine
* This port can be used to connect to the server from any software like Power BI or Tableau

## Design Considerations
* The new data is appened to old table so if you rerun the `main.py` file in container it will add duplicate records
* The password is hardcoded here which should be passed through an external parameter store service
* This read raw data directly from the github repo link shared