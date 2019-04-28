# opensrp-db-conver

There is only 1 file here.

## How to Run
You can either build the code first or just run the code.
`go run couchdb2sql.go`

## Logic
1. The program will read each document one by one from couchdb.
2. The program tries to insert the data to sql database.
3. If there is an error, and the error is because of no table, then the program create the table
4. The program will re-try again to insert the data
5. If there is an error, and the error is because of no field, then the program will alter the table
6. The program will re-try again to insert the data
7. The process step 5 and 6 may execute several time for one error query.

With this logic, we hope the sql database will dynamically changing based on the data in couchdb 