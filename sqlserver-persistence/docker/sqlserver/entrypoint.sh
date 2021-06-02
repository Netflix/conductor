#!/use/bin/env bash
set -m
/opt/mssql/bin/sqlservr &

# Wait for SQLSERVER to become online
while true; do
    timeout 4 bash -c "opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P $MSSQL_SA_PASSWORD -l 3 -t 3 -d master -Q \"SELECT 1\"" 2>/dev/null >/dev/null
    if [ $? -eq 0 ]
    then
        break
    fi
    sleep 3
done
echo ''
echo '### SqlServer is ready...'
echo '### Running initialization script /sqlinit/sql/init.sql'
/opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P $MSSQL_SA_PASSWORD -i /sqlinit/sql/init.sql

fg
