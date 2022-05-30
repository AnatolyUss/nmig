echo Count rows in MySQL and Postgres
echo
mysql -h localhost -u simon -pwhmcc -P3316 db << EOF
select count(*) as MySQL from uuid_test;
EOF
echo
PGPASSWORD=whmcc psql -h 127.0.0.1 --port=5430 -U simon --dbname=db << EOF
select count(*) as Postgres from uuid_test;
EOF
