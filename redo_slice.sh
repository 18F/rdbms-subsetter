set -v 
dropdb crime_data_api_medium
createdb crime_data_api_medium
pg_restore -x --no-owner -d crime_data_api_medium --schema-only ~/werk/fbi/raw/uploader/fbi_full_with_indexes.pgdump 
psql -c "drop schema public; alter schema cde rename to public" crime_data_api_medium 
rdbms-subsetter --schema public postgresql://:@/crime_data_api_dev postgresql://:@/crime_data_api_medium 0.1


