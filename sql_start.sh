docker-compose -f container.yml up -d
sleep 5 && docker exec -it my_postgres psql -U postgres -c "create database my_database"