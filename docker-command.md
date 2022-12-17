# init docker
## create docker network
``
docker network create default_network
``

# airflow
``
docker-compose up -d --build
``

# spark
``
docker-compose up -d
``

# kafka
``
docker-compose up -d
``

# postgresql
``
docker run --name postgres --network=default_network --hostname postgres -e POSTGRES_PASSWORD=password -p 5433:5432 -d postgres
``

# mysql
``
docker run --name mysql --network=default_network --hostname mysql -e MYSQL_ROOT_PASSWORD=password -p 3307:3306 -d mysql
``