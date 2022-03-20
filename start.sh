minikube start
eval `minikube docker-env`

docker run --name pgsql -e POSTGRES_PASSWORD=1234 -d -P postgres:14-alpine
PORT=`docker inspect pgsql --format '{{range $k, $v := .NetworkSettings.Ports}}{{(index $v 0).HostPort}}{{end}}'`

IP=`minikube ip -n minikube`
echo postgresql://postgres@$IP:$PORT/postgres

docker run -it --rm postgres:14-alpine psql -h `docker inspect pgsql --format '{{.NetworkSettings.IPAddress}}'` -U postgres

