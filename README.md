# Web-scale Data Management Project Template

Basic project structure with Python's Flask and Redis.
**You are free to use any web framework in any language and any database you like for this project.**

### Project structure

-   `env`
    Folder containing the Redis env variables for the docker-compose deployment
-   `helm-config`
    Helm chart values for Redis and ingress-nginx
-   `k8s`
    Folder containing the kubernetes deployments, apps and services for the ingress, order, payment and stock services.
-   `order`
    Folder containing the order application logic and dockerfile.
-   `payment`
    Folder containing the payment application logic and dockerfile.

-   `stock`
    Folder containing the stock application logic and dockerfile.

-   `test`
    Folder containing some basic correctness tests for the entire system. (Feel free to enhance them)

### Deployment types:

#### docker-compose (local development)

After coding the REST endpoint logic run `docker-compose up --build` in the base folder to test if your logic is correct
(you can use the provided tests in the `\test` folder and change them as you wish).

**_Requirements:_** You need to have docker and docker-compose installed on your machine.

#### minikube (local k8s cluster)

This setup is for local k8s testing to see if your k8s config works before deploying to the cloud.
First deploy your database using helm by running the `deploy-charts-minicube.sh` file (in this example the DB is Redis
but you can find any database you want in https://artifacthub.io/ and adapt the script). Then adapt the k8s configuration files in the
`\k8s` folder to mach your system and then run `kubectl apply -f .` in the k8s folder.

**_Requirements:_** You need to have minikube (with ingress enabled) and helm installed on your machine.

#### kubernetes cluster (managed k8s cluster in the cloud)

Similarly to the `minikube` deployment but run the `deploy-charts-cluster.sh` in the helm step to also install an ingress to the cluster.

**_Requirements:_** You need to have access to kubectl of a k8s cluster.

### Phase 2 - Implement transactional protocol
Implemented SAGAs choreography that ensures transactions check for the availablility of stock and funds before executing an order. When one of the services fails, changes in the database are rolled back and hte order is cancelled.

### Phase 3 - Testing fault tolerance
With docker compose, implemented redundancy for each service, periodic health checks for each service and database, and restart policiest to ensure that services continue running.

`docker-compose up -d`

`docker-compose ps`

`docker kill wdm24-4-order-service-1-1`

`docker start wdm24-4-order-service-1-1`

`sleep 30`

`docker-compose ps`

`docker-compose logs -f`
