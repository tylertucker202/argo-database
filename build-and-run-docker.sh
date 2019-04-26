echo `date`
echo "building docker argo-db:1.0"
docker build -t argo-db:1.0 .
echo "running docker as tytu6322"
docker run -u 716031:106254 --net=host -v /data/argovis/argo-database:/usr/src/argo-database/:Z argo-db:1.0
echo "finished running argo-db:1.0"
echo `date`
