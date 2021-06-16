Start with:
docker-compose up

By default, it will create a volume link from server log to your mysql/log

log: `- ./logs:/app/logs`


Problems?:
Elasticsearch Container Stopped with Exit 78
run `sudo sysctl -w vm.max_map_count = 262144` in terminal (Not docker)
https://github.com/laradock/laradock/issues/1699

Unable to start with mysql 5.6 and below
Flyway has dropped support for 5.6 and below. Use 5.7 and above instead.

