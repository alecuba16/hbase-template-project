Depending on the configuration of your HBASE cluster, it may require the modification of the /etc/hosts or windows /windows/system32/driver/etc/hosts
for providing the required dns resolution of the HBASE region server, zookeeper etc.

In my case I'm using the docker composer configuration of my repo (https://github.com/alecuba16/hbase-docker-composer-apple-silicon-m1) and configured the hosts to:

```
127.0.0.1 hbase-master
127.0.0.1 hbase-region
127.0.0.1 hbase-master.hbase-docker-composer-apple-silicon-m1_hbase
127.0.0.1 hbase-regionserver.hbase-docker-composer-apple-silicon-m1_hbase
127.0.0.1 zoo
```