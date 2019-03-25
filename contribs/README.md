## Configuring a static Dynomite cluster

In order to use a static dynomite cluster you must inform the cluster 
topology in the properties file, with host, port, rack, datacenter and 
token and load the ```StaticDynomiteClusterModule``` as configurations.

```properties
#Format is host:port:rack:datacenter:token separated by semicolon

workflow.dynomite.cluster.hosts=host1:port:rack:datacenter:token1;host2:port:rack:datacenter:token2;host3:port:rack:datacenter:token3

conductor.additional.modules=com.netflix.conductor.contribs.StaticDynomiteClusterModule
```

## Configuring cluster affinity

In order to define the rack and datacenter affinity you must use the system properties 
```LOCAL_RACK```, ```LOCAL_DATACENTER``` or ```EC2_AVAILABILITY_ZONE``` and```EC2_REGION```.

```properties
LOCAL_RACK=dc-rack1
LOCAL_DATACENTER=dc
# or
EC2_AVAILABILITY_ZONE=us-east-1c
EC2_REGION=us-east-1
```
