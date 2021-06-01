# Spark submit client plugin for Pipeline CI/CD

This repo contains a plugin that can be used to set up a `spark-submit` step in the Banzai Cloud [Pipeline](https://github.com/banzaicloud/pipeline) CI/CD workflow.

>For better understanding of the Banzai Pipeline CI/CD workflow and PaaS please check [this](https://github.com/banzaicloud/pipeline/README.md) documentation

This plugin executes a fully configurable `spark-submit` step, as described [here](https://spark.apache.org/docs/latest/submitting-applications.html).

The plugin supports all the configuration options available for the spark-submit command.
> Please note that the plugin is primarily intended to be used on Kubernetes clusters thus some configuration is automatically taken from items provided by the k8s cluster (eg.: --master)

In the Banzai Cloud Pipeline CI/CD flow definition the `spark-submit` step description may have three configuration sections (reflected by the names of the step elements):

 * `spark_submit_options` - the configuration entries that are passed in the form:
```
--[option] value
```

*  `spark_submit_configs` - items passed to the command in the form:
```
--conf [key]=[value]
```

* `spark_submit_app_args` - a collection (list) that is passed to the command "as is" - a space delimited set of entries

>The first two groups of configuration are represented as yaml maps while the last as a yaml list.
All sections are built dynamically, custom configuration options, spark configuration and application arguments can be passed in following the described conventions.

## Usage

For using the plugin please configure the `.pipeline.yml` properly, and let the magic happen.

If you need help configuring the `yml` please read the [Readme](https://github.com/banzaicloud/drone-plugin-pipeline-client) of the related plugin, which handles the cluster related operations.


## Examples

### Spark-Pi

```
run:
   image: banzaicloud/plugin-k8s-proxy:latest
   pull: true
   service_account: spark

   original_image: banzaicloud/plugin-spark-submit-k8s:latest
   spark_submit_options:
     class: banzaicloud.SparkPi
     kubernetes-namespace: default
   spark_submit_configs:
     spark.app.name: sparkpi
     spark.local.dir: /tmp/spark-locals
     spark.kubernetes.driver.docker.image: banzaicloud/spark-driver:v2.2.0-k8s-1.0.197
     spark.kubernetes.executor.docker.image: banzaicloud/spark-executor:v2.2.0-k8s-1.0.197
     spark.kubernetes.initcontainer.docker.image: banzaicloud/spark-init:v2.2.0-k8s-1.0.197
     spark.dynamicAllocation.enabled: "true"
     spark.kubernetes.resourceStagingServer.uri: http://spark-rss:10000
     spark.kubernetes.resourceStagingServer.internal.uri: http://spark-rss:10000
     spark.shuffle.service.enabled: "true"
     spark.kubernetes.shuffle.namespace: default
     spark.kubernetes.shuffle.labels: app=spark-shuffle-service,spark-version=2.2.0
     spark.kubernetes.authenticate.driver.serviceAccountName: spark
     spark.metrics.conf: /opt/spark/conf/metrics.properties
   spark_submit_app_args:
     - target/spark-pi-1.0-SNAPSHOT.jar
     - 1000
```
For the full the configuration file please click [here](https://raw.githubusercontent.com/lpuskas/spark-pi-example/master/.pipeline.yml.gke.template).
