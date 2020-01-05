sed -i -e "s|server-2019.0.3|server-${PREVIEW_VERSION}|" ./netflix-conductor/values.yaml
sed -i -e "s|rep: x|repository: docker.elastic.co/elasticsearch/elasticsearch|" ./preview/values.yaml
sed -i -e "s|t: y|tag: 5.6.16|" ./preview/values.yaml