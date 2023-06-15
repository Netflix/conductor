#
# conductor:ui - Netflix Conductor UI
#
FROM node:16-alpine as build-deps
LABEL maintainer="Netflix OSS <conductor@netflix.com>"

# Install the required packages for the node build
# to run on alpine
RUN apk update && apk add --no-cache python3 py3-pip make g++
 
# A directory within the virtualized Docker environment
# Becomes more relevant when using Docker Compose later
WORKDIR /usr/src/app
 
# Copies package.json to Docker environment in a separate layer as a performance optimization
COPY ./ui .
 
# Installs all node packages. Cached unless package.json changes
RUN yarn install \
      && yarn run build
 
FROM nginx:1.12-alpine
COPY --from=build-deps /usr/src/app/build /usr/share/nginx/html
COPY ./docker/ui/default.conf /etc/nginx/conf.d/default.conf
EXPOSE 5000
CMD ["nginx", "-g", "daemon off;"]
