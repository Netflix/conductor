# Base image alredy creates app folder and installs modules there
FROM frinx/conductor-ui_base:latest

# Copy the ui files onto the image
COPY ./docker/ui/bin /app
COPY ./ui /app/ui

# Copy the files for the server into the app folders
RUN chmod +x /app/startup.sh

# Get and install conductor UI
RUN cd /app/ui \
  && npm run build --server

EXPOSE 5000

CMD [ "/app/startup.sh" ]
ENTRYPOINT ["/bin/sh"]
