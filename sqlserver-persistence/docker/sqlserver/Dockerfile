FROM mcr.microsoft.com/mssql/server

COPY sqlserver/ /sqlinit
USER root
RUN chmod +x /sqlinit/entrypoint.sh
RUN mkdir -p /var/sqlmem && chown -R mssql /var/sqlmem
USER mssql
CMD [ "/bin/bash", "/sqlinit/entrypoint.sh" ]
