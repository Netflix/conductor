FROM mcr.microsoft.com/mssql/server

COPY sqlserver/ /sqlinit
USER root
RUN chmod +x /sqlinit/entrypoint.sh
USER mssql
CMD [ "/bin/bash", "/sqlinit/entrypoint.sh" ]