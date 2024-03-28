FROM acdhch/noske:5.66.3-2.223.6-open
LABEL maintainer omar.siam@oeaw.ac.at

ENV CORPLIST diarium
COPY data/verticals/ /var/lib/manatee/data/verticals/diarium/
COPY nosketchengine/diarium /var/lib/manatee/registry/diarium
# Build will not save anything in /var/lib/manatee/data/ as this is defined as VOLUME
USER root
RUN path=$(grep -h ^PATH $MANATEE_REGISTRY/$CORPLIST | sed -e 's/^PATH[^"]*"\(.*\)"$/\1/g'); \
    mkdir -p $path; compilecorp $CORPLIST;\
    mkdir -p /data.precompiled;\
    mv $path /data.precompiled/;\
    chown -R www-data:www-data /data.precompiled
USER www-data
CMD ["/bin/sh", "-c", "mv /data.precompiled/* /var/lib/manatee/data; exec /bin/sh /run_lighttpd.sh"]