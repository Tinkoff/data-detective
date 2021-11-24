FROM alpine:3.14

RUN apk add --update --no-cache openssh

RUN mkdir -p /opt \
 && echo "#!/bin/bash\nDUMMY=echo">/opt/common.sh \
 && chmod -R 777 /opt \
 && mkdir /var/run/sshd  \
 && ssh-keygen -A \
 && adduser -D airflow \
 && echo 'airflow:airflow' | chpasswd \
 && sed -i 's/\#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

EXPOSE 22

CMD ["/usr/sbin/sshd", "-D"]
