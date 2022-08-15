FROM python:3.9-buster as prod

ARG POETRY_VERSION=1.1.13
ARG AIRFLOW_USER_UID=50000
ENV POETRY_VERSION=${POETRY_VERSION} \
    POETRY_CORE_VERSION=${POETRY_CORE_VERSION} \
    AIRFLOW_USER_UID=${AIRFLOW_USER_UID} \
    AIRFLOW_HOME=/usr/local/airflow

RUN groupadd -g ${AIRFLOW_USER_UID} airflow \
    && useradd -ms /bin/bash -d $AIRFLOW_HOME -G sudo -u ${AIRFLOW_USER_UID} -g ${AIRFLOW_USER_UID} airflow

USER airflow

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=$PATH:$AIRFLOW_HOME:$AIRFLOW_HOME/dags \
    PATH=${AIRFLOW_HOME}/.local/bin:$PATH \
    POETRY_INSTALLER_PARALLEL=false

WORKDIR $AIRFLOW_HOME
COPY pyproject.toml poetry.lock $AIRFLOW_HOME/
RUN pip install --no-cache-dir "poetry==${POETRY_VERSION}" \
    && poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi \
    && rm -rf .cache/pypoetry

# Custom connections
COPY /docker/init-connections.sh /docker/entrypoint.sh $AIRFLOW_HOME/

EXPOSE 8080 5555

ENTRYPOINT ["./entrypoint.sh"]
CMD ["all-in-one"]

FROM prod as dev

USER root
RUN apt-get update --allow-releaseinfo-change \
 && apt-get install -q -y --no-install-recommends curl openssh-server netcat gcc graphviz graphviz-dev \
 && apt-get install -q -y --no-install-recommends sudo -o DPkg::Options::="--force-confnew" \
 && apt-get clean all \
 && echo 'airflow:airflow' | chpasswd \
 && echo "airflow ALL = NOPASSWD: /usr/sbin/sshd" >> /etc/sudoers.d/airflow \
 && mkdir /var/run/sshd \
 && sed -i 's/\#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
 && sed -i 's/\#PermitUserEnvironment no/PermitUserEnvironment yes/' /etc/ssh/sshd_config \
 && mkdir $AIRFLOW_HOME/.ssh \
 && echo "AIRFLOW_HOME=$AIRFLOW_HOME" > $AIRFLOW_HOME/.ssh/environment \
 && echo "PYTHONPATH=$PATH:$AIRFLOW_HOME:$AIRFLOW_HOME/dags" >> $AIRFLOW_HOME/.ssh/environment \
 && chown -R airflow:airflow $AIRFLOW_HOME
EXPOSE 22

USER airflow

# The python module pygraphviz (only needed at the development stage) requires the graphviz system module, so it is not added to the production image.
# Install via pip so as not to affect pyproject.toml
RUN pip install --no-cache-dir pygraphviz==1.7

RUN mkdir $AIRFLOW_HOME/dags \
 && mkdir $AIRFLOW_HOME/common \
 && mkdir $AIRFLOW_HOME/tests \
 && mkdir $AIRFLOW_HOME/tests_data

VOLUME $AIRFLOW_HOME/dags \
       $AIRFLOW_HOME/common \
       $AIRFLOW_HOME/tests \
       $AIRFLOW_HOME/tests_data
