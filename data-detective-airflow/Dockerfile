# Main docker building
ARG PYTHON_VERSION=${PYTHON_VERSION:-3.9}
FROM python:${PYTHON_VERSION}-slim-bullseye as base

ARG AIRFLOW_USER_UID=50000
ENV DEBIAN_FRONTEND=noninteractive \
    TERM=linux \
    AIRFLOW_USER_UID=${AIRFLOW_USER_UID} \
    AIRFLOW_HOME=/usr/local/airflow

RUN apt-get update \
 && apt-get -q -y install sudo -o DPkg::Options::="--force-confnew" \
 && groupadd -g $AIRFLOW_USER_UID airflow \
 && useradd -ms /bin/bash -d ${AIRFLOW_HOME} -G sudo -u ${AIRFLOW_USER_UID} -g ${AIRFLOW_USER_UID} airflow \
 && echo 'airflow:airflow' | chpasswd \
 && apt-get clean

USER airflow
RUN mkdir $AIRFLOW_HOME/data_detective_airflow && chown airflow:airflow ${AIRFLOW_HOME}/data_detective_airflow
ENV PYTHONPATH=$PATH:${AIRFLOW_HOME} \
    PATH=${AIRFLOW_HOME}/.local/bin:${PATH}


# Устанавливаем pip из более полного образа python-image (в основном - slim не достаточно утилит для pip install)
FROM python:${PYTHON_VERSION}-bullseye as builder

ARG AIRFLOW_USER_UID=50000
ARG POETRY_VERSION=${POETRY_VERSION:-1.2.1}

ENV AIRFLOW_HOME=/usr/local/airflow/ \
    AIRFLOW_USER_UID=${AIRFLOW_USER_UID}

RUN groupadd -g ${AIRFLOW_USER_UID} airflow \
  && useradd -ms /bin/bash -d ${AIRFLOW_HOME} -G sudo -u ${AIRFLOW_USER_UID} -g ${AIRFLOW_USER_UID} airflow

USER airflow

ENV POETRY_VERSION=${POETRY_VERSION} \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=120 \
    POETRY_INSTALLER_PARALLEL=false \
    PATH=${AIRFLOW_HOME}/.local/bin:${PATH}

WORKDIR ${AIRFLOW_HOME}
COPY pyproject.toml poetry.lock ${AIRFLOW_HOME}/
RUN pip install --no-cache-dir "poetry==${POETRY_VERSION}" \
    && poetry config virtualenvs.create false \
    && poetry export -f requirements.txt --with dev --output requirements.txt \
    && pip install -r requirements.txt


FROM base as dev

# Копируем установленные в builder pip зависимости
COPY --chown=airflow --from=builder ${AIRFLOW_HOME}/.local ${AIRFLOW_HOME}/.local
COPY --chown=airflow [ "docker/entrypoint.sh", "docker/init-connections.sh", "${AIRFLOW_HOME}/"]

WORKDIR ${AIRFLOW_HOME}

# Установка SSH сервера и возможность подключиться под пользователем airflow
USER root
RUN echo "airflow ALL = NOPASSWD: /usr/sbin/sshd" >> /etc/sudoers.d/airflow \
 && apt-get update --allow-releaseinfo-change\
 && apt-get install -y openssh-server netcat make libpq5\
 && apt-get clean \
 && mkdir /var/run/sshd \
 && sed -i 's/\#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
 && sed -i 's/\#PermitUserEnvironment no/PermitUserEnvironment yes/' /etc/ssh/sshd_config \
 && mkdir ${AIRFLOW_HOME}/.ssh \
 && echo "AIRFLOW_HOME=${AIRFLOW_HOME}" > ${AIRFLOW_HOME}/.ssh/environment \
 && echo "PYTHONPATH=$PATH:${AIRFLOW_HOME}:${AIRFLOW_HOME}/dags" >> ${AIRFLOW_HOME}/.ssh/environment \
 && chown airflow:airflow ${AIRFLOW_HOME}/.ssh/environment

USER airflow

RUN mkdir ${AIRFLOW_HOME}/dags \
 && mkdir ${AIRFLOW_HOME}/tests \
 && mkdir ${AIRFLOW_HOME}/tests_data

VOLUME ${AIRFLOW_HOME}/dags \
       ${AIRFLOW_HOME}/data_detective_airflow \
       ${AIRFLOW_HOME}/tests \
       ${AIRFLOW_HOME}/tests_data

EXPOSE 22 8080 5555
ENTRYPOINT ["./entrypoint.sh"]
CMD ["all-in-one"]
