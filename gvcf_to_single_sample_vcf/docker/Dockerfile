FROM bitnami/minideb:stretch

RUN install_packages \
    ca-certificates \
    wget \
    curl \
    unzip \
    bzip2 \
    zlib1g-dev \
    libcurl4-openssl-dev \
    libbz2-dev \
    liblzma-dev \
    libssl-dev

# install java-8    (adds 340Mb)
RUN install_packages default-jdk

# install python3.7 (adds 800Mb)
RUN apt-get update \
	&& apt-get dist-upgrade -y \
	&& apt-get install -y python-smbus libncursesw5-dev libgdbm-dev libc6-dev zlib1g-dev libsqlite3-dev libssl-dev openssl libffi-dev

ENV PYTHON_VERSION="3.7.1"
RUN wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tar.xz \
	&& tar xvf Python-${PYTHON_VERSION}.tar.xz \
	&& rm Python-${PYTHON_VERSION}.tar.xz \
	&& cd Python-${PYTHON_VERSION} \
	&& ./configure --enable-optimizations \
	&& make install \
	&& make clean

# python3 packages
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install --no-cache-dir -U crcmod

# install gcloud
ENV GCLOUD_SDK_VERISON="380.0.0"
RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${GCLOUD_SDK_VERISON}-linux-x86_64.tar.gz \
    && tar xzf google-cloud-sdk-${GCLOUD_SDK_VERISON}-linux-x86_64.tar.gz \
    && /google-cloud-sdk/install.sh -q \
    && /google-cloud-sdk/bin/gcloud components update --quiet

ENV PATH=/google-cloud-sdk/bin:$PATH

# install GATK  --  install in downstream images with:
#     COPY --from=weisburd/base-vcf-tools:latest  /gatk.jar /gatk.jar
ENV GATK_VERSION="4.2.5.0"
RUN wget https://github.com/broadinstitute/gatk/releases/download/${GATK_VERSION}/gatk-${GATK_VERSION}.zip  \
	&& unzip gatk-${GATK_VERSION}.zip \
	&& rm gatk-${GATK_VERSION}.zip \
	&& ln -s gatk-${GATK_VERSION}/gatk-package-${GATK_VERSION}-local.jar /gatk.jar
