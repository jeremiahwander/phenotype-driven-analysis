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
    libssl-dev \
	default-jdk \
    python3-dev \
    python3-pip

RUN wget https://github.com/TheJacksonLaboratory/LIRICAL/releases/download/v1.3.4/LIRICAL.jar

RUN python3 -m pip install --upgrade pip pandas
