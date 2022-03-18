FROM ubuntu:20.04

RUN apt-get update \
    && apt-get install --no-install-recommends --yes \
		ca-certificates \
		wget \
		curl \
		unzip \
		bzip2 \
		pkg-config \
		gcc \
		g++ \
		make \
		autoconf \
		zlib1g-dev \
		libcurl4-openssl-dev \
		libbz2-dev \
		liblzma-dev \
		libssl-dev \
		default-jdk \
		python3-dev \
		python3-pip

# install htslib
ENV SAMTOOLS_VERSION="1.13"
RUN wget https://github.com/samtools/htslib/releases/download/${SAMTOOLS_VERSION}/htslib-${SAMTOOLS_VERSION}.tar.bz2 \
	&& tar xjf htslib-${SAMTOOLS_VERSION}.tar.bz2 \
	&& rm htslib-${SAMTOOLS_VERSION}.tar.bz2 \
	&& cd htslib-${SAMTOOLS_VERSION} \
	&& ./configure \
	&& make \
	&& make install \
	&& make clean


# install gcloud
ENV GCLOUD_SDK_VERISON="377.0.0"
RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${GCLOUD_SDK_VERISON}-linux-x86_64.tar.gz \
    && tar xzf google-cloud-sdk-${GCLOUD_SDK_VERISON}-linux-x86_64.tar.gz \
    && /google-cloud-sdk/install.sh -q \
    && /google-cloud-sdk/bin/gcloud components update --quiet

ENV PATH=/google-cloud-sdk/bin:$PATH

RUN python3 -m pip install --upgrade pip

COPY vcf_header.txt /
COPY convert_tsv_to_vcf.py /
