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
    default-jdk

# install GATK
ENV GATK_VERSION="4.2.0.0"
RUN wget https://github.com/broadinstitute/gatk/releases/download/${GATK_VERSION}/gatk-${GATK_VERSION}.zip  \
    && unzip gatk-${GATK_VERSION}.zip \
    && rm gatk-${GATK_VERSION}.zip \
    && ln -s gatk-${GATK_VERSION}/gatk-package-${GATK_VERSION}-local.jar /gatk.jar

RUN wget https://github.com/brentp/slivar/releases/download/v0.2.7/slivar \
    && chmod +x ./slivar \
    && wget https://raw.githubusercontent.com/brentp/slivar/master/js/slivar-functions.js

RUN install_packages tabix

# wget https://slivar.s3.amazonaws.com/gnomad.hg38.genomes.v3.fix.zip
    
# install gcloud
#RUN python3 -m pip install --upgrade pip pandas

#ENV GCLOUD_SDK_VERISON="373.0.0"
#RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${GCLOUD_SDK_VERISON}-linux-x86_64.tar.gz \
#    && tar xzf google-cloud-sdk-${GCLOUD_SDK_VERISON}-linux-x86_64.tar.gz \
#    && /google-cloud-sdk/install.sh -q \
#    && /google-cloud-sdk/bin/gcloud components update --quiet
#
#ENV PATH=/google-cloud-sdk/bin:$PATH
