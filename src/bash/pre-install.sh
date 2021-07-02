#!/bin/bash
easy_install pip \
&& apt update \
&& apt install python-pip -y \
&& pip install 'google-cloud-storage==1.25.0' \
&& pip install 'google-cloud-bigquery==1.24.0' \
&& pip install 'SobolSequence==0.2' \
&& pip install 'scipy==1.5.4' \
&& pip install 'pandas==1.1.5' \
&& pip install 'google-cloud-storage==1.38.0' \
&& pip install 'matplotlib==3.3.4'