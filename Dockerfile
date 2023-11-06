FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

USER root
RUN yum install -y jq

USER glue_user
WORKDIR /home/glue_user/workspace/jupyter_workspace/
