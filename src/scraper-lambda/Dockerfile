FROM public.ecr.aws/lambda/python:3.12

COPY scraper-lambda/ ${LAMBDA_TASK_ROOT}

RUN pip install --no-cache-dir -r requirements.txt

COPY common/python/common_utils ${LAMBDA_TASK_ROOT}/common_utils

CMD [ "service_dispatcher.handler" ]
