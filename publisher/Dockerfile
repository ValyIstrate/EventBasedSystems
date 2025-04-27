FROM localstack/localstack:latest

ENV AWS_REGION=eu-central-1

COPY entrypoint.sh /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]