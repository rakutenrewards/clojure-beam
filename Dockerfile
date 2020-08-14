FROM clojure
COPY . /usr/src/app
WORKDIR /usr/src/app

ARG GITHUB_ACTOR
ARG GITHUB_TOKEN

ENV GITHUB_ACTOR=${GITHUB_ACTOR} \
    GITHUB_TOKEN=${GITHUB_TOKEN}

RUN lein deps
CMD ["lein", "repl", ":headless"]
