FROM clojure
COPY . /usr/src/app
WORKDIR /usr/src/app
ARG JFROG_PASSWORD
RUN lein deps
CMD ["lein", "repl", ":headless"]
