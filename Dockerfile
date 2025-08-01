FROM ruby:3.4.2-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends postgresql-client \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY Gemfile Gemfile.lock ./
RUN bundle config set --local frozen true \
  && bundle config set --local without test \
  && bundle install

COPY lib/ lib/
COPY pg-diff ./

CMD [ "/pg-diff" ]