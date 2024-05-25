FROM ruby:3.3.1

RUN apt-get update \
  && apt-get install -y --no-install-recommends postgresql-client \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY Gemfile Gemfile.lock ./
RUN bundle install --without test

COPY lib/ lib/
COPY pg-diff ./

CMD [ "/pg-diff" ]