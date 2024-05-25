FROM ruby:3.3.1

RUN apt-get update -qq && apt-get install -y postgresql-client

COPY Gemfile Gemfile.lock ./
RUN bundle install

COPY lib/ lib/
COPY pg-diff ./

CMD [ "/pg-diff" ]