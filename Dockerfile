# Build stage
FROM ruby:3.4.7-slim AS builder

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     build-essential \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY Gemfile Gemfile.lock ./
RUN bundle config set --local frozen true \
  && bundle config set --local without test \
  && bundle install

# Runtime stage
FROM ruby:3.4.7-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends postgresql-client \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Copy gems from builder stage
COPY --from=builder /usr/local/bundle /usr/local/bundle

COPY lib/ lib/
COPY pg-diff ./

CMD [ "/pg-diff" ]