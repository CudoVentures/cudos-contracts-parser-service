FROM ubuntu:18.04

# Update default packages
RUN apt-get update

# Get Ubuntu packages
RUN apt-get install -y \
    build-essential \
    curl \
    git

# Update new packages
RUN apt-get update

# Install Node
RUN curl -sL https://deb.nodesource.com/setup_16.x | bash

RUN apt-get install nodejs -y

# Install Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

RUN echo 'source $HOME/.cargo/env' >> $HOME/.bashrc

# Compile Rust2JSON
WORKDIR /rust/src

RUN git clone https://github.com/taiki-e/syn-serde

WORKDIR /rust/src/syn-serde/examples/rust2json

RUN ~/.cargo/bin/cargo build --release

# Service start

WORKDIR /service

COPY . .

RUN npm install

RUN npm install pm2 -g

CMD ["pm2-runtime", "src/server.js"]