#Web Mantis Publish Sample

## Introduction

## Requirements 

## How to run

### Updating docker compose

```bash
mantispublish:
     image: dev/mantispublishweb
     ports:
      - "80:8080"
     depends_on:
         - mantisapi
     networks:
        mantis_net:
          ipv4_address: 172.16.186.8

```