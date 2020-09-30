#Mantis Publish Sample

## Introduction

## Requirements 

## How to run

### Updating docker compose

```bash
mantispublish:
     image: dev/mantispublish
     ports:
      - "8101:8101"
     depends_on:
         - mantisapi
     networks:
        mantis_net:
          ipv4_address: 172.16.186.8

```