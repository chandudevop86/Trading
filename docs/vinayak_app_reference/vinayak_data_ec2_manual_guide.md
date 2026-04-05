# Vinayak Data EC2 Manual Guide

## Purpose

This guide covers the manual setup of the Data EC2 in the 3-EC2 Vinayak topology.

## Responsibilities

The Data EC2 runs:
- PostgreSQL
- Redis
- RabbitMQ

## PostgreSQL

Create:
- database `vinayak`
- user `vinayak`
- strong password

Bind PostgreSQL to the private interface only.

## Redis

Bind Redis to the private interface only.
Do not expose Redis publicly.

## RabbitMQ

Create a dedicated user such as `vinayak`.
Do not rely on guest credentials outside isolated local use.

## Required Ports

Open only to the App EC2 security group:
- 5432
- 6379
- 5672

Do not expose broker management ports publicly unless explicitly required.

## Validation

Confirm services are listening and reachable from App EC2.

## Backups

At minimum:
- daily PostgreSQL backup
- configuration backup for RabbitMQ
- documented restore test plan

## Operations

- monitor disk growth on PostgreSQL
- watch memory pressure because Redis and RabbitMQ are stateful
- rotate logs and keep storage headroom
