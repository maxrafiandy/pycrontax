#!/bin/sh

docker run -it --rm --name etax_dashboard_jobs --mount \
  type=bind,source="$(pwd)",target=/usr/local/etax-dashboard-jobs $1