#!/usr/bin/env bash
# One-time setup of a fresh Ubuntu 24.04 VM (the free Oracle Cloud ARM
# instance) so it can run the broker. Safe to run again: every step checks
# whether it is already done.
#
# Run it as your normal login user (it uses sudo itself), not as root:
#   bash scripts/bootstrap-vm.sh
#
# What it does:
#   1. installs Docker, the Compose plugin, the build plugin and git from
#      Ubuntu's own package repository;
#   2. lets your user run docker without sudo;
#   3. opens TCP 9092 in the VM's own firewall and saves that across reboots.
#
# It does NOT open anything in Oracle's cloud firewall (the security list in
# the web console). That is a separate switch you flip yourself; see
# docs/deployment.md.
set -euo pipefail

PORT=9092

say() { printf '\n==> %s\n' "$*"; }

if [ "$(id -u)" -eq 0 ]; then
  echo "Run this as your normal user, not root, so that the docker group is given to you." >&2
  exit 1
fi

say "Installing Docker, Compose, buildx and git"
sudo apt-get update -y
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io docker-compose-v2 docker-buildx git
sudo systemctl enable --now docker

# id -un instead of $USER: that variable is not set in every kind of shell,
# and with "set -u" an unset variable would stop the script here.
me=$(id -un)
say "Letting $me run docker without sudo"
relogin=0
if id -nG "$me" | tr ' ' '\n' | grep -qx docker; then
  echo "already in the docker group"
else
  sudo usermod -aG docker "$me"
  relogin=1
fi

# Oracle's Ubuntu images ship iptables rules that reject everything except
# SSH. Docker's published ports normally bypass that INPUT chain, but an
# explicit rule keeps the port reachable if that ever changes, and it is the
# rule a natively-run broker would need. The rule goes in before the first
# REJECT or DROP so it can actually match.
say "Opening TCP $PORT in the VM's firewall"
rule=(-p tcp --dport "$PORT" -m conntrack --ctstate NEW -j ACCEPT)
if sudo iptables -C INPUT "${rule[@]}" 2>/dev/null; then
  echo "rule already present"
else
  pos=$(sudo iptables -L INPUT --line-numbers -n | awk '$1 ~ /^[0-9]+$/ && /REJECT|DROP/ {print $1; exit}')
  sudo iptables -I INPUT "${pos:-1}" "${rule[@]}"
  echo "rule added at position ${pos:-1}"
fi

say "Saving the firewall rules across reboots"
if ! command -v netfilter-persistent >/dev/null 2>&1; then
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y iptables-persistent
fi
sudo netfilter-persistent save

say "Done"
if [ "$relogin" -eq 1 ]; then
  echo "You were just added to the docker group. Log out and back in (or run: newgrp docker)"
  echo "before using docker without sudo."
fi
echo "Next: copy deploy/prod/.env.example to deploy/prod/.env, set KAFKA_ADVERTISED_HOST,"
echo "then run: bash scripts/redeploy.sh"
