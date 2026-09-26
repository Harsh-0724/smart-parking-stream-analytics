# Deploying to an Ubuntu VM

Target: Ubuntu 22.04 or 24.04, either an Oracle Cloud Always Free ARM instance (VM.Standard.A1.Flex,
4 OCPU / 24 GB) or any x86 VM with 4 vCPU / 8 GB. CI publishes multi-arch images (linux/amd64 and
linux/arm64), so the same compose files work on both.

**What is exposed:** only Caddy, on ports 80 and 443. Kafka, TimescaleDB, Prometheus, Grafana and the
API have no published ports in `docker-compose.prod.yml`. Users reach the API and WebSocket through
Caddy at `/api` and `/ws`, and Grafana at `/grafana/` (Grafana's own login protects it).

Nothing here was deployed to a live VM; the artifacts are validated with
`docker compose -f docker-compose.yml -f docker-compose.prod.yml config`, which shows `web` as the
only service with published ports (80, 443) and fails if a required secret is unset.

## 1. Firewall: only 22, 80, 443

Cloud firewall (Oracle: VCN security list / OCI network security group; AWS/Azure/GCP: security group):
allow inbound TCP 22, 80, 443 and UDP 443 (HTTP/3). Nothing else.

On the VM as well (defence in depth):

```bash
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow 22/tcp
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw allow 443/udp
sudo ufw enable
```

Docker publishes container ports by editing iptables directly, which bypasses UFW. That is why the
prod compose file publishes nothing except 80 and 443: there is no other published port for UFW to be
bypassed for. Verify after the stack is up:

```bash
sudo ss -tlnp | grep -E 'docker-proxy|caddy'    # expect only :80 and :443
```

Oracle Ubuntu images ship restrictive iptables rules; if 80/443 do not answer from outside, allow
them there too: `sudo iptables -I INPUT -p tcp -m multiport --dports 80,443 -j ACCEPT` and persist with
`sudo apt install iptables-persistent`.

## 2. Install Docker

```bash
sudo apt-get update && sudo apt-get install -y ca-certificates curl git
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker "$USER" && newgrp docker
docker compose version          # v2.24 or newer is required for the prod override
```

## 3. Get the code and configure secrets

```bash
sudo mkdir -p /opt/smart-parking && sudo chown "$USER" /opt/smart-parking
git clone https://github.com/Harsh-0724/smart-parking-stream-analytics.git /opt/smart-parking
cd /opt/smart-parking
cp .env.example .env
chmod 600 .env
```

Edit `.env` (all secrets stay on the VM; `.env` is git-ignored):

| Variable | Set to |
|---|---|
| `POSTGRES_PASSWORD` | a long random string (the prod compose refuses to start without it) |
| `GRAFANA_PASSWORD` | a long random string (required in prod) |
| `SIM_SALT` | a random string; salts the vehicle-token hash |
| `KAFKA_CLUSTER_ID` | `docker run --rm apache/kafka:3.9.0 /opt/kafka/bin/kafka-storage.sh random-uuid` |
| `GHCR_OWNER` | your GitHub user or org name, **lowercase** (images are `ghcr.io/<owner>/smart-parking-*`) |
| `SITE_ADDRESS` | `:80` to serve plain HTTP by IP, or your domain (see step 6) |
| `IMAGE_TAG` | optional; `latest` by default, or a commit SHA to pin/roll back |

## 4. Pull images and start

If the GHCR packages are private, log in once with a token that has `read:packages`:

```bash
echo "$GHCR_PAT" | docker login ghcr.io -u <github-user> --password-stdin
```

```bash
export COMPOSE="docker compose -f docker-compose.yml -f docker-compose.prod.yml"
$COMPOSE pull
$COMPOSE up -d --wait
$COMPOSE ps
```

`--wait` returns when every service with a healthcheck is healthy (the one-shot `init` container that
creates the topics exits 0 by design). Open `http://<vm-ip>/`: the Overview shows live data within
about a minute (the simulator is the data source of the deployed demo). Processors default to 2
replicas; `$COMPOSE up -d --scale processor=3 processor` adds a third.

Check that nothing else is listening: `sudo ss -tlnp | grep -E 'docker-proxy|caddy'`.

## 5. Resource sizing

Kafka brokers are capped at `-Xmx512m` each. Approximate steady footprint: 3 brokers 2.5 GB, TimescaleDB 0.5 GB,
Prometheus 0.3 GB, Grafana 0.2 GB, Python services about 0.15 GB each. 8 GB is enough; the load test
in `docs/RESULTS.md` was measured on a 10-CPU laptop.

## 6. HTTPS (automatic)

1. Create a DNS `A` record for your name (for example `parking.example.com`) pointing at the VM.
2. Set `SITE_ADDRESS=parking.example.com` in `.env`.
3. `$COMPOSE up -d web`. Caddy obtains and renews a Let's Encrypt certificate itself; ports 80 and 443
   must be reachable from the internet for the challenge. Certificates persist in the `caddy-data` volume.

Without a domain, keep `SITE_ADDRESS=:80` and use plain HTTP by IP.

## 7. Backups and retention

- **Nightly pg_dump:** `crontab -e` and add
  `0 2 * * * /opt/smart-parking/infra/backup/pg_backup.sh >> /var/log/parking-backup.log 2>&1`.
  Dumps go to `/var/backups/smart-parking` (override with `BACKUP_DIR`), the last 14 are kept, and an empty
  dump aborts before anything is rotated.
- **Restore:** `$COMPOSE exec -T timescaledb pg_restore -U parking -d parking --clean --if-exists < parking-<stamp>.dump`
- **Kafka retention:** `parking.raw` keeps 3 days (`retention.ms` in `infra/create_topics.sh`); DLQ, late and
  alert topics keep 7 days; state and metadata topics are compacted.
- **TimescaleDB:** chunks older than 7 days are compressed, older than 90 days are dropped (`infra/timescale/init.sql`).

## 8. CI/CD from GitHub

`.github/workflows/ci.yml` runs on every push. On pushes to `main`, once backend, integration, frontend and
Playwright jobs pass, the `images` job builds and pushes `smart-parking-{processor,sink,alerter,api,web}`
(amd64 and arm64) to GHCR, tagged `latest` and the commit SHA, using the built-in `GITHUB_TOKEN`.
No credential is stored in the repository.

To enable the SSH deploy job, create a key pair for deployment only, add the public key to
`~/.ssh/authorized_keys` on the VM, then in the repository settings add:

| Kind | Name | Value |
|---|---|---|
| Variable | `DEPLOY_ENABLED` | `true` |
| Secret | `DEPLOY_HOST` | VM address |
| Secret | `DEPLOY_USER` | SSH user |
| Secret | `DEPLOY_SSH_KEY` | the private key |

The job runs `git pull`, `compose pull` and `compose up -d` with `IMAGE_TAG` set to the commit that was built.
To roll back: `IMAGE_TAG=<older-sha> $COMPOSE up -d`.

## 9. Operating it

```bash
$COMPOSE logs -f processor          # structured JSON logs
$COMPOSE ps
docker kill <processor-container>   # the demo failure: the survivor takes over its partitions
```

Grafana: `https://<host>/grafana/` (user `admin`, password `GRAFANA_PASSWORD`), dashboard "Pipeline health".
Prometheus is internal only. To query it from the VM:
`$COMPOSE exec prometheus wget -qO- 'http://localhost:9090/api/v1/query?query=up'`.

**Reset event-time state** (needed if you restart the simulator with a different start time; a stale
watermark would classify every new event as late): `$COMPOSE down` then remove the Kafka volumes
(`docker volume rm $(docker volume ls -q | grep -E 'kafka-[123]-data|timescale-data')`) and `up -d` again. For a
softer reset on a running dev stack use `make demo-reset`.
