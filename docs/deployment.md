# Deploying the broker on a free Oracle Cloud VM

This is the runbook for putting the broker on a public address at no cost. It
was written for Oracle Cloud's Always Free tier, the only permanent free VM
that gives you a real public IP and lets you open a raw TCP port (the broker
speaks the Kafka protocol over TCP, not HTTP, so hosts that only run web apps
cannot run it).

Oracle's console changes its wording from time to time. If a button is named
slightly differently from what is written here, look for the nearest match.

## What you are building

```
your laptop ----TCP 9092----> [ Oracle security list ] -> [ VM firewall ] -> Docker -> broker
your laptop ----SSH 22 (your IP only)---------------------> VM  (admin, and tunnels to Grafana)
```

- One ARM virtual machine with 2 CPUs, 12 GB of memory and a 50 GB disk.
- Docker runs the broker. The data lives in a Docker volume on the VM's disk.
- Only port 9092 (Kafka) is open to everyone. SSH is open to your own IP only.
  Grafana, Prometheus and the metrics port are never exposed; you reach them
  through an SSH tunnel.

## What "free" means here, and the catches

Checked against [Oracle's Always Free resources page](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm):

- **Compute:** the Always Free Arm allowance is 1,500 OCPU-hours and 9,000
  GB-hours a month, which Oracle says is equivalent to 2 OCPUs and 12 GB of
  memory. Some older guides say 4 OCPUs and 24 GB; that is no longer what the
  page says.
- **Disk:** 200 GB of block storage in total, boot volume included. The VM's
  minimum boot volume is about 50 GB.
- **Traffic:** 10 TB a month outbound.
- **Idle reclaim:** Oracle may reclaim an Always Free instance that is idle,
  meaning that over 7 days its CPU, network and (on Arm) memory use all stay
  under 20 percent. A quiet demo broker is idle by that definition. The plan
  for this is not to prevent it but to make recovery cheap: everything needed
  to rebuild is in this repository, and `scripts/redeploy.sh` brings a new VM
  back in a few minutes. The data on a reclaimed VM is lost.
- **Trial credit:** a new account gets a 30-day trial credit. When it ends the
  account falls back to Always Free; a VM inside the limits above keeps
  running at no charge. Do **not** click "Upgrade" unless you decide to.
- **Capacity:** popular regions often show "Out of capacity for shape
  VM.Standard.A1.Flex". That is Oracle having no free Arm hosts at that
  moment, not an error of yours. Retrying later, or choosing another
  availability domain in the same region, usually works.

## Part 1: what you do in the Oracle console and on your laptop

### 1. Make an SSH key (on your laptop)

SSH logs you into the VM using a key pair instead of a password: a private key
that never leaves your laptop, and a public key you give to Oracle. In
PowerShell:

```powershell
ssh-keygen -t ed25519 -f $env:USERPROFILE\.ssh\oracle_kafka -C "oracle-kafka"
```

Press Enter at the passphrase prompt for none, or set one. This creates
`oracle_kafka` (private; never share or commit it) and `oracle_kafka.pub`
(public). To copy the public key to the clipboard:

```powershell
Get-Content $env:USERPROFILE\.ssh\oracle_kafka.pub | Set-Clipboard
```

### 2. Create the Oracle Cloud account

1. Go to <https://www.oracle.com/cloud/free/> and choose **Start for free**.
2. Fill in your name and email and verify the email.
3. Choose the **home region**. This is permanent for the account and is where
   Always Free resources must be created. Pick a region close to you or your
   audience. Arm capacity varies by region and the signup page does not show
   it, so if you can choose among regions you would be equally happy with,
   prefer a large one.
4. Enter an address, then a credit or debit card. Oracle uses it to prove you
   are a person and may place a small temporary authorization hold that is
   released. Prepaid and virtual cards are often rejected.
5. Finish. Account setup can take a few minutes to a few hours; you get an
   email when it is ready.

### 3. Create the VM

In the console, open the menu (top left) and choose **Compute**, then
**Instances**, then **Create instance**.

- **Name:** `kafka-go`.
- **Image and shape:** click **Edit**.
  - **Change shape:** pick **Ampere**, then `VM.Standard.A1.Flex`. Set
    **OCPUs to 2** and **memory to 12 GB**.
  - **Change image:** pick **Ubuntu**, then **Canonical Ubuntu 24.04**. Make
    sure it is the **aarch64** (Arm) build; the Ampere shape only offers
    those.
- **Networking:** keep the defaults (it creates a virtual network and a public
  subnet for you) and make sure **Automatically assign public IPv4 address** is
  on.
- **Add SSH keys:** choose **Paste public keys** and paste the contents of
  `oracle_kafka.pub`.
- **Boot volume:** leave the default (about 50 GB). Do not enlarge it past what
  keeps the account under 200 GB in total.
- Click **Create**. Wait until the instance state is **Running** and copy the
  **public IP address** shown on its page.

If creation fails with "Out of capacity", change the **Availability domain**
(under Placement) and try again, then try again later in the day.

### 4. Make the public IP permanent

An automatically assigned IP is ephemeral: it can change if the instance is
stopped. Clients are told the address in `KAFKA_ADVERTISED_HOST`, so it must
not change.

1. Menu, **Networking**, **IP management**, **Reserved public IPs**, then
   **Reserve public IP address**. Name it `kafka-go-ip`.
2. Go back to the instance, open **Attached VNICs** (or **Networking**), click
   the primary VNIC, then **IPv4 addresses**.
3. On the IP row choose **Edit**, change to **No public IP**, and save. Then
   **Edit** again, choose **Reserved public IP**, pick `kafka-go-ip`, and save.
4. Note the reserved address. This is the one you use from now on.

A reserved IP is free while it is attached to a running instance.

### 5. Open port 9092, and lock SSH to your IP

Oracle's cloud firewall is called a **security list**, and it is separate from
any firewall on the VM itself. Both must allow the port.

1. Menu, **Networking**, **Virtual cloud networks**, click your network, then
   click the **public subnet**, then the **Default Security List**.
2. **Add Ingress Rules**:
   - Source CIDR: `0.0.0.0/0`
   - IP protocol: TCP
   - Destination port range: `9092`
   - Description: `kafka-go broker`
3. The default list already has an SSH rule for port 22 from `0.0.0.0/0`
   (everyone). Edit it so the source is **your own IP followed by /32**. To
   find your IP, open <https://ifconfig.me> from the laptop you will use.

Do **not** open ports 3000, 9090 or 9101. If your home IP later changes, SSH
stops working; edit that rule in the console to fix it.

### 6. Set a spending alert (optional, recommended)

Menu, **Billing & Cost Management**, **Budgets**, **Create budget**: set the
amount to 1 US dollar a month with an alert on actual spend at 100 percent to
your email. An Always Free account that was never upgraded cannot be charged;
this is a second safety net for the day someone clicks Upgrade.

## Part 2: set up the VM

Connect from your laptop:

```powershell
ssh -i $env:USERPROFILE\.ssh\oracle_kafka ubuntu@<the reserved IP>
```

Answer `yes` to the fingerprint question the first time. Then, on the VM:

```bash
git clone https://github.com/AfzalRaja001/kafka-go.git
cd kafka-go
bash scripts/bootstrap-vm.sh
```

If the script says you were added to the docker group, log out (`exit`),
connect again, and `cd kafka-go`.

Create the settings file:

```bash
cp deploy/prod/.env.example deploy/prod/.env
nano deploy/prod/.env
```

Set `KAFKA_ADVERTISED_HOST=` to the reserved IP. Save with Ctrl+O, Enter, and
leave with Ctrl+X. Then start the broker:

```bash
bash scripts/redeploy.sh
```

The first build takes several minutes on the VM. When it finishes it prints the
container status and a test command.

## Part 3: check it from outside

From a machine that is **not** the VM, ideally one on a different network (a
phone hotspot works), with the Apache Kafka command-line tools unpacked:

```bash
bin/kafka-topics.sh --bootstrap-server <IP>:9092 --create --topic hello --partitions 1 --replication-factor 1
echo "first message" | bin/kafka-console-producer.sh --bootstrap-server <IP>:9092 --topic hello --producer-property enable.idempotence=false
bin/kafka-console-consumer.sh --bootstrap-server <IP>:9092 --topic hello --from-beginning --max-messages 1
```

You should see `first message`. If it works from the hotspot, it works from
the internet.

## Daily operations

| Task | Command (run on the VM, in the repository) |
|---|---|
| Update to the latest code and restart | `bash scripts/redeploy.sh` |
| Also run Prometheus and Grafana | `MONITORING=1 bash scripts/redeploy.sh` |
| See the broker's log | `docker compose --env-file deploy/prod/.env -f deploy/prod/docker-compose.prod.yml logs -f broker` |
| Restart without rebuilding | `docker compose --env-file deploy/prod/.env -f deploy/prod/docker-compose.prod.yml restart broker` |
| Check the disk | `df -h /` and `docker system df` |

To look at the dashboards, open a tunnel from your laptop and then browse to
<http://localhost:3000>:

```powershell
ssh -i $env:USERPROFILE\.ssh\oracle_kafka -L 3000:127.0.0.1:3000 ubuntu@<IP>
```

## If something is wrong

| Symptom | Likely cause and fix |
|---|---|
| The connection hangs or times out | The security list rule for 9092 is missing, or the VM firewall is. On the VM, `sudo iptables -L INPUT -n --line-numbers` should show an ACCEPT for port 9092; rerun `scripts/bootstrap-vm.sh` if not. |
| Listing topics works, but produce or consume hangs | `KAFKA_ADVERTISED_HOST` is wrong. The broker tells clients to reconnect to that address. Fix `deploy/prod/.env` and rerun `scripts/redeploy.sh`. |
| `redeploy.sh` says the broker did not start | Read the log lines it prints. A common cause is `KAFKA_ADVERTISED_PORT` or a limit variable that is set but not a valid number; the broker refuses to start and says which. |
| SSH stopped working | Your home IP changed. Update the port 22 rule in the security list. |
| The instance disappeared | Oracle reclaimed an idle instance. Create a new one (step 3), reattach the reserved IP (step 4), then Part 2. Topics and records are gone. |
| Someone is abusing the broker | Delete the port 9092 ingress rule in the console. That closes it immediately. The broker's limits (`KAFKA_MAX_PARTITIONS`, `KAFKA_RETENTION_MAX_BYTES`) bound the damage but are not authentication. |

## What this deployment does not protect against

- The broker has no authentication or encryption. Anyone who can reach the
  port can read and write every topic. This is a demo broker; do not put
  anything private in it.
- The data is on one disk of one VM with no backup. If the VM goes, so does
  the data.
- The size limit is checked every five minutes, so a fast client can exceed it
  briefly (see the README's Configuration section).
