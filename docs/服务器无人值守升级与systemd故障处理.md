# 服务器无人值守升级与 systemd 故障处理

最后更新：2026-10-22 20:15:32

## 适用范围

远程服务器 `trafficingestor-NF5280M6`（Ubuntu 24.04）上 Docker 采集任务因 systemd 管理器失联而无法启动容器时的排查、恢复与自动升级关闭方法。

## 故障现象

- `docker run` 创建容器时报 `unable to apply cgroup configuration` / `Failed to activate service 'org.freedesktop.systemd1'`。
- Docker 守护进程可用，但容器全部停留在 `Created` 状态。

## 诊断命令

```bash
systemctl is-system-running
systemctl list-units --state=failed --no-pager
busctl --system call org.freedesktop.DBus /org/freedesktop/DBus org.freedesktop.DBus GetNameOwner s org.freedesktop.systemd1
grep systemd/private /proc/net/unix
docker info --format 'cgroup={{.CgroupDriver}}/{{.CgroupVersion}} containers={{.Containers}} running={{.ContainersRunning}}'
```

判定依据：

- `systemctl` 超时，`GetNameOwner` 返回 `no such name`。
- `/proc/net/unix` 中没有 `/run/systemd/private`。
- Docker 自身正常，但启动容器时 systemd cgroup scope 创建失败。

## 关闭无人值守升级

### 仅关闭自动安装升级

保留软件源列表刷新，只停止自动升级：

```bash
sudo sed -i 's/^APT::Periodic::Unattended-Upgrade .*/APT::Periodic::Unattended-Upgrade 0;/' /etc/apt/apt.conf.d/20auto-upgrades
grep Periodic /etc/apt/apt.conf.d/20auto-upgrades
```

或交互式选择 `No`：

```bash
sudo dpkg-reconfigure unattended-upgrades
```

### 彻底关闭 apt 后台任务

```bash
sudo systemctl disable --now apt-daily.timer apt-daily-upgrade.timer
sudo systemctl disable --now unattended-upgrades.service
```

### 仅屏蔽 systemd 等高风险包

编辑 `/etc/apt/apt.conf.d/50unattended-upgrades`：

```text
Unattended-Upgrade::Package-Blacklist {
    systemd;
    systemd-*;
    udev;
    libsystemd0;
    libudev1;
    libnss-systemd;
    libpam-systemd;
};
```

## systemd 失联后的恢复

systemd 管理接口不可用时，`systemctl reboot` 也可能失败。确认无运行任务后执行：

```bash
docker ps
tmux ls
sudo systemctl reboot -ff
```

`-ff` 为立即重启，不等待服务退出；无法执行时的最后手段：

```bash
sudo sh -c 'echo b > /proc/sysrq-trigger'
```

重启后：

```bash
systemctl is-system-running
sudo dpkg --configure -a
docker info --format '{{.CgroupDriver}}'
```

确认 systemd 与 Docker 正常后，重新执行采集命令；脚本默认 `CLEAN_AND_RECREATE_CONTAINERS=True`，会清理失败遗留的容器。

## 验证

```bash
grep -Rhs 'Periodic' /etc/apt/apt.conf.d/
systemctl list-timers 'apt-daily*' --all
```

## 风险与注意事项

- 关闭自动升级后需定期手动执行 `sudo apt update && sudo apt upgrade`，并关注安全补丁。
- `systemctl reboot -ff` 与 sysrq 强制重启可能导致未落盘数据丢失。
- 屏蔽 systemd 相关包会同时延迟其安全修复，需安排维护窗口手动升级并重启。
