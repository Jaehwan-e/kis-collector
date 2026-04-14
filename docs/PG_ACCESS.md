# PostgreSQL 외부 접속 허용 설정

## pg_hba.conf 위치 확인

```bash
psql stock_data -t -c "SHOW hba_file;"
```

macOS Homebrew: `/opt/homebrew/var/postgresql@17/pg_hba.conf`

## 특정 IP 허용 추가

```bash
# 형식
echo 'host [DB명] [유저] [IP]/32 md5' >> /opt/homebrew/var/postgresql@17/pg_hba.conf

# 예시: 158.179.168.237에서 gyeol 유저로 stock_data 접속 허용
echo 'host stock_data gyeol 172.30.1.36/32 md5' >> /opt/homebrew/var/postgresql@17/pg_hba.conf

# 예시: 모든 IP 허용
echo 'host stock_data gyeol 0.0.0.0/0 md5' >> /opt/homebrew/var/postgresql@17/pg_hba.conf
```

## 설정 적용

```bash
brew services restart postgresql@17
```

## 현재 설정 확인

```bash
cat /opt/homebrew/var/postgresql@17/pg_hba.conf | grep -v '^#' | grep -v '^$'
```

## 주요 옵션

| 항목 | 값 | 설명 |
|------|----|------|
| TYPE | `host` | TCP/IP (비암호화), `hostssl`은 SSL 필수 |
| DATABASE | `stock_data` / `all` | 허용할 DB |
| USER | `gyeol` / `all` | 허용할 유저 |
| ADDRESS | `IP/32` | 단일 IP, `/24`면 서브넷 전체 |
| METHOD | `md5` | 비밀번호 인증, `trust`면 비밀번호 없이 허용 |

## listen_addresses 확인

외부 접속을 받으려면 PostgreSQL이 외부 인터페이스에서 리슨해야 함.

```bash
psql stock_data -t -c "SHOW listen_addresses;"
```

`localhost`만 나오면 외부 접속 불가. 변경:

```bash
# postgresql.conf 위치
psql stock_data -t -c "SHOW config_file;"

# listen_addresses를 '*'로 변경
# listen_addresses = '*'

# 재시작 필요
brew services restart postgresql@17
```
